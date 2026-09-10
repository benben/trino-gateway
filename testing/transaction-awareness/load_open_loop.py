#!/usr/bin/env python3
"""Bounded open-loop HTTP load. Keep endpoint configuration and results private."""

import argparse
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import http.client
import json
import math
import os
from pathlib import Path
import queue
import re
import ssl
import threading
import time
from urllib.parse import urlsplit


RESPONSE_503_CATEGORIES = {
    b"Transaction-aware request capacity is unavailable; no backend request was dispatched": "request_capacity",
    b"Transaction routing state is unavailable; the request was not reassigned": "routing_state_unavailable",
    b"Transaction routing state rejected the operation: NOT_ACTIVE": "backend_not_active",
    b"No backend belongs to the selected routing group": "no_backend_for_group",
    b"Backend must expose a ready Trino coordinator process identity": "process_identity_not_ready",
    b"Backend process identity is unavailable": "process_identity_unavailable",
}


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def distribution(values):
    return {name: percentile(values, value) for name, value in [("p50", .5), ("p95", .95), ("p99", .99)]}


def invalid(result):
    if result is None:
        return False
    return bool(result["errors"] or result["client_bottleneck_detected"] or result["unconsumed_continuations"])


def read_client_cpu():
    """Read cgroup-v2 counters without substituting unavailable measurements."""
    row = {"available": True, "valid": False, "reason": None, "cpu_max": None, "counters": None,
           "read_start_monotonic_seconds": time.monotonic(),
           "read_start_utc": datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")}
    try:
        maximum = Path("/sys/fs/cgroup/cpu.max").read_text().strip()
        raw = Path("/sys/fs/cgroup/cpu.stat").read_text()
        if not re.fullmatch(r"(max|[1-9][0-9]*) [1-9][0-9]*", maximum):
            raise ValueError("Malformed CPU quota")
        quota, period = maximum.split()
        counters = {}
        for line in raw.splitlines():
            match = re.fullmatch(r"([a-z_]+) ([0-9]+)", line)
            if not match or match[1] in counters:
                raise ValueError("Malformed CPU counter")
            counters[match[1]] = int(match[2])
        if not {"usage_usec", "nr_periods", "nr_throttled", "throttled_usec"} <= counters.keys():
            raise ValueError("Missing CPU counters")
        row.update(valid=True, cpu_max=[None if quota == "max" else int(quota), int(period)], counters=counters)
    except OSError:
        row.update(available=False, reason="cgroup_v2_unavailable_or_unreadable")
    except (ValueError, UnicodeError):
        row["reason"] = "malformed_cgroup_v2_cpu_data"
    row["read_end_utc"] = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    row["read_end_monotonic_seconds"] = time.monotonic()
    return row


def client_cpu_span(before, after):
    result = {"available": before["available"] and after["available"], "valid": False, "reason": None,
              "elapsed_seconds_bounds": None, "counter_deltas": None, "average_cpu_cores_bounds": None}
    if not result["available"] or not before["valid"] or not after["valid"]:
        result["reason"] = "snapshot_unavailable_or_invalid"
        return result
    times = [row[key] for row in (before, after)
             for key in ("read_start_monotonic_seconds", "read_end_monotonic_seconds")]
    if (any(type(value) not in (int, float) or not math.isfinite(value) for value in times) or
            not times[0] <= times[1] < times[2] <= times[3]):
        result["reason"] = "invalid_or_nonincreasing_read_times"
    elif before["cpu_max"] != after["cpu_max"]:
        result["reason"] = "cpu_quota_changed"
    elif (before["counters"].keys() != after["counters"].keys() or
          any(after["counters"][key] < value for key, value in before["counters"].items())):
        result["reason"] = "cpu_counter_reset_or_shape_changed"
    else:
        elapsed = {"min": times[2] - times[1], "max": times[3] - times[0]}
        deltas = {key: after["counters"][key] - value for key, value in before["counters"].items()}
        result.update(valid=True, elapsed_seconds_bounds=elapsed, counter_deltas=deltas,
                      average_cpu_cores_bounds={"min": deltas["usage_usec"] / 1_000_000 / elapsed["max"],
                                               "max": deltas["usage_usec"] / 1_000_000 / elapsed["min"]})
    return result


def client_cpu_diagnostics(snapshots):
    if set(snapshots) != {"before_setup", "before_workload", "after_completion"}:
        raise ValueError("Require all three client CPU snapshots")
    spans = {"setup_and_scheduled_wait": client_cpu_span(snapshots["before_setup"], snapshots["before_workload"]),
             "workload_and_late_completion": client_cpu_span(snapshots["before_workload"], snapshots["after_completion"])}
    return {"available": all(row["available"] for row in snapshots.values()),
            "valid": all(row["valid"] for row in snapshots.values()) and all(row["valid"] for row in spans.values()),
            "snapshots": snapshots, "spans": spans,
            "interpretation": "Cgroup counter deltas cover actual read-bounded spans, not exact timed-window CPU. "
                              "Setup includes scheduled waiting; workload includes late completion and excludes continuation cleanup."}


def prepare_workers(executor, count, timeout=30):
    """Start bounded client workers without opening backend connections."""
    if not 1 <= count <= 512 or not 0 < timeout <= 60:
        raise ValueError("Worker preparation requires bounded capacity and timeout")
    deadline = time.monotonic() + timeout
    release = threading.Event()
    condition = threading.Condition()
    ready = 0
    futures = []

    def prepare():
        nonlocal ready
        with condition:
            ready += 1
            condition.notify_all()
        release.wait(max(0, deadline - time.monotonic()))

    try:
        for _ in range(count):
            if time.monotonic() >= deadline:
                raise TimeoutError("Client worker preparation exceeded its deadline")
            futures.append(executor.submit(prepare))
        with condition:
            complete = condition.wait_for(lambda: ready == count, timeout=max(0, deadline - time.monotonic()))
        if not complete or time.monotonic() >= deadline:
            raise TimeoutError("Client worker preparation exceeded its deadline")
    finally:
        release.set()
    for future in futures:
        future.result(timeout=max(0, deadline - time.monotonic()))
    if time.monotonic() >= deadline:
        raise TimeoutError("Client worker preparation exceeded its deadline")


class Connections:
    """Reuse verified connections without retrying potentially accepted requests."""

    def __init__(self, gateways, timeout):
        self.gateways = gateways
        self.timeout = timeout
        self.context = ssl.create_default_context(cafile=os.environ.get("TX_CA_FILE"))
        self.pools = [queue.SimpleQueue() for _ in gateways]

    def request(self, index, method, path, body, headers):
        pool = self.pools[index]
        try:
            connection = pool.get_nowait()
        except queue.Empty:
            url = urlsplit(self.gateways[index])
            constructor = VerifiedNameConnection if url.scheme == "https" else http.client.HTTPConnection
            options = {"context": self.context} if url.scheme == "https" else {}
            if url.scheme == "https":
                options["server_name"] = os.environ.get("TX_TLS_SERVER_NAME")
            connection = constructor(url.hostname, url.port, timeout=self.timeout, **options)
        try:
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            data = response.read()
            status = response.status
            if response.will_close:
                connection.close()
            else:
                pool.put(connection)
            return status, data
        except Exception:
            connection.close()
            raise

    def close(self):
        for pool in self.pools:
            while True:
                try:
                    pool.get_nowait().close()
                except queue.Empty:
                    break


class VerifiedNameConnection(http.client.HTTPSConnection):
    """Verify a configured service identity when addressing its individual pods."""

    def __init__(self, *args, server_name=None, **kwargs):
        self.server_name = server_name
        super().__init__(*args, **kwargs)

    def connect(self):
        if not self.server_name:
            return super().connect()
        http.client.HTTPConnection.connect(self)
        self.sock = self._context.wrap_socket(self.sock, server_hostname=self.server_name)


class OpenLoop:
    def __init__(self, gateways, groups, authorization, *, rate, duration, concurrency=128,
                 timeout=10, maximum_lag=.05, transport=None, expected_backends=None,
                 measurement_start_utc=None, processes=1):
        if not gateways or not groups or not authorization:
            raise ValueError("Explicit gateways, routing groups, and runtime authorization are required")
        if not 1 <= rate <= 10000 or not 0 < duration <= 600 or rate * duration > 1_000_000:
            raise ValueError("Keep each run bounded to at most one million requests and ten minutes")
        if not 1 <= concurrency <= 512 or not 0 < timeout <= 120 or maximum_lag <= 0:
            raise ValueError("Invalid bounded client resource or timeout settings")
        if type(processes) is not int or processes not in (1, 8):
            raise ValueError("Use one or eight client processes")
        if processes == 8 and (type(rate) is not int or rate not in (100, 1000) or
                               type(concurrency) is not int or concurrency % 8 or transport is not None or
                               not math.isfinite(duration) or int(rate * duration) < 8 or
                               not math.isfinite(timeout) or not math.isfinite(maximum_lag)):
            raise ValueError("Eight processes require a supported aggregate rate, divisible capacity, and independent transports")
        self.processes = processes
        self.rotation_offset = 0
        self.gateways, self.groups, self.authorization = gateways, groups, authorization
        self.rate, self.duration, self.concurrency = rate, duration, concurrency
        self.timeout = timeout
        self.maximum_lag = maximum_lag
        self.transport = transport or (Connections(gateways, timeout) if processes == 1 else None)
        self.expected_backends = expected_backends or {}
        self.measurement_start = None
        if measurement_start_utc is not None:
            if not measurement_start_utc.endswith("Z") or "T" not in measurement_start_utc:
                raise ValueError("Measurement start must use ISO 8601 UTC with a Z suffix")
            self.measurement_start = datetime.fromisoformat(measurement_start_utc[:-1] + "+00:00")
        self.pending = deque()
        self.lock = threading.Lock()
        self.counts = Counter()
        self.statuses, self.errors, self.backend_rows = Counter(), Counter(), Counter()
        self.response_error_categories = {"measured": Counter(), "cleanup": Counter()}
        self.gateway_counts = [Counter() for _ in gateways]
        self.method_counts = Counter()
        self.latencies, self.corrected_latencies, self.lags = [], [], []
        self.inflight = self.peak_inflight = 0
        self.second_buckets = [
            dict(offset_seconds=offset, duration_seconds=min(1, duration - offset),
                 started=0, completed=0, successful=0, errors=0)
            for offset in range(math.ceil(duration))
        ]

    def record_window_event(self, timestamp, event):
        if self.window_beginning <= timestamp <= self.window_end:
            offset = min(int(timestamp - self.window_beginning), len(self.second_buckets) - 1)
            self.second_buckets[offset][event] += 1

    def perform(self, sequence, scheduled, end, cleanup=False):
        started = time.monotonic()
        with self.lock:
            continuation = self.pending.popleft() if self.pending else None
            if cleanup and continuation is None:
                return
            self.counts["cleanup_started" if cleanup else "started"] += 1
            if not cleanup:
                self.lags.append((started - scheduled) * 1000)
                if started <= end:
                    self.counts["started_in_window"] += 1
                self.record_window_event(started, "started")
        headers = {}
        if continuation:
            path, expected_backend = continuation
            method, body = "GET", None
        else:
            method, path, body = "POST", "/v1/statement", "SELECT 1"
            with self.lock:
                sql_sequence = self.counts["sql_submissions"]
                self.counts["sql_submissions"] += 1
            headers = {"Authorization": self.authorization, "X-Trino-User": "user",
                       "X-Trino-Transaction-Id": "NONE", "Content-Type": "text/plain",
                       "X-Trino-Routing-Group": self.groups[(sql_sequence + self.rotation_offset) % len(self.groups)]}
            expected_backend = self.expected_backends.get(headers["X-Trino-Routing-Group"])
        with self.lock:
            index = (self.method_counts[method] + self.rotation_offset + (1 if method == "GET" else 0)) % len(self.gateways)
            self.method_counts[method] += 1
        status, payload, error, response_category = None, None, None, None
        try:
            status, raw = self.transport.request(index, method, path, body, headers)
            if status == 200:
                payload = json.loads(raw)
                if "error" in payload:
                    error = "sql_error"
                if expected_backend is not None and payload.get("data") != [[expected_backend]] and not payload.get("nextUri"):
                    error = "wrong_backend_result"
            else:
                error = "http_" + str(status)
                if status == 503:
                    response_category = RESPONSE_503_CATEGORIES.get(raw, "unknown_503") if len(raw) <= 256 else "unknown_503"
        except Exception as failure:
            error = type(failure).__name__
        finished = time.monotonic()
        with self.lock:
            self.counts["cleanup_completed" if cleanup else "completed"] += 1
            prefix = "cleanup_" if cleanup else "measured_"
            self.gateway_counts[index][prefix + method] += 1
            self.gateway_counts[index][prefix + "status_" + str(status)] += 1
            if status is not None:
                self.statuses[str(status)] += 1
            if error:
                self.errors[error] += 1
            if response_category:
                self.response_error_categories["cleanup" if cleanup else "measured"][response_category] += 1
            if not cleanup:
                self.latencies.append((finished - started) * 1000)
                self.corrected_latencies.append((finished - scheduled) * 1000)
                if finished <= end:
                    self.counts["completed_in_window"] += 1
                    if not error:
                        self.counts["successful_in_window"] += 1
                self.record_window_event(finished, "completed")
                self.record_window_event(finished, "errors" if error else "successful")
            if payload and not error:
                if payload.get("nextUri"):
                    uri = urlsplit(payload["nextUri"])
                    self.pending.append((uri.path + ("?" + uri.query if uri.query else ""), expected_backend))
                else:
                    self.counts["sql_completed"] += 1
                    for row in payload.get("data", []):
                        if row and isinstance(row[0], str):
                            self.backend_rows[row[0]] += 1

    def run(self, *, _phase=None):
        if self.processes == 8:
            from load_multiprocess import run_multiprocess
            return run_multiprocess(self)
        permit = threading.BoundedSemaphore(self.concurrency)
        cpu_snapshots = {"before_setup": read_client_cpu()} if _phase is None else None
        setup_beginning = time.monotonic()
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            prepare_workers(executor, self.concurrency)
            setup_elapsed = time.monotonic() - setup_beginning
            if _phase is not None:
                beginning, window_start_utc, start_drift = _phase.start(setup_elapsed)
                self.rotation_offset = _phase.process_index
            elif self.measurement_start is not None:
                delay = self.measurement_start.timestamp() - time.time()
                if delay > 600:
                    raise ValueError("Measurement start must be at most 600 seconds away after warmup")
                if delay < -1:
                    raise ValueError("Scheduled measurement start was missed by more than one second")
                if delay > 0:
                    time.sleep(delay)
            if _phase is None:
                cpu_snapshots["before_workload"] = read_client_cpu()
                beginning = time.monotonic()
                window_start_utc = datetime.fromtimestamp(time.time(), timezone.utc)
                start_drift = None if self.measurement_start is None else (window_start_utc - self.measurement_start).total_seconds()
            if start_drift is not None and abs(start_drift) > 1:
                raise ValueError("Scheduled measurement start was missed or the UTC clock changed")
            cpu_beginning = time.process_time()
            end = beginning + self.duration
            self.window_beginning, self.window_end = beginning, end
            sequences = range(int(self.rate * self.duration)) if _phase is None else range(
                _phase.process_index, int(_phase.global_rate * self.duration), 8)
            planned = len(sequences)
            schedule_rate = self.rate if _phase is None else _phase.global_rate
            futures = []

            def worker(sequence, scheduled):
                try:
                    self.perform(sequence, scheduled, end)
                finally:
                    with self.lock:
                        self.inflight -= 1
                    permit.release()

            for sequence in sequences:
                scheduled = beginning + sequence / schedule_rate
                delay = scheduled - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                if time.monotonic() - scheduled > self.maximum_lag:
                    self.counts["client_late_drops"] += 1
                    continue
                if not permit.acquire(blocking=False):
                    self.counts["client_capacity_drops"] += 1
                    continue
                with self.lock:
                    self.inflight += 1
                    self.peak_inflight = max(self.peak_inflight, self.inflight)
                future = executor.submit(worker, sequence, scheduled)
                if _phase is not None:
                    futures.append(future)
        for future in futures:
            future.result()
        measured_elapsed = time.monotonic() - beginning
        cpu_elapsed = time.process_time() - cpu_beginning
        if _phase is None:
            cpu_snapshots["after_completion"] = read_client_cpu()
        else:
            _phase.complete(measured_elapsed, cpu_elapsed)
        cleanup_deadline = time.monotonic() + min(120, max(30, self.timeout * 2))
        for sequence in range(planned, planned + self.concurrency * 20):
            if time.monotonic() >= cleanup_deadline:
                break
            with self.lock:
                if not self.pending:
                    break
            self.perform(sequence, time.monotonic(), end, cleanup=True)
        self.transport.close()
        counts = dict(self.counts)
        dropped = counts.get("client_late_drops", 0) + counts.get("client_capacity_drops", 0)
        request_rates = {}
        for event in ("started", "completed", "successful", "errors"):
            rates = [bucket[event] / bucket["duration_seconds"] for bucket in self.second_buckets]
            request_rates[event] = dict(min=min(rates), max=max(rates),
                                        avg=sum(bucket[event] for bucket in self.second_buckets) / self.duration)
        return {**({"final_inflight": self.inflight} if _phase is not None else {}),
                "scheduled_http_requests": planned, "target_http_rps": self.rate,
                "client_setup_seconds": setup_elapsed,
                "client_setup_method": "all executor workers started before the window; no backend traffic",
                "client_cgroup_cpu": client_cpu_diagnostics(cpu_snapshots) if cpu_snapshots else None,
                "window_start_utc": window_start_utc.isoformat(timespec="microseconds").replace("+00:00", "Z"),
                "window_end_utc": (window_start_utc + timedelta(seconds=self.duration)).isoformat(timespec="microseconds").replace("+00:00", "Z"),
                "planned_window_start_utc": None if self.measurement_start is None else self.measurement_start.isoformat(timespec="microseconds").replace("+00:00", "Z"),
                "measurement_start_drift_seconds": start_drift,
                "second_buckets": self.second_buckets, "request_rates": request_rates,
                "window_seconds": self.duration, "completion_elapsed_seconds": measured_elapsed,
                "offered_http_rps": counts.get("started_in_window", 0) / self.duration,
                "achieved_http_rps": counts.get("completed_in_window", 0) / self.duration,
                "successful_http_rps": counts.get("successful_in_window", 0) / self.duration,
                "counts": counts, "http_statuses": dict(self.statuses), "errors": dict(self.errors),
                "response_error_categories": {phase: dict(counts) for phase, counts in self.response_error_categories.items()},
                "latency_ms": distribution(self.latencies), "scheduled_latency_ms": distribution(self.corrected_latencies),
                "client_lag_ms": distribution(self.lags), "client_dropped_requests": dropped,
                "client_bottleneck_detected": dropped > 0 or (percentile(self.lags, .99) or 0) > 5,
                "client_concurrency": self.concurrency, "peak_client_inflight": self.peak_inflight,
                "gateway_endpoints": len(self.gateways), "routing_groups": len(self.groups),
                "per_gateway_counts": [dict(counts) for counts in self.gateway_counts],
                "client_cpu_seconds": cpu_elapsed, "client_average_cpu_cores": cpu_elapsed / measured_elapsed,
                "backend_result_rows": dict(self.backend_rows), "unconsumed_continuations": len(self.pending),
                "sql_note": "SQL submissions and completions are separate from HTTP requests; cleanup is outside the measured window."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=int, required=True, help="Aggregate scheduled HTTP requests per second, not SQL queries")
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--concurrency", type=int, default=128)
    parser.add_argument("--processes", type=int, choices=(1, 8), default=1)
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--warmup", type=float, default=10)
    parser.add_argument("--measurement-start-utc", help="Optional ISO 8601 Z start after warmup, at most 600 seconds away")
    parser.add_argument("--output", required=True, help="New private JSON receipt path")
    parser.add_argument("--print-receipt", action="store_true", help="Print metrics for capture from a completed disposable Job")
    args = parser.parse_args()
    if os.environ.get("TX_ALLOW_FIXTURE_MUTATION") != "yes":
        parser.error("Only explicitly authorized disposable fixtures may receive load")
    gateways = [item.rstrip("/") for item in os.environ.get("TX_GATEWAY_URLS", "").split(",") if item]
    groups = [item for item in os.environ.get("TX_LOAD_ROUTING_GROUPS", "").split(",") if item]
    expected = json.loads(os.environ.get("TX_LOAD_EXPECTED_BACKENDS", "{}"))
    if args.warmup < 0 or args.warmup > 60:
        parser.error("Warmup must be between zero and sixty seconds")
    options = dict(rate=args.rate, concurrency=args.concurrency, timeout=args.timeout, expected_backends=expected,
                   processes=args.processes)
    authorization = os.environ.get("TX_QUERY_AUTHORIZATION", "")
    warmup = OpenLoop(gateways, groups, authorization, duration=args.warmup, **options).run() if args.warmup else None
    if args.processes == 8 and invalid(warmup):
        result = {"measurement_not_started": True, "errors": {"invalid_warmup": 1},
                  "client_processes": 8, "client_bottleneck_detected": True,
                  "unconsumed_continuations": None, "offered_http_rps": None, "achieved_http_rps": None,
                  "successful_http_rps": None, "latency_ms": None}
    else:
        result = OpenLoop(gateways, groups, authorization, duration=args.duration,
                          measurement_start_utc=args.measurement_start_utc, **options).run()
    result["warmup"] = warmup
    result["valid_run"] = not (invalid(result) or invalid(warmup))
    descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as output:
        json.dump(result, output, indent=2)
    print(json.dumps(result if args.print_receipt else {key: result[key] for key in ["offered_http_rps", "achieved_http_rps", "latency_ms", "errors", "client_bottleneck_detected"]}))
    return int(not result["valid_run"])


if __name__ == "__main__":
    raise SystemExit(main())
