"""Run eight bounded client processes with a common arrival window."""

from datetime import datetime, timedelta, timezone
import json
import math
import multiprocessing
import queue
import threading
import time


PROCESSES = 8
MAX_FRAME_BYTES = 32 * 1024 * 1024


def ordinals(index, rate, duration):
    if type(index) is not int or not 0 <= index < PROCESSES:
        raise ValueError("Invalid worker index")
    return range(index, int(rate * duration), PROCESSES)


def stamp(value):
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def send(connection, message):
    payload = json.dumps(message, allow_nan=False, separators=(",", ":")).encode()
    if len(payload) > MAX_FRAME_BYTES:
        raise ValueError("Worker message exceeds its bounded frame")
    connection.send_bytes(payload)


class ChildPhase:
    def __init__(self, connection, index, rate):
        self.connection, self.process_index, self.global_rate = connection, index, rate

    def start(self, elapsed):
        send(self.connection, {"kind": "ready", "process_index": self.process_index, "setup_seconds": elapsed})
        message = json.loads(self.connection.recv_bytes(MAX_FRAME_BYTES))
        if message.get("kind") != "start":
            raise ValueError("Missing common start")
        return (message["monotonic"], datetime.fromisoformat(message["utc"].replace("Z", "+00:00")),
                message["drift"])

    def complete(self, elapsed, cpu):
        send(self.connection, {"kind": "completed", "process_index": self.process_index,
                               "elapsed_seconds": elapsed, "cpu_seconds": cpu})
        message = json.loads(self.connection.recv_bytes(MAX_FRAME_BYTES))
        if message != {"kind": "cleanup"}:
            raise ValueError("Missing cleanup release")


def worker(connection, index, configuration, transport_factory=None):
    from load_open_loop import OpenLoop
    loop = None
    try:
        options = dict(configuration)
        rate = options.pop("rate")
        options["concurrency"] //= PROCESSES
        gateways, groups, authorization = (options.pop(key) for key in ("gateways", "groups", "authorization"))
        loop = OpenLoop(gateways, groups, authorization, rate=rate / PROCESSES,
                        transport=transport_factory() if transport_factory else None, **options)
        result = loop.run(_phase=ChildPhase(connection, index, rate))
        report = {
            "process_index": index, "process_count": PROCESSES,
            "schedule_offset_seconds": index / rate, "schedule_stride_seconds": PROCESSES / rate,
            "result": result, "raw_samples": {"latency_ms": loop.latencies,
                "scheduled_latency_ms": loop.corrected_latencies, "client_lag_ms": loop.lags}}
        if loop.owner_policy is not None:
            report["cutover_samples"] = loop.cutover_samples
        send(connection, {"kind": "result", "process_index": index, "report": report})
    except BaseException as error:
        try:
            send(connection, {"kind": "error", "process_index": index, "error_type": type(error).__name__})
        except (OSError, ValueError):
            pass
    finally:
        if loop is not None:
            loop.transport.close()
        connection.close()


class Supervision:
    """Bound frame reads and process lifetimes independently of the caller."""

    def __init__(self, timeout):
        self.processes, self.connections, self.readers = [], [], []
        self.messages = queue.Queue(maxsize=40)
        self.expired, self.reader_failed = threading.Event(), threading.Event()
        self.closed = False
        self.clean = False
        self.deadline = time.monotonic() + timeout
        self.watchdog = threading.Timer(timeout, self.expire)
        self.watchdog.daemon = True
        self.watchdog.start()

    def expire(self):
        self.expired.set()
        for process in self.processes:
            try:
                if process.is_alive():
                    process.kill()
            except OSError:
                pass

    def read(self, index, connection):
        try:
            while True:
                raw = connection.recv_bytes(MAX_FRAME_BYTES)
                message = json.loads(raw)
                self.messages.put_nowait((index, message))
        except (EOFError, OSError, ValueError, queue.Full):
            try:
                self.messages.put_nowait((index, {"kind": "closed"}))
            except queue.Full:
                self.reader_failed.set()

    def attach(self, process, connection):
        self.processes.append(process)
        self.connections.append(connection)
        reader = threading.Thread(target=self.read, args=(len(self.processes) - 1, connection), daemon=True)
        self.readers.append(reader)
        reader.start()
        if self.expired.is_set():
            self.expire()

    def collect(self, kind, deadline, *, reports=None):
        received = {}
        while len(received) != PROCESSES:
            if self.expired.is_set() or self.reader_failed.is_set() or time.monotonic() >= deadline:
                raise TimeoutError("Worker phase exceeded its deadline")
            try:
                index, message = self.messages.get(timeout=min(.1, max(0, deadline - time.monotonic())))
            except queue.Empty:
                continue
            if time.monotonic() >= deadline or self.expired.is_set():
                raise TimeoutError("Worker message arrived after its deadline")
            if message.get("kind") == "closed" and index in received and kind == "result":
                continue
            if (message.get("kind") != kind or message.get("process_index") != index or index in received):
                raise ValueError("Missing, duplicate, or unexpected worker phase")
            received[index] = message
            if reports is not None:
                reports.append(message["report"])
        return received

    def close(self):
        if self.closed:
            return self.clean
        self.closed = True
        self.watchdog.cancel()
        self.watchdog.join(timeout=1)
        for process in self.processes:
            if process.is_alive():
                process.terminate()
        deadline = time.monotonic() + 3
        for process in self.processes:
            process.join(timeout=max(0, deadline - time.monotonic()))
        for process in self.processes:
            if process.is_alive():
                process.kill()
        deadline = time.monotonic() + 3
        for process in self.processes:
            process.join(timeout=max(0, deadline - time.monotonic()))
        for connection in self.connections:
            connection.close()
        for reader in self.readers:
            reader.join(timeout=max(0, deadline - time.monotonic()))
        self.clean = (not self.watchdog.is_alive() and not any(process.is_alive() for process in self.processes) and
                      not any(reader.is_alive() for reader in self.readers))
        return self.clean


def failure_receipt(loop, error, reports, snapshots):
    return {"errors": {"multiprocess_coordination_failure": 1}, "failure_type": type(error).__name__,
            "client_processes": PROCESSES, "client_bottleneck_detected": True,
            "unconsumed_continuations": None, "valid_run": False, "successful_http_rps": None,
            "offered_http_rps": None, "achieved_http_rps": None, "latency_ms": None,
            "target_http_rps": loop.rate, "window_seconds": loop.duration,
            "completed_child_receipts": [row["result"] for row in reports],
            "client_cgroup_cpu": {"available": False, "valid": False, "snapshots": snapshots},
            "no_automatic_retry": True, "uncertain_backend_work": True}


def run_multiprocess(loop, *, transport_factory=None, worker_target=worker, setup_timeout=60):
    from load_aggregate import aggregate_reports
    from load_open_loop import client_cpu_diagnostics, read_client_cpu

    if (type(loop.rate) is not int or loop.rate not in (100, 1000) or type(loop.concurrency) is not int or
            not 8 <= loop.concurrency <= 512 or loop.concurrency % PROCESSES or
            not math.isfinite(loop.duration) or not 8 <= int(loop.rate * loop.duration) <= 1_000_000 or
            not 0 < loop.duration <= 600 or not 0 < setup_timeout <= 60):
        raise ValueError("Invalid bounded eight-process profile")
    configuration = dict(gateways=loop.gateways, groups=loop.groups, authorization=loop.authorization,
                         rate=loop.rate, duration=loop.duration, concurrency=loop.concurrency,
                         timeout=loop.timeout, maximum_lag=loop.maximum_lag,
                         expected_backends=loop.expected_backends,
                         query_owners=loop.query_owners,
                         measurement_start_utc=None if loop.measurement_start is None else stamp(loop.measurement_start))
    snapshots = {"before_setup": read_client_cpu()}
    setup_beginning = time.monotonic()
    cleanup_budget = min(120, max(30, loop.timeout * 2)) + loop.timeout + 30
    supervisor = Supervision(setup_timeout + 601 + loop.duration + loop.timeout + cleanup_budget + 30)
    reports = []
    control = loop.cutover_control
    accepted = False
    stage = "worker_setup"
    try:
        context = multiprocessing.get_context("spawn")
        for index in range(PROCESSES):
            if time.monotonic() >= setup_beginning + setup_timeout:
                raise TimeoutError("Process startup exceeded its shared deadline")
            parent, child = context.Pipe(duplex=True)
            process = context.Process(target=worker_target, args=(child, index, configuration, transport_factory))
            try:
                process.start()
            except BaseException:
                parent.close()
                raise
            finally:
                child.close()
            supervisor.attach(process, parent)
            if time.monotonic() >= setup_beginning + setup_timeout:
                raise TimeoutError("Process startup exceeded its shared deadline")
        supervisor.collect("ready", setup_beginning + setup_timeout)
        setup_elapsed = time.monotonic() - setup_beginning
        delay = 0 if loop.measurement_start is None else loop.measurement_start.timestamp() - time.time()
        if delay > 600 or delay < -1:
            raise ValueError("Scheduled measurement start is outside its bounded interval")
        if delay > .1:
            time.sleep(delay - .1)
        snapshots["before_workload"] = read_client_cpu()
        beginning = time.monotonic() + .1
        window = datetime.fromtimestamp(time.time() + .1, timezone.utc)
        drift = None if loop.measurement_start is None else (window - loop.measurement_start).total_seconds()
        if drift is not None and abs(drift) > 1:
            raise ValueError("Scheduled measurement start was missed")
        for connection in supervisor.connections:
            send(connection, {"kind": "start", "monotonic": beginning, "utc": stamp(window), "drift": drift})
        if control is not None:
            control.start(beginning, beginning + loop.duration, window)
        stage = "background_completion"
        supervisor.collect("completed", beginning + loop.duration + loop.timeout + 30)
        stage = "cutover_control"
        event = control.finish_control() if control is not None else None
        completed_elapsed = time.monotonic() - beginning
        snapshots["after_completion"] = read_client_cpu()
        for connection in supervisor.connections:
            send(connection, {"kind": "cleanup"})
        supervisor.collect("result", time.monotonic() + cleanup_budget, reports=reports)
        deadline = time.monotonic() + 5
        for process in supervisor.processes:
            process.join(timeout=max(0, deadline - time.monotonic()))
        if supervisor.expired.is_set() or any(process.exitcode != 0 for process in supervisor.processes):
            raise ValueError("Worker did not exit successfully")
        result = aggregate_reports(reports, rate=loop.rate, duration=loop.duration, concurrency=loop.concurrency,
                                 window_start_utc=stamp(window), window_end_utc=stamp(window + timedelta(seconds=loop.duration)),
                                 planned_window_start_utc=None if loop.measurement_start is None else stamp(loop.measurement_start),
                                 measurement_start_drift_seconds=drift, cgroup_cpu=client_cpu_diagnostics(snapshots),
                                 completion_elapsed_seconds=completed_elapsed, client_setup_seconds=setup_elapsed)
        if control is not None:
            stage = "cutover_ownership_and_overlap"
            from load_cutover_aggregate import summarize_cutover
            result["cutover_metrics"] = summarize_cutover(reports, duration=loop.duration,
                cutover_start_seconds=event["cutover_start_seconds"], cutover_ack_seconds=event["cutover_ack_seconds"])
            result["cutover_control"] = event
            checks = result["cutover_metrics"]["ownership_checks"]
            if any(checks[name] <= 0 for name in ("pre_cutover_post_responses", "post_ack_post_starts", "pinned_continuations")):
                raise ValueError("Cutover lacks positive request ownership evidence")
            acknowledged, proved = event["cutover_ack_seconds"], event["proofs_completed_seconds"]
            if loop.duration - proved < 10:
                raise ValueError("Cutover proofs leave less than ten seconds of traffic")
            records = [row for child in reports for row in child["cutover_samples"]]
            intervals = {}
            for name, lower, upper in (("during_proofs", acknowledged, proved), ("after_proofs", proved, loop.duration)):
                intervals[name] = {"started": sum(lower < row[1] <= upper for row in records),
                                   "completed": sum(lower < row[3] <= upper for row in records)}
                if min(intervals[name].values()) <= 0:
                    raise ValueError("No background traffic overlaps the control proof or its tail")
            result["cutover_background_overlap"] = intervals
            result["cutover_control_valid"] = True
        if not supervisor.close() or supervisor.expired.is_set() or time.monotonic() >= supervisor.deadline:
            raise TimeoutError("Process supervision did not finish within its deadline")
        accepted = True
        return result
    except Exception as error:
        failed = failure_receipt(loop, error, reports, snapshots)
        if control is not None:
            failed["cutover_failure"] = {"stage": stage, "event": dict(control.event)}
        return failed
    finally:
        supervisor.close()
        if control is not None and not accepted:
            control.cancel()
