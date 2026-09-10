"""Validate and combine bounded child load receipts without averaging percentiles."""

from collections import Counter
from copy import deepcopy
from datetime import datetime, timedelta
import math


def number(value, *, integer=False, minimum=0):
    if type(value) not in ((int,) if integer else (int, float)) or not math.isfinite(value) or value < minimum:
        raise ValueError("Invalid finite nonnegative measurement")
    return value


def counters(value):
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError("Invalid counter map")
    return Counter({key: number(count, integer=True) for key, count in value.items()})


def utc(value):
    if not isinstance(value, str):
        raise ValueError("Missing UTC window")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() != timedelta(0):
        raise ValueError("Window must have an explicit UTC offset")
    return parsed


def aggregate_reports(children, *, rate, duration, concurrency, window_start_utc, window_end_utc,
                      planned_window_start_utc, measurement_start_drift_seconds, cgroup_cpu,
                      completion_elapsed_seconds, client_setup_seconds):
    """Require every child and preserve the global measurement denominator."""
    try:
        return _aggregate(children, rate=rate, duration=duration, concurrency=concurrency,
                          window_start_utc=window_start_utc, window_end_utc=window_end_utc,
                          planned_window_start_utc=planned_window_start_utc,
                          measurement_start_drift_seconds=measurement_start_drift_seconds,
                          cgroup_cpu=cgroup_cpu, completion_elapsed_seconds=completion_elapsed_seconds,
                          client_setup_seconds=client_setup_seconds)
    except (KeyError, TypeError, IndexError, OverflowError) as error:
        raise ValueError("Incomplete or malformed child evidence") from error


def _aggregate(children, *, rate, duration, concurrency, window_start_utc, window_end_utc,
               planned_window_start_utc, measurement_start_drift_seconds, cgroup_cpu,
               completion_elapsed_seconds, client_setup_seconds):
    from load_open_loop import distribution

    number(rate, integer=True, minimum=1)
    number(duration, minimum=.000001)
    number(concurrency, integer=True, minimum=8)
    number(completion_elapsed_seconds, minimum=.000001)
    number(client_setup_seconds)
    planned = int(rate * duration)
    if rate > 10000 or duration > 600 or not 1 <= planned <= 1_000_000 or concurrency > 512 or concurrency % 8:
        raise ValueError("Global load exceeds the reviewed bounds")
    if abs((utc(window_end_utc) - utc(window_start_utc)).total_seconds() - duration) > .000001:
        raise ValueError("Global UTC window differs from its duration")
    if planned_window_start_utc is None:
        if measurement_start_drift_seconds is not None:
            raise ValueError("Unexpected planned-window drift")
    else:
        drift = (utc(window_start_utc) - utc(planned_window_start_utc)).total_seconds()
        if type(measurement_start_drift_seconds) not in (int, float) or not math.isfinite(measurement_start_drift_seconds):
            raise ValueError("Invalid planned-window drift")
        if abs(drift - measurement_start_drift_seconds) > .000001 or abs(drift) > 1:
            raise ValueError("Planned start was missed or inconsistent")
    if not isinstance(children, list) or len(children) != 8:
        raise ValueError("Require all eight child receipts")
    indices = [child["process_index"] for child in children]
    if any(type(index) is not int for index in indices) or set(indices) != set(range(8)):
        raise ValueError("Missing or duplicate child identity")

    events = ("started", "completed", "successful", "errors")
    buckets = [dict(offset_seconds=offset, duration_seconds=min(1, duration - offset),
                    started=0, completed=0, successful=0, errors=0) for offset in range(math.ceil(duration))]
    totals, statuses, errors, backend_rows = Counter(), Counter(), Counter(), Counter()
    categories = {"measured": Counter(), "cleanup": Counter()}
    samples = {name: [] for name in ("latency_ms", "scheduled_latency_ms", "client_lag_ms")}
    gateways = None
    groups = None
    peaks, child_metadata = [], []
    worker_cpu, unconsumed = 0, 0
    for child in sorted(children, key=lambda row: row["process_index"]):
        index, result = child["process_index"], child["result"]
        number(child["schedule_offset_seconds"])
        number(child["schedule_stride_seconds"], minimum=.000001)
        if (child["process_count"] != 8 or type(child["process_count"]) is not int or
                child["schedule_offset_seconds"] != index / rate or child["schedule_stride_seconds"] != 8 / rate):
            raise ValueError("Child schedule differs from its global ordinal partition")
        local_planned = len(range(index, planned, 8))
        for key in ("target_http_rps", "window_seconds"):
            number(result[key], minimum=.000001)
        for key in ("scheduled_http_requests", "client_concurrency", "final_inflight"):
            number(result[key], integer=True)
        if (result["target_http_rps"] != rate / 8 or result["scheduled_http_requests"] != local_planned or
                result["client_concurrency"] != concurrency // 8 or result["window_seconds"] != duration or
                result["window_start_utc"] != window_start_utc or result["window_end_utc"] != window_end_utc or
                result["final_inflight"] != 0):
            raise ValueError("Child profile, window or completion differs")
        count = counters(result["counts"])
        started, completed = count["started"], count["completed"]
        drops = count["client_late_drops"] + count["client_capacity_drops"]
        if (started + drops != local_planned or completed != started or
                count["started_in_window"] > started or count["completed_in_window"] > completed or
                count["successful_in_window"] > count["completed_in_window"] or
                count["cleanup_started"] != count["cleanup_completed"]):
            raise ValueError("Child request accounting does not balance")
        if not isinstance(child["raw_samples"], dict) or set(child["raw_samples"]) != set(samples):
            raise ValueError("Missing raw percentile samples")
        for name in samples:
            values = child["raw_samples"][name]
            if not isinstance(values, list) or len(values) != started:
                raise ValueError("Raw sample count differs from started requests")
            samples[name].extend(number(value) for value in values)
        child_buckets = result["second_buckets"]
        if not isinstance(child_buckets, list) or len(child_buckets) != len(buckets):
            raise ValueError("Child buckets do not cover the common window")
        bucket_totals = Counter()
        for target, source in zip(buckets, child_buckets):
            number(source["offset_seconds"], integer=True)
            number(source["duration_seconds"], minimum=.000001)
            if set(source) != set(target) or any(source[key] != target[key] for key in ("offset_seconds", "duration_seconds")):
                raise ValueError("Child buckets have different boundaries")
            if source["completed"] != source["successful"] + source["errors"]:
                raise ValueError("Child bucket outcomes do not balance")
            for event in events:
                value = number(source[event], integer=True)
                target[event] += value
                bucket_totals[event] += value
        for event, key in (("started", "started_in_window"), ("completed", "completed_in_window"),
                           ("successful", "successful_in_window")):
            if bucket_totals[event] != count[key]:
                raise ValueError("Window bucket totals differ from request counts")
        gateway_count = number(result["gateway_endpoints"], integer=True, minimum=1)
        group_count = number(result["routing_groups"], integer=True, minimum=1)
        if not isinstance(result["per_gateway_counts"], list) or len(result["per_gateway_counts"]) != gateway_count:
            raise ValueError("Child routing inventory does not match its counters")
        if gateways is None:
            gateways, groups = [Counter() for _ in range(gateway_count)], group_count
        if gateway_count != len(gateways) or group_count != groups or len(result["per_gateway_counts"]) != gateway_count:
            raise ValueError("Child routing inventories differ")
        per_gateway = [counters(row) for row in result["per_gateway_counts"]]
        if (count["sql_completed"] > count["sql_submissions"] or
                count["sql_submissions"] != sum(row["measured_POST"] for row in per_gateway) or
                any(row["cleanup_POST"] for row in per_gateway)):
            raise ValueError("SQL submissions differ from POST requests or completions")
        for phase, expected in (("measured", started), ("cleanup", count["cleanup_completed"])):
            methods = sum(row[phase + "_POST"] + row[phase + "_GET"] for row in per_gateway)
            outcomes = sum(value for row in per_gateway for key, value in row.items() if key.startswith(phase + "_status_"))
            if methods != expected or outcomes != expected:
                raise ValueError("Per-Gateway HTTP counters do not balance")
        child_statuses, child_errors = counters(result["http_statuses"]), counters(result["errors"])
        observed_statuses = Counter()
        for row in per_gateway:
            for key, value in row.items():
                for prefix in ("measured_status_", "cleanup_status_"):
                    if key.startswith(prefix):
                        observed_statuses[key[len(prefix):]] += value
        missing_status = observed_statuses.pop("None", 0)
        if child_statuses != observed_statuses:
            raise ValueError("Per-Gateway and global status counters differ")
        failed_http = missing_status + sum(value for status, value in observed_statuses.items() if status != "200")
        if not failed_http <= sum(child_errors.values()) <= completed + count["cleanup_completed"]:
            raise ValueError("HTTP errors do not cover failed or missing statuses")
        if bucket_totals["errors"] > sum(child_errors.values()):
            raise ValueError("Window errors exceed total errors")
        if set(result["response_error_categories"]) != set(categories):
            raise ValueError("Missing response error phases")
        for phase in categories:
            phase_categories = counters(result["response_error_categories"][phase])
            if sum(phase_categories.values()) > sum(row[phase + "_status_503"] for row in per_gateway):
                raise ValueError("Response categories exceed HTTP 503 responses")
            categories[phase].update(phase_categories)
        for target, source in zip(gateways, per_gateway):
            target.update(source)
        totals.update(count)
        statuses.update(child_statuses)
        errors.update(child_errors)
        backend_rows.update(counters(result["backend_result_rows"]))
        unconsumed += number(result["unconsumed_continuations"], integer=True)
        peak = number(result["peak_client_inflight"], integer=True)
        if peak > concurrency // 8 or peak > started:
            raise ValueError("Child peak exceeds its request or concurrency bound")
        peaks.append(peak)
        elapsed = number(result["completion_elapsed_seconds"])
        if elapsed > completion_elapsed_seconds + .000001:
            raise ValueError("Child completion exceeds the parent completion span")
        cpu = number(result["client_cpu_seconds"])
        worker_cpu += cpu
        child_metadata.append(dict(process_index=index, scheduled_http_requests=local_planned,
                                   raw_sample_count=started, completion_elapsed_seconds=elapsed,
                                   worker_cpu_seconds=cpu, peak_client_inflight=peak))

    request_rates = {event: dict(min=min(row[event] / row["duration_seconds"] for row in buckets),
                                max=max(row[event] / row["duration_seconds"] for row in buckets),
                                avg=sum(row[event] for row in buckets) / duration) for event in events}
    dropped = totals["client_late_drops"] + totals["client_capacity_drops"]
    distributions = {name: distribution(values) for name, values in samples.items()}
    return dict(scheduled_http_requests=planned, target_http_rps=rate, client_setup_seconds=client_setup_seconds,
                client_setup_method="eight spawned processes and all executor workers ready before the common window; no backend traffic",
                client_cgroup_cpu=deepcopy(cgroup_cpu), window_start_utc=window_start_utc, window_end_utc=window_end_utc,
                planned_window_start_utc=planned_window_start_utc, measurement_start_drift_seconds=measurement_start_drift_seconds,
                second_buckets=buckets, request_rates=request_rates, window_seconds=duration,
                completion_elapsed_seconds=completion_elapsed_seconds,
                offered_http_rps=totals["started_in_window"] / duration,
                achieved_http_rps=totals["completed_in_window"] / duration,
                successful_http_rps=totals["successful_in_window"] / duration,
                counts=dict(totals), http_statuses=dict(statuses), errors=dict(errors),
                response_error_categories={phase: dict(value) for phase, value in categories.items()},
                **distributions, client_dropped_requests=dropped,
                client_bottleneck_detected=dropped > 0 or (distributions["client_lag_ms"]["p99"] or 0) > 5,
                client_concurrency=concurrency, client_processes=8, peak_client_inflight=None,
                peak_client_inflight_bounds=dict(min=max(peaks), max=sum(peaks)),
                peak_client_inflight_interpretation="Bounds from child peaks, not an observed global peak",
                gateway_endpoints=len(gateways), routing_groups=groups, per_gateway_counts=[dict(row) for row in gateways],
                client_cpu_seconds=worker_cpu, client_average_cpu_cores=worker_cpu / completion_elapsed_seconds,
                client_cpu_interpretation="Sum of worker process CPU through their late completions, excluding cleanup and parent CPU; denominator is the common parent completion span",
                backend_result_rows=dict(backend_rows), unconsumed_continuations=unconsumed,
                child_measurements=child_metadata,
                percentile_interpretation="Exact percentiles from pooled raw numeric samples in memory; raw samples are not retained for later independent recomputation",
                sql_note="SQL submissions and completions are separate from HTTP requests; cleanup is outside the measured window")
