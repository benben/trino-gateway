"""Validate query ownership and summarize compact mid-load cutover cohorts."""

from collections import Counter

from load_aggregate import counters, number
from load_open_loop import distribution


def summarize_cutover(children, *, duration, cutover_start_seconds, cutover_ack_seconds):
    """Keep response ownership separate from request-start latency cohorts."""
    try:
        return _summarize(children, duration, cutover_start_seconds, cutover_ack_seconds)
    except (KeyError, TypeError, IndexError, OverflowError) as error:
        raise ValueError("Incomplete or malformed cutover evidence") from error


def _summarize(children, duration, cutover_start, cutover_ack):
    number(duration, minimum=.000001)
    number(cutover_start)
    number(cutover_ack)
    if duration > 600 or not 0 < cutover_start <= cutover_ack < duration:
        raise ValueError("Cutover must finish inside the common window")
    if not isinstance(children, list) or len(children) != 8:
        raise ValueError("Require all eight cutover child receipts")
    indices = [child["process_index"] for child in children]
    if any(type(index) is not int for index in indices) or set(indices) != set(range(8)):
        raise ValueError("Missing or duplicate cutover child identity")
    bounds = {"before": (0, cutover_start), "during": (cutover_start, cutover_ack), "after": (cutover_ack, duration)}
    phases = {}
    arrays = {}
    for name, (start, end) in bounds.items():
        phases[name] = dict(start_seconds=start, end_seconds=end, requests=0, errors=0, successful=0,
                            started_in_window=0, completed_in_window=0, late_completions=0, late_starts=0,
                            methods=Counter(), http_statuses=Counter(), observed_owners=Counter())
        arrays[name] = {key: [] for key in ("latency_ms", "scheduled_latency_ms", "client_lag_ms")}
    checks = dict(pre_cutover_post_responses=0, post_ack_post_starts=0, overlapping_post_requests=0,
                  pinned_continuations=0)
    total = 0
    for child in children:
        result, records = child["result"], child["cutover_samples"]
        count = counters(result["counts"])
        if not isinstance(records, list) or len(records) != count["started"] or count["completed"] != count["started"]:
            raise ValueError("Cutover samples differ from completed request accounting")
        total += len(records)
        if total > 1_000_000:
            raise ValueError("Cutover sample count exceeds the global bound")
        actual = Counter()
        actual_errors = 0
        in_window = Counter()
        for record in records:
            if not isinstance(record, list) or len(record) != 9:
                raise ValueError("Require exactly nine numeric cutover fields")
            method, worker_start, transport_start, response_end, scheduled, status, error, owner, expected = record
            for value, allowed in ((method, (0, 1)), (error, (0, 1)), (owner, (-1, 0, 1)), (expected, (-1, 0, 1))):
                if type(value) is not int or value not in allowed:
                    raise ValueError("Invalid cutover method, error or owner code")
            if type(status) is not int or not (status == 0 or 100 <= status <= 599):
                raise ValueError("Invalid cutover HTTP status")
            for value in (worker_start, transport_start, response_end, scheduled):
                number(value)
            if not scheduled < duration or not scheduled <= worker_start <= transport_start <= response_end:
                raise ValueError("Cutover timestamps are unordered or outside their schedule")
            if (status != 200 or owner == -1) and error != 1:
                raise ValueError("Unknown owner or failed HTTP status requires an error")
            if method == 0:
                if expected != -1:
                    raise ValueError("Initial POST cannot declare a retained continuation owner")
                if owner != -1:
                    if response_end < cutover_start:
                        if owner != 0:
                            raise ValueError("POST completed before cutover on the wrong owner")
                        checks["pre_cutover_post_responses"] += 1
                    elif transport_start > cutover_ack:
                        if owner != 1:
                            raise ValueError("POST started after acknowledgement on the old owner")
                        checks["post_ack_post_starts"] += 1
                    else:
                        checks["overlapping_post_requests"] += 1
            else:
                if expected not in (0, 1) or (owner != -1 and owner != expected):
                    raise ValueError("Continuation changed its retained query owner")
                if owner != -1:
                    checks["pinned_continuations"] += 1
            actual["measured_" + ("POST" if method == 0 else "GET")] += 1
            actual["measured_status_" + (str(status) if status else "None")] += 1
            actual_errors += error
            in_window["started_in_window"] += int(worker_start <= duration)
            in_window["completed_in_window"] += int(response_end <= duration)
            in_window["successful_in_window"] += int(response_end <= duration and not error)
            phase = "before" if worker_start < cutover_start else "during" if worker_start < cutover_ack else "after"
            entry = phases[phase]
            entry["requests"] += 1
            entry["errors"] += error
            entry["successful"] += 1 - error
            entry["started_in_window"] += int(worker_start <= duration)
            entry["completed_in_window"] += int(response_end <= duration)
            entry["late_completions"] += int(response_end > duration)
            entry["late_starts"] += int(worker_start > duration)
            entry["methods"]["POST" if method == 0 else "GET"] += 1
            entry["http_statuses"][str(status)] += 1
            entry["observed_owners"][{-1: "unknown", 0: "source", 1: "target"}[owner]] += 1
            arrays[phase]["latency_ms"].append((response_end - worker_start) * 1000)
            arrays[phase]["scheduled_latency_ms"].append((response_end - scheduled) * 1000)
            arrays[phase]["client_lag_ms"].append((worker_start - scheduled) * 1000)
        if any(count[key] != in_window[key] for key in ("started_in_window", "completed_in_window", "successful_in_window")):
            raise ValueError("Cutover timestamps disagree with window counters")
        expected_counts = Counter()
        for gateway in result["per_gateway_counts"]:
            expected_counts.update({key: value for key, value in counters(gateway).items() if key.startswith("measured_")})
        if expected_counts != actual:
            raise ValueError("Cutover method or status samples disagree with Gateway counters")
        all_errors = sum(counters(result["errors"]).values())
        if not actual_errors <= all_errors <= actual_errors + count["cleanup_completed"]:
            raise ValueError("Cutover errors disagree with measured and cleanup outcomes")
    if not phases["before"]["started_in_window"] or not phases["after"]["started_in_window"]:
        raise ValueError("Background requests did not span the cutover event")
    for phase, entry in phases.items():
        for key in ("methods", "http_statuses", "observed_owners"):
            entry[key] = dict(entry[key])
        entry.update({key: distribution(values) for key, values in arrays[phase].items()})
    return dict(cutover_start_seconds=cutover_start, cutover_ack_seconds=cutover_ack,
                cutover_duration_seconds=cutover_ack - cutover_start,
                request_start_cohorts=phases, ownership_checks=checks,
                interpretation="Cohorts use worker start, not response completion or route admission. Nominal bounds describe the common window; late starts and completions remain counted separately. POST ownership uses transport start and initial response end. Exact pooled percentiles were computed in memory; raw numeric records are not retained for later independent recomputation. Cleanup and whole-window acceptance gates remain separate.")
