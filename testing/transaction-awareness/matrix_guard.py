"""Compare approved server-side ledger fingerprints without raw identifiers."""

import hashlib
import math
import re
from datetime import datetime, timedelta

class PreservationError(ValueError):
    pass

EMPTY = hashlib.sha256(b"").hexdigest()
ALGORITHM = "ascii-field-length-prefix-colon-concatenation-c-id-order-sha256-v1"


def utc_timestamp(value):
    try:
        if not isinstance(value, str) or "T" not in value:
            raise ValueError("Missing ISO timestamp")
        parsed = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
        if parsed.utcoffset() != timedelta(0):
            raise ValueError("Timestamp must explicitly identify UTC")
        return parsed
    except (TypeError, ValueError) as error:
        raise PreservationError("Evidence timestamp must be explicit ISO UTC") from error


def ascii_fingerprint(rows, fields):
    ordered = sorted(rows, key=lambda row: row[fields[0]].encode("ascii"))
    encoded = bytearray()
    for row in ordered:
        for field in fields:
            value = row[field].encode("ascii")
            encoded.extend(str(len(value)).encode("ascii") + b":" + value)
    return {"count": len(rows), "sha256": hashlib.sha256(encoded).hexdigest()}


def digest(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise PreservationError("Invalid digest evidence")
    return value


def counted(value):
    if not isinstance(value, dict) or type(value.get("count")) is not int or value["count"] < 0:
        raise PreservationError("Invalid count evidence")
    result = (value["count"], digest(value.get("sha256")))
    if (result[0] == 0) != (result[1] == EMPTY):
        raise PreservationError("Count and empty digest disagree")
    return result


def canonical(sample):
    if sample.get("ascii_valid") is not True:
        raise PreservationError("Length-prefix encoding requires verified ASCII fields")
    if counted(sample.get("pending_admissions")) != (0, EMPTY) or counted(sample.get("open_transactions")) != (0, EMPTY):
        raise PreservationError("Pending admissions or open transactions remain")
    retained = sample.get("retained_terminal_counts")
    if not isinstance(retained, list) or any(not isinstance(row, dict) or type(row.get("count")) is not int or row["count"] != 0 for row in retained):
        raise PreservationError("Terminal retention has not expired")
    values = sample.get("backends")
    if not isinstance(values, list) or not values:
        raise PreservationError("Backend manifest is missing")
    backends = {}
    for row in values:
        if not isinstance(row, dict) or not re.fullmatch(r"[a-z0-9-]+", row.get("name", "")):
            raise PreservationError("Invalid backend manifest name")
        if row["name"] in backends or row.get("state") != "ACTIVE" or type(row.get("generation")) is not int or row["generation"] < 0:
            raise PreservationError("Backend manifest is duplicated or not ACTIVE")
        backends[row["name"]] = (digest(row.get("incarnation_hash")), row["generation"])
    if len({row[0] for row in backends.values()}) != len(backends):
        raise PreservationError("Backend incarnations are duplicated")
    queries = counted(sample.get("nonterminal_queries"))
    groups = sample.get("query_groups")
    if not isinstance(groups, list):
        raise PreservationError("Per-incarnation query evidence is missing")
    query_groups = {}
    for group in groups:
        identity = digest(group.get("incarnation_hash"))
        if identity in query_groups or identity not in {row[0] for row in backends.values()}:
            raise PreservationError("Query group identity is invalid")
        query_groups[identity] = counted(group)
        if not query_groups[identity][0]:
            raise PreservationError("Empty query group is unexpected")
    if sum(group[0] for group in query_groups.values()) != queries[0]:
        raise PreservationError("Per-incarnation query totals disagree")
    return {"backends": backends, "queries": queries, "groups": query_groups,
            "backend_set_sha256": digest(sample.get("backend_set_sha256"))}


def stable(samples):
    if len(samples) < 2:
        raise PreservationError("Require two stable aggregate samples")
    timestamps = [utc_timestamp(sample.get("sample_time")) for sample in samples]
    if any(second <= first for first, second in zip(timestamps, timestamps[1:])):
        raise PreservationError("Evidence timestamps must strictly increase")
    values = [canonical(sample) for sample in samples]
    if any(value != values[0] for value in values[1:]):
        raise PreservationError("Aggregate evidence is not quiescent")
    return values[-1]


def compare(before, after, historical_backend, expected_backend_names, *, allow_initialized=(), require_complete=True,
            case_completed_utc=None):
    baseline, actual = stable(before), stable(after)
    if utc_timestamp(after[0]["sample_time"]) <= utc_timestamp(before[-1]["sample_time"]):
        raise PreservationError("Post evidence must be newer than every pre sample")
    if case_completed_utc is not None and utc_timestamp(after[0]["sample_time"]) < utc_timestamp(case_completed_utc):
        raise PreservationError("Post evidence predates case completion")
    original, current = baseline["backends"], actual["backends"]
    expected, allowed = set(expected_backend_names), set(allow_initialized)
    if not set(original) <= expected or not set(current) <= expected or not allowed <= expected:
        raise PreservationError("Backend inventory is outside the approved fixtures")
    if historical_backend not in original or historical_backend not in current:
        raise PreservationError("Historical owner is missing")
    if require_complete and set(current) != expected:
        raise PreservationError("Establish every expected backend before matrix measurement")
    if set(original) - set(current) or set(current) - set(original) != allowed:
        raise PreservationError("Backend initialization was not explicitly approved")
    for name, (incarnation, generation) in original.items():
        if current[name][0] != incarnation or current[name][1] < generation:
            raise PreservationError("An existing backend was replaced or its generation decreased")
    if current[historical_backend] != original[historical_backend]:
        raise PreservationError("Historical owner's incarnation or lifecycle generation changed")
    owner = original[historical_backend][0]
    if set(baseline["groups"]) - {owner}:
        raise PreservationError("Baseline obligations exist outside the approved historical owner")
    if baseline["groups"] and baseline["groups"][owner] != baseline["queries"]:
        raise PreservationError("Historical query fingerprint disagrees with its global fingerprint")
    if baseline["queries"] != actual["queries"] or baseline["groups"] != actual["groups"]:
        raise PreservationError("Historical obligations changed or new query obligations remain")
    if not allowed and baseline["backend_set_sha256"] != actual["backend_set_sha256"]:
        raise PreservationError("Backend set fingerprint changed")
    return {"passed": True, "algorithm": ALGORITHM, "preserved_nonterminal_count": baseline["queries"][0],
            "preserved_nonterminal_sha256": baseline["queries"][1], "pending_admissions": 0, "open_transactions": 0,
            "backend_count": len(current), "explicitly_initialized_backends": sorted(allowed)}


def pod_inventory(snapshot, replicas, expected_fixture_names, image_digest):
    utc_timestamp(snapshot.get("sample_time"))
    pods = snapshot.get("pods")
    fixtures = set(expected_fixture_names)
    if len(fixtures) != 4 or not isinstance(pods, list) or len(pods) != replicas + 4:
        raise PreservationError("Require every Gateway and all four fixture pods")
    result, gateway_count, fixture_names = {}, 0, set()
    for pod in pods:
        if not isinstance(pod, dict) or not isinstance(pod.get("name"), str) or not pod["name"] or pod["name"] in result:
            raise PreservationError("Invalid or duplicate pod name")
        if not isinstance(pod.get("uid"), str) or not pod["uid"] or pod.get("ready") is not True or pod.get("phase") != "Running" or pod.get("deletion_timestamp") is not None:
            raise PreservationError("Pod is not a stable Ready running process")
        if type(pod.get("restart_count")) is not int or pod["restart_count"] < 0:
            raise PreservationError("Pod restart evidence is invalid")
        if not isinstance(pod.get("spec_image"), str) or not re.fullmatch(r"\S+", pod["spec_image"]):
            raise PreservationError("Require the pod spec image")
        if not isinstance(pod.get("observed_image_id"), str) or not re.search(r"sha256:[0-9a-f]{64}$", pod["observed_image_id"]):
            raise PreservationError("Require the observed container image identity")
        if pod.get("role") == "gateway":
            gateway_count += 1
            if (not re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", pod["spec_image"])
                    or pod["spec_image"].rsplit("@", 1)[1] != image_digest):
                raise PreservationError("Gateway uses the wrong measured image")
        elif pod.get("role") == "fixture" and pod.get("fixture_name") in fixtures:
            if pod["fixture_name"] in fixture_names:
                raise PreservationError("Fixture identity has duplicate pods")
            fixture_names.add(pod["fixture_name"])
        else:
            raise PreservationError("Unexpected pod role or fixture")
        result[pod["name"]] = (pod["uid"], pod["role"], pod.get("fixture_name"), pod["spec_image"], pod["observed_image_id"], pod["restart_count"])
    if gateway_count != replicas or fixture_names != fixtures or len({row[0] for row in result.values()}) != len(result):
        raise PreservationError("Pod inventory is incomplete or contains duplicate identities")
    return result


def finalize(case_result, ledger_before, ledger_after, pods_before, pods_after, historical_backend,
             expected_backend_names, expected_fixture_names, *, maximum_capture_gap_seconds=120):
    if type(maximum_capture_gap_seconds) is not int or not 0 < maximum_capture_gap_seconds <= 120:
        raise PreservationError("Capture freshness must be bounded to at most 120 seconds")
    stable(ledger_before)
    stable(ledger_after)
    start, end = utc_timestamp(case_result.get("started_utc")), utc_timestamp(case_result.get("completed_utc"))
    if end <= start:
        raise PreservationError("Case timestamps are invalid")
    for snapshot in (ledger_before[-1], pods_before):
        age = (start - utc_timestamp(snapshot.get("sample_time"))).total_seconds()
        if not 0 <= age <= maximum_capture_gap_seconds:
            raise PreservationError("Pre evidence is missing, future-dated, or stale")
    for snapshot in (ledger_after[0], pods_after):
        age = (utc_timestamp(snapshot.get("sample_time")) - end).total_seconds()
        if not 0 <= age <= maximum_capture_gap_seconds:
            raise PreservationError("Post evidence is missing, premature, or stale")
    preserved = compare(ledger_before, ledger_after, historical_backend, expected_backend_names, case_completed_utc=case_result["completed_utc"])
    before = pod_inventory(pods_before, case_result["replicas"], expected_fixture_names, case_result["image_digest"])
    after = pod_inventory(pods_after, case_result["replicas"], expected_fixture_names, case_result["image_digest"])
    if before != after:
        raise PreservationError("Gateway or fixture pod UID, image, or restart count changed")
    acceptance, measurement = case_result.get("target_acceptance"), case_result.get("measurement")
    target = case_result.get("rate")
    if type(target) is not int or target not in (100, 1000) or not isinstance(acceptance, dict) or not isinstance(measurement, dict):
        raise PreservationError("Missing case rate or measurement acceptance evidence")
    actual = measurement.get("successful_http_rps")
    if type(actual) not in (int, float) or not math.isfinite(actual) or actual < 0:
        raise PreservationError("Invalid actual successful request rate")
    target_met = actual >= .99 * target
    if (measurement.get("target_http_rps") != target or acceptance.get("target_http_rps") != target
            or acceptance.get("successful_http_rps") != actual or acceptance.get("minimum_fraction") != .99
            or acceptance.get("minimum_successful_http_rps") != .99 * target
            or acceptance.get("target_achieved") is not target_met):
        raise PreservationError("Case, measurement, and acceptance evidence disagree")
    workload_passed = all(case_result.get(field) is True for field in ("workload_checks_passed", "transport_valid", "checkpoints_valid"))
    result = dict(case_result)
    result.update(valid_run=workload_passed and target_met, external_postguard_required=False,
                  external_postguard={"ledger": preserved, "unchanged_pod_count": len(before), "capture_gap_limit_seconds": maximum_capture_gap_seconds})
    return result
