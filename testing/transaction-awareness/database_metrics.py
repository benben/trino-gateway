#!/usr/bin/env python3
"""Capture private Aurora metrics and estimate explicitly priced database costs."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import math
import os
import re
from pathlib import Path
import subprocess


INSTANCE_METRICS = ("ServerlessDatabaseCapacity", "ACUUtilization", "DatabaseConnections",
                    "CPUUtilization", "FreeableMemory", "CommitThroughput",
                    "NetworkReceiveThroughput", "NetworkTransmitThroughput")
IO_METRICS = ("VolumeReadIOPs", "VolumeWriteIOPs")


def epoch(value):
    if isinstance(value, (float, int)):
        result = float(value)
    else:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("Timestamps must include a timezone")
        result = parsed.timestamp()
    if not math.isfinite(result):
        raise ValueError("Timestamp must be finite")
    return result


def iso(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def selected_points(points, start, end, period):
    start, end = epoch(start), epoch(end)
    if end <= start or period <= 0:
        raise ValueError("Use a positive measurement window and period")
    seen = set()
    selected = []
    for point in points:
        timestamp = epoch(point["Timestamp"])
        overlap = max(0, min(end, timestamp + period) - max(start, timestamp))
        if not overlap:
            continue
        if timestamp in seen or timestamp % period:
            raise ValueError("Metric buckets must be unique and period-aligned")
        seen.add(timestamp)
        selected.append((point, overlap))
    return start, end, selected


def coverage(start, end, points, period):
    observed = sum(overlap for _, overlap in points)
    partial = any(overlap != period for _, overlap in points)
    return {"window_seconds": end - start, "observed_seconds": observed,
            "missing_seconds": end - start - observed, "partial_buckets": partial,
            "complete_aligned_window": not partial and observed == end - start,
            "bucket_count": len(points), "period_seconds": period}


def gauge_summary(points, start, end, period=60, expected_samples=None):
    start, end, selected = selected_points(points, start, end, period)
    result = coverage(start, end, selected, period)
    integral, lows, highs = 0, [], []
    for point, overlap in selected:
        values = [float(point[key]) for key in ("Minimum", "Average", "Maximum")]
        low, average, high = values
        if not all(math.isfinite(value) for value in values) or not 0 <= low <= average <= high:
            raise ValueError("Invalid gauge statistics")
        lows.append(low)
        highs.append(high)
        integral += average * overlap
    samples = [float(point.get("SampleCount", 0)) for point, _ in selected]
    if any(not math.isfinite(value) or value < 0 for value in samples):
        raise ValueError("Invalid sample counts")
    sample_coverage = all(value == expected_samples for value in samples) if expected_samples is not None else None
    return result | {"min": min(lows) if lows else None, "max": max(highs) if highs else None,
                     "avg": integral / result["observed_seconds"] if selected else None,
                     "value_seconds": integral if selected else None,
                     "sample_count": sum(samples), "complete_sample_coverage": sample_coverage}


def io_summary(points, start, end, period=300):
    start, end, selected = selected_points(points, start, end, period)
    result = coverage(start, end, selected, period)
    values = [float(point["Sum"]) for point, _ in selected]
    if not all(math.isfinite(value) and value >= 0 for value in values):
        raise ValueError("Invalid I/O counts")
    count = sum(values) if result["complete_aligned_window"] else None
    return result | {"count": count, "avg_per_second": count / (end - start) if count is not None else None,
                     "overlapping_bucket_counts": sum(values) if selected else None}


def cost_summary(capacity, io, successes, attempts, acu_hour_usd, io_million_usd, idle_acu_average=None):
    if (type(successes) is not int or type(attempts) is not int or not 0 <= successes <= attempts
            or any(not math.isfinite(value) or value < 0 for value in (acu_hour_usd, io_million_usd))):
        raise ValueError("Invalid request counts or price inputs")
    if idle_acu_average is not None and (not math.isfinite(idle_acu_average) or idle_acu_average < 0):
        raise ValueError("Invalid idle capacity")
    usable = capacity["complete_aligned_window"] and capacity.get("complete_sample_coverage") is not False
    compute = capacity["value_seconds"] / 3600 * acu_hour_usd if usable else None
    io_cost = None
    if io and all(io[key]["complete_aligned_window"] for key in ("read", "write")):
        io_cost = (io["read"]["count"] + io["write"]["count"]) / 1_000_000 * io_million_usd
    total = compute + io_cost if compute is not None and io_cost is not None else None
    delta = (compute - idle_acu_average * capacity["window_seconds"] / 3600 * acu_hour_usd
             if compute is not None and idle_acu_average is not None else None)
    return {"observed_successful_requests": successes, "observed_attempts": attempts,
            "compute_usd": compute, "io_usd": io_cost, "compute_plus_io_usd": total,
            "compute_usd_per_successful_request": compute / successes if compute is not None and successes else None,
            "compute_usd_per_attempt": compute / attempts if compute is not None and attempts else None,
            "compute_plus_io_usd_per_successful_request": total / successes if total is not None and successes else None,
            "allocated_compute_delta_usd": delta,
            "incremental_allocated_compute_usd": max(0, delta) if delta is not None else None,
            "price_inputs": {"acu_hour_usd": acu_hour_usd, "io_million_usd": io_million_usd},
            "exclusions": ["storage", "backup", "transfer", "Gateway compute", "discounts", "tax"],
            "interpretation": "Allocated capacity cost, not per-statement CPU cost. Zero incremental allocation does not mean free database work."}


def write_private(path, result):
    path = Path(path).resolve()
    if Path(__file__).resolve().parents[2] in path.parents:
        raise ValueError("Keep raw resource identifiers and runtime receipts outside the repository")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as output:
        json.dump(result, output, indent=2, allow_nan=False)
        output.write("\n")


def capture(args):
    start, end = epoch(args.start), epoch(args.end)
    if not 0 < end - start <= 86400 or not 1 <= args.replicas <= 100:
        raise ValueError("Use a bounded one-day window and one to one hundred Gateways")
    base = ["aws", "--profile", args.profile, "--region", args.region, "--output", "json"]

    def aws(arguments):
        result = subprocess.run(base + arguments, capture_output=True, text=True, timeout=90)
        if result.returncode:
            raise RuntimeError("Read-only AWS metric request failed; inspect access without printing runtime configuration")
        return json.loads(result.stdout)

    cluster = aws(["rds", "describe-db-clusters", "--db-cluster-identifier", args.cluster])["DBClusters"][0]
    members = {member["DBInstanceIdentifier"] for member in cluster["DBClusterMembers"]}
    if members != {args.instance}:
        raise ValueError("This collector requires exactly the selected single writer; multi-instance cost needs summed per-instance capacity")
    instance = aws(["rds", "describe-db-instances", "--db-instance-identifier", args.instance])["DBInstances"][0]
    if (not cluster["DBClusterMembers"][0]["IsClusterWriter"] or instance["DBInstanceClass"] != "db.serverless"
            or instance["DBClusterIdentifier"] != args.cluster or cluster["Engine"] != "aurora-postgresql"):
        raise ValueError("Expected one Aurora PostgreSQL Serverless writer")
    if not re.fullmatch(r"[a-zA-Z0-9_.-]+", args.case_id) or not re.fullmatch(r"[^\s]+@sha256:[a-f0-9]{64}", args.gateway_image):
        raise ValueError("Provide a case identifier and immutable Gateway image")
    metadata = {key: cluster.get(key) for key in ("DBClusterIdentifier", "Engine", "EngineVersion", "StorageType", "ServerlessV2ScalingConfiguration")}

    def metric(name):
        is_io = name in IO_METRICS
        period = 300 if is_io else 60
        dimension = "DBClusterIdentifier" if is_io else "DBInstanceIdentifier"
        identity = args.cluster if is_io else args.instance
        result = aws(["cloudwatch", "get-metric-statistics", "--namespace", "AWS/RDS", "--metric-name", name,
                      "--dimensions", "Name=" + dimension + ",Value=" + identity,
                      "--start-time", iso(math.floor(start / period) * period),
                      "--end-time", iso(math.ceil(end / period) * period), "--period", str(period),
                      "--statistics"] + (["Sum", "SampleCount"] if is_io else ["Minimum", "Maximum", "Average", "SampleCount"]))
        summary = (io_summary(result["Datapoints"], start, end, period) if is_io else
                   gauge_summary(result["Datapoints"], start, end, period, expected_samples=60 if name == "ServerlessDatabaseCapacity" else None))
        return name, {"summary": summary, "raw": result}

    with ThreadPoolExecutor(max_workers=4) as executor:
        metrics = dict(executor.map(metric, INSTANCE_METRICS + IO_METRICS))
    write_private(args.output, {"start_utc": iso(start), "end_utc": iso(end), "replicas": args.replicas,
                                "region": args.region, "cluster_configuration": metadata, "metrics": metrics,
                                "writer_identity": {"instance_identifier": args.instance, "resource_id": instance["DbiResourceId"]},
                                "case_id": args.case_id, "gateway_image": args.gateway_image,
                                "note": "Missing samples remain missing; retrieve again after publication delay. Connection extrema are CloudWatch sampled extrema, not instantaneous peaks."})


def report(args):
    data = json.loads(Path(args.capture).read_text())
    load = json.loads(Path(args.load).read_text())
    start, end = epoch(data["start_utc"]), epoch(data["end_utc"])
    drift = max(abs(epoch(load["window_start_utc"]) - start), abs(epoch(load["window_end_utc"]) - end))
    if drift > 1:
        raise ValueError("Database and actual load windows must align within one second for cost attribution")
    idle = None
    if args.idle:
        baseline = json.loads(Path(args.idle).read_text())
        if any(baseline[key] != data[key] for key in ("replicas", "region", "cluster_configuration", "gateway_image", "writer_identity")):
            raise ValueError("Idle baseline must use the same database configuration and replica count")
        idle_summary = baseline["metrics"]["ServerlessDatabaseCapacity"]["summary"]
        if not idle_summary["complete_aligned_window"] or idle_summary.get("complete_sample_coverage") is False:
            raise ValueError("Idle baseline capacity coverage is incomplete")
        idle = idle_summary["avg"]
    metrics = data["metrics"]
    costs = cost_summary(metrics["ServerlessDatabaseCapacity"]["summary"],
                         {"read": metrics["VolumeReadIOPs"]["summary"], "write": metrics["VolumeWriteIOPs"]["summary"]},
                         load["counts"].get("successful_in_window", 0), load["counts"].get("started_in_window", 0),
                         args.acu_hour_usd, args.io_million_usd, idle)
    write_private(args.output, {"database": data, "load": load, "cost_estimate": costs,
                                "valid_throughput_run": load.get("valid_run") is True,
                                "maximum_window_alignment_error_seconds": drift,
                                "warning": "Capacity and I/O are shared-window estimates. Successful-request costs include rejected-request work and background overhead. Tail costs require separate capture."})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    collect = commands.add_parser("capture")
    for name in ("profile", "region", "cluster", "instance", "start", "end", "output", "case-id", "gateway-image"):
        collect.add_argument("--" + name, required=True)
    collect.add_argument("--replicas", type=int, required=True)
    summarize = commands.add_parser("report")
    for name in ("capture", "load", "output"):
        summarize.add_argument("--" + name, required=True)
    summarize.add_argument("--idle")
    summarize.add_argument("--acu-hour-usd", type=float, required=True)
    summarize.add_argument("--io-million-usd", type=float, required=True)
    args = parser.parse_args()
    (capture if args.command == "capture" else report)(args)


if __name__ == "__main__":
    main()
