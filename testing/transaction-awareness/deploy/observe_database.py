#!/usr/bin/env python3
"""Render and summarize a bounded, read-only PostgreSQL connection observer."""

import argparse
from datetime import datetime
import json
from pathlib import Path
import re

from render import TASK, validate_priority_class


SQL = """SELECT json_build_object(
  'sample_time', to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
  'observer_pid', pg_backend_pid(),
  'client_connections', (SELECT count(*) FROM clients),
  'states', (SELECT coalesce(json_object_agg(label, n), '{}'::json)
             FROM (SELECT coalesce(state, 'unknown') AS label, count(*) AS n FROM clients GROUP BY 1) counts),
  'wait_types', (SELECT coalesce(json_object_agg(label, n), '{}'::json)
                 FROM (SELECT coalesce(wait_event_type, 'none') AS label, count(*) AS n FROM clients GROUP BY 1) counts),
  'database', (SELECT json_build_object('xact_commit', xact_commit, 'xact_rollback', xact_rollback,
      'blks_read', blks_read, 'blks_hit', blks_hit, 'temp_bytes', temp_bytes,
      'deadlocks', deadlocks, 'sessions', sessions, 'stats_reset', stats_reset)
    FROM pg_stat_database WHERE datname = current_database()))
"""
QUERY = """WITH clients AS MATERIALIZED (
  SELECT state, wait_event_type FROM pg_stat_activity
  WHERE datname = current_database() AND backend_type = 'client backend'
    AND application_name <> 'gateway-metrics-observer' AND pid <> pg_backend_pid()
)
""" + SQL
COUNTERS = ("xact_commit", "xact_rollback", "blks_read", "blks_hit", "temp_bytes", "deadlocks", "sessions")


def render_job(namespace, name, database_secret, ca_configmap, *, samples=300, priority_class=None):
    priority_name = validate_priority_class(priority_class)
    dns_label = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    if (not re.fullmatch(r"gateway-tx-lab-[a-z0-9-]+", namespace)
            or not re.fullmatch(r"metrics-[a-z0-9-]+", name)
            or not all(re.fullmatch(dns_label, value) for value in (namespace, name, database_secret, ca_configmap))):
        raise ValueError("Use a disposable lab namespace, metrics-* Job, and explicit local resource names")
    if type(samples) is not int or not 2 <= samples <= 900:
        raise ValueError("Collect between 2 and 900 one-second samples")
    labels = {"task": TASK, "app": "gateway-metrics-observer"}
    metadata = {"name": name, "namespace": namespace, "labels": labels}
    script = "\\set ON_ERROR_STOP on\n" + QUERY + f"\\watch i=1 c={samples}\n"
    config = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": metadata, "data": {"observe.sql": script}}
    env = {"PGAPPNAME": "gateway-metrics-observer", "PGCONNECT_TIMEOUT": "5", "PGSSLMODE": "verify-full",
           "PGSSLROOTCERT": "/etc/database-ca/ca.pem",
           "PGOPTIONS": "-c default_transaction_read_only=on -c statement_timeout=3000 -c lock_timeout=250"}
    container = {"name": "observer", "image": "postgres:17-alpine",
                 "command": ["sh", "-ec", "if ! psql -XqAtw -f /source/observe.sql 2>/tmp/observer-error; then printf 'Observer failed; no complete receipt\\n' >&2; exit 1; fi"],
                 "envFrom": [{"secretRef": {"name": database_secret}}],
                 "env": [{"name": key, "value": value} for key, value in env.items()],
                 "resources": {kind: {"cpu": "100m", "memory": "128Mi"} for kind in ("requests", "limits")},
                 "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                     "capabilities": {"drop": ["ALL"]}},
                 "volumeMounts": [{"name": "source", "mountPath": "/source", "readOnly": True},
                                  {"name": "ca", "mountPath": "/etc/database-ca", "readOnly": True},
                                  {"name": "tmp", "mountPath": "/tmp"}]}
    pod = {"restartPolicy": "Never", "automountServiceAccountToken": False, "priorityClassName": priority_name,
           "preemptionPolicy": "Never", "securityContext": {"runAsNonRoot": True, "runAsUser": 1000,
               "runAsGroup": 1000, "seccompProfile": {"type": "RuntimeDefault"}},
           "containers": [container], "volumes": [{"name": "source", "configMap": {"name": name}},
               {"name": "ca", "configMap": {"name": ca_configmap, "items": [{"key": "ca.pem", "path": "ca.pem"}]}},
               {"name": "tmp", "emptyDir": {"sizeLimit": "16Mi"}}]}
    job = {"apiVersion": "batch/v1", "kind": "Job", "metadata": metadata,
           "spec": {"backoffLimit": 0, "activeDeadlineSeconds": samples + 60, "ttlSecondsAfterFinished": 3600,
                    "template": {"metadata": {"labels": labels}, "spec": pod}}}
    return {"apiVersion": "v1", "kind": "List", "items": [config, job]}


def instant(value):
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError("Use ISO 8601 UTC timestamps ending in Z")
    return datetime.fromisoformat(value[:-1] + "+00:00").timestamp()


def summarize(rows, *, expected_samples=None, start=None, end=None):
    if (start is None) != (end is None):
        raise ValueError("Provide both measurement-window boundaries")
    if start is not None:
        beginning, ending = instant(start), instant(end)
        if beginning >= ending:
            raise ValueError("The measurement window must have positive duration")
        rows = [row for row in rows if beginning <= instant(row["sample_time"]) < ending]
    if not rows:
        raise ValueError("No database samples in the measurement window")
    times = [instant(row["sample_time"]) for row in rows]
    if any(right <= left for left, right in zip(times, times[1:])):
        raise ValueError("Database samples must be strictly ordered")
    for row in rows:
        counts = [row["client_connections"], *row["states"].values(), *row["wait_types"].values()]
        if any(type(value) is not int or value < 0 for value in counts):
            raise ValueError("Connection samples must contain nonnegative integer counts")
        if any(sum(row[field].values()) != row["client_connections"] for field in ("states", "wait_types")):
            raise ValueError("State and wait counts must cover all sampled clients")
    problems = []
    if expected_samples is not None and len(rows) != expected_samples:
        problems.append("sample_count_mismatch")
    if len({row["observer_pid"] for row in rows}) != 1:
        problems.append("observer_reconnected")
    if any(right - left > 2 for left, right in zip(times, times[1:])):
        problems.append("sampling_gap_over_two_seconds")
    if start is not None and (times[0] - beginning > 2 or ending - times[-1] > 2):
        problems.append("incomplete_window_coverage")
    if len({row["database"]["stats_reset"] for row in rows}) != 1:
        problems.append("database_statistics_reset")
    delta = {}
    for field in COUNTERS:
        values = [row["database"][field] for row in rows]
        if any(type(value) is not int or value < 0 for value in values):
            raise ValueError("Database counters must be nonnegative integers")
        if any(right < left for left, right in zip(values, values[1:])):
            problems.append("database_counter_decreased")
        delta[field] = values[-1] - values[0]
    def stats(values):
        return {"min": min(values), "max": max(values), "avg": sum(values) / len(values)}
    return {"valid": not problems, "problems": sorted(set(problems)), "samples": len(rows),
            "first_sample_utc": rows[0]["sample_time"], "last_sample_utc": rows[-1]["sample_time"],
            "sample_span_seconds": times[-1] - times[0],
            "client_connections": stats([row["client_connections"] for row in rows]),
            "states": {key: stats([row["states"].get(key, 0) for row in rows]) for key in sorted(set().union(*(row["states"] for row in rows)))},
            "wait_types": {key: stats([row["wait_types"].get(key, 0) for row in rows]) for key in sorted(set().union(*(row["wait_types"] for row in rows)))},
            "database_delta": delta if not problems else None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    render = commands.add_parser("render")
    for option in ("namespace", "name", "database-secret", "ca-configmap", "priority-class-file"):
        render.add_argument("--" + option, required=True)
    render.add_argument("--samples", type=int, default=300)
    summary = commands.add_parser("summarize")
    summary.add_argument("receipt", type=Path)
    summary.add_argument("--expected-samples", type=int)
    summary.add_argument("--start")
    summary.add_argument("--end")
    args = parser.parse_args()
    if args.command == "render":
        result = render_job(args.namespace, args.name, args.database_secret, args.ca_configmap, samples=args.samples,
                            priority_class=json.loads(Path(args.priority_class_file).read_text()))
    else:
        rows = [json.loads(line) for line in args.receipt.read_text().splitlines() if line.strip()]
        result = summarize(rows, expected_samples=args.expected_samples, start=args.start, end=args.end)
    print(json.dumps(result, indent=2))
    if args.command == "summarize" and not result["valid"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
