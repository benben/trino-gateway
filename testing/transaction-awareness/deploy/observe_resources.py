"""Sample cgroup CPU and memory with an existing container Python interpreter."""

import json
import re

import observe_cpu


SCRIPT = """import json
from datetime import datetime, timezone
from pathlib import Path
import sys
import time

count = int(sys.argv[1])
if not 2 <= count <= 600:
    raise SystemExit("Invalid sample count")
deadline = time.monotonic() + count + 30
for index in range(count):
    if time.monotonic() > deadline:
        raise SystemExit("Sampling deadline exceeded")
    sample = {"read_start_utc": datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")}
    for name in ("cpu.max", "cpu.stat", "memory.current", "memory.max"):
        sample[name] = Path("/sys/fs/cgroup", name).read_text().strip()
    sample["read_end_utc"] = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    print(json.dumps(sample), flush=True)
    if index + 1 < count:
        time.sleep(1)
"""


def sampling_command(samples=180):
    if type(samples) is not int or not 2 <= samples <= 600:
        raise ValueError("Use two to six hundred samples")
    return ["python3", "-u", "-c", SCRIPT, str(samples)]


def sampling_timeout(samples=180):
    sampling_command(samples)
    return samples + 60


def parse_samples(raw, expected_samples):
    sampling_command(expected_samples)
    records = [json.loads(line) for line in raw.splitlines() if line.strip()]
    blocks = []
    memories = []
    for record in records:
        fields = ("read_start_utc", "read_end_utc", "cpu.max", "cpu.stat", "memory.current", "memory.max")
        if not isinstance(record, dict) or set(record) != set(fields) or any(not isinstance(record[key], str) for key in fields):
            raise ValueError("Malformed resource sample")
        if not re.fullmatch(r"\d+", record["memory.current"]) or not re.fullmatch(r"[1-9]\d*", record["memory.max"]):
            raise ValueError("Require nonnegative memory usage and a finite positive memory limit")
        memories.append((int(record["memory.current"]), int(record["memory.max"])))
        blocks.append("\n".join(record[key] for key in ("read_start_utc", "cpu.max", "cpu.stat", "read_end_utc")))
    rows = observe_cpu.parse_samples("\n\n".join(blocks), expected_samples)
    for row, (current, maximum) in zip(rows, memories):
        row.update(memory_current_bytes=current, memory_limit_bytes=maximum)
    return rows


def summarize(rows, start, end, *, identity_before, identity_after, expected_cpu, expected_memory_bytes):
    if type(expected_memory_bytes) is not int or expected_memory_bytes <= 0:
        raise ValueError("Require a finite positive expected memory limit")
    result = observe_cpu.summarize(rows, start, end, identity_before=identity_before,
                                  identity_after=identity_after, expected_cpu=expected_cpu)
    problems = list(result["problems"])
    for row in rows:
        if (type(row.get("memory_current_bytes")) is not int or row["memory_current_bytes"] < 0 or
                type(row.get("memory_limit_bytes")) is not int or row["memory_limit_bytes"] != expected_memory_bytes):
            problems.append("memory_limit_changed_or_unexpected_or_invalid_usage")
    beginning, ending = observe_cpu.instant(start), observe_cpu.instant(end)
    selected = [row for row in rows if beginning <= observe_cpu.instant(row["read_start_utc"]) < ending
                and observe_cpu.instant(row["read_end_utc"]) <= ending]
    if len(selected) < 2:
        problems.append("insufficient_in_window_memory_samples")
    result.update(valid=not problems, problems=sorted(set(problems)), memory=None)
    if not problems:
        values = [row["memory_current_bytes"] for row in selected]
        result["memory"] = {"samples": len(values), "min_bytes": min(values), "max_bytes": max(values),
                            "avg_bytes": sum(values) / len(values), "limit_bytes": expected_memory_bytes,
                            "sampled_change_bytes": values[-1] - values[0],
                            "sampled_max_fraction_of_limit": max(values) / expected_memory_bytes,
                            "first_sample_utc": selected[0]["read_start_utc"],
                            "last_sample_utc": selected[-1]["read_end_utc"],
                            "interpretation": "In-window sampled cgroup memory, including cache; not an exact peak, time-weighted mean, or window-boundary growth"}
    else:
        result["counter_delta_bounds"] = None
        result["average_cpu_cores_bounds"] = None
    return result
