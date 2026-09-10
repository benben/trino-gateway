"""Read-only cgroup sampling with timestamps from the measured container."""

from datetime import datetime
import re


def sampling_command(samples=180):
    if type(samples) is not int or not 2 <= samples <= 600:
        raise ValueError("Use two to six hundred samples")
    return ["sh", "-c", "set -eu\nremaining=" + str(samples) + "\n" + """while [ "$remaining" -gt 0 ]; do
    date -u '+%Y-%m-%dT%H:%M:%S.%NZ'
    cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/cpu.stat
    date -u '+%Y-%m-%dT%H:%M:%S.%NZ'
    printf '\\n'
    remaining=$((remaining - 1))
    if [ "$remaining" -gt 0 ]; then sleep 1; fi
done
"""]


def instant(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6,9}Z", value):
        raise ValueError("Container timestamps require fractional UTC precision")
    return datetime.fromisoformat(value[:-1] + "+00:00").timestamp()


def parse_samples(raw, expected_samples):
    if type(expected_samples) is not int or not 2 <= expected_samples <= 600:
        raise ValueError("Invalid expected sample count")
    rows = []
    for block in raw.strip().split("\n\n"):
        lines = block.splitlines()
        if len(lines) < 5 or instant(lines[0]) > instant(lines[-1]):
            raise ValueError("Incomplete or reversed CPU sample")
        if not re.fullmatch(r"[1-9]\d* [1-9]\d*", lines[1]):
            raise ValueError("Require a finite positive CPU quota and period")
        counters = {}
        for line in lines[2:-1]:
            match = re.fullmatch(r"([a-z_]+) (\d+)", line)
            if not match or match[1] in counters:
                raise ValueError("Malformed or duplicate CPU counter")
            counters[match[1]] = int(match[2])
        if not {"usage_usec", "nr_periods", "nr_throttled", "throttled_usec"} <= counters.keys():
            raise ValueError("Required CPU counters are missing")
        rows.append({"read_start_utc": lines[0], "read_end_utc": lines[-1],
                     "cpu_max": [int(value) for value in lines[1].split()], "counters": counters})
    if len(rows) != expected_samples:
        raise ValueError("Incomplete CPU sample count")
    return rows


def summarize(rows, start, end, *, identity_before, identity_after, expected_cpu):
    beginning, ending = instant(start), instant(end)
    if not rows or beginning >= ending or type(expected_cpu) not in (int, float) or expected_cpu <= 0:
        raise ValueError("Require CPU samples, a positive window and expected CPU quota")
    fields = ("uid", "container_id", "restarts")
    problems = []
    if (any(key not in identity_before or key not in identity_after for key in fields) or
            not identity_before.get("uid") or not identity_before.get("container_id") or
            type(identity_before.get("restarts")) is not int or identity_before.get("restarts", -1) < 0 or
            any(identity_before.get(key) != identity_after.get(key) for key in fields)):
        problems.append("container_identity_changed_or_missing")
    for index, row in enumerate(rows):
        left, right = instant(row["read_start_utc"]), instant(row["read_end_utc"])
        if not 0 <= right - left <= .1:
            problems.append("counter_read_interval_over_100ms_or_reversed")
        quota = row["cpu_max"]
        if quota != rows[0]["cpu_max"] or quota[1] <= 0 or quota[0] / quota[1] != expected_cpu:
            problems.append("cpu_quota_changed_or_unexpected")
        if index:
            previous = rows[index - 1]
            gap = left - instant(previous["read_end_utc"])
            if not 0 < gap <= 2:
                problems.append("cpu_sample_order_or_gap")
            if (row["counters"].keys() != previous["counters"].keys() or
                    any(row["counters"].get(key, -1) < value for key, value in previous["counters"].items())):
                problems.append("cpu_counter_reset_or_shape_changed")

    def before(boundary):
        candidates = [row for row in rows if instant(row["read_end_utc"]) <= boundary]
        return candidates[-1] if candidates else None

    def after(boundary):
        return next((row for row in rows if instant(row["read_start_utc"]) >= boundary), None)

    outer_start, inner_start = before(beginning), after(beginning)
    inner_end, outer_end = before(ending), after(ending)
    selected = (outer_start, inner_start, inner_end, outer_end)
    if any(row is None for row in selected):
        problems.append("cpu_window_not_bracketed")
    else:
        offsets = [beginning - instant(outer_start["read_start_utc"]), instant(inner_start["read_end_utc"]) - beginning,
                   ending - instant(inner_end["read_start_utc"]), instant(outer_end["read_end_utc"]) - ending]
        if any(not 0 <= offset <= 2 for offset in offsets):
            problems.append("cpu_boundary_bracket_over_two_seconds")
        if instant(inner_start["read_end_utc"]) >= instant(inner_end["read_start_utc"]):
            problems.append("cpu_inner_interval_is_empty")

    result = {"valid": not problems, "problems": sorted(set(problems)), "samples": len(rows),
              "window_start_utc": start, "window_end_utc": end,
              "interpretation": "Inside-container timestamps bound counter reads; inner/outer samples bound window CPU, not an exact point estimate",
              "counter_delta_bounds": None, "average_cpu_cores_bounds": None}
    if not problems:
        bounds = {key: {"min": inner_end["counters"][key] - inner_start["counters"][key],
                        "max": outer_end["counters"][key] - outer_start["counters"][key]} for key in rows[0]["counters"]}
        result["counter_delta_bounds"] = bounds
        result["average_cpu_cores_bounds"] = {key: value / 1_000_000 / (ending - beginning) for key, value in bounds["usage_usec"].items()}
        result["boundary_samples"] = {key: row for key, row in zip(("outer_start", "inner_start", "inner_end", "outer_end"), selected)}
    return result
