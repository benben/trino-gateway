# Container CPU window sampling

`observe_cpu.py` provides read-only sampling and analysis helpers. It does not
select a cluster, execute a command, change resources, or publish measurements.
Keep runtime samples and reports private. Run the host parser with Python 3.11 or
newer so it accepts the container's nanosecond-format timestamps.

Use `sampling_command(samples)` through an explicitly approved container exec.
The container needs an existing shell, GNU-compatible fractional UTC `date`,
`cat`, `sleep`, and cgroup v2 `cpu.max`/`cpu.stat` files. No package installation
or JVM attachment is required. Sample counts are bounded to 2–600; the caller
must also enforce a subprocess timeout and preserve any incomplete output.

Start one sampler per container before the workload. Read timestamps surround
each counter read **inside** the container. Kubernetes API and network latency
are not counter timestamps. Capture outer collection times separately, and
record the Pod UID, container ID and restart count before and after collection.
Wait for valid initial output before scheduling the measured window.

Pass the complete output to `parse_samples(raw, expected_samples)`. Then call:

```python
report = summarize(
    rows,
    actual_window_start_utc,
    actual_window_end_utc,
    identity_before=before,  # uid, container_id, restarts
    identity_after=after,
    expected_cpu=allocated_cpu_cores,
)
```

The summary requires stable identity/quota, monotonic counters, read intervals
of at most 100 ms, and sampling gaps no longer than two seconds. It requires samples
on both sides of each actual measurement boundary. Missing or invalid evidence
does not become zero CPU usage. Workload and container UTC clocks must be
sufficiently synchronized; this helper does not establish clock synchronization.

Inner samples bound the minimum counter increase inside the window; outer
samples bound its maximum. `average_cpu_cores_bounds` divides those CPU-time
bounds by the actual window duration. These are bounds, not an exact CPU point
estimate. The returned boundary samples retain the timing uncertainty. Sampling
itself consumes a small amount of container CPU, so use the same collection
method for comparison and idle windows. This is not a capacity guarantee or a
substitute for workload, database and preservation gates.
