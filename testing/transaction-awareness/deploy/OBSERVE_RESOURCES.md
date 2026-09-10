# Python container resource sampler

`observe_resources.py` generates a finite read-only command for an existing Python 3.11+ interpreter in a test fixture or load-client container. It does not need GNU `date`, additional packages, image changes, or mounted scripts. The host parser also needs Python 3.11+ and the adjacent `observe_cpu.py` module.

The command reads cgroup v2 `cpu.max`, `cpu.stat`, `memory.current`, and `memory.max` between two UTC timestamps. It emits one JSON line per sample. Sample counts must be integers from 2 through 600. The loop sleeps one second between reads and checks a monotonic deadline of sample count plus 30 seconds. A stalled read or output write still needs the caller timeout.

The caller must:

- Verify the exact test namespace, pod ownership, and container before execution. Pass an explicit Kubernetes context.
- Execute `sampling_command(samples)` with a subprocess timeout of `sampling_timeout(samples)` seconds. This adds a bounded 60-second transport allowance.
- Capture exit status and all output privately outside the repository. A timeout, nonzero exit, or incomplete receipt invalidates the capture.
- Collect pod UID, container ID, and restart count before and after sampling. Pass these identities to `summarize`, together with expected CPU and memory limits from the approved resource specification.
- Start before the load window and finish after it. Do not include warmup, checkpoints, or cleanup in the requested summary window.

Use `parse_samples(raw, expected_samples)` to validate the JSON lines. Then call `summarize(rows, start, end, identity_before=..., identity_after=..., expected_cpu=..., expected_memory_bytes=...)`. Window timestamps need fractional UTC precision with a `Z` suffix. Both finite cgroup limits must remain unchanged. Failed identity, quota, cadence, boundary, or counter-reset checks invalidate the receipt.

CPU output reuses the inner/outer counter bounds from `observe_cpu`. It is not an exact instantaneous utilization measurement. The sampler's own small CPU and memory costs are included, so compared cases must use the same sampler configuration.

The workload clock and sampled container clock must be synchronized. The helper checks sample ordering and intervals but cannot establish cross-node clock synchronization. Retain that prerequisite when aligning the samples with a workload window.

Memory is a nonmonotonic gauge, not a cumulative CPU counter. Its summary uses only samples whose entire read interval lies inside the requested window. At least two such samples are required. It reports sampled minimum, maximum, arithmetic mean, last-minus-first change, and maximum fraction of the configured limit. Decreasing memory is valid. `memory.current` includes cgroup-charged cache; it is not process RSS. These samples do not prove an exact peak, time-weighted average, or growth between the precise window boundaries. Retain independent OOM/restart checks and fresh metric coverage before interpreting throughput.
