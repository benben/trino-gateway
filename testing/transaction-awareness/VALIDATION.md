# Transaction-awareness verification checklist

This document defines verification procedures and scope, not an execution
receipt. Keep all deployment and test results, including sanitized summaries,
in private artifacts shared directly with the operator.

## Bind verification to an implementation

Record the source revision, immutable image digest, runtime configuration,
database engine, and fixture identities privately. Verify the running image
through every Gateway process rather than relying only on a deployment manifest.
If production code changes, identify and rerun the affected verification gates.

Run repository formatting, compilation, unit tests, and container-dependent
checks. Use a real PostgreSQL fixture for ledger concurrency and migrations.
Mock-only tests do not establish database locking or multi-process correctness.

## Required coverage

| Area | Required assertions |
| --- | --- |
| Identity and parsing | Reject unknown ownership, conflicting headers, malformed paths, duplicate JSON fields, and forged capabilities before unsafe forwarding |
| Ledger | Preserve atomic binding, callback idempotence, rollback on failure, concurrent admission ordering, and historical incarnation identity |
| Bootstrap | Preserve feature-disabled behavior and initialize the enabled dependency graph and database migrations |
| HTTP protocol | Retain transaction and result ownership across independent Gateway processes and backend cutover |
| Lifecycle | Keep pending work and open transactions as drain blockers; enforce generation checks and atomic sealing |
| Cancellation | Settle only an acknowledged cancellation request while preserving its query and transaction obligations |
| Faults | Retain uncertainty after database loss, response loss, ambiguous outcomes, and coordinator identity changes |
| Real clients | Preserve original protocol URLs, verified TLS, transaction affinity, and commit/rollback behavior with a supported client |
| Process replacement | Retain durable ownership and pre-existing result capabilities after replacing every Gateway process |

## Ordered disposable-lab runs

Use distinct Gateway process endpoints. Register independent backend processes
with different identities. Normal and irreversible-fault suites need separate
databases and credentials; an uncertain fault ledger is not a clean normal fixture.

```sh
cd testing/transaction-awareness
python3 run_contracts.py --suite normal --list
python3 run_contracts.py --suite normal
python3 run_contracts.py --suite fault
python3 run_contracts.py --suite real
```

Complete the native client procedure in [client/README.md](client/README.md).
Run the all-Gateway restart harness only after other tests using that lab finish.
It must verify changed Gateway process identities and unchanged database,
coordinator, configuration, credential, image, and replica snapshots.

For database outages, check recovery through every Gateway under one bounded
deadline before continuing the original transaction assertion. Preserve both
the no-forward assertion during the outage and the owner-affinity assertion
after recovery. Do not substitute an unrelated successful query for either.

See [BASELINE.md](BASELINE.md) for baseline comparisons and
[REVIEW-REGRESSIONS.md](REVIEW-REGRESSIONS.md) for targeted regression oracles.

## Boundaries

See [OPERATIONS.md](OPERATIONS.md) for the supported protocol and operational
contract. Important exclusions are:

- Stable Basic credentials and inline results are the initial supported mode.
- Coordinator loss still loses in-memory transactions. Gateway does not provide
  transaction replication or exactly-once execution.
- Ambiguous outcomes and indefinitely open transactions can block retirement
  indefinitely. Automatic coordinator-side reconciliation is not implemented.
- Result affinity has a finite retry window; response bodies are not durably
  replayed forever.
- Placement-header tests do not establish tenant provisioning, tenant
  authorization, warehouse data movement, or connector write semantics.
- Database failover and capacity claims require their own representative tests.
- Passing process-replacement assertions does not establish uninterrupted
  availability while all Gateway processes are stopped.
- Mixed-version compatibility needs explicit tests; do not infer it from
  single-version verification.

Correctness verification does not establish a throughput target. Follow
[SCALE-VALIDATION.md](SCALE-VALIDATION.md) for resource and concurrency checks.
