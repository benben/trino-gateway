# Scale-safety verification procedure

Keep benchmark measurements, deployment outcomes, execution counts, timings,
artifact receipts, and diagnostic traces private. This document describes
reproducible checks and interpretation limits, not measured capacity.

## Resource and deadline contracts

Verify these properties independently before a load comparison:

- Database connections are reused and the configured per-process pool is bounded.
- Admission pressure leaves completion capacity available.
- JDBC acquisition, statement execution, lock waiting, and cancellation respect
  their configured phase budgets.
- Per-process request capacity rejects excess work before backend dispatch and
  provides explicit retry guidance.
- Process-identity probes have a deadline that cannot outlive the admission
  budget; a late probe must not dispatch a statement.
- Completion retains its request context and capacity lease after the servlet
  is recycled or the client stops waiting.
- Completion releases capacity only after durable response or uncertainty
  processing finishes.
- Drain queries inspect live obligations without scanning completed history.
- A scheduled cleanup failure does not disable future cleanup executions.

## Database and adapter tests

Run the focused suite with a disposable real PostgreSQL database configured
through `TX_STORE_TEST_JDBC_URL` and `TX_STORE_TEST_USERNAME`:

```sh
./mvnw -B -ntp -pl gateway-ha -am test \
  -Dskip.installnodepnpm -Dskip.pnpm \
  '-Dtest=TestTransaction*,TestStrictProxyResponseHandler,TestDatabaseDeadline,TestJdbcConnectionManager,TestRouteToBackendResource' \
  -Dsurefire.failIfNoSpecifiedTests=false
```

Keep formatting and build-quality checks enabled. Container-dependent tests
require a suitable container runtime or CI runner; do not classify an unavailable
runtime as a passing test.

Use database-observed barriers for concurrency regressions. A delayed completion
for one query must not block an unrelated query solely because both use the same
backend. At the same time, per-admission, query, and transaction identity fences
must preserve conflicting-callback rejection and atomic first binding.

Cover late START observations, transaction close versus admission, capability
publication, failure rollback, hash collisions, and administrative drain, seal,
cutover, and reincarnation races. Shared normal-operation fences do not authorize
weakening exclusive administrative state transitions.

For drain-query efficiency, use expired history plus live blockers and inspect
the actual PostgreSQL plan. Keep an explicit buffer-access bound instead of a
machine-specific elapsed-time oracle. Never delete unresolved ledger state as
a performance optimization.

## Multi-process and load gates

Run the correctness procedures in [VALIDATION.md](VALIDATION.md), including
database faults, native clients, and Gateway process replacement. Keep any failed
case and its uncertainty intact before attempting a corrected comparison.

Use [LOAD.md](LOAD.md) for bounded open-loop workloads and
[MATRIX.md](MATRIX.md) for per-case workload and preservation guards. Compare
identical database, client, payload, monitor cadence, resources, and topology
between baseline and candidate images. An inherited database is not a fresh
baseline merely because the measured backend name changed.

Keep before/after identity fingerprints for historical obligations, and require
clean measured owners. A workload must not gain a clean drain result by clearing
uncertain admissions or discarding retained queries.

Use [DATABASE_COST.md](DATABASE_COST.md) for aligned request windows, connection
sampling, idle baselines, ACUs, and cost attribution. Tool self-tests validate the
measurement implementation, not the capacity or cost of a deployed environment.

## Interpretation limits

Pool and request limits are per process. They bound particular resources, not
fleet throughput or total heap. A hot query or transaction can still serialize
operations that correctly share its identity fence. A bounded pool can queue
unrelated callers when its connections are occupied.

PostgreSQL phase deadlines do not limit Trino transaction lifetimes. Completion
queue waiting and driver cancellation margins are not a hard end-to-end latency
guarantee. Private results must distinguish offered HTTP requests, successful
HTTP requests, SQL submissions, rejected load, and client or fixture saturation.
Rejected requests do not demonstrate successful throughput at the offered rate.
