# Scale-safety validation

This is an in-progress receipt for the bounded database/request implementation.
It does not establish a production throughput target. The earlier protocol receipt
in [VALIDATION.md](VALIDATION.md) describes a different implementation checkpoint.

## Checkpoints

- Baseline: `c7e2970`.
- Bounded pool, request capacity, phase deadlines and drain indexes: `2adce62`.
- Candidate shaded artifact SHA-256:
  `47043d0410eddd294fb0c8065bf1d364ea936b21d99e09c645f6da1d1aa30daf`.

## Genuine red tests

These failures were assertions against the baseline, not build, formatting,
container setup or test-fixture failures:

| Regression | Baseline observation |
| --- | --- |
| Production connection reuse | Consecutive operations used different PostgreSQL backend PIDs. |
| Session statement deadline | The session had an unlimited statement timeout. |
| Pool capacity | A fifth connection succeeded instead of reaching the configured bound. |
| Contended backend row | An update exceeded the bounded observation window while another session held the row lock. |
| Admission overload | The seventeenth held request was admitted instead of rejected before dispatch. |
| Coordinator identity probe | Its outgoing request had no explicit per-request timeout. |
| Completed-history drain scan | Correct counts required 413 shared-buffer accesses with 40,000 completed rows per ledger table. |
| Query-history cleanup retry | A scheduled cleanup exception prevented the next cleanup execution. |
| Multi-process overload | A held seventeenth continuation reached the client timeout instead of receiving an immediate 503. |
| Multi-process probe deadline | A held identity probe exceeded a two-second client wait despite a 500 ms routing budget. |

The drain test exercises the production SQL with live blockers and expired history.
It asserts fewer than 64 shared-buffer accesses, not a machine-specific elapsed time.
No ledger deletion or uncertainty expiry is part of this change.

## Completed candidate checks

The integrated Java command ran 210 tests with zero failures, errors or skips.
After the test-only pooled-completion addition, it passed all 211 tests:

```sh
./mvnw -B -ntp -pl gateway-ha -am test \
  -Dskip.installnodepnpm -Dskip.pnpm \
  '-Dtest=TestTransaction*,TestStrictProxyResponseHandler,TestDatabaseDeadline,TestJdbcConnectionManager' \
  -Dsurefire.failIfNoSpecifiedTests=false
```

The test database was a dedicated PostgreSQL 18 fixture, selected through
`TX_STORE_TEST_JDBC_URL` and `TX_STORE_TEST_USERNAME`. Normal formatting and Maven
checks were enabled. The shaded package also built successfully.

Coverage includes real JDBC statement cancellation and rollback, a completion
connection left available under admission pressure, pool shutdown, bounded
completion workers and queue, cancellation detachment, callback-binding failure,
synchronous transport failure, duplicate permit release and feature-disabled paths.

The additional integration case uses the actual service, proxy completion callback,
Hikari pool and PostgreSQL ledger with a controlled asynchronous upstream transport.
It records a terminal response after client timeout while three admission
connections remain occupied, and observes exactly four pooled PostgreSQL sessions.
It does not replace network or multi-process tests.

The integrated Python fixtures, fault proxy and open-loop scheduler passed 36
self-tests. The disposable deployment helpers passed 23 checks. The scheduler's
tests cover routing-group and per-method replica distribution, rejected requests,
client scheduling drops and invalid warmups.

Two upstream proxy suites require Docker and could not run on the local host.
The full CI matrix must cover them; this receipt does not count their setup errors
as successful tests. Kubernetes protocol, native JDBC, restart and load reruns are
not yet complete at this checkpoint.

## Interpretation limits

Pool and request limits are per process. They bound particular resources, not
fleet throughput or total heap. PostgreSQL phase deadlines do not limit Trino
transaction lifetimes. Completion queue waiting and driver cancellation margins
are not a hard end-to-end latency guarantee.

The existing exclusive backend lock and per-request coordinator checks remain.
Benchmark results must distinguish offered HTTP requests, successful HTTP requests,
SQL submissions, client saturation and backend-fixture saturation. Rejected load
does not demonstrate successful throughput at the offered rate.

See [OPERATIONS.md](OPERATIONS.md) for migration constraints and failure boundaries.
