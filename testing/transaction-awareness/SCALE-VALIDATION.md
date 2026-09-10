# Scale-safety validation

This is an in-progress receipt for the bounded database/request implementation.
It does not establish a production throughput target. The earlier protocol receipt
in [VALIDATION.md](VALIDATION.md) describes a different implementation checkpoint.

## Checkpoints

- Baseline: `c7e2970`.
- Bounded pool, request capacity, phase deadlines and drain indexes: `2adce62`.
- Candidate shaded artifact SHA-256:
  `47043d0410eddd294fb0c8065bf1d364ea936b21d99e09c645f6da1d1aa30daf`.
- Published candidate image:
  `ghcr.io/benben/trino-gateway@sha256:1b55f3bfe11ef83fb55b2a338968a1dfbd781899ed70c43195aa7e05810c6f29`.
  Both architecture manifests identify source `2adce62`. Its CI-packaged JAR has
  SHA-256 `9d2e78975a2d9f6d8ff69964cd798efbf38b254c27493b7171147df0b7db390c`.
  This published artifact, not the local archive, is used for live candidate tests.

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
| Load-checkpoint oracle | Seven contradictory readiness/count cases incorrectly passed the original test helper. |

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

The integrated Python fixtures, fault proxy, open-loop scheduler and checkpoint
oracle passed 52 self-tests. The disposable deployment helpers passed 24 checks. The scheduler's
tests cover routing-group and per-method replica distribution, rejected requests,
client scheduling drops and invalid warmups.

Two upstream proxy suites require Docker and could not run on the local host.
The full CI matrix subsequently passed on Java 25, 26 and 27-ea, including the
container-dependent suites, plus Docker and the dedicated transaction workflow.
The [implementation CI run](https://github.com/benben/trino-gateway/actions/runs/34466159500)
and [publisher run](https://github.com/benben/trino-gateway/actions/runs/34466159538)
both completed successfully.

The live deadline regression passed on the published image with the same 500 ms
routing budget used for its baseline failure. Full Kubernetes protocol, native
JDBC, restart and load reruns are not yet complete at this checkpoint.

### Live gate failure and recycling regression

The published `2adce62` candidate is not accepted for transaction-aware use.
Two independent labs exhausted their per-process request capacity during ordinary
sequential requests, before fault injection. The fault lab reported zero pending
requests, open transactions and active queries while new requests received 503.
This is leaked process capacity, not a legitimate durable admission backlog.

The overload/normal runner stopped during overload setup, before its normal
contract cases. The fault runner passed four capability cases and then stopped
during the next case's setup. These are failed acceptance runs, not passing suites.
Their ledger state is preserved. Sanitized Gateway exception traces point to
`completeRequest` inside the proxy's completion callback, where a recycled
servlet request no longer has its lease attribute.

Two new deterministic tests reproduced the failure before repair: recycling the
servlet at response delivery leaked capacity, and recycling it at client timeout
prevented recording the original admission. The repair captures admission, lease,
method, URI and user before dispatch. Completion and failure handlers use that
snapshot, including their cleanup paths. The real PostgreSQL completion test now
also recycles the servlet at timeout. Complete live acceptance must still be
rerun on the replacement image before this failure is considered resolved.

The integrated repair passed 220 Java tests with zero failures, errors or skips,
using the command above plus `TestRouteToBackendResource` in its test selector.
It also covers recycled-request transport failure, malformed responses, POST
user capture and response-binding failure. The original admission remains the
target of completion or uncertainty recording, and the slot releases only after
that processing finishes.

### Repaired image runtime checkpoint

The replacement image uses source `af8bc75ea292bbc1ad517a3a74a7b6ed1ccfe7cd`:
`ghcr.io/benben/trino-gateway@sha256:e10742bf46cf069b8930ef9e282dfee1093055bef48059eb9da4cec7f1f70f4d`.
Anonymous pulls and source labels were verified for both architecture manifests.
The source passed its full Java-version CI matrix and focused transaction tests.

On that exact image, the complete 20-case fault suite passed. All five real-Trino
HTTP cases and both native JDBC 483 cases passed, using verified TLS and the
driver's original protocol URLs. The fault suite's deliberately uncertain state
is retained; passing it does not make its database a clean normal-test fixture.

The first repaired-image overload attempt timed out while awaiting the rejected
seventeenth request. A diagnostic repeat received the complete 503 response in
0.607 seconds, and the unchanged original regression then passed. No assertion
was relaxed and no further production change was made. The first timeout remains
unexplained and is not erased by those successful repeats.

Normal, deadline, all-Gateway restart, and Aurora load acceptance are still in
progress at this checkpoint. No successful 1000-request/second or 100-replica
capacity claim is established by these results.

The repository now includes [database cost measurements](DATABASE_COST.md),
one-second connection sampling, and aligned HTTP request windows. Their unit
tests pass. CI also executes the persistent PostgreSQL observer integration test
against its PostgreSQL service; it does not merely skip that test. These tool
checks are separate from a measured Aurora load result.

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
