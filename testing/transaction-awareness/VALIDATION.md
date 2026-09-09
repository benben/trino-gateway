# Transaction-awareness validation receipt

Date: 2026-09-09. This validates the supported routing protocol in disposable
environments. It is not production deployment approval or a performance benchmark.

## Tested implementation

The final implementation checkpoint is `5920d79`. Its shaded Gateway artifact
has SHA-256:

```text
8e3a4ac72ca983f7186327ebb67a4696011f35903df54b0e863016f7d49ec583
```

The reproducible ordered runner and strengthened restart harness are in
`4f2115e`. No production Java changes followed the implementation checkpoint.
The implementation checkpoint passed full CI: Docker and Maven on Java 25, 26,
and 27-ea, plus the dedicated PostgreSQL transaction workflow.

## Executed checks

| Check | Result |
| --- | --- |
| Focused Java adapter, identity, configuration, legacy-resource and strict-response checks | 108 passed |
| Real PostgreSQL ledger checks, including concurrent operations and result-capability persistence | 40 passed |
| Full production-module bootstrap with the feature enabled and disabled | 2 passed |
| Python protocol and TCP fault-fixture self-tests | 27 passed |
| Disposable deployment and restart safety checks | 17 passed |
| Normal multi-process HTTP contract | 57 passed in 1312.159 seconds |
| Capability and irreversible-fault contract | 20 passed in 486.127 seconds |
| Real Trino HTTP contract after replacing all Gateways | 5 passed in 211.030 seconds |
| Unmodified JDBC 483 transaction cutover | Passed through both Gateway replicas |
| All-Gateway process replacement | Open transaction and pre-existing query continuation both survived |

These counts describe separate suites, not one combined invocation. The normal
57-test run used the preceding `da3a5d4` artifact, SHA-256
`fcf43886557d3f3b7568c38451ab22d787df58dfd61d40a6c7cfd6842aa1d6b1`.
The only subsequent production change narrowed successful DELETE accounting:
HTTP 204 settles its request without completing its query or transaction. The
20-test fault suite, real-client checks, and Gateway replacement used the final
artifact. The fault suite explicitly tests this refinement and a concurrent poll.

The original upstream failures are recorded in [BASELINE.md](BASELINE.md).
Additional genuine red tests and mutation evidence are recorded in
[REVIEW-REGRESSIONS.md](REVIEW-REGRESSIONS.md). No expected-failure annotations
were used to turn the acceptance runs green.

## Live topology and observations

The normal lab used two independent Gateway pods sharing PostgreSQL, two
controlled backends in one routing group, a third independent backend in another
group, and two stock Trino 483 coordinators with TPCH catalogs. A separate fault
lab had its own two Gateways, PostgreSQL, controlled backends and TCP fault proxy.
Clients and Gateway-to-Trino connections verified TLS with a private test CA.
Credentials and infrastructure addresses stayed outside the public repository.

The normal run exercised transaction affinity, ownership rejection, lifecycle
headers on continuation pages, replay, idle transactions, admission/drain races,
generation fencing, malformed paths, heartbeat handling and result capabilities.
It changed the routing-group input while existing transactions retained their
owner. It also completed three blue/green reincarnation cycles without rebinding
old query IDs to replacement processes. This simulates placement changes; it does
not implement tenant provisioning or migrate warehouse data.

The fault run exercised database loss before admission and after backend
acceptance, cancellation races, stage cancellation, malformed and duplicate JSON,
conflicting lifecycle headers, lost responses, ambiguous HTTP 408 responses and
coordinator identity replacement. Every injected ambiguous outcome retained a
drain blocker. Database connectivity was restored after each outage. The fault
lab intentionally ended with unresolved ledger state; it was not declared clean
or safely drained.

The restart harness replaced every Gateway pod and verified different pod UIDs,
unchanged PostgreSQL and Trino pod identities/restart counts, and unchanged
credential/configuration Secret data. A query issued before replacement delivered
its expected result afterward through a persisted capability. The open transaction
then worked through both new Gateways and committed. This demonstrates persistent
ownership, not uninterrupted request availability while every Gateway is down.

Native JDBC tests used the standard driver without URL rewriting or an encoding
override. New connections reached the replacement coordinator while the existing
transaction stayed on its original coordinator. Separate HTTP tests exercised
requests across individually addressed Gateway replicas.

## Boundaries

See [OPERATIONS.md](OPERATIONS.md) for the full contract. Important exclusions are:

- Stable Basic credentials and inline results are the supported initial mode.
- Coordinator loss still loses its in-memory transactions. Gateway does not
  provide transaction replication or exactly-once execution. Query or transaction
  failure after coordinator loss is explicitly accepted and outside this scope.
- Ambiguous outcomes and indefinitely open transactions can block retirement
  indefinitely. Automatic coordinator-side reconciliation is not implemented.
- Result affinity has a finite retry window; response bodies are not durably
  replayed forever.
- Database failover, six-replica load, production performance, DuckLake connector
  write semantics, tenant authorization/provisioning and warehouse data movement
  were not validated by these tests.
- Mixed feature-disabled or older prototype replicas are outside the guarantee.

No production packaging, image publication, DNS, IAM or production deployment
was performed for this implementation exercise.
