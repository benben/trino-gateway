# Transaction-aware routing and safe backend draining

Status: implementation contract and adversarial acceptance checklist. This document
does not claim that the feature, its tests, or a production rollout are complete.

## Scope and guarantees

The opt-in feature preserves explicit Trino transaction affinity and prevents a
planned backend retirement while that backend has unresolved work. The first
implementation uses PostgreSQL. Enabling it with an unsupported datastore or
incomplete security configuration must fail startup. The disabled configuration
must preserve existing routing behavior.

This is routing, not transaction replication. A transaction cannot move between
coordinators. A coordinator failure can still fail its transactions. The gateway
must not retry a possibly accepted statement on a different coordinator, invent
an outcome for an ambiguous commit, or claim exactly-once statement execution.

A successful socket write is not proof that a client received a response. Result
retention and retry guarantees therefore need an explicit bounded contract.
Unknown outcomes block automatic retirement until authoritative reconciliation;
a timeout is not, by itself, evidence that work finished.

Tenant placement and tenant data migration remain external responsibilities.
This feature can preserve old transactions while new work uses a new placement.
It cannot establish that the target has the required catalogs, authorization,
data freshness, or write consistency. Those are cutover prerequisites.

## Threat model and supported identity

Clients can forge headers, replay transaction identifiers, send contradictory
query and transaction identities, change credentials, and disconnect at any
point. A replica can crash between any two operations. Requests and responses can
arrive concurrently, out of order, or more than once. The database can become
unavailable. An operator can attempt a backend rename, deletion, or URL change
while old requests remain outstanding.

Backend authentication remains authoritative. An affinity binding does not
authenticate a credential or authorize a SQL operation. Existing parsed request
metadata is not automatically a trusted identity: `TrinoRequestUser` accepts
`X-Trino-User` and decodes JWT claims, while `NoopFilter` creates a synthetic
security-context principal. Neither is sufficient alone for transaction ownership.

The minimum supported production mode is credential-bound identity:

The initial supported and tested credential mode is HTTP Basic with a stable
credential throughout an explicit transaction. Bearer-token refresh, browser
cookie identity and stable authenticated-principal migration are not claimed by
this contract. They require separate resolver and client compatibility tests.

- Require a supported Authorization credential on new statement submissions.
- Derive a binding from HMAC-SHA256 using a shared key of at least 32 bytes and the
  credential plus relevant effective
  user/tenant context. Do not persist or log raw credentials. Do not use an
  unkeyed Basic-credential digest, which exposes password guessing to database
  readers and backups.
- Bind the effective user as well as the credential when one service credential
  can impersonate multiple users. Caller-controlled tenant labels are not proof
  of tenant membership. Backend authorization must still validate impersonation.
- Reject a credential or effective-user change within an explicit transaction.
  Supporting credential rotation requires a separately verified stable-principal
  resolver; it must not silently fall back to a user header.
- All replicas must share the binding key and configuration. Key rotation must
  preserve existing bindings through their lifetime or require a completed drain.
- Unit tests can inject a fake resolver. Do not introduce a production
  configuration switch that treats unverified user headers as authenticated.

Verified mTLS principal binding is a possible separate resolver. It requires an
explicitly tested certificate-validation and mapping boundary. Generic servlet
principals, UI cookies, and decoded JWT claims must not be assumed verified.

Continuation GET/HEAD/DELETE requests can omit Authorization according to the client
protocol. Preserve the original query binding and the exact opaque continuation
path; let the backend validate its capability. A bare query ID is not equivalent
to a continuation capability. If a continuation supplies credentials or a
transaction ID, contradictory ownership must fail closed. Do not synthesize
missing credentials, reveal the binding, or route to another backend on denial.
Query metadata and cancellation through `/v1/query/{id}` require matching Basic
credentials: those paths contain no opaque result capability.
Separate the credential HMAC from the effective/original-user context HMAC in the
internal owner binding. An authenticated continuation that omits user headers can
then prove the same credential without guessing the original impersonated user.
If it supplies user headers, validate the full canonical context. Internal query
lookup can expose this binding to the service, but public status APIs must not.

## Durable state and atomic operations

Reuse the existing PostgreSQL connection and migration infrastructure. Do not use
the optional, creation-time-retained `query_history` table as the live ledger.
Live state must survive all gateway replicas restarting and query-history cleanup.

The following are semantic records, not a promise of finalized table names:

| Record | Minimum meaning |
| --- | --- |
| Backend incarnation | Immutable identifier, exact destination and external URL, logical backend name, routing group, ACTIVE/DRAINING/SEALED state and monotonically increasing generation |
| Transaction binding | Transaction ID, owner binding, backend incarnation, monotonic state and reconciliation metadata |
| Query binding | Query ID, owner binding, backend incarnation, optional transaction ID, execution/result state and replay-retention metadata |
| Admission obligation | A statement selected for a backend that has not yet been conclusively bound to a query or rejected before acceptance |

Every forwarded protocol request, including continuation GET, HEAD and DELETE, creates an
admission obligation. Status counts unresolved admissions, open transactions,
and queries that are nonterminal or still inside their terminal retry window.
Database time determines retry-window expiry so replica clock skew cannot seal
a backend early.

Names such as blue and green can be reused for later deployments, but an old
incarnation must never be repointed to a new coordinator. Updating or deleting an
incarnation with live obligations must fail. Reactivation must not erase those
obligations or bypass an in-progress retirement operation.
The implementation rejects multiple current backend names for the same endpoint
or observed process. It does not support independent alias ledgers. Incarnation
records remain immutable after sealing. Explicit reincarnation changes only the
logical name's current-incarnation pointer and preserves all historical bindings.
It requires the expected incarnation and generation, SEALED state, zero remaining
obligations, a different observed process, and no route pointing to the old slot.
The replacement starts DRAINING with a generation greater than the old incarnation.
An explicit resume makes it eligible for new statements. Old query and admission
operations still lock their historical incarnation, so they cannot reach a newly
started coordinator at the same URL. Stale seal, resume, and replacement requests
must not affect the replacement. This permits repeated blue/green deployment
cycles without deleting transaction tombstones or reassigning old query IDs.

Where the supported coordinator exposes `nodeId` and `coordinatorId`, bind both
to the registered incarnation and reject a mismatch. They detect ordinary
same-address restarts but do not provide a cryptographic process identity:
coordinator IDs have a finite collision space, and a process can restart between
a health check and request dispatch. Validate response query identity as well.
Do not describe an internal ledger UUID as proof that the remote process survived.

The minimum store interface should express atomic operations, not independent
read-then-write decisions:

1. Admit a new statement against an eligible backend generation and create its
   obligation. If drain wins first, admission fails or selects another eligible
   backend before any request is sent.
2. Admit a transaction statement only against its existing owner and backend.
   Existing transactions may continue on a draining backend, but not on a retired
   incarnation. A concurrent transaction close cannot resurrect its binding.
3. Record a backend response: bind its query, apply transaction-start or clear
   signals, update query/result state, and settle the corresponding admission.
   Conflicting owner/backend bindings fail; identical observations are idempotent.
4. Begin drain and read a durable drain status. Retirement is permitted only when
   admissions, live transactions, query/result obligations, and unresolved
   observations are settled under the same generation fence.

The selected implementation uses a durable row per admission and locks the
backend-incarnation row for each state transition. Admission, response recording,
drain initiation and final sealing share that lock. This removes the need for an
all-replica acknowledgement protocol. Local reference counts or an elapsed replica
lease alone cannot establish safe drain. Do not hold a database transaction open
across a remote HTTP request.

Reading drain status must not change state. Report `readyToSeal` when no blocking
obligations remain. Report `drained` only after an explicit seal operation locks
the same backend row, rechecks all obligations, and commits SEALED. A sealed
backend rejects late continuation admission as well as new statements. This
prevents a late GET racing between a zero-count observation and backend teardown.
Seal and resume require the caller's expected generation. Drain and resume advance
the generation. Resuming the same incarnation does not discard old obligations
or reset transaction tombstones. Responses to already admitted requests remain
recordable across drain/resume transitions; generation changes fence lifecycle
decisions, not the durable identity of an accepted request.

## Request and response lifecycle

On a new statement, resolve transaction affinity before selecting a new route.
If both a query ID and a transaction ID are present, validate their consistency;
do not let either bypass the other. Unknown, malformed, conflicting, closed, or
indeterminate transaction IDs must not fall through to ordinary routing.

Capture transaction-start and transaction-clear response headers on every
relevant proxied response, including continuation GET responses. Persist required
state before exposing those headers or a new query continuation to the client.
Header names are case-insensitive. Reject duplicate or contradictory ownership
signals rather than selecting an arbitrary value. Do not infer commit or rollback
success solely from SQL text or an HTTP 200 status.
The presence of the clear header signals clearing; its value is not a transaction ID. Resolve its
transaction through the admission or query binding. A start response also binds
its query to the newly created transaction, even though the original request had
no transaction ID. This relationship must survive delayed header replays.

An observed transaction clear permanently prevents new statements on that
transaction. Retain a tombstone for replay detection. A late or repeated start
observation for the same ID must not reopen it. Already admitted queries retain
their own obligations and may need their remaining results served after the
transaction closes.

Cancellation is an outcome to reconcile, not permission to delete all state.
DELETE can race with a response, fail in transit, or cancel a query without
closing its enclosing transaction. Likewise, a query error can leave a failed
transaction that still requires rollback. Idle transactions remain obligations
until an authoritative expiry or terminal outcome is established.

The initial terminal-response retry policy retains the original backend for a
configurable window, defaulting to 120 seconds. Tests can use a shorter window.
This is a finite retry guarantee, not durable replay of response bodies:

- Keep the original backend available throughout the documented retry interval.
  Durable response replay is a possible later extension, not an initial guarantee.
- No `nextUri` in a successful protocol response is a useful terminal signal, not
  proof of client receipt. Malformed bodies and transport failures are not
  terminal signals.
- A delayed old response cannot change a terminal query back into a live query
  or overwrite a newer continuation state.
- Spooling, external result URLs, cancellation paths, and response-size limits
  require explicit supported behavior. Unsupported modes must fail clearly or
  remain excluded from the retirement guarantee.

If a request might have reached Trino but no response can be durably recorded,
retain an unresolved admission. Reconcile it against authoritative coordinator
state before retirement; do not assume it never executed and do not replay it
automatically. This deliberately favors safety over guaranteed drain completion.

An authenticated coordinator query/transaction inventory can support reconciliation,
but a snapshot alone does not resolve a request still in flight. Reconciliation
must fence admissions and establish that the observed process is the original
incarnation. Automatic reconciliation is not complete until these races have
tests. An explicit operator abandonment action must be labeled potentially
disruptive, never reported as a proven graceful drain.

## Adversarial acceptance matrix

Each row requires an automated assertion or an explicit unsupported-mode startup
or request rejection. Passing happy-path affinity tests is not sufficient.

| ID | Scenario | Required result |
| --- | --- | --- |
| CFG-1 | Feature disabled | Existing behavior and migrations remain compatible |
| CFG-2 | Feature enabled with non-PostgreSQL datastore, missing key, or unsafe identity configuration | Startup rejects configuration |
| ID-1 | Same credential, different effective user or tenant context | Cannot inherit another owner's transaction |
| ID-2 | Forged `X-Trino-User`, unsigned JWT claims, synthetic servlet principal | No authenticated identity is inferred from metadata alone |
| ID-3 | Missing, changed, duplicated, or malformed Authorization on statement | Deterministic rejection; no backend submission on rejection |
| ID-4 | Headerless continuation with valid opaque path | Uses existing exact binding without inventing a new backend |
| ID-5 | Continuation carries conflicting credential, transaction, or query identity | Reject before forwarding |
| ID-6 | Credential/key rotation during an open transaction | Explicit supported transition or fail-closed rejection, never silent rebinding |
| ID-7 | Impersonated query continuation supplies same Authorization but omits user headers | Validates credential component against original binding without changing user context |
| TX-1 | Start response arrives on initial POST or continuation GET | Binding exists durably before start header reaches client |
| TX-2 | Following statements alternate among six gateway replicas | All reach the original incarnation |
| TX-3 | Unknown transaction ID with healthy default backend | Reject; zero fallback submissions |
| TX-4 | Same start observation replayed concurrently | One identical binding; no duplicate live obligation |
| TX-5 | Same ID observed with another owner or incarnation | Reject conflict and retain original binding |
| TX-6 | Clear races with duplicate or delayed start response | Tombstone wins; transaction never reopens |
| TX-7 | Query error, failed commit, successful rollback, idle transaction | State follows authoritative protocol outcome, not SQL string matching |
| TX-8 | Concurrent transaction close and new statement admission | One serialized outcome; no admission after terminal state |
| Q-1 | Query-history disabled or its retention cleanup runs | Active affinity and drain safety are unaffected |
| Q-2 | Query binding observed more than once | Same binding succeeds; conflicting binding fails |
| Q-3 | Terminal query with transaction still open | Query can settle without permitting backend retirement |
| Q-4 | Transaction cleared with query/results still outstanding | Transaction tombstone does not erase result obligation |
| Q-5 | Final page recorded, then client disconnects or gateway crashes | Defined retry contract holds; no claim of client acknowledgement |
| Q-6 | Reordered continuation responses or terminal-page retries | Lifecycle state is monotonic and affinity remains exact |
| Q-7 | DELETE races with GET, timeout, or transaction clear | No double release; uncertainty remains an obligation |
| Q-8 | Malformed, oversized, contradictory, or unexpected response | Fail closed without declaring accepted work complete |
| Q-9 | Completed admission callback repeats or a late failure callback arrives | Identical observation is idempotent; conflicting observation fails; completion never becomes uncertain again |
| D-1 | New admission races with drain on another replica | Admission is fenced or remains counted against old incarnation |
| D-2 | Existing transaction submits during drain | Original backend accepts it subject to backend auth; no new-route selection |
| D-3 | Replica crashes after admission, before send, or after possible acceptance | Retirement blocks until authoritative reconciliation |
| D-4 | Database fails before admission or during response recording | No untracked successful admission is exposed to client |
| D-5 | Drain coordinator/process restarts | Durable status resumes without resetting obligations |
| D-6 | Backend delete, URL mutation, or blue/green reuse while live | Mutation rejects; old affinity never targets a replacement incarnation |
| D-7 | Deactivate then reactivate during another retirement request | Generation/CAS rejects stale retirement decisions |
| D-8 | INFO_API reports zero queries or backend becomes unreachable | Neither condition is accepted as evidence of complete drain |
| D-9 | Idle/abandoned transaction exceeds gateway timeout | No time-only deletion; authoritative expiry or explicit intervention required |
| D-10 | Backend coordinator crashes and restarts at the same Service URL | Old work is failed/reconciled, never considered transferred to the new process |
| D-11 | Late continuation races with final seal | It creates a counted admission before seal, or fails after SEALED; no post-seal forwarding |
| OBS-1 | Logs, errors, metrics, and database inspection | No raw Authorization, passwords, tokens, or unbounded-cardinality identities |
| COMP-1 | Real supported Trino client transactions, result pagination and cancellation | Protocol compatibility demonstrated, not just fake-server behavior |

Concurrency tests must force interleavings with barriers instead of relying on
sleep timing. Failure tests must assert both the response and durable state, plus
the number and destination of backend submissions. The lab must use disposable
resources and synthetic identities. A passing lab is not authorization to package
or roll out the feature to production.

### PostgreSQL store tests

Run the focused store suite with a Docker-compatible Testcontainers runtime:

```sh
./mvnw -pl gateway-ha -am -Dtest=TestTransactionStore -Dsurefire.failIfNoSpecifiedTests=false test
```

Alternatively, supply `TX_STORE_TEST_JDBC_URL`, `TX_STORE_TEST_USERNAME`, and
optionally `TX_STORE_TEST_PASSWORD` for a disposable PostgreSQL instance. The
suite creates a randomly named private test schema and removes that schema after
the run. Never point these variables at a production database. The tests use
separate store instances and real database transactions; they complement, not
replace, the multi-process Gateway and real-Trino protocol suites.

## Existing extension boundaries

The configurable `RoutingManager` binding can change routing decisions, but it
does not observe all proxy response lifecycle events. Implementing this contract
requires request/response integration and durable lifecycle operations, not only
a new routing rule or chart setting. Existing synchronous query-history insertion
within the asynchronous initial-response chain is a useful integration point,
but it does not cover continuation responses or safe admission fencing.
