# Transaction-awareness integration tests

See [OPERATIONS.md](OPERATIONS.md) for supported modes, configuration, cutover
steps and failure boundaries. See [BASELINE.md](BASELINE.md) for upstream red
evidence and [client/README.md](client/README.md) for the native JDBC fixture.
The executed acceptance results are in [VALIDATION.md](VALIDATION.md).

This directory contains a Python standard-library black-box test suite and a protocol-focused Trino test double. Python 3.9 or newer is sufficient. The suite deliberately separates baseline controls from the new transaction-awareness contract. Contract failures against upstream are regression evidence, not a successful implementation.

All endpoints must belong to an isolated, disposable test environment. The suite changes backend activation, routing and drain state. It must never target customer services. Fixture control endpoints have no authentication and must remain restricted to the test network.

After configuring the runtime variables below, `run_contracts.py --suite normal`
runs the complete normal contract in order and stops at the first failure.
Use `--suite fault` only in a separate expendable lab; coordinator replacement
runs last. `--suite real` uses the real Trino endpoints. Add `--list` to inspect
the selected tests without contacting any endpoint. The runner can derive
`TX_QUERY_AUTHORIZATION` from the private `TX_TRINO_USER` and `TX_TRINO_PASSWORD`
variables without printing either credential. Native JDBC and Gateway restart
tests remain separate, exclusive operations.

## Components

- `fake_trino.py`: independent backend-local transactions, standard statement and continuation responses, transaction lifecycle headers, replayable result pages and controllable response barriers.
- `protocol.py`: HTTP client that preserves repeated headers and can send successive requests through different Gateway processes.
- `test_fake_trino.py`: local fixture self-tests. These establish fixture behavior, not Gateway correctness.
- `test_gateway.py`: baseline controls and transaction contract assertions against separate Gateway processes sharing one PostgreSQL database.
- `test_adversarial.py`: additional identity, replay, query-error and seal-interleaving assertions.
- `test_protocol_boundaries.py`: canonical statement paths, heartbeat accounting and unsupported method/result-mode checks.
- `test_reincarnation.py`: three blue/green reuse cycles with stale-generation and old-query rejection.
- `network_fault_proxy.py`: TCP-transparent database fault boundary with an isolated HTTP control port.
- `test_faults.py`: response-loss, conflicting-header, cancellation, database-outage and coordinator-restart assertions.
- `deploy/`: environment setup and runtime configuration, maintained separately from the protocol suite.

Start two fake backends with different identities:

```sh
python3 fake_trino.py --host 0.0.0.0 --port 8080 --identity blue
python3 fake_trino.py --host 0.0.0.0 --port 8081 --identity green
```

The fixture supports `GET /v1/info`, `GET /v1/cluster`, `POST /v1/statement`, `GET` continuation pages, `HEAD` heartbeats and query cancellation. It accepts a trailing slash on statement POSTs, as verified against real Trino. `START TRANSACTION` returns `X-Trino-Started-Transaction-Id`; `COMMIT` and `ROLLBACK` return `X-Trino-Clear-Transaction-Id`. An unknown transaction produces a Trino-style JSON query error with HTTP 200, which differs from Gateway rejection before forwarding.

Direct fixture controls are `GET /__test/state`, `POST /__test/reset`, `POST /__test/config`, `POST /__test/restart` and `POST /__test/release`. Each simulated process has a distinct `nodeId` and `coordinatorId`; query IDs use that coordinator suffix. Restart changes both identities and clears backend-local transactions and query results. It does not simulate process recovery or migrate a transaction.

Configuration supports `start_header_page` and `clear_header_page` (0 for the initial response, 1 for the continuation), `hold_start`, `hold_poll`, `drop_start_response`, `drop_poll_response`, `duplicate_start_headers` (`same` or `conflict`), `force_transaction_id`, `query_error`, `fail_commit`, `lowercase_headers`, `malformed_terminal`, `terminal_trailing_bytes`, `terminal_padding_bytes` and `initial_status`. The last option changes the response status after backend acceptance; it tests ambiguous errors rather than authentication failures. Release can select a barrier with `{"kind":"start"}` or `{"kind":"poll"}`; the default releases both. State records request methods, paths, SQL and transaction headers, but not Authorization. Do not put real credentials or data in fixture queries.

## Configuration

Register the two fixture backends in the same Gateway routing group. Use distinct Gateway process endpoints, not two URLs for one load-balanced service. At least two Gateway URLs are required; six can be supplied to exercise every replica. Backend health and database connectivity must work before running either suite.

| Variable | Meaning |
| --- | --- |
| `TX_ALLOW_FIXTURE_MUTATION` | Must be `yes`; explicit acknowledgement of test mutations. |
| `TX_GATEWAY_URLS` | Comma-separated URLs for at least two independent Gateway processes. |
| `TX_BACKEND_URLS` | Two directly reachable fixture URLs, in blue/green order. |
| `TX_BACKEND_PROXY_URLS` | Corresponding URLs registered with Gateway; defaults to the direct URLs. |
| `TX_BACKEND_NAMES` | Registered names; defaults to `blue,green`. |
| `TX_ROUTING_GROUP` | Shared fixture group; defaults to `transaction-test`. |
| `TX_ADMIN_TOKEN` | Required bearer token for feature-enabled admin calls. Omit it only for the feature-disabled upstream baseline. |
| `TX_QUERY_AUTHORIZATION` | Required query authorization header, supplied from disposable fixture credentials at runtime. There is no working default. |
| `TX_READINESS_TIMEOUT_SECONDS` | Backend health convergence deadline; defaults to 120 seconds for upstream health polling. |
| `TX_CA_FILE` | Optional private CA file for HTTPS fixture endpoints. Certificate and hostname verification remain enabled. |
| `TX_DATABASE_FAULT_URL` | Direct URL for the isolated TCP proxy's HTTP control port. |
| `TX_ALLOW_IRREVERSIBLE_FAULTS` | Must be `yes` for fault suites that intentionally leave uncertain obligations or restart a coordinator. |

Keep actual endpoint values, secrets and raw environment output outside the repository. For transaction-aware runs, enable the feature and configure the same randomly generated identity key on every Gateway. See the feature configuration documentation for the key requirements. Do not disable authentication binding to make tests pass.

The fixture setup must generate its credentials outside the repository. Tests use those runtime credentials for successful requests and generate different credentials for negative cases. Do not publish environment files or successful Authorization values.

## Run the suites independently

From this directory:

```sh
python3 -m unittest -v test_fake_trino
python3 -m unittest -v test_network_fault_proxy
python3 -m unittest -v test_gateway.BaselineControls
python3 -m unittest -v test_gateway.TransactionContract
python3 -m unittest -v test_adversarial
python3 -m unittest -v test_protocol_boundaries
python3 -m unittest -v test_reincarnation
```

The separate `test_cross_group` suite needs a third independent fake backend and a different routing group. Supply `TX_DESTINATION_BACKEND_URL`, `TX_DESTINATION_BACKEND_NAME`, and `TX_DESTINATION_ROUTING_GROUP` after registering that backend. It changes the routing-group input for new requests and verifies that existing transactions and result continuations keep their original owner. This simulates a placement decision; it does not test customer provisioning, catalog migration, or tenant authorization.

```sh
python3 -m unittest -v test_cross_group
python3 -m unittest -v test_capabilities
```

`test_capabilities` verifies advertised result URLs across replicas and rejects forged result and cancellation paths before backend dispatch.

Missing fixture configuration is an error, not a skipped test. Baseline controls must pass before interpreting contract failures. There are no expected-failure annotations. Record the tested Gateway commit, image, process count, database configuration and commands with each run. Keep private runtime details in an untracked receipt outside the repository.

## Contract

The tests exercise transaction affinity across backend cutover and every configured Gateway, ownership discovered on continuation pages, unknown and malformed IDs, duplicate headers, identity replay, completion, backend URL reuse, pending admissions, drain races and explicit seal rejection. New transactions can move to the new backend; existing transactions cannot migrate between coordinators.

Modern clients can advertise spooled-result encodings while still accepting inline results. The boundary test requires Gateway to remove that advertisement before forwarding and return a complete inline query result. The fake backend records the encoding header so an ignored advertisement cannot falsely pass this check.

The admin contract uses `/gateway/transactions/backends/{name}/drain`, `/seal`, `/resume`, and `/gateway/transactions/cutover`. Resume and seal carry the current `generation` from drain status. Cutover is removed with `DELETE /gateway/transactions/cutover/{routingGroup}` during cleanup. A drain snapshot is not shutdown authorization: `drained` is true only after an atomic seal succeeds.

With the feature enabled, the fixture changes placement through durable cutover and resume operations. Legacy activate/deactivate operations and active-flag changes must reject with HTTP 409. The upstream baseline uses those legacy operations because the durable API does not exist there. The backend-affinity assertions are the same in both modes.

Reusing a fixed backend URL requires routing away, draining and sealing its old incarnation before restarting its process. `POST /gateway/transactions/backends/{name}/reincarnate` receives the expected old `generation` and `incarnation`. The replacement starts in `DRAINING` and requires an explicit resume. Old query bindings remain attached to the sealed historical incarnation; they must never reach the new process.

Run each complete contract against a fresh fixture when collecting upstream failure evidence. Upstream cannot reconcile its missing ownership ledger; failed scenarios can leave backend-local transactions. The fixture does not pretend that direct backend cleanup reconciles an enabled Gateway ledger. A failed run must be diagnosed before reusing its state for drain assertions.

## Fault injection

Point every test Gateway's JDBC connection at the TCP proxy, not directly at PostgreSQL:

```sh
python3 network_fault_proxy.py --host 0.0.0.0 --port 15432 \
  --control-port 8080 --upstream-host postgres --upstream-port 5432
```

`POST /__test/config` with `{"available":false}` closes existing sockets and rejects new connections. `{"available":true}` permits new connections again. There is no bypass or fallback route. The tests force backend acceptance with an observable barrier before disabling database access, so response-recording failures are distinct from failures before admission.

Run irreversible fault tests after all normal suites, preferably with a fresh fixture per method. Database-outage and uncertain-response tests intentionally leave durable obligations. The restarted-coordinator test is last and intentionally does not resume or clean up the old incarnation. Discard the lab afterward; do not delete ledger rows to describe these cases as successfully drained.

```sh
python3 -m unittest -v test_faults.DatabaseFaultContract
python3 -m unittest -v test_partial_cancel
python3 -m unittest -v test_faults.FaultContract
```

These suites require explicit fault opt-in. Their assertions compare obligation counts before and after each injected fault. Existing uncertain rows cannot substitute for evidence that the new fault was tracked. A PostgreSQL outage affects every fixture Gateway, so no other suite may run concurrently against that lab.

`test_partial_cancel` uses the optional fake `partial_cancel` setting to advertise Trino's stage-cancellation URI shape. DELETE must remain on the original owner after cutover. A 204 response settles only that request, not its query or transaction. Unsupported-method checks retain uncertain admissions, so run this suite before the coordinator-restart test. It does not replace cancellation against real Trino.

The optional `duplicate_next_uri` setting emits two conflicting raw JSON fields: a URL followed by `null`. Rejection must preserve uncertainty instead of making the query appear complete.

## Limits of this suite

The fake server implements the protocol subset needed for deterministic routing tests. It does not execute SQL, implement connector transactions, enforce backend authentication or reproduce Trino memory loss. It is not a substitute for real-Trino tests using supported clients and transaction-capable catalogs.

Production acceptance also requires real coordinator restart/loss and Gateway restart between database commit and response delivery. The fake-process restart and TCP fault boundary do not replace those process-level experiments. Passing fixture self-tests only validates the test tools; the black-box assertions still need execution against the actual implementation. Never claim that an in-memory transaction survives the loss of its coordinator.
