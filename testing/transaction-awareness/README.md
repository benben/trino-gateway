# Transaction-awareness integration tests

This directory contains a Python standard-library black-box test suite and a protocol-focused Trino test double. Python 3.9 or newer is sufficient. The suite deliberately separates baseline controls from the new transaction-awareness contract. Contract failures against upstream are regression evidence, not a successful implementation.

All endpoints must belong to an isolated, disposable test environment. The suite changes backend activation, routing and drain state. It must never target customer services. Fixture control endpoints have no authentication and must remain restricted to the test network.

## Components

- `fake_trino.py`: independent backend-local transactions, standard statement and continuation responses, transaction lifecycle headers, replayable result pages and controllable response barriers.
- `protocol.py`: HTTP client that preserves repeated headers and can send successive requests through different Gateway processes.
- `test_fake_trino.py`: local fixture self-tests. These establish fixture behavior, not Gateway correctness.
- `test_gateway.py`: baseline controls and transaction contract assertions against separate Gateway processes sharing one PostgreSQL database.
- `deploy/`: environment setup and runtime configuration, maintained separately from the protocol suite.

Start two fake backends with different identities:

```sh
python3 fake_trino.py --host 0.0.0.0 --port 8080 --identity blue
python3 fake_trino.py --host 0.0.0.0 --port 8081 --identity green
```

The fixture supports `GET /v1/info`, `GET /v1/cluster`, `POST /v1/statement`, `GET` continuation pages and query cancellation. `START TRANSACTION` returns `X-Trino-Started-Transaction-Id`; `COMMIT` and `ROLLBACK` return `X-Trino-Clear-Transaction-Id`. An unknown transaction produces a Trino-style JSON query error with HTTP 200, which differs from Gateway rejection before forwarding.

Direct fixture controls are `GET /__test/state`, `POST /__test/reset`, `POST /__test/config` and `POST /__test/release`. Configuration supports `start_header_page` (0 for the initial response, 1 for the continuation) and `hold_start` (pause before the start response). State records request methods, paths, SQL and transaction headers. Do not put real credentials or data in fixture queries.

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
| `TX_ADMIN_TOKEN` | Optional API-role bearer token for admin calls. |
| `TX_QUERY_AUTHORIZATION` | Query authorization header; defaults to synthetic Basic credentials `user:test-password`. |

Keep actual endpoint values, secrets and raw environment output outside the repository. For transaction-aware runs, enable the feature and configure the same randomly generated identity key on every Gateway. See the feature configuration documentation for the key requirements. Do not disable authentication binding to make tests pass.

## Run the suites independently

From this directory:

```sh
python3 -m unittest -v test_fake_trino
python3 -m unittest -v test_gateway.BaselineControls
python3 -m unittest -v test_gateway.TransactionContract
```

Missing fixture configuration is an error, not a skipped test. Baseline controls must pass before interpreting contract failures. There are no expected-failure annotations. Record the tested Gateway commit, image, process count, database configuration and commands with each run. Keep private runtime details in an untracked receipt outside the repository.

## Contract

The tests exercise transaction affinity across backend cutover and every configured Gateway, ownership discovered on continuation pages, unknown and malformed IDs, duplicate headers, identity replay, completion, backend URL reuse, pending admissions, drain races and explicit seal rejection. New transactions can move to the new backend; existing transactions cannot migrate between coordinators.

The admin contract uses `/gateway/transactions/backends/{name}/drain`, `/seal`, `/resume`, and `/gateway/transactions/cutover`. Resume and seal carry the current `generation` from drain status. Cutover is removed with `DELETE /gateway/transactions/cutover/{routingGroup}` during cleanup. A drain snapshot is not shutdown authorization: `drained` is true only after an atomic seal succeeds.

Run each complete contract against a fresh fixture when collecting upstream failure evidence. Upstream cannot reconcile its missing ownership ledger; failed scenarios can leave backend-local transactions. The fixture does not pretend that direct backend cleanup reconciles an enabled Gateway ledger. A failed run must be diagnosed before reusing its state for drain assertions.

## Limits of this suite

The fake server implements the protocol subset needed for deterministic routing tests. It does not execute SQL, implement connector transactions, enforce backend authentication or reproduce Trino memory loss. It is not a substitute for real-Trino tests using supported clients and transaction-capable catalogs.

Production acceptance also requires real coordinator restart/loss, Gateway restart between database commit and response delivery, PostgreSQL faults, lost responses, cancellation ambiguity, terminal-result retry windows and seal-versus-continuation races. These scenarios must use actual fault injection; passing mock or fixture-only tests does not establish those guarantees. Never claim that an in-memory transaction survives the loss of its coordinator.
