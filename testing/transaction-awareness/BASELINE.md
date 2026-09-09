# Upstream regression checkpoint

Date: 2026-09-09. This is a deliberately failing regression checkpoint, not a completed implementation or production acceptance report.

The protocol fixture and black-box suite were introduced before any transaction-awareness Java changes. The tested application was the upstream Gateway 21 container. The lab used two separate Gateway processes, one shared PostgreSQL instance, and two independent fake Trino backends. Every resource and query was synthetic. Private runtime addresses and infrastructure details are intentionally omitted.

The source audit found no Java implementation or test changes between Gateway tag `21` and source commit `9b0e9038e87be1f600dc40d8c8bf1d09dd509491`. That source's existing six routing/proxy test classes also passed: 83 tests, zero failures, errors or skips. That Maven run skipped frontend and build-quality plugins; it was not a full upstream verification run.

## Executed black-box proof

Fixture self-tests: six passed. After the fixture's existing health-monitor configuration was shortened for test readiness, this combined command ran against both independent Gateway process endpoints:

```sh
python3 -m unittest -v \
  test_gateway.BaselineControls \
  test_gateway.TransactionContract.test_transaction_stays_on_owner_after_cutover \
  test_gateway.TransactionContract.test_unknown_id_is_rejected_before_backend
```

Result: five tests in 48.725 seconds; three baseline controls passed and both new contract assertions failed. No expected-failure annotations or skipped assertions were used.

| Assertion | Actual upstream result |
| --- | --- |
| Query and continuation across independent Gateway processes | Passed |
| Terminal poll replay through another Gateway process | Passed |
| Start, query and rollback without a backend switch | Passed |
| Transaction remains on its original backend after activation switches | Failed: a transaction created on blue reached green, which returned `UNKNOWN_TRANSACTION` |
| Unknown transaction ID is rejected before forwarding | Failed: Gateway forwarded the request and returned HTTP 200 with a query continuation |

An earlier baseline attempt had two setup failures because the test's 30-second readiness deadline was shorter than the upstream default health interval. Those were fixture-timing failures, not transaction regression evidence. The harness now avoids redundant activation and defaults to a 120-second readiness deadline. Baseline controls passed independently after that correction and again in the combined run above.

The complete initial contract then ran with `python3 -m unittest -v test_gateway.TransactionContract`: 18 tests in 194.100 seconds. All 18 failed their contract assertions; unittest reported 19 failure entries because the all-replica test failed separately on both replicas. There were no test errors or skips.

- Five tests exposed incorrect transaction affinity, including commit, rollback and a start header returned on a continuation page.
- Six tests exposed forwarded unknown, malformed, duplicated or differently owned transaction identities.
- One test showed that upstream accepted a live backend URL replacement instead of rejecting it.
- Six tests failed because the required drain, seal or atomic-cutover API was absent (HTTP 404).

The missing-API failures establish incomplete feature coverage; the misrouting and forwarding failures establish the existing behavioral gap. All baseline and initial contract tests completed before transaction-awareness Java implementation began.

## Expanded upstream checks

After the initial checkpoint, 20 additional checks ran with `python3 -m unittest -v test_adversarial`. Gateway connections used HTTPS with certificate and hostname verification against the lab CA. The application remained upstream Gateway 21; PostgreSQL remained available throughout this run.

Result: 20 tests in 296.597 seconds; 20 assertion failures, zero errors or skips. Thirteen failures concerned identity/context forwarding, continuation ownership, closed-start replay or lowercase-header affinity. Seven concerned missing durable-state or drain APIs. These additional tests were authored during implementation review; only the initial checkpoint above is the pre-implementation gate.

A separate real-Trino check also confirmed that `POST /v1/statement/` accepts a trailing slash: a synthetic `SELECT 1` returned HTTP 200 and completed across five response pages. Therefore, exact-path-only Gateway accounting would leave a real protocol bypass. Source inspection confirmed that Trino's client sends `HEAD` heartbeats and its executing-statement resource returns an empty successful response. Heartbeats need accounting without terminal-query inference; they are not an unsupported method.

## Interpretation

Real Trino 483 over verified HTTPS also reproduced the affinity failure. Two
baseline controls passed. The five-test HTTP run had two assertion failures:
an idle transaction and an active transaction reached the replacement coordinator
and returned `UNKNOWN_TRANSACTION`. The third cutover check, rollback after an
error, initially passed because Trino can report rollback success for an unknown
transaction. That check now also asserts the original coordinator's query-ID
suffix; success alone was an inadequate oracle.

The unmodified JDBC 483 client passed its baseline through both Gateway replicas,
without continuation-URL rewriting or encoding overrides. Its upstream cutover
regression then failed with `UNKNOWN_TRANSACTION` after fresh connections had
verified the replacement coordinator's exact node ID. This independently confirms
the problem with a native client rather than only the Python protocol harness.

This proves that existing query-ID routing works in the lab while the two new transaction guarantees fail on upstream. It does not prove the later implementation, real-client compatibility, fault recovery or safe drain. The full acceptance matrix requires additional positive tests after implementation, including real Trino processes and deliberately forced fault interleavings.
