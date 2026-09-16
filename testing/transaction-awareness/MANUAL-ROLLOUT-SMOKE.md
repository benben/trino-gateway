# Manual deployment and cutover checks

These scripts exercise an operator-prepared development backend pair. They do
not implement Kargo promotion, publication, warehouse provisioning, roster
certification, Kubernetes scaling, or a deployment controller. Passing them does
not establish that an automated rollout performs those operations correctly.

Keep credentials, deployment-specific arguments, and all results outside this
public repository. Supply passwords through process environment variables; do
not put credentials in command-line arguments or shell history.

## Gateway restart workload

`rollout_workload.py` starts three read-only transactions, retains a paginated
result, and issues concurrent autocommit queries. Set `TX_TRINO_PASSWORD` and
pass `--server`, `--user`, `--catalog`, and `--group`. Wait for the JSON event
`workload_ready_for_rollout` before independently initiating a Gateway restart.
Check that all intended Gateway replicas were replaced while the transactions
were open. The client does not identify which replica handles each request.

### Operation accounting

Each logical statement receives a unique operation identifier. This includes
transaction BEGIN, each transaction read, COMMIT, ROLLBACK, cleanup ROLLBACK,
retained-result queries, and offered autocommit queries. Statement success requires
the expected rows and applicable transaction identity, owner, and terminal-state
checks. Opening a transaction or retaining a result does not prove that expensive
query computation remains active during a deployment.
Independently verify that transactions and retained results overlap the actual
deployment window. The readiness event records one observation, not continuous
overlap. This workload does not yet test a long-running CPU query.

The summary reports `submitted = succeeded + failed + unresolved` and
`offered = submitted + not_submitted_capacity + not_submitted_stopped + not_submitted_statement_budget + not_submitted_pending`.
Submitted means the client invoked the logical statement, not that Trino accepted
or executed it. Failed means the client observed a failure; backend execution can
still be uncertain. Unresolved means an invoked operation has no recorded client
outcome. Pending means an offered operation has not reached the client invocation.
Capacity drops remain explicit failures of workload coverage.

The first failure or capacity drop requests a graceful stop. The workload stops
offering new autocommit work, skips already offered statements that have not
started, releases the retained-result pause, and attempts to roll back open transactions.
Already running requests keep their existing deadlines. Releasing the pause
continues normal result polling; it does not retry a failed request or replay SQL.
Cleanup failures retain their ordinary failure and private-recovery reporting.

Ctrl+C requests the same graceful stop. Wait for the final summary before exiting
the process. The summary separates planned autocommit arrivals that were never
offered from offered statements stopped before submission. It also reports aborted
transactions and threads that never started. A stopped run always exits nonzero,
including a user interruption with no query failure. This procedure does not
certify that failed queries stopped executing or remove Gateway ledger records.

Zero-error acceptance requires both equations, at least one submitted operation,
no failures, unresolved operations, capacity drops, stopped, budget-skipped or pending offers, and the
existing transaction, retained-result, and rollout-readiness checks. HTTP page
counts are separate from logical-statement counts. Operation events contain no
SQL text, credentials, result rows, or continuation capabilities.
Successful operations also emit their validated Trino query identifier, page count,
and row count for reconciliation with the Gateway ledger. Keep all runtime output
private and outside this repository. Failed continuations retain their original
operation identifier alongside the opt-in private recovery handle.

The accounting helper supports a separately identified explicit continuation
resumption linked to its failed original operation. The workload does not perform
automatic resumption. A successful resumption never changes the original failure
or makes the original run satisfy zero-error acceptance.

### Optional statement budget

Set `--max-concurrent-statements` to a positive integer when the warehouse has a
limited active-query budget. This optional limit covers every complete statement,
from initial submission through its final result page, including BEGIN, reads,
COMMIT, ROLLBACK and cleanup. A retained result holds its slot during the pause.
An idle open transaction does not hold a slot between statements. Three open
transactions plus a retained result therefore do not require four active statements.
Without this option, the existing concurrency behavior remains unchanged.

Offered operations wait in FIFO order before submission. `operation_client_wait`
events report this delay separately from query latency. The arrival schedule still
offers autocommit work at the requested rate; the existing 16-task bound remains.
If that bound fills, the run records the dropped offer and stops. The budget does
not turn an excessive offered rate into a successful lower-throughput test.
Choose a rate the warehouse can sustain and verify the readiness event before
starting a rollout. This client limit does not reserve capacity against other clients.

A failed dispatched statement retains its slot because backend execution may
continue. The workload stops ordinary admissions before another waiter can use
that slot. Running peers may finish. Cleanup uses only immediately available safe
slots; otherwise it records `not_submitted_statement_budget`, without waiting
indefinitely or exceeding the configured limit. The summary includes remaining
occupied slots, uncertain statements, and `transactions_left_open` when applicable.
Neither a failed query nor a skipped cleanup counts as success.

If a known transaction remains open, `transaction_recovery_required` reports its
workload index without exposing the transaction ID. With `--recovery-directory`,
it saves a mode-0600 `rollout-open-transaction-v1` record containing the known ID
and connection-context hash. This is an operator recovery record, not a query
continuation file; do not pass it to `--resume-file`. No transaction ID is invented
when the initial response was lost. Without private recovery persistence, known
transaction IDs disappear when the process exits. Recovery still requires the
original connection identity and credentials, supplied separately by the operator.

### Failed continuations

The client preserves the exact last validated continuation in memory when a
request fails. It does not retry requests, invent continuation tokens, replay
statement POSTs, or change a failed run into a pass. Failure output contains an
opaque recovery identifier and bounded HTTP classifications, not response bodies,
credentials, SQL, or continuation capabilities. An unsuccessful initial POST has
no recovery handle unless the client actually received and validated a next URI.

Handles disappear when the process exits unless the operator explicitly passes
`--recovery-directory` to either rollout script. Create an owned mode-0700 directory
outside this repository first. The scripts create unique mode-0600 JSON files
there and never overwrite an existing file. These files contain sensitive next-URI
capabilities and transaction identifiers, but no passwords, Authorization headers,
SQL text, or result rows. Do not commit or share them. Remove them after recovery.

For an autocommit query, explicitly resume its original GET with:

```sh
python3 rollout_client.py --server "$GATEWAY_URL" --user "$TRINO_USER" \
  --catalog "$TRINO_CATALOG" --group "$ROUTING_GROUP" \
  --resume-file "$PRIVATE_RECOVERY_FILE"
```

Supply the same server, user, catalog, and routing group as the failed run. The
client prompts for a password again. A resumed result contains only rows obtained
after the saved handle, plus a count of earlier rows. It does not reconstruct or
revalidate the full original result. The original benchmark failure remains a
failure even if resumption succeeds. Retrying the same saved handle later can
repeat result pages; this is not an exactly-once result consumer.

Transaction handles can resume only in memory on a client that still holds the
same transaction identity. The CLI cannot recreate that session from a file.
Workload cleanup attempts ROLLBACK after failures when its statement budget permits,
which can invalidate transaction
continuations. Expired Trino results, a lost coordinator, or a missing initial
response may make recovery impossible. These helpers do not clear Gateway ledger
records or certify that an abandoned query has stopped executing.

## Manual fenced cutover

Before running `manual_cutover_smoke.py`, independently verify:

- Catalog writers are paused and no catalog mutation is in flight.
- Both colors share the intended catalog store and expose the complete admitted
  warehouse roster. Both coordinators and their workers are ready.
- Green has sufficient capacity and both backend incarnations are registered
  as `ACTIVE` with current process identities.
- The durable route points to blue. No deployment controller or other operator
  will mutate the route, backend state, or coordinator processes during this test.
- No active Gateway rollout operation owns the routing group.

Set `TX_TRINO_PASSWORD`, `TX_ADMIN_USER`, `TX_ADMIN_PASSWORD`, and `TX_ADMIN_TOKEN`.
Pass `--server`, `--user`, `--catalog`, `--group`, `--blue`, `--green`, and
`--allow-route-mutation`. There are no deployment-specific defaults.

The script opens a transaction and retains a result on blue, switches new
queries to green with route-generation and incarnation fences, and verifies
that old work remains on blue. It checks that sealing fails while work remains,
finishes the transaction, drains and seals blue, resumes the same blue process,
and switches new queries back to blue. It then drains and seals green. Each
drain wait defaults to 300 seconds to allow terminal-result retention to expire.
The intended final state is blue routed and active, green sealed. The script
does not scale green down.

Requests are not retried. A failed or ambiguous administrative operation stops
further administrative mutations. The script attempts transaction rollback and
reads final state, but never automatically changes the route to undo a failure.
Do not scale or delete either coordinator after a failure without inspecting its
current state and outstanding work. A successful run tests a manual API sequence,
not the unfinished Kargo integration.

Run local unit checks with:

```sh
python3 -m unittest -v test_rollout_client test_rollout_workload test_statement_budget test_manual_cutover_smoke
```
