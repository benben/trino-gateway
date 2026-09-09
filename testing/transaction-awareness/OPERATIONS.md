# Transaction-aware Gateway: validation and operation

This is an opt-in implementation under validation. Read the test receipts before
using any guarantee. The disposable lab is not a production deployment template.

## Supported configuration

All Gateway replicas must use the same PostgreSQL database and these settings:

```yaml
transactionAwareness:
  enabled: true
  identityKey: <shared-random-key-from-private-runtime-configuration>
  adminToken: <different-shared-random-token-from-private-runtime-configuration>
  terminalRetentionSeconds: 120
```

Each key must contain at least 32 UTF-8 bytes. Generate them outside source
control. Do not rotate them while transactions or retained queries remain.
The lab helper generates and preserves them. The admin endpoints also retain the
Gateway's existing API-role check and require this separate bearer token.

The initial identity mode requires stable HTTP Basic credentials and consistent
effective/original Trino users. Backend authentication and authorization remain
authoritative. Bearer refresh, browser sessions, external request-header rewrites,
and credential rotation during a transaction are not supported.

The Gateway removes the optional `X-Trino-Query-Data-Encoding` advertisement so
Trino returns inline results. Standard JDBC can consume this fallback without a
client setting change. Spooling lifecycle guarantees are not implemented.
Canonical protocol paths are required; alternate encoded, matrix-parameter,
dot-segment and repeated-slash path spellings are rejected. Query-string values
are not subject to this path restriction.

Result and cancellation paths are capabilities. Before exposing `nextUri` or
`partialCancelUri`, Gateway stores its path hash with query ownership in the same
database transaction. A continuation must match an advertised capability; a query
ID alone is insufficient. Metadata requests without a capability require the
owner's Basic credentials. These checks do not depend on a replica-local cache.

Existing backend registration still uses Gateway's normal API. Once enabled,
legacy activation/deactivation, destination changes, and deletion cannot bypass
the durable transaction controls. A new backend must expose a ready coordinator
with `nodeId` and `coordinatorId` in `/v1/info`.

## Planned cutover

These operations use the `/gateway/transactions` API prefix. Every administrative
request needs the configured bearer token. The coordinator's catalogs, data,
authorization and capacity must already be ready; Gateway does not provision them.

1. Prepare and verify the destination. If its durable state is DRAINING or SEALED,
   read `GET /backends/{name}/drain` and POST its observed `generation` to
   `/backends/{name}/resume`. Resume requires the same coordinator process.
2. POST `/cutover` with `routingGroup` and `backendName`. After this commits, new
   independent statements use that destination. Previously admitted statements
   and existing transactions retain their original backend.
3. POST `/backends/{old}/drain`. Poll GET on the same path. Existing transactions,
   result polling and heartbeats can continue; new independent statements cannot.
4. Wait for `readyToSeal: true`. This includes the configured terminal-result
   retry window, not merely a zero running-query count.
5. POST `/backends/{old}/seal` with the observed `generation`. Only a successful
   response with `drained: true` authorizes retirement under this protocol.
   A stale generation or new outstanding obligation prevents sealing.
6. Stop the old coordinator through the external deployment controller.

`readyToSeal` is an observation, not permission to stop a backend. Sealing shares
the admission lock and rejects later continuations to that incarnation.

To reuse a blue/green slot, first move every durable route away, seal it, then
replace its coordinator. POST `/backends/{name}/reincarnate` with the old
`incarnation` and `generation`. Gateway verifies a different process, preserves
historical bindings and creates a new DRAINING incarnation. Resume that returned
generation before making the slot a destination again. Old query IDs never bind
to the replacement. All feature-enabled replicas must understand incarnation
history before this operation; mixed unreleased V5/V6 prototypes are unsupported.

The V7 capability migration cannot reconstruct result URLs issued by earlier
unreleased prototypes. Finish those queries before upgrading all replicas to V7;
old result paths fail closed. This prototype upgrade restriction does not apply
to a V7-to-V7 Gateway restart, which preserves the database and shared keys.

## Failure boundaries

A coordinator owns its in-memory transactions. Gateway does not replicate them
or recover them after coordinator loss. Do not retry an ambiguous statement on
another coordinator. Ordinary cutover can coexist with a busy warehouse, but an
indefinitely open transaction can prevent finite, zero-abort retirement.

Transport loss, database failure after admission, cancellation and malformed or
contradictory responses can leave durable uncertainty. There is no automatic
expiry or reconciliation of these obligations in this implementation. They block
drain rather than being reported as successful completion. A human deleting such
state would be an explicit loss-of-guarantee action, not graceful draining; no
force-clear API is provided.

The terminal retry window defaults to 120 seconds and can be 1–86400 seconds.
Gateway retains affinity, not response bodies. It cannot prove receipt by the
client or guarantee result replay indefinitely. The original backend must remain
available during the window. State is retained conservatively; production data
retention and cleanup policy need a separate design before large-scale use.

The observed coordinator identifiers detect ordinary restarts, not adversarial
identity collisions or an atomic check-and-send boundary. Direct traffic that
bypasses these Gateways is outside the drain guarantee.
