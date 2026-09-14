# Principal-based warehouse routing

Principal routing removes the need for clients to select a routing group.
Clients supply their usual Basic username and password at one Gateway endpoint.
The control plane owns the principal-to-group assignment; Gateway selects a
healthy backend in that group. This feature is disabled by default.

```yaml
routing:
  defaultRoutingGroup: unassigned
  principalRouting:
    enabled: true
    url: https://control-plane.example.test/api/v1/trino/routing-snapshot
    token: ${ENV:ROUTING_SNAPSHOT_TOKEN}
    refreshIntervalSeconds: 5
    maxStaleSeconds: 15
    requestTimeoutMillis: 2000
    maxEntries: 100000
```

The service token is a read-only routing/discovery credential, never the control
plane's administrative credential. Use HTTPS or an explicitly protected internal
network. Gateway sends only that credential in `X-Duckgres-Internal-Secret` and
an `Accept` header. It never forwards customer credentials to the snapshot
service and never follows redirects. Configuration serialization excludes the
token. Treat the snapshot response as internal topology, not a public endpoint.

The endpoint returns one complete, freshly read snapshot:

```json
{"routes":[{"principal":"warehouse_a","routingGroup":"cell-a"}]}
```

Only enabled, ready warehouses with a known configured assignment belong in the
response. Unknown, disabled and unassigned warehouses have no entry. An empty
array withdraws every assignment. Duplicate principals, invalid identities,
invalid JSON, more than `maxEntries` entries or more than 8 MiB invalidate the
whole response. The service must not repeatedly return an old snapshot as a new
successful response: Gateway measures freshness from its successful fetch, not
from a source database revision. Registered groups must exist in Gateway's
backend configuration; a missing or unhealthy assigned group fails closed,
including when a default group has healthy backends.

## Authentication and compatibility

The Basic username is a **routing hint, not an authenticated identity**. Gateway
does not check the warehouse password. The assigned coordinator authenticates
the original credentials and applies its authorization policy. Never enable
this mode against coordinators that permit unauthenticated requests or tenant
impersonation. Changing `X-Trino-User` does not change the assigned group and
must still fail the coordinator's impersonation check.

Caller-supplied `X-Trino-Routing-Group` values are ignored, including a previously
used correct value. Gateway cookies cannot override the assignment for new
requests. Missing or malformed Basic credentials receive a 401 challenge;
duplicate Authorization headers receive 400. An absent principal receives a
generic 403 without disclosing a group or backend. This is not a credential
verification service: response status can reveal whether a supplied principal
has an eligible routing entry. Coordinators remain the authentication boundary.

Other routing modes retain their existing behavior when principal routing is
disabled. Principal routing takes precedence over file and external routing
rules when enabled; invalid principal-routing configuration fails startup,
never falls back to caller headers.

## Freshness and failure behavior

Each Gateway independently refreshes an immutable snapshot in one background
thread, with randomized initial delay. Queries perform an in-memory map lookup;
neither the control plane nor its database is called per query. Responses replace
the snapshot atomically, including deletions. Failed, timed-out or invalid
refreshes retain the old snapshot's original expiry. New requests fail with 503
before the first successful refresh or once the snapshot reaches its maximum
age. Successful updates can take one refresh interval plus fetch time to appear.
Membership revocation is consequently bounded but not instantaneous.

Defaults permit at most 15 seconds of snapshot reuse, with a 5-second refresh
delay and 2-second request timeout. The delay starts after each refresh finishes.
The configurable entry cap cannot exceed 100,000; this is a defensive bound,
not a claim of tested warehouse scale. At 100 Gateway replicas and a 5-second
delay, the upper steady-state polling rate is approximately 20 exports per
second, independent of SQL request rate. Export work and bandwidth still grow
with the snapshot size: a full 8 MiB response at that rate is about 160 MiB/s.
Measure actual fleet size before changing these bounds.

Existing query continuations and cancellations retain their recorded backend.
With transaction awareness enabled, established transactions also retain their
durable owner, even after snapshot deletion, reassignment or expiration. The
snapshot selects **new admissions only**; it does not migrate transactions.
Without transaction awareness, this feature does not add transaction affinity
or safe blue/green cutovers. Enable transaction awareness separately using its
documented fleet-wide rollout requirements. A coordinator loss can still end
its queries and transactions.

## Verification

`TestPrincipalRoutingGroupSelector` covers snapshot replacement, expiry,
malformed input, credential parsing and bounded HTTP transport.
`TestAuthoritativeRoutingTargetHandler` covers cookie and default-group bypass
regressions. The local black-box fixture launches two actual Gateway processes
against one disposable PostgreSQL database and authenticated protocol test
backends, with transaction awareness both disabled and enabled:

```sh
./mvnw -pl gateway-ha -am test-compile -DskipTests -Dskip.pnpm -Dskip.installnodepnpm
./mvnw -pl gateway-ha dependency:build-classpath -Dmdep.outputFile=/tmp/gateway-test-classpath
export GATEWAY_TEST_CLASSPATH="$PWD/gateway-ha/target/classes:$(cat /tmp/gateway-test-classpath)"
export GATEWAY_TEST_JAVA="$(command -v java)"
export GATEWAY_TEST_PG_BIN="$(pg_config --bindir)"
cd testing/transaction-awareness
python3 -m unittest -v test_principal_routing
```

The fixture generates disposable credentials and removes its processes and
database afterward. It uses loopback interfaces only. It verifies Gateway
protocol and routing behavior, not real Trino authorization or Duckgres catalog
provisioning. Validate the integrated control plane and real Trino separately
before enabling this feature in a customer environment. Keep runtime reports
and deployment outcomes outside this public repository.

For an already configured candidate Gateway and real coordinator, the bounded
`probe_real_principal_routing.py` client performs only read-only SQL. Supply
`REAL_GATEWAY_URLS` (comma-separated candidate URLs), `REAL_TRINO_PRINCIPAL`,
`REAL_TRINO_CATALOG` and the optional `TX_CA_FILE` trust bundle. It prompts for
the password without saving it. The probe follows advertised result URLs
without rewriting them and checks that they remain on the candidate Gateway.
It also checks an ignored client group and a read-only transaction followed by
rollback. Use only an explicitly authorized test warehouse.
