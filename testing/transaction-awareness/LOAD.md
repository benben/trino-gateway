# Bounded load and deadline regressions

Use only an explicitly authorized disposable lab. Keep endpoints, credentials,
generated manifests, and raw results outside this public repository.

`test_overload.OverloadContract` holds sixteen accepted continuations on one
Gateway. The seventeenth must receive `503` with `Retry-After` before backend
dispatch. It releases and consumes all held pages. `TX_EXPECT_MAX_IN_FLIGHT`
can select an intentionally different configured limit.

`test_overload.DeadlineContract` requires the existing
`routing.asyncTimeout: 500ms` and `TX_EXPECT_ROUTING_TIMEOUT_MS=500`. It holds
the process-identity response before statement forwarding, checks a bounded
failure, rejects late dispatch, and checks lease recovery. Use separate backend
identities: an old implementation can accept a query after the client stops
waiting. Preserve that unresolved ledger as evidence.

## Open-loop measurement

`load_open_loop.py` schedules aggregate **HTTP requests**, not SQL queries.
Each arrival submits `SELECT 1` or consumes an advertised continuation. A
two-response fake query normally needs two HTTP requests. SQL counts and
post-window cleanup are separate. The schedule does not wait for earlier work.

The client reuses verified connections, never retries uncertain requests, caps
in-flight work, and drops arrivals that exceed capacity or scheduling lag.
Receipts include offered/achieved/successful HTTP rates, response and scheduled
p50/p95/p99, errors, scheduling lag, dropped arrivals, CPU time, and outstanding
continuations. `503` responses are not successful target throughput. Client or
fixture saturation invalidates a server-capacity conclusion.

`window_start_utc` and `window_end_utc` identify the scheduled measurement
window as ISO 8601 UTC timestamps. One wall-clock sample anchors the monotonic
schedule; subsequent wall-clock adjustments do not change its end timestamp.
Warmup has a separate nested receipt. Cleanup traffic and requests completed
after the window do not enter its `second_buckets` or `request_rates`.
Existing overall error counts still include cleanup failures and invalidate
the run when appropriate.

Each second bucket records its offset, duration, and HTTP starts, completions,
successful completions, and failed completions. Empty buckets contain zeros.
The final bucket can be shorter than one second. Rate minima and maxima use
each bucket's actual duration; average rates divide total counts by the full
window duration. These rates count POSTs and continuation GETs together, not
SQL queries. Existing per-Gateway method counters retain their separate POST
and GET totals, including separately named cleanup counters.

For aligned cost measurements, `--measurement-start-utc '<ISO-8601-Z-time>'`
waits after warmup before starting the measurement. The start must be at most
600 seconds away when warmup finishes. The client rejects starts missed by more
than one second, including an excessive delay while waiting. Receipts preserve
the actual window and add `planned_window_start_utc` and
`measurement_start_drift_seconds`; both are null when no start was requested.
These timestamps do not establish clock synchronization with the database.

```sh
export TX_ALLOW_FIXTURE_MUTATION=yes
export TX_GATEWAY_URLS='<independent-gateway-https-endpoints>'
export TX_CA_FILE='<private-lab-ca-file>'
export TX_QUERY_AUTHORIZATION='<runtime-only-query-authorization>'
export TX_LOAD_ROUTING_GROUPS='<comma-separated-groups>'
export TX_LOAD_EXPECTED_BACKENDS='<JSON-group-to-fixture-identity-map>'
python3 testing/transaction-awareness/load_open_loop.py \
  --rate 1000 --warmup 10 --duration 60 --concurrency 128 \
  --output '<new-private-receipt-file>'
```

For capacity results, use the in-cluster Job from `deploy/render_load_job.py`,
not a local port-forward bottleneck. Address every Gateway pod separately.
`TX_TLS_SERVER_NAME=gateway` verifies the lab service certificate while
connecting to pod addresses; verification remains enabled. Jobs have 2 CPU/
2 GiB, no API credentials, no retries, no preemption, and finite deadlines.
They generate synthetic query credentials inside the process and print metrics
for private capture. Fake backends do not authenticate these credentials; the
separate real-Trino suites establish authentication and client compatibility.

`load_checkpoints.Checkpoints` holds a transaction during measurement. Afterwards
it proves that transaction blocks sealing, switches the group, checks existing
transaction and new-query placement through every Gateway, rolls back, and
seals only after observed obligations and retention end. A failure preserves
evidence. These post-load checks complement, but do not replace, concurrent
cutover/race tests. The timed throughput window measures steady-state routing.

## Matrix and safety

Compare identical database/client/payload/resources between baseline and fixed
images. Record image digest, engine/version, actual database capacity, replicas,
fixture/client resources, and errors. Run ten-second warmups and sixty-second
measurements at 100 and 1000 HTTP requests/second for 2, 20, and 100 Gateways.
Compare a hot backend with two independent backends in distinct groups; two
groups do not establish one-hundred-cell capacity. Repeat successful highest-
replica 1000-request/second cases for five minutes. Report unmet targets honestly.
Measure Gateway, client, fixture, and database CPU/memory. Local PostgreSQL is
not Aurora evidence; configured maximum ACUs are not measured headroom.

The monitor runs once per second in both versions. One hundred Gateways with
two backends add about two hundred monitor probes per second, beyond measured
client requests and request-time process checks. Include this background traffic
when interpreting fixture CPU; keep cadence identical between versions.

Create with `--without-real-trino --benchmark --gateway-image <verified-digest> --priority-class <approved-test-class>`.
The existing PriorityClass must have the task ownership label, a negative priority,
`preemptionPolicy: Never`, and `globalDefault: false`. Obtain explicit approval before
creating this cluster-scoped test resource. The helper never adopts another application's class.
Pass the verified class object as `priority_class` to `render_job` and database setup.
Setting the pod's preemption policy alone does not override priority admission.
Benchmark fixtures request 1 CPU/512 MiB. Review eligible node CPU, memory,
pod-IP capacity, and quota before external database setup or replica increases.

```sh
python3 testing/transaction-awareness/deploy/lab.py \
  --context '<authorized-development-context>' \
  --namespace 'gateway-tx-lab-<unique-id>' configure-load \
  --database-config '<private-database-json>' --database-ca '<public-ca-file>' \
  --database-address '<private-database-ip>' --replicas 2 --capacity-reviewed \
  --priority-class '<approved-test-class>'
```

Database JSON contains only `jdbcUrl`, `user`, `password`, and `driver`.
Require explicit authorization for credential transfer. JDBC requires
`sslmode=verify-full&sslrootcert=/etc/database-ca/ca.pem`. Only Gateway pods get
egress to the supplied private addresses and JDBC port. The quota is bounded
at 60 CPU/140 GiB/120 pods; only the selected replicas start. Scale one phase
at a time, wait for every pod, and scale down after high-replica measurements.
Keep failed-case evidence; use new schemas/identities when prior cases leave
uncertainty, never purge unresolved state to pass.

```sh
cd testing/transaction-awareness
python3 -m unittest test_load_open_loop test_fake_trino test_network_fault_proxy -v
python3 -m unittest discover -s deploy -p 'test_*.py' -v
```
