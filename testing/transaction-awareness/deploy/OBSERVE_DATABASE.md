# Read-only database connection observer

This optional test harness samples one dedicated benchmark database through one
persistent `psql` connection. It does not create database objects or change Aurora
configuration. The renderer creates only a ConfigMap and a bounded Job in an
explicit disposable namespace. Applying them requires separate authorization.

Prerequisites are a namespace-local Secret containing `PGHOST`, `PGPORT`,
`PGDATABASE`, `PGUSER`, and `PGPASSWORD`; a public CA ConfigMap with `ca.pem`;
and the existing task-owned, negative, non-default, non-preempting PriorityClass.
Read that exact class into a private JSON file and pass it to the renderer. The
Job overrides TLS, application name, connection timeout, and read-only SQL
timeouts. It has no service account token, retries, or cluster permissions.

```sh
python3 observe_database.py render \
  --namespace gateway-tx-lab-example --name metrics-example \
  --database-secret metrics-database --ca-configmap database-ca \
  --priority-class-file /private/tmp/approved-priority-class.json --samples 360
```

Capture the approved Job's logs to a private JSON-lines file. Require successful
Job completion and the expected total sample count before relying on its receipt.
The summarizer never fills missing samples with zero:

```sh
python3 observe_database.py summarize /private/tmp/observer.jsonl --expected-samples 360
python3 observe_database.py summarize /private/tmp/observer.jsonl \
  --start 2026-01-01T00:00:00Z --end 2026-01-01T00:05:00Z
```

Start observation before the measured load window and retain samples beyond its
end. Window selection is start-inclusive and end-exclusive. The full receipt
must pass count validation before selecting a smaller measurement window.
Reconnects, counter resets, decreasing counters, and sampling gaps over two
seconds invalidate a summary. Job failure or truncated logs invalidate the run
even if a selected subset appears complete.

Connection counts cover client backends in the connected database, excluding
the observer application name and its own PID. They include other clients, not
only Gateway pools. No SQL text, usernames, database names, or hostnames appear in
samples. State and wait-type aggregates may contain `unknown` when the database
user cannot see another session's state. The PID identifies only the observer
session, so a reconnect can be detected.

Min/max/average are statistics of the observed approximately one-second samples,
not proof of instantaneous peaks. SQL execution time can delay sampling. Database
counter deltas span the first through last selected sample, not unsampled window
edges. These database-wide counters include the observer's own read transactions
and background activity. They are neither Gateway request counts nor billable
storage I/O. Use CloudWatch volume I/O metrics for billing estimates.

Offline tests run with `python3 -m unittest test_observe_database`. An additional
integration test requires an explicitly owned, otherwise idle local PostgreSQL
fixture and normal `PG*` connection variables:

```sh
TX_OBSERVER_LOCAL_PG=yes python3 -m unittest test_observe_database
```

That test checks three JSON samples from one observer PID while a synthetic client
holds a connection. Its TLS disablement applies only to that local test, not the
rendered Job. The observer remains read-only throughout.
