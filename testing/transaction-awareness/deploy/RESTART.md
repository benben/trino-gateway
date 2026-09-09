# Gateway process restart regression

Run this only during an exclusive test window. It replaces **all** Gateway pods,
so every other lab client and port-forward will be interrupted. It never scales
the Deployment, replaces PostgreSQL, or restarts Trino.

The test starts a real transaction and verifies its feature-managed ownership.
It invokes the existing artifact helper, requires every Gateway pod UID to
change, and checks that PostgreSQL and Trino pod UIDs and restart counts remain
unchanged. It also compares Secret data hashes without printing their contents.
After reopening its own loopback forwards, it verifies the transaction through
every Gateway and commits through the last replica. Fresh queries must still
reach the original configured coordinator.
It also leaves one real query's advertised continuation unconsumed before the
replacement. Afterward, that query must deliver its expected result through a
new Gateway process. This checks persisted query ownership and capabilities,
not only the transaction binding used by subsequent statements.

Export the private `TX_TRINO_USER`, `TX_TRINO_PASSWORD`, `TX_ADMIN_TOKEN`, and
`TX_CA_FILE` settings. The transaction ID remains only in process memory.

```sh
export TX_ALLOW_FIXTURE_MUTATION=yes
python3 testing/transaction-awareness/deploy/restart_gateways.py \
  --context <development-context> \
  --namespace gateway-tx-lab-<unique-id> \
  --confirm-restart gateway-tx-lab-<unique-id> \
  --jar <local-feature-jar>
```

The test requires a feature-enabled Gateway, at least two ready replicas, an
existing real-backend route, and unused local ports starting at 19081. Use
`--base-port` to choose another range. It creates and closes only its own
port-forward processes. Reopen other lab forwards afterwards.

This proves retained ownership across Gateway process replacement, not request
availability while all replicas are down. It does not recover a transaction if
its Trino coordinator restarts or its transaction timeout expires. Slow artifact
uploads can exceed that timeout and correctly fail the test. Failed runs report
whether transaction cleanup could be attempted; inspect the private lab before
reusing it.

The harness accepts the current replica count, including six, without changing
it. Six replicas fit the fixture's resource quota: 5.5 CPUs, 12.8125 GiB, and 12
pods versus limits of 8 CPUs, 16 GiB, and 20 pods. This calculation does not prove
available node capacity. Check capacity before any separately authorized scale
operation. Uploads run in pairs; copying to six replicas may require a longer explicitly
configured test transaction timeout; this harness does not change that timeout.
