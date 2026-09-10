# Disposable Kubernetes lab

This lab runs two independent Gateway processes sharing PostgreSQL, two stock
Trino single-node coordinators, and two controllable Python backends. It is for
synthetic development tests, not customer traffic. The initial Gateway image is
upstream version 21. Trino is version 483.

Use `--gateway-image` with a verified digest for a reproducible fork baseline.
The bounded performance workflow and external database safety requirements are
in [../LOAD.md](../LOAD.md).

The namespace has restricted pod security, no mounted service-account tokens,
no host privileges, and no public Ingress or load balancer. NetworkPolicy permits
only same-namespace test traffic and cluster DNS. Resources request 3.5 CPUs and
8.8125 GiB total; a quota caps requests and limits at 8 CPUs, 16 GiB, and 20 pods.
PostgreSQL uses `emptyDir`: pod replacement loses all test state.

For a separate fault-testing namespace, pass `--without-real-trino` to both
`create` and `forward`. This retains two Gateways, PostgreSQL, the fault proxy,
and two controlled backends, but omits both real coordinators. It requests
1.5 CPUs and 2.8125 GiB across six pods. Give each lab its own namespace,
runtime directory, and loopback port range. Never share its database or keys.

For a controlled backend in another routing group, repeat `--extra-fixture`
on `create` and `forward`, for example `--extra-fixture cell-two`. Each additional
fixture requests 50 millicores and 64 MiB, reuses only the fixture script
ConfigMap, and has an independent process identity and Service. Register its
backend metadata in the intended routing group separately. Extra forwards
follow the fault-proxy port and appear in `TX_EXTRA_BACKEND_URLS`.

The real Trino coordinators use a randomly generated, per-lab password.
Only its bcrypt hash is stored in the namespace-local authentication Secret.
The private runtime file `trino.env` contains `TX_TRINO_USER` and
`TX_TRINO_PASSWORD` for test processes. No working credential is checked in.
Queries use HTTPS with a private, seven-day test CA. Both Gateways verify the
backend certificates, and clients verify the Gateway certificates. HTTP remains
available for isolated health checks, not password-authenticated queries. Never
reuse these credentials or test certificates for a deployed service.

Review cluster capacity and the rendered resources before creating the lab.
Use an explicitly authorized **development** context and a unique namespace:

```sh
python3 testing/transaction-awareness/deploy/lab.py \
  --context <development-context> --namespace gateway-tx-lab-<unique-id> create
python3 testing/transaction-awareness/deploy/lab.py \
  --context <development-context> --namespace gateway-tx-lab-<unique-id> status
python3 testing/transaction-awareness/deploy/lab.py \
  --context <development-context> --namespace gateway-tx-lab-<unique-id> \
  forward --ca-file <private-runtime-directory>/tls/ca.crt
```

`create` requires OpenSSL 3, a JDK `keytool`, and Apache `htpasswd` on PATH.
Use `--openssl`, `--keytool`, and `--htpasswd` to select explicit executable
paths. Password hashing sends the generated password through standard input,
not process arguments. The command refuses existing namespaces
and generates test credentials and certificates in a private
temporary directory and performs server-side dry runs. Do not publish that
directory, generated Secret manifests, or raw cluster diagnostics. `forward`
opens loopback listeners for distinct Gateway pods, the fixtures, and real Trino
coordinators. It prints the fixture suite's endpoint variables. Export those
variables in another terminal and follow the parent test README.
Set `TX_CA_FILE` to the generated `tls/ca.crt`. Do not disable TLS verification.

The PostgreSQL fault proxy exposes a separate namespace-only HTTP control port.
The forward command prints `TX_DATABASE_FAULT_URL` for this endpoint. Before
testing a feature-enabled artifact, configure the owned Gateway Secret:

```sh
python3 testing/transaction-awareness/deploy/lab.py \
  --context <development-context> --namespace gateway-tx-lab-<unique-id> \
  configure-transactions --enabled --env-file <new-private-environment-file>
```

This preserves database credentials and shared transaction keys, uses a Secret
resource-version check, and selects the fault proxy for database traffic. It
sets two-second terminal retention for tests, not the production default. The
private environment file contains `TX_ADMIN_TOKEN`; export it only to test
processes. The command does not restart Gateway pods. Deploy the matching
feature artifact next. Existing malformed keys cause an error, not rotation.

No registry publication is needed to test a local shaded Gateway JAR:

```sh
python3 testing/transaction-awareness/deploy/lab.py \
  --context <development-context> --namespace gateway-tx-lab-<unique-id> \
  artifact --jar gateway-ha/target/gateway-ha-22-SNAPSHOT-jar-with-dependencies.jar
```

This deliberately replaces only the lab Gateway pods. It copies the artifact
into each new pod's `emptyDir`, then starts Java. It does not build an image or
upload anything to a registry. Reopen port-forwards afterwards. The helper
uses at most two concurrent uploads, verifies each SHA-256 digest, and renames
each verified file atomically before starting Java. A replacement
pod loses its uploaded artifact; repeat the artifact command after replacement.
For a slow development connection, `--upload-timeout 900` extends the default
300-second bound. The helper rejects values outside 60-900 seconds. Do not run
multiple labs' uploads concurrently when the connection is already saturated.
Use this destructive lab operation only between test runs, not as a model of a
production rolling deployment.

The real Trino services are `https://trino-blue:8443` and
`https://trino-green:8443`. Register them with separate backend names and routing
groups from the Python fixtures. Trino processes forwarded headers so result
continuation URLs can remain at the Gateway. Test this using an unmodified
client; a fixture URL rewriter does not establish client compatibility.

Cleanup requires both the task ownership label and exact namespace confirmation:

```sh
python3 testing/transaction-awareness/deploy/lab.py \
  --context <development-context> --namespace gateway-tx-lab-<unique-id> \
  delete --confirm-namespace gateway-tx-lab-<unique-id>
```

Deletion permanently removes this lab's disposable database and artifacts.
The command never discovers or deletes other namespaces.

Local renderer checks require only Python:

```sh
python3 -m unittest discover -s testing/transaction-awareness/deploy -p 'test_*.py' -v
```
