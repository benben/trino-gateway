# Disposable Kubernetes lab

This lab runs two independent Gateway processes sharing PostgreSQL, two stock
Trino single-node coordinators, and two controllable Python backends. It is for
synthetic development tests, not customer traffic. The initial Gateway image is
upstream version 21. Trino is version 483.

The namespace has restricted pod security, no mounted service-account tokens,
no host privileges, and no public Ingress or load balancer. NetworkPolicy permits
only same-namespace test traffic and cluster DNS. Resources request 3.45 CPUs and
8.75 GiB total; a quota caps requests and limits at 8 CPUs, 16 GiB, and 20 pods.
PostgreSQL uses `emptyDir`: pod replacement loses all test state.

Review cluster capacity and the rendered resources before creating the lab.
Use an explicitly authorized **development** context and a unique namespace:

```sh
python3 testing/transaction-awareness/deploy/lab.py \
  --context <development-context> --namespace gateway-tx-lab-<unique-id> create
python3 testing/transaction-awareness/deploy/lab.py \
  --context <development-context> --namespace gateway-tx-lab-<unique-id> status
python3 testing/transaction-awareness/deploy/lab.py \
  --context <development-context> --namespace gateway-tx-lab-<unique-id> forward
```

`create` refuses existing namespaces. It generates test credentials in a private
temporary directory and performs server-side dry runs. Do not publish that
directory, generated Secret manifests, or raw cluster diagnostics. `forward`
opens loopback listeners for distinct Gateway pods, the fixtures, and real Trino
coordinators. It prints the fixture suite's endpoint variables. Export those
variables in another terminal and follow the parent test README.

No registry publication is needed to test a local shaded Gateway JAR:

```sh
python3 testing/transaction-awareness/deploy/lab.py \
  --context <development-context> --namespace gateway-tx-lab-<unique-id> \
  artifact --jar gateway-ha/target/gateway-ha-22-SNAPSHOT-jar-with-dependencies.jar
```

This deliberately replaces only the lab Gateway pods. It copies the artifact
into each new pod's `emptyDir`, then starts Java. It does not build an image or
upload anything to a registry. Reopen port-forwards afterwards. A replacement
pod loses its uploaded artifact; repeat the artifact command after replacement.
Use this destructive lab operation only between test runs, not as a model of a
production rolling deployment.

The real Trino services are `http://trino-blue:8080` and
`http://trino-green:8080`. Register them with separate backend names and routing
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
