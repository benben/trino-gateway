#!/usr/bin/env python3
"""Manage only an explicitly selected disposable Gateway transaction lab."""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import subprocess
import tempfile
import time

from render import TASK, password_hash, render, validate_extra_fixtures
from tls import generate_tls
from transaction_config import configure_transactions


def upload_artifacts(kubectl, pods, jar):
    digest = hashlib.sha256()
    with jar.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    expected = digest.hexdigest()

    def upload(pod):
        kubectl("cp", str(jar), f"{pod}:/artifact/upload.jar", "-c", "gateway", timeout=300)
        result = kubectl("exec", pod, "-c", "gateway", "--", "sha256sum", "/artifact/upload.jar", capture=True)
        if not result.stdout.split() or result.stdout.split()[0] != expected:
            raise RuntimeError("Uploaded lab artifact checksum did not match")
        kubectl("exec", pod, "-c", "gateway", "--", "mv", "/artifact/upload.jar", "/artifact/launch.jar")

    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(upload, pods))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True, help="Explicit development Kubernetes context")
    parser.add_argument("--namespace", required=True, help="Unique gateway-tx-lab-* namespace")
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create", help="Create a new isolated namespace; refuses existing namespaces")
    create.add_argument("--fake-source", type=Path, default=Path(__file__).resolve().parent.parent / "fake_trino.py")
    create.add_argument("--proxy-source", type=Path, default=Path(__file__).resolve().parent.parent / "network_fault_proxy.py")
    create.add_argument("--openssl", default="openssl")
    create.add_argument("--keytool", default="keytool")
    create.add_argument("--htpasswd", default="htpasswd")
    create.add_argument("--without-real-trino", action="store_true", help="Create only controlled backends for isolated fault tests")
    create.add_argument("--extra-fixture", action="append", default=[])
    commands.add_parser("status")
    forward = commands.add_parser("forward", help="Keep loopback forwards running; stop with Ctrl-C")
    forward.add_argument("--base-port", type=int, default=18081)
    forward.add_argument("--ca-file", type=Path, required=True)
    forward.add_argument("--without-real-trino", action="store_true")
    forward.add_argument("--extra-fixture", action="append", default=[])
    feature = commands.add_parser("configure-transactions", help="Preserve lab credentials and configure the feature before artifact deployment")
    feature.add_argument("--enabled", action=argparse.BooleanOptionalAction, required=True)
    feature.add_argument("--env-file", type=Path, required=True, help="New private file for the test admin token")
    feature.add_argument("--terminal-retention", type=int, default=2)
    artifact = commands.add_parser("artifact", help="Replace lab Gateway processes with a locally built shaded JAR")
    artifact.add_argument("--jar", type=Path, required=True)
    delete = commands.add_parser("delete", help="Delete this disposable namespace and all its test data")
    delete.add_argument("--confirm-namespace", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"gateway-tx-lab-[a-z0-9-]+", args.namespace) or len(args.namespace) > 63:
        parser.error("namespace must be a dedicated gateway-tx-lab-* DNS label")
    if not args.context.strip():
        parser.error("context must not be empty")
    base = ["kubectl", "--context", args.context]

    def kubectl(*parts, capture=False, check=True, timeout=60):
        return subprocess.run(base + list(parts), text=True, capture_output=capture, check=check, timeout=timeout)

    def namespace_owned():
        result = kubectl("get", "namespace", args.namespace, "-o", "json", capture=True)
        if json.loads(result.stdout)["metadata"].get("labels", {}).get("task") != TASK:
            raise SystemExit("Refusing a namespace without this lab's ownership label")

    def gateway_pods(artifact_upload=None):
        result = kubectl("-n", args.namespace, "get", "pods", "-l", f"task={TASK},app=gateway", "-o", "json", capture=True)
        return sorted(pod["metadata"]["name"] for pod in json.loads(result.stdout)["items"]
                      if not pod["metadata"].get("deletionTimestamp") and pod["status"].get("phase") == "Running"
                      and (artifact_upload is None or pod["metadata"].get("annotations", {}).get("transaction-test/artifact-upload") == artifact_upload))

    if args.command == "create":
        existing = kubectl("get", "namespace", args.namespace, "-o", "name", capture=True, check=False)
        if existing.returncode == 0:
            raise SystemExit("Refusing to recreate an existing namespace or replace its credentials")
        if "NotFound" not in existing.stderr:
            raise SystemExit("Namespace preflight failed; no resources were changed")
        source = args.fake_source.read_text()
        runtime = Path(tempfile.mkdtemp(prefix="gateway-tx-lab-runtime-"))
        os.chmod(runtime, 0o700)
        password = secrets.token_hex(24)
        generate_tls(runtime / "tls", args.namespace, password, openssl=args.openssl, keytool=args.keytool)
        tls_files = {name: (runtime / "tls" / name).read_bytes() for name in ["gateway.p12", "trino-blue.p12", "trino-green.p12", "truststore.p12"]}
        trino_password = secrets.token_hex(24)
        hashed = password_hash(trino_password, args.htpasswd)
        descriptor = os.open(runtime / "trino.env", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as output:
            output.write("TX_TRINO_USER=user\nTX_TRINO_PASSWORD=" + shlex.quote(trino_password) + "\n")
        resources = render(args.namespace, password, source, tls_files=tls_files, proxy_source=args.proxy_source.read_text(), trino_password_hash=hashed,
                           include_real_trino=not args.without_real_trino, extra_fixtures=args.extra_fixture)
        for name, data in [("namespace.json", resources["items"][0]), ("resources.json", resources)]:
            target = runtime / name
            with target.open("x") as output:
                os.chmod(target, 0o600)
                json.dump(data, output)
        kubectl("apply", "--dry-run=server", "-f", str(runtime / "namespace.json"))
        kubectl("create", "-f", str(runtime / "namespace.json"))
        kubectl("apply", "--dry-run=server", "-f", str(runtime / "resources.json"))
        kubectl("apply", "-f", str(runtime / "resources.json"))
        print(f"Private generated credentials and manifests: {runtime}")
        print("Keep this directory private. Never publish its contents or raw cluster diagnostics.")
        return

    namespace_owned()
    if args.command == "status":
        kubectl("-n", args.namespace, "get", "pods,services,resourcequota")
    elif args.command == "configure-transactions":
        if args.env_file.exists():
            raise SystemExit("Refusing to overwrite an existing private environment file")
        result = kubectl("-n", args.namespace, "get", "secret", "gateway-config", "-o", "json", capture=True)
        secret = json.loads(result.stdout)
        if secret["metadata"].get("labels", {}).get("task") != TASK:
            raise SystemExit("Refusing a Secret without this lab's ownership label")
        config = json.loads(base64.b64decode(secret["data"]["config.yaml"]))
        if config.get("dataStore", {}).get("jdbcUrl") not in {"jdbc:postgresql://postgres:5432/gateway", "jdbc:postgresql://postgres-fault-proxy:15432/gateway"}:
            raise SystemExit("Refusing to replace an unexpected lab database address")
        config = configure_transactions(config, enabled=args.enabled, terminal_retention=args.terminal_retention)
        config["dataStore"]["jdbcUrl"] = "jdbc:postgresql://postgres-fault-proxy:15432/gateway"
        secret["data"]["config.yaml"] = base64.b64encode(json.dumps(config).encode()).decode()
        secret["metadata"].pop("managedFields", None)
        secret["metadata"].get("annotations", {}).pop("kubectl.kubernetes.io/last-applied-configuration", None)
        response = subprocess.run(base + ["-n", args.namespace, "replace", "-f", "-"], input=json.dumps(secret), text=True, capture_output=True, timeout=60)
        if response.returncode:
            raise SystemExit("Secret compare-and-swap failed; no token was written. Inspect the lab resource version before retrying.")
        descriptor = os.open(args.env_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as output:
            output.write("TX_ADMIN_TOKEN=" + shlex.quote(config["transactionAwareness"]["adminToken"]) + "\n")
        print("Updated the owned lab Secret with its resource version. Restart through artifact deployment when ready.")
    elif args.command == "delete":
        if args.confirm_namespace != args.namespace:
            raise SystemExit("Deletion requires the exact namespace confirmation")
        kubectl("delete", "namespace", args.namespace, "--wait=false")
        print("Deleted the disposable namespace. Its in-memory and emptyDir test data cannot be recovered.")
    elif args.command == "artifact":
        jar = args.jar.resolve(strict=True)
        if not jar.is_file() or jar.suffix != ".jar":
            raise SystemExit("Expected a locally built shaded .jar artifact")
        upload_id = str(time.time_ns())
        patch = {"spec": {"template": {"metadata": {"annotations": {"transaction-test/artifact-upload": upload_id}},
                                       "spec": {"containers": [{"name": "gateway", "command": ["sh", "-c"],
                                                                 "args": ["while [ ! -f /artifact/launch.jar ]; do sleep 1; done; exec java -Xmx512m -jar /artifact/launch.jar /etc/trino-gateway/config.yaml"]}]}}}}
        kubectl("-n", args.namespace, "patch", "deployment", "gateway", "--type=strategic", "-p", json.dumps(patch))
        expected = int(kubectl("-n", args.namespace, "get", "deployment", "gateway", "-o", "jsonpath={.spec.replicas}", capture=True).stdout)
        deadline = time.monotonic() + 180
        pods = []
        while time.monotonic() < deadline:
            pods = gateway_pods(upload_id)
            if len(pods) == expected:
                break
            time.sleep(1)
        if len(pods) != expected:
            raise SystemExit("Timed out waiting for isolated Gateway upload pods; no artifact was published")
        upload_artifacts(lambda *parts, **options: kubectl("-n", args.namespace, *parts, **options), pods, jar)
        print("Artifact uploaded only to lab emptyDir volumes. Reopen forwards after pod replacement.")
    elif args.command == "forward":
        validate_extra_fixtures(args.extra_fixture)
        pods = gateway_pods()
        if len(pods) < 2:
            raise SystemExit("At least two running Gateway pods are required")
        args.ca_file.resolve(strict=True)
        targets = [("pod/" + pod, 8443) for pod in pods]
        targets += [("service/fixture-blue", 8080), ("service/fixture-green", 8080)]
        if not args.without_real_trino:
            targets += [("service/trino-blue", 8443), ("service/trino-green", 8443)]
        targets.append(("service/postgres-fault-proxy", 8080))
        fault_port = args.base_port + len(targets) - 1
        extra_port = fault_port + 1
        targets += [("service/fixture-" + name, 8080) for name in args.extra_fixture]
        if args.base_port < 1024 or args.base_port + len(targets) > 65536:
            raise SystemExit("Invalid local port range")
        processes = []
        try:
            for offset, (target, remote_port) in enumerate(targets):
                processes.append(subprocess.Popen(base + ["-n", args.namespace, "port-forward", "--address", "127.0.0.1", target, f"{args.base_port + offset}:{remote_port}"]))
            gateway_urls = ",".join(f"https://127.0.0.1:{args.base_port + index}" for index in range(len(pods)))
            fixture_port = args.base_port + len(pods)
            print(f"TX_GATEWAY_URLS={gateway_urls}", flush=True)
            print(f"TX_BACKEND_URLS=http://127.0.0.1:{fixture_port},http://127.0.0.1:{fixture_port + 1}", flush=True)
            print("TX_BACKEND_PROXY_URLS=http://fixture-blue:8080,http://fixture-green:8080", flush=True)
            print(f"TX_CA_FILE={args.ca_file.resolve()}", flush=True)
            print(f"TX_DATABASE_FAULT_URL=http://127.0.0.1:{fault_port}", flush=True)
            if args.extra_fixture:
                extra_urls = {name: f"http://127.0.0.1:{extra_port + index}" for index, name in enumerate(args.extra_fixture)}
                print("TX_EXTRA_BACKEND_URLS=" + json.dumps(extra_urls), flush=True)
            while all(process.poll() is None for process in processes):
                time.sleep(1)
            raise SystemExit("A port-forward exited; stop tests and reopen forwards")
        except KeyboardInterrupt:
            pass
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
            for process in processes:
                process.wait(timeout=10)


if __name__ == "__main__":
    main()
