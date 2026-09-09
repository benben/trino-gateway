#!/usr/bin/env python3
"""Manage only an explicitly selected disposable Gateway transaction lab."""

import argparse
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import tempfile
import time

from render import TASK, render


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True, help="Explicit development Kubernetes context")
    parser.add_argument("--namespace", required=True, help="Unique gateway-tx-lab-* namespace")
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create", help="Create a new isolated namespace; refuses existing namespaces")
    create.add_argument("--fake-source", type=Path, default=Path(__file__).resolve().parent.parent / "fake_trino.py")
    commands.add_parser("status")
    forward = commands.add_parser("forward", help="Keep loopback forwards running; stop with Ctrl-C")
    forward.add_argument("--base-port", type=int, default=18081)
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
        resources = render(args.namespace, secrets.token_hex(24), source)
        runtime = Path(tempfile.mkdtemp(prefix="gateway-tx-lab-runtime-"))
        os.chmod(runtime, 0o700)
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
        for pod in pods:
            kubectl("-n", args.namespace, "cp", str(jar), f"{pod}:/artifact/upload.jar", "-c", "gateway", timeout=300)
            kubectl("-n", args.namespace, "exec", pod, "-c", "gateway", "--", "mv", "/artifact/upload.jar", "/artifact/launch.jar")
        print("Artifact uploaded only to lab emptyDir volumes. Reopen forwards after pod replacement.")
    elif args.command == "forward":
        pods = gateway_pods()
        if len(pods) < 2:
            raise SystemExit("At least two running Gateway pods are required")
        targets = ["pod/" + pod for pod in pods]
        targets += ["service/fixture-blue", "service/fixture-green", "service/trino-blue", "service/trino-green"]
        if args.base_port < 1024 or args.base_port + len(targets) > 65536:
            raise SystemExit("Invalid local port range")
        processes = []
        try:
            for offset, target in enumerate(targets):
                processes.append(subprocess.Popen(base + ["-n", args.namespace, "port-forward", "--address", "127.0.0.1", target, f"{args.base_port + offset}:8080"]))
            gateway_urls = ",".join(f"http://127.0.0.1:{args.base_port + index}" for index in range(len(pods)))
            fixture_port = args.base_port + len(pods)
            print(f"TX_GATEWAY_URLS={gateway_urls}", flush=True)
            print(f"TX_BACKEND_URLS=http://127.0.0.1:{fixture_port},http://127.0.0.1:{fixture_port + 1}", flush=True)
            print("TX_BACKEND_PROXY_URLS=http://fixture-blue:8080,http://fixture-green:8080", flush=True)
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
