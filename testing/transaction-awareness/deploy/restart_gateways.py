#!/usr/bin/env python3
"""Replace every disposable Gateway process while retaining a real transaction."""

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time

from render import TASK


def validate_scope(context, namespace, confirmation):
    if not context.strip() or not re.fullmatch(r"gateway-tx-lab-[a-z0-9-]{1,48}", namespace) or confirmation != namespace:
        raise ValueError("An explicit development context and exact dedicated namespace confirmation are required")


def verify_replacement(before, after):
    if len(before["gateways"]) < 2 or len(before["gateways"]) != len(after["gateways"]) or before["gateways"] & after["gateways"]:
        raise AssertionError("Every Gateway pod must be replaced without changing replica count")
    if before["dependencies"] != after["dependencies"]:
        raise AssertionError("PostgreSQL or a Trino process changed during the restart")
    if before["secrets"] != after["secrets"]:
        raise AssertionError("Persistent lab credentials or configuration changed during the restart")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--confirm-restart", required=True)
    parser.add_argument("--jar", required=True, type=Path)
    parser.add_argument("--base-port", type=int, default=19081)
    args = parser.parse_args()
    validate_scope(args.context, args.namespace, args.confirm_restart)
    if os.environ.get("TX_ALLOW_FIXTURE_MUTATION") != "yes":
        raise ValueError("Explicit disposable-fixture mutation authorization is required")
    for key in ["TX_CA_FILE", "TX_TRINO_USER", "TX_TRINO_PASSWORD", "TX_ADMIN_TOKEN"]:
        if not os.environ.get(key):
            raise ValueError("Missing runtime configuration: " + key)
    jar = args.jar.resolve(strict=True)
    if not jar.is_file() or jar.suffix != ".jar":
        raise ValueError("A locally built shaded Gateway JAR is required")
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from protocol import finish, request, statement

    base = ["kubectl", "--context", args.context, "-n", args.namespace]

    def read(*parts):
        result = subprocess.run(base + list(parts), capture_output=True, text=True, timeout=30, check=True)
        return json.loads(result.stdout)

    namespace = read("get", "namespace", args.namespace, "-o", "json")
    if namespace["metadata"].get("labels", {}).get("task") != TASK:
        raise ValueError("The namespace does not belong to this fixture")

    def snapshot():
        deployment = read("get", "deployment", "gateway", "-o", "json")
        if deployment["metadata"].get("labels", {}).get("task") != TASK:
            raise AssertionError("The Gateway Deployment does not belong to this fixture")
        pods = read("get", "pods", "-l", "task=" + TASK, "-o", "json")["items"]
        selected = {}
        gateway_pods = []
        for pod in pods:
            app = pod["metadata"].get("labels", {}).get("app")
            if pod["metadata"].get("deletionTimestamp") or app not in {"gateway", "postgres", "trino-blue", "trino-green"}:
                continue
            statuses = pod.get("status", {}).get("containerStatuses", [])
            if not statuses or not all(status.get("ready") for status in statuses):
                raise AssertionError("A required lab process is not ready")
            if app == "gateway":
                gateway_pods.append(pod)
            else:
                if app in selected:
                    raise AssertionError("Expected exactly one process per dependency")
                selected[app] = (pod["metadata"]["uid"], sum(status["restartCount"] for status in statuses))
        if set(selected) != {"postgres", "trino-blue", "trino-green"}:
            raise AssertionError("A required dependency is missing")
        if len(gateway_pods) != deployment["spec"]["replicas"]:
            raise AssertionError("All desired Gateway replicas must be ready before this operation")
        config = {}
        for name in ["gateway-config", "postgres-password", "trino-test-auth", "lab-tls"]:
            secret = read("get", "secret", name, "-o", "json")
            if secret["metadata"].get("labels", {}).get("task") != TASK:
                raise AssertionError("A dependency Secret does not belong to this fixture")
            config[name] = secret["data"]
        gateway_pods.sort(key=lambda pod: pod["metadata"]["name"])
        return {"gateways": {pod["metadata"]["uid"] for pod in gateway_pods}, "dependencies": selected,
                "secrets": hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest(),
                "pods": [pod["metadata"]["name"] for pod in gateway_pods]}

    processes = []

    def close_forwards():
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            process.wait(timeout=10)
        processes.clear()

    def forward(pods):
        if not 1024 <= args.base_port <= 65536 - len(pods):
            raise ValueError("Invalid loopback port range")
        urls = []
        for index, pod in enumerate(pods):
            port = args.base_port + index
            with socket.socket() as probe:
                probe.bind(("127.0.0.1", port))
            process = subprocess.Popen(base + ["port-forward", "--address", "127.0.0.1", "pod/" + pod, f"{port}:8443"],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            processes.append(process)
            urls.append(f"https://127.0.0.1:{port}")
        deadline = time.monotonic() + 30
        for url in urls:
            while True:
                if any(process.poll() is not None for process in processes):
                    raise AssertionError("A task-owned port-forward stopped")
                try:
                    if request(url + "/gateway/backend/all", timeout=2).status == 200:
                        break
                except OSError:
                    pass
                if time.monotonic() >= deadline:
                    raise AssertionError("Task-owned TLS forwards did not become ready")
                time.sleep(0.2)
        return urls

    authorization = "Basic " + base64.b64encode((os.environ["TX_TRINO_USER"] + ":" + os.environ["TX_TRINO_PASSWORD"]).encode()).decode()
    group = os.environ.get("TX_REAL_ROUTING_GROUP", "real-transaction-test")

    def query(url, sql, transaction="NONE"):
        initial = statement(url, sql, transaction, os.environ["TX_TRINO_USER"], group, [("Authorization", authorization)])
        pages = finish(initial, url, limit=200)
        if any(page.status != 200 or "error" in page.json() for page in pages):
            raise AssertionError("A real query failed; transaction identifiers remain private")
        if pages[-1].json().get("nextUri"):
            raise AssertionError("The query did not finish")
        return pages

    def scalar(url, sql, transaction="NONE"):
        rows = [row for page in query(url, sql, transaction) for row in page.json().get("data", [])]
        if len(rows) != 1 or len(rows[0]) != 1:
            raise AssertionError("Expected exactly one scalar result")
        return rows[0][0]

    def binding(url, transaction):
        response = request(url + "/gateway/transactions/" + transaction,
                           headers=[("Authorization", "Bearer " + os.environ["TX_ADMIN_TOKEN"])])
        if response.status != 200:
            raise AssertionError("The feature API did not expose retained transaction ownership")
        data = response.json()
        if data.get("state") != "OPEN":
            raise AssertionError("The retained transaction is not open")
        return {key: data[key] for key in ["backendName", "backendUrl", "incarnation"]}

    transaction = None
    urls = []
    try:
        before = snapshot()
        if len(before["gateways"]) < 2:
            raise AssertionError("At least two ready Gateway processes are required")
        urls = forward(before["pods"])
        initial_node = scalar(urls[0], "SELECT node_id FROM system.runtime.nodes WHERE coordinator")
        pages = query(urls[0], "START TRANSACTION READ ONLY")
        identifiers = {value for page in pages for value in page.values("X-Trino-Started-Transaction-Id")}
        if len(identifiers) != 1:
            raise AssertionError("Trino did not return one transaction identity")
        transaction = identifiers.pop()
        owner = binding(urls[-1], transaction)
        if scalar(urls[-1], "SELECT count(*) FROM tpch.tiny.nation", transaction) != 25:
            raise AssertionError("The transaction baseline result was incorrect")
        close_forwards()
        print("Real transaction is open; replacing all Gateway processes while preserving its private identity.")
        subprocess.run([sys.executable, str(Path(__file__).with_name("lab.py")), "--context", args.context, "--namespace", args.namespace,
                        "artifact", "--jar", str(jar)], check=True, timeout=900)
        deadline = time.monotonic() + 180
        while True:
            try:
                after = snapshot()
                verify_replacement(before, after)
                break
            except AssertionError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1)
        urls = forward(after["pods"])
        for url in urls:
            if binding(url, transaction) != owner:
                raise AssertionError("Restart changed the retained transaction ownership")
            if scalar(url, "SELECT count(*) FROM tpch.tiny.nation", transaction) != 25:
                raise AssertionError("The retained transaction result was incorrect")
            if scalar(url, "SELECT node_id FROM system.runtime.nodes WHERE coordinator") != initial_node:
                raise AssertionError("Restart changed the destination of new queries")
        pages = query(urls[-1], "COMMIT", transaction)
        if not any(page.values("X-Trino-Clear-Transaction-Id") for page in pages):
            raise AssertionError("COMMIT did not clear the retained transaction")
        transaction = None
        print(f"PASS real transaction survived replacement of all {len(urls)} Gateway processes")
        print("PostgreSQL, Trino processes, credentials, and new-query destination stayed unchanged.")
    finally:
        if transaction and urls and processes:
            try:
                query(urls[0], "ROLLBACK", transaction)
            except Exception:
                print("Cleanup could not confirm transaction closure; inspect the private lab before reuse.", file=sys.stderr)
        elif transaction:
            print("No live test forward remains for cleanup; inspect the retained transaction before reusing the lab.", file=sys.stderr)
        close_forwards()
        print("Reopen any external lab forwards after Gateway replacement.")


if __name__ == "__main__":
    try:
        main()
    except Exception as failure:
        print("Gateway restart regression failed: " + type(failure).__name__, file=sys.stderr)
        raise SystemExit(1)
