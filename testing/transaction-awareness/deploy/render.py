#!/usr/bin/env python3
"""Render an isolated, disposable transaction-routing lab as Kubernetes JSON."""

import argparse
import base64
import hashlib
import hmac
import json
from pathlib import Path
import re
import secrets
import subprocess


TASK = "gateway-transaction-awareness"

def password_hash(password, htpasswd="htpasswd"):
    result = subprocess.run([htpasswd, "-niB", "-C", "10", "user"], input=password + "\n", text=True,
                            capture_output=True, check=True)
    hashed = result.stdout.strip().removeprefix("user:")
    if not re.fullmatch(r"\$2[aby]\$10\$[./A-Za-z0-9]{53}", hashed):
        raise ValueError("Password utility did not return a bcrypt hash")
    return hashed


def validate_extra_fixtures(names):
    if len(set(names)) != len(names) or any(name in {"blue", "green"} or not re.fullmatch(r"[a-z](?:[a-z0-9-]{0,29}[a-z0-9])?", name) for name in names):
        raise ValueError("Extra fixture names must be unique DNS labels other than blue or green")


def validate_priority_class(priority_class):
    if not isinstance(priority_class, dict):
        raise ValueError("Provide an explicitly approved, existing non-preempting PriorityClass")
    metadata = priority_class.get("metadata", {})
    name = metadata.get("name", "")
    if (not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", name)
            or metadata.get("labels", {}).get("task") != TASK
            or type(priority_class.get("value")) is not int or priority_class["value"] >= 0
            or priority_class.get("preemptionPolicy") != "Never"
            or priority_class.get("globalDefault", False) is not False):
        raise ValueError("PriorityClass must be task-owned, negative-priority, non-default, and non-preempting")
    return name


def render(namespace, password, fake_source=None, *, tls_files=None, proxy_source=None, trino_password_hash="!", include_real_trino=True, extra_fixtures=(), gateway_image="trinodb/trino-gateway:21", benchmark=False, priority_class=None):
    priority_name = validate_priority_class(priority_class) if benchmark or priority_class is not None else None
    validate_extra_fixtures(extra_fixtures)
    if extra_fixtures and not fake_source:
        raise ValueError("Extra fixtures require the controlled backend source")
    labels = {"task": TASK}
    objects = [{"apiVersion": "v1", "kind": "Namespace", "metadata": {
        "name": namespace, "labels": labels | {
            "pod-security.kubernetes.io/enforce": "restricted",
            "pod-security.kubernetes.io/enforce-version": "latest"}}}]

    def add(kind, name, spec=None, **fields):
        obj = {"apiVersion": "apps/v1" if kind == "Deployment" else "v1",
               "kind": kind, "metadata": {"name": name, "namespace": namespace, "labels": labels}}
        if spec is not None:
            obj["spec"] = spec
        obj.update(fields)
        objects.append(obj)

    objects.append({"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
                    "metadata": {"name": "lab-isolation", "namespace": namespace, "labels": labels},
                    "spec": {"podSelector": {}, "policyTypes": ["Ingress", "Egress"],
                             "ingress": [{"from": [{"podSelector": {"matchLabels": labels}}]}],
                             "egress": [{"to": [{"podSelector": {"matchLabels": labels}}]},
                                        {"to": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "kube-system"}},
                                                 "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}}}],
                                         "ports": [{"protocol": protocol, "port": 53} for protocol in ["UDP", "TCP"]]}]}})
    add("ResourceQuota", "lab-budget", {"hard": {"requests.cpu": "8", "limits.cpu": "8",
                                                   "requests.memory": "16Gi", "limits.memory": "16Gi", "pods": "20"}})

    def config(name, data):
        add("ConfigMap", name, data=data)

    def deployment(name, image, cpu, memory, volumes, mounts, *, replicas=1, command=None, args=None, env=None, uid=1000, port=8080):
        pod_labels = labels | {"app": name}
        container = {"name": name, "image": image, "imagePullPolicy": "IfNotPresent",
                     "resources": {key: {"cpu": cpu, "memory": memory} for key in ["requests", "limits"]},
                     "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
                     "ports": [{"containerPort": port, "name": "http"}], "volumeMounts": mounts,
                     "readinessProbe": {"tcpSocket": {"port": port}, "initialDelaySeconds": 5, "periodSeconds": 5},
                     "startupProbe": {"tcpSocket": {"port": port}, "periodSeconds": 5, "failureThreshold": 120}}
        if command:
            container["command"] = command
        if args:
            container["args"] = args
        if env:
            container["env"] = env
        add("Deployment", name, {"replicas": replicas, "strategy": {"type": "Recreate"},
                                  "selector": {"matchLabels": pod_labels},
                                  "template": {"metadata": {"labels": pod_labels}, "spec": {
                                      "automountServiceAccountToken": False, "terminationGracePeriodSeconds": 30,
                                      "securityContext": {"runAsNonRoot": True, "runAsUser": uid, "runAsGroup": uid,
                                                          "fsGroup": uid, "seccompProfile": {"type": "RuntimeDefault"}},
                                      "containers": [container], "volumes": volumes}}})
        add("Service", name, {"type": "ClusterIP", "selector": pod_labels, "ports": [{"port": port, "targetPort": port}]})

    add("Secret", "postgres-password", type="Opaque", stringData={"password": password})
    deployment("postgres", "postgres:17-alpine", "250m", "512Mi",
               [{"name": "data", "emptyDir": {}}], [{"name": "data", "mountPath": "/var/lib/postgresql/data"}], uid=70, port=5432,
               env=[{"name": "POSTGRES_DB", "value": "gateway"}, {"name": "POSTGRES_USER", "value": "gateway"},
                    {"name": "PGDATA", "value": "/var/lib/postgresql/data/pgdata"},
                    {"name": "POSTGRES_PASSWORD", "valueFrom": {"secretKeyRef": {"name": "postgres-password", "key": "password"}}}])
    gateway_config = {"serverConfig": {"node.environment": "transaction_test", "http-server.http.port": 8080},
                      "routingRules": {"rulesEngineEnabled": False},
                      "dataStore": {"jdbcUrl": "jdbc:postgresql://postgres:5432/gateway", "user": "gateway", "password": password,
                                    "driver": "org.postgresql.Driver", "queryHistoryHoursRetention": 24},
                      "clusterStatsConfiguration": {"monitorType": "INFO_API"}, "monitor": {"taskDelay": "1s", "queryTimeout": "5s"}}
    if tls_files:
        add("Secret", "lab-tls", type="Opaque", data={name: base64.b64encode(value).decode() for name, value in tls_files.items()})
        gateway_config["serverConfig"].update({"http-server.https.enabled": True, "http-server.https.port": 8443,
                                               "http-server.https.keystore.path": "/etc/lab-tls/gateway.p12", "http-server.https.keystore.key": password})
        for client in ["proxy", "monitor"]:
            gateway_config["serverConfig"][client + ".http-client.trust-store-path"] = "/etc/lab-tls/truststore.p12"
            gateway_config["serverConfig"][client + ".http-client.trust-store-password"] = password
    add("Secret", "gateway-config", type="Opaque", stringData={"config.yaml": json.dumps(gateway_config)})
    deployment("gateway", gateway_image, "500m", "1Gi",
               [{"name": "config", "secret": {"secretName": "gateway-config"}}, {"name": "artifact", "emptyDir": {}}],
               [{"name": "config", "mountPath": "/etc/trino-gateway", "readOnly": True}, {"name": "artifact", "mountPath": "/artifact"}], replicas=2,
               command=["sh", "-c"], args=["exec java -Xmx512m -jar /usr/lib/trino-gateway/gateway-ha-jar-with-dependencies.jar /etc/trino-gateway/config.yaml"])
    trino_colors = ["blue", "green"] if include_real_trino else []
    trino_auth = {"password.db": "user:" + trino_password_hash + "\n"}
    for color in trino_colors:
        trino_auth["internal-secret-" + color] = hmac.new(password.encode(), ("trino-test-internal-" + color).encode(), hashlib.sha256).hexdigest()
    if include_real_trino:
        add("Secret", "trino-test-auth", type="Opaque", stringData=trino_auth)
    for color in trino_colors:
        name = "trino-" + color
        config(name, {"config.properties": "coordinator=true\nnode-scheduler.include-coordinator=true\nhttp-server.http.port=8080\nhttp-server.process-forwarded=true\nhttp-server.authentication.type=PASSWORD\nhttp-server.authentication.allow-insecure-over-http=true\ninternal-communication.shared-secret=${ENV:TRINO_INTERNAL_SECRET}\ndiscovery.uri=http://localhost:8080\nquery.max-memory-per-node=256MB\n",
                      "node.properties": "node.environment=transaction_test\nnode.data-dir=/data/trino\n",
                      "jvm.config": "-server\n-Xmx1G\n-XX:+UseG1GC\n-XX:+ExitOnOutOfMemoryError\n",
                      "password-authenticator.properties": "password-authenticator.name=file\nfile.password-file=/etc/trino-auth/password.db\n",
                      "log.properties": "io.trino=INFO\n", "tpch.properties": "connector.name=tpch\n"})
        deployment(name, "trinodb/trino:483", "1", "3Gi",
                   [{"name": "config", "configMap": {"name": name, "items": [{"key": key, "path": "catalog/tpch.properties" if key == "tpch.properties" else key}
                                                                                              for key in ["config.properties", "node.properties", "jvm.config", "log.properties", "password-authenticator.properties", "tpch.properties"]]}},
                    {"name": "auth", "secret": {"secretName": "trino-test-auth"}},
                    {"name": "data", "emptyDir": {}}],
                   [{"name": "config", "mountPath": "/etc/trino", "readOnly": True}, {"name": "auth", "mountPath": "/etc/trino-auth", "readOnly": True}, {"name": "data", "mountPath": "/data/trino"}],
                   env=[{"name": "TRINO_INTERNAL_SECRET", "valueFrom": {"secretKeyRef": {"name": "trino-test-auth", "key": "internal-secret-" + color}}}])
    if fake_source:
        config("fake-backend-source", {"fake_trino.py": fake_source})
        for color in ["blue", "green", *extra_fixtures]:
            cpu, memory = ("100m", "128Mi") if color in {"blue", "green"} else ("50m", "64Mi")
            if benchmark:
                cpu, memory = "1", "512Mi"
            deployment("fixture-" + color, "python:3.12-alpine", cpu, memory,
                       [{"name": "source", "configMap": {"name": "fake-backend-source"}}],
                       [{"name": "source", "mountPath": "/fixture", "readOnly": True}],
                       command=["python", "/fixture/fake_trino.py"], args=["--host", "0.0.0.0", "--port", "8080", "--identity", color])
    if proxy_source:
        config("postgres-fault-proxy-source", {"network_fault_proxy.py": proxy_source})
        deployment("postgres-fault-proxy", "python:3.12-alpine", "50m", "64Mi",
                   [{"name": "source", "configMap": {"name": "postgres-fault-proxy-source"}}],
                   [{"name": "source", "mountPath": "/fixture", "readOnly": True}],
                   command=["python", "/fixture/network_fault_proxy.py"],
                   args=["--host", "0.0.0.0", "--port", "15432", "--control-port", "8080", "--upstream-host", "postgres", "--upstream-port", "5432"])
        objects[-1]["spec"]["ports"] = [{"name": "control", "port": 8080, "targetPort": 8080}, {"name": "postgres", "port": 15432, "targetPort": 15432}]
    if tls_files:
        for item in objects:
            name = item["metadata"]["name"]
            if name not in {"gateway", "trino-blue", "trino-green"}:
                continue
            if item["kind"] == "Deployment":
                pod = item["spec"]["template"]["spec"]
                certificate = name + ".p12"
                keys = [certificate, "truststore.p12"] if name == "gateway" else [certificate]
                pod["volumes"].append({"name": "tls", "secret": {"secretName": "lab-tls", "items": [{"key": key, "path": key} for key in keys]}})
                pod["containers"][0]["volumeMounts"].append({"name": "tls", "mountPath": "/etc/lab-tls", "readOnly": True})
                if name != "gateway":
                    pod["containers"][0]["env"].append({"name": "TLS_STORE_PASSWORD", "valueFrom": {"secretKeyRef": {"name": "postgres-password", "key": "password"}}})
            elif item["kind"] == "Service":
                item["spec"]["ports"] = [{"name": "http", "port": 8080, "targetPort": 8080}, {"name": "https", "port": 8443, "targetPort": 8443}]
            elif item["kind"] == "ConfigMap":
                item["data"]["config.properties"] += ("http-server.https.enabled=true\nhttp-server.https.port=8443\n"
                                                        f"http-server.https.keystore.path=/etc/lab-tls/{name}.p12\n"
                                                        "http-server.https.keystore.key=${ENV:TLS_STORE_PASSWORD}\n")
    if priority_name:
        for item in objects:
            if item["kind"] == "Deployment":
                item["spec"]["template"]["spec"]["preemptionPolicy"] = "Never"
                item["spec"]["template"]["spec"]["priorityClassName"] = priority_name
    return {"apiVersion": "v1", "kind": "List", "items": objects}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--password-file", type=Path, help="Existing private lab password file; otherwise generate a new password.")
    parser.add_argument("--fake-source", type=Path)
    parser.add_argument("--trino-password-file", type=Path, required=True)
    parser.add_argument("--htpasswd", default="htpasswd")
    parser.add_argument("--extra-fixture", action="append", default=[])
    args = parser.parse_args()
    if not re.fullmatch(r"gateway-tx-lab-[a-z0-9-]+", args.namespace) or len(args.namespace) > 63:
        parser.error("namespace must be a dedicated gateway-tx-lab-* DNS label")
    password = args.password_file.read_text().strip() if args.password_file else secrets.token_hex(24)
    if not re.fullmatch(r"[a-zA-Z0-9]{24,128}", password):
        parser.error("lab password must contain 24-128 alphanumeric characters")
    hashed = password_hash(args.trino_password_file.read_text().strip(), args.htpasswd)
    print(json.dumps(render(args.namespace, password, args.fake_source.read_text() if args.fake_source else None, trino_password_hash=hashed, extra_fixtures=args.extra_fixture), indent=2))


if __name__ == "__main__":
    main()
