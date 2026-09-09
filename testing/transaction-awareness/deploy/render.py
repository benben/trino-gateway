#!/usr/bin/env python3
"""Render an isolated, disposable transaction-routing lab as Kubernetes JSON."""

import argparse
import json
from pathlib import Path
import re
import secrets


TASK = "gateway-transaction-awareness"


def render(namespace, password, fake_source=None):
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
    gateway_config = f"""serverConfig:
  node.environment: transaction_test
  http-server.http.port: 8080
routingRules:
  rulesEngineEnabled: false
dataStore:
  jdbcUrl: jdbc:postgresql://postgres:5432/gateway
  user: gateway
  password: {password}
  driver: org.postgresql.Driver
  queryHistoryHoursRetention: 24
clusterStatsConfiguration:
  monitorType: INFO_API
monitor:
  taskDelay: 1s
  queryTimeout: 5s
"""
    add("Secret", "gateway-config", type="Opaque", stringData={"config.yaml": gateway_config})
    deployment("gateway", "trinodb/trino-gateway:21", "500m", "1Gi",
               [{"name": "config", "secret": {"secretName": "gateway-config"}}, {"name": "artifact", "emptyDir": {}}],
               [{"name": "config", "mountPath": "/etc/trino-gateway", "readOnly": True}, {"name": "artifact", "mountPath": "/artifact"}], replicas=2,
               command=["sh", "-c"], args=["exec java -Xmx512m -jar /usr/lib/trino-gateway/gateway-ha-jar-with-dependencies.jar /etc/trino-gateway/config.yaml"])
    for color in ["blue", "green"]:
        name = "trino-" + color
        config(name, {"config.properties": "coordinator=true\nnode-scheduler.include-coordinator=true\nhttp-server.http.port=8080\nhttp-server.process-forwarded=true\ndiscovery.uri=http://localhost:8080\nquery.max-memory-per-node=256MB\n",
                      "node.properties": "node.environment=transaction_test\nnode.data-dir=/data/trino\n",
                      "jvm.config": "-server\n-Xmx1G\n-XX:+UseG1GC\n-XX:+ExitOnOutOfMemoryError\n",
                      "log.properties": "io.trino=INFO\n", "tpch.properties": "connector.name=tpch\n"})
        deployment(name, "trinodb/trino:483", "1", "3Gi",
                   [{"name": "config", "configMap": {"name": name, "items": [{"key": key, "path": "catalog/tpch.properties" if key == "tpch.properties" else key}
                                                                                              for key in ["config.properties", "node.properties", "jvm.config", "log.properties", "tpch.properties"]]}},
                    {"name": "data", "emptyDir": {}}],
                   [{"name": "config", "mountPath": "/etc/trino", "readOnly": True}, {"name": "data", "mountPath": "/data/trino"}])
    if fake_source:
        config("fake-backend-source", {"fake_trino.py": fake_source})
        for color in ["blue", "green"]:
            deployment("fixture-" + color, "python:3.12-alpine", "100m", "128Mi",
                       [{"name": "source", "configMap": {"name": "fake-backend-source"}}],
                       [{"name": "source", "mountPath": "/fixture", "readOnly": True}],
                       command=["python", "/fixture/fake_trino.py"], args=["--host", "0.0.0.0", "--port", "8080", "--identity", color])
    return {"apiVersion": "v1", "kind": "List", "items": objects}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--password-file", type=Path, help="Existing private lab password file; otherwise generate a new password.")
    parser.add_argument("--fake-source", type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"gateway-tx-lab-[a-z0-9-]+", args.namespace) or len(args.namespace) > 63:
        parser.error("namespace must be a dedicated gateway-tx-lab-* DNS label")
    password = args.password_file.read_text().strip() if args.password_file else secrets.token_hex(24)
    if not re.fullmatch(r"[a-zA-Z0-9]{24,128}", password):
        parser.error("lab password must contain 24-128 alphanumeric characters")
    print(json.dumps(render(args.namespace, password, args.fake_source.read_text() if args.fake_source else None), indent=2))


if __name__ == "__main__":
    main()
