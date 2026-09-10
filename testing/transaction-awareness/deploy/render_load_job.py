"""Render a bounded, non-preempting in-cluster benchmark client."""

import json
import re

from render import TASK


def render_job(namespace, name, gateway_urls, groups, script, ca_certificate, *, rate=100,
               duration=60, warmup=10, expected_backends=None, concurrency=128):
    if not re.fullmatch(r"gateway-tx-lab-[a-z0-9-]+", namespace) or not re.fullmatch(r"load-[a-z0-9-]+", name):
        raise ValueError("Use an explicit disposable namespace and unique load-* job")
    if not 1 <= rate <= 1000 or not 1 <= duration <= 300 or not 0 <= warmup <= 60 or not 1 <= concurrency <= 512:
        raise ValueError("Run only the bounded benchmark matrix")
    if not 2 <= len(gateway_urls) <= 100 or not groups or any(not url.startswith("https://") for url in gateway_urls):
        raise ValueError("Address two to one hundred verified Gateway HTTPS endpoints")
    if "-----BEGIN CERTIFICATE-----" not in ca_certificate or "PRIVATE KEY" in ca_certificate:
        raise ValueError("Mount only the public lab CA")
    labels = {"task": TASK, "app": "load-generator"}
    config = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": name, "namespace": namespace, "labels": labels},
              "data": {"load_open_loop.py": script, "ca.pem": ca_certificate}}
    environment = {"TX_GATEWAY_URLS": ",".join(gateway_urls), "TX_LOAD_ROUTING_GROUPS": ",".join(groups),
                   "TX_ALLOW_FIXTURE_MUTATION": "yes", "TX_CA_FILE": "/source/ca.pem", "TX_TLS_SERVER_NAME": "gateway",
                   "TX_LOAD_EXPECTED_BACKENDS": json.dumps(expected_backends or {})}
    launch = ("import base64,os,runpy,secrets; "
              "os.environ['TX_QUERY_AUTHORIZATION']='Basic '+base64.b64encode(('user:'+secrets.token_hex(24)).encode()).decode(); "
              "runpy.run_path('/source/load_open_loop.py',run_name='__main__')")
    job = {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": name, "namespace": namespace, "labels": labels},
           "spec": {"backoffLimit": 0, "activeDeadlineSeconds": duration + warmup + 180, "ttlSecondsAfterFinished": 3600,
                    "template": {"metadata": {"labels": labels}, "spec": {
                        "restartPolicy": "Never", "automountServiceAccountToken": False, "preemptionPolicy": "Never",
                        "securityContext": {"runAsNonRoot": True, "runAsUser": 1000, "runAsGroup": 1000,
                                            "seccompProfile": {"type": "RuntimeDefault"}},
                        "volumes": [{"name": "source", "configMap": {"name": name}}],
                        "containers": [{"name": "load", "image": "python:3.12-alpine", "command": ["python", "-c", launch],
                                        "args": ["--rate", str(rate), "--duration", str(duration), "--warmup", str(warmup),
                                                 "--concurrency", str(concurrency), "--output", "/tmp/load-receipt.json", "--print-receipt"],
                                        "resources": {key: {"cpu": "2", "memory": "2Gi"} for key in ("requests", "limits")},
                                        "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
                                        "volumeMounts": [{"name": "source", "mountPath": "/source", "readOnly": True}],
                                        "env": [{"name": key, "value": value} for key, value in environment.items()]}]}}}}
    return {"apiVersion": "v1", "kind": "List", "items": [config, job]}
