"""Render a bounded, non-preempting in-cluster benchmark client."""

import json
import re
from datetime import datetime

from render import TASK, validate_priority_class


def render_job(namespace, name, gateway_urls, groups, script, ca_certificate, *, rate=100,
               duration=60, warmup=10, expected_backends=None, concurrency=128, priority_class=None,
               measurement_start_utc=None, processes=1, process_sources=None):
    priority_name = validate_priority_class(priority_class)
    if measurement_start_utc is not None:
        if not isinstance(measurement_start_utc, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z", measurement_start_utc):
            raise ValueError("Measurement start must use an explicit ISO 8601 UTC timestamp")
        datetime.fromisoformat(measurement_start_utc[:-1] + "+00:00")
    if not re.fullmatch(r"gateway-tx-lab-[a-z0-9-]+", namespace) or not re.fullmatch(r"load-[a-z0-9-]+", name):
        raise ValueError("Use an explicit disposable namespace and unique load-* job")
    if not 1 <= rate <= 1000 or not 1 <= duration <= 300 or not 0 <= warmup <= 60 or not 1 <= concurrency <= 512:
        raise ValueError("Run only the bounded benchmark matrix")
    if type(processes) is not int or processes not in (1, 8):
        raise ValueError("Use one or eight load processes")
    if processes == 8:
        if (rate not in (100, 1000) or type(concurrency) is not int or concurrency % 8 or
                not isinstance(process_sources, dict) or set(process_sources) != {"load_multiprocess.py", "load_aggregate.py"} or
                any(not isinstance(value, str) or not value for value in process_sources.values())):
            raise ValueError("Bundle both process modules with a supported divisible profile")
    elif process_sources is not None:
        raise ValueError("Additional process sources require explicit eight-process mode")
    if not 2 <= len(gateway_urls) <= 100 or not groups or any(not url.startswith("https://") for url in gateway_urls):
        raise ValueError("Address two to one hundred verified Gateway HTTPS endpoints")
    if "-----BEGIN CERTIFICATE-----" not in ca_certificate or "PRIVATE KEY" in ca_certificate:
        raise ValueError("Mount only the public lab CA")
    labels = {"task": TASK, "app": "load-generator"}
    config = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": name, "namespace": namespace, "labels": labels},
              "data": {"load_open_loop.py": script, "ca.pem": ca_certificate}}
    if processes == 8:
        config["data"].update(process_sources)
    environment = {"TX_GATEWAY_URLS": ",".join(gateway_urls), "TX_LOAD_ROUTING_GROUPS": ",".join(groups),
                   "TX_ALLOW_FIXTURE_MUTATION": "yes", "TX_CA_FILE": "/source/ca.pem", "TX_TLS_SERVER_NAME": "gateway",
                   "TX_LOAD_EXPECTED_BACKENDS": json.dumps(expected_backends or {})}
    launch = ("import base64,os,runpy,secrets; "
              "os.environ['TX_QUERY_AUTHORIZATION']='Basic '+base64.b64encode(('user:'+secrets.token_hex(24)).encode()).decode(); "
              "runpy.run_path('/source/load_open_loop.py',run_name='__main__')")
    if processes == 8:
        launch = "import sys; sys.path.insert(0,'/source'); " + launch
    job = {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": name, "namespace": namespace, "labels": labels},
           "spec": {"backoffLimit": 0, "activeDeadlineSeconds": duration + warmup + 180 + (600 if measurement_start_utc else 0), "ttlSecondsAfterFinished": 3600,
                    "template": {"metadata": {"labels": labels}, "spec": {
                        "restartPolicy": "Never", "automountServiceAccountToken": False, "preemptionPolicy": "Never", "priorityClassName": priority_name,
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
    if measurement_start_utc:
        job["spec"]["template"]["spec"]["containers"][0]["args"].extend(["--measurement-start-utc", measurement_start_utc])
    if processes == 8:
        job["spec"]["template"]["spec"]["containers"][0]["args"].extend(["--processes", "8"])
    return {"apiVersion": "v1", "kind": "List", "items": [config, job]}
