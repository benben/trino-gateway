"""Compose one private case Job from the repository's bounded load renderer."""

import json
import re

from matrix_case import validate


def compose(renderer, namespace, name, case, sources, ca_certificate, *, priority_class,
            admin_secret, admin_secret_key):
    validate(case)
    expected = {"load_open_loop.py", "protocol.py", "load_checkpoints.py", "matrix_case.py"}
    if set(sources) != expected or any(not isinstance(source, str) or not source for source in sources.values()):
        raise ValueError("Bundle the reviewed load, TLS protocol, checkpoint, and wrapper sources")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,252}", admin_secret) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,253}", admin_secret_key):
        raise ValueError("Use an existing namespace-local synthetic admin Secret reference")
    rendered = renderer(namespace, name, [pod["url"] for pod in case["pod_inventory"]],
                        [group["name"] for group in case["groups"]], sources["load_open_loop.py"], ca_certificate,
                        rate=case["rate"], duration=case["duration"], warmup=case["warmup"],
                        expected_backends={group["name"]: group["source_identity"] for group in case["groups"]},
                        concurrency=case.get("concurrency", 128), priority_class=priority_class,
                        measurement_start_utc=case.get("measurement_start_utc"))
    config, job = rendered["items"]
    if config["kind"] != "ConfigMap" or job["kind"] != "Job":
        raise ValueError("Unexpected renderer output")
    config["data"].update(sources)
    config["data"]["case.json"] = json.dumps(case, sort_keys=True)
    container = job["spec"]["template"]["spec"]["containers"][0]
    container["command"] = ["python", "/source/matrix_case.py"]
    container["args"] = []
    container["env"].append({"name": "TX_ADMIN_TOKEN", "valueFrom": {"secretKeyRef": {"name": admin_secret, "key": admin_secret_key}}})
    job["spec"]["activeDeadlineSeconds"] += 240 + 60 + 600
    if job["spec"]["activeDeadlineSeconds"] > 2000:
        raise ValueError("Case exceeds the bounded orchestration deadline")
    return rendered
