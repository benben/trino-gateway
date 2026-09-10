"""Run one approved private case; this module does not create or scale resources."""

from contextlib import contextmanager
from datetime import datetime, timezone
import base64
import ipaddress
import json
import math
import os
import re
import secrets
import signal
from urllib.parse import urlsplit


def utc():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def validate(case):
    if not re.fullmatch(r"[a-z0-9-]{1,63}", case["case_id"]):
        raise ValueError("Invalid case identifier")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", case["image_digest"]) or not re.fullmatch(r"[0-9a-f]{40}", case["source_commit"]):
        raise ValueError("Require exact image and source identities")
    if case["replicas"] not in (2, 20, 100) or len(case["pod_inventory"]) != case["replicas"]:
        raise ValueError("Require exact approved replica inventory")
    urls, uids = set(), set()
    for pod in case["pod_inventory"]:
        url = urlsplit(pod["url"])
        ipaddress.ip_address(url.hostname)
        if url.scheme != "https" or url.port != 8443 or url.path or url.query or url.fragment or url.username or url.password:
            raise ValueError("Address each verified Gateway pod directly")
        pin = pod.get("spec_image", "")
        if not re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", pin) or pin.rsplit("@", 1)[1] != case["image_digest"]:
            raise ValueError("Gateway pod spec must pin the measured immutable image index")
        if not pod["uid"] or pod["uid"] in uids or pod["url"] in urls:
            raise ValueError("Gateway inventory is duplicated or has the wrong image")
        uids.add(pod["uid"])
        urls.add(pod["url"])
    groups = case["groups"]
    if not 1 <= len(groups) <= 2 or len({group["name"] for group in groups}) != len(groups):
        raise ValueError("Use one or two independent routing groups")
    for group in groups:
        if any(not isinstance(group.get(key), str) or not group[key] for key in ("name", "source", "target", "source_identity", "target_identity")):
            raise ValueError("Incomplete checkpoint identity")
        if group["source"] == group["target"] or group["source_identity"] == group["target_identity"]:
            raise ValueError("Checkpoint requires distinct real fixture processes")
    if len({name for group in groups for name in (group["source"], group["target"])}) != 2 * len(groups):
        raise ValueError("Independent groups must not alias a fixture")
    if len({identity for group in groups for identity in (group["source_identity"], group["target_identity"])}) != 2 * len(groups):
        raise ValueError("Every fixture response identity must be globally distinct")
    if case["rate"] not in (100, 1000) or case["duration"] not in (10, 60, 300) or case["warmup"] not in (0, 10):
        raise ValueError("Case is outside the approved bounded matrix")
    if type(case.get("concurrency", 128)) is not int or not 1 <= case.get("concurrency", 128) <= 512:
        raise ValueError("Invalid client concurrency")


@contextmanager
def phase_deadline(seconds):
    def expired(signum, frame):
        raise TimeoutError("Checkpoint phase deadline exceeded; preserve all state")
    previous = signal.signal(signal.SIGALRM, expired)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def run(case, authorization, admin_token, runner, checkpoint_factory, is_invalid, phase=phase_deadline):
    validate(case)
    if not authorization or not admin_token:
        raise ValueError("Runtime authorization is required")
    gateways = [pod["url"] for pod in case["pod_inventory"]]
    groups = [group["name"] for group in case["groups"]]
    options = dict(rate=case["rate"], concurrency=case.get("concurrency", 128), timeout=10,
                   expected_backends={group["name"]: group["source_identity"] for group in case["groups"]})
    result = {"case_id": case["case_id"], "image_digest": case["image_digest"], "source_commit": case["source_commit"],
              "replicas": case["replicas"], "rate": case["rate"], "valid_run": False, "workload_checks_passed": False,
              "external_postguard_required": True, "checkpoints": [], "stages": [], "started_utc": utc()}
    checkpoints = []
    stage = "warmup"
    try:
        result["stages"].append({"name": stage, "start_utc": utc()})
        result["warmup"] = runner(gateways, groups, authorization, duration=case["warmup"], **options).run() if case["warmup"] else None
        result["stages"][-1]["end_utc"] = utc()
        if is_invalid(result["warmup"]):
            raise AssertionError("Warmup failed; measured traffic was not started")
        stage = "checkpoint_begin"
        result["stages"].append({"name": stage, "start_utc": utc()})
        with phase(60):
            for group in case["groups"]:
                checkpoint = checkpoint_factory(gateways, group["name"], group["source"], group["target"],
                                                group["source_identity"], group["target_identity"], authorization, admin_token)
                checkpoints.append(checkpoint)
                checkpoint.begin()
        result["stages"][-1]["end_utc"] = utc()
        stage = "measurement_and_continuation_cleanup"
        result["stages"].append({"name": stage, "start_utc": utc()})
        result["measurement"] = runner(gateways, groups, authorization, duration=case["duration"],
                                       measurement_start_utc=case.get("measurement_start_utc"), **options).run()
        result["stages"][-1]["end_utc"] = utc()
        actual = result["measurement"].get("successful_http_rps")
        target_achieved = type(actual) in (int, float) and math.isfinite(actual) and actual >= .99 * case["rate"]
        result["target_acceptance"] = {"target_http_rps": case["rate"], "successful_http_rps": actual,
                                       "minimum_fraction": .99, "minimum_successful_http_rps": .99 * case["rate"],
                                       "target_achieved": target_achieved}
        result["transport_valid"] = not is_invalid(result["measurement"])
        result["throughput_valid"] = result["transport_valid"] and target_achieved
        stage = "checkpoint_finish"
        result["stages"].append({"name": stage, "start_utc": utc()})
        with phase(600):
            for checkpoint in checkpoints:
                result["checkpoints"].append(checkpoint.finish())
        result["stages"][-1]["end_utc"] = utc()
        result["checkpoints_valid"] = True
        result["workload_checks_passed"] = result["throughput_valid"]
    except Exception as error:
        result["failure"] = {"stage": stage, "type": type(error).__name__, "time_utc": utc()}
        result["state_preserved_without_unconditional_cleanup"] = True
    result["completed_utc"] = utc()
    return result


if __name__ == "__main__":
    from load_checkpoints import Checkpoints
    from load_open_loop import OpenLoop, invalid
    if os.environ.get("TX_ALLOW_FIXTURE_MUTATION") != "yes":
        raise SystemExit("Explicit disposable fixture authorization is required")
    with open("/source/case.json") as stream:
        case = json.load(stream)
    authorization = "Basic " + base64.b64encode(("user:" + secrets.token_hex(24)).encode()).decode()
    result = run(case, authorization, os.environ["TX_ADMIN_TOKEN"], OpenLoop, Checkpoints, invalid)
    print(json.dumps(result), flush=True)
    raise SystemExit(0 if result["workload_checks_passed"] else 1)
