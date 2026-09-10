"""Verify one bounded routing cutover while background requests continue."""

from collections import Counter
from datetime import timedelta
import hashlib
import json
import re
import threading
import time

from load_checkpoints import Checkpoints
from protocol import request, through_gateway


class QueryOwners:
    def __init__(self, configuration):
        self.coordinators = configuration["coordinator_ids"]
        self.identities = configuration["identities"]
        if (not isinstance(self.coordinators, list) or len(self.coordinators) != 2 or
                any(not isinstance(value, str) or not re.fullmatch(r"[a-z0-9]{5}", value) for value in self.coordinators) or
                len(set(self.coordinators)) != 2 or not isinstance(self.identities, list) or len(self.identities) != 2 or
                any(not isinstance(value, str) or not value for value in self.identities) or len(set(self.identities)) != 2):
            raise ValueError("Require two distinct verified fixture query owners")

    def observe(self, payload, require_row=True):
        identifier = payload.get("id")
        if not isinstance(identifier, str) or not re.fullmatch(r"[0-9]{8}_[0-9]{6}_[0-9]+_[a-z0-9]{5}", identifier):
            raise ValueError("Missing fixture query identifier")
        suffix = identifier.rsplit("_", 1)[1]
        if suffix not in self.coordinators:
            raise ValueError("Unknown fixture query owner")
        owner = self.coordinators.index(suffix)
        if require_row and not payload.get("nextUri") and payload.get("data") != [[self.identities[owner]]]:
            raise ValueError("Terminal result differs from its initial fixture owner")
        return owner, identifier


def process_fingerprint(info):
    values = [info.get("nodeId"), info.get("coordinatorId")]
    if any(not isinstance(value, str) or not value or not value.isascii() for value in values):
        raise ValueError("Incomplete fixture process identity")
    return hashlib.sha256("".join(str(len(value)) + ":" + value for value in values).encode("ascii")).hexdigest()


class CutoverCheckpoint(Checkpoints):
    def __init__(self, *args, fixture_group, offset_seconds=30):
        super().__init__(*args)
        self.fixture_group, self.offset_seconds = fixture_group, offset_seconds
        self.cancelled = threading.Event()
        self.thread = None
        self.deadline = None
        self.failure = None
        self.event = {"proofs": [], "control_http_counts": {}}
        self.http_counts = Counter()
        self.owner_configuration = None
        self.completed = False

    def check_budget(self):
        if self.cancelled.is_set() or (self.deadline is not None and time.monotonic() >= self.deadline):
            raise TimeoutError("Cutover control deadline expired")
        return 2 if self.deadline is None else min(2, self.deadline - time.monotonic())

    def call(self, url, method="GET", body=None, headers=()):
        timeout = self.check_budget()
        self.http_counts[method] += 1
        response = request(url, method, body, headers, timeout=timeout)
        self.check_budget()
        return response

    def admin(self, path, method="GET", body=None):
        return self.call(self.gateways[0] + "/gateway/transactions/" + path, method,
                         json.dumps(body) if body is not None else None,
                         [("Authorization", "Bearer " + self.admin_token), ("Content-Type", "application/json")])

    def submit(self, sql, transaction="NONE", index=0):
        return self.call(self.gateways[index] + "/v1/statement", "POST", sql,
                         [("Authorization", self.authorization), ("X-Trino-User", "user"),
                          ("X-Trino-Transaction-Id", transaction), ("X-Trino-Routing-Group", self.group),
                          ("Content-Type", "text/plain")])

    def pages(self, response, index, require_row, expected=None):
        pages = []
        for _ in range(20):
            self.check_budget()
            if response.status != 200 or "error" in response.json():
                raise AssertionError("Concurrent checkpoint query failed")
            payload = response.json()
            identity = self.owner_policy.observe(payload, require_row=require_row)
            if expected is None:
                expected = identity
            if identity != expected:
                raise AssertionError("Checkpoint continuation changed query or owner")
            pages.append(response)
            if not payload.get("nextUri"):
                return pages
            response = self.call(through_gateway(payload["nextUri"], self.gateways[index]))
        raise AssertionError("Checkpoint exceeded its bounded page count")

    def query(self, sql, transaction="NONE", index=0):
        return self.pages(self.submit(sql, transaction, index), (index + 1) % len(self.gateways),
                          require_row=sql == "SELECT 1")

    def begin(self):
        self.deadline = time.monotonic() + 60
        coordinators = []
        for prefix in ("source", "target"):
            response = self.call(self.fixture_group[prefix + "_url"].rstrip("/") + "/v1/info")
            info = response.json()
            if (response.status != 200 or info.get("coordinator") is not True or info.get("starting") is not False or
                    process_fingerprint(info) != self.fixture_group[prefix + "_process_sha256"]):
                raise AssertionError("Fixture process differs from its verified inventory")
            coordinators.append(info["coordinatorId"])
        self.owner_configuration = {"coordinator_ids": coordinators, "identities": [self.source_identity, self.target_identity]}
        self.owner_policy = QueryOwners(self.owner_configuration)
        super().begin()
        self.retained = self.submit("SELECT 1")
        if self.retained.status != 200 or not self.retained.json().get("nextUri"):
            raise AssertionError("Require an advertised pre-cutover continuation")
        self.retained_identity = self.owner_policy.observe(self.retained.json())
        if self.retained_identity[0] != 0:
            raise AssertionError("Retained continuation did not start on blue")
        self.deadline = None

    def at(self, monotonic):
        return (self.window_utc + timedelta(seconds=monotonic - self.window_start)).isoformat(timespec="microseconds").replace("+00:00", "Z")

    def start(self, common_start, common_end, common_utc):
        if self.thread is not None or self.owner_configuration is None:
            raise ValueError("Cutover control must be prepared and started exactly once")
        self.window_start, self.window_end, self.window_utc = common_start, common_end, common_utc
        self.deadline = common_end - 10
        self.event["setup_http_counts"] = dict(self.http_counts)
        self.http_counts.clear()
        self.thread = threading.Thread(target=self.act, daemon=True)
        self.thread.start()

    def act(self):
        try:
            if self.cancelled.wait(max(0, self.window_start + self.offset_seconds - time.monotonic())):
                raise TimeoutError("Cutover was cancelled before dispatch")
            self.check_budget()
            started = time.monotonic()
            self.event.update(cutover_start_seconds=started - self.window_start, cutover_request_utc=self.at(started))
            response = self.admin("cutover", "POST", {"routingGroup": self.group, "backendName": self.target})
            acknowledged = time.monotonic()
            if response.status != 200:
                raise AssertionError("Concurrent atomic cutover failed")
            route = response.json()
            if (route.get("routingGroup") != self.group or route.get("backendName") != self.target or
                    type(route.get("generation")) is not int or route["generation"] < 1):
                raise AssertionError("Cutover acknowledgement does not match the requested route")
            self.event.update(cutover_ack_seconds=acknowledged - self.window_start, cutover_ack_utc=self.at(acknowledged))
            self.drain_and_block_seal()
            for index in range(len(self.gateways)):
                start = time.monotonic()
                if self.query("SELECT 1", self.transaction, index)[-1].json().get("data") != [[self.source_identity]]:
                    raise AssertionError("Open transaction moved during background traffic")
                if self.query("SELECT 1", index=index)[-1].json().get("data") != [[self.target_identity]]:
                    raise AssertionError("Post-acknowledgement query did not use green")
                self.event["proofs"].append({"gateway_index": index, "start_seconds": start - self.window_start,
                                              "end_seconds": time.monotonic() - self.window_start,
                                              "old_transaction_blue": True, "new_query_green": True})
            retained_start = time.monotonic()
            pages = self.pages(self.retained, 1, True, expected=self.retained_identity)
            if pages[-1].json().get("data") != [[self.source_identity]]:
                raise AssertionError("Pre-cutover continuation changed backend")
            self.check_budget()
            self.event.update(retained_continuation_blue=True,
                              retained_continuation_start_seconds=retained_start - self.window_start,
                              proofs_completed_seconds=time.monotonic() - self.window_start,
                              open_transaction_blocked_seal=True)
            self.completed = True
        except Exception as error:
            self.failure = type(error).__name__
        finally:
            self.event["control_http_counts"] = dict(self.http_counts)

    def finish_control(self):
        if self.thread is None:
            raise AssertionError("Cutover control never started")
        self.thread.join(timeout=max(0, self.deadline - time.monotonic()))
        if self.thread.is_alive():
            self.cancelled.set()
            self.thread.join(timeout=2)
        if self.thread.is_alive() or self.failure is not None or not self.completed:
            raise AssertionError("Cutover control did not complete within its deadline")
        if not 0 < self.event["cutover_start_seconds"] <= self.event["cutover_ack_seconds"] < self.event["proofs_completed_seconds"] <= self.window_end - self.window_start - 10:
            raise AssertionError("Cutover evidence is outside the sustained-load window")
        return dict(self.event)

    def cancel(self):
        self.cancelled.set()
        if self.thread is not None:
            self.thread.join(timeout=2)

    def finish(self):
        self.finish_control()
        self.deadline = time.monotonic() + 180
        self.settle_and_restore()
        return {"open_transaction_blocked_seal": True, "existing_transaction_pinned_after_cutover": True,
                "new_queries_used_destination": True, "all_gateway_endpoints_checked": len(self.gateways),
                "observed_final_drain_and_seal": True, "cutover_during_load": dict(self.event)}
