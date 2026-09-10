"""Keep a real protocol transaction open across a disposable load measurement."""

import json
import time

from protocol import finish, request, statement


class Checkpoints:
    def __init__(self, gateways, group, source_name, target_name, source_identity, target_identity, authorization, admin_token):
        self.gateways, self.group = gateways, group
        self.source, self.target = source_name, target_name
        self.source_identity, self.target_identity = source_identity, target_identity
        self.authorization, self.admin_token = authorization, admin_token
        self.transaction = None

    def admin(self, path, method="GET", body=None):
        return request(self.gateways[0] + "/gateway/transactions/" + path, method,
                       json.dumps(body) if body is not None else None,
                       [("Authorization", "Bearer " + self.admin_token), ("Content-Type", "application/json")])

    def query(self, sql, transaction="NONE", index=0):
        response = statement(self.gateways[index], sql, transaction, "user", self.group,
                             [("Authorization", self.authorization)])
        pages = finish(response, self.gateways[(index + 1) % len(self.gateways)])
        for page in pages:
            if page.status != 200 or "error" in page.json():
                raise AssertionError("Load checkpoint query failed")
        return pages

    def begin(self):
        pages = self.query("START TRANSACTION")
        identifiers = {value for page in pages for value in page.values("X-Trino-Started-Transaction-Id")}
        if len(identifiers) != 1:
            raise AssertionError("Load checkpoint needs exactly one real transaction identifier")
        self.transaction = identifiers.pop()
        if self.query("SELECT 1", self.transaction)[-1].json().get("data") != [[self.source_identity]]:
            raise AssertionError("Checkpoint transaction did not start on the expected source")

    def finish(self):
        status = self.admin("backends/" + self.source + "/drain", "POST")
        if status.status != 200 or status.json()["openTransactions"] < 1 or status.json()["drained"]:
            raise AssertionError("Open checkpoint transaction must block drain after load")
        blocked = self.admin("backends/" + self.source + "/seal", "POST", {"generation": status.json()["generation"]})
        if blocked.status != 409:
            raise AssertionError("Seal succeeded with an open checkpoint transaction")
        route = self.admin("cutover", "POST", {"routingGroup": self.group, "backendName": self.target})
        if route.status != 200:
            raise AssertionError("Post-load atomic cutover failed")
        for index in range(len(self.gateways)):
            if self.query("SELECT 1", self.transaction, index)[-1].json().get("data") != [[self.source_identity]]:
                raise AssertionError("Existing transaction moved to the new backend")
            if self.query("SELECT 1", index=index)[-1].json().get("data") != [[self.target_identity]]:
                raise AssertionError("New query did not use the cutover backend")
        self.query("ROLLBACK", self.transaction)
        self.transaction = None
        deadline = time.monotonic() + 150
        while True:
            status = self.admin("backends/" + self.source + "/drain")
            if status.status != 200:
                raise AssertionError("Post-load drain status failed")
            if status.json()["readyToSeal"]:
                break
            if time.monotonic() >= deadline:
                raise AssertionError("Post-load obligations remain; do not reset the ledger to hide this failure")
            time.sleep(.1)
        sealed = self.admin("backends/" + self.source + "/seal", "POST", {"generation": status.json()["generation"]})
        if sealed.status != 200 or not sealed.json()["drained"]:
            raise AssertionError("Final seal failed after all observed obligations completed")
        resumed = self.admin("backends/" + self.source + "/resume", "POST", {"generation": sealed.json()["generation"]})
        restored = self.admin("cutover", "POST", {"routingGroup": self.group, "backendName": self.source})
        if resumed.status != 200 or restored.status != 200:
            raise AssertionError("Could not restore the disposable fixture after verified sealing")
        return {"open_transaction_blocked_seal": True, "existing_transaction_pinned_after_cutover": True,
                "new_queries_used_destination": True, "all_gateway_endpoints_checked": len(self.gateways),
                "observed_final_drain_and_seal": True}
