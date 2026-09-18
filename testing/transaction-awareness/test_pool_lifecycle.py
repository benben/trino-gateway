"""Pooled member lifecycle across two real Gateway processes sharing one PostgreSQL instance.

Black box: every assertion is made through the HTTP protocol of two independently started Gateway
JVMs against synthetic coordinators. Nothing here starts Kubernetes, a real coordinator or a real
credential store, so this proves the Gateway's own multi-process serialization and routing, not
backend authentication or any timing property.
"""

import base64
import json
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

from protocol import finish, request, through_gateway
from test_rollout_api import local_gateways

# The pool identity is the routing group the Gateway backends are registered in.
POOL = "cell"
MEMBERS = ("m0", "m1", "m2", "m3", "m4")
POOL_CONFIG = {"enabled": True}
CREDENTIAL = [("Authorization", "Basic " + base64.b64encode(b"warehouse:disposable").decode())]


class PoolLifecycleContract(unittest.TestCase):
    OWNER = "controller-a"

    def setUp(self):
        self.admissions = {}

    def pool_api(self, gateways, token):
        def api(path, method="GET", body=None, replica=0):
            return request(
                gateways[replica] + "/gateway/v1/pools/" + POOL + path,
                method,
                json.dumps(body) if body is not None else None,
                [("Authorization", "Bearer " + token), ("Content-Type", "application/json")])
        return api

    @staticmethod
    def identity(backend):
        return {"url": f"http://127.0.0.1:{backend.server_port}",
                "nodeId": backend.state.node_id,
                "coordinatorId": backend.state.coordinator_id,
                "instanceId": backend.state.identity}

    def successful(self, response):
        self.assertEqual(response.status, 200, response.body)
        return response.json()

    def refused(self, response, code):
        self.assertIn(response.status, (409, 503), response.body)
        self.assertEqual(response.values("X-Trino-Gateway-Error"), [code], response.body)

    def configure(self, api, epoch, step, owner="controller-a", replica=0, **overrides):
        body = {"operationId": "op-configure", "stepId": step, "controllerEpoch": epoch,
                "ownerIdentity": owner, "apiMode": "POOLED", "minServing": 2, "desiredMembers": 3,
                "maxSurge": 1, "maxRepair": 1, "desiredRevision": "r-1", "tenantAdmissionEnabled": False}
        body.update(overrides)
        return api("", "PUT", body, replica=replica)

    def bootstrap(self, api, backend, epoch, operation, replica=0):
        """Registers and certifies one member, returning its ACTIVE member record."""
        member = self.identity(backend)
        registered = self.successful(api("/members", "POST", {
            "operationId": operation, "stepId": "register", "controllerEpoch": epoch, "ownerIdentity": self.OWNER,
            "instanceId": member["instanceId"], "backendName": member["instanceId"],
            "url": member["url"], "externalUrl": member["url"],
            "podUid": "pod-" + member["instanceId"], "bootId": "boot-" + member["instanceId"],
            "configRevision": "r-1"}, replica=replica))
        self.assertEqual(registered["phase"], "PREPARING")
        self.assertFalse(registered["eligible"])
        # The admission body is kept so a lost response can be resolved by resending it verbatim.
        body = self.admission(member, epoch, registered["generation"], operation)
        self.admissions[member["instanceId"]] = body
        return self.successful(api("/members/" + member["instanceId"] + "/admit", "POST", body, replica=replica))

    @staticmethod
    def admission(member, epoch, generation, operation):
        return {"operationId": operation, "stepId": "admit", "controllerEpoch": epoch,
                "ownerIdentity": PoolLifecycleContract.OWNER,
                "expectedGeneration": generation,
                "receipt": {"certificateHash": "a" * 64, "configRevision": "r-1", "authRevision": "auth-1",
                            "podUid": "pod-" + member["instanceId"], "bootId": "boot-" + member["instanceId"],
                            "nodeId": member["nodeId"], "coordinatorId": member["coordinatorId"],
                            "readyWorkers": 3,
                            "checks": ["image", "workers", "catalog-revision", "auth-revision"]}}

    def served_by(self, gateways, statement="SELECT 1", replica=0):
        """Runs an independent statement to completion and returns the member identity that served it."""
        submitted = request(gateways[replica] + "/v1/statement", "POST", statement, CREDENTIAL)
        self.assertEqual(submitted.status, 200, submitted.body)
        pages = finish(submitted, gateways[replica])
        for page in pages:
            self.assertEqual(page.status, 200, page.body)
            self.assertNotIn("error", page.json())
        data = [page.json()["data"] for page in pages if page.json().get("data")]
        self.assertTrue(data, "A completed statement returned no data")
        return data[-1][0][0]

    def test_bootstrap_surge_floor_and_leader_takeover_across_replicas(self):
        with local_gateways(pool=POOL_CONFIG, backend_names=MEMBERS) as (gateways, backends, token):
            api = self.pool_api(gateways, token)
            epoch = 7
            state = self.successful(self.configure(api, epoch, "configure"))
            self.assertEqual(state["apiMode"], "POOLED")
            self.assertEqual(state["controllerEpoch"], epoch)
            self.assertEqual(state["minServing"], 2)

            # Bootstrap is not deadlocked by the serving floor: three members join from both replicas.
            active = {}
            for index, backend in enumerate(backends[:3]):
                member = self.bootstrap(api, backend, epoch, "op-boot-" + backend.state.identity, replica=index % 2)
                self.assertEqual(member["phase"], "ACTIVE")
                self.assertTrue(member["eligible"])
                active[member["instanceId"]] = member
            observed = self.successful(api("", replica=1))
            self.assertEqual(observed["servingMembers"], 3)
            self.assertEqual(observed["liveMembers"], 3)

            # One surge replacement is allowed above the desired count; a second is not.
            surge = self.bootstrap(api, backends[3], epoch, "op-boot-surge", replica=1)
            self.assertEqual(surge["phase"], "ACTIVE")
            self.assertEqual(self.successful(api(""))["surgeInUse"], 1)
            over = self.identity(backends[4])
            self.refused(api("/members", "POST", {
                "operationId": "op-boot-over", "stepId": "register", "controllerEpoch": epoch, "ownerIdentity": self.OWNER,
                "instanceId": over["instanceId"], "backendName": over["instanceId"],
                "url": over["url"], "externalUrl": over["url"],
                "podUid": "pod-over", "bootId": "boot-over", "configRevision": "r-1"}), "POOL_SURGE_BUDGET")

            # A lost admission response is resolvable from the other replica by resending the body.
            first = backends[0].state.identity
            replayed = self.successful(api("/members/" + first + "/admit", "POST", self.admissions[first], replica=1))
            self.assertTrue(replayed["replayed"])
            self.assertEqual(replayed["incarnation"], active[first]["incarnation"])
            self.assertEqual(replayed["generation"], active[first]["generation"])

            # Competing planned drains cannot spend the same capacity. Four are ACTIVE and the floor is
            # two, so at most two of three concurrent drains may be granted.
            members = {member["instanceId"]: member for member in self.successful(api("/members", replica=1))}
            requested = [name for name in (backends[index].state.identity for index in range(3))]
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(api, "/members/" + name + "/drain", "POST",
                                           {"operationId": "op-drain-" + name, "stepId": "drain",
                                            "controllerEpoch": epoch, "ownerIdentity": self.OWNER,
                                            "expectedGeneration": members[name]["generation"]},
                                           index % 2)
                           for index, name in enumerate(requested)]
                results = [future.result() for future in futures]
            self.assertEqual(sorted(result.status for result in results), [200, 200, 409])
            for result in results:
                if result.status == 409:
                    self.assertEqual(result.values("X-Trino-Gateway-Error"), ["POOL_SERVING_FLOOR"], result.body)
            after = self.successful(api("", replica=1))
            self.assertEqual(after["servingMembers"], 2)
            self.assertEqual(after["counts"]["DRAINING"], 2)

            # Only serving members are dispatched, and both replicas agree on that set.
            serving = {name for name, member in
                       {member["instanceId"]: member for member in self.successful(api("/members"))}.items()
                       if member["phase"] == "ACTIVE"}
            self.assertEqual(len(serving), 2)
            for replica in (0, 1):
                for _ in range(4):
                    self.assertIn(self.served_by(gateways, replica=replica), serving)

            # A new leader configures with its own step identity, and the epoch it reports is real.
            taken = self.successful(self.configure(api, epoch + 1, "configure-takeover", owner="controller-b", replica=1))
            self.assertEqual(taken["controllerEpoch"], epoch + 1)
            self.assertFalse(taken["replayed"])
            resent = self.successful(self.configure(api, epoch + 1, "configure-takeover", owner="controller-b"))
            self.assertTrue(resent["replayed"])
            self.assertEqual(resent["controllerEpoch"], epoch + 1)
            # The predecessor's epoch cannot mutate the pool any more.
            self.refused(self.configure(api, epoch, "configure-stale"), "POOL_STALE_EPOCH")
            self.assertEqual(self.successful(api("", replica=1))["controllerEpoch"], epoch + 1)

    def test_transactions_and_continuations_stay_pinned_while_a_member_drains(self):
        with local_gateways(pool=POOL_CONFIG, backend_names=MEMBERS[:3]) as (gateways, backends, token):
            api = self.pool_api(gateways, token)
            epoch = 3
            self.successful(self.configure(api, epoch, "configure"))
            members = {}
            for index, backend in enumerate(backends):
                member = self.bootstrap(api, backend, epoch, "op-boot-" + backend.state.identity, replica=index % 2)
                members[member["instanceId"]] = member
            self.assertEqual(self.successful(api(""))["servingMembers"], 3)
            by_coordinator = {backend.state.coordinator_id: backend.state.identity for backend in backends}

            # A transaction opened through one replica binds one member.
            started = request(gateways[0] + "/v1/statement", "POST", "START TRANSACTION", CREDENTIAL)
            self.assertEqual(started.status, 200, started.body)
            transaction = started.values("X-Trino-Started-Transaction-Id")
            self.assertEqual(len(transaction), 1)
            for page in finish(started, gateways[0]):
                self.assertEqual(page.status, 200, page.body)
            owner = by_coordinator[started.json()["id"].rsplit("_", 1)[1]]

            # An independent statement submitted before the drain leaves a continuation behind.
            pending = request(gateways[0] + "/v1/statement", "POST", "SELECT 1",
                              CREDENTIAL + [("X-Trino-Transaction-Id", transaction[0])])
            self.assertEqual(pending.status, 200, pending.body)
            self.assertEqual(by_coordinator[pending.json()["id"].rsplit("_", 1)[1]], owner)
            continuation = pending.json()["nextUri"]

            # Draining the owner stops new independent work there without disturbing what it holds.
            drained = self.successful(api("/members/" + owner + "/drain", "POST",
                                          {"operationId": "op-drain", "stepId": "drain", "controllerEpoch": epoch,
                                           "ownerIdentity": self.OWNER,
                                           "expectedGeneration": members[owner]["generation"]}, replica=1))
            self.assertEqual(drained["phase"], "DRAINING")
            self.assertFalse(drained["eligible"])

            # The retained continuation still completes, through the other replica.
            page = request(through_gateway(continuation, gateways[1]))
            self.assertEqual(page.status, 200, page.body)
            self.assertEqual(page.json()["id"], pending.json()["id"])
            self.assertNotIn("error", page.json())
            for page in finish(page, gateways[1]):
                self.assertEqual(page.status, 200, page.body)
                self.assertNotIn("error", page.json())

            # A further statement inside the bound transaction stays on the draining member, from either
            # replica, while new independent work never lands there.
            for replica in (1, 0):
                inside = request(gateways[replica] + "/v1/statement", "POST", "SELECT 2",
                                 CREDENTIAL + [("X-Trino-Transaction-Id", transaction[0])])
                self.assertEqual(inside.status, 200, inside.body)
                self.assertEqual(by_coordinator[inside.json()["id"].rsplit("_", 1)[1]], owner)
                for page in finish(inside, gateways[replica]):
                    self.assertEqual(page.status, 200, page.body)
                    self.assertNotIn("error", page.json())
            for replica in (0, 1):
                for _ in range(4):
                    self.assertNotEqual(self.served_by(gateways, replica=replica), owner)

            # Committing through the other replica releases the binding, and the member then seals.
            committed = request(gateways[1] + "/v1/statement", "POST", "COMMIT",
                                CREDENTIAL + [("X-Trino-Transaction-Id", transaction[0])])
            self.assertEqual(committed.status, 200, committed.body)
            pages = finish(committed, gateways[1])
            self.assertTrue(any(page.values("X-Trino-Clear-Transaction-Id") for page in pages))
            deadline = time.monotonic() + 10
            while not self.successful(api("/members/" + owner + "/obligations", replica=1))["readyToSeal"]:
                self.assertLess(time.monotonic(), deadline, "Draining member never became sealable")
                time.sleep(0.1)
            current = self.successful(api("/members/" + owner, replica=1))
            sealed = self.successful(api("/members/" + owner + "/seal", "POST",
                                         {"operationId": "op-seal", "stepId": "seal", "controllerEpoch": epoch,
                                          "ownerIdentity": self.OWNER,
                                          "expectedGeneration": current["generation"]}))
            self.assertEqual(sealed["phase"], "SEALED")
            # A sealed member admits nothing further, including inside a transaction it once held.
            refused = request(gateways[0] + "/v1/statement", "POST", "SELECT 3",
                              CREDENTIAL + [("X-Trino-Transaction-Id", transaction[0])])
            self.assertNotEqual(refused.status, 200)


if __name__ == "__main__":
    unittest.main()
