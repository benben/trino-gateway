"""Routing-group placement changes preserve existing transaction and query ownership."""

import os
import unittest

from protocol import request, statement
from test_gateway import GatewayFixture


class CrossGroupContract(GatewayFixture):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.destination_url = os.environ.get("TX_DESTINATION_BACKEND_URL", "").rstrip("/")
        cls.destination_name = os.environ.get("TX_DESTINATION_BACKEND_NAME", "")
        cls.destination_group = os.environ.get("TX_DESTINATION_ROUTING_GROUP", "")
        if not cls.destination_url or not cls.destination_name or not cls.destination_group:
            raise RuntimeError("Cross-group tests require a third backend URL, name and routing group")
        if cls.destination_url in cls.backends or cls.destination_name in cls.names or cls.destination_group == cls.group:
            raise RuntimeError("The destination must use a distinct process, backend name and routing group")

    def setUp(self):
        super().setUp()
        destination = request(self.destination_url + "/__test/state")
        self.assertEqual(destination.status, 200, destination.body)
        self.destination_identity = destination.json()["identity"]
        process = destination.json()["coordinatorId"]
        self.assertNotIn(process, [self.state(index)["coordinatorId"] for index in (0, 1)])
        self.assertNotIn(self.destination_identity, [self.state(index)["identity"] for index in (0, 1)])
        status_path = "/gateway/transactions/backends/" + self.destination_name
        status = self.admin(status_path + "/drain")
        if status.status == 200:
            resumed = self.admin(status_path + "/resume", "POST", {"generation": status.json()["generation"]})
            self.assertEqual(resumed.status, 200, resumed.body)
        else:
            self.assertEqual(status.status, 404, status.body)
        cutover = self.admin("/gateway/transactions/cutover", "POST",
                             {"routingGroup": self.destination_group, "backendName": self.destination_name})
        self.assertEqual(cutover.status, 200, cutover.body)

    def destination_submit(self, sql, transaction="NONE", gateway=0):
        return statement(self.gateways[gateway], sql, transaction, "user", self.destination_group,
                         [("Authorization", self.authorization)])

    def test_new_group_moves_new_queries_but_preserves_open_transaction(self):
        transaction = self.start()
        for gateway in range(len(self.gateways)):
            with self.subTest(gateway=gateway):
                fresh = self.complete(self.destination_submit("SELECT 1", gateway=gateway), gateway)
                self.assertEqual(fresh[-1].json()["data"], [[self.destination_identity]])
                existing = self.complete(self.destination_submit("SELECT 1", transaction, gateway), gateway)
                self.assertEqual(existing[-1].json()["data"], [[self.state(0)["identity"]]])

    def test_original_result_continuation_remains_on_old_group(self):
        initial = self.submit("SELECT 1")
        self.assertEqual(initial.status, 200, initial.body)
        fresh = self.complete(self.destination_submit("SELECT 1", gateway=1), gateway=1)
        self.assertEqual(fresh[-1].json()["data"], [[self.destination_identity]])
        original = self.complete(initial, gateway=1)
        self.assertEqual(original[-1].json()["data"], [[self.state(0)["identity"]]])

    def test_commit_with_new_group_closes_original_transaction(self):
        self.close_original_transaction("COMMIT")

    def test_rollback_with_new_group_closes_original_transaction(self):
        self.close_original_transaction("ROLLBACK")

    def close_original_transaction(self, sql):
        transaction = self.start()
        before = len(request(self.destination_url + "/__test/state").json()["requests"])
        pages = self.complete(self.destination_submit(sql, transaction, gateway=1), gateway=1)
        self.assertTrue(any(page.values("X-Trino-Clear-Transaction-Id") for page in pages))
        self.assertEqual(len(request(self.destination_url + "/__test/state").json()["requests"]), before)
        for gateway in range(len(self.gateways)):
            closed = self.admin("/gateway/transactions/" + transaction, gateway=gateway)
            self.assertEqual(closed.status, 200, closed.body)
            self.assertEqual(closed.json()["state"], "CLOSED")
            self.assertEqual(closed.json()["backendName"], self.names[0])


if __name__ == "__main__":
    unittest.main()
