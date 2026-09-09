"""Fail-closed checks for protocol paths, methods and unsupported result modes."""

import unittest

from protocol import request, through_gateway
from test_gateway import GatewayFixture


class ProtocolBoundaryContract(GatewayFixture):
    def request_count(self):
        return [len(self.state(index)["requests"]) for index in (0, 1)]

    def test_trailing_slash_statement_cannot_bypass_accounting(self):
        before = self.request_count()
        response = request(self.gateways[1] + "/v1/statement/", "POST", "START TRANSACTION",
                           [("Authorization", self.authorization), ("X-Trino-User", "user"),
                            ("X-Trino-Transaction-Id", "NONE"), ("X-Trino-Routing-Group", self.group)])
        self.assertGreaterEqual(response.status, 400, response.body)
        self.assertLess(response.status, 500, response.body)
        self.assertEqual(self.request_count(), before)

    def test_spooling_advertisement_negotiates_inline_results(self):
        before = self.request_count()
        response = self.submit("SELECT 1", extra=[("X-Trino-Query-Data-Encoding", "json+zstd")])
        pages = self.complete(response)
        self.assertIsInstance(pages[-1].json()["data"], list)
        forwarded = [item for index in (0, 1) for item in self.state(index)["requests"][before[index]:]
                     if item["method"] == "POST" and item["path"] == "/v1/statement"]
        self.assertEqual(len(forwarded), 1)
        self.assertEqual(forwarded[0]["queryDataEncoding"], [])

    def test_head_heartbeat_preserves_query_and_transaction(self):
        transaction = self.start()
        initial = self.submit("SELECT 1", transaction)
        self.assertEqual(initial.status, 200, initial.body)
        before = self.backend_status()
        self.assertEqual(before.status, 200, before.body)
        response = request(through_gateway(initial.json()["nextUri"], self.gateways[1]), "HEAD")
        self.assertEqual(response.status, 200, response.body)
        self.assertEqual(response.body, b"")
        after = self.backend_status()
        self.assertEqual(after.status, 200, after.body)
        self.assertEqual(after.json()["pendingRequests"], before.json()["pendingRequests"])
        self.assertEqual(after.json()["openTransactions"], before.json()["openTransactions"])
        self.assertGreaterEqual(after.json()["activeQueries"], 1)
        self.complete(initial)

    def test_put_continuation_is_rejected_before_admission(self):
        self.reject_method("PUT")

    def test_legacy_backend_toggles_cannot_bypass_durable_routing(self):
        for action in ("deactivate", "activate"):
            with self.subTest(action=action):
                response = self.admin("/gateway/backend/" + action + "/" + self.names[0], "POST")
                self.assertEqual(response.status, 409, response.body)

    def test_legacy_active_flag_update_cannot_bypass_durable_routing(self):
        current = self.admin("/gateway/backend/all")
        self.assertEqual(current.status, 200, current.body)
        backend = next(value for value in current.json() if value["name"] == self.names[0])
        backend["active"] = not backend["active"]
        response = self.admin("/gateway/backend/modify/update", "POST", backend)
        self.assertEqual(response.status, 409, response.body)

    def reject_method(self, method):
        initial = self.submit("SELECT 1")
        self.assertEqual(initial.status, 200, initial.body)
        before = self.request_count()
        try:
            response = request(through_gateway(initial.json()["nextUri"], self.gateways[1]), method)
            self.assertGreaterEqual(response.status, 400, response.body)
            self.assertLess(response.status, 500, response.body)
            self.assertEqual(self.request_count(), before)
        finally:
            self.complete(initial)


if __name__ == "__main__":
    unittest.main()
