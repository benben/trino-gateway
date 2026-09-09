"""Partial-stage cancellation keeps query and transaction drain obligations."""

import os
import unittest

from protocol import request, through_gateway
from test_gateway import GatewayFixture


class PartialCancellationContract(GatewayFixture):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        if os.environ.get("TX_ALLOW_IRREVERSIBLE_FAULTS") != "yes":
            raise RuntimeError("Partial-cancellation tests require an expendable fixture and explicit fault opt-in")

    def tearDown(self):
        self.configure(0, partial_cancel=False)
        super().tearDown()

    def partial_query(self):
        transaction = self.start()
        self.configure(0, partial_cancel=True)
        initial = self.submit("SELECT 1", transaction)
        self.assertEqual(initial.status, 200, initial.body)
        self.assertIn("partialCancelUri", initial.json())
        return transaction, initial

    def test_advertised_delete_stays_on_owner_and_cannot_retire_backend(self):
        transaction, initial = self.partial_query()
        self.activate(1)
        before = self.backend_status().json()["pendingRequests"]
        other = len(self.state(1)["requests"])
        stages = len(self.state(0)["cancelledStages"])
        response = request(through_gateway(initial.json()["partialCancelUri"], self.gateways[1]), "DELETE")
        self.assertEqual(response.status, 204, response.body)
        self.assertEqual(len(self.state(1)["requests"]), other)
        self.assertEqual(len(self.state(0)["cancelledStages"]), stages + 1)
        self.assertIn(transaction, self.state(0)["transactions"])
        status = self.backend_status().json()
        self.assertEqual(status["pendingRequests"], before)
        self.assertGreaterEqual(status["openTransactions"], 1)
        self.assertGreaterEqual(status["activeQueries"], 1)
        path = "/gateway/transactions/backends/" + self.names[0]
        draining = self.admin(path + "/drain", "POST")
        self.assertEqual(draining.status, 200, draining.body)
        status = self.backend_status().json()
        self.assertEqual(status["pendingRequests"], before)
        self.assertFalse(status["readyToSeal"])
        self.assertFalse(status["drained"])
        sealed = self.admin(path + "/seal", "POST", {"generation": status["generation"]})
        self.assertEqual(sealed.status, 409, sealed.body)
        self.complete(initial, gateway=1)
        self.complete(self.submit("ROLLBACK", transaction, gateway=1), gateway=1)
        self.assertEqual(self.backend_status().json()["pendingRequests"], before)

    def test_forged_stage_is_rejected_before_forwarding(self):
        self.reject_forgery("/1/token/1", "/2/token/1")

    def test_forged_slug_is_rejected_before_forwarding(self):
        self.reject_forgery("/token/", "/forged/")

    def reject_forgery(self, original, replacement):
        _, initial = self.partial_query()
        uri = through_gateway(initial.json()["partialCancelUri"], self.gateways[1]).replace(original, replacement)
        before = [len(self.state(index)["requests"]) for index in (0, 1)]
        pending = self.backend_status().json()["pendingRequests"]
        try:
            response = request(uri, "DELETE")
            self.assertGreaterEqual(response.status, 400, response.body)
            self.assertLess(response.status, 500, response.body)
            self.assertEqual([len(self.state(index)["requests"]) for index in (0, 1)], before)
            self.assertEqual(self.backend_status().json()["pendingRequests"], pending)
        finally:
            self.complete(initial)

    def test_unsupported_partial_cancel_methods_do_not_clear_obligations(self):
        transaction, initial = self.partial_query()
        uri = through_gateway(initial.json()["partialCancelUri"], self.gateways[1])
        pending = self.backend_status().json()["pendingRequests"]
        for method in ("GET", "HEAD"):
            with self.subTest(method=method):
                response = request(uri, method)
                self.assertEqual(response.status, 405, response.body)
                self.assertIn(transaction, self.state(0)["transactions"])
        self.assertGreaterEqual(self.backend_status().json()["pendingRequests"], pending + 2)
        self.complete(initial)


if __name__ == "__main__":
    unittest.main()
