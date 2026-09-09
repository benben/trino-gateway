"""Irreversible fault assertions. Run last, or use a fresh lab for each method."""

import concurrent.futures
import json
import os
import time
import unittest

from protocol import finish, request, through_gateway
from test_gateway import GatewayFixture


class FaultContract(GatewayFixture):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        if os.environ.get("TX_ALLOW_IRREVERSIBLE_FAULTS") != "yes":
            raise RuntimeError("Set TX_ALLOW_IRREVERSIBLE_FAULTS=yes only when the fixture can be discarded afterward")

    def tearDown(self):
        if getattr(self, "coordinator_restarted", False):
            for backend in self.backends:
                request(backend + "/__test/release", "POST", "{}")
            return
        super().tearDown()

    def pending_count(self):
        state = self.backend_status()
        self.assertEqual(state.status, 200, state.body)
        return state.json()["pendingRequests"]

    def assert_new_uncertainty(self, before):
        state = self.backend_status(gateway=1)
        self.assertEqual(state.status, 200, state.body)
        self.assertGreaterEqual(state.json()["pendingRequests"], before + 1)
        path = "/gateway/transactions/backends/" + self.names[0]
        response = self.admin(path + "/drain", "POST")
        self.assertEqual(response.status, 200, response.body)
        try:
            state = self.backend_status(gateway=1)
            self.assertFalse(state.json()["readyToSeal"])
            self.assertFalse(state.json()["drained"])
            sealed = self.admin(path + "/seal", "POST", {"generation": state.json()["generation"]})
            self.assertEqual(sealed.status, 409, sealed.body)
        finally:
            self.resume()

    def test_conflicting_start_response_headers_fail_closed(self):
        self.check_ambiguous_start("conflict")

    def test_duplicate_next_uri_cannot_mark_query_terminal(self):
        self.configure(0, duplicate_next_uri=True)
        initial = self.submit("SELECT 1")
        self.assertEqual(initial.status, 200, initial.body)
        before = self.pending_count()
        response = request(through_gateway(initial.json()["nextUri"], self.gateways[1]))
        self.assertIn(response.status, (502, 503), response.body)
        self.assert_new_uncertainty(before)

    def test_duplicate_start_response_headers_fail_closed(self):
        self.check_ambiguous_start("same")

    def check_ambiguous_start(self, mode):
        self.configure(0, duplicate_start_headers=mode)
        before = self.pending_count()
        response = self.submit("START TRANSACTION")
        self.assertGreaterEqual(response.status, 400, response.body)
        self.assertEqual(response.values("X-Trino-Started-Transaction-Id"), [])
        self.assert_new_uncertainty(before)

    def test_lost_start_response_retains_admission(self):
        self.configure(0, drop_start_response=True)
        before = self.pending_count()
        transactions_before = len(self.state(0)["transactions"])
        response = self.submit("START TRANSACTION")
        self.assertGreaterEqual(response.status, 400, response.body)
        self.assertEqual(len(self.state(0)["transactions"]), transactions_before + 1)
        self.assert_new_uncertainty(before)

    def test_malformed_terminal_response_does_not_complete_query(self):
        self.configure(0, malformed_terminal=True)
        initial = self.submit("SELECT 1")
        self.assertEqual(initial.status, 200, initial.body)
        before = self.pending_count()
        response = request(through_gateway(initial.json()["nextUri"], self.gateways[1]))
        self.assertGreaterEqual(response.status, 400, response.body)
        self.assert_new_uncertainty(before)

    def test_trailing_json_bytes_do_not_complete_query(self):
        self.configure(0, terminal_trailing_bytes=True)
        initial = self.submit("SELECT 1")
        self.assertEqual(initial.status, 200, initial.body)
        before = self.pending_count()
        response = request(through_gateway(initial.json()["nextUri"], self.gateways[1]))
        self.assertGreaterEqual(response.status, 400, response.body)
        self.assert_new_uncertainty(before)

    def test_408_after_possible_acceptance_is_not_an_auth_rejection(self):
        self.configure(0, initial_status=408)
        before = self.pending_count()
        transactions = len(self.state(0)["transactions"])
        response = self.submit("START TRANSACTION")
        self.assertGreaterEqual(response.status, 400, response.body)
        self.assertEqual(len(self.state(0)["transactions"]), transactions + 1)
        self.assert_new_uncertainty(before)

    def test_cancel_racing_result_keeps_uncertainty(self):
        initial = self.submit("SELECT 1")
        self.assertEqual(initial.status, 200, initial.body)
        uri = initial.json()["nextUri"]
        self.configure(0, hold_poll=True)
        before_requests = len(self.state(0)["requests"])
        before_pending = self.pending_count()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(request, through_gateway(uri, self.gateways[0]))
            try:
                deadline = time.monotonic() + 10
                while len(self.state(0)["requests"]) == before_requests:
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.01)
                cancelled = request(through_gateway(uri, self.gateways[1]), "DELETE")
                self.assertEqual(cancelled.status, 204, cancelled.body)
            finally:
                request(self.backends[0] + "/__test/release", "POST", '{"kind": "poll"}')
                response = pending.result(timeout=10)
                self.assertEqual(response.status, 200, response.body)
        self.assert_new_uncertainty(before_pending)

    def test_same_transaction_id_from_another_backend_cannot_replace_owner(self):
        transaction = self.start()
        self.activate(1)
        self.configure(1, force_transaction_id=transaction)
        response = self.submit("START TRANSACTION", gateway=1)
        self.assertGreaterEqual(response.status, 400, response.body)
        self.assertEqual(response.values("X-Trino-Started-Transaction-Id"), [])
        self.assertEqual(self.select_backend(transaction, gateway=1), self.state(0)["identity"])

    def test_z_restarted_coordinator_cannot_inherit_old_transaction(self):
        transaction = self.start()
        old = self.state(0)
        restarted = request(self.backends[0] + "/__test/restart", "POST", "{}")
        self.coordinator_restarted = True
        self.assertEqual(restarted.status, 200, restarted.body)
        self.assertNotEqual(restarted.json()["coordinatorId"], old["coordinatorId"])
        before = self.submissions()
        response = self.submit("SELECT 1", transaction, gateway=1)
        self.assertGreaterEqual(response.status, 400, response.body)
        self.assertEqual(self.submissions(), before)
        state = self.backend_status()
        self.assertEqual(state.status, 200, state.body)
        self.assertFalse(state.json()["drained"])


class DatabaseFaultContract(GatewayFixture):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.fault_url = os.environ.get("TX_DATABASE_FAULT_URL", "").rstrip("/")
        if not cls.fault_url or os.environ.get("TX_ALLOW_IRREVERSIBLE_FAULTS") != "yes":
            raise RuntimeError("An isolated TX_DATABASE_FAULT_URL and TX_ALLOW_IRREVERSIBLE_FAULTS=yes are required")

    def database(self, available):
        response = request(self.fault_url + "/__test/config", "POST", json.dumps({"available": available}))
        self.assertEqual(response.status, 200, response.body)
        self.assertEqual(response.json()["available"], available)

    def await_database(self):
        deadline = time.monotonic() + 30
        while True:
            response = self.backend_status()
            if response.status == 200:
                return response.json()
            self.assertLess(time.monotonic(), deadline, "Gateway database access did not recover")
            time.sleep(0.1)

    def test_database_outage_before_admission_forwards_nothing(self):
        transaction = self.start()
        before = self.submissions()
        self.database(False)
        try:
            response = self.submit("SELECT 1", transaction, gateway=1)
            self.assertGreaterEqual(response.status, 500, response.body)
            self.assertEqual(self.submissions(), before)
        finally:
            self.database(True)
            self.await_database()
        self.assertEqual(self.select_backend(transaction, gateway=1), self.state(0)["identity"])

    def test_database_outage_after_backend_acceptance_retains_obligation(self):
        self.configure(0, hold_start=True)
        before_requests = len(self.state(0)["requests"])
        before = self.backend_status().json()["pendingRequests"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(self.submit, "START TRANSACTION")
            try:
                deadline = time.monotonic() + 10
                while not any(item["sql"] == "START TRANSACTION" for item in self.state(0)["requests"][before_requests:]):
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.01)
                self.database(False)
                request(self.backends[0] + "/__test/release", "POST", "{}")
                response = pending.result(timeout=40)
                self.assertGreaterEqual(response.status, 500, response.body)
                self.assertEqual(response.values("X-Trino-Started-Transaction-Id"), [])
            finally:
                request(self.backends[0] + "/__test/release", "POST", "{}")
                self.database(True)
                state = self.await_database()
        self.assertGreaterEqual(state["pendingRequests"], before + 1)
        drained = self.admin("/gateway/transactions/backends/" + self.names[0] + "/drain", "POST", gateway=1)
        self.assertEqual(drained.status, 200, drained.body)
        try:
            self.assertFalse(self.backend_status().json()["readyToSeal"])
            self.assertFalse(self.backend_status().json()["drained"])
        finally:
            self.resume()


if __name__ == "__main__":
    unittest.main()
