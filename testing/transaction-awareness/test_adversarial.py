"""Additional protocol adversaries; run only against a transaction-aware fixture."""

import concurrent.futures
import time
import unittest
import uuid

from protocol import finish, request, through_gateway
from test_gateway import GatewayFixture


class IdentityContract(GatewayFixture):
    def reject_submission(self, headers, transaction="NONE"):
        before = self.submissions()
        response = request(self.gateways[1] + "/v1/statement", "POST", "SELECT 1",
                           [("X-Trino-User", "user"), ("X-Trino-Transaction-Id", transaction),
                            ("X-Trino-Routing-Group", self.group)] + headers)
        try:
            self.assertGreaterEqual(response.status, 400, response.body)
            self.assertLess(response.status, 500, response.body)
            self.assertEqual(self.submissions(), before)
        finally:
            if response.status == 200:
                finish(response, self.gateways[1])

    def test_missing_credentials_reject_before_admission(self):
        self.reject_submission([])

    def test_malformed_basic_credentials_reject_before_admission(self):
        self.reject_submission([("Authorization", "Basic not-valid-base64!")])

    def test_basic_credentials_without_separator_reject_before_admission(self):
        self.reject_submission([("Authorization", "Basic dXNlcg==")])

    def test_repeated_authorization_reject_before_admission(self):
        self.reject_submission([("Authorization", self.authorization), ("Authorization", self.authorization)])

    def test_unsigned_bearer_claims_do_not_establish_identity(self):
        self.reject_submission([("Authorization", "Bearer eyJhbGciOiJub25lIn0.eyJzdWIiOiJ1c2VyIn0.")])

    def test_original_user_change_cannot_inherit_transaction(self):
        self.rejected_without_forward(self.start(), [("X-Trino-Original-User", "different-user")])

    def test_comma_joined_transaction_header_is_not_accepted(self):
        transaction = self.start()
        self.rejected_without_forward(transaction + "," + transaction)

    def reject_continuation(self, headers):
        initial = self.submit("SELECT 1")
        self.assertEqual(initial.status, 200, initial.body)
        uri = initial.json()["nextUri"]
        before = [len(self.state(index)["requests"]) for index in (0, 1)]
        response = request(through_gateway(uri, self.gateways[1]), headers=headers)
        try:
            self.assertGreaterEqual(response.status, 400, response.body)
            self.assertLess(response.status, 500, response.body)
            self.assertEqual([len(self.state(index)["requests"]) for index in (0, 1)], before)
        finally:
            self.complete(initial)

    def test_continuation_with_changed_credentials_rejects(self):
        self.reject_continuation([("Authorization", "Basic dXNlcjphbm90aGVyLXBhc3N3b3Jk")])

    def test_continuation_with_conflicting_transaction_rejects(self):
        self.reject_continuation([("X-Trino-Transaction-Id", str(uuid.uuid4()))])

    def test_continuation_with_duplicate_transaction_headers_rejects(self):
        self.reject_continuation([("X-Trino-Transaction-Id", "NONE"), ("X-Trino-Transaction-Id", "NONE")])


class ReplayAndSealContract(GatewayFixture):
    def test_transaction_lookup_agrees_across_replicas(self):
        transaction = self.start()
        states = []
        for gateway in range(len(self.gateways)):
            response = self.admin("/gateway/transactions/" + transaction, gateway=gateway)
            self.assertEqual(response.status, 200, response.body)
            states.append(response.json())
        self.assertTrue(states[0]["incarnation"])
        self.assertEqual(states[0]["backendName"], self.names[0])
        self.assertTrue(all(value == states[0] for value in states))

    def test_replayed_start_cannot_reopen_closed_transaction(self):
        self.configure(0, start_header_page=1)
        initial = self.submit("START TRANSACTION")
        pages = self.complete(initial)
        transaction = pages[-1].values("X-Trino-Started-Transaction-Id")[0]
        self.transactions.append(transaction)
        self.complete(self.submit("ROLLBACK", transaction, gateway=1), gateway=1)
        replay = request(through_gateway(initial.json()["nextUri"], self.gateways[1]))
        self.assertGreaterEqual(replay.status, 400, replay.body)
        self.rejected_without_forward(transaction)

    def test_concurrent_start_replay_has_single_binding(self):
        self.configure(0, start_header_page=1)
        initial = self.submit("START TRANSACTION")
        self.assertEqual(initial.status, 200, initial.body)
        uri = initial.json()["nextUri"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.gateways)) as pool:
            responses = list(pool.map(lambda gateway: request(through_gateway(uri, gateway)), self.gateways))
        transactions = set()
        for response in responses:
            self.assertEqual(response.status, 200, response.body)
            transactions.update(response.values("X-Trino-Started-Transaction-Id"))
        self.assertEqual(len(transactions), 1)
        transaction = transactions.pop()
        self.transactions.append(transaction)
        self.assertEqual(self.select_backend(transaction, gateway=1), self.state(0)["identity"])
        status = self.backend_status()
        self.assertEqual(status.status, 200, status.body)
        self.assertEqual(status.json()["openTransactions"], 1)

    def test_failed_commit_keeps_transaction_open_until_rollback(self):
        transaction = self.start()
        self.configure(0, fail_commit=True)
        responses = finish(self.submit("COMMIT", transaction, gateway=1), self.gateways[1])
        self.assertIn("error", responses[-1].json())
        self.assertFalse(any(page.values("X-Trino-Clear-Transaction-Id") for page in responses))
        status = self.backend_status()
        self.assertEqual(status.status, 200, status.body)
        self.assertGreaterEqual(status.json()["openTransactions"], 1)
        self.complete(self.submit("ROLLBACK", transaction))
        self.rejected_without_forward(transaction)

    def test_query_error_does_not_close_enclosing_transaction(self):
        transaction = self.start()
        self.configure(0, query_error=True)
        responses = finish(self.submit("SELECT 1", transaction), self.gateways[0])
        self.assertIn("error", responses[-1].json())
        self.configure(0, query_error=False)
        status = self.backend_status()
        self.assertEqual(status.status, 200, status.body)
        self.assertGreaterEqual(status.json()["openTransactions"], 1)
        self.complete(self.submit("ROLLBACK", transaction))
        self.rejected_without_forward(transaction)

    def test_clear_from_continuation_is_observed_by_another_gateway(self):
        transaction = self.start()
        self.configure(0, clear_header_page=1, lowercase_headers=True)
        pages = self.complete(self.submit("ROLLBACK", transaction), gateway=1)
        self.assertFalse(pages[0].values("X-Trino-Clear-Transaction-Id"))
        self.assertEqual(pages[-1].values("X-Trino-Clear-Transaction-Id"), ["true"])
        self.rejected_without_forward(transaction)

    def test_lowercase_start_header_preserves_affinity(self):
        self.configure(0, lowercase_headers=True)
        transaction = self.start()
        self.activate(1)
        self.assertEqual(self.select_backend(transaction, gateway=1), self.state(0)["identity"])

    def test_stale_generation_cannot_seal_after_resume(self):
        path = "/gateway/transactions/backends/" + self.names[0]
        response = self.admin(path + "/drain", "POST")
        self.assertEqual(response.status, 200, response.body)
        old = self.backend_status().json()["generation"]
        self.resume()
        stale = self.admin(path + "/seal", "POST", {"generation": old}, gateway=1)
        self.assertEqual(stale.status, 409, stale.body)
        current = self.backend_status()
        self.assertEqual(current.status, 200, current.body)
        self.assertTrue(current.json()["acceptingNewQueries"])
        self.assertFalse(current.json()["sealed"])

    def test_terminal_retry_window_precedes_explicit_seal(self):
        path = "/gateway/transactions/backends/" + self.names[0]
        initial = self.submit("SELECT 1")
        self.complete(initial)
        response = self.admin(path + "/drain", "POST", gateway=1)
        self.assertEqual(response.status, 200, response.body)
        try:
            status = self.backend_status()
            self.assertEqual(status.status, 200, status.body)
            self.assertFalse(status.json()["drained"])
            replay = request(through_gateway(initial.json()["nextUri"], self.gateways[1]))
            self.assertEqual(replay.status, 200, replay.body)
            self.assertEqual(replay.json()["data"], [[self.state(0)["identity"]]])
            deadline = time.monotonic() + 150
            while True:
                status = self.backend_status()
                self.assertEqual(status.status, 200, status.body)
                self.assertFalse(status.json()["drained"])
                if status.json()["readyToSeal"]:
                    break
                self.assertLess(time.monotonic(), deadline, "Retry window did not settle")
                time.sleep(0.1)
            sealed = self.admin(path + "/seal", "POST", {"generation": status.json()["generation"]}, gateway=1)
            self.assertEqual(sealed.status, 200, sealed.body)
            final = self.backend_status()
            self.assertTrue(final.json()["sealed"])
            self.assertTrue(final.json()["drained"])
            before = [len(self.state(index)["requests"]) for index in (0, 1)]
            late = request(through_gateway(initial.json()["nextUri"], self.gateways[0]))
            self.assertGreaterEqual(late.status, 400, late.body)
            self.assertEqual([len(self.state(index)["requests"]) for index in (0, 1)], before)
        finally:
            self.resume()

    def test_continuation_admitted_before_seal_blocks_seal(self):
        initial = self.submit("SELECT 1")
        self.complete(initial)
        path = "/gateway/transactions/backends/" + self.names[0]
        draining = self.admin(path + "/drain", "POST")
        self.assertEqual(draining.status, 200, draining.body)
        try:
            deadline = time.monotonic() + 150
            while not self.backend_status().json()["readyToSeal"]:
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.1)
            self.configure(0, hold_poll=True)
            before = len(self.state(0)["requests"])
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(request, through_gateway(initial.json()["nextUri"], self.gateways[1]))
                try:
                    deadline = time.monotonic() + 10
                    while len(self.state(0)["requests"]) == before:
                        self.assertLess(time.monotonic(), deadline)
                        time.sleep(0.01)
                    self.assertFalse(pending.done())
                    state = self.backend_status()
                    self.assertGreaterEqual(state.json()["pendingRequests"], 1)
                    sealed = self.admin(path + "/seal", "POST", {"generation": state.json()["generation"]})
                    self.assertEqual(sealed.status, 409, sealed.body)
                finally:
                    request(self.backends[0] + "/__test/release", "POST", '{"kind": "poll"}')
                    result = pending.result(timeout=10)
                    self.assertEqual(result.status, 200, result.body)
        finally:
            self.resume()


if __name__ == "__main__":
    unittest.main()
