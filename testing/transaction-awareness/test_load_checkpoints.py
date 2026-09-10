import unittest
from unittest.mock import patch

from load_checkpoints import Checkpoints


class Response:
    def __init__(self, status=200, body=None, transaction_ids=()):
        self.status, self.body, self.transaction_ids = status, body or {}, transaction_ids

    def json(self):
        return self.body

    def values(self, name):
        return self.transaction_ids if name == "X-Trino-Started-Transaction-Id" else ()


class FinishScenario:
    def __init__(self):
        self.calls = []
        self.initial_drain = Response(body={"openTransactions": 1, "drained": False, "readyToSeal": False, "generation": 11})
        self.blocked_seal = Response(409)
        self.final_drain = Response(body={"readyToSeal": True, "generation": 11, "pendingRequests": 0, "openTransactions": 0, "activeQueries": 0})
        self.final_seal = Response(body={"drained": True, "generation": 12, "pendingRequests": 0, "openTransactions": 0, "activeQueries": 0})
        self.cutover = Response()
        self.resume = Response()
        self.bad_old_gateway = None
        self.bad_new_gateway = None
        self.seals = 0

    def admin(self, path, method="GET", body=None):
        self.calls.append((path, method, body))
        if path == "backends/blue/drain":
            return self.initial_drain if method == "POST" else self.final_drain
        if path == "backends/blue/seal":
            self.seals += 1
            return self.blocked_seal if self.seals == 1 else self.final_seal
        if path == "cutover":
            return self.cutover
        if path == "backends/blue/resume":
            return self.resume
        raise AssertionError("Unexpected checkpoint operation")

    def query(self, sql, transaction="NONE", index=0):
        self.calls.append((sql, transaction, index))
        if sql == "ROLLBACK":
            return [Response()]
        if sql != "SELECT 1":
            raise AssertionError("Unexpected checkpoint query")
        if transaction == "checkpoint-transaction":
            identity = "target" if index == self.bad_old_gateway else "source"
        elif transaction == "NONE":
            identity = "source" if index == self.bad_new_gateway else "target"
        else:
            raise AssertionError("Unexpected transaction identity")
        return [Response(body={"data": [[identity]]})]


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        for function in ("request", "statement", "finish"):
            guard = patch("load_checkpoints." + function, side_effect=AssertionError("Unexpected transport call"))
            guard.start()
            self.addCleanup(guard.stop)
        self.scenario = FinishScenario()
        self.checkpoint = self.make_checkpoint(3)

    def make_checkpoint(self, count):
        result = Checkpoints([f"http://gateway-{index}.example.test" for index in range(count)],
                             "group", "blue", "green", "source", "target",
                             "Basic synthetic", "synthetic-admin-token")
        result.transaction = "checkpoint-transaction"
        return result

    def finish(self):
        with patch.object(self.checkpoint, "admin", side_effect=self.scenario.admin), \
                patch.object(self.checkpoint, "query", side_effect=self.scenario.query):
            return self.checkpoint.finish()

    def test_checks_old_and_new_placement_through_every_gateway(self):
        for count in (3, 20, 100):
            with self.subTest(gateways=count):
                self.scenario = FinishScenario()
                self.checkpoint = self.make_checkpoint(count)
                result = self.finish()
                old = [call[2] for call in self.scenario.calls if call[:2] == ("SELECT 1", "checkpoint-transaction")]
                new = [call[2] for call in self.scenario.calls if call[:2] == ("SELECT 1", "NONE")]
                self.assertEqual(old, list(range(count)))
                self.assertEqual(new, list(range(count)))
                self.assertEqual(result["all_gateway_endpoints_checked"], count)
                self.assertTrue(result["observed_final_drain_and_seal"])
                self.assertIsNone(self.checkpoint.transaction)
                self.assertIn(("backends/blue/seal", "POST", {"generation": 11}), self.scenario.calls)
                self.assertIn(("backends/blue/resume", "POST", {"generation": 12}), self.scenario.calls)
                self.assertEqual(self.scenario.calls[-1], ("cutover", "POST", {"routingGroup": "group", "backendName": "blue"}))

    def test_seal_with_open_transaction_is_a_failure_before_cutover(self):
        self.scenario.blocked_seal = Response(body={"drained": True})
        with self.assertRaisesRegex(AssertionError, "Seal succeeded with an open"):
            self.finish()
        self.assertEqual(len(self.scenario.calls), 2)
        self.assertEqual(self.checkpoint.transaction, "checkpoint-transaction")

    def test_ready_to_seal_with_open_checkpoint_transaction_is_rejected(self):
        self.scenario.initial_drain.body["readyToSeal"] = True
        with self.assertRaisesRegex(AssertionError, "Open checkpoint transaction must block drain"):
            self.finish()
        self.assertEqual(len(self.scenario.calls), 1)

    def test_ready_to_seal_does_not_override_positive_obligation_counts(self):
        for field in ("pendingRequests", "openTransactions", "activeQueries"):
            with self.subTest(field=field):
                self.scenario = FinishScenario()
                self.checkpoint = self.make_checkpoint(3)
                self.scenario.final_drain.body[field] = 1
                with self.assertRaisesRegex(AssertionError, "readiness with outstanding obligations"):
                    self.finish()
                self.assertEqual(self.scenario.seals, 1)

    def test_final_seal_cannot_hide_positive_obligation_counts(self):
        for field in ("pendingRequests", "openTransactions", "activeQueries"):
            with self.subTest(field=field):
                self.scenario = FinishScenario()
                self.checkpoint = self.make_checkpoint(3)
                self.scenario.final_seal.body[field] = 1
                with self.assertRaisesRegex(AssertionError, "Final seal failed"):
                    self.finish()
                self.assertFalse(any("resume" in call[0] for call in self.scenario.calls))

    def test_missing_open_transaction_or_early_drained_claim_is_rejected(self):
        for response in (Response(503), Response(body={"openTransactions": 0, "drained": False}),
                         Response(body={"openTransactions": 1, "drained": True})):
            with self.subTest(status=response.status, body=response.body):
                self.scenario.initial_drain = response
                with self.assertRaisesRegex(AssertionError, "Open checkpoint transaction must block drain"):
                    self.finish()

    def test_wrong_old_transaction_placement_at_last_gateway_fails(self):
        self.scenario.bad_old_gateway = 2
        with self.assertRaisesRegex(AssertionError, "Existing transaction moved"):
            self.finish()
        self.assertNotIn(("ROLLBACK", "checkpoint-transaction", 0), self.scenario.calls)
        self.assertEqual(self.scenario.seals, 1)

    def test_wrong_new_query_placement_at_last_gateway_fails(self):
        self.scenario.bad_new_gateway = 2
        with self.assertRaisesRegex(AssertionError, "New query did not use"):
            self.finish()
        self.assertEqual(self.scenario.seals, 1)

    def test_failed_cutover_stops_before_placement_queries(self):
        self.scenario.cutover = Response(503)
        with self.assertRaisesRegex(AssertionError, "atomic cutover failed"):
            self.finish()
        self.assertFalse(any(call[0] == "SELECT 1" for call in self.scenario.calls))

    def test_outstanding_obligations_expire_checkpoint_without_reset_or_seal(self):
        self.scenario.final_drain = Response(body={"readyToSeal": False, "pendingRequests": 1, "generation": 11})
        with patch("load_checkpoints.time.monotonic", side_effect=[0, 151]), \
                patch("load_checkpoints.time.sleep") as sleep:
            with self.assertRaisesRegex(AssertionError, "Post-load obligations remain"):
                self.finish()
            sleep.assert_not_called()
        self.assertEqual(self.scenario.seals, 1)
        self.assertFalse(any("resume" in call[0] or "reset" in call[0] for call in self.scenario.calls))

    def test_failed_status_or_final_seal_is_not_a_success_receipt(self):
        self.scenario.final_drain = Response(503)
        with self.assertRaisesRegex(AssertionError, "drain status failed"):
            self.finish()
        for response in (Response(409), Response(body={"drained": False})):
            with self.subTest(status=response.status):
                self.scenario = FinishScenario()
                self.checkpoint = self.make_checkpoint(3)
                self.scenario.final_seal = response
                with self.assertRaisesRegex(AssertionError, "Final seal failed"):
                    self.finish()

    def test_failed_fixture_restoration_is_not_a_success_receipt(self):
        self.scenario.resume = Response(409)
        with self.assertRaisesRegex(AssertionError, "Could not restore"):
            self.finish()

    def test_begin_requires_exactly_one_transaction_identity(self):
        for identities in ((), ("one", "two")):
            with self.subTest(identities=identities), \
                    patch.object(self.checkpoint, "query", return_value=[Response(transaction_ids=identities)]):
                with self.assertRaisesRegex(AssertionError, "exactly one real transaction"):
                    self.checkpoint.begin()

    def test_begin_rejects_wrong_source_and_accepts_replayed_identity(self):
        pages = [Response(transaction_ids=("checkpoint-transaction",)), Response(transaction_ids=("checkpoint-transaction",))]
        for identity in ("target", "source"):
            with self.subTest(identity=identity), patch.object(self.checkpoint, "query", side_effect=[pages, [Response(body={"data": [[identity]]})]]):
                if identity == "target":
                    with self.assertRaisesRegex(AssertionError, "expected source"):
                        self.checkpoint.begin()
                else:
                    self.checkpoint.begin()
                    self.assertEqual(self.checkpoint.transaction, "checkpoint-transaction")

    def test_query_uses_selected_gateway_and_next_gateway_for_results(self):
        initial, terminal = Response(), Response(body={"data": [["source"]]})
        with patch("load_checkpoints.statement", return_value=initial) as statement, \
                patch("load_checkpoints.finish", return_value=[initial, terminal]) as finish:
            self.assertEqual(self.checkpoint.query("SELECT 1", "checkpoint-transaction", 2), [initial, terminal])
            statement.assert_called_once_with(self.checkpoint.gateways[2], "SELECT 1", "checkpoint-transaction", "user", "group", [("Authorization", "Basic synthetic")])
            finish.assert_called_once_with(initial, self.checkpoint.gateways[0])

    def test_query_fails_on_any_http_or_sql_error_page(self):
        for failed in (Response(503), Response(body={"error": {"message": "synthetic failure"}})):
            with self.subTest(status=failed.status), patch("load_checkpoints.statement", return_value=Response()), \
                    patch("load_checkpoints.finish", return_value=[Response(), failed, Response()]):
                with self.assertRaisesRegex(AssertionError, "checkpoint query failed"):
                    self.checkpoint.query("SELECT 1")


if __name__ == "__main__":
    unittest.main()
