"""Exercise optional statement pacing without deployment-specific traffic."""

from concurrent.futures import ThreadPoolExecutor
import json
import os
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace

import rollout_workload as workload


class StatementBudgetTest(unittest.TestCase):
    def setUp(self):
        self.events = []
        self.event = lambda name, **fields: self.events.append({"event": name, **fields})
        self.stop = workload.StopController(self.event)
        self.ledger = workload.OperationLedger(self.event)

    def test_all_kinds_share_budget_and_retained_pause_holds_slot(self):
        budget = workload.StatementBudget(3)
        held, release = threading.Event(), threading.Event()
        lock, active, peak = threading.Lock(), [0], [0]

        def operation(retained=False):
            with lock:
                active[0] += 1
                peak[0] = max(peak[0], active[0])
            try:
                if retained:
                    held.set()
                    self.assertTrue(release.wait(2))
                else:
                    time.sleep(.01)
            finally:
                with lock:
                    active[0] -= 1

        with ThreadPoolExecutor(max_workers=16) as executor:
            retained = executor.submit(self.ledger.run, "retained", lambda: operation(True),
                                       stop=self.stop, budget=budget)
            self.assertTrue(held.wait(1))
            futures = [executor.submit(self.ledger.run, kind, operation, stop=self.stop, budget=budget)
                       for kind in sorted(self.ledger.KINDS - {"continuation_resume", "retained"}) for _ in range(4)]
            for future in futures:
                future.result(timeout=2)
            self.assertFalse(retained.done())
            release.set()
            retained.result(timeout=1)
        self.assertEqual(peak[0], 3)
        self.assertTrue(self.ledger.snapshot()["zero_error_acceptance"])
        self.assertTrue(any(event["event"] == "operation_client_wait" for event in self.events))

    def test_stop_cancels_waiters_without_invocation_and_cleanup_does_not_wait(self):
        budget = workload.StatementBudget(1)
        held, release = threading.Event(), threading.Event()
        def operation():
            held.set()
            self.assertTrue(release.wait(2))
        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(self.ledger.run, "retained", operation, stop=self.stop, budget=budget)
            self.assertTrue(held.wait(1))
            waiting = executor.submit(self.ledger.run, "transaction_read", lambda: self.fail("not submitted"),
                                      stop=self.stop, budget=budget)
            self.stop.request("interrupt")
            with self.assertRaises(workload.OperationStopped):
                waiting.result(timeout=1)
            before = time.monotonic()
            with self.assertRaises(workload.OperationStopped):
                self.ledger.run("transaction_cleanup", lambda: self.fail("no safe slot"), budget=budget, cleanup=True)
            self.assertLess(time.monotonic() - before, .2)
            release.set()
            first.result(timeout=1)
        self.ledger.run("transaction_cleanup", lambda: None, budget=budget, cleanup=True)
        snapshot = self.ledger.snapshot()
        self.assertEqual(snapshot["submitted"], 2)
        self.assertEqual(snapshot["not_submitted_stopped"], 1)
        self.assertEqual(snapshot["not_submitted_statement_budget"], 1)
        self.assertTrue(snapshot["accounting_valid"])

    def test_failure_keeps_unknown_permit_and_stops_before_waiter_can_submit(self):
        budget = workload.StatementBudget(1)
        with self.assertRaises(OSError):
            self.ledger.run("autocommit", lambda: (_ for _ in ()).throw(OSError("private payload")),
                            stop=self.stop, budget=budget)
        self.assertTrue(self.stop.is_set())
        with self.assertRaises(workload.OperationStopped):
            self.ledger.run("autocommit", lambda: self.fail("stopped"), stop=self.stop, budget=budget)
        with self.assertRaises(workload.OperationStopped):
            self.ledger.run("transaction_cleanup", lambda: self.fail("unknown still occupies slot"),
                            budget=budget, cleanup=True)
        self.assertEqual(budget.snapshot()["uncertain_statements"], 1)
        self.assertNotIn("private payload", str(self.events))

    def test_safe_remaining_slot_permits_cleanup_after_failure(self):
        budget = workload.StatementBudget(2)
        with self.assertRaises(OSError):
            self.ledger.run("transaction_read", lambda: (_ for _ in ()).throw(OSError()), stop=self.stop, budget=budget)
        self.ledger.run("transaction_cleanup", lambda: None, budget=budget, cleanup=True)
        self.assertEqual(budget.snapshot()["uncertain_statements"], 1)
        self.assertEqual(self.ledger.snapshot()["succeeded"], 1)

    def test_failure_stops_queued_work_before_successful_peer_releases_slot(self):
        budget = workload.StatementBudget(2)
        failing, successful, release_failure, release_success = [threading.Event() for _ in range(4)]

        def fail():
            failing.set()
            self.assertTrue(release_failure.wait(2))
            raise OSError("unknown completion")

        def succeed():
            successful.set()
            self.assertTrue(release_success.wait(2))

        with ThreadPoolExecutor(max_workers=3) as executor:
            failed = executor.submit(self.ledger.run, "transaction_read", fail, stop=self.stop, budget=budget)
            peer = executor.submit(self.ledger.run, "retained", succeed, stop=self.stop, budget=budget)
            self.assertTrue(failing.wait(1) and successful.wait(1))
            queued = executor.submit(self.ledger.run, "autocommit", lambda: self.fail("must not invoke"),
                                     stop=self.stop, budget=budget)
            deadline = time.monotonic() + 1
            while budget.snapshot()["waiting_operations"] == 0 and time.monotonic() < deadline:
                time.sleep(.001)
            self.assertEqual(budget.snapshot()["waiting_operations"], 1)
            release_failure.set()
            with self.assertRaises(OSError):
                failed.result(timeout=1)
            self.assertTrue(self.stop.is_set())
            release_success.set()
            peer.result(timeout=1)
            with self.assertRaises(workload.OperationStopped):
                queued.result(timeout=1)
        self.assertEqual(budget.snapshot(), {"limit": 2, "occupied_slots": 1,
                                            "uncertain_statements": 1, "waiting_operations": 0})

    def test_validation_failure_keeps_exactly_one_uncertain_slot(self):
        budget = workload.StatementBudget(2)
        with self.assertRaises(ValueError):
            self.ledger.run("retained", lambda: None,
                            lambda _: (_ for _ in ()).throw(ValueError("callback")), stop=self.stop, budget=budget)
        self.assertEqual(budget.snapshot()["occupied_slots"], 1)
        self.assertEqual(budget.snapshot()["uncertain_statements"], 1)

    def test_stop_after_acquisition_releases_unused_slot(self):
        budget = workload.StatementBudget(1)
        original = self.stop.begin_operation

        def stop_before_submit(submit):
            self.stop.request("interrupt")
            return original(submit)

        self.stop.begin_operation = stop_before_submit
        with self.assertRaises(workload.OperationStopped):
            self.ledger.run("transaction_begin", lambda: self.fail("not submitted"), stop=self.stop, budget=budget)
        self.assertEqual(budget.snapshot()["occupied_slots"], 0)
        self.assertEqual(self.ledger.snapshot()["not_submitted_stopped"], 1)

    def test_open_transaction_recovery_is_private_opt_in_and_not_a_continuation(self):
        client = SimpleNamespace(transaction="private-transaction", context_hash="a" * 64)
        report = workload.transaction_recovery_report(client, None)
        self.assertFalse(report["transaction_recovery_saved"])
        self.assertNotIn(client.transaction, json.dumps(report))
        with tempfile.TemporaryDirectory() as directory:
            os.chmod(directory, 0o700)
            report = workload.transaction_recovery_report(client, directory)
            self.assertTrue(report["transaction_recovery_saved"])
            self.assertNotIn(client.transaction, json.dumps(report))
            path = os.path.join(directory, report["transaction_recovery_file"])
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)
            with open(path) as source:
                payload = json.load(source)
            self.assertEqual(payload, {"transaction_id": client.transaction, "context_hash": client.context_hash,
                                       "format": "rollout-open-transaction-v1"})
            os.chmod(directory, 0o755)
            self.assertTrue(workload.transaction_recovery_report(client, directory)["transaction_recovery_save_failed"])

    def test_invalid_limits_are_rejected(self):
        for limit in (0, -1, True, 1.5):
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                workload.StatementBudget(limit)


if __name__ == "__main__":
    unittest.main()
