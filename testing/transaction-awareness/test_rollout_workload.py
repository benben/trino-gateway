"""Verify complete logical-operation accounting without live query traffic."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
import io
import json
import threading
import time
import unittest
from unittest.mock import patch

from rollout_client import RolloutFailure
from rollout_workload import OperationLedger, OperationStopped, StopController, main


class OperationLedgerTest(unittest.TestCase):
    def ledger(self):
        events = []
        return OperationLedger(lambda name, **fields: events.append({"event": name, **fields})), events

    def test_outcomes_partition_submissions_and_offers(self):
        ledger, _ = self.ledger()
        ledger.run("autocommit", lambda: 1)
        with self.assertRaises(RuntimeError):
            ledger.run("transaction_read", lambda: (_ for _ in ()).throw(RuntimeError("private SQL")))
        pending = ledger.offer("retained")
        ledger.submit(pending)
        dropped = ledger.offer("autocommit")
        ledger.drop_capacity(dropped)
        waiting = ledger.offer("autocommit")
        result = ledger.snapshot()
        self.assertEqual(result["submitted"], 3)
        self.assertEqual((result["succeeded"], result["failed"], result["unresolved"]), (1, 1, 1))
        self.assertEqual(result["offered"], 5)
        self.assertEqual(result["not_submitted_capacity"], 1)
        self.assertEqual(result["not_submitted_pending"], 1)
        self.assertTrue(result["accounting_valid"])
        self.assertFalse(result["zero_error_acceptance"])
        self.assertNotEqual(waiting, pending)

    def test_result_and_transaction_validation_happen_before_success(self):
        ledger, events = self.ledger()
        for kind in ("autocommit", "transaction_begin", "transaction_read", "transaction_commit",
                     "transaction_rollback", "transaction_cleanup", "retained"):
            with self.assertRaisesRegex(RuntimeError, "result_mismatch") as failure:
                ledger.run(kind, lambda: {"rows": [[2]]}, lambda result: (_ for _ in ()).throw(RuntimeError("result_mismatch")))
            self.assertIsNotNone(failure.exception.operation_id)
        self.assertEqual(ledger.snapshot()["succeeded"], 0)
        self.assertEqual(ledger.snapshot()["failed"], 7)
        self.assertEqual(len([row for row in events if row.get("state") == "failed"]), 7)

    def test_successful_explicit_resume_never_overwrites_original_failure(self):
        ledger, _ = self.ledger()
        with self.assertRaises(OSError) as failed:
            ledger.run("autocommit", lambda: (_ for _ in ()).throw(OSError("private capability")))
        ledger.run("continuation_resume", lambda: 1, parent_operation_id=failed.exception.operation_id)
        result = ledger.snapshot()
        self.assertEqual((result["submitted"], result["succeeded"], result["failed"]), (2, 1, 1))
        self.assertFalse(result["zero_error_acceptance"])

    def test_duplicate_finalization_and_unknown_resume_parent_are_rejected(self):
        ledger, _ = self.ledger()
        operation = ledger.offer("autocommit")
        ledger.submit(operation)
        ledger.finish(operation, "failed")
        with self.assertRaises(ValueError):
            ledger.finish(operation, "succeeded")
        with self.assertRaises(ValueError):
            ledger.offer("continuation_resume", parent_operation_id="unknown")
        self.assertEqual(ledger.snapshot()["failed"], 1)

    def test_threaded_operations_are_unique_complete_and_do_not_log_payloads(self):
        ledger, events = self.ledger()
        with ThreadPoolExecutor(max_workers=16) as executor:
            list(executor.map(lambda _: ledger.run("autocommit", lambda: "private SQL/password/capability"), range(200)))
        snapshot = ledger.snapshot()
        self.assertEqual(snapshot["submitted"], 200)
        self.assertEqual(snapshot["succeeded"], 200)
        self.assertTrue(snapshot["zero_error_acceptance"])
        offered = [row["operation_id"] for row in events if row["event"] == "operation_offered"]
        self.assertEqual(len(set(offered)), 200)
        self.assertNotIn("private", json.dumps(events))

    def test_workload_accounts_all_invocations_and_failed_commit_cleanup(self):
        clock, calls, lock = [0], [], threading.Lock()

        def monotonic():
            with lock:
                clock[0] += .1
                return clock[0]

        class Client:
            transaction = "NONE"

            def query(self, sql, **kwargs):
                with lock:
                    calls.append(sql)
                    sequence = len(calls)
                rows, pages = [], 1
                if sql.startswith("START"):
                    self.transaction = "transaction"
                elif sql == "COMMIT":
                    pass
                elif sql == "ROLLBACK":
                    self.transaction = "NONE"
                elif sql == "SELECT 1":
                    rows = [[1]]
                elif "information_schema" in sql:
                    rows = [[True]]
                else:
                    kwargs["first_page_callback"]()
                    rows, pages = [[n] for n in range(1, 10001)], 2
                return {"rows": rows, "pages": pages, "query_id": f"20260101_000000_{sequence:05d}_owner", "duration_seconds": .01}

        args = ["rollout_workload.py", "--server", "https://gateway.example", "--user", "reader",
                "--catalog", "catalog", "--seconds", "30", "--rate", ".1", "--transaction-seconds", "10",
                "--retained-seconds", "5"]
        output = io.StringIO()
        with patch("sys.argv", args), patch.dict("os.environ", {"TX_TRINO_PASSWORD": "private-password"}), \
                patch("rollout_workload.RolloutClient", side_effect=lambda *args: Client()), \
                patch("rollout_workload.time.monotonic", side_effect=monotonic), \
                patch("rollout_workload.StopController.wait", return_value=False), \
                patch("rollout_workload.StopController.request"), \
                redirect_stdout(output), self.assertRaises(SystemExit) as stopped:
            main()
        self.assertEqual(stopped.exception.code, 1)
        events = [json.loads(line) for line in output.getvalue().splitlines()]
        summary = next(row["operation_accounting"] for row in events if row["event"] == "summary")
        self.assertEqual(summary["submitted"], len(calls))
        self.assertEqual(summary["failed"], 2)
        self.assertEqual(summary["unresolved"], 0)
        self.assertEqual(summary["by_kind"]["transaction_commit"], {"failed": 2})
        self.assertEqual(summary["by_kind"]["transaction_cleanup"], {"succeeded": 2})
        self.assertEqual(summary["by_kind"]["transaction_rollback"], {"succeeded": 1})
        self.assertTrue(summary["accounting_valid"])
        query_events = [row for row in events if row.get("state") == "succeeded" and "query_id" in row]
        self.assertEqual(len(query_events), summary["succeeded"])
        self.assertEqual(len({row["query_id"] for row in query_events}), summary["succeeded"])
        for secret in ("private-password", "SELECT", "Authorization", "nextUri"):
            self.assertNotIn(secret, output.getvalue())

    def test_malformed_query_metadata_is_failed_not_successful(self):
        ledger, events = self.ledger()
        valid = {"query_id": "20260101_000000_00001_abcde", "pages": 2, "rows": [[1]]}
        for change in ({"query_id": "https://private.example/token"}, {"pages": True}, {"pages": 501},
                       {"rows": "private result"}, {"query_id": "q" * 300}):
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, "invalid_query_metadata"):
                ledger.run("autocommit", lambda: {**valid, **change}, include_query_metadata=True)
        self.assertEqual(ledger.snapshot()["failed"], 5)
        self.assertEqual(ledger.snapshot()["succeeded"], 0)
        self.assertFalse(any("query_id" in row for row in events))

    def test_first_failure_is_latched_and_queued_operation_never_submits(self):
        ledger, events = self.ledger()
        stop = StopController(lambda name, **fields: events.append({"event": name, **fields}))
        queued = ledger.offer("autocommit")
        stop.request("first_failure", "original-operation")
        stop.request("interrupt", "later-operation")
        with self.assertRaises(OperationStopped):
            ledger.run("autocommit", lambda: self.fail("must not submit"), operation_id=queued, stop=stop)
        snapshot = ledger.snapshot()
        self.assertEqual(snapshot["submitted"], 0)
        self.assertEqual(snapshot["not_submitted_stopped"], 1)
        self.assertTrue(snapshot["accounting_valid"])
        self.assertFalse(snapshot["zero_error_acceptance"])
        stopped = [row for row in events if row["event"] == "workload_stop_requested"]
        self.assertEqual(len(stopped), 1)
        self.assertEqual(stopped[0]["operation_id"], "original-operation")

    def run_stopped_workload(self, interrupt=False, statement_limit=None):
        ready, retained, lock = threading.Event(), threading.Event(), threading.Lock()
        readers, calls = set(), []
        active, peak = [0], [0]
        original_wait = StopController.wait

        class Client:
            transaction = "NONE"

            def query(self, sql, **kwargs):
                with lock:
                    active[0] += 1
                    peak[0] = max(peak[0], active[0])
                try:
                    return self.run_query(sql, **kwargs)
                finally:
                    with lock:
                        active[0] -= 1

            def run_query(self, sql, **kwargs):
                with lock:
                    calls.append(sql)
                    sequence = len(calls)
                rows, pages = [], 1
                if sql.startswith("START"):
                    self.transaction = "transaction"
                elif sql in ("COMMIT", "ROLLBACK"):
                    self.transaction = "NONE"
                elif sql == "SELECT 1":
                    if not ready.wait(2) or not retained.wait(2):
                        raise RuntimeError("fixture_not_ready")
                    raise RolloutFailure("http_status:503:capacity")
                elif "information_schema" in sql:
                    with lock:
                        readers.add(threading.get_ident())
                        if len(readers) == 3:
                            ready.set()
                    rows = [[True]]
                else:
                    kwargs["first_page_callback"]()
                    retained.set()
                    if not kwargs["first_page_release"].wait(2):
                        raise RuntimeError("retained_pause_not_released")
                    rows, pages = [[n] for n in range(1, 10001)], 2
                return {"rows": rows, "pages": pages, "query_id": f"20260101_000000_{sequence:05d}_owner", "duration_seconds": .01}

        def wait(controller, seconds):
            if interrupt and threading.current_thread() is threading.main_thread() and not controller.is_set():
                raise KeyboardInterrupt()
            return original_wait(controller, seconds)

        args = ["rollout_workload.py", "--server", "https://gateway.example", "--user", "reader", "--catalog", "catalog",
                "--seconds", "600", "--rate", "4", "--transaction-seconds", "540", "--retained-seconds", "240"]
        if statement_limit is not None:
            args += ["--max-concurrent-statements", str(statement_limit)]
        output, started = io.StringIO(), time.monotonic()
        with patch("sys.argv", args), patch.dict("os.environ", {"TX_TRINO_PASSWORD": "private-password"}), \
                patch("rollout_workload.RolloutClient", side_effect=lambda *args: Client()), \
                patch.object(StopController, "wait", wait), redirect_stdout(output), self.assertRaises(SystemExit) as stopped:
            main()
        self.assertEqual(stopped.exception.code, 1)
        self.assertLess(time.monotonic() - started, 3)
        events = [json.loads(line) for line in output.getvalue().splitlines()]
        summary = next(row for row in events if row["event"] == "summary")
        self.assertTrue(summary["stopped_early"])
        self.assertTrue(summary["operation_accounting"]["accounting_valid"])
        self.assertEqual(summary["operation_accounting"]["submitted"], len(calls))
        self.assertEqual(summary["operation_accounting"]["unresolved"], 0)
        self.assertEqual(summary["counts"]["small_planned"], summary["counts"]["small_offered"] + summary["counts"]["small_not_offered"])
        if statement_limit is not None:
            self.assertLessEqual(peak[0], statement_limit)
        return summary, events, calls

    def test_bounded_workload_keeps_three_transactions_and_retained_result(self):
        summary, events, calls = self.run_stopped_workload(statement_limit=3)
        self.assertEqual(sum(row["event"] == "transaction_open" for row in events), 3)
        self.assertTrue(any(row["event"] == "workload_ready_for_rollout" for row in events))
        self.assertTrue(any(row["event"] == "retained_query_finished" for row in events))
        self.assertEqual(summary["counts"]["small_offered"], 1)
        self.assertEqual(summary["statement_budget"]["uncertain_statements"], 1)
        self.assertTrue(any(row["event"] == "operation_client_wait" for row in events))

    def test_first_failure_stops_arrivals_wakes_retention_and_rolls_back_open_transactions(self):
        summary, events, calls = self.run_stopped_workload()
        self.assertEqual(summary["counts"]["small_failed"], 1)
        self.assertEqual(summary["counts"]["small_offered"], 1)
        self.assertEqual(summary["counts"]["small_not_offered"], 2399)
        self.assertEqual(summary["counts"]["transactions_aborted"], 3)
        self.assertEqual(summary["operation_accounting"]["failed"], 1)
        self.assertEqual(summary["operation_accounting"]["by_kind"]["transaction_cleanup"], {"succeeded": 3})
        self.assertEqual(calls.count("COMMIT"), 0)
        self.assertEqual(calls.count("ROLLBACK"), 3)
        failures = [row for row in events if row["event"] == "small_failure"]
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["detail"], "http_status:503:capacity")

    def test_keyboard_interrupt_stops_gracefully_and_still_reports_failed_run(self):
        summary, events, calls = self.run_stopped_workload(interrupt=True)
        self.assertEqual(summary["counts"]["small_offered"], 0)
        self.assertEqual(summary["counts"]["small_not_offered"], 2400)
        self.assertFalse(any(sql == "COMMIT" for sql in calls))
        self.assertEqual([row["reason"] for row in events if row["event"] == "workload_stop_requested"], ["interrupt"])


if __name__ == "__main__":
    unittest.main()
