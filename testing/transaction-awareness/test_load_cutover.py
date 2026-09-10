"""Exercise cutover coordination and immutable query ownership without live services."""

import json
import functools
import multiprocessing
import threading
import time
import unittest
from unittest.mock import patch
from datetime import datetime, timezone
from unittest.mock import Mock

from load_open_loop import OpenLoop


class OwnerTransport:
    def __init__(self, changed_id=False, changed_owner=False):
        self.changed_id, self.changed_owner = changed_id, changed_owner

    def request(self, index, method, path, body, headers):
        identifier = "20260101_000000_1_aaaaa"
        if method == "POST":
            return 200, json.dumps({"id": identifier, "nextUri": "/result"}).encode()
        return 200, json.dumps({"id": identifier.replace("_1_", "_2_") if self.changed_id else identifier,
                                "data": [["green" if self.changed_owner else "blue"]]}).encode()

    def close(self):
        pass


class SwitchingTransport:
    def __init__(self, switched):
        self.switched, self.sequence = switched, 0

    def request(self, index, method, path, body, headers):
        if method == "POST":
            self.sequence += 1
            suffix = "bbbbb" if self.switched.is_set() else "aaaaa"
            identifier = "20260101_000000_%d_%s" % (self.sequence, suffix)
            payload = {"id": identifier, "nextUri": "/result/" + identifier}
        else:
            identifier = path.rsplit("/", 1)[1]
            payload = {"id": identifier, "data": [["blue" if identifier.endswith("aaaaa") else "green"]]}
        return 200, json.dumps(payload).encode()

    def close(self):
        pass


class TestControl:
    def __init__(self, switched):
        self.switched, self.event, self.thread = switched, {}, None

    def start(self, start, end, utc):
        def change():
            time.sleep(max(0, start + .1 - time.monotonic()))
            self.event["cutover_start_seconds"] = time.monotonic() - start
            self.switched.set()
            self.event["cutover_ack_seconds"] = time.monotonic() - start
            time.sleep(.15)
            self.event["proofs_completed_seconds"] = time.monotonic() - start
        self.thread = threading.Thread(target=change)
        self.thread.start()

    def finish_control(self):
        self.thread.join(timeout=2)
        if self.thread.is_alive():
            raise AssertionError("Synthetic control did not finish")
        return self.event

    def cancel(self):
        self.thread.join(timeout=2)


class QueryOwnerTests(unittest.TestCase):
    def test_real_spawn_switch_runs_during_background_and_keeps_old_continuations(self):
        from load_multiprocess import run_multiprocess
        switched = multiprocessing.get_context("spawn").Event()
        control = TestControl(switched)
        loop = OpenLoop(["https://gateway"] * 2, ["group"], "runtime", rate=100, duration=10.5,
                        concurrency=8, processes=8, cutover_control=control,
                        query_owners={"coordinator_ids": ["aaaaa", "bbbbb"], "identities": ["blue", "green"]})
        result = run_multiprocess(loop, transport_factory=functools.partial(SwitchingTransport, switched))
        self.assertFalse(result["errors"], result)
        self.assertTrue(result["cutover_control_valid"])
        self.assertFalse(control.thread.is_alive())
        for interval in result["cutover_background_overlap"].values():
            self.assertGreater(interval["started"], 0)
            self.assertGreater(interval["completed"], 0)
        serialized = json.dumps(result)
        self.assertNotIn("aaaaa", serialized)
        self.assertNotIn("cutover_samples", serialized)

    def run_load(self, transport):
        loop = OpenLoop(["https://gateway"], ["group"], "runtime", rate=100, duration=.03, concurrency=1,
                        transport=transport, query_owners={"coordinator_ids": ["aaaaa", "bbbbb"],
                                                          "identities": ["blue", "green"]})
        return loop, loop.run()

    def test_continuations_keep_initial_identifier_and_owner(self):
        loop, result = self.run_load(OwnerTransport())
        self.assertFalse(result["errors"])
        self.assertEqual(len(loop.cutover_samples), result["counts"]["started"])
        self.assertTrue(all(len(row) == 9 for row in loop.cutover_samples))
        self.assertNotIn("aaaaa", json.dumps(loop.cutover_samples))
        self.assertNotIn("20260101", json.dumps(result))

    def test_changed_identifier_or_terminal_owner_is_an_error(self):
        for transport in (OwnerTransport(changed_id=True), OwnerTransport(changed_owner=True)):
            with self.subTest(transport=transport):
                _, result = self.run_load(transport)
                self.assertTrue(result["errors"])

    def test_unknown_or_ambiguous_coordinator_mapping_is_rejected(self):
        from load_cutover import QueryOwners
        with self.assertRaises(ValueError):
            QueryOwners({"coordinator_ids": ["aaaaa", "aaaaa"], "identities": ["blue", "green"]})
        policy = QueryOwners({"coordinator_ids": ["aaaaa", "bbbbb"], "identities": ["blue", "green"]})
        with self.assertRaises(ValueError):
            policy.observe({"id": "20260101_000000_1_ccccc"})


class ControllerTests(unittest.TestCase):
    def scenario(self, *, failed_ack=False, slow_proofs=False):
        from load_cutover import CutoverCheckpoint, QueryOwners
        from test_load_checkpoints import Response
        actor = CutoverCheckpoint(["https://gateway-%d" % index for index in range(20)], "group", "blue", "green",
                                  "blue", "green", "runtime", "runtime-admin", fixture_group={})
        actor.transaction = "in-memory-transaction"
        actor.owner_policy = QueryOwners({"coordinator_ids": ["aaaaa", "bbbbb"], "identities": ["blue", "green"]})
        actor.retained = Response(body={"id": "20260101_000000_1_aaaaa", "nextUri": "/result"})
        actor.retained_identity = (0, "20260101_000000_1_aaaaa")
        actor.window_start, actor.window_end, actor.deadline = 0, 60, 50
        actor.window_utc = datetime(2030, 1, 1, tzinfo=timezone.utc)
        clock, calls = [30.0], []

        def admin(path, method="GET", body=None):
            actor.check_budget()
            calls.append(path)
            clock[0] += .01
            if path == "cutover":
                return Response(status=503 if failed_ack else 200,
                                body={"routingGroup": "group", "backendName": "green", "generation": 2})
            if path.endswith("/drain"):
                return Response(body={"openTransactions": 1, "drained": False, "readyToSeal": False, "generation": 2})
            return Response(status=409)

        def query(sql, transaction="NONE", index=0):
            actor.check_budget()
            clock[0] += 1 if slow_proofs else .01
            calls.append((sql, transaction == actor.transaction, index))
            return [Response(body={"data": [["blue" if transaction == actor.transaction else "green"]]})]

        with patch("load_cutover.time.monotonic", side_effect=lambda: clock[0]), \
                patch.object(actor, "admin", side_effect=admin), patch.object(actor, "query", side_effect=query), \
                patch.object(actor, "pages", return_value=[Response(body={"data": [["blue"]]})]):
            actor.act()
        return actor, calls

    def test_cutover_precedes_drain_and_all_twenty_proofs_leave_tail(self):
        actor, calls = self.scenario()
        self.assertIsNone(actor.failure)
        self.assertEqual(calls[:3], ["cutover", "backends/blue/drain", "backends/blue/seal"])
        self.assertEqual(len(actor.event["proofs"]), 20)
        self.assertEqual([row["gateway_index"] for row in actor.event["proofs"]], list(range(20)))
        self.assertLessEqual(actor.event["proofs_completed_seconds"], 50)
        self.assertTrue(actor.event["retained_continuation_blue"])
        self.assertFalse(any(isinstance(call, tuple) and call[0] == "ROLLBACK" for call in calls))
        self.assertNotIn(actor.transaction, json.dumps(actor.event))

    def test_unknown_ack_does_not_drain_or_restore(self):
        actor, calls = self.scenario(failed_ack=True)
        self.assertIsNotNone(actor.failure)
        self.assertEqual(calls, ["cutover"])
        self.assertFalse(actor.completed)

    def test_proofs_that_consume_tail_fail_without_further_requests(self):
        actor, calls = self.scenario(slow_proofs=True)
        self.assertEqual(actor.failure, "TimeoutError")
        self.assertFalse(actor.completed)
        self.assertLess(len(actor.event["proofs"]), 20)

    def test_cancelled_control_never_dispatches_mutation(self):
        from load_cutover import CutoverCheckpoint
        actor = CutoverCheckpoint(["https://gateway"] * 20, "group", "blue", "green", "blue", "green",
                                  "runtime", "runtime-admin", fixture_group={})
        actor.cancelled.set()
        with patch("load_cutover.request") as request, self.assertRaises(TimeoutError):
            actor.admin("cutover", "POST", {})
        request.assert_not_called()

    def test_control_requests_use_remaining_budget_not_protocol_default(self):
        from load_cutover import CutoverCheckpoint
        actor = CutoverCheckpoint(["https://gateway"] * 20, "group", "blue", "green", "blue", "green",
                                  "runtime", "runtime-admin", fixture_group={})
        actor.deadline = 11
        with patch("load_cutover.time.monotonic", return_value=10), patch("load_cutover.request") as request:
            actor.admin("cutover", "POST", {})
        self.assertEqual(request.call_args.kwargs["timeout"], 1)

    def test_live_or_failed_actor_cannot_authorize_restoration(self):
        actor, _ = self.scenario(failed_ack=True)
        actor.thread = threading.Thread(target=lambda: None)
        actor.thread.start()
        actor.thread.join()
        with patch.object(actor, "settle_and_restore") as restore, self.assertRaises(AssertionError):
            actor.finish()
        restore.assert_not_called()

    def test_actor_that_remains_alive_cannot_authorize_restoration(self):
        actor, _ = self.scenario()
        actor.thread = Mock()
        actor.thread.is_alive.return_value = True
        with patch.object(actor, "settle_and_restore") as restore, self.assertRaises(AssertionError):
            actor.finish()
        restore.assert_not_called()
        self.assertTrue(actor.cancelled.is_set())

    def test_reply_received_after_deadline_fails_before_next_mutation(self):
        from load_cutover import CutoverCheckpoint
        actor = CutoverCheckpoint(["https://gateway"] * 20, "group", "blue", "green", "blue", "green",
                                  "runtime", "runtime-admin", fixture_group={})
        actor.deadline = 11
        with patch("load_cutover.time.monotonic", side_effect=[10, 10, 12]), \
                patch("load_cutover.request") as request, self.assertRaises(TimeoutError):
            actor.admin("cutover", "POST", {})
        self.assertEqual(request.call_count, 1)


class CaseProfileTests(unittest.TestCase):
    def case(self):
        from test_matrix_case import fixture_case
        case = fixture_case(20)
        case.update(rate=1000, concurrency=512, client_processes=8, cutover_during_load={"offset_seconds": 30})
        case["groups"][0].update(source_url="http://10.1.0.1:8080", target_url="http://10.1.0.2:8080",
                                source_process_sha256="a" * 64, target_process_sha256="b" * 64)
        return case

    def test_only_explicit_profile_and_bound_fixture_endpoints_are_accepted(self):
        import copy
        from matrix_case import validate
        case = self.case()
        validate(case)
        changes = [lambda value: value.update(rate=100), lambda value: value.update(client_processes=1),
                   lambda value: value.update(concurrency=128), lambda value: value.update(duration=300),
                   lambda value: value.update(cutover_during_load={"offset_seconds": 5}),
                   lambda value: value["groups"][0].pop("source_process_sha256"),
                   lambda value: value["groups"][0].update(target_url=value["groups"][0]["source_url"])]
        for change in changes:
            candidate = copy.deepcopy(case)
            change(candidate)
            with self.subTest(change=change), self.assertRaises(ValueError):
                validate(candidate)

    def test_exact_eight_source_bundle_and_control_failure_stops_restoration(self):
        import pathlib
        import sys
        from matrix_case import run
        from contextlib import nullcontext
        from deploy.render_matrix_case import compose
        deploy = pathlib.Path(__file__).resolve().parent / "deploy"
        sys.path.insert(0, str(deploy))
        try:
            from render_load_job import render_job
            sources = {name: "source" for name in ("load_open_loop.py", "protocol.py", "load_checkpoints.py", "matrix_case.py",
                                                   "load_multiprocess.py", "load_aggregate.py", "load_cutover.py", "load_cutover_aggregate.py")}
            priority = {"metadata": {"name": "test-low", "labels": {"task": "gateway-transaction-awareness"}},
                        "value": -10, "globalDefault": False, "preemptionPolicy": "Never"}
            options = dict(priority_class=priority, admin_secret="admin", admin_secret_key="token")
            rendered = compose(render_job, "gateway-tx-lab-test", "load-test", self.case(), sources,
                               "-----BEGIN CERTIFICATE-----\nfixture", **options)
            self.assertEqual(set(rendered["items"][0]["data"]), set(sources) | {"ca.pem", "case.json"})
            for missing in ("load_cutover.py", "load_cutover_aggregate.py"):
                with self.subTest(missing=missing), self.assertRaises(ValueError):
                    compose(render_job, "gateway-tx-lab-test", "load-test", self.case(),
                            {key: value for key, value in sources.items() if key != missing},
                            "-----BEGIN CERTIFICATE-----\nfixture", **options)
        finally:
            sys.path.remove(str(deploy))
        runner = Mock()
        runner.return_value.run.side_effect = [{"errors": {}}, {"errors": {"control": 1}}]
        with patch("load_cutover.CutoverCheckpoint") as factory:
            result = run(self.case(), "runtime", "admin", runner, Mock(), lambda value: bool(value["errors"]),
                         phase=lambda seconds: nullcontext())
        self.assertNotIn("cutover_control", runner.call_args_list[0].kwargs)
        self.assertIs(runner.call_args_list[1].kwargs["cutover_control"], factory.return_value)
        self.assertFalse(result["workload_checks_passed"])
        factory.return_value.finish.assert_not_called()


if __name__ == "__main__":
    unittest.main()
