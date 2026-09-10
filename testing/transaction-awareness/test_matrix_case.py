import copy
from contextlib import nullcontext
import json
import unittest
from unittest.mock import Mock

from matrix_guard import PreservationError
from matrix_case import run, validate
from deploy.render_matrix_case import compose
import matrix_guard as fingerprints


def snapshot():
    return {"sample_time": "2026-09-10T12:00:00Z",
            "backends": [{"current_name": name, "incarnation": "inc-" + name, "state": "ACTIVE", "generation": 1}
                         for name in ("old", "new", "other-blue", "other-green")],
            "nonterminal_queries": [{"query_id": "fixture-query", "incarnation": "inc-old", "current_name": "old"}],
            "pending_admissions": [], "open_transactions": [], "retained_terminal_counts": []}


def fixture_case(replicas=2):
    image = "sha256:" + "a" * 64
    return {"case_id": "fixed-two-hot", "image_digest": image, "source_commit": "b" * 40,
            "replicas": replicas, "rate": 100, "duration": 60, "warmup": 10,
            "pod_inventory": [{"url": "https://10.0.0.%d:8443" % (index + 1), "uid": "pod-%d" % index, "spec_image": "example/gateway@" + image}
                              for index in range(replicas)],
            "groups": [{"name": "hot", "source": "new", "target": "old", "source_identity": "new", "target_identity": "old"}]}


def aggregate(names=("old", "new", "other-blue", "other-green")):
    raw = snapshot()
    raw["backends"] = [row for row in raw["backends"] if row["current_name"] in names]
    query = fingerprints.ascii_fingerprint(raw["nonterminal_queries"], ("query_id", "incarnation", "current_name"))
    hashed = lambda text: fingerprints.hashlib.sha256(text.encode("ascii")).hexdigest()
    return {"sample_time": "2026-09-10T12:00:00Z", "ascii_valid": True, "nonterminal_queries": query,
            "pending_admissions": {"count": 0, "sha256": fingerprints.EMPTY},
            "open_transactions": {"count": 0, "sha256": fingerprints.EMPTY},
            "retained_terminal_counts": [], "query_groups": [dict(query, incarnation_hash=hashed("inc-old"))],
            "backends": [{"name": row["current_name"], "state": row["state"], "generation": row["generation"],
                          "incarnation_hash": hashed(row["incarnation"])} for row in raw["backends"]],
            "backend_set_sha256": fingerprints.ascii_fingerprint(raw["backends"], ("current_name", "incarnation"))["sha256"]}


class FingerprintTests(unittest.TestCase):
    def compare(self, before, after, **kwargs):
        before_samples, after_samples = [copy.deepcopy(before), copy.deepcopy(before)], [copy.deepcopy(after), copy.deepcopy(after)]
        for sample, second in zip(before_samples + after_samples, range(4)):
            sample["sample_time"] = "2026-09-10T12:00:0%dZ" % second
        return fingerprints.compare(before_samples, after_samples, "old",
                                    ("old", "new", "other-blue", "other-green"), **kwargs)

    def test_duplicate_samples_are_not_two_fresh_observations(self):
        sample = aggregate()
        with self.assertRaises(PreservationError):
            fingerprints.compare([sample, sample], [sample, sample], "old", ("old", "new", "other-blue", "other-green"))

    def test_pre_snapshot_cannot_be_reused_as_post_snapshot(self):
        first, second = aggregate(), aggregate()
        second["sample_time"] = "2026-09-10T12:00:01+00:00"
        with self.assertRaises(PreservationError):
            fingerprints.compare([first, second], [first, second], "old", ("old", "new", "other-blue", "other-green"))

    def test_timestamp_requires_explicit_utc_and_post_follows_completion(self):
        for timestamp in ("2026-09-10T12:00:00", "2026-09-10T13:00:00+01:00", "invalid"):
            with self.assertRaises(PreservationError):
                fingerprints.utc_timestamp(timestamp)
        self.assertEqual(fingerprints.utc_timestamp("2026-09-10T12:00:00Z"), fingerprints.utc_timestamp("2026-09-10T12:00:00+00:00"))
        with self.assertRaises(PreservationError):
            self.compare(aggregate(), aggregate(), case_completed_utc="2026-09-10T12:00:10Z")

    def test_same_exact_aggregate_passes_without_exported_ids(self):
        value = aggregate()
        self.assertTrue(self.compare(value, value)["passed"])
        self.assertNotIn("fixture-query", json.dumps(value))

    def test_same_count_different_digest_rejects(self):
        before, after = aggregate(), aggregate()
        after["nonterminal_queries"]["sha256"] = "b" * 64
        with self.assertRaises(PreservationError):
            self.compare(before, after)

    def test_explicit_lazy_initialization_does_not_allow_replacement(self):
        before = aggregate(("old", "other-blue"))
        after = aggregate(("old", "other-blue", "new"))
        self.assertTrue(self.compare(before, after, allow_initialized=("new",), require_complete=False)["passed"])
        with self.assertRaises(PreservationError):
            self.compare(before, after, require_complete=False)
        with self.assertRaises(PreservationError):
            self.compare(before, after, allow_initialized=("new",))
        after["backends"][0]["incarnation_hash"] = "c" * 64
        with self.assertRaises(PreservationError):
            self.compare(before, after, allow_initialized=("new",), require_complete=False)

    def test_non_ascii_or_missing_count_cannot_pass(self):
        for mutate in (lambda item: item.update(ascii_valid=False),
                       lambda item: item["nonterminal_queries"].pop("count"),
                       lambda item: item["pending_admissions"].update(count=1)):
            after = aggregate()
            mutate(after)
            with self.assertRaises(PreservationError):
                self.compare(aggregate(), after)

    def test_ascii_length_prefix_is_unambiguous_and_order_independent(self):
        first = [{"id": "a", "value": "bc"}, {"id": "d", "value": "ef"}]
        second = [{"id": "ab", "value": "c"}, {"id": "d", "value": "ef"}]
        fields = ("id", "value")
        self.assertNotEqual(fingerprints.ascii_fingerprint(first, fields), fingerprints.ascii_fingerprint(second, fields))
        self.assertEqual(fingerprints.ascii_fingerprint(first, fields), fingerprints.ascii_fingerprint(list(reversed(first)), fields))
        self.assertEqual(fingerprints.ascii_fingerprint([], fields), {"count": 0, "sha256": fingerprints.EMPTY})
        with self.assertRaises(UnicodeEncodeError):
            fingerprints.ascii_fingerprint([{"id": "é", "value": "x"}], fields)


class RunnerTests(unittest.TestCase):
    def execute(self, case=None, receipts=None, checkpoint=None):
        runner = Mock()
        runner.return_value.run.side_effect = receipts or [{"bad": False}, {"bad": False, "successful_http_rps": 100}]
        checkpoint = checkpoint or Mock()
        checkpoint.finish.return_value = {"all_gateway_endpoints_checked": (case or fixture_case())["replicas"]}
        factory = Mock(return_value=checkpoint)
        result = run(case or fixture_case(), "runtime-query-auth", "runtime-admin-token", runner, factory,
                     lambda receipt: bool(receipt and receipt.get("bad")), phase=lambda seconds: nullcontext())
        return result, runner, factory, checkpoint

    def test_all_hundred_distinct_pods_reach_checkpoint_and_load(self):
        result, runner, factory, checkpoint = self.execute(fixture_case(100))
        self.assertTrue(result["workload_checks_passed"])
        self.assertFalse(result["valid_run"])
        self.assertTrue(result["external_postguard_required"])
        self.assertEqual(len(factory.call_args.args[0]), 100)
        self.assertEqual(len(runner.call_args.args[0]), 100)
        self.assertEqual(result["checkpoints"][0]["all_gateway_endpoints_checked"], 100)
        checkpoint.begin.assert_called_once()
        checkpoint.finish.assert_called_once()

    def test_invalid_warmup_does_not_start_transaction_or_measurement(self):
        result, runner, factory, checkpoint = self.execute(receipts=[{"bad": True}])
        self.assertFalse(result["valid_run"])
        self.assertEqual(runner.call_count, 1)
        factory.assert_not_called()

    def test_capacity_failure_retains_invalid_flag_but_runs_known_checkpoint_finish(self):
        result, runner, factory, checkpoint = self.execute(receipts=[{"bad": False}, {"bad": True}])
        self.assertFalse(result["valid_run"])
        self.assertTrue(result["checkpoints_valid"])
        checkpoint.finish.assert_called_once()

    def test_success_below_target_is_not_accepted_even_without_transport_errors(self):
        result, runner, factory, checkpoint = self.execute(receipts=[{"bad": False}, {"bad": False, "successful_http_rps": 60}])
        self.assertFalse(result["workload_checks_passed"])
        checkpoint.finish.assert_called_once()

    def test_target_threshold_is_explicit_and_boundary_inclusive(self):
        for actual, expected in ((98.999, False), (99, True), (100, True), (float("inf"), False), (float("nan"), False)):
            result, _, _, _ = self.execute(receipts=[{"bad": False}, {"bad": False, "successful_http_rps": actual}])
            self.assertEqual(result["workload_checks_passed"], expected)
            self.assertEqual(result["target_acceptance"]["minimum_fraction"], .99)
            self.assertEqual(result["target_acceptance"]["minimum_successful_http_rps"], 99)

    def test_groups_cannot_share_response_identities_under_distinct_backend_names(self):
        case = fixture_case()
        case["groups"].append(dict(case["groups"][0], name="other", source="other-blue", target="other-green"))
        with self.assertRaises(ValueError):
            validate(case)

    def test_checkpoint_failure_does_not_retry_or_run_unconditional_cleanup(self):
        checkpoint = Mock()
        checkpoint.finish.side_effect = AssertionError("private server response must not be printed")
        result, runner, factory, checkpoint = self.execute(checkpoint=checkpoint)
        self.assertFalse(result["valid_run"])
        self.assertEqual(result["failure"]["stage"], "checkpoint_finish")
        self.assertTrue(result["state_preserved_without_unconditional_cleanup"])
        self.assertNotIn("private server", json.dumps(result))
        checkpoint.finish.assert_called_once()

    def test_wrong_or_duplicate_image_endpoint_and_alias_groups_rejected(self):
        changes = [lambda c: c["pod_inventory"][0].update(spec_image="example/gateway@sha256:" + "c" * 64),
                   lambda c: c["pod_inventory"][0].update(url=c["pod_inventory"][1]["url"]),
                   lambda c: c["pod_inventory"][0].update(url="https://gateway:8443"),
                   lambda c: c["groups"].append(dict(c["groups"][0], name="other"))]
        for change in changes:
            case = fixture_case()
            change(case)
            with self.assertRaises(ValueError):
                validate(case)

    def test_scheduled_start_applies_only_to_measurement(self):
        case = fixture_case()
        case["measurement_start_utc"] = "2026-09-10T12:00:00Z"
        result, runner, factory, checkpoint = self.execute(case)
        self.assertNotIn("measurement_start_utc", runner.call_args_list[0].kwargs)
        self.assertEqual(runner.call_args_list[1].kwargs["measurement_start_utc"], case["measurement_start_utc"])


class RendererTests(unittest.TestCase):
    def test_composition_preserves_scope_and_secret_references(self):
        def renderer(*args, **kwargs):
            return {"items": [{"kind": "ConfigMap", "data": {"ca.pem": "public"}},
                              {"kind": "Job", "spec": {"activeDeadlineSeconds": 250, "backoffLimit": 0,
                                                        "template": {"spec": {"automountServiceAccountToken": False,
                                                                              "containers": [{"env": [], "resources": {"limits": {"cpu": "2", "memory": "2Gi"}}}]}}}}]}
        sources = {name: "source" for name in ("load_open_loop.py", "protocol.py", "load_checkpoints.py", "matrix_case.py")}
        result = compose(renderer, "gateway-tx-lab-test", "load-test", fixture_case(), sources, "public",
                         priority_class={}, admin_secret="existing-admin", admin_secret_key="token")
        config, job = result["items"]
        container = job["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(container["env"][-1], {"name": "TX_ADMIN_TOKEN", "valueFrom": {"secretKeyRef": {"name": "existing-admin", "key": "token"}}})
        self.assertEqual(container["command"], ["python", "/source/matrix_case.py"])
        self.assertEqual(set(config["data"]), set(sources) | {"case.json", "ca.pem"})
        self.assertEqual(job["spec"]["activeDeadlineSeconds"], 1150)
        self.assertEqual(job["spec"]["backoffLimit"], 0)
        self.assertFalse(job["spec"]["template"]["spec"]["automountServiceAccountToken"])
        self.assertEqual([item["kind"] for item in result["items"]], ["ConfigMap", "Job"])

    def test_scheduled_five_minute_job_remains_bounded(self):
        import pathlib
        import sys
        deploy = pathlib.Path(__file__).resolve().parent / "deploy"
        sys.path.insert(0, str(deploy))
        try:
            from render_load_job import render_job
            case = fixture_case(100)
            case.update(duration=300, measurement_start_utc="2026-09-10T12:00:00Z")
            sources = {name: "source" for name in ("load_open_loop.py", "protocol.py", "load_checkpoints.py", "matrix_case.py")}
            priority = {"metadata": {"name": "gateway-transaction-test-nonpreempting", "labels": {"task": "gateway-transaction-awareness"}},
                        "value": -1000, "globalDefault": False, "preemptionPolicy": "Never"}
            result = compose(render_job, "gateway-tx-lab-test", "load-test", case, sources,
                             "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----", priority_class=priority,
                             admin_secret="existing-admin", admin_secret_key="token")
            self.assertEqual(result["items"][1]["spec"]["activeDeadlineSeconds"], 1990)
        finally:
            sys.path.remove(str(deploy))


class FinalGuardTests(unittest.TestCase):
    def inputs(self):
        before, after = [aggregate(), aggregate()], [aggregate(), aggregate()]
        for item, timestamp in zip(before + after, ("11:59:58", "11:59:59", "12:01:01", "12:01:02")):
            item["sample_time"] = "2026-09-10T" + timestamp + "Z"
        image = "sha256:" + "a" * 64
        names = ("old", "new", "other-blue", "other-green")
        pods = [{"name": "gateway-%d" % index, "uid": "gateway-uid-%d" % index, "role": "gateway", "spec_image": "example/gateway@" + image,
                 "observed_image_id": "example/gateway@sha256:" + ("c" if index else "d") * 64,
                 "restart_count": 0, "ready": True, "phase": "Running", "deletion_timestamp": None} for index in range(2)]
        pods.extend({"name": "fixture-" + name, "uid": "fixture-uid-" + name, "role": "fixture", "fixture_name": name,
                     "spec_image": "example/fixture@sha256:" + "b" * 64, "observed_image_id": "example/fixture@sha256:" + "e" * 64,
                     "restart_count": 0, "ready": True, "phase": "Running", "deletion_timestamp": None}
                    for name in names)
        pod_before = {"sample_time": "2026-09-10T11:59:59Z", "pods": pods}
        pod_after = copy.deepcopy(pod_before)
        pod_after["sample_time"] = "2026-09-10T12:01:03Z"
        result = {"replicas": 2, "image_digest": image, "rate": 100, "started_utc": "2026-09-10T12:00:00Z",
                  "completed_utc": "2026-09-10T12:01:00Z", "workload_checks_passed": True,
                  "transport_valid": True, "checkpoints_valid": True,
                  "measurement": {"target_http_rps": 100, "successful_http_rps": 100},
                  "target_acceptance": {"target_http_rps": 100, "successful_http_rps": 100, "minimum_fraction": .99,
                                        "minimum_successful_http_rps": 99, "target_achieved": True}}
        return [result, before, after, pod_before, pod_after, "old", names, names]

    def test_final_acceptance_requires_fresh_ledger_and_all_pods(self):
        result = fingerprints.finalize(*self.inputs())
        self.assertTrue(result["valid_run"])
        self.assertFalse(result["external_postguard_required"])
        self.assertEqual(result["external_postguard"]["unchanged_pod_count"], 6)

    def test_existing_tagged_fixture_requires_unchanged_observed_identity(self):
        args = self.inputs()
        for snapshot in args[3:5]:
            for pod in snapshot["pods"][2:]:
                pod["spec_image"] = "example/fixture:test"
        self.assertTrue(fingerprints.finalize(*args)["valid_run"])
        args[4]["pods"][2]["observed_image_id"] = "example/fixture@sha256:" + "f" * 64
        with self.assertRaises(PreservationError):
            fingerprints.finalize(*args)

    def test_pod_replacement_restart_image_or_missing_fixture_fails(self):
        changes = [lambda item: item["pods"][0].update(uid="replacement"),
                   lambda item: item["pods"][0].update(spec_image="example/gateway@sha256:" + "b" * 64),
                   lambda item: item["pods"][0].update(observed_image_id="example/gateway@sha256:" + "f" * 64),
                   lambda item: item["pods"][2].update(restart_count=1),
                   lambda item: item["pods"][2].update(observed_image_id="example/fixture@sha256:" + "f" * 64),
                   lambda item: item["pods"].pop(),
                   lambda item: item["pods"][0].update(ready=False)]
        for change in changes:
            args = self.inputs()
            change(args[4])
            with self.assertRaises(PreservationError):
                fingerprints.finalize(*args)

    def test_premature_or_stale_evidence_does_not_finalize(self):
        for index, timestamp in ((3, "2026-09-10T11:00:00Z"), (4, "2026-09-10T12:00:59Z"), (4, "2026-09-10T12:04:00Z")):
            args = self.inputs()
            args[index]["sample_time"] = timestamp
            with self.assertRaises(PreservationError):
                fingerprints.finalize(*args)
        args = self.inputs()
        args[2][0]["sample_time"] = "2026-09-10T12:00:59Z"
        with self.assertRaises(PreservationError):
            fingerprints.finalize(*args)

    def test_clean_postguard_does_not_convert_under_target_or_invalid_transport_to_success(self):
        args = self.inputs()
        args[0]["target_acceptance"]["successful_http_rps"] = 60
        args[0]["target_acceptance"]["target_achieved"] = False
        args[0]["measurement"]["successful_http_rps"] = 60
        args[0]["workload_checks_passed"] = False
        result = fingerprints.finalize(*args)
        self.assertFalse(result["valid_run"])
        self.assertTrue(result["external_postguard"]["ledger"]["passed"])
        args = self.inputs()
        args[0]["transport_valid"] = False
        self.assertFalse(fingerprints.finalize(*args)["valid_run"])

    def test_contradictory_or_missing_measurement_cannot_finalize(self):
        changes = [lambda item: item.pop("measurement"),
                   lambda item: item.pop("rate"),
                   lambda item: item["measurement"].update(successful_http_rps=60),
                   lambda item: item["measurement"].update(target_http_rps=1000),
                   lambda item: item["target_acceptance"].update(minimum_fraction=.5),
                   lambda item: item["target_acceptance"].update(successful_http_rps=1000),
                   lambda item: item["target_acceptance"].update(target_achieved=False)]
        for change in changes:
            args = self.inputs()
            change(args[0])
            with self.assertRaises(PreservationError):
                fingerprints.finalize(*args)


if __name__ == "__main__":
    unittest.main()
