import copy
from datetime import datetime, timedelta, timezone
import importlib
import math
import unittest


class AggregateTests(unittest.TestCase):
    def setUp(self):
        self.aggregate = importlib.import_module("load_aggregate").aggregate_reports

    def fixture(self, rate=8, duration=1, concurrency=512):
        children = []
        total = int(rate * duration)
        end = (datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=duration)).isoformat().replace("+00:00", "Z")
        for index in range(8):
            count = len(range(index, total, 8))
            buckets = []
            for offset in range(math.ceil(duration)):
                n = sum(offset <= ordinal / rate < min(duration, offset + 1) for ordinal in range(index, total, 8))
                buckets.append(dict(offset_seconds=offset, duration_seconds=min(1, duration - offset), started=n,
                                    completed=n, successful=n, errors=0))
            result = dict(target_http_rps=rate / 8, scheduled_http_requests=count, client_concurrency=concurrency // 8,
                          window_seconds=duration, window_start_utc="2026-01-01T00:00:00Z",
                          window_end_utc=end, gateway_endpoints=2,
                          routing_groups=1, final_inflight=0, peak_client_inflight=min(count, 2),
                          completion_elapsed_seconds=duration + .1, client_cpu_seconds=.01,
                          counts=dict(started=count, completed=count, started_in_window=count,
                                      completed_in_window=count, successful_in_window=count,
                                      sql_submissions=count, sql_completed=count),
                          second_buckets=buckets, http_statuses={"200": count}, errors={},
                          response_error_categories={"measured": {}, "cleanup": {}},
                          per_gateway_counts=[{"measured_POST": count, "measured_status_200": count}, {}],
                          backend_result_rows={"fixture": count}, unconsumed_continuations=0)
            children.append(dict(process_index=index, process_count=8, schedule_offset_seconds=index / rate,
                                 schedule_stride_seconds=8 / rate, result=result,
                                 raw_samples={name: [index] * count for name in
                                              ("latency_ms", "scheduled_latency_ms", "client_lag_ms")}))
        options = dict(rate=rate, duration=duration, concurrency=concurrency,
                       window_start_utc="2026-01-01T00:00:00Z",
                       window_end_utc=end,
                       planned_window_start_utc=None, measurement_start_drift_seconds=None,
                       cgroup_cpu={"available": False, "valid": False},
                       completion_elapsed_seconds=duration + .2, client_setup_seconds=.3)
        return children, options

    def test_exact_percentiles_counts_and_peak_bounds(self):
        children, options = self.fixture()
        result = self.aggregate(children, **options)
        self.assertEqual(result["latency_ms"], {"p50": 3, "p95": 7, "p99": 7})
        self.assertEqual(result["counts"]["started"], 8)
        self.assertEqual(result["per_gateway_counts"][0]["measured_POST"], 8)
        self.assertEqual(result["request_rates"]["started"], dict(min=8, max=8, avg=8))
        self.assertTrue(result["client_bottleneck_detected"])
        self.assertIsNone(result["peak_client_inflight"])
        self.assertEqual(result["peak_client_inflight_bounds"], dict(min=1, max=8))
        self.assertAlmostEqual(result["client_cpu_seconds"], .08)
        self.assertAlmostEqual(result["client_average_cpu_cores"], .08 / 1.2)
        self.assertEqual(result["client_cgroup_cpu"], options["cgroup_cpu"])
        self.assertNotIn("raw_samples", result)

    def test_fractional_local_rate_keeps_global_schedule_and_buckets(self):
        children, options = self.fixture(rate=100, duration=2, concurrency=128)
        result = self.aggregate(children, **options)
        self.assertEqual(result["scheduled_http_requests"], 200)
        self.assertEqual([row["started"] for row in result["second_buckets"]], [100, 100])
        self.assertEqual(result["client_concurrency"], 128)

    def test_uneven_partition_uses_raw_sample_weights(self):
        children, options = self.fixture(rate=9)
        children[0]["raw_samples"]["latency_ms"] = [100, 100]
        result = self.aggregate(children, **options)
        self.assertEqual(result["scheduled_http_requests"], 9)
        self.assertEqual(result["latency_ms"], dict(p50=5, p95=100, p99=100))

    def test_missing_duplicate_or_failed_child_never_renormalizes(self):
        children, options = self.fixture()
        for changed in (children[:-1], children + [children[0]], [children[0]] * 8,
                        [dict(process_index=0, error="worker_failure")] + children[1:]):
            with self.subTest(changed=len(changed)), self.assertRaises(ValueError):
                self.aggregate(changed, **options)

    def test_malformed_or_inconsistent_child_rejected(self):
        mutations = [
            lambda c: c.update(schedule_offset_seconds=.9),
            lambda c: c.update(process_count=7),
            lambda c: c["result"].update(window_start_utc="2026-01-01T00:00:01Z"),
            lambda c: c["result"].update(target_http_rps=2),
            lambda c: c["result"].update(final_inflight=1),
            lambda c: c["result"].update(peak_client_inflight=65),
            lambda c: c["result"].update(completion_elapsed_seconds=9),
            lambda c: c["result"]["counts"].update(completed=0),
            lambda c: c["result"]["counts"].update(started=True),
            lambda c: c["result"]["second_buckets"][0].update(started=9),
            lambda c: c["result"]["per_gateway_counts"][0].update(measured_POST=9),
            lambda c: c["raw_samples"]["latency_ms"].append(1),
            lambda c: c["raw_samples"]["client_lag_ms"].__setitem__(0, float("nan")),
            lambda c: c["raw_samples"]["scheduled_latency_ms"].__setitem__(0, -1),
        ]
        for mutation in mutations:
            children, options = self.fixture()
            mutation(children[0])
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                self.aggregate(children, **options)

    def test_errors_cleanup_and_unconsumed_work_are_not_lost(self):
        children, options = self.fixture()
        result = children[0]["result"]
        result["errors"] = {"http_503": 1}
        result["counts"]["successful_in_window"] = 0
        result["second_buckets"][0].update(successful=0, errors=1)
        result["http_statuses"] = {"503": 1}
        result["per_gateway_counts"][0] = {"measured_POST": 1, "measured_status_503": 1}
        result["response_error_categories"]["measured"] = {"unknown_503": 1}
        result["unconsumed_continuations"] = 1
        merged = self.aggregate(children, **options)
        self.assertEqual(merged["errors"], {"http_503": 1})
        self.assertEqual(merged["unconsumed_continuations"], 1)
        self.assertEqual(merged["response_error_categories"]["measured"], {"unknown_503": 1})
        self.assertEqual(merged["request_rates"]["errors"], dict(min=1, max=1, avg=1))

    def test_cleanup_errors_remain_outside_window_but_invalidate_work(self):
        children, options = self.fixture()
        result = children[0]["result"]
        result["counts"].update(cleanup_started=1, cleanup_completed=1)
        result["http_statuses"]["503"] = 1
        result["errors"]["http_503"] = 1
        result["per_gateway_counts"][0].update(cleanup_GET=1, cleanup_status_503=1)
        result["response_error_categories"]["cleanup"] = {"unknown_503": 1}
        merged = self.aggregate(children, **options)
        self.assertEqual(merged["counts"]["started_in_window"], 8)
        self.assertEqual(merged["request_rates"]["errors"]["avg"], 0)
        self.assertEqual(merged["errors"], {"http_503": 1})
        self.assertEqual(merged["counts"]["cleanup_completed"], 1)

    def test_inputs_are_not_modified(self):
        children, options = self.fixture()
        original = copy.deepcopy((children, options))
        self.aggregate(children, **options)
        self.assertEqual((children, options), original)

    def test_http_status_contradictions_cannot_hide_failed_requests(self):
        children, options = self.fixture()
        children[0]["result"]["http_statuses"] = {"503": 1}
        with self.assertRaises(ValueError):
            self.aggregate(children, **options)
        children[0]["result"]["per_gateway_counts"][0] = {"measured_POST": 1, "measured_status_503": 1}
        with self.assertRaises(ValueError):
            self.aggregate(children, **options)

    def test_last_completion_may_precede_window_end(self):
        children, options = self.fixture()
        options["completion_elapsed_seconds"] = .99
        for child in children:
            child["result"]["completion_elapsed_seconds"] = .98
        result = self.aggregate(children, **options)
        self.assertEqual(result["window_seconds"], 1)
        self.assertEqual(result["completion_elapsed_seconds"], .99)

    def test_partial_last_bucket_and_zero_arrival_children(self):
        children, options = self.fixture(rate=9, duration=1.5)
        result = self.aggregate(children, **options)
        self.assertEqual(result["scheduled_http_requests"], 13)
        self.assertEqual(result["request_rates"]["started"], dict(min=8, max=9, avg=13 / 1.5))
        self.assertEqual(result["second_buckets"][-1]["duration_seconds"], .5)
        children, options = self.fixture(rate=1)
        result = self.aggregate(children, **options)
        self.assertEqual(result["scheduled_http_requests"], 1)
        self.assertEqual(result["latency_ms"], dict(p50=0, p95=0, p99=0))

    def test_profile_booleans_nonfinite_and_invalid_windows_rejected(self):
        for field in ("rate", "duration", "concurrency", "completion_elapsed_seconds", "client_setup_seconds"):
            for value in (True, float("nan"), float("inf")):
                children, options = self.fixture()
                options[field] = value
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    self.aggregate(children, **options)
        for field in ("target_http_rps", "window_seconds", "client_concurrency", "scheduled_http_requests", "final_inflight"):
            children, options = self.fixture()
            children[0]["result"][field] = True
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.aggregate(children, **options)
        for start in ("2026-01-01T00:00:00", "2026-01-01T00:00:00+01:00"):
            children, options = self.fixture()
            options["window_start_utc"] = start
            with self.assertRaises(ValueError):
                self.aggregate(children, **options)

    def test_none_status_exception_accounting(self):
        children, options = self.fixture()
        result = children[0]["result"]
        result["http_statuses"] = {}
        result["errors"] = {"TimeoutError": 1}
        result["per_gateway_counts"][0] = dict(measured_POST=1, measured_status_None=1)
        result["counts"]["successful_in_window"] = 0
        result["second_buckets"][0].update(successful=0, errors=1)
        merged = self.aggregate(children, **options)
        self.assertEqual(merged["http_statuses"], {"200": 7})
        self.assertEqual(merged["errors"], {"TimeoutError": 1})
        result["http_statuses"] = {"None": 1}
        with self.assertRaises(ValueError):
            self.aggregate(children, **options)

    def test_sql_counts_match_post_submissions(self):
        for field, value in (("sql_completed", 2), ("sql_submissions", 0)):
            children, options = self.fixture()
            children[0]["result"]["counts"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.aggregate(children, **options)


if __name__ == "__main__":
    unittest.main()
