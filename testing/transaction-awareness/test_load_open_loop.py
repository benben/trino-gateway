import json
import threading
import time
import unittest
from unittest.mock import patch

from load_open_loop import OpenLoop, distribution, invalid


class Transport:
    def __init__(self, delay=0, status=200):
        self.delay, self.status = delay, status
        self.lock = threading.Lock()
        self.requests = []

    def request(self, index, method, path, body, headers):
        with self.lock:
            self.requests.append((index, method, path, headers))
            number = len(self.requests)
        time.sleep(self.delay)
        result = {"nextUri": "http://backend/query/" + str(number)} if method == "POST" else {"data": [["blue"]]}
        return self.status, json.dumps(result).encode()

    def close(self):
        pass


class OpenLoopTests(unittest.TestCase):
    def deterministic_load(self, *, duration=2.5, rate=2, delays=None, statuses=None):
        clock = [100.0]
        delays = iter(delays or [])
        statuses = iter(statuses or [])

        class InlineExecutor:
            def __init__(self, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def submit(self, function, *args):
                function(*args)

        class ScriptedTransport(Transport):
            def request(self, index, method, path, body, headers):
                clock[0] += next(delays, .1)
                status = next(statuses, 200)
                result = {"nextUri": "http://backend/query/1"} if method == "POST" else {"data": [["blue"]]}
                return status, json.dumps(result).encode()

        def advance(seconds):
            clock[0] += seconds

        with patch("load_open_loop.ThreadPoolExecutor", InlineExecutor), \
                patch("load_open_loop.time.monotonic", side_effect=lambda: clock[0]), \
                patch("load_open_loop.time.sleep", side_effect=advance), \
                patch("load_open_loop.time.time", return_value=1700000000):
            return self.run_load(ScriptedTransport(), duration=duration, rate=rate)

    def test_window_telemetry_uses_utc_and_fractional_bucket_rates(self):
        result = self.deterministic_load(statuses=[200, 503, 200, 200, 200])
        self.assertIn("window_start_utc", result)
        self.assertEqual(result["window_start_utc"], "2023-11-14T22:13:20.000000Z")
        self.assertEqual(result["window_end_utc"], "2023-11-14T22:13:22.500000Z")
        self.assertEqual(result["second_buckets"], [
            dict(offset_seconds=0, duration_seconds=1, started=2, completed=2, successful=1, errors=1),
            dict(offset_seconds=1, duration_seconds=1, started=2, completed=2, successful=2, errors=0),
            dict(offset_seconds=2, duration_seconds=.5, started=1, completed=1, successful=1, errors=0),
        ])
        self.assertEqual(result["request_rates"], {
            "started": dict(min=2, max=2, avg=2),
            "completed": dict(min=2, max=2, avg=2),
            "successful": dict(min=1, max=2, avg=1.6),
            "errors": dict(min=0, max=1, avg=.4),
        })
        self.assertEqual(result["counts"]["started"], 5)
        self.assertEqual(result["counts"]["cleanup_started"], 1)
        self.assertEqual(sum(bucket["started"] for bucket in result["second_buckets"]), 5)

    def test_window_telemetry_includes_idle_seconds_but_not_cleanup_errors(self):
        result = self.deterministic_load(duration=3, rate=1, delays=[2.2, .1], statuses=[200, 503])
        self.assertIn("second_buckets", result)
        self.assertEqual(result["second_buckets"], [
            dict(offset_seconds=0, duration_seconds=1, started=1, completed=0, successful=0, errors=0),
            dict(offset_seconds=1, duration_seconds=1, started=0, completed=0, successful=0, errors=0),
            dict(offset_seconds=2, duration_seconds=1, started=0, completed=1, successful=1, errors=0),
        ])
        self.assertEqual(result["request_rates"]["started"], dict(min=0, max=1, avg=1 / 3))
        self.assertEqual(result["request_rates"]["errors"], dict(min=0, max=0, avg=0))
        self.assertEqual(result["errors"], {"http_503": 1})
        self.assertTrue(invalid(result))

    def test_window_telemetry_excludes_late_completions(self):
        result = self.deterministic_load(duration=.5, rate=2, delays=[.75])
        self.assertIn("second_buckets", result)
        self.assertEqual(result["second_buckets"], [
            dict(offset_seconds=0, duration_seconds=.5, started=1, completed=0, successful=0, errors=0),
        ])
        self.assertEqual(result["request_rates"]["started"], dict(min=2, max=2, avg=2))
        self.assertEqual(result["request_rates"]["completed"], dict(min=0, max=0, avg=0))
        self.assertEqual(result["counts"]["completed"], 1)
        self.assertEqual(result["achieved_http_rps"], 0)

    def test_window_telemetry_retains_inclusive_end_boundary(self):
        result = self.deterministic_load(duration=.5, rate=2, delays=[.5])
        self.assertEqual(result["second_buckets"][0]["completed"], 1)
        self.assertEqual(result["request_rates"]["completed"], dict(min=2, max=2, avg=2))
        self.assertEqual(result["achieved_http_rps"], 2)

    def run_load(self, transport, **overrides):
        options = dict(rate=100, duration=.2, concurrency=8)
        options.update(overrides)
        return OpenLoop(["http://one", "http://two"], ["group-a", "group-b"], "synthetic-placeholder",
                        transport=transport, **options).run()

    def test_rate_counts_http_separately_from_queries_and_cleanup(self):
        result = self.run_load(Transport())
        self.assertEqual(result["scheduled_http_requests"], 20)
        self.assertEqual(result["counts"]["started"], 20)
        self.assertEqual(result["counts"]["completed"], 20)
        self.assertLess(result["counts"]["sql_submissions"], 20)
        self.assertEqual(result["counts"]["sql_completed"], result["counts"]["sql_submissions"])
        self.assertEqual(result["unconsumed_continuations"], 0)

    def test_slow_server_does_not_turn_schedule_into_closed_loop(self):
        result = self.run_load(Transport(delay=.08), concurrency=1)
        self.assertEqual(result["scheduled_http_requests"], 20)
        self.assertLess(result["counts"]["started"], 10)
        self.assertGreater(result["client_dropped_requests"], 0)
        self.assertTrue(result["client_bottleneck_detected"])
        self.assertEqual(result["peak_client_inflight"], 1)

    def test_http_errors_are_not_reported_as_success(self):
        result = self.run_load(Transport(status=503))
        self.assertEqual(result["errors"], {"http_503": 20})
        self.assertEqual(result["successful_http_rps"], 0)
        self.assertEqual(result["counts"].get("sql_completed", 0), 0)

    def test_continuations_remain_gateway_relative_and_no_raw_credentials_in_receipt(self):
        transport = Transport()
        result = self.run_load(transport)
        self.assertTrue(any(method == "GET" and path.startswith("/query/") for _, method, path, _ in transport.requests))
        self.assertNotIn("synthetic-placeholder", json.dumps(result))
        self.assertNotIn("http://one", json.dumps(result))

    def test_limits_and_percentiles(self):
        self.assertEqual(distribution([1, 2, 3, 4, 5]), {"p50": 3, "p95": 5, "p99": 5})
        self.assertIsNone(distribution([])["p99"])
        for options in [{"rate": 10001}, {"duration": 601}, {"concurrency": 513}, {"timeout": 0}]:
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.run_load(Transport(), **options)

    def test_wrong_backend_is_a_failure_not_a_successful_query(self):
        result = self.run_load(Transport(), expected_backends={"group-a": "green", "group-b": "green"})
        self.assertGreater(result["errors"].get("wrong_backend_result", 0), 0)
        self.assertEqual(result["counts"].get("sql_completed", 0), 0)

    def test_sql_groups_round_robin_independently_of_get_arrivals(self):
        transport = Transport()
        result = self.run_load(transport, concurrency=1)
        groups = [headers["X-Trino-Routing-Group"] for _, method, _, headers in transport.requests if method == "POST"]
        self.assertEqual(groups, ["group-a" if index % 2 == 0 else "group-b" for index in range(len(groups))])
        self.assertGreaterEqual(len(groups), 2)
        self.assertEqual(len(result["per_gateway_counts"]), 2)
        self.assertEqual(sum(counts.get("measured_POST", 0) for counts in result["per_gateway_counts"]), result["counts"]["sql_submissions"])
        self.assertTrue(all(sum(counts.values()) > 0 for counts in result["per_gateway_counts"]))
        self.assertTrue(all(counts.get("measured_POST", 0) > 0 and counts.get("measured_GET", 0) > 0 for counts in result["per_gateway_counts"]))

    def test_invalid_warmup_cannot_be_discarded_from_overall_validity(self):
        good = {"errors": {}, "client_bottleneck_detected": False, "unconsumed_continuations": 0}
        self.assertFalse(invalid(good))
        self.assertFalse(invalid(None))
        for warmup in [dict(good, errors={"http_503": 1}), dict(good, client_bottleneck_detected=True), dict(good, unconsumed_continuations=1)]:
            self.assertTrue(invalid(warmup) or invalid(good))


if __name__ == "__main__":
    unittest.main()
