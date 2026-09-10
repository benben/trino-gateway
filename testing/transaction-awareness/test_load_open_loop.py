import json
import threading
import time
import unittest

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
