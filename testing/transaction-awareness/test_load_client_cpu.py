"""Validate portable cgroup diagnostics without running backend traffic."""

import copy
import unittest
from unittest.mock import patch

import load_open_loop as load
import test_load_open_loop as fixtures


class ClientCpuTests(unittest.TestCase):
    def read(self, maximum="800000 100000", counters=None):
        if counters is None:
            counters = "usage_usec 100\nnr_periods 10\nnr_throttled 2\nthrottled_usec 20"
        with patch.object(load.Path, "read_text", side_effect=[maximum, counters]), \
                patch.object(load.time, "monotonic", side_effect=[1, 1.01]):
            return load.read_client_cpu()

    def samples(self):
        first = self.read()
        result = {}
        for index, name in enumerate(("before_setup", "before_workload", "after_completion")):
            row = copy.deepcopy(first)
            row["read_start_monotonic_seconds"] += index
            row["read_end_monotonic_seconds"] += index
            row["counters"] = {key: value + index * 10 for key, value in row["counters"].items()}
            result[name] = row
        return result

    def test_reads_verified_quota_and_numeric_counters(self):
        row = self.read()
        self.assertTrue(row["available"])
        self.assertTrue(row["valid"])
        self.assertEqual(row["cpu_max"], [800000, 100000])
        self.assertEqual(row["counters"]["nr_throttled"], 2)
        self.assertTrue(row["read_start_utc"].endswith("Z"))
        self.assertEqual(row["read_end_monotonic_seconds"], 1.01)

    def test_missing_or_unreadable_cgroup_is_explicitly_unavailable(self):
        for error in (FileNotFoundError("private path"), PermissionError("private path")):
            with self.subTest(error=type(error).__name__), patch.object(load.Path, "read_text", side_effect=error):
                row = load.read_client_cpu()
                self.assertFalse(row["available"])
                self.assertFalse(row["valid"])
                self.assertIsNone(row["counters"])
                self.assertIsNone(row["cpu_max"])
                self.assertNotIn("private path", str(row))

    def test_malformed_duplicate_missing_and_negative_counters_are_invalid(self):
        for maximum, counters in (("0 100000", None), ("max 0", None), ("800000 100000 extra", None),
                                  ("800000 100000", "usage_usec -1"),
                                  ("800000 100000", "usage_usec 1\nusage_usec 2"),
                                  ("800000 100000", "usage_usec 1")):
            with self.subTest(maximum=maximum, counters=counters):
                row = self.read(maximum, counters)
                self.assertTrue(row["available"])
                self.assertFalse(row["valid"])
                self.assertIsNone(row["counters"])

    def test_unlimited_quota_is_explicit_not_invented_capacity(self):
        row = self.read("max 100000")
        self.assertTrue(row["valid"])
        self.assertEqual(row["cpu_max"], [None, 100000])

    def test_span_reports_actual_read_bounds_and_counter_deltas(self):
        result = load.client_cpu_diagnostics(self.samples())
        self.assertTrue(result["available"])
        self.assertTrue(result["valid"])
        self.assertEqual(set(result["spans"]), {"setup_and_scheduled_wait", "workload_and_late_completion"})
        span = result["spans"]["workload_and_late_completion"]
        self.assertAlmostEqual(span["elapsed_seconds_bounds"]["min"], .99)
        self.assertAlmostEqual(span["elapsed_seconds_bounds"]["max"], 1.01)
        self.assertEqual(span["counter_deltas"]["nr_throttled"], 10)
        self.assertAlmostEqual(span["average_cpu_cores_bounds"]["min"], 10 / 1_000_000 / 1.01)
        self.assertIn("not exact timed-window", result["interpretation"])

    def test_reset_quota_change_shape_change_and_bad_order_have_no_deltas(self):
        for mutation in ("reset", "quota", "shape", "reversed", "nonfinite"):
            with self.subTest(mutation=mutation):
                samples = self.samples()
                row = samples["after_completion"]
                if mutation == "reset": row["counters"]["usage_usec"] = 0
                if mutation == "quota": row["cpu_max"] = [200000, 100000]
                if mutation == "shape": row["counters"]["new_counter"] = 1
                if mutation == "reversed": row["read_start_monotonic_seconds"] = 0
                if mutation == "nonfinite": row["read_end_monotonic_seconds"] = float("inf")
                result = load.client_cpu_diagnostics(samples)
                self.assertFalse(result["valid"])
                self.assertIsNone(result["spans"]["workload_and_late_completion"]["counter_deltas"])

    def test_receipt_captures_setup_workload_and_completion_in_order(self):
        samples = self.samples()
        reads = []
        transport = fixtures.Transport()

        def capture():
            reads.append(len(transport.requests))
            return samples[tuple(samples)[len(reads) - 1]]

        with patch.object(load, "read_client_cpu", side_effect=capture):
            result = fixtures.OpenLoopTests().run_load(transport)
        self.assertEqual(reads[:2], [0, 0])
        self.assertEqual(reads[2], result["counts"]["completed"])
        self.assertTrue(result["client_cgroup_cpu"]["valid"])


if __name__ == "__main__":
    unittest.main()
