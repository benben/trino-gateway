import unittest
from unittest.mock import patch
from types import SimpleNamespace
import json
import tempfile
from pathlib import Path

from database_metrics import gauge_summary, io_summary, cost_summary, capture, report, write_private


def gauge(timestamp, average, low=None, high=None):
    return {"Timestamp": timestamp, "Average": average, "Minimum": average if low is None else low,
            "Maximum": average if high is None else high, "SampleCount": 60}


class DatabaseMetricTests(unittest.TestCase):
    def test_capacity_partial_sample_counts_prevent_cost_claim(self):
        point = gauge(0, 16)
        point["SampleCount"] = 30
        capacity = gauge_summary([point], 0, 60, expected_samples=60)
        self.assertFalse(capacity["complete_sample_coverage"])
        self.assertIsNone(cost_summary(capacity, None, 1, 1, .12, .20)["compute_usd"])
        self.assertIsNone(gauge_summary([point], 0, 60)["complete_sample_coverage"])

    def test_invalid_request_counts_are_rejected(self):
        capacity = gauge_summary([gauge(0, 16)], 0, 60)
        for successes, attempts in ((float("inf"), float("inf")), (True, 1), (1.5, 2), (-1, 2), (3, 2)):
            with self.subTest(successes=successes), self.assertRaises(ValueError):
                cost_summary(capacity, None, successes, attempts, .12, .20)

    def test_capture_dimensions_identity_and_sample_coverage(self):
        args = SimpleNamespace(start=0, end=300, replicas=2, profile="test", region="example", cluster="lab",
                               instance="writer", output="unused", case_id="idle-2", gateway_image="example@sha256:" + "a" * 64)
        calls = []
        def aws(command, **kwargs):
            calls.append(command)
            if "describe-db-clusters" in command:
                result = {"DBClusters": [{"DBClusterMembers": [{"DBInstanceIdentifier": "writer", "IsClusterWriter": True}],
                                         "DBClusterIdentifier": "lab", "Engine": "aurora-postgresql"}]}
            elif "describe-db-instances" in command:
                result = {"DBInstances": [{"DBInstanceClass": "db.serverless", "DBClusterIdentifier": "lab", "DbiResourceId": "unique-writer"}]}
            else:
                name = command[command.index("--metric-name") + 1]
                is_io = name.startswith("Volume")
                self.assertIn("Name=DBClusterIdentifier,Value=lab" if is_io else "Name=DBInstanceIdentifier,Value=writer", command)
                self.assertEqual(command[command.index("--period") + 1], "300" if is_io else "60")
                self.assertEqual("Sum" in command, is_io)
                result = {"Datapoints": [{"Timestamp": 0, "Sum": 10}] if is_io else [gauge(i, 16) for i in range(0, 300, 60)]}
            return SimpleNamespace(returncode=0, stdout=json.dumps(result))
        with patch("database_metrics.subprocess.run", side_effect=aws), patch("database_metrics.write_private") as write:
            capture(args)
        data = write.call_args.args[1]
        self.assertEqual(len(calls), 12)
        self.assertTrue(data["metrics"]["ServerlessDatabaseCapacity"]["summary"]["complete_sample_coverage"])
        self.assertEqual(data["case_id"], "idle-2")
        self.assertEqual(data["writer_identity"], {"instance_identifier": "writer", "resource_id": "unique-writer"})

    def test_report_rejects_window_mismatch_and_mismatched_idle_image(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            capacity = gauge_summary([gauge(0, 16)], 0, 60)
            data = {"start_utc": 0, "end_utc": 60, "replicas": 2, "region": "example", "cluster_configuration": {},
                    "gateway_image": "image-a", "writer_identity": "writer-a", "metrics": {"ServerlessDatabaseCapacity": {"summary": capacity},
                    "VolumeReadIOPs": {"summary": io_summary([], 0, 60)}, "VolumeWriteIOPs": {"summary": io_summary([], 0, 60)}}}
            load = {"window_start_utc": 2, "window_end_utc": 62, "counts": {"started_in_window": 1}}
            for name, value in (("capture", data), ("load", load), ("idle", data | {"gateway_image": "image-b"})):
                write_private(root / name, value)
            args = SimpleNamespace(capture=root / "capture", load=root / "load", idle=None, output=root / "out", acu_hour_usd=.12, io_million_usd=.2)
            with self.assertRaisesRegex(ValueError, "align"):
                report(args)
            load.update(window_start_utc=0, window_end_utc=60)
            write_private(root / "aligned", load)
            args.load, args.idle = root / "aligned", root / "idle"
            with self.assertRaisesRegex(ValueError, "baseline"):
                report(args)
            write_private(root / "other-writer", data | {"writer_identity": "writer-b"})
            args.idle = root / "other-writer"
            with self.assertRaisesRegex(ValueError, "baseline"):
                report(args)
            args.idle = None
            report(args)
            self.assertFalse(json.loads(args.output.read_text())["valid_throughput_run"])
            self.assertEqual(args.output.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                report(args)

    def test_capacity_integrates_actual_values_not_configured_maximum(self):
        result = gauge_summary([gauge(0, 16), gauge(60, 20, 16, 24)], 0, 120)
        self.assertEqual((result["min"], result["max"], result["avg"]), (16, 24, 18))
        self.assertEqual(result["value_seconds"], 2160)
        self.assertTrue(result["complete_aligned_window"])

    def test_missing_capacity_is_not_zero_or_complete(self):
        result = gauge_summary([gauge(0, 16)], 0, 120)
        self.assertEqual(result["avg"], 16)
        self.assertEqual(result["missing_seconds"], 60)
        self.assertFalse(result["complete_aligned_window"])
        self.assertIsNone(cost_summary(result, None, 100, 100, .12, .20)["compute_usd"])

    def test_partial_buckets_are_labelled_not_exact_cost(self):
        result = gauge_summary([gauge(0, 16), gauge(60, 20)], 30, 90)
        self.assertEqual(result["avg"], 18)
        self.assertTrue(result["partial_buckets"])
        self.assertFalse(result["complete_aligned_window"])

    def test_duplicate_and_invalid_samples_fail_closed(self):
        for points in ([gauge(0, 16), gauge(0, 20)], [gauge(0, float("nan"))], [gauge(0, 5, 8, 10)]):
            with self.subTest(points=points), self.assertRaises(ValueError):
                gauge_summary(points, 0, 60)

    def test_five_minute_io_is_a_count_not_an_iops_gauge(self):
        result = io_summary([{"Timestamp": 0, "Sum": 300, "SampleCount": 1}], 0, 300)
        self.assertEqual(result["count"], 300)
        self.assertEqual(result["avg_per_second"], 1)
        self.assertTrue(result["complete_aligned_window"])
        self.assertFalse(io_summary([{"Timestamp": 0, "Sum": 300}], 60, 120)["complete_aligned_window"])

    def test_zero_successes_do_not_create_zero_request_cost(self):
        capacity = gauge_summary([gauge(0, 16)], 0, 60)
        result = cost_summary(capacity, None, 0, 100, .12, .20)
        self.assertAlmostEqual(result["compute_usd"], .032)
        self.assertIsNone(result["compute_usd_per_successful_request"])
        self.assertAlmostEqual(result["compute_usd_per_attempt"], .00032)

    def test_idle_floor_and_observed_cost_are_separate(self):
        capacity = gauge_summary([gauge(0, 16)], 0, 60)
        result = cost_summary(capacity, None, 6000, 6000, .12, .20, idle_acu_average=16)
        self.assertEqual(result["incremental_allocated_compute_usd"], 0)
        self.assertGreater(result["compute_usd_per_successful_request"], 0)

    def test_io_requires_both_complete_read_and_write_windows(self):
        capacity = gauge_summary([gauge(i, 16) for i in range(0, 300, 60)], 0, 300)
        io = {"read": io_summary([{"Timestamp": 0, "Sum": 100}], 0, 300),
              "write": io_summary([{"Timestamp": 0, "Sum": 900}], 0, 300)}
        result = cost_summary(capacity, io, 1000, 1000, .12, .20)
        self.assertAlmostEqual(result["io_usd"], .0002)
        self.assertAlmostEqual(result["compute_plus_io_usd"], .1602)
        io["write"] = io_summary([], 0, 300)
        self.assertIsNone(cost_summary(capacity, io, 1000, 1000, .12, .20)["compute_plus_io_usd"])


if __name__ == "__main__":
    unittest.main()
