from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

import observe_resources as resources


def raw_samples(count=15):
    return "\n".join(json.dumps({
        "read_start_utc": f"2024-01-01T00:00:{second:02d}.000000Z",
        "read_end_utc": f"2024-01-01T00:00:{second:02d}.001000Z",
        "cpu.max": "200000 100000",
        "cpu.stat": f"usage_usec {second * 100000}\nnr_periods {second * 10}\nnr_throttled 0\nthrottled_usec 0",
        "memory.current": str(1000 + (second % 4) * 100), "memory.max": "8192",
    }) for second in range(count))


class TestResourceObserver(unittest.TestCase):
    def result(self, rows=None, **kwargs):
        identity = {"uid": "example-pod", "container_id": "example-container", "restarts": 0}
        options = dict(identity_before=identity, identity_after=identity, expected_cpu=2, expected_memory_bytes=8192)
        options.update(kwargs)
        return resources.summarize(rows if rows is not None else resources.parse_samples(raw_samples(), 15),
                                   "2024-01-01T00:00:02.500000Z", "2024-01-01T00:00:12.500000Z", **options)

    def test_sampler_uses_existing_python_not_external_date(self):
        command = resources.sampling_command(3)
        self.assertEqual(command[0], "python3")
        self.assertNotIn("date -u", " ".join(command))

    def test_sampler_reads_memory_as_well_as_cpu(self):
        command = " ".join(resources.sampling_command(3))
        self.assertIn("memory.current", command)
        self.assertIn("memory.max", command)

    def test_actual_embedded_program_reads_only_four_files_and_stops(self):
        sample = json.loads(raw_samples(2).splitlines()[0])
        paths = []

        def read(path):
            paths.append(str(path))
            return sample[path.name]

        command = resources.sampling_command(3)
        output = io.StringIO()
        with patch.object(sys, "argv", ["-c", command[-1]]), patch.object(Path, "read_text", read), \
                patch("time.sleep") as sleep, redirect_stdout(output):
            exec(compile(command[3], "<sampler>", "exec"), {})
        rows = resources.parse_samples(output.getvalue(), 3)
        self.assertEqual(len(rows), 3)
        self.assertEqual(sleep.call_args_list, [((1,),), ((1,),)])
        self.assertEqual(set(paths), {"/sys/fs/cgroup/" + name for name in ("cpu.max", "cpu.stat", "memory.current", "memory.max")})
        self.assertEqual(len(paths), 12)

    def test_cpu_bounds_and_nonmonotonic_memory_are_separate(self):
        result = self.result()
        self.assertTrue(result["valid"])
        self.assertAlmostEqual(result["average_cpu_cores_bounds"]["min"], .09)
        memory = result["memory"]
        self.assertEqual(memory["samples"], 10)
        self.assertEqual(memory["min_bytes"], 1000)
        self.assertEqual(memory["max_bytes"], 1300)
        self.assertEqual(memory["avg_bytes"], 1150)
        self.assertEqual(memory["sampled_change_bytes"], -300)
        self.assertEqual(memory["sampled_max_fraction_of_limit"], 1300 / 8192)
        self.assertNotIn("memory_current_bytes", result["counter_delta_bounds"])

    def test_outside_window_memory_is_not_in_summary(self):
        rows = resources.parse_samples(raw_samples(), 15)
        rows[0]["memory_current_bytes"] = 8000
        rows[-1]["memory_current_bytes"] = 7000
        self.assertEqual(self.result(rows)["memory"]["max_bytes"], 1300)

    def test_count_and_caller_timeout_are_bounded(self):
        self.assertEqual(resources.sampling_timeout(600), 660)
        for value in (True, 1, 601, "3; exit", 3.5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                resources.sampling_command(value)

    def test_embedded_program_checks_wall_budget_before_reading(self):
        command = resources.sampling_command(3)
        with patch.object(sys, "argv", ["-c", command[-1]]), patch("time.monotonic", side_effect=[0, 34]), \
                patch.object(Path, "read_text") as read, self.assertRaisesRegex(SystemExit, "deadline"):
            exec(compile(command[3], "<sampler>", "exec"), {})
        read.assert_not_called()

    def test_file_read_failure_does_not_emit_a_partial_sample(self):
        command = resources.sampling_command(3)
        output = io.StringIO()
        with patch.object(sys, "argv", ["-c", command[-1]]), patch.object(Path, "read_text", side_effect=OSError), \
                redirect_stdout(output), self.assertRaises(OSError):
            exec(compile(command[3], "<sampler>", "exec"), {})
        self.assertEqual(output.getvalue(), "")

    def test_missing_or_unlimited_memory_fails(self):
        for raw in (raw_samples().replace('"memory.max": "8192"', '"memory.max": "max"'),
                    raw_samples().replace('"memory.current": "1000"', '"memory.current": "-1"')):
            with self.assertRaises(ValueError):
                resources.parse_samples(raw, 15)

    def test_changed_memory_limit_invalidates_all_estimates(self):
        rows = resources.parse_samples(raw_samples(), 15)
        rows[8]["memory_limit_bytes"] = 4096
        result = self.result(rows)
        self.assertFalse(result["valid"])
        self.assertIsNone(result["memory"])
        self.assertIsNone(result["average_cpu_cores_bounds"])

    def test_identity_and_counter_reset_still_fail(self):
        self.assertFalse(self.result(identity_after={})["valid"])
        rows = resources.parse_samples(raw_samples(), 15)
        rows[8]["counters"]["usage_usec"] = 0
        self.assertFalse(self.result(rows)["valid"])

    def test_truncation_is_not_a_complete_receipt(self):
        with self.assertRaises(ValueError):
            resources.parse_samples(raw_samples(3), 4)

    def test_missing_window_bracket_fails(self):
        result = self.result(resources.parse_samples(raw_samples(), 15)[4:])
        self.assertFalse(result["valid"])
        self.assertIsNone(result["memory"])


if __name__ == "__main__":
    unittest.main()
