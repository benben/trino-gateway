import unittest

from observe_cpu import parse_samples, sampling_command, summarize


def raw_samples(count=15):
    return "\n\n".join(
        f"2024-01-01T00:00:{second:02d}.000000001Z\n200000 100000\nusage_usec {second * 100000}\n"
        f"nr_periods {second * 10}\nnr_throttled 0\nthrottled_usec 0\n2024-01-01T00:00:{second:02d}.001000001Z"
        for second in range(count))


class TestCpuObserver(unittest.TestCase):
    def result(self, rows=None, before=None, after=None):
        identity = {"uid": "example-pod", "container_id": "example-container", "restarts": 0}
        return summarize(rows if rows is not None else parse_samples(raw_samples(), 15),
                         "2024-01-01T00:00:02.500000Z", "2024-01-01T00:00:12.500000Z",
                         identity_before=identity if before is None else before,
                         identity_after=identity if after is None else after, expected_cpu=2)

    def test_inside_timestamps_bound_window_without_network_timestamps(self):
        result = self.result()
        self.assertTrue(result["valid"])
        self.assertAlmostEqual(result["average_cpu_cores_bounds"]["min"], .09)
        self.assertAlmostEqual(result["average_cpu_cores_bounds"]["max"], .11)
        self.assertEqual(result["counter_delta_bounds"]["throttled_usec"], {"min": 0, "max": 0})

    def test_finite_sampling_command_is_read_only(self):
        command = sampling_command(3)
        self.assertEqual(command[:2], ["sh", "-c"])
        self.assertIn("remaining=3", command[2])
        self.assertIn("/sys/fs/cgroup/cpu.stat", command[2])
        self.assertEqual(command[2].count("date -u"), 2)
        for forbidden in ("curl", "wget", "jcmd", "kill", "rm ", "export "):
            self.assertNotIn(forbidden, command[2])

    def test_sample_count_rejects_unbounded_or_injected_values(self):
        for value in (True, 1, 601, "3; exit", 3.5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                sampling_command(value)

    def test_truncated_output_fails(self):
        with self.assertRaises(ValueError):
            parse_samples(raw_samples(3), 4)

    def test_literal_nanoseconds_or_coarse_clock_fails(self):
        for replacement in ("%NZ", "Z"):
            with self.subTest(replacement=replacement), self.assertRaises(ValueError):
                parse_samples(raw_samples(3).replace(".000000001Z", replacement), 3)

    def test_duplicate_counter_fails(self):
        with self.assertRaises(ValueError):
            parse_samples(raw_samples(3).replace("nr_periods 0", "usage_usec 0"), 3)

    def test_unlimited_quota_fails(self):
        with self.assertRaises(ValueError):
            parse_samples(raw_samples(3).replace("200000 100000", "max 100000"), 3)

    def test_identity_and_restart_must_match(self):
        for after in ({}, {"uid": "replacement", "container_id": "example-container", "restarts": 0},
                      {"uid": "example-pod", "container_id": "replacement", "restarts": 0},
                      {"uid": "example-pod", "container_id": "example-container", "restarts": 1}):
            with self.subTest(after=after):
                self.assertFalse(self.result(after=after)["valid"])

    def test_counter_reset_fails(self):
        rows = parse_samples(raw_samples(), 15)
        rows[8]["counters"]["usage_usec"] = 0
        self.assertIn("cpu_counter_reset_or_shape_changed", self.result(rows)["problems"])

    def test_sample_gap_fails(self):
        rows = parse_samples(raw_samples(), 15)
        del rows[6:9]
        self.assertIn("cpu_sample_order_or_gap", self.result(rows)["problems"])

    def test_quota_change_fails(self):
        rows = parse_samples(raw_samples(), 15)
        rows[8]["cpu_max"] = [100000, 100000]
        self.assertIn("cpu_quota_changed_or_unexpected", self.result(rows)["problems"])

    def test_slow_inside_read_fails_without_threshold_relaxation(self):
        rows = parse_samples(raw_samples(), 15)
        rows[8]["read_end_utc"] = "2024-01-01T00:00:08.200000Z"
        self.assertIn("counter_read_interval_over_100ms_or_reversed", self.result(rows)["problems"])

    def test_missing_window_bracket_never_yields_zero_usage(self):
        result = self.result(parse_samples(raw_samples(), 15)[4:])
        self.assertFalse(result["valid"])
        self.assertIsNone(result["average_cpu_cores_bounds"])


if __name__ == "__main__":
    unittest.main()
