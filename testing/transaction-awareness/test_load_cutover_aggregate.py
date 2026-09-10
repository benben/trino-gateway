from collections import Counter
import copy
import importlib
import math
import unittest


class CutoverAggregateTests(unittest.TestCase):
    def setUp(self):
        self.summarize = importlib.import_module("load_cutover_aggregate").summarize_cutover

    def children(self, records=None):
        records = records or [[0, 1, 1.1, 1.2, .9, 200, 0, 0, -1],
                              [0, 4, 4.1, 4.2, 3.9, 200, 0, 1, -1]]
        children = []
        for index in range(8):
            rows = copy.deepcopy(records if index == 0 else [])
            methods, statuses = Counter(), Counter()
            for row in rows:
                methods["measured_" + ("POST" if row[0] == 0 else "GET")] += 1
                statuses["measured_status_" + (str(row[5]) if row[5] else "None")] += 1
            children.append(dict(process_index=index, cutover_samples=rows,
                                 result=dict(counts=dict(started=len(rows), completed=len(rows),
                                                        started_in_window=sum(row[1] <= 6 for row in rows),
                                                        completed_in_window=sum(row[3] <= 6 for row in rows),
                                                        successful_in_window=sum(row[3] <= 6 and not row[6] for row in rows)),
                                             errors={"synthetic_error": sum(row[6] for row in rows)} if any(row[6] for row in rows) else {},
                                             per_gateway_counts=[dict(methods + statuses)])))
        return children

    def summarize_rows(self, records=None):
        return self.summarize(self.children(records), duration=6, cutover_start_seconds=2, cutover_ack_seconds=3)

    def test_exact_cohorts_empty_during_and_no_raw_output(self):
        result = self.summarize_rows()
        phases = result["request_start_cohorts"]
        self.assertEqual([phases[key]["requests"] for key in ("before", "during", "after")], [1, 0, 1])
        self.assertEqual(phases["during"]["latency_ms"], dict(p50=None, p95=None, p99=None))
        self.assertAlmostEqual(phases["before"]["latency_ms"]["p99"], 200)
        self.assertNotIn("cutover_samples", result)
        self.assertEqual(result["ownership_checks"]["post_ack_post_starts"], 1)

    def test_overlap_allows_both_owners_and_old_get_remains_source(self):
        rows = [[0, 1, 1.1, 1.2, .9, 200, 0, 0, -1],
                [0, 1.8, 2.5, 3.5, 1.7, 200, 0, 1, -1],
                [0, 2.2, 2.3, 3.6, 2.1, 200, 0, 0, -1],
                [1, 4, 4.1, 4.2, 3.9, 200, 0, 0, 0],
                [0, 4.2, 4.3, 4.4, 4.1, 200, 0, 1, -1]]
        result = self.summarize_rows(rows)
        self.assertEqual(result["ownership_checks"], dict(pre_cutover_post_responses=1,
                         post_ack_post_starts=1, overlapping_post_requests=2, pinned_continuations=1))
        self.assertEqual(result["request_start_cohorts"]["after"]["observed_owners"]["source"], 1)

    def test_wrong_temporal_owner_or_reassigned_get_rejected(self):
        for row in ([0, 1, 1.1, 1.2, .9, 200, 0, 1, -1],
                    [0, 4, 4.1, 4.2, 3.9, 200, 0, 0, -1],
                    [1, 4, 4.1, 4.2, 3.9, 200, 0, 1, 0]):
            with self.subTest(row=row), self.assertRaises(ValueError):
                self.summarize_rows([row] + self.children()[0]["cutover_samples"])

    def test_equality_boundaries_remain_overlap(self):
        rows = self.children()[0]["cutover_samples"] + [
            [0, 1.5, 1.6, 2, 1.4, 200, 0, 1, -1],
            [0, 2.5, 3, 3.1, 2.4, 200, 0, 0, -1]]
        self.assertEqual(self.summarize_rows(rows)["ownership_checks"]["overlapping_post_requests"], 2)

    def test_late_completion_and_late_start_are_explicit(self):
        rows = self.children()[0]["cutover_samples"] + [
            [0, 5.5, 5.6, 6.2, 5.4, 200, 0, 1, -1],
            [1, 6.1, 6.2, 6.3, 5.9, 200, 0, 0, 0]]
        phase = self.summarize_rows(rows)["request_start_cohorts"]["after"]
        self.assertEqual((phase["late_completions"], phase["late_starts"]), (2, 1))
        self.assertEqual((phase["started_in_window"], phase["completed_in_window"]), (2, 1))

    def test_failed_unknown_owner_and_cleanup_error_are_not_erased(self):
        rows = self.children()[0]["cutover_samples"] + [[0, 2.2, 2.3, 2.4, 2.1, 0, 1, -1, -1]]
        children = self.children(rows)
        children[0]["result"]["counts"].update(cleanup_completed=1)
        children[0]["result"]["errors"]["cleanup_error"] = 1
        result = self.summarize(children, duration=6, cutover_start_seconds=2, cutover_ack_seconds=3)
        self.assertEqual(result["request_start_cohorts"]["during"]["errors"], 1)
        self.assertEqual(result["request_start_cohorts"]["during"]["http_statuses"], {"0": 1})

    def test_missing_duplicate_and_mismatched_accounting_rejected(self):
        for change in (lambda c: c.pop(), lambda c: c[0].update(process_index=1),
                       lambda c: c[0]["cutover_samples"].pop(),
                       lambda c: c[0]["result"]["counts"].update(started=True),
                       lambda c: c[0]["result"]["per_gateway_counts"][0].update(measured_status_503=1)):
            children = self.children()
            change(children)
            with self.assertRaises(ValueError):
                self.summarize(children, duration=6, cutover_start_seconds=2, cutover_ack_seconds=3)

    def test_malformed_times_codes_and_records_rejected(self):
        for field, values in ((0, (True, 2)), (1, (float("nan"), float("inf"), -.1)),
                              (2, (.5,)), (3, (1.05,)), (4, (1.1, 6)),
                              (5, (True, -1, 0, 99, 503, 600)), (6, (True, 2)),
                              (7, (True, -1, 2)), (8, (0,))):
            for value in values:
                children = self.children()
                children[0]["cutover_samples"][0][field] = value
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    self.summarize(children, duration=6, cutover_start_seconds=2, cutover_ack_seconds=3)

    def test_invalid_event_bounds_or_absent_background_phase_rejected(self):
        for start, ack in ((-1, 3), (3, 2), (2, 6), (True, 3), (2, math.inf)):
            with self.assertRaises(ValueError):
                self.summarize(self.children(), duration=6, cutover_start_seconds=start, cutover_ack_seconds=ack)
        with self.assertRaises(ValueError):
            self.summarize_rows([[0, 1, 1.1, 1.2, .9, 200, 0, 0, -1]])

    def test_empty_child_cannot_claim_window_activity(self):
        children = self.children()
        children[1]["result"]["counts"]["started_in_window"] = 1
        with self.assertRaises(ValueError):
            self.summarize(children, duration=6, cutover_start_seconds=2, cutover_ack_seconds=3)

    def test_missing_error_accounting_cannot_hide_unknown_response(self):
        rows = self.children()[0]["cutover_samples"] + [[0, 2.2, 2.3, 2.4, 2.1, 0, 1, -1, -1]]
        children = self.children(rows)
        children[0]["result"]["errors"] = {}
        with self.assertRaises(ValueError):
            self.summarize(children, duration=6, cutover_start_seconds=2, cutover_ack_seconds=3)

    def test_exact_pooled_percentiles_and_input_preservation(self):
        children = self.children()
        for index in range(1, 8):
            row = [0, 4, 4.1, 4.2 + index / 10, 3.9, 200, 0, 1, -1]
            replacement = self.children([row])[0]
            replacement["process_index"] = index
            children[index] = replacement
        before = copy.deepcopy(children)
        result = self.summarize(children, duration=6, cutover_start_seconds=2, cutover_ack_seconds=3)
        values = result["request_start_cohorts"]["after"]["latency_ms"]
        self.assertAlmostEqual(values["p50"], 500)
        self.assertAlmostEqual(values["p95"], 900)
        self.assertAlmostEqual(values["p99"], 900)
        self.assertEqual(children, before)

    def test_cohort_with_only_late_completions_keeps_its_latency(self):
        rows = self.children()[0]["cutover_samples"] + [[0, 2.2, 2.3, 6.5, 2.1, 200, 0, 1, -1]]
        phase = self.summarize_rows(rows)["request_start_cohorts"]["during"]
        self.assertEqual((phase["requests"], phase["completed_in_window"], phase["late_completions"]), (1, 0, 1))
        self.assertAlmostEqual(phase["latency_ms"]["p99"], 4300)

    def test_invalid_global_duration_rejected(self):
        for duration in (True, 0, -1, math.inf, math.nan):
            with self.subTest(duration=duration), self.assertRaises(ValueError):
                self.summarize(self.children(), duration=duration, cutover_start_seconds=2, cutover_ack_seconds=3)


if __name__ == "__main__":
    unittest.main()
