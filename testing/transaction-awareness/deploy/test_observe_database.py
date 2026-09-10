import json
import os
import select
import subprocess
import unittest

from observe_database import render_job, summarize
from render import TASK


class ObserverTests(unittest.TestCase):
    def priority(self):
        return {"metadata": {"name": "fixture-observer", "labels": {"task": TASK}},
                "value": -10, "preemptionPolicy": "Never", "globalDefault": False}

    def render(self, **kwargs):
        return render_job("gateway-tx-lab-test", "metrics-test", "metrics-database", "database-ca",
                          priority_class=self.priority(), **kwargs)

    def sample(self, second, count=2):
        return {"sample_time": f"2026-01-01T00:00:{second:02d}.000000Z", "observer_pid": 123,
                "client_connections": count, "states": {"idle": count}, "wait_types": {"Client": count},
                "database": {"xact_commit": second + 10, "xact_rollback": 0, "blks_read": 0,
                             "blks_hit": second + 20, "temp_bytes": 0, "deadlocks": 0, "stats_reset": None}}

    def test_job_is_bounded_read_only_and_non_preempting(self):
        objects = self.render(samples=4)["items"]
        job = objects[1]["spec"]
        self.assertEqual(job["backoffLimit"], 0)
        self.assertEqual(job["activeDeadlineSeconds"], 64)
        pod = job["template"]["spec"]
        self.assertFalse(pod["automountServiceAccountToken"])
        self.assertEqual(pod["priorityClassName"], "fixture-observer")
        self.assertEqual(pod["preemptionPolicy"], "Never")
        container = pod["containers"][0]
        self.assertEqual(container["envFrom"], [{"secretRef": {"name": "metrics-database"}}])
        env = {x["name"]: x["value"] for x in container["env"]}
        self.assertIn("default_transaction_read_only=on", env["PGOPTIONS"])
        self.assertEqual(env["PGSSLMODE"], "verify-full")
        self.assertEqual(env["PGAPPNAME"], "gateway-metrics-observer")
        self.assertEqual(container["resources"]["requests"], container["resources"]["limits"])
        self.assertIn("\\watch i=1 c=4", objects[0]["data"]["observe.sql"])
        self.assertNotIn("PGPASSWORD", json.dumps(objects))

    def test_scope_and_duration_guards(self):
        for count in (0, 1, 901, 2.5, True):
            with self.subTest(count=count), self.assertRaises(ValueError):
                self.render(samples=count)
        for namespace, name, secret, ca in (("production", "metrics-test", "s", "c"),
                                             ("gateway-tx-lab-test", "other", "s", "c"),
                                             ("gateway-tx-lab-test", "metrics-test", "../s", "c")):
            with self.assertRaises(ValueError):
                render_job(namespace, name, secret, ca, priority_class=self.priority())
        with self.assertRaises(ValueError):
            render_job("gateway-tx-lab-test", "metrics-test", "s", "c", priority_class=None)

    def test_summary_includes_zero_and_counter_deltas(self):
        result = summarize([self.sample(0, 0), self.sample(1, 4), self.sample(2, 2)], expected_samples=3)
        self.assertEqual(result["client_connections"], {"min": 0, "max": 4, "avg": 2})
        self.assertEqual(result["database_delta"]["xact_commit"], 2)
        self.assertTrue(result["valid"])
        self.assertEqual(result["samples"], 3)

    def test_missing_samples_or_reconnect_are_not_silently_accepted(self):
        samples = [self.sample(0), self.sample(1)]
        self.assertFalse(summarize(samples, expected_samples=3)["valid"])
        samples[1]["observer_pid"] = 124
        self.assertFalse(summarize(samples)["valid"])

    def test_reset_regression_and_large_gaps_invalidate(self):
        for update in ({"stats_reset": "2026-01-01T00:00:01Z"}, {"xact_commit": 0}):
            samples = [self.sample(0), self.sample(1)]
            samples[1]["database"].update(update)
            self.assertFalse(summarize(samples)["valid"])
        self.assertFalse(summarize([self.sample(0), self.sample(5)])["valid"])

    def test_invalid_data_and_out_of_order_rejected(self):
        for rows in ([], [self.sample(1), self.sample(0)]):
            with self.assertRaises(ValueError):
                summarize(rows)
        sample = self.sample(0)
        sample["client_connections"] = -1
        with self.assertRaises(ValueError):
            summarize([sample])

    def test_window_selection_uses_only_in_window_observations(self):
        result = summarize([self.sample(0, 20), self.sample(1, 2), self.sample(2, 4), self.sample(3, 30)],
                           start="2026-01-01T00:00:01Z", end="2026-01-01T00:00:03Z")
        self.assertEqual(result["client_connections"], {"min": 2, "max": 4, "avg": 3})
        self.assertEqual(result["samples"], 2)

    @unittest.skipUnless(os.getenv("TX_OBSERVER_LOCAL_PG") == "yes", "Requires an explicitly owned local PostgreSQL fixture")
    def test_real_postgres_persistent_observer_excludes_itself(self):
        environment = dict(os.environ, PGAPPNAME="synthetic-observed-client")
        client = subprocess.Popen(["psql", "-XqAtw", "-c", "SELECT pg_backend_pid()", "-c", "SELECT pg_sleep(10)"],
                                  env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertTrue(select.select([client.stdout], [], [], 5)[0], "Client did not connect")
            self.assertTrue(client.stdout.readline().strip().isdigit())
            manifest = self.render(samples=3)
            container = manifest["items"][1]["spec"]["template"]["spec"]["containers"][0]
            observer = dict(os.environ, **{item["name"]: item["value"] for item in container["env"]})
            observer["PGSSLMODE"] = "disable"
            result = subprocess.run(["psql", "-XqAtw", "-f", "-"],
                                    input=manifest["items"][0]["data"]["observe.sql"], env=observer,
                                    text=True, capture_output=True, timeout=10, check=True)
            rows = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
            summary = summarize(rows, expected_samples=3)
            self.assertTrue(summary["valid"], summary)
            self.assertEqual(summary["client_connections"], {"min": 1, "max": 1, "avg": 1})
            self.assertEqual(len({row["observer_pid"] for row in rows}), 1)
        finally:
            client.terminate()
            client.communicate(timeout=5)


if __name__ == "__main__":
    unittest.main()
