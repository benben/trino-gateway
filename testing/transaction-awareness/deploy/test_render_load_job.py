import json
import unittest

from render_load_job import render_job
from render import TASK


class LoadJobTests(unittest.TestCase):
    def test_job_without_explicit_class_fails_closed(self):
        with self.assertRaises(ValueError):
            render_job("gateway-tx-lab-test", "load-case-one", ["https://one:8443", "https://two:8443"],
                       ["group"], "fixture source", "-----BEGIN CERTIFICATE-----\nplaceholder")

    def render(self, **kwargs):
        kwargs.setdefault("priority_class", {"metadata": {"name": "test-low", "labels": {"task": TASK}},
                                              "value": -10, "preemptionPolicy": "Never", "globalDefault": False})
        return render_job("gateway-tx-lab-test", "load-case-one", ["https://one:8443", "https://two:8443"],
                          ["group"], "fixture source", "-----BEGIN CERTIFICATE-----\nplaceholder", **kwargs)

    def test_no_keys_credentials_restart_retries_or_unbounded_jobs(self):
        objects = self.render()["items"]
        job = objects[1]["spec"]
        self.assertEqual(job["backoffLimit"], 0)
        self.assertEqual(job["activeDeadlineSeconds"], 250)
        pod = job["template"]["spec"]
        self.assertFalse(pod["automountServiceAccountToken"])
        self.assertEqual(pod["preemptionPolicy"], "Never")
        self.assertEqual(pod["priorityClassName"], "test-low")
        container = pod["containers"][0]
        self.assertEqual(container["resources"]["requests"], container["resources"]["limits"])
        self.assertEqual(container["resources"]["requests"], {"cpu": "2", "memory": "2Gi"})
        self.assertNotIn("secretKeyRef", json.dumps(objects))
        self.assertEqual({value["name"]: value["value"] for value in container["env"]}["TX_TLS_SERVER_NAME"], "gateway")

    def test_rejects_unbounded_matrix(self):
        for options in ({"rate": 1001}, {"duration": 301}, {"concurrency": 513}, {"warmup": 61}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.render(**options)

    def test_eight_process_bundle_is_explicit_and_complete(self):
        sources = {"load_multiprocess.py": "worker source", "load_aggregate.py": "aggregate source"}
        objects = self.render(processes=8, process_sources=sources)["items"]
        self.assertEqual(set(objects[0]["data"]), set(sources) | {"load_open_loop.py", "ca.pem"})
        container = objects[1]["spec"]["template"]["spec"]["containers"][0]
        self.assertIn("--processes", container["args"])
        self.assertIn("sys.path.insert", container["command"][-1])
        for options in ({"processes": 8}, {"processes": 2}, {"process_sources": sources},
                        {"processes": 8, "process_sources": sources, "concurrency": 127}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.render(**options)

    def test_rejects_missing_or_preempting_priority(self):
        for priority in (None, {"metadata": {"name": "default"}, "value": 0}):
            with self.subTest(priority=priority), self.assertRaises(ValueError):
                self.render(priority_class=priority)

    def test_explicit_measurement_window_adds_only_bounded_wait(self):
        start = "2030-01-01T00:05:00Z"
        job = self.render(measurement_start_utc=start)["items"][1]["spec"]
        self.assertEqual(job["activeDeadlineSeconds"], 850)
        self.assertEqual(job["template"]["spec"]["containers"][0]["args"][-2:], ["--measurement-start-utc", start])
        for invalid in ("tomorrow", "2030-01-01T00:05:00", "2030-13-01T00:05:00Z", 12):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                self.render(measurement_start_utc=invalid)


if __name__ == "__main__":
    unittest.main()
