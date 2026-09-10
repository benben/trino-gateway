"""Offline checks for bounded, per-replica database recovery readiness."""

import json
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit

from protocol import Response
import test_faults


class RecoveryScenario:
    def __init__(self, states, request_seconds=0.1):
        self.states = {name: list(values) for name, values in states.items()}
        self.now = 0.0
        self.request_seconds = request_seconds
        self.calls = []

    def request(self, url, method="GET", body=None, headers=(), timeout=40):
        name = urlsplit(url).hostname
        self.calls.append((name, method, self.now, timeout))
        if self.request_seconds > timeout:
            self.now += timeout
            raise TimeoutError("Synthetic recovery timeout")
        self.now += self.request_seconds
        states = self.states[name]
        status = states.pop(0) if len(states) > 1 else states[0]
        return Response(status, [], json.dumps({"pendingRequests": 1, "replica": name}).encode())

    def sleep(self, seconds):
        self.now += seconds


class FaultRecoveryTests(unittest.TestCase):
    def run_recovery(self, scenario):
        fixture = test_faults.DatabaseFaultContract("test_database_outage_before_admission_forwards_nothing")
        fixture.gateways = ["https://" + name for name in scenario.states]
        fixture.names = ["blue", "green"]
        fixture.admin_headers = []
        with patch("test_faults.request", side_effect=scenario.request), \
                patch("test_gateway.request", side_effect=scenario.request), \
                patch("test_faults.time.monotonic", side_effect=lambda: scenario.now), \
                patch("test_faults.time.sleep", side_effect=scenario.sleep):
            return fixture.await_database()

    def test_first_replica_does_not_hide_second_replica_recovery(self):
        scenario = RecoveryScenario({"gateway-0.example.test": [200], "gateway-1.example.test": [503, 200]})
        result = self.run_recovery(scenario)
        self.assertEqual(result["pendingRequests"], 1)
        self.assertEqual([name for name, _, _, _ in scenario.calls].count("gateway-1.example.test"), 2)
        self.assertTrue(all(method == "GET" for _, method, _, _ in scenario.calls))

    def test_unrecovered_replica_fails_with_one_shared_deadline(self):
        scenario = RecoveryScenario({"gateway-0.example.test": [200], "gateway-1.example.test": [503]}, request_seconds=0.5)
        with self.assertRaisesRegex(AssertionError, "recover"):
            self.run_recovery(scenario)
        self.assertLessEqual(scenario.now, 30)
        self.assertTrue(all(start + timeout <= 30 for _, _, start, timeout in scenario.calls))

    def test_six_replicas_do_not_multiply_the_recovery_budget(self):
        scenario = RecoveryScenario({f"gateway-{index}.example.test": [200] for index in range(6)}, request_seconds=6)
        with self.assertRaisesRegex(AssertionError, "recover"):
            self.run_recovery(scenario)
        self.assertLessEqual(scenario.now, 30)
        self.assertTrue(all(start + timeout <= 30 for _, _, start, timeout in scenario.calls))

    def test_success_requires_every_configured_replica(self):
        scenario = RecoveryScenario({f"gateway-{index}.example.test": [200] for index in range(6)})
        self.run_recovery(scenario)
        self.assertEqual({name for name, _, _, _ in scenario.calls}, set(scenario.states))

    def test_response_after_global_deadline_is_not_recovery(self):
        scenario = RecoveryScenario({"gateway-0.example.test": [200], "gateway-1.example.test": [200]})

        def delayed_response(*args, **kwargs):
            scenario.now += 31
            return Response(200, [], b'{"pendingRequests": 1}')

        scenario.request = delayed_response
        with self.assertRaisesRegex(AssertionError, "recover"):
            self.run_recovery(scenario)

    def test_malformed_success_is_not_treated_as_recovery(self):
        scenario = RecoveryScenario({"gateway-0.example.test": [200], "gateway-1.example.test": [200]})
        scenario.request = lambda *args, **kwargs: Response(200, [], b'not-json')
        with self.assertRaises(json.JSONDecodeError):
            self.run_recovery(scenario)


if __name__ == "__main__":
    unittest.main()
