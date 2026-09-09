"""Repeated blue/green reuse after a generation-fenced, successful drain."""

import time
import unittest

from protocol import request, through_gateway
from test_gateway import GatewayFixture


class ReincarnationContract(GatewayFixture):
    def test_three_cycles_preserve_old_query_fences_and_reject_stale_replacement(self):
        route = "/gateway/transactions/cutover"
        current = 0
        try:
            for cycle in range(3):
                with self.subTest(cycle=cycle):
                    target = 1 - current
                    initial = self.submit("SELECT 1", gateway=cycle % len(self.gateways))
                    self.complete(initial)
                    old_uri = initial.json()["nextUri"]
                    switched = self.admin(route, "POST", {"routingGroup": self.group, "backendName": self.names[target]})
                    self.assertEqual(switched.status, 200, switched.body)
                    self.assertEqual(self.select_backend(gateway=1), self.state(target)["identity"])
                    path = "/gateway/transactions/backends/" + self.names[current]
                    draining = self.admin(path + "/drain", "POST")
                    self.assertEqual(draining.status, 200, draining.body)
                    deadline = time.monotonic() + 150
                    while True:
                        status = self.backend_status(current)
                        self.assertEqual(status.status, 200, status.body)
                        if status.json()["readyToSeal"]:
                            break
                        self.assertLess(time.monotonic(), deadline, "Old color did not become sealable")
                        time.sleep(0.1)
                    old = status.json()
                    sealed = self.admin(path + "/seal", "POST", {"generation": old["generation"]})
                    self.assertEqual(sealed.status, 200, sealed.body)
                    self.assertTrue(sealed.json()["drained"])
                    sealed_generation = sealed.json()["generation"]
                    self.assertGreater(sealed_generation, old["generation"])
                    stale_resume = self.admin(path + "/resume", "POST", {"generation": old["generation"]}, gateway=1)
                    self.assertEqual(stale_resume.status, 409, stale_resume.body)
                    self.assertTrue(self.backend_status(current).json()["sealed"])
                    restarted = request(self.backends[current] + "/__test/restart", "POST", "{}")
                    self.assertEqual(restarted.status, 200, restarted.body)
                    expected = {"generation": sealed_generation, "incarnation": old["incarnation"]}
                    replacement = self.admin(path + "/reincarnate", "POST", expected, gateway=1)
                    self.assertEqual(replacement.status, 200, replacement.body)
                    new = replacement.json()
                    self.assertNotEqual(new["incarnation"], old["incarnation"])
                    self.assertGreater(new["generation"], sealed_generation)
                    self.assertEqual(new["state"], "DRAINING")
                    self.assertFalse(new["acceptingNewQueries"])
                    stale = self.admin(path + "/reincarnate", "POST", expected)
                    self.assertEqual(stale.status, 409, stale.body)
                    before = [len(self.state(index)["requests"]) for index in (0, 1)]
                    old_poll = request(through_gateway(old_uri, self.gateways[1]))
                    self.assertGreaterEqual(old_poll.status, 400, old_poll.body)
                    self.assertEqual([len(self.state(index)["requests"]) for index in (0, 1)], before)
                    resumed = self.admin(path + "/resume", "POST", {"generation": new["generation"]})
                    self.assertEqual(resumed.status, 200, resumed.body)
                    self.assertTrue(resumed.json()["acceptingNewQueries"])
                    current = target
        finally:
            self.admin(route + "/" + self.group, "DELETE")


if __name__ == "__main__":
    unittest.main()
