"""Only advertised result capabilities can create continuation obligations."""

import unittest
import uuid
from urllib.parse import urlsplit, urlunsplit

from protocol import request, through_gateway
from test_gateway import GatewayFixture


class CapabilityContract(GatewayFixture):
    def reject_forged_capability(self, method):
        initial = self.submit("SELECT 1")
        self.assertEqual(initial.status, 200, initial.body)
        uri = urlsplit(through_gateway(initial.json()["nextUri"], self.gateways[1]))
        parts = uri.path.split("/")
        parts[-2] = uuid.uuid4().hex
        forged = urlunsplit((uri.scheme, uri.netloc, "/".join(parts), uri.query, ""))
        before = [len(self.state(index)["requests"]) for index in (0, 1)]
        pending = self.backend_status().json()["pendingRequests"]
        try:
            response = request(forged, method)
            self.assertGreaterEqual(response.status, 400, response.body)
            self.assertLess(response.status, 500, response.body)
            self.assertEqual([len(self.state(index)["requests"]) for index in (0, 1)], before,
                             "A guessed query ID must not authorize a forged result capability")
            self.assertEqual(self.backend_status().json()["pendingRequests"], pending)
        finally:
            self.complete(initial)

    def test_forged_get_capability_never_reaches_backend(self):
        self.reject_forged_capability("GET")

    def test_forged_head_capability_never_reaches_backend(self):
        self.reject_forged_capability("HEAD")

    def test_forged_delete_capability_never_reaches_backend(self):
        self.reject_forged_capability("DELETE")

    def test_advertised_capability_replays_across_replicas_without_credentials(self):
        initial = self.submit("SELECT 1")
        self.assertEqual(initial.status, 200, initial.body)
        uri = initial.json()["nextUri"]
        self.complete(initial, gateway=1)
        for gateway in self.gateways:
            replay = request(through_gateway(uri, gateway))
            self.assertEqual(replay.status, 200, replay.body)
            self.assertEqual(replay.json()["id"], initial.json()["id"])
            self.assertNotIn("error", replay.json())


if __name__ == "__main__":
    unittest.main()
