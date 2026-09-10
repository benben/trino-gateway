"""Bounded admission contracts. Run only on an otherwise idle disposable lab."""

import concurrent.futures
import os
import socket
import time

from protocol import request, through_gateway
from test_gateway import GatewayFixture


class OverloadContract(GatewayFixture):
    def test_per_process_cap_rejects_before_backend_admission(self):
        limit = int(os.environ.get("TX_EXPECT_MAX_IN_FLIGHT", "16"))
        self.assertTrue(1 <= limit <= 64, "Keep the regression fixture bounded")
        initial = [self.submit("SELECT 1") for _ in range(limit + 1)]
        for response in initial:
            self.assertEqual(response.status, 200, response.body)
        paths = [through_gateway(response.json()["nextUri"], self.gateways[0]) for response in initial]
        self.configure(0, hold_poll=True)
        before = len(self.state(0)["requests"])
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=limit)
        held = []
        rejected = None
        elapsed = None
        admitted = None
        try:
            held = [executor.submit(request, path) for path in paths[:limit]]
            deadline = time.monotonic() + 15
            while len(self.state(0)["requests"]) - before < limit:
                self.assertLess(time.monotonic(), deadline, "Could not establish the held-admission barrier")
                time.sleep(0.02)
            started = time.monotonic()
            try:
                rejected = request(paths[-1], timeout=3)
            except (TimeoutError, socket.timeout):
                pass
            elapsed = time.monotonic() - started
            admitted = len(self.state(0)["requests"]) - before
        finally:
            request(self.backends[0] + "/__test/release", "POST", "{}")
            executor.shutdown(wait=True)
            self.configure(0, hold_poll=False)
            for path in paths:
                response = request(path)
                self.assertEqual(response.status, 200, response.body)
        self.assertIsNotNone(rejected, "Overload request waited for backend instead of rejecting before dispatch")
        self.assertEqual(rejected.status, 503, rejected.body)
        self.assertTrue(rejected.values("Retry-After"), "Overload response needs explicit retry guidance")
        self.assertLess(elapsed, 2, "Predispatch overload response must not wait for the held backend")
        self.assertEqual(admitted, limit, "Rejected request must never reach the backend")
        for future in held:
            self.assertEqual(future.result().status, 200)


class DeadlineContract(GatewayFixture):
    def test_existing_routing_timeout_bounds_process_probe_before_dispatch(self):
        if os.environ.get("TX_EXPECT_ROUTING_TIMEOUT_MS") != "500":
            self.fail("Run this fixture with the existing routing.asyncTimeout set to 500ms")
        before = self.submissions()
        self.configure(0, hold_info=True)
        response = None
        started = time.monotonic()
        try:
            try:
                response = request(self.gateways[0] + "/v1/statement", "POST", "SELECT 1",
                                   [("Authorization", self.authorization), ("X-Trino-User", "user"),
                                    ("X-Trino-Transaction-Id", "NONE"), ("X-Trino-Routing-Group", self.group)], timeout=2)
            except (TimeoutError, socket.timeout):
                pass
            elapsed = time.monotonic() - started
            self.assertIsNotNone(response, "Process probe exceeded the existing routing timeout before response binding")
            self.assertIn(response.status, (502, 503, 504), response.body)
            self.assertLess(elapsed, 1.5)
            self.assertEqual(self.submissions(), before, "Expired predispatch request reached the statement backend")
        finally:
            self.configure(0, hold_info=False)
            request(self.backends[0] + "/__test/release", "POST", "{}")
        time.sleep(1)
        self.assertEqual(self.submissions(), before, "Detached process probe dispatched a statement after its deadline")
        self.assertEqual(self.select_backend(), self.state(0)["identity"], "Request lease leaked after deadline")


if __name__ == "__main__":
    import unittest
    unittest.main()
