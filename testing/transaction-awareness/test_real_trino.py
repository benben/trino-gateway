"""Real Trino protocol regressions against disposable coordinators and shared Gateways."""

import os
import time
import unittest

from protocol import finish, request, statement


class RealTrinoFixture(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.environ.get("TX_ALLOW_FIXTURE_MUTATION") != "yes":
            raise RuntimeError("Explicit disposable-fixture mutation permission is required")
        cls.gateways = os.environ["TX_GATEWAY_URLS"].split(",")
        if len(cls.gateways) < 2:
            raise RuntimeError("At least two independently addressed Gateway replicas are required")
        cls.names = os.environ.get("TX_REAL_BACKEND_NAMES", "real-blue,real-green").split(",")
        cls.group = os.environ.get("TX_REAL_ROUTING_GROUP", "real-transaction-test")
        cls.authorization = os.environ.get("TX_QUERY_AUTHORIZATION", "Basic dXNlcjp0ZXN0LXBhc3N3b3Jk")
        cls.admin_headers = []
        if os.environ.get("TX_ADMIN_TOKEN"):
            cls.admin_headers.append(("Authorization", "Bearer " + os.environ["TX_ADMIN_TOKEN"]))

    def admin(self, action, name, gateway):
        return request(self.gateways[gateway] + "/gateway/backend/" + action + "/" + name,
                       "POST", headers=self.admin_headers)

    def submit(self, sql, transaction="NONE", gateway=0):
        return statement(self.gateways[gateway], sql, transaction, "user", self.group,
                         [("Authorization", self.authorization)])

    def consume(self, response, gateway=0, allow_error=False):
        self.assertEqual(response.status, 200, response.body)
        pages = finish(response, self.gateways[gateway], limit=200)
        for page in pages:
            self.assertEqual(page.status, 200, page.body)
            if not allow_error:
                self.assertNotIn("error", page.json(), page.body)
        return pages

    def activate(self, index):
        for gateway in range(len(self.gateways)):
            for action, name in (("activate", self.names[index]), ("deactivate", self.names[1 - index])):
                response = self.admin(action, name, gateway)
                self.assertEqual(response.status, 200, response.body)
        deadline = time.monotonic() + 90
        for gateway in range(len(self.gateways)):
            while True:
                response = self.submit("SELECT 1", gateway=gateway)
                if response.status == 200:
                    self.consume(response, gateway)
                    break
                self.assertIn(response.status, (500, 502, 503, 504), response.body)
                self.assertLess(time.monotonic(), deadline, "Real backend did not become routable")
                time.sleep(0.2)

    def setUp(self):
        self.transactions = []
        self.activate(0)

    def tearDown(self):
        self.activate(0)
        for transaction in self.transactions:
            response = self.submit("ROLLBACK", transaction)
            if response.status == 200:
                self.consume(response, allow_error=True)

    def start(self):
        pages = self.consume(self.submit("START TRANSACTION READ ONLY"), gateway=1)
        ids = [value for page in pages for value in page.values("X-Trino-Started-Transaction-Id")]
        self.assertTrue(ids, "Real Trino did not return a transaction identifier")
        self.assertEqual(len(set(ids)), 1)
        self.transactions.append(ids[0])
        return ids[0]

    def nation_count(self, transaction="NONE", gateway=0):
        pages = self.consume(self.submit("SELECT count(*) FROM tpch.tiny.nation", transaction, gateway), gateway)
        rows = [row for page in pages for row in page.json().get("data", [])]
        self.assertEqual(rows, [[25]])


class RealBaselineControls(RealTrinoFixture):
    def test_real_transaction_across_gateway_replicas(self):
        transaction = self.start()
        for gateway in range(len(self.gateways)):
            self.nation_count(transaction, gateway)
        pages = self.consume(self.submit("COMMIT", transaction, gateway=1))
        self.assertTrue(any(page.values("X-Trino-Clear-Transaction-Id") for page in pages))

    def test_real_query_error_then_rollback(self):
        transaction = self.start()
        pages = self.consume(self.submit("SELECT * FROM tpch.tiny.table_that_does_not_exist", transaction), allow_error=True)
        self.assertTrue(any("error" in page.json() for page in pages))
        pages = self.consume(self.submit("ROLLBACK", transaction, gateway=1))
        self.assertTrue(any(page.values("X-Trino-Clear-Transaction-Id") for page in pages))


class RealTransactionContract(RealTrinoFixture):
    def test_real_transaction_survives_backend_cutover(self):
        transaction = self.start()
        self.nation_count(transaction, gateway=1)
        self.activate(1)
        for gateway in range(len(self.gateways)):
            self.nation_count(transaction, gateway)
            self.nation_count(gateway=gateway)
        pages = self.consume(self.submit("COMMIT", transaction, gateway=1))
        self.assertTrue(any(page.values("X-Trino-Clear-Transaction-Id") for page in pages))

    def test_idle_real_transaction_survives_cutover(self):
        transaction = self.start()
        self.activate(1)
        self.nation_count(gateway=1)
        self.nation_count(transaction, gateway=0)
        self.consume(self.submit("ROLLBACK", transaction, gateway=1))

    def test_failed_real_transaction_can_rollback_after_cutover(self):
        transaction = self.start()
        pages = self.consume(self.submit("SELECT * FROM tpch.tiny.table_that_does_not_exist", transaction), allow_error=True)
        self.assertTrue(any("error" in page.json() for page in pages))
        self.activate(1)
        pages = self.consume(self.submit("ROLLBACK", transaction, gateway=1))
        self.assertTrue(any(page.values("X-Trino-Clear-Transaction-Id") for page in pages))
        self.nation_count(gateway=1)


if __name__ == "__main__":
    unittest.main()
