"""Black-box transaction ownership contract for an isolated Gateway fixture."""

import concurrent.futures
import json
import os
import time
import unittest
import uuid

from protocol import finish, request, statement, through_gateway


def required_list(name, exactly_two=True):
    values = [value.rstrip("/") for value in os.environ.get(name, "").split(",") if value]
    if len(values) < 2 or (exactly_two and len(values) != 2):
        raise RuntimeError(name + " must identify " + ("exactly" if exactly_two else "at least") + " two isolated test endpoints")
    return values


class GatewayFixture(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.environ.get("TX_ALLOW_FIXTURE_MUTATION") != "yes":
            raise RuntimeError("Set TX_ALLOW_FIXTURE_MUTATION=yes only for an isolated disposable fixture")
        cls.gateways = required_list("TX_GATEWAY_URLS", exactly_two=False)
        cls.backends = required_list("TX_BACKEND_URLS")
        cls.proxy_urls = os.environ.get("TX_BACKEND_PROXY_URLS", ",".join(cls.backends)).split(",")
        cls.names = os.environ.get("TX_BACKEND_NAMES", "blue,green").split(",")
        if len(cls.names) != 2 or len(cls.proxy_urls) != 2:
            raise RuntimeError("Exactly two backend names and registered proxy URLs are required")
        cls.group = os.environ.get("TX_ROUTING_GROUP", "transaction-test")
        cls.authorization = os.environ.get("TX_QUERY_AUTHORIZATION", "Basic dXNlcjp0ZXN0LXBhc3N3b3Jk")
        cls.admin_headers = []
        if os.environ.get("TX_ADMIN_TOKEN"):
            cls.admin_headers = [("Authorization", "Bearer " + os.environ["TX_ADMIN_TOKEN"])]

    def admin(self, path, method="GET", body=None, gateway=0):
        headers = self.admin_headers + [("Content-Type", "application/json")]
        return request(self.gateways[gateway] + path, method,
                       json.dumps(body) if body is not None else None, headers)

    def backend_status(self, index=0, gateway=0):
        return self.admin("/gateway/transactions/backends/" + self.names[index] + "/drain", gateway=gateway)

    def resume(self, index=0):
        status = self.backend_status(index)
        if status.status == 404:
            return
        self.assertEqual(status.status, 200, status.body)
        response = self.admin("/gateway/transactions/backends/" + self.names[index] + "/resume",
                              "POST", {"generation": status.json()["generation"]})
        self.assertEqual(response.status, 200, response.body)

    def configure(self, index, **values):
        response = request(self.backends[index] + "/__test/config", "POST", json.dumps(values))
        self.assertEqual(response.status, 200)

    def state(self, index):
        return request(self.backends[index] + "/__test/state").json()

    def submissions(self):
        return sum(sum(item["method"] == "POST" and item["path"] == "/v1/statement"
                       for item in self.state(index)["requests"]) for index in (0, 1))

    def submit(self, sql, transaction="NONE", gateway=0, extra=(), user="user"):
        return statement(self.gateways[gateway], sql, transaction, user, self.group,
                         [("Authorization", self.authorization)] + list(extra))

    def complete(self, response, gateway=0):
        self.assertEqual(response.status, 200, response.body)
        pages = finish(response, self.gateways[gateway])
        for page in pages:
            self.assertEqual(page.status, 200, page.body)
            self.assertNotIn("error", page.json(), page.body)
        return pages

    def select_backend(self, transaction="NONE", gateway=0, **kwargs):
        result = self.complete(self.submit("SELECT 1", transaction, gateway, **kwargs), gateway)[-1]
        return result.json()["data"][0][0]

    def activate(self, index):
        target = self.names[index]
        old = self.names[1 - index]
        for gateway in range(len(self.gateways)):
            current = self.admin("/gateway/backend/all", gateway=gateway)
            self.assertEqual(current.status, 200, current.body)
            states = {backend["name"]: backend["active"] for backend in current.json()}
            for action, name in (("activate", target), ("deactivate", old)):
                if states.get(name) == (action == "activate"):
                    continue
                response = self.admin("/gateway/backend/" + action + "/" + name, "POST", gateway=gateway)
                self.assertEqual(response.status, 200, response.body)
        expected = self.state(index)["identity"]
        deadline = time.monotonic() + float(os.environ.get("TX_READINESS_TIMEOUT_SECONDS", "120"))
        while True:
            try:
                results = [self.select_backend(gateway=gateway) for gateway in range(len(self.gateways))]
            except AssertionError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.1)
                continue
            if results == [expected] * len(self.gateways):
                return
            self.assertLess(time.monotonic(), deadline, "Backend activation did not converge")
            time.sleep(0.1)

    def setUp(self):
        self.transactions = []
        self.resume(0)
        self.resume(1)
        self.activate(0)
        for index in (0, 1):
            self.configure(index, start_header_page=0, clear_header_page=0,
                           hold_start=False, hold_poll=False, duplicate_start_headers=None,
                           force_transaction_id=None, query_error=False, fail_commit=False,
                           lowercase_headers=False, malformed_terminal=False,
                           terminal_padding_bytes=0, drop_start_response=False, drop_poll_response=False)

    def tearDown(self):
        for index in (0, 1):
            request(self.backends[index] + "/__test/release", "POST", "{}")
            self.configure(index, query_error=False, fail_commit=False,
                           drop_start_response=False, drop_poll_response=False,
                           malformed_terminal=False, terminal_padding_bytes=0)
        for transaction in self.transactions:
            try:
                finish(self.submit("ROLLBACK", transaction), self.gateways[0])
            except (OSError, ValueError, AssertionError):
                pass
        self.resume(0)
        self.resume(1)
        if self.backend_status().status == 404:
            for index, backend in enumerate(self.backends):
                outstanding = self.state(index)["transactions"]
                for transaction in self.transactions:
                    if transaction in outstanding:
                        finish(statement(backend, "ROLLBACK", transaction), backend)

    def start(self, gateway=0):
        pages = self.complete(self.submit("START TRANSACTION", gateway=gateway), gateway)
        transaction_ids = [value for page in pages for value in page.values("X-Trino-Started-Transaction-Id")]
        self.assertTrue(transaction_ids, "START response omitted the transaction ID")
        self.assertEqual(len(set(transaction_ids)), 1)
        self.transactions.append(transaction_ids[0])
        return transaction_ids[0]

    def rejected_without_forward(self, transaction, extra=(), user="user"):
        before = self.submissions()
        response = self.submit("SELECT 1", transaction, gateway=1, extra=extra, user=user)
        try:
            self.assertGreaterEqual(response.status, 400, response.body)
            self.assertLess(response.status, 500, response.body)
            self.assertEqual(self.submissions(), before, "Gateway forwarded a rejected transaction")
        finally:
            if response.status == 200:
                finish(response, self.gateways[1])


class BaselineControls(GatewayFixture):
    def test_query_and_continuation_across_replicas(self):
        first = self.submit("SELECT 1", gateway=0)
        pages = self.complete(first, gateway=1)
        self.assertEqual(pages[-1].json()["data"], [[self.state(0)["identity"]]])

    def test_transaction_without_cutover(self):
        transaction = self.start()
        self.assertEqual(self.select_backend(transaction, gateway=1), self.state(0)["identity"])
        pages = self.complete(self.submit("ROLLBACK", transaction, gateway=1), gateway=0)
        self.assertTrue(any(page.values("X-Trino-Clear-Transaction-Id") for page in pages))

    def test_poll_replay_returns_same_result(self):
        first = self.submit("SELECT 1")
        self.assertEqual(first.status, 200, first.body)
        uri = first.json()["nextUri"]
        left = request(through_gateway(uri, self.gateways[0]))
        right = request(through_gateway(uri, self.gateways[1]))
        self.assertEqual(left.status, 200, left.body)
        self.assertEqual(right.status, 200, right.body)
        for key in ("id", "data", "stats", "warnings"):
            self.assertEqual(left.json()[key], right.json()[key])


class TransactionContract(GatewayFixture):
    def test_transaction_stays_on_owner_after_cutover(self):
        transaction = self.start(gateway=0)
        self.activate(1)
        self.assertEqual(self.select_backend(transaction, gateway=1), self.state(0)["identity"])
        self.assertEqual(self.select_backend(gateway=0), self.state(1)["identity"])

    def test_started_header_from_poll_is_persisted_across_replicas(self):
        self.configure(0, start_header_page=1)
        transaction = self.start(gateway=0)
        self.activate(1)
        self.assertEqual(self.select_backend(transaction, gateway=1), self.state(0)["identity"])

    def test_unknown_id_is_rejected_before_backend(self):
        self.rejected_without_forward(str(uuid.uuid4()))

    def test_malformed_id_is_rejected_before_backend(self):
        self.rejected_without_forward("not-a-transaction")

    def test_duplicate_transaction_headers_are_rejected(self):
        transaction = self.start()
        self.rejected_without_forward(transaction, [("X-Trino-Transaction-Id", str(uuid.uuid4()))])

    def test_identical_duplicate_transaction_headers_are_rejected(self):
        transaction = self.start()
        self.rejected_without_forward(transaction, [("X-Trino-Transaction-Id", transaction)])

    def test_other_user_cannot_replay_transaction(self):
        self.rejected_without_forward(self.start(), user="another-user")

    def test_other_credentials_cannot_replay_transaction(self):
        transaction = self.start()
        before = self.submissions()
        response = statement(self.gateways[1], "SELECT 1", transaction, "user", self.group,
                             [("Authorization", "Basic dXNlcjphbm90aGVyLXBhc3N3b3Jk")])
        self.assertGreaterEqual(response.status, 400, response.body)
        self.assertLess(response.status, 500, response.body)
        self.assertEqual(self.submissions(), before)

    def test_commit_after_cutover_clears_owner(self):
        self.check_completion("COMMIT")

    def test_rollback_after_cutover_clears_owner(self):
        self.check_completion("ROLLBACK")

    def check_completion(self, command):
        transaction = self.start()
        self.activate(1)
        pages = self.complete(self.submit(command, transaction, gateway=1), gateway=0)
        self.assertTrue(any(page.values("X-Trino-Clear-Transaction-Id") for page in pages))
        self.rejected_without_forward(transaction)

    def test_pending_start_blocks_drain(self):
        self.configure(0, hold_start=True)
        before_requests = len(self.state(0)["requests"])
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.submit, "START TRANSACTION")
            try:
                deadline = time.monotonic() + 10
                while not any(item["sql"] == "START TRANSACTION" for item in self.state(0)["requests"][before_requests:]):
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.01)
                path = "/gateway/transactions/backends/" + self.names[0] + "/drain"
                response = self.admin(path, "POST", gateway=1)
                self.assertEqual(response.status, 200, response.body)
                state = self.admin(path, gateway=0)
                self.assertEqual(state.status, 200, state.body)
                self.assertGreaterEqual(state.json()["pendingRequests"], 1)
                self.assertFalse(state.json()["drained"])
            finally:
                request(self.backends[0] + "/__test/release", "POST", "{}")
                response = future.result(timeout=10)
                self.transactions.extend(response.values("X-Trino-Started-Transaction-Id"))
                finish(response, self.gateways[0])
                self.resume()

    def test_idle_transaction_blocks_drain_on_other_replica(self):
        self.start(gateway=0)
        path = "/gateway/transactions/backends/" + self.names[0] + "/drain"
        try:
            response = self.admin(path, "POST", gateway=1)
            self.assertEqual(response.status, 200, response.body)
            state = self.admin(path, gateway=0)
            self.assertEqual(state.status, 200, state.body)
            self.assertFalse(state.json()["acceptingNewQueries"])
            self.assertGreaterEqual(state.json()["openTransactions"], 1)
            self.assertFalse(state.json()["drained"])
        finally:
            self.resume()

    def test_drain_prevents_new_admission_but_keeps_existing_transaction(self):
        transaction = self.start()
        path = "/gateway/transactions/backends/" + self.names[0] + "/drain"
        try:
            response = self.admin(path, "POST", gateway=1)
            self.assertEqual(response.status, 200, response.body)
            before = self.submissions()
            rejected = self.submit("START TRANSACTION", gateway=0)
            self.assertGreaterEqual(rejected.status, 400, rejected.body)
            self.assertEqual(self.submissions(), before)
            self.assertEqual(self.select_backend(transaction, gateway=1), self.state(0)["identity"])
        finally:
            self.resume()

    def test_backend_identity_cannot_be_rebound_with_open_transaction(self):
        transaction = self.start()
        replacement = {"name": self.names[0], "proxyTo": self.proxy_urls[1],
                       "externalUrl": self.proxy_urls[1], "active": True, "routingGroup": self.group}
        try:
            response = self.admin("/gateway/backend/modify/update", "POST", replacement)
            self.assertEqual(response.status, 409, response.body)
            self.assertEqual(self.select_backend(transaction, gateway=1), self.state(0)["identity"])
        finally:
            replacement.update(proxyTo=self.proxy_urls[0], externalUrl=self.proxy_urls[0])
            restored = self.admin("/gateway/backend/modify/update", "POST", replacement)
            self.assertEqual(restored.status, 200, restored.body)

    def test_atomic_cutover_is_observed_across_replicas(self):
        transaction = self.start()
        path = "/gateway/transactions/cutover"
        try:
            response = self.admin(path, "POST", {"routingGroup": self.group, "backendName": self.names[1]})
            self.assertEqual(response.status, 200, response.body)
            self.assertEqual(self.select_backend(gateway=1), self.state(1)["identity"])
            self.assertEqual(self.select_backend(transaction, gateway=1), self.state(0)["identity"])
        finally:
            self.admin(path + "/" + self.group, "DELETE")

    def test_transaction_is_shared_by_every_configured_gateway(self):
        transaction = self.start()
        self.activate(1)
        for gateway in range(len(self.gateways)):
            with self.subTest(gateway=gateway):
                self.assertEqual(self.select_backend(transaction, gateway=gateway), self.state(0)["identity"])

    def test_open_transaction_cannot_be_sealed(self):
        self.start()
        path = "/gateway/transactions/backends/" + self.names[0]
        try:
            drained = self.admin(path + "/drain", "POST")
            self.assertEqual(drained.status, 200, drained.body)
            status = self.backend_status(gateway=1)
            self.assertEqual(status.status, 200, status.body)
            self.assertFalse(status.json()["readyToSeal"])
            self.assertFalse(status.json()["drained"])
            seal = self.admin(path + "/seal", "POST", {"generation": status.json()["generation"]}, gateway=1)
            self.assertEqual(seal.status, 409, seal.body)
        finally:
            self.resume()

    def test_admission_racing_drain_is_counted_or_rejected(self):
        path = "/gateway/transactions/backends/" + self.names[0] + "/drain"
        self.configure(0, hold_start=True)
        before = self.submissions()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            admission = pool.submit(self.submit, "START TRANSACTION")
            draining = pool.submit(self.admin, path, "POST", None, 1)
            try:
                response = draining.result(timeout=10)
                self.assertEqual(response.status, 200, response.body)
                deadline = time.monotonic() + 10
                while not admission.done() and self.submissions() == before:
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.01)
                state = self.backend_status(gateway=1)
                self.assertEqual(state.status, 200, state.body)
                if admission.done():
                    result = admission.result()
                    self.assertGreaterEqual(result.status, 400, result.body)
                    self.assertEqual(self.submissions(), before)
                else:
                    self.assertGreaterEqual(state.json()["pendingRequests"], 1)
                    self.assertFalse(state.json()["readyToSeal"])
            finally:
                request(self.backends[0] + "/__test/release", "POST", "{}")
                result = admission.result(timeout=10)
                if result.status == 200:
                    self.transactions.extend(result.values("X-Trino-Started-Transaction-Id"))
                    finish(result, self.gateways[0])
                self.resume()


if __name__ == "__main__":
    unittest.main()
