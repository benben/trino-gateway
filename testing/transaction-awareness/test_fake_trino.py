"""Self-tests for the fixture; these do not establish Gateway correctness."""

import threading
import unittest

from fake_trino import make_server
from protocol import finish, request, statement


class FakeTrinoTests(unittest.TestCase):
    def setUp(self):
        self.server = make_server()
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = "http://127.0.0.1:" + str(self.server.server_port)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_start_continue_commit(self):
        start = statement(self.url, "START TRANSACTION")
        transaction = start.values("X-Trino-Started-Transaction-Id")[0]
        self.assertEqual(finish(start, self.url)[-1].json()["updateType"], "START TRANSACTION")
        select = finish(statement(self.url, "SELECT 1", transaction), self.url)[-1]
        self.assertEqual(select.json()["data"], [["blue"]])
        commit = statement(self.url, "COMMIT", transaction)
        self.assertEqual(commit.values("X-Trino-Clear-Transaction-Id"), ["true"])
        self.assertEqual(finish(commit, self.url)[-1].json()["updateType"], "COMMIT")
        self.assertEqual(request(self.url + "/__test/state").json()["transactions"], {})

    def test_unknown_transaction_is_backend_json_error(self):
        response = finish(statement(self.url, "SELECT 1", "unknown"), self.url)[-1]
        self.assertEqual(response.status, 200)
        self.assertEqual(response.json()["error"]["errorName"], "UNKNOWN_TRANSACTION")

    def test_started_header_can_arrive_on_continuation(self):
        request(self.url + "/__test/config", "POST", '{"start_header_page": 1}')
        response = statement(self.url, "START TRANSACTION")
        self.assertEqual(response.values("X-Trino-Started-Transaction-Id"), [])
        responses = finish(response, self.url)
        self.assertEqual(len(responses[-1].values("X-Trino-Started-Transaction-Id")), 1)

    def test_continuation_replay_is_identical(self):
        initial = statement(self.url, "SELECT 1")
        uri = initial.json()["nextUri"]
        first, second = request(uri), request(uri)
        self.assertEqual(first.body, second.body)

    def test_repeated_request_headers_are_preserved(self):
        statement(self.url, "SELECT 1", "one", extra=[("X-Trino-Transaction-Id", "two")])
        recorded = request(self.url + "/__test/state").json()["requests"]
        self.assertEqual(recorded[0]["transactions"], ["one", "two"])

    def test_start_barrier_is_observable_and_releasable(self):
        request(self.url + "/__test/config", "POST", '{"hold_start": true}')
        responses = []
        task = threading.Thread(target=lambda: responses.append(statement(self.url, "START TRANSACTION")))
        task.start()
        import time
        deadline = time.monotonic() + 3
        while not request(self.url + "/__test/state").json()["transactions"]:
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.01)
        self.assertTrue(task.is_alive())
        request(self.url + "/__test/release", "POST", "{}")
        task.join(3)
        self.assertFalse(task.is_alive())
        self.assertEqual(responses[0].status, 200)

    def test_coordinator_identity_matches_query_and_changes_on_restart(self):
        first = request(self.url + "/v1/info").json()
        query = statement(self.url, "SELECT 1").json()["id"]
        self.assertTrue(query.endswith("_" + first["coordinatorId"]))
        start = statement(self.url, "START TRANSACTION")
        transaction = start.values("X-Trino-Started-Transaction-Id")[0]
        request(self.url + "/__test/restart", "POST", "{}")
        second = request(self.url + "/v1/info").json()
        self.assertNotEqual(first["nodeId"], second["nodeId"])
        self.assertNotEqual(first["coordinatorId"], second["coordinatorId"])
        result = finish(statement(self.url, "SELECT 1", transaction), self.url)[-1]
        self.assertIn("error", result.json())

    def test_duplicate_start_headers_remain_separate_fields(self):
        request(self.url + "/__test/config", "POST", '{"duplicate_start_headers": "same"}')
        response = statement(self.url, "START TRANSACTION")
        headers = response.values("X-Trino-Started-Transaction-Id")
        self.assertEqual(len(headers), 2)
        self.assertEqual(headers[0], headers[1])

    def test_conflicting_start_headers_are_distinct(self):
        request(self.url + "/__test/config", "POST", '{"duplicate_start_headers": "conflict"}')
        response = statement(self.url, "START TRANSACTION")
        self.assertEqual(len(set(response.values("X-Trino-Started-Transaction-Id"))), 2)

    def test_failed_commit_does_not_clear_transaction(self):
        start = statement(self.url, "START TRANSACTION")
        transaction = start.values("X-Trino-Started-Transaction-Id")[0]
        request(self.url + "/__test/config", "POST", '{"fail_commit": true}')
        responses = finish(statement(self.url, "COMMIT", transaction), self.url)
        self.assertIn("error", responses[-1].json())
        self.assertFalse(any(response.values("X-Trino-Clear-Transaction-Id") for response in responses))
        self.assertIn(transaction, request(self.url + "/__test/state").json()["transactions"])

    def test_clear_header_can_arrive_on_continuation(self):
        transaction = statement(self.url, "START TRANSACTION").values("X-Trino-Started-Transaction-Id")[0]
        request(self.url + "/__test/config", "POST", '{"clear_header_page": 1, "lowercase_headers": true}')
        initial = statement(self.url, "ROLLBACK", transaction)
        self.assertEqual(initial.values("X-Trino-Clear-Transaction-Id"), [])
        self.assertEqual(finish(initial, self.url)[-1].values("X-Trino-Clear-Transaction-Id"), ["true"])

    def test_lost_start_response_keeps_backend_transaction(self):
        import http.client
        request(self.url + "/__test/config", "POST", '{"drop_start_response": true}')
        with self.assertRaises(http.client.RemoteDisconnected):
            statement(self.url, "START TRANSACTION")
        self.assertEqual(len(request(self.url + "/__test/state").json()["transactions"]), 1)

    def test_malformed_terminal_body_is_not_json(self):
        import json
        request(self.url + "/__test/config", "POST", '{"malformed_terminal": true}')
        initial = statement(self.url, "SELECT 1")
        terminal = request(initial.json()["nextUri"])
        self.assertEqual(terminal.status, 200)
        with self.assertRaises(json.JSONDecodeError):
            terminal.json()

    def test_poll_barrier_remains_blocked_while_delete_completes(self):
        import time
        initial = statement(self.url, "SELECT 1")
        uri = initial.json()["nextUri"]
        request(self.url + "/__test/config", "POST", '{"hold_poll": true}')
        responses = []
        task = threading.Thread(target=lambda: responses.append(request(uri)))
        task.start()
        deadline = time.monotonic() + 3
        while not any(item["method"] == "GET" for item in request(self.url + "/__test/state").json()["requests"]):
            self.assertLess(time.monotonic(), deadline)
            time.sleep(0.01)
        self.assertTrue(task.is_alive())
        self.assertEqual(request(uri, "DELETE").status, 204)
        request(self.url + "/__test/release", "POST", '{"kind": "poll"}')
        task.join(3)
        self.assertFalse(task.is_alive())
        self.assertEqual(responses[0].status, 200)


if __name__ == "__main__":
    unittest.main()
