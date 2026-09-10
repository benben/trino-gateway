"""Self-tests for the fixture; these do not establish Gateway correctness."""

import threading
import unittest
from unittest.mock import patch
import uuid

from fake_trino import make_server
from protocol import finish, request, statement


class FakeTrinoTests(unittest.TestCase):
    def test_query_ids_do_not_collide_when_random_values_repeat(self):
        with patch("fake_trino.uuid.uuid4", return_value=uuid.UUID(int=1)):
            first = statement(self.url, "SELECT 1")
            second = statement(self.url, "SELECT 1")
        self.assertNotEqual(first.json()["id"], second.json()["id"])
        self.assertEqual(len(self.server.state.queries), 2)
        for response in (first, second):
            self.assertEqual(finish(response, self.url)[-1].json()["id"], response.json()["id"])

    def test_query_sequence_resets_only_with_new_incarnation(self):
        first = statement(self.url, "SELECT 1").json()["id"]
        request(self.url + "/__test/config", "POST", '{"lowercase_headers": true}')
        second = statement(self.url, "SELECT 1").json()["id"]
        self.assertEqual(int(second.split("_")[2]), int(first.split("_")[2]) + 1)
        different_prefix = "0" if first.split("_")[3][0] != "0" else "1"
        with patch("fake_trino.uuid.uuid4", return_value=uuid.UUID(hex=different_prefix + "0" * 31)):
            request(self.url + "/__test/restart", "POST", "{}")
        restarted = statement(self.url, "SELECT 1").json()["id"]
        self.assertEqual(int(restarted.split("_")[2]), 1)
        self.assertNotEqual(first.split("_")[3], restarted.split("_")[3])

    def test_duplicate_next_uri_contains_two_conflicting_raw_fields(self):
        request(self.url + "/__test/config", "POST", '{"duplicate_next_uri": true}')
        initial = statement(self.url, "SELECT 1")
        terminal = request(initial.json()["nextUri"])
        self.assertEqual(terminal.status, 200)
        self.assertEqual(terminal.body.count(b'"nextUri"'), 2)
        self.assertIn(b'"nextUri":"http://', terminal.body)
        self.assertTrue(terminal.body.endswith(b'"nextUri":null}'))

    def test_partial_cancel_preserves_transaction_and_result(self):
        transaction = statement(self.url, "START TRANSACTION").values("X-Trino-Started-Transaction-Id")[0]
        request(self.url + "/__test/config", "POST", '{"partial_cancel": true}')
        initial = statement(self.url, "SELECT 1", transaction)
        uri = initial.json()["partialCancelUri"]
        self.assertIn("/executing/partialCancel/" + initial.json()["id"] + "/1/token/1", uri)
        cancelled = request(uri, "DELETE")
        self.assertEqual(cancelled.status, 204)
        state = request(self.url + "/__test/state").json()
        self.assertIn(transaction, state["transactions"])
        self.assertEqual(state["cancelledStages"], [{"queryId": initial.json()["id"], "stage": 1}])
        self.assertEqual(finish(initial, self.url)[-1].json()["data"], [["blue"]])

    def test_partial_cancel_rejects_wrong_stage_slug_and_methods(self):
        request(self.url + "/__test/config", "POST", '{"partial_cancel": true}')
        uri = statement(self.url, "SELECT 1").json()["partialCancelUri"]
        self.assertEqual(request(uri.replace("/1/token/1", "/2/token/1"), "DELETE").status, 404)
        self.assertEqual(request(uri.replace("/token/", "/wrong/"), "DELETE").status, 404)
        self.assertEqual(request(uri, "GET").status, 405)
        self.assertEqual(request(uri, "HEAD").status, 405)
        self.assertEqual(request(self.url + "/__test/state").json()["cancelledStages"], [])

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

    def test_process_info_barrier_does_not_block_control_endpoint(self):
        request(self.url + "/__test/config", "POST", '{"hold_info": true}')
        responses = []
        task = threading.Thread(target=lambda: responses.append(request(self.url + "/v1/info")))
        task.start()
        self.assertTrue(request(self.url + "/__test/state").json()["config"]["hold_info"])
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

    def test_head_heartbeat_has_no_body_and_does_not_close_transaction(self):
        transaction = statement(self.url, "START TRANSACTION").values("X-Trino-Started-Transaction-Id")[0]
        initial = statement(self.url, "SELECT 1", transaction)
        response = request(initial.json()["nextUri"], "HEAD")
        self.assertEqual(response.status, 200)
        self.assertEqual(response.body, b"")
        self.assertIn(transaction, request(self.url + "/__test/state").json()["transactions"])
        self.assertEqual(finish(initial, self.url)[-1].json()["data"], [["blue"]])

    def test_trailing_slash_statement_matches_real_trino(self):
        response = request(self.url + "/v1/statement/", "POST", "START TRANSACTION")
        self.assertEqual(response.status, 200)
        self.assertEqual(len(response.values("X-Trino-Started-Transaction-Id")), 1)
        self.assertEqual(request(self.url + "/__test/state").json()["requests"][0]["path"], "/v1/statement/")

    def test_unexpected_method_is_recorded_before_rejection(self):
        response = request(self.url + "/v1/statement/unknown", "PUT")
        self.assertEqual(response.status, 405)
        self.assertEqual(request(self.url + "/__test/state").json()["requests"][0]["method"], "PUT")

    def test_unexpected_408_can_follow_backend_acceptance(self):
        request(self.url + "/__test/config", "POST", '{"initial_status": 408}')
        response = statement(self.url, "START TRANSACTION")
        self.assertEqual(response.status, 408)
        self.assertEqual(len(request(self.url + "/__test/state").json()["transactions"]), 1)

    def test_terminal_trailing_bytes_are_not_valid_json(self):
        import json
        request(self.url + "/__test/config", "POST", '{"terminal_trailing_bytes": true}')
        initial = statement(self.url, "SELECT 1")
        response = request(initial.json()["nextUri"])
        with self.assertRaises(json.JSONDecodeError):
            response.json()

    def test_records_query_data_encoding_without_recording_authorization(self):
        statement(self.url, "SELECT 1", extra=[("X-Trino-Query-Data-Encoding", "json+zstd")])
        recorded = request(self.url + "/__test/state").json()["requests"][0]
        self.assertEqual(recorded["queryDataEncoding"], ["json+zstd"])
        self.assertNotIn("authorization", {name.lower() for name in recorded})


if __name__ == "__main__":
    unittest.main()
