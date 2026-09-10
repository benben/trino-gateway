import json
import unittest

from load_open_loop import OpenLoop, invalid


MESSAGES = {
    b"Transaction-aware request capacity is unavailable; no backend request was dispatched": "request_capacity",
    b"Transaction routing state is unavailable; the request was not reassigned": "routing_state_unavailable",
    b"Transaction routing state rejected the operation: NOT_ACTIVE": "backend_not_active",
    b"No backend belongs to the selected routing group": "no_backend_for_group",
    b"Backend must expose a ready Trino coordinator process identity": "process_identity_not_ready",
    b"Backend process identity is unavailable": "process_identity_unavailable",
}


class FixedResponse:
    def __init__(self, status, body, cleanup=False):
        self.status, self.body, self.cleanup = status, body, cleanup
        self.calls = 0

    def request(self, index, method, path, body, headers):
        self.calls += 1
        if self.cleanup and method == "POST":
            return 200, b'{"nextUri":"http://fixture/v1/statement/executing/synthetic/1"}'
        return self.status, self.body

    def close(self):
        pass


class ErrorCategoryTests(unittest.TestCase):
    def run_load(self, transport):
        return OpenLoop(["http://fixture"], ["synthetic"], "synthetic-placeholder",
                        rate=10, duration=.1, transport=transport).run()

    def test_actual_plain_entity_messages_have_fixed_categories_without_retry(self):
        for body, category in MESSAGES.items():
            with self.subTest(category=category):
                transport = FixedResponse(503, body)
                result = self.run_load(transport)
                self.assertEqual(result.get("response_error_categories"),
                                 {"measured": {category: 1}, "cleanup": {}})
                self.assertEqual(result["errors"], {"http_503": 1})
                self.assertEqual(result["http_statuses"], {"503": 1})
                self.assertTrue(invalid(result))
                self.assertEqual(transport.calls, 1)
                self.assertNotIn(body.decode(), json.dumps(result))

    def test_wrapped_partial_dynamic_and_invalid_messages_remain_unknown(self):
        known = next(iter(MESSAGES))
        for body in [b"synthetic-private-canary", b"<html>" + known + b"</html>",
                     json.dumps({"message": known.decode()}).encode(), known + b"\n",
                     b"prefix " + known, b"\xff", b"", b"a" * 8192]:
            with self.subTest(length=len(body)):
                result = self.run_load(FixedResponse(503, body))
                self.assertEqual(result.get("response_error_categories"),
                                 {"measured": {"unknown_503": 1}, "cleanup": {}})
                self.assertNotIn("synthetic-private-canary", json.dumps(result))

    def test_status_must_be_503_and_existing_error_semantics_are_unchanged(self):
        result = self.run_load(FixedResponse(502, next(iter(MESSAGES))))
        self.assertEqual(result.get("response_error_categories"), {"measured": {}, "cleanup": {}})
        self.assertEqual(result["errors"], {"http_502": 1})
        self.assertTrue(invalid(result))

    def test_cleanup_categories_do_not_enter_measured_counter(self):
        result = self.run_load(FixedResponse(503, next(iter(MESSAGES)), cleanup=True))
        self.assertEqual(result.get("response_error_categories"),
                         {"measured": {}, "cleanup": {"request_capacity": 1}})
        self.assertEqual(result["errors"], {"http_503": 1})
        self.assertTrue(invalid(result))


if __name__ == "__main__":
    unittest.main()
