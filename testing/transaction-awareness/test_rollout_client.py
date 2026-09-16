"""Protocol and reporting checks for a credential-safe rollout client."""

import unittest
import os
from pathlib import Path
import tempfile
import threading
from unittest.mock import patch

from protocol import Response
from rollout_client import RolloutClient


def response(body, headers=(), status=200):
    import json
    if isinstance(body, dict):
        body = {"stats": {"state": "FAILED" if body.get("error") is not None else "RUNNING" if body.get("nextUri") else "FINISHED"}, **body}
    return Response(status, list(headers), json.dumps(body).encode())


class RolloutClientTest(unittest.TestCase):
    def client(self):
        return RolloutClient("https://gateway.example", "reader", "catalog", "password", "group")

    @patch("rollout_client.request")
    def test_pages_keep_credentials_and_collect_actual_rows(self, request):
        request.side_effect = [response({"id": "q_owner", "nextUri": "https://gateway.example/page"}),
                               response({"id": "q_owner", "data": [[1]]})]
        result = self.client().query("SELECT 1")
        self.assertEqual(result["rows"], [[1]])
        self.assertEqual(result["pages"], 2)
        self.assertIn("Authorization", dict(request.call_args.kwargs["headers"]))

    @patch("rollout_client.request")
    def test_cross_origin_continuation_is_never_sent_credentials(self, request):
        request.return_value = response({"id": "q_owner", "nextUri": "https://other.example/page"})
        with self.assertRaisesRegex(RuntimeError, "continuation_origin"):
            self.client().query("SELECT 1")
        self.assertEqual(request.call_count, 1)

    @patch("rollout_client.request")
    def test_transaction_headers_are_preserved(self, request):
        request.side_effect = [response({"id": "q_owner"}, [("X-Trino-Started-Transaction-Id", "tx")]),
                               response({"id": "q_owner"}, [("X-Trino-Clear-Transaction-Id", "true")])]
        client = self.client()
        client.query("START TRANSACTION READ ONLY")
        client.query("COMMIT")
        self.assertEqual(dict(request.call_args.kwargs["headers"])["X-Trino-Transaction-Id"], "tx")
        self.assertEqual(client.transaction, "NONE")

    @patch("rollout_client.request")
    def test_http_200_query_error_is_failure_and_not_retried(self, request):
        request.return_value = response({"id": "q_owner", "error": {"errorName": "NO_TABLE", "message": "private"}})
        with self.assertRaisesRegex(RuntimeError, "query_error:NO_TABLE"):
            self.client().query("SELECT 1")
        self.assertEqual(request.call_count, 1)

    @patch("rollout_client.request")
    def test_continuation_query_identity_cannot_change(self, request):
        request.side_effect = [response({"id": "q_owner", "nextUri": "https://gateway.example/page"}),
                               response({"id": "other_owner", "data": [[1]]})]
        with self.assertRaisesRegex(RuntimeError, "query_identity_changed"):
            self.client().query("SELECT 1")

    @patch("rollout_client.request")
    def test_existing_transaction_identity_cannot_change(self, request):
        request.return_value = response({"id": "q_owner"}, [("X-Trino-Started-Transaction-Id", "new-tx")])
        client = self.client()
        client.transaction = "existing-tx"
        with self.assertRaisesRegex(RuntimeError, "transaction_identity_changed"):
            client.query("SELECT 1")
        self.assertEqual(client.transaction, "existing-tx")
        self.assertEqual(request.call_count, 1)

    @patch("rollout_client.time.sleep")
    @patch("rollout_client.request")
    def test_retained_callback_requires_a_continuation(self, request, sleep):
        called = []
        request.side_effect = [response({"id": "q_owner", "nextUri": "https://gateway.example/page"}),
                               response({"id": "q_owner", "data": [[1]]})]
        self.client().query("SELECT 1", first_page_pause=1, first_page_callback=lambda: called.append(True))
        self.assertEqual(called, [True])
        sleep.assert_called_once_with(1)

    @patch("rollout_client.request")
    def test_http_diagnostics_are_bounded_and_do_not_echo_body(self, request):
        request.return_value = Response(503, [("Retry-After", "1"), ("Content-Type", "application/json"),
                                             ("Server", "gateway")],
                                       b'Transaction routing state is unavailable; private-password https://private.example')
        with self.assertRaises(RuntimeError) as failure:
            self.client().query("SELECT private_catalog")
        message = str(failure.exception)
        self.assertIn('"classification": ["routing_state"]', message)
        self.assertIn('"method": "POST"', message)
        self.assertIn('"Retry-After": "1"', message)
        self.assertNotIn("private", message)
        self.assertEqual(request.call_count, 1)

    @patch("rollout_client.request")
    def test_http_diagnostics_redact_unbounded_headers(self, request):
        request.return_value = Response(503, [("Server", "x" * 101)], b"unclassified internal diagnostic")
        with self.assertRaisesRegex(RuntimeError, '"Server": "redacted"'):
            self.client().query("SELECT 1")

    @patch("rollout_client.request")
    def test_shutdown_error_code_is_captured_without_body(self, request):
        request.return_value = Response(503, [("X-Trino-Gateway-Error", "GATEWAY_STOPPING")], b"private details")
        with self.assertRaises(RuntimeError) as failure:
            self.client().query("SELECT 1")
        message = str(failure.exception)
        self.assertIn('"classification": ["shutdown"]', message)
        self.assertIn('"X-Trino-Gateway-Error": "GATEWAY_STOPPING"', message)
        self.assertNotIn("private", message)

    @patch("rollout_client.request")
    def test_unknown_gateway_error_code_is_not_echoed(self, request):
        request.return_value = Response(503, [("X-Trino-Gateway-Error", "private-backend")], b"private details")
        with self.assertRaises(RuntimeError) as failure:
            self.client().query("SELECT 1")
        self.assertNotIn("private", str(failure.exception))

    @patch("rollout_client.request")
    def test_failed_get_preserves_exact_handle_and_explicit_resume_never_posts(self, request):
        uri = "https://gateway.example/page?capability=private-token"
        request.side_effect = [response({"id": "q_owner", "data": [[1]], "nextUri": uri}),
                               Response(503, [], b"private upstream details"),
                               response({"id": "q_owner", "data": [[2]]})]
        client = self.client()
        with self.assertRaises(RuntimeError) as failure:
            client.query("SELECT private_sql")
        handle = failure.exception.recovery
        self.assertEqual(handle.next_uri, uri)
        self.assertNotIn("private-token", str(failure.exception))
        self.assertNotIn("private-token", repr(handle))
        self.assertEqual(request.call_count, 2)
        result = client.resume(handle)
        self.assertEqual(request.call_args.args[:3], (uri, "GET", None))
        self.assertEqual(result["rows"], [[2]])
        self.assertEqual(result["previous_rows"], 1)
        self.assertTrue(result["resumed"])

    @patch("rollout_client.request")
    def test_failed_post_has_no_invented_recovery_and_redacts_transport_message(self, request):
        request.side_effect = OSError("private-url private-password")
        with self.assertRaises(RuntimeError) as failure:
            self.client().query("SELECT private_sql")
        self.assertIsNone(failure.exception.recovery)
        self.assertNotIn("private", str(failure.exception))
        self.assertEqual(request.call_count, 1)

    @patch("rollout_client.request")
    def test_recovery_files_are_opt_in_private_and_omit_secrets_and_sql(self, request):
        from rollout_client import save_recovery, load_recovery
        request.side_effect = [response({"id": "q_owner", "nextUri": "https://gateway.example/page?token=cap"}),
                               Response(503, [], b"")]
        with self.assertRaises(RuntimeError) as failure:
            self.client().query("SELECT private_sql")
        with tempfile.TemporaryDirectory() as directory:
            path = save_recovery(failure.exception.recovery, directory)
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)
            content = Path(path).read_text()
            for sensitive in ("Authorization", "password", "private_sql"):
                self.assertNotIn(sensitive, content)
            self.assertEqual(load_recovery(path), failure.exception.recovery)
            os.chmod(path, 0o644)
            with self.assertRaises(ValueError):
                load_recovery(path)

    @patch("rollout_client.request")
    def test_resume_rejects_changed_owner_without_sending_request(self, request):
        request.side_effect = [response({"id": "q_owner", "nextUri": "https://gateway.example/page"}),
                               Response(503, [], b"")]
        with self.assertRaises(RuntimeError) as failure:
            self.client().query("SELECT 1")
        other = RolloutClient("https://gateway.example", "other", "catalog", "password", "group")
        with self.assertRaisesRegex(ValueError, "recovery_context"):
            other.resume(failure.exception.recovery)
        self.assertEqual(request.call_count, 2)

    @patch("rollout_client.request")
    def test_continuation_url_with_userinfo_is_rejected_before_credentials(self, request):
        request.return_value = response({"id": "q_owner", "nextUri": "https://injected:secret@gateway.example/page"})
        with self.assertRaisesRegex(RuntimeError, "continuation_origin"):
            self.client().query("SELECT 1")
        self.assertEqual(request.call_count, 1)

    @patch("rollout_client.request")
    def test_resume_cannot_reopen_a_transaction_after_cleanup(self, request):
        request.side_effect = [response({"id": "q_owner", "nextUri": "https://gateway.example/page"}),
                               Response(503, [], b"")]
        client = self.client()
        client.transaction = "original-tx"
        with self.assertRaises(RuntimeError) as failure:
            client.query("SELECT 1")
        client.transaction = "NONE"
        with self.assertRaisesRegex(ValueError, "recovery_context"):
            client.resume(failure.exception.recovery)
        self.assertEqual(request.call_count, 2)

    @patch("rollout_client.request")
    def test_repeated_failure_keeps_original_handle_without_advancing_page(self, request):
        uri = "https://gateway.example/page?original=capability"
        request.side_effect = [response({"id": "q_owner", "nextUri": uri}), Response(503, [], b""),
                               OSError("private transport details")]
        client = self.client()
        with self.assertRaises(RuntimeError) as first:
            client.query("SELECT 1")
        with self.assertRaises(RuntimeError) as second:
            client.resume(first.exception.recovery)
        self.assertEqual(first.exception.recovery, second.exception.recovery)
        self.assertNotIn("private", str(second.exception))
        self.assertEqual([call.args[1] for call in request.call_args_list], ["POST", "GET", "GET"])

    @patch("rollout_client.request")
    def test_retained_callback_failure_keeps_handle_and_redacts_callback_message(self, request):
        request.return_value = response({"id": "q_owner", "nextUri": "https://gateway.example/page"})
        def fail():
            raise RuntimeError("private callback credentials")
        with self.assertRaises(RuntimeError) as failure:
            self.client().query("SELECT 1", first_page_pause=1, first_page_callback=fail)
        self.assertIsNotNone(failure.exception.recovery)
        self.assertNotIn("private", str(failure.exception))
        self.assertEqual(request.call_count, 1)

    def test_recovery_reader_rejects_symlinks_and_writer_rejects_public_directory(self):
        from rollout_client import load_recovery, save_recovery
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "target"
            path.write_text("{}")
            os.chmod(path, 0o600)
            link = Path(directory) / "link"
            link.symlink_to(path)
            with self.assertRaises(OSError):
                load_recovery(link)
            os.chmod(directory, 0o755)
            with self.assertRaises(ValueError):
                save_recovery(None, directory)

    @patch("rollout_client.request")
    def test_recovery_report_does_not_persist_without_opt_in(self, request):
        from rollout_client import recovery_report
        request.side_effect = [response({"id": "q_owner", "nextUri": "https://gateway.example/private-token"}),
                               Response(503, [], b"")]
        with self.assertRaises(RuntimeError) as failure:
            self.client().query("SELECT 1")
        with patch("rollout_client.save_recovery") as save:
            report = recovery_report(failure.exception)
        self.assertTrue(report["recovery_available"])
        self.assertNotIn("private-token", str(report))
        save.assert_not_called()

    @patch("rollout_client.request")
    def test_resume_does_not_report_malformed_terminal_response_as_success(self, request):
        for body in ({"id": "q_owner", "stats": {"state": "RUNNING"}},
                     {"id": "q_owner", "stats": {"state": "FAILED"}},
                     {"id": "q_owner", "stats": None},
                     {"id": "q_owner", "data": "not rows"},
                     {"id": "q_owner", "data": [1]},
                     {"id": "q_owner", "error": {}},
                     {"id": "q_owner", "error": "not an error object"}):
            with self.subTest(body=body):
                request.side_effect = [response({"id": "q_owner", "nextUri": "https://gateway.example/page"}),
                                       Response(503, [], b""), response(body)]
                client = self.client()
                with self.assertRaises(RuntimeError) as original:
                    client.query("SELECT 1")
                with self.assertRaisesRegex(RuntimeError, "query_response_invalid") as resumed:
                    client.resume(original.exception.recovery)
                self.assertEqual(resumed.exception.recovery, original.exception.recovery)

    @patch("rollout_client.request")
    def test_terminal_null_optional_fields_are_valid(self, request):
        request.return_value = response({"id": "q_owner", "data": None, "error": None, "nextUri": None})
        self.assertEqual(self.client().query("SELECT 1")["rows"], [])

    @patch("rollout_client.time.sleep")
    @patch("rollout_client.request")
    def test_stop_releases_retained_pause_without_replaying_statement(self, request, sleep):
        release = threading.Event()
        request.side_effect = [response({"id": "q_owner", "nextUri": "https://gateway.example/page"}),
                               response({"id": "q_owner", "data": [[1]]})]
        result = self.client().query("SELECT 1", first_page_pause=500,
                                     first_page_callback=release.set, first_page_release=release)
        self.assertEqual(result["rows"], [[1]])
        self.assertEqual([call.args[1] for call in request.call_args_list], ["POST", "GET"])
        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
