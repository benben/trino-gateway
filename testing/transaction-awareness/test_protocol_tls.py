"""Real TLS checks for direct-pod checkpoint requests."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
from pathlib import Path
import ssl
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch

from protocol import request


class ProtocolTlsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runtime = tempfile.TemporaryDirectory(prefix="gateway-protocol-tls-")
        cls.certificate = Path(cls.runtime.name) / "certificate.pem"
        key = Path(cls.runtime.name) / "key.pem"
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                        "-subj", "/CN=fixture.test", "-addext", "subjectAltName=DNS:fixture.test",
                        "-keyout", str(key), "-out", str(cls.certificate)],
                       check=True, capture_output=True, timeout=15)
        cls.received = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                cls.received.append(self.headers.get_all("X-Test-Repeated"))
                self.send_response(200)
                self.send_header("X-Test-Response", "one")
                self.send_header("X-Test-Response", "two")
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"ok")

        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cls.certificate, key)
        cls.server.socket = context.wrap_socket(cls.server.socket, server_side=True)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.url = "https://127.0.0.1:" + str(cls.server.server_port)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()
        cls.runtime.cleanup()

    def test_explicit_verified_name_supports_direct_address_and_repeated_headers(self):
        with patch.dict(os.environ, TX_CA_FILE=str(self.certificate), TX_TLS_SERVER_NAME="fixture.test"):
            try:
                result = request(self.url, headers=[("X-Test-Repeated", "first"), ("X-Test-Repeated", "second")])
            except ssl.SSLCertVerificationError as failure:
                self.fail("The explicit verified TLS identity was ignored: " + failure.verify_message)
        self.assertEqual((result.status, result.body), (200, b"ok"))
        self.assertEqual(result.values("X-Test-Response"), ["one", "two"])
        self.assertEqual(self.received[-1], ["first", "second"])

    def test_wrong_name_is_rejected_without_dispatch(self):
        before = len(self.received)
        with patch.dict(os.environ, TX_CA_FILE=str(self.certificate), TX_TLS_SERVER_NAME="wrong.test"):
            with self.assertRaises(ssl.SSLCertVerificationError):
                request(self.url)
        self.assertEqual(len(self.received), before)

    def test_default_still_checks_the_url_hostname(self):
        with patch.dict(os.environ, TX_CA_FILE=str(self.certificate)):
            os.environ.pop("TX_TLS_SERVER_NAME", None)
            with self.assertRaises(ssl.SSLCertVerificationError):
                request(self.url)

    def test_explicit_name_does_not_bypass_certificate_trust(self):
        with patch.dict(os.environ, TX_TLS_SERVER_NAME="fixture.test"):
            os.environ.pop("TX_CA_FILE", None)
            with self.assertRaises(ssl.SSLCertVerificationError):
                request(self.url)


if __name__ == "__main__":
    unittest.main()
