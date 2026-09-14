"""Local real-Gateway routing contracts with two processes and disposable PostgreSQL.

Build Gateway classes and set GATEWAY_TEST_CLASSPATH to classes plus dependencies.
Set GATEWAY_TEST_JAVA and GATEWAY_TEST_PG_BIN to local Java and PostgreSQL binaries.
The fixture binds only loopback and never contacts a deployed environment.
"""

import base64
import contextlib
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import threading
import time
import unittest
from urllib.parse import urlsplit
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from fake_trino import make_server
from protocol import request, through_gateway


def unused_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


class PrincipalRoutingContract(unittest.TestCase):
    def test_real_gateway_routes_and_preserves_affinity_in_both_modes(self):
        for transactional in (False, True):
            with self.subTest(transactional=transactional), self.lab(transactional) as lab:
                gateways, backends, snapshot, passwords, token = lab
                authorization = "Basic " + base64.b64encode(("warehouse_a:" + passwords["warehouse_a"]).encode()).decode()
                headers = [("Authorization", authorization), ("X-Trino-User", "warehouse_a"), ("Content-Type", "text/plain")]

                def submit(gateway=0, extra=(), sql="SELECT 1"):
                    return request(gateways[gateway] + "/v1/statement", "POST", sql, headers + list(extra))

                def complete(response, gateway=1):
                    for _ in range(20):
                        self.assertEqual(response.status, 200, response.body)
                        if "nextUri" not in response.json():
                            return response
                        self.assertIn(urlsplit(response.json()["nextUri"]).netloc, [urlsplit(endpoint).netloc for endpoint in gateways])
                        response = request(through_gateway(response.json()["nextUri"], gateways[gateway]), headers=headers)
                    self.fail("Query exceeded its page bound")

                for gateway in range(2):
                    response = complete(submit(gateway))
                    self.assertEqual(response.json()["data"], [["cell-a"]])
                    response = complete(submit(gateway, [("X-Trino-Routing-Group", "cell-b")]))
                    self.assertEqual(response.json()["data"], [["cell-a"]])

                challenge = request(gateways[0] + "/v1/statement", "POST", "SELECT 1", [("X-Trino-User", "warehouse_a")])
                self.assertEqual(challenge.status, 401, challenge.body)
                self.assertTrue(challenge.values("WWW-Authenticate"))
                self.assertEqual(submit(extra=[("Authorization", authorization)]).status, 400)
                wrong_password = "Basic " + base64.b64encode(b"warehouse_a:incorrect").decode()
                denied = request(gateways[0] + "/v1/statement", "POST", "SELECT 1", [("Authorization", wrong_password), ("X-Trino-User", "warehouse_a")])
                self.assertEqual(denied.status, 401, denied.body)
                spoofed_user = request(gateways[0] + "/v1/statement", "POST", "SELECT 1", [("Authorization", authorization), ("X-Trino-User", "warehouse_b")])
                self.assertEqual(spoofed_user.status, 403, spoofed_user.body)

                pending = submit()
                self.assertEqual(pending.status, 200, pending.body)
                cancel_pending = submit()
                self.assertEqual(cancel_pending.status, 200, cancel_pending.body)
                transaction = None
                if transactional:
                    started = submit(sql="START TRANSACTION")
                    self.assertEqual(started.status, 200, started.body)
                    transaction = started.values("X-Trino-Started-Transaction-Id")[0]
                    complete(started)

                snapshot.routes = [{"principal": "warehouse_a", "routingGroup": "cell-b"}]
                self.wait_until(lambda: complete(submit()).json()["data"] == [["cell-b"]])
                self.assertEqual(complete(pending).json()["data"], [["cell-a"]])
                cancelled = request(through_gateway(cancel_pending.json()["nextUri"], gateways[1]), "DELETE", headers=headers)
                self.assertEqual(cancelled.status, 204, cancelled.body)
                if transactional:
                    response = complete(submit(1, [("X-Trino-Transaction-Id", transaction)]))
                    self.assertEqual(response.json()["data"], [["cell-a"]])

                snapshot.routes = []
                self.wait_until(lambda: submit().status == 403)
                self.wait_until(lambda: submit(1).status == 403)
                if transactional:
                    response = complete(submit(1, [("X-Trino-Transaction-Id", transaction)]))
                    self.assertEqual(response.json()["data"], [["cell-a"]])

                snapshot.available = False
                self.wait_until(lambda: submit().status == 503)
                self.wait_until(lambda: submit(1).status == 503)
                if transactional:
                    committed = submit(1, [("X-Trino-Transaction-Id", transaction)], "COMMIT")
                    self.assertTrue(committed.values("X-Trino-Clear-Transaction-Id"))
                    complete(committed)

                snapshot.routes = [{"principal": "warehouse_a", "routingGroup": "missing-cell"}]
                snapshot.available = True
                time.sleep(2)
                before = [len(backend.state.requests) for backend in backends]
                unavailable = submit()
                self.assertEqual(unavailable.status, 503, unavailable.body)
                self.assertEqual([len(backend.state.requests) for backend in backends], before)
                self.assertGreater(snapshot.calls, 0)
                self.assertFalse(snapshot.leaked_client_headers)

    @staticmethod
    def wait_until(condition, timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if condition():
                return
            time.sleep(0.1)
        raise AssertionError("Condition did not converge within the deadline")

    @contextlib.contextmanager
    def lab(self, transactional):
        classpath = os.environ["GATEWAY_TEST_CLASSPATH"]
        java = os.environ["GATEWAY_TEST_JAVA"]
        pg_bin = Path(os.environ["GATEWAY_TEST_PG_BIN"])
        with tempfile.TemporaryDirectory(prefix="gateway-principal-routing-") as directory, contextlib.ExitStack() as cleanup:
            root = Path(directory)
            pg_port = unused_port()
            subprocess.run([str(pg_bin / "initdb"), "-D", str(root / "pg"), "-U", "gateway_test", "-A", "trust", "--no-locale"], check=True, stdout=subprocess.DEVNULL)
            subprocess.run([str(pg_bin / "pg_ctl"), "-D", str(root / "pg"), "-l", str(root / "postgres.log"), "-o", f"-h 127.0.0.1 -p {pg_port} -k {directory}", "-w", "start"], check=True, stdout=subprocess.DEVNULL)
            cleanup.callback(subprocess.run, [str(pg_bin / "pg_ctl"), "-D", str(root / "pg"), "-m", "fast", "-w", "stop"], check=True, stdout=subprocess.DEVNULL)
            token = secrets.token_hex(32)
            passwords = {principal: secrets.token_hex(24) for principal in ("warehouse_a", "warehouse_b")}
            snapshot = ThreadingHTTPServer(("127.0.0.1", 0), self.snapshot_handler(token))
            snapshot.routes = [{"principal": "warehouse_a", "routingGroup": "cell-a"}, {"principal": "warehouse_b", "routingGroup": "cell-b"}]
            snapshot.available = True
            snapshot.calls = 0
            snapshot.leaked_client_headers = False
            self.start_server(snapshot, cleanup)
            backends = []
            for group in ("cell-a", "cell-b", "default-group"):
                backend = make_server(identity=group)
                backend.RequestHandlerClass = self.authenticated_backend(backend.RequestHandlerClass, passwords)
                self.start_server(backend, cleanup)
                backends.append(backend)
            gateways = []
            identity_key = secrets.token_hex(32)
            admin_token = secrets.token_hex(32)
            for index in range(2):
                port = unused_port()
                gateways.append(f"http://127.0.0.1:{port}")
                configuration = {
                    "serverConfig": {"node.environment": "test", "node.bind-ip": "127.0.0.1", "http-server.http.port": str(port)},
                    "dataStore": {"jdbcUrl": f"jdbc:postgresql://127.0.0.1:{pg_port}/postgres", "user": "gateway_test", "password": "unused", "driver": "org.postgresql.Driver", "connectionPoolEnabled": True},
                    "monitor": {"taskDelay": "1s"},
                    "routing": {"defaultRoutingGroup": "default-group", "principalRouting": {"enabled": True, "url": f"http://127.0.0.1:{snapshot.server_port}/snapshot", "token": token, "refreshIntervalSeconds": 1, "maxStaleSeconds": 3, "requestTimeoutMillis": 500}},
                    "transactionAwareness": {"enabled": transactional, "identityKey": identity_key, "adminToken": admin_token},
                }
                config_file = root / f"gateway-{index}.json"
                config_file.write_text(json.dumps(configuration))
                logfile = cleanup.enter_context((root / f"gateway-{index}.log").open("wb"))
                process = subprocess.Popen([java, "-Xmx384m", "-cp", classpath, "io.trino.gateway.ha.HaGatewayLauncher", str(config_file)], stdout=logfile, stderr=subprocess.STDOUT)
                cleanup.callback(self.stop_process, process)

                def ready():
                    if process.poll() is not None:
                        raise RuntimeError(f"Gateway process exited; inspect the isolated runtime log before cleanup: {root}")
                    try:
                        return request(gateways[index] + "/trino-gateway/readyz", timeout=1).status == 200
                    except (OSError, TimeoutError):
                        return False

                self.wait_until(ready, timeout=60)
                if index == 0:
                    for backend in backends:
                        url = f"http://127.0.0.1:{backend.server_port}"
                        body = {"name": backend.state.identity, "proxyTo": url, "externalUrl": url, "active": True, "routingGroup": backend.state.identity}
                        registered = request(gateways[0] + "/entity?entityType=GATEWAY_BACKEND", "POST", json.dumps(body), [("Content-Type", "application/json")])
                        self.assertEqual(registered.status, 200, registered.body)
            time.sleep(2)
            yield gateways, backends, snapshot, passwords, token

    @staticmethod
    def start_server(server, cleanup):
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        cleanup.callback(server.server_close)
        cleanup.callback(server.shutdown)

    @staticmethod
    def stop_process(process):
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

    @staticmethod
    def snapshot_handler(token):
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_GET(self):
                self.server.calls += 1
                if self.headers.get("Authorization") or self.headers.get("X-Trino-User"):
                    self.server.leaked_client_headers = True
                status = 200 if self.server.available else 503
                if self.headers.get("X-Duckgres-Internal-Secret") != token:
                    status = 401
                body = json.dumps({"routes": self.server.routes}).encode() if status == 200 else b"{}"
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        return Handler

    @staticmethod
    def authenticated_backend(parent, passwords):
        class Handler(parent):
            def base(self):
                forwarded_host = self.headers.get("X-Forwarded-Host")
                if forwarded_host:
                    scheme = self.headers.get("X-Forwarded-Proto", "http")
                    port = self.headers.get("X-Forwarded-Port")
                    return scheme + "://" + forwarded_host + (":" + port if port else "")
                return super().base()

            def do_POST(self):
                if self.path != "/v1/statement":
                    return super().do_POST()
                try:
                    scheme, encoded = self.headers.get("Authorization", "").split(" ", 1)
                    principal, password = base64.b64decode(encoded, validate=True).decode().split(":", 1)
                    valid = scheme.lower() == "basic" and passwords.get(principal) == password
                except (ValueError, UnicodeError):
                    valid = False
                if not valid:
                    self.body()
                    return self.respond(401, {"error": "authentication failed"}, [("WWW-Authenticate", 'Basic realm="Trino"')])
                if self.headers.get("X-Trino-User", principal) != principal:
                    self.body()
                    return self.respond(403, {"error": "impersonation denied"})
                return super().do_POST()

        return Handler


if __name__ == "__main__":
    unittest.main()
