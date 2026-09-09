#!/usr/bin/env python3
"""Protocol-focused Trino test double. Never expose this server outside a test network."""

import argparse
import json
import socket
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit


class State:
    def __init__(self, identity):
        self.identity = identity
        self.lock = threading.RLock()
        self.release = threading.Event()
        self.poll_release = threading.Event()
        self.release.set()
        self.reset()

    def reset(self):
        with self.lock:
            self.node_id = str(uuid.uuid4())
            self.coordinator_id = uuid.uuid4().hex[:5]
            self.transactions = {}
            self.queries = {}
            self.requests = []
            self.config = {"start_header_page": 0, "clear_header_page": 0,
                           "hold_start": False, "hold_poll": False}
            self.release.set()
            self.poll_release.set()


def make_server(host="127.0.0.1", port=0, identity="blue"):
    state = State(identity)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def respond(self, status, value, headers=()):
            body = b"" if status == 204 or self.command == "HEAD" else value if isinstance(value, bytes) else json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            for name, value in headers:
                self.send_header(name, value)
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def body(self):
            return self.rfile.read(int(self.headers.get("Content-Length", "0")))

        def disconnect(self):
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()

        def base(self):
            return "http://" + self.headers["Host"]

        def record(self, sql=None):
            entry = {"method": self.command, "path": self.path, "sql": sql,
                     "transactions": self.headers.get_all("X-Trino-Transaction-Id", []),
                     "queryDataEncoding": self.headers.get_all("X-Trino-Query-Data-Encoding", []),
                     "user": self.headers.get("X-Trino-User")}
            with state.lock:
                state.requests.append(entry)

        def do_GET(self):
            path = urlsplit(self.path).path
            if path == "/__test/state":
                with state.lock:
                    self.respond(200, {"identity": state.identity,
                                       "transactions": dict(state.transactions),
                                       "nodeId": state.node_id, "coordinatorId": state.coordinator_id,
                                       "requests": list(state.requests),
                                       "config": dict(state.config)})
                return
            if path == "/v1/info":
                self.respond(200, {"nodeVersion": {"version": "test"},
                                   "environment": "test", "coordinator": True,
                                   "nodeId": state.node_id, "coordinatorId": state.coordinator_id,
                                   "starting": False, "uptime": "1.00m"})
                return
            if path == "/v1/info/state":
                self.respond(200, "ACTIVE")
                return
            if path == "/v1/cluster":
                self.respond(200, {"runningQueries": 0, "blockedQueries": 0,
                                   "queuedQueries": 0, "activeWorkers": 2,
                                   "runningDrivers": 0, "reservedMemory": 0,
                                   "totalInputRows": 0, "totalInputBytes": 0,
                                   "totalCpuTimeSecs": 0})
                return
            self.record()
            with state.lock:
                response = state.queries.get(path)
                config = dict(state.config)
            if response is None:
                self.respond(404, {"error": "Unknown query"})
                return
            if config.get("hold_poll") and not state.poll_release.wait(30):
                self.respond(503, {"error": "Test poll barrier timed out"})
                return
            if config.get("drop_poll_response"):
                self.disconnect()
                return
            self.respond(*response)

        def do_DELETE(self):
            self.body()
            self.record()
            with state.lock:
                known = urlsplit(self.path).path in state.queries
            self.respond(204 if known else 404, {})

        def do_HEAD(self):
            self.record()
            with state.lock:
                known = urlsplit(self.path).path in state.queries
            self.respond(200 if known else 404, {})

        def do_PUT(self):
            self.body()
            self.record()
            self.respond(405, {"error": "Unsupported method"})

        def do_POST(self):
            raw = self.body()
            path = urlsplit(self.path).path
            if path == "/__test/reset":
                state.reset()
                self.respond(200, {})
                return
            if path == "/__test/restart":
                state.reset()
                self.respond(200, {"nodeId": state.node_id, "coordinatorId": state.coordinator_id})
                return
            if path == "/__test/release":
                kind = json.loads(raw or b"{}").get("kind", "all")
                if kind in ("all", "start"):
                    state.release.set()
                if kind in ("all", "poll"):
                    state.poll_release.set()
                self.respond(200, {})
                return
            if path == "/__test/config":
                values = json.loads(raw or b"{}")
                with state.lock:
                    state.config.update(values)
                    if values.get("hold_start"):
                        state.release.clear()
                    if values.get("hold_poll"):
                        state.poll_release.clear()
                self.respond(200, {})
                return
            sql = raw.decode()
            self.record(sql)
            if path.rstrip("/") != "/v1/statement":
                self.respond(404, {})
                return
            command = " ".join(sql.strip().rstrip(";").upper().split())
            supplied = self.headers.get_all("X-Trino-Transaction-Id", [])
            transaction = supplied[0] if supplied else "NONE"
            with state.lock:
                config = dict(state.config)
                query_id = "20260101_000000_" + str(uuid.uuid4().int % 100000000) + "_" + state.coordinator_id
                result = {"id": query_id, "infoUri": self.base() + "/ui/query.html?" + query_id,
                          "stats": {"state": "FINISHED", "queued": False,
                                    "scheduled": True, "nodes": 1, "totalSplits": 1,
                                    "queuedSplits": 0, "runningSplits": 0,
                                    "completedSplits": 1, "cpuTimeMillis": 0,
                                    "wallTimeMillis": 1, "queuedTimeMillis": 0,
                                    "elapsedTimeMillis": 1, "processedRows": 1,
                                    "processedBytes": 1, "peakMemoryBytes": 0,
                                    "spilledBytes": 0}, "warnings": []}
                headers = []
                starts = command.startswith("START TRANSACTION")
                clears = command in ("COMMIT", "ROLLBACK")
                if starts:
                    transaction = config.get("force_transaction_id") or str(uuid.uuid4())
                    state.transactions[transaction] = {"user": self.headers.get("X-Trino-User")}
                    headers.append(("X-Trino-Started-Transaction-Id", transaction))
                    duplicate = config.get("duplicate_start_headers")
                    if duplicate:
                        headers.append(("X-Trino-Started-Transaction-Id",
                                        transaction if duplicate == "same" else str(uuid.uuid4())))
                    result["updateType"] = "START TRANSACTION"
                elif transaction != "NONE" and transaction not in state.transactions:
                    result["error"] = {"message": "Unknown transaction on " + state.identity,
                                       "errorCode": 65541, "errorName": "UNKNOWN_TRANSACTION",
                                       "errorType": "USER_ERROR"}
                elif config.get("query_error") or (command == "COMMIT" and config.get("fail_commit")):
                    result["error"] = {"message": "Synthetic query failure", "errorCode": 1,
                                       "errorName": "GENERIC_USER_ERROR", "errorType": "USER_ERROR"}
                elif clears:
                    state.transactions.pop(transaction, None)
                    headers.append(("X-Trino-Clear-Transaction-Id", "true"))
                    result["updateType"] = command
                else:
                    result["columns"] = [{"name": "backend", "type": "varchar",
                                          "typeSignature": {"rawType": "varchar", "arguments": []}}]
                    result["data"] = [[state.identity]]
                poll_path = "/v1/statement/executing/" + query_id + "/token/1"
                initial_headers = headers
                terminal_headers = []
                if (starts and config["start_header_page"] == 1) or (clears and config["clear_header_page"] == 1):
                    initial_headers, terminal_headers = [], headers
                if config.get("lowercase_headers"):
                    initial_headers = [(name.lower(), value) for name, value in initial_headers]
                    terminal_headers = [(name.lower(), value) for name, value in terminal_headers]
                terminal = result
                if config.get("malformed_terminal"):
                    terminal = b"not-a-protocol-response"
                elif config.get("terminal_trailing_bytes"):
                    terminal = json.dumps(result).encode() + b" trailing-content"
                elif config.get("terminal_padding_bytes"):
                    terminal["testPadding"] = "x" * min(int(config["terminal_padding_bytes"]), 4 * 1024 * 1024)
                state.queries[poll_path] = (200, terminal, terminal_headers)
                first = {"id": query_id, "infoUri": result["infoUri"],
                         "nextUri": self.base() + poll_path,
                         "stats": {**result["stats"], "state": "RUNNING"}, "warnings": []}
            if starts and config.get("hold_start"):
                if not state.release.wait(30):
                    self.respond(503, {"error": "Test barrier timed out"})
                    return
            if starts and config.get("drop_start_response"):
                self.disconnect()
                return
            self.respond(int(config.get("initial_status", 200)), first, initial_headers)

    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    server.state = state
    return server


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--identity", default="blue")
    args = parser.parse_args()
    with make_server(args.host, args.port, args.identity) as server:
        server.serve_forever()
