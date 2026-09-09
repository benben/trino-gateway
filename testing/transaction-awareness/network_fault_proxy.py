#!/usr/bin/env python3
"""Disposable TCP fault boundary. Restrict both ports to an isolated test network."""

import argparse
import json
import select
import socket
import socketserver
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class ProxyState:
    def __init__(self):
        self.lock = threading.RLock()
        self.available = True
        self.connections = set()
        self.accepted = 0
        self.rejected = 0

    def configure(self, available):
        with self.lock:
            self.available = available
            if not available:
                for connection in tuple(self.connections):
                    try:
                        connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    connection.close()
                self.connections.clear()

    def snapshot(self):
        with self.lock:
            return {"available": self.available, "openSockets": len(self.connections),
                    "accepted": self.accepted, "rejected": self.rejected}


def make_proxy(host, port, control_port, upstream_host, upstream_port):
    state = ProxyState()

    class TCPHandler(socketserver.BaseRequestHandler):
        def handle(self):
            upstream = None
            try:
                with state.lock:
                    if not state.available:
                        state.rejected += 1
                        return
                    state.connections.add(self.request)
                upstream = socket.create_connection((upstream_host, upstream_port), timeout=3)
                with state.lock:
                    if not state.available:
                        state.rejected += 1
                        return
                    state.connections.add(upstream)
                    state.accepted += 1
                sockets = (self.request, upstream)
                while True:
                    with state.lock:
                        if not state.available:
                            return
                    ready, _, _ = select.select(sockets, [], [], 0.2)
                    for source in ready:
                        data = source.recv(65536)
                        if not data:
                            return
                        target = upstream if source is self.request else self.request
                        target.sendall(data)
            except (OSError, ValueError):
                return
            finally:
                with state.lock:
                    state.connections.discard(self.request)
                    if upstream is not None:
                        state.connections.discard(upstream)
                if upstream is not None:
                    upstream.close()

    class ControlHandler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def respond(self, status, value):
            body = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            self.respond(200 if self.path == "/__test/state" else 404, state.snapshot())

        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            if self.path != "/__test/config":
                self.respond(404, {})
                return
            try:
                value = json.loads(body)
                if type(value.get("available")) is not bool:
                    raise ValueError("available must be a boolean")
            except (ValueError, AttributeError):
                self.respond(400, {"error": "Expected an available boolean"})
                return
            state.configure(value["available"])
            self.respond(200, state.snapshot())

    class TCPServer(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

    proxy = TCPServer((host, port), TCPHandler)
    control = ThreadingHTTPServer((host, control_port), ControlHandler)
    control.daemon_threads = True
    proxy.state = state
    return proxy, control


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=15432)
    parser.add_argument("--control-port", type=int, default=8080)
    parser.add_argument("--upstream-host", required=True)
    parser.add_argument("--upstream-port", type=int, default=5432)
    args = parser.parse_args()
    proxy, control = make_proxy(args.host, args.port, args.control_port, args.upstream_host, args.upstream_port)
    control_thread = threading.Thread(target=control.serve_forever, daemon=True)
    control_thread.start()
    try:
        proxy.serve_forever()
    finally:
        proxy.state.configure(False)
        proxy.server_close()
        control.shutdown()
        control.server_close()
