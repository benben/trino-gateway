"""Tests of actual socket blocking, not a mocked database outage."""

import socket
import socketserver
import threading
import unittest

from network_fault_proxy import make_proxy
from protocol import request


class NetworkFaultProxyTests(unittest.TestCase):
    def setUp(self):
        class Echo(socketserver.BaseRequestHandler):
            def handle(self):
                try:
                    while True:
                        data = self.request.recv(1024)
                        if not data:
                            return
                        self.request.sendall(data)
                except OSError:
                    pass

        self.echo = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Echo)
        self.echo.daemon_threads = True
        self.proxy, self.control = make_proxy("127.0.0.1", 0, 0, "127.0.0.1", self.echo.server_address[1])
        self.threads = []
        for server in (self.echo, self.proxy, self.control):
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.threads.append(thread)
        self.control_url = "http://127.0.0.1:" + str(self.control.server_port)

    def tearDown(self):
        self.proxy.state.configure(False)
        for server in (self.proxy, self.control, self.echo):
            server.shutdown()
            server.server_close()
        for thread in self.threads:
            thread.join()

    def connect(self):
        return socket.create_connection(self.proxy.server_address, timeout=2)

    def configure(self, value):
        response = request(self.control_url + "/__test/config", "POST", value)
        self.assertEqual(response.status, 200, response.body)

    def test_forwards_when_available(self):
        with self.connect() as client:
            client.sendall(b"test")
            self.assertEqual(client.recv(4), b"test")

    def test_block_closes_existing_connections(self):
        with self.connect() as client:
            client.sendall(b"first")
            self.assertEqual(client.recv(5), b"first")
            self.configure('{"available": false}')
            self.assertEqual(client.recv(5), b"")
            self.assertEqual(request(self.control_url + "/__test/state").json()["openSockets"], 0)

    def test_block_rejects_new_connections_and_restore_allows_them(self):
        self.configure('{"available": false}')
        with self.connect() as client:
            try:
                client.sendall(b"blocked")
                self.assertEqual(client.recv(7), b"")
            except ConnectionResetError:
                pass
        self.configure('{"available": true}')
        with self.connect() as client:
            client.sendall(b"restored")
            self.assertEqual(client.recv(8), b"restored")

    def test_invalid_configuration_cannot_enable_proxy(self):
        self.configure('{"available": false}')
        response = request(self.control_url + "/__test/config", "POST", '{"available": "true"}')
        self.assertEqual(response.status, 400)
        self.assertFalse(request(self.control_url + "/__test/state").json()["available"])


if __name__ == "__main__":
    unittest.main()
