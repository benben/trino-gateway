"""Small HTTP client that preserves repeated request and response headers."""

import http.client
import json
import os
import ssl
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit


@dataclass
class Response:
    status: int
    headers: list
    body: bytes

    def values(self, name):
        return [value for key, value in self.headers if key.lower() == name.lower()]

    def json(self):
        return json.loads(self.body)


def request(url, method="GET", body=None, headers=(), timeout=40):
    parsed = urlsplit(url)
    if parsed.scheme == "https":
        context = ssl.create_default_context(cafile=os.environ.get("TX_CA_FILE"))
        connection = http.client.HTTPSConnection(parsed.hostname, parsed.port, timeout=timeout, context=context)
    elif parsed.scheme == "http":
        connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=timeout)
    else:
        raise ValueError("Only HTTP and HTTPS endpoints are supported")
    payload = body.encode() if isinstance(body, str) else body
    try:
        connection.putrequest(method, urlunsplit(("", "", parsed.path or "/", parsed.query, "")))
        for key, value in headers:
            connection.putheader(key, value)
        if payload is not None:
            connection.putheader("Content-Length", str(len(payload)))
        connection.endheaders(payload)
        response = connection.getresponse()
        return Response(response.status, response.getheaders(), response.read())
    finally:
        connection.close()


def through_gateway(next_uri, gateway):
    uri = urlsplit(next_uri)
    return gateway.rstrip("/") + urlunsplit(("", "", uri.path, uri.query, ""))


def statement(gateway, sql, transaction="NONE", user="test-user", routing_group="transaction-test", extra=()):
    headers = [("X-Trino-User", user), ("X-Trino-Transaction-Id", transaction),
               ("X-Trino-Routing-Group", routing_group), ("Content-Type", "text/plain")]
    return request(gateway.rstrip("/") + "/v1/statement", "POST", sql, headers + list(extra))


def finish(response, gateway, limit=20):
    responses = [response]
    while 200 <= response.status < 300:
        payload = response.json()
        if "nextUri" not in payload:
            break
        if len(responses) >= limit:
            raise AssertionError("Query exceeded the bounded page count")
        response = request(through_gateway(payload["nextUri"], gateway))
        responses.append(response)
    return responses
