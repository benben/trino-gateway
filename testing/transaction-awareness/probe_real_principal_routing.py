"""Bounded read-only SQL probe for a candidate Gateway and real Trino.

Supply REAL_GATEWAY_URLS, REAL_TRINO_PRINCIPAL, REAL_TRINO_CATALOG and TX_CA_FILE.
The password is read interactively and never written to a file or printed.
No routing-group header is required. This probe never changes deployment state.
"""

import base64
import getpass
import os
from urllib.parse import urlsplit

from protocol import request


def main():
    gateways = [value.rstrip("/") for value in os.environ["REAL_GATEWAY_URLS"].split(",")]
    principal = os.environ["REAL_TRINO_PRINCIPAL"]
    catalog = os.environ["REAL_TRINO_CATALOG"]
    password = getpass.getpass("Warehouse password: ")
    authorization = "Basic " + base64.b64encode((principal + ":" + password).encode()).decode()
    allowed_origins = {(urlsplit(url).scheme, urlsplit(url).netloc) for url in gateways}
    headers = [("Authorization", authorization), ("X-Trino-User", principal), ("X-Trino-Catalog", catalog), ("Content-Type", "text/plain")]

    def query(sql, gateway=0, transaction=None, extra=()):
        selected = headers + list(extra)
        if transaction:
            selected.append(("X-Trino-Transaction-Id", transaction))
        else:
            selected.append(("X-Trino-Transaction-Id", "NONE"))
        response = request(gateways[gateway] + "/v1/statement", "POST", sql, selected)
        data, started, cleared = [], None, False
        for _ in range(100):
            if response.status != 200:
                raise AssertionError(f"Read-only SQL returned HTTP {response.status}")
            result = response.json()
            if "error" in result:
                raise AssertionError("Read-only SQL returned a Trino error; inspect the private coordinator diagnostic")
            data.extend(result.get("data", []))
            if response.values("X-Trino-Started-Transaction-Id"):
                started = response.values("X-Trino-Started-Transaction-Id")[0]
            cleared |= bool(response.values("X-Trino-Clear-Transaction-Id"))
            next_uri = result.get("nextUri")
            if not next_uri:
                return data, started, cleared
            parsed = urlsplit(next_uri)
            if (parsed.scheme, parsed.netloc) not in allowed_origins:
                raise AssertionError("Advertised nextUri bypasses the candidate Gateway")
            response = request(next_uri, headers=headers)
        raise AssertionError("Read-only SQL exceeded its page bound")

    for index in range(len(gateways)):
        assert query("SELECT 1", index)[0] == [[1]]
        assert query("SELECT 1", index, extra=[("X-Trino-Routing-Group", "untrusted-client-group")])[0] == [[1]]
        assert query("SHOW SCHEMAS", index)[0]
    _, transaction, _ = query("START TRANSACTION READ ONLY")
    if not transaction:
        raise AssertionError("Trino did not start a read-only transaction")
    try:
        assert query("SELECT 1", len(gateways) - 1, transaction)[0] == [[1]]
    finally:
        assert query("ROLLBACK", len(gateways) - 1, transaction)[2]
    print("PASS: headerless SQL, ignored client group, schema listing, Gateway result URLs and read-only transaction")


if __name__ == "__main__":
    main()
