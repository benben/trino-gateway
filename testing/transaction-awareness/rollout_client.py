"""Bounded read-only rollout probes with no automatic request retries."""

import argparse
import base64
from dataclasses import asdict, dataclass, field
import getpass
import hashlib
import json
import os
import re
import stat
import time
import uuid
from urllib.parse import urlsplit

from protocol import request


@dataclass(frozen=True)
class RecoveryHandle:
    recovery_id: str
    next_uri: str = field(repr=False)
    query_id: str = field(repr=False)
    transaction_id: str = field(repr=False)
    context_hash: str = field(repr=False)
    previous_rows: int


class RolloutFailure(RuntimeError):
    def __init__(self, message, recovery=None):
        super().__init__(message)
        self.recovery = recovery


def private_descriptor(path, flags, directory=False):
    descriptor = os.open(path, flags | os.O_NOFOLLOW | os.O_NONBLOCK | (os.O_DIRECTORY if directory else 0))
    info = os.fstat(descriptor)
    if info.st_uid != os.getuid() or info.st_mode & 0o077 or not (
            stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)):
        os.close(descriptor)
        raise ValueError("recovery_path_must_be_private_and_owned")
    return descriptor


def validate_recovery_directory(directory):
    if directory:
        os.close(private_descriptor(directory, os.O_RDONLY, directory=True))


def save_recovery(handle, directory):
    descriptor = private_descriptor(directory, os.O_RDONLY, directory=True)
    name = str(uuid.uuid4()) + ".json"
    try:
        file_descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                  0o600, dir_fd=descriptor)
        with os.fdopen(file_descriptor, "w") as output:
            json.dump(asdict(handle), output)
    finally:
        os.close(descriptor)
    return os.path.join(directory, name)


def load_recovery(path):
    with os.fdopen(private_descriptor(path, os.O_RDONLY), "r") as source:
        content = source.read(65537)
    if len(content) > 65536:
        raise ValueError("recovery_file_too_large")
    try:
        handle = RecoveryHandle(**json.loads(content))
        if not all(isinstance(value, str) and 0 < len(value) <= 16384 for value in (
                handle.recovery_id, handle.next_uri, handle.query_id, handle.transaction_id, handle.context_hash)):
            raise ValueError()
        if type(handle.previous_rows) is not int or handle.previous_rows < 0:
            raise ValueError()
        if (str(uuid.UUID(handle.recovery_id)) != handle.recovery_id or
                not re.fullmatch(r"[0-9a-f]{64}", handle.context_hash) or
                not re.fullmatch(r"[A-Za-z0-9_]{1,256}", handle.query_id) or
                not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", handle.transaction_id)):
            raise ValueError()
        return handle
    except (TypeError, ValueError):
        raise ValueError("invalid_recovery_file") from None


def recovery_report(error, directory=None):
    handle = getattr(error, "recovery", None)
    if handle is None:
        return {"recovery_available": False}
    report = {"recovery_available": True, "recovery_id": handle.recovery_id}
    if directory:
        try:
            report["recovery_file"] = os.path.basename(save_recovery(handle, directory))
        except (OSError, ValueError):
            report["recovery_save_failed"] = True
    return report


def http_failure(result, method, page):
    body = result.body[:4096].decode("utf-8", errors="replace").lower()
    markers = {
        "transaction_capacity": ("transaction-aware request capacity", "transaction request capacity", "transaction request limit"),
        "backend_identity": ("ready trino coordinator process identity", "backend process identity is unavailable"),
        "routing_state": ("transaction routing state is unavailable", "no backend belongs to the selected routing group",
                          "backend does not accept new", "route target does not accept new statements"),
        "shutdown": ("shutting down", "server is stopping", "server shutdown", "service unavailable: shutdown"),
    }
    categories = [name for name, phrases in markers.items() if any(phrase in body for phrase in phrases)]
    headers = {}
    known_codes = {"CAPACITY_EXHAUSTED": "transaction_capacity", "GATEWAY_STOPPING": "shutdown",
                   "ROUTING_STATE_UNAVAILABLE": "routing_state", "ROUTING_STATE_NOT_ACTIVE": "routing_state"}
    codes = result.values("X-Trino-Gateway-Error")
    if len(codes) == 1 and codes[0] in known_codes:
        headers["X-Trino-Gateway-Error"] = codes[0]
        if known_codes[codes[0]] not in categories:
            categories.append(known_codes[codes[0]])
    allowed_headers = {
        "Retry-After": r"[0-9]{1,6}",
        "Content-Type": r"(?:application/json|text/plain|text/html)(?:; *charset=(?:utf-8|UTF-8|iso-8859-1|ISO-8859-1|us-ascii))?",
        "Server": r"(?:gateway|Jetty|nginx|envoy)(?:[/()]?[0-9.]{1,20}\)?)?",
    }
    for name in ("Retry-After", "Content-Type", "Server"):
        values = result.values(name)
        if values:
            value = values[0]
            headers[name] = value if len(value) <= 100 and re.fullmatch(allowed_headers[name], value) else "redacted"
    return "http_status:" + str(result.status) + ":" + json.dumps({
        "method": method, "page": page, "headers": headers, "classification": categories or ["unknown"]}, sort_keys=True)


class RolloutClient:
    def __init__(self, server, user, catalog, password, group=None):
        self.server = server.rstrip("/")
        self.origin = urlsplit(self.server)
        if (self.origin.scheme != "https" or not self.origin.hostname or self.origin.username or
                self.origin.password or self.origin.query or self.origin.fragment or self.origin.path):
            raise ValueError("Require HTTPS without credentials in the server URL")
        authorization = base64.b64encode((user + ":" + password).encode()).decode()
        self.headers = [("Authorization", "Basic " + authorization), ("X-Trino-User", user),
                        ("X-Trino-Catalog", catalog), ("Content-Type", "text/plain")]
        if group:
            self.headers.append(("X-Trino-Routing-Group", group))
        self.transaction = "NONE"
        self.context_hash = hashlib.sha256(json.dumps([self.server, user, catalog, group]).encode()).hexdigest()
        self.pending_continuation = None

    def validate_continuation(self, uri):
        if not isinstance(uri, str) or len(uri) > 16384 or any(ord(char) <= 32 for char in uri):
            raise RuntimeError("continuation_origin")
        target = urlsplit(uri)
        if (target.username or target.password or target.fragment or (target.scheme, target.hostname, target.port) != (
                self.origin.scheme, self.origin.hostname, self.origin.port)):
            raise RuntimeError("continuation_origin")

    def query(self, sql, deadline_seconds=60, max_pages=500, first_page_pause=0, first_page_callback=None,
              first_page_release=None):
        self.pending_continuation = None
        return self._guarded_run(self.server + "/v1/statement", "POST", sql, None, 0,
                                 deadline_seconds, max_pages, first_page_pause, first_page_callback, first_page_release)

    def resume(self, handle, deadline_seconds=60, max_pages=500):
        if handle.context_hash != self.context_hash or self.transaction != handle.transaction_id:
            raise ValueError("recovery_context")
        self.validate_continuation(handle.next_uri)
        self.pending_continuation = handle
        result = self._guarded_run(handle.next_uri, "GET", None, handle.query_id, handle.previous_rows,
                                   deadline_seconds, max_pages, 0, None, None)
        return {**result, "resumed": True, "previous_rows": handle.previous_rows}

    def _guarded_run(self, *args):
        try:
            return self._run(*args)
        except RolloutFailure:
            raise
        except Exception as error:
            known = r"(?:query_deadline|query_identity_changed|missing_query_identity|conflicting_transaction_identity|transaction_identity_changed|continuation_origin|query_page_limit|query_response_invalid|query_error:[A-Z0-9_]{1,100})"
            message = str(error) if type(error) in (RuntimeError, TimeoutError) and re.fullmatch(known, str(error)) else "client_error:" + type(error).__name__
            raise RolloutFailure(message, self.pending_continuation) from None

    def _run(self, url, method, body, identity, previous_rows, deadline_seconds, max_pages,
             first_page_pause, first_page_callback, first_page_release):
        started = time.monotonic()
        deadline = started + deadline_seconds
        rows, started_ids = [], set()
        for page in range(max_pages):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("query_deadline")
            try:
                result = request(url, method, body, headers=self.headers + [
                    ("X-Trino-Transaction-Id", self.transaction)], timeout=min(30, remaining))
            except Exception as error:
                raise RolloutFailure("request_error:" + type(error).__name__, self.pending_continuation) from None
            if result.status != 200:
                raise RolloutFailure(http_failure(result, method, page), self.pending_continuation)
            payload = result.json()
            if not isinstance(payload, dict) or not isinstance(payload.get("id"), str):
                raise RuntimeError("query_response_invalid")
            stats, data, query_error = payload.get("stats"), payload.get("data"), payload.get("error")
            if (not isinstance(stats, dict) or not isinstance(stats.get("state"), str) or
                    (data is not None and (not isinstance(data, list) or any(not isinstance(row, list) for row in data))) or
                    (query_error is not None and not isinstance(query_error, dict))):
                raise RuntimeError("query_response_invalid")
            if query_error is not None and (stats["state"] != "FAILED" or payload.get("nextUri") is not None or
                    not isinstance(query_error.get("errorName"), str) or
                    not re.fullmatch(r"[A-Z0-9_]{1,100}", query_error["errorName"])):
                raise RuntimeError("query_response_invalid")
            if payload.get("nextUri") is not None:
                self.validate_continuation(payload["nextUri"])
            elif query_error is None and stats["state"] != "FINISHED":
                raise RuntimeError("query_response_invalid")
            if identity is not None and payload.get("id") != identity:
                raise RuntimeError("query_identity_changed")
            identity = payload.get("id")
            if not identity:
                raise RuntimeError("missing_query_identity")
            started_ids.update(result.values("X-Trino-Started-Transaction-Id"))
            if len(started_ids) > 1:
                raise RuntimeError("conflicting_transaction_identity")
            if started_ids:
                if self.transaction != "NONE" and started_ids != {self.transaction}:
                    raise RuntimeError("transaction_identity_changed")
                self.transaction = next(iter(started_ids))
            if result.values("X-Trino-Clear-Transaction-Id"):
                self.transaction = "NONE"
            if query_error is not None:
                self.pending_continuation = None
                name = query_error.get("errorName", "UNKNOWN")
                raise RuntimeError("query_error:" + (name if isinstance(name, str) and re.fullmatch(r"[A-Z0-9_]{1,100}", name) else "UNKNOWN"))
            rows.extend(data or [])
            if not payload.get("nextUri"):
                self.pending_continuation = None
                return {"rows": rows, "pages": page + 1, "query_id": identity,
                        "duration_seconds": time.monotonic() - started}
            self.validate_continuation(payload["nextUri"])
            self.pending_continuation = RecoveryHandle(str(uuid.uuid4()), payload["nextUri"], identity,
                                                       self.transaction, self.context_hash, previous_rows + len(rows))
            if page == 0 and first_page_pause:
                if first_page_callback:
                    first_page_callback()
                if first_page_release is None:
                    time.sleep(first_page_pause)
                else:
                    first_page_release.wait(first_page_pause)
            url, method, body = payload["nextUri"], "GET", None
        raise RuntimeError("query_page_limit")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--group")
    parser.add_argument("--recovery-directory", help="Opt in to private capability files in an existing mode-0700 directory")
    parser.add_argument("--resume-file", help="Explicitly GET the original continuation from a private recovery file")
    args = parser.parse_args()
    validate_recovery_directory(args.recovery_directory)
    client = RolloutClient(args.server, args.user, args.catalog, getpass.getpass("Trino password: "), args.group)
    probes = [("constant", "SELECT 1", [[1]]),
              ("catalog_metadata", "SELECT count(*) >= 0 FROM information_schema.tables", [[True]]),
              ("transaction_begin", "START TRANSACTION READ ONLY", None),
              ("transaction_constant", "SELECT 42", [[42]]),
              ("transaction_catalog", "SELECT count(*) >= 0 FROM information_schema.tables", [[True]]),
              ("transaction_commit", "COMMIT", None),
              ("rollback_begin", "START TRANSACTION READ ONLY", None),
              ("transaction_rollback", "ROLLBACK", None)]
    failed = False
    try:
        if args.resume_file:
            result = client.resume(load_recovery(args.resume_file))
            print(json.dumps({"event": "continuation_resumed", "rows": len(result["rows"]),
                              "previous_rows": result["previous_rows"], "original_run_still_failed": True}), flush=True)
            probes = []
        for label, sql, expected in probes:
            result = client.query(sql)
            if expected is not None and result["rows"] != expected:
                raise RuntimeError("result_mismatch:" + label)
            print(json.dumps({"probe": label, "status": "PASS", "rows": len(result["rows"]),
                              "pages": result["pages"], "duration_seconds": result["duration_seconds"],
                              "query_owner": result["query_id"].rsplit("_", 1)[-1],
                              "transaction_open": client.transaction != "NONE", "client_retries": 0}), flush=True)
    except Exception as error:
        failed = True
        detail = str(error) if isinstance(error, RolloutFailure) else "client_error:" + type(error).__name__
        print(json.dumps({"status": "FAIL", "kind": type(error).__name__, "error": detail,
                          **recovery_report(error, args.recovery_directory)}), flush=True)
    finally:
        if client.transaction != "NONE":
            try:
                client.query("ROLLBACK")
                print(json.dumps({"cleanup": "rollback_pass"}), flush=True)
            except Exception as error:
                failed = True
                print(json.dumps({"cleanup": "rollback_failed", "kind": type(error).__name__,
                                  **recovery_report(error, args.recovery_directory)}), flush=True)
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
