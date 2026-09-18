"""Two real Gateway processes exercise administrative CAS against disposable PostgreSQL."""

import contextlib
import base64
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
from concurrent.futures import ThreadPoolExecutor

from fake_trino import make_server
from protocol import request, through_gateway


def unused_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def stop_process(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


@contextlib.contextmanager
def local_gateways(form_auth=False, processes=None, server_config=None, pool=None, backend_names=("blue", "green")):
    """Two real Gateway processes over one disposable PostgreSQL, with synthetic coordinators.

    ``pool`` is the optional ``transactionAwareness.pool`` block; omitted by default, so every
    existing caller runs the legacy configuration unchanged. ``backend_names`` names the synthetic
    coordinators, two by default.
    """
    pg_bin = Path(os.environ["GATEWAY_TEST_PG_BIN"])
    with tempfile.TemporaryDirectory(prefix="gateway-rollout-api-") as directory, contextlib.ExitStack() as cleanup:
        root = Path(directory)
        pg_port = unused_port()
        subprocess.run([str(pg_bin / "initdb"), "-D", str(root / "pg"), "-U", "gateway_test", "-A", "trust", "--no-locale"], check=True, stdout=subprocess.DEVNULL)
        subprocess.run([str(pg_bin / "pg_ctl"), "-D", str(root / "pg"), "-l", str(root / "postgres.log"), "-o", f"-h 127.0.0.1 -p {pg_port} -k {directory}", "-w", "start"], check=True, stdout=subprocess.DEVNULL)
        cleanup.callback(subprocess.run, [str(pg_bin / "pg_ctl"), "-D", str(root / "pg"), "-m", "fast", "-w", "stop"], check=True, stdout=subprocess.DEVNULL)
        token = secrets.token_hex(32)
        key = secrets.token_hex(32)
        backends = []
        for name in backend_names:
            backend = make_server(identity=name)
            threading.Thread(target=backend.serve_forever, daemon=True).start()
            cleanup.callback(backend.server_close)
            cleanup.callback(backend.shutdown)
            backends.append(backend)
        gateways = []
        for index in range(2):
            port = unused_port()
            endpoint = f"http://127.0.0.1:{port}"
            gateways.append(endpoint)
            config = {
                "serverConfig": {"node.environment": "test", "node.bind-ip": "127.0.0.1", "http-server.http.port": str(port)},
                "dataStore": {"jdbcUrl": f"jdbc:postgresql://127.0.0.1:{pg_port}/postgres", "user": "gateway_test", "password": "unused", "driver": "org.postgresql.Driver"},
                "monitor": {"taskDelay": "1s"},
                "routing": {"defaultRoutingGroup": "cell"},
                "transactionAwareness": {"enabled": True, "identityKey": key, "adminToken": token, "terminalRetentionSeconds": 1},
            }
            if pool is not None:
                config["transactionAwareness"]["pool"] = pool
            config["serverConfig"].update(server_config or {})
            if form_auth:
                auth = Path(__file__).resolve().parents[2] / "gateway-ha/src/test/resources/auth"
                config.update({
                    "presetUsers": {"rollout": {"password": token, "privileges": "API"}, "ordinary": {"password": token, "privileges": "USER"}, "fixture-admin": {"password": token, "privileges": "ADMIN_USER_API"}},
                    "authentication": {"defaultType": "form", "form": {"selfSignKeyPair": {"privateKey": str(auth / "test_ec_private_key.pem"), "publicKey": str(auth / "test_ec_public_key.pem")}}},
                    "authorization": {"admin": "(.*)ADMIN(.*)", "api": "(.*)API(.*)", "user": "(.*)USER(.*)"},
                })
            config_file = root / f"gateway-{index}.json"
            config_file.write_text(json.dumps(config))
            logfile = cleanup.enter_context((root / f"gateway-{index}.log").open("wb"))
            process = subprocess.Popen([os.environ["GATEWAY_TEST_JAVA"], "-Xmx384m", "-cp", os.environ["GATEWAY_TEST_CLASSPATH"], "io.trino.gateway.ha.HaGatewayLauncher", str(config_file)], stdout=logfile, stderr=subprocess.STDOUT)
            if processes is not None:
                processes.append(process)
            cleanup.callback(stop_process, process)
            deadline = time.monotonic() + 60
            while True:
                if process.poll() is not None or time.monotonic() > deadline:
                    raise AssertionError("Isolated Gateway did not become ready")
                try:
                    if request(endpoint + "/trino-gateway/readyz", timeout=1).status == 200:
                        break
                except (OSError, TimeoutError):
                    pass
                time.sleep(0.1)
            if index == 0:
                for backend in backends:
                    url = f"http://127.0.0.1:{backend.server_port}"
                    body = {"name": backend.state.identity, "proxyTo": url, "externalUrl": url, "active": True, "routingGroup": "cell"}
                    headers = [("Content-Type", "application/json")]
                    if form_auth:
                        headers.append(("Authorization", "Basic " + base64.b64encode(("fixture-admin:" + token).encode()).decode()))
                    response = request(endpoint + "/entity?entityType=GATEWAY_BACKEND", "POST", json.dumps(body), headers)
                    if response.status != 200:
                        raise AssertionError("Isolated backend registration failed")
        time.sleep(2)
        yield gateways, backends, token


class RolloutApiContract(unittest.TestCase):
    def test_form_api_machine_identity_and_separate_admin_token(self):
        with local_gateways(form_auth=True) as (gateways, _backends, token):
            endpoint = gateways[0] + "/gateway/transactions/routes/cell"
            basic = "Basic " + base64.b64encode(("rollout:" + token).encode()).decode()
            headers = [("Authorization", basic), ("X-Gateway-Transaction-Admin-Token", token)]
            allowed = request(endpoint, headers=headers)
            self.assertEqual(allowed.status, 200, allowed.body)
            self.assertEqual(request(endpoint).status, 403)
            self.assertEqual(request(endpoint, headers=[("Authorization", basic)]).status, 403)
            self.assertEqual(request(endpoint, headers=[("Authorization", basic), ("X-Gateway-Transaction-Admin-Token", "incorrect")]).status, 403)
            self.assertEqual(request(endpoint, headers=headers + [("X-Gateway-Transaction-Admin-Token", token)]).status, 400)
            self.assertEqual(request(endpoint, headers=headers + [("Authorization", basic)]).status, 400)
            ordinary = "Basic " + base64.b64encode(("ordinary:" + token).encode()).decode()
            denied = request(endpoint, headers=[("Authorization", ordinary), ("X-Gateway-Transaction-Admin-Token", token)])
            self.assertEqual(denied.status, 403, denied.body)
            self.assertNotIn("routingGroup", denied.json())
            legacy_denied = request(gateways[0] + "/entity", headers=[("Authorization", ordinary)])
            self.assertEqual(legacy_denied.status, 200)
            self.assertEqual(legacy_denied.json()["code"], 401)
            wrong = "Basic " + base64.b64encode(b"rollout:incorrect").decode()
            self.assertEqual(request(endpoint, headers=[("Authorization", wrong), ("X-Gateway-Transaction-Admin-Token", token)]).status, 403)
            warehouse = "Basic " + base64.b64encode(b"warehouse:warehouse-password").decode()
            query = request(gateways[0] + "/v1/statement", "POST", "SELECT 1", [("Authorization", warehouse), ("X-Trino-User", "warehouse"), ("Content-Type", "text/plain")])
            self.assertEqual(query.status, 200, query.body)
            for _ in range(20):
                if "nextUri" not in query.json():
                    break
                query = request(through_gateway(query.json()["nextUri"], gateways[1]))
                self.assertEqual(query.status, 200, query.body)
            else:
                self.fail("Warehouse query exceeded page bound")

    def test_fenced_rollout_operations_across_replicas(self):
        with local_gateways() as (gateways, backends, token):
            def admin(path, method="GET", body=None, replica=0, guard=None):
                headers = [("Authorization", "Bearer " + token), ("Content-Type", "application/json")]
                if guard is not None:
                    headers += [("X-Gateway-Operation-Id", guard["operationId"]), ("X-Gateway-Operation-Version", str(guard["version"]))]
                return request(gateways[replica] + "/gateway/transactions" + path, method,
                               json.dumps(body) if body is not None else None,
                               headers)

            def successful(response):
                self.assertEqual(response.status, 200, response.body)
                return response.json()

            route_path = "/routes/cell"
            initial = successful(admin(route_path))
            self.assertEqual(initial, {"routingGroup": "cell", "generation": 0, "backendName": None, "backendIncarnation": None})
            self.assertEqual(request(gateways[0] + "/gateway/transactions" + route_path).status, 403)
            self.assertEqual(admin(route_path, "PUT", {}).status, 400)
            statuses = {}
            for name in ("blue", "green"):
                path = "/backends/" + name
                drained = successful(admin(path + "/drain", "POST"))
                statuses[name] = successful(admin(path + "/resume", "POST", {"generation": drained["generation"]}))
                self.assertTrue(statuses[name]["nodeId"])
                self.assertTrue(statuses[name]["coordinatorId"])
            desired = [{"expectedGeneration": 0, "expectedBackendName": None, "backendName": name,
                        "backendIncarnation": statuses[name]["incarnation"]} for name in ("blue", "green")]
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [executor.submit(admin, route_path, "PUT", body, index) for index, body in enumerate(desired)]
                results = [future.result() for future in futures]
            self.assertEqual(sorted(result.status for result in results), [200, 409])
            observed = successful(admin(route_path, replica=1))
            self.assertEqual(observed["generation"], 1)
            winner = observed["backendName"]
            other = "green" if winner == "blue" else "blue"
            self.assertEqual(admin(route_path, "PUT", desired[0]).status, 409)
            self.assertEqual(admin(route_path, "PUT", desired[1], replica=1).status, 409)
            moved = {"expectedGeneration": 1, "expectedBackendName": winner, "backendName": other,
                     "backendIncarnation": statuses[other]["incarnation"]}
            self.assertEqual(successful(admin(route_path, "PUT", moved))["generation"], 2)
            returned = {"expectedGeneration": 2, "expectedBackendName": other, "backendName": winner,
                        "backendIncarnation": statuses[winner]["incarnation"]}
            self.assertEqual(successful(admin(route_path, "PUT", returned, replica=1))["generation"], 3)
            self.assertEqual(admin(route_path, "PUT", moved).status, 409)
            path = "/backends/" + other
            prior = successful(admin(path + "/drain"))
            expected = {"expectedIncarnation": prior["incarnation"], "expectedGeneration": prior["generation"]}
            self.assertEqual(admin(path + "/drain", "POST", {}).status, 400)
            draining = successful(admin(path + "/drain", "POST", expected, replica=1))
            self.assertEqual(admin(path + "/drain", "POST", expected).status, 409)
            self.assertTrue(draining["readyToSeal"])
            sealed = successful(admin(path + "/seal", "POST", {"generation": draining["generation"]}))
            self.assertTrue(sealed["drained"])
            backend = backends[0 if other == "blue" else 1]
            self.assertEqual(request(f"http://127.0.0.1:{backend.server_port}/__test/restart", "POST", "{}").status, 200)
            reincarnation = {"incarnation": sealed["incarnation"], "generation": sealed["generation"]}
            replaced = successful(admin(path + "/reincarnate", "POST", reincarnation, replica=1))
            self.assertNotEqual(replaced["incarnation"], sealed["incarnation"])
            self.assertNotEqual((replaced["nodeId"], replaced["coordinatorId"]), (sealed["nodeId"], sealed["coordinatorId"]))
            self.assertEqual(admin(path + "/reincarnate", "POST", reincarnation).status, 409)
            self.assertEqual(successful(admin(path + "/drain")), replaced)
            resumed = successful(admin(path + "/resume", "POST", {"generation": replaced["generation"]}))
            self.assertTrue(resumed["acceptingNewQueries"])
            stale_target = {"expectedGeneration": 3, "expectedBackendName": winner, "backendName": other,
                            "backendIncarnation": sealed["incarnation"]}
            self.assertEqual(admin(route_path, "PUT", stale_target).status, 409)
            self.assertEqual(successful(admin(route_path))["generation"], 3)

            operation_path = "/rollouts/cell"
            plan = {"operationId": "promotion-one", "planHash": "a" * 64, "expectedRouteGeneration": 3,
                    "sourceBackend": winner, "sourceIncarnation": statuses[winner]["incarnation"],
                    "targetBackend": other, "targetIncarnation": resumed["incarnation"]}
            operation = successful(admin(operation_path + "/acquire", "POST", plan))
            self.assertEqual(successful(admin(operation_path + "/acquire", "POST", plan, replica=1)), operation)
            self.assertEqual(admin(operation_path + "/acquire", "POST", dict(plan, operationId="promotion-two")).status, 409)
            self.assertEqual(admin(operation_path + "/acquire", "POST", dict(plan, planHash="b" * 64)).status, 409)
            self.assertEqual(admin("/backends/" + winner + "/drain", "POST").status, 409)
            self.assertEqual(admin("/cutover/cell", "DELETE").status, 409)
            self.assertEqual(admin("/cutover", "POST", {"routingGroup": "cell", "backendName": other}, guard=operation).status, 409)
            claim = {"operationId": operation["operationId"], "expectedVersion": operation["version"], "planHash": plan["planHash"]}
            operation = successful(admin(operation_path + "/publications/warm/claim", "POST", claim))
            self.assertEqual(admin(operation_path + "/publications/warm/claim", "POST", dict(claim, expectedVersion=operation["version"]), replica=1).status, 409)

            def checkpoint(phase, evidence=None):
                nonlocal operation
                body = {"operationId": operation["operationId"], "expectedVersion": operation["version"], "phase": phase, "evidence": evidence or {}}
                operation = successful(admin(operation_path + "/checkpoint", "PUT", body, replica=1))
                self.assertEqual(admin(operation_path + "/checkpoint", "PUT", body).status, 409)

            publication = {"branch": "promotion-one-warm", "baseSha": "b" * 40, "headSha": "c" * 40, "pullRequest": 1}
            checkpoint("CLAIMED", {"warmPublication": publication})
            checkpoint("WARMED")
            checkpoint("VERIFIED")
            pending = request(gateways[0] + "/v1/statement", "POST", "SELECT 1", [("Authorization", "Basic dXNlcjpwYXNz"), ("X-Trino-User", "user"), ("Content-Type", "text/plain")])
            self.assertEqual(pending.status, 200, pending.body)
            desired_route = {"expectedGeneration": 3, "expectedBackendName": winner, "backendName": other, "backendIncarnation": resumed["incarnation"]}
            self.assertEqual(successful(admin(route_path, "PUT", desired_route, guard=operation))["generation"], 4)
            checkpoint("CUTOVER")
            source_path = "/backends/" + winner
            prior = successful(admin(source_path + "/drain"))
            draining = successful(admin(source_path + "/drain", "POST", {"expectedIncarnation": prior["incarnation"], "expectedGeneration": prior["generation"]}, guard=operation))
            self.assertFalse(draining["readyToSeal"])
            checkpoint("DRAINING")
            self.assertEqual(admin(source_path + "/resume", "POST", {"generation": draining["generation"]}, guard=operation).status, 409)
            self.assertEqual(admin(source_path + "/seal", "POST", {"generation": draining["generation"]}, guard=operation).status, 409)
            for _ in range(20):
                if "nextUri" not in pending.json():
                    break
                pending = request(through_gateway(pending.json()["nextUri"], gateways[1]))
                self.assertEqual(pending.status, 200, pending.body)
            else:
                self.fail("Retained query exceeded page bound")
            deadline = time.monotonic() + 5
            while not successful(admin(source_path + "/drain"))["readyToSeal"]:
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.1)
            sealed_source = successful(admin(source_path + "/seal", "POST", {"generation": draining["generation"]}, guard=operation))
            self.assertTrue(sealed_source["drained"])
            checkpoint("SEALED")
            self.assertEqual(admin(source_path + "/resume", "POST", {"generation": sealed_source["generation"]}, guard=operation).status, 409)
            claim = {"operationId": operation["operationId"], "expectedVersion": operation["version"], "planHash": plan["planHash"]}
            operation = successful(admin(operation_path + "/publications/stop/claim", "POST", claim, replica=1))
            checkpoint("SEALED", {"stopPublication": dict(publication, branch="promotion-one-stop", headSha="d" * 40, pullRequest=2)})
            checkpoint("STOPPED")
            checkpoint("COMPLETE")
            self.assertEqual(successful(admin(operation_path, replica=1)), operation)
            self.assertEqual(admin(source_path + "/resume", "POST", {"generation": sealed_source["generation"]}, guard=operation).status, 409)
            self.assertEqual(admin(operation_path + "/publications/stop/claim", "POST", dict(claim, expectedVersion=operation["version"])).status, 409)
            later_plan = {"operationId": "promotion-two", "planHash": "e" * 64, "expectedRouteGeneration": 4,
                          "sourceBackend": other, "sourceIncarnation": resumed["incarnation"],
                          "targetBackend": winner, "targetIncarnation": sealed_source["incarnation"]}
            later = successful(admin(operation_path + "/acquire", "POST", later_plan, replica=1))
            self.assertEqual(later["phase"], "CLAIMED")
            self.assertEqual(successful(admin(operation_path + "/acquire", "POST", plan)), operation)
            self.assertEqual(admin(source_path + "/resume", "POST", {"generation": sealed_source["generation"]}, guard=operation).status, 409)
            self.assertEqual(successful(admin(operation_path))["operationId"], "promotion-two")


if __name__ == "__main__":
    unittest.main()
