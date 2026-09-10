import unittest
import base64
import json
from unittest.mock import patch
from subprocess import CompletedProcess

from render import TASK, password_hash, render


class LabRenderTests(unittest.TestCase):
    def setUp(self):
        self.namespace = "gateway-tx-lab-unit-test"
        self.items = render(self.namespace, "synthetic-test-password", "print('fixture')")["items"]

    def test_all_resources_are_task_scoped(self):
        for resource in self.items:
            self.assertEqual(resource["metadata"]["labels"]["task"], TASK)
            if resource["kind"] == "Namespace":
                self.assertEqual(resource["metadata"]["name"], self.namespace)
            else:
                self.assertEqual(resource["metadata"]["namespace"], self.namespace)
            self.assertNotIn(resource["kind"], {"ClusterRole", "ClusterRoleBinding", "Ingress", "PersistentVolumeClaim"})
            if resource["kind"] == "Service":
                self.assertEqual(resource["spec"]["type"], "ClusterIP")

    def test_non_root_pods_have_no_service_account_credentials(self):
        for resource in self.items:
            if resource["kind"] != "Deployment":
                continue
            pod = resource["spec"]["template"]["spec"]
            self.assertFalse(pod["automountServiceAccountToken"])
            self.assertTrue(pod["securityContext"]["runAsNonRoot"])
            self.assertEqual(pod["securityContext"]["seccompProfile"]["type"], "RuntimeDefault")
            self.assertNotIn("hostNetwork", pod)
            self.assertNotIn("tolerations", pod)
            for volume in pod["volumes"]:
                self.assertNotIn("hostPath", volume)
            for container in pod["containers"]:
                self.assertFalse(container["securityContext"]["allowPrivilegeEscalation"])
                self.assertEqual(container["securityContext"]["capabilities"]["drop"], ["ALL"])
                self.assertEqual(container["resources"]["requests"], container["resources"]["limits"])

    def test_two_gateways_share_postgres_and_budget_is_bounded(self):
        gateway = next(item for item in self.items if item["kind"] == "Deployment" and item["metadata"]["name"] == "gateway")
        self.assertEqual(gateway["spec"]["replicas"], 2)
        quota = next(item for item in self.items if item["kind"] == "ResourceQuota")
        self.assertEqual(quota["spec"]["hard"], {"requests.cpu": "8", "limits.cpu": "8", "requests.memory": "16Gi", "limits.memory": "16Gi", "pods": "20"})
        config = next(item for item in self.items if item["kind"] == "Secret" and item["metadata"]["name"] == "gateway-config")
        self.assertIn("jdbc:postgresql://postgres:5432/gateway", config["stringData"]["config.yaml"])

    def test_gateway_image_override_does_not_change_other_resources(self):
        custom = render(self.namespace, "synthetic-test-password", "print('fixture')", gateway_image="example.invalid/gateway@sha256:" + "a" * 64)["items"]
        original = next(item for item in self.items if item["kind"] == "Deployment" and item["metadata"]["name"] == "gateway")
        updated = next(item for item in custom if item["kind"] == "Deployment" and item["metadata"]["name"] == "gateway")
        self.assertEqual(updated["spec"]["template"]["spec"]["containers"][0]["image"], "example.invalid/gateway@sha256:" + "a" * 64)
        updated["spec"]["template"]["spec"]["containers"][0]["image"] = original["spec"]["template"]["spec"]["containers"][0]["image"]
        self.assertEqual(custom, self.items)

    def test_benchmark_backends_have_explicit_resources_and_never_preempt(self):
        priority = {"metadata": {"name": "test-low", "labels": {"task": TASK}}, "value": -10,
                    "preemptionPolicy": "Never", "globalDefault": False}
        items = render(self.namespace, "synthetic-test-password", "fake", benchmark=True, priority_class=priority)["items"]
        quota = next(item for item in items if item["kind"] == "ResourceQuota")["spec"]["hard"]
        self.assertEqual((quota["requests.memory"], quota["limits.memory"]), ("16Gi", "16Gi"))
        self.assertEqual(items, render(self.namespace, "synthetic-test-password", "fake", benchmark=True,
                                      priority_class=priority, benchmark_fixture_memory="512Mi")["items"])
        for item in items:
            if item["kind"] == "Deployment":
                pod = item["spec"]["template"]["spec"]
                self.assertEqual(pod["preemptionPolicy"], "Never")
                self.assertEqual(pod["priorityClassName"], "test-low")
                if item["metadata"]["name"].startswith("fixture-"):
                    self.assertEqual(pod["containers"][0]["resources"]["requests"], {"cpu": "1", "memory": "512Mi"})

    def test_benchmark_rejects_absent_or_unsafe_priority_class(self):
        safe = {"metadata": {"name": "test-low", "labels": {"task": TASK}}, "value": -10,
                "preemptionPolicy": "Never", "globalDefault": False}
        for value in (None, safe | {"value": 0}, safe | {"globalDefault": True},
                      safe | {"preemptionPolicy": "PreemptLowerPriority"}, safe | {"metadata": {"name": "other-app"}}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                render(self.namespace, "synthetic-test-password", "fake", benchmark=True, priority_class=value)

    def test_benchmark_fixture_memory_is_explicit_and_bounded(self):
        priority = {"metadata": {"name": "fixture-benchmark", "labels": {"task": TASK}},
                    "value": -10, "preemptionPolicy": "Never", "globalDefault": False}
        for memory in ("512Mi", "2Gi", "4Gi", "8Gi"):
            with self.subTest(memory=memory):
                items = render(self.namespace, "synthetic-test-password", "fake", benchmark=True,
                               priority_class=priority, benchmark_fixture_memory=memory)["items"]
                fixtures = [item for item in items if item["kind"] == "Deployment" and item["metadata"]["name"].startswith("fixture-")]
                self.assertEqual(len(fixtures), 2)
                for fixture in fixtures:
                    resources = fixture["spec"]["template"]["spec"]["containers"][0]["resources"]
                    self.assertEqual(resources, {kind: {"cpu": "1", "memory": memory} for kind in ("requests", "limits")})
        for memory in ("1Gi", "16Gi", "unbounded", None):
            with self.subTest(memory=memory), self.assertRaises(ValueError):
                render(self.namespace, "synthetic-test-password", "fake", benchmark=True,
                       priority_class=priority, benchmark_fixture_memory=memory)
        with self.assertRaises(ValueError):
            render(self.namespace, "synthetic-test-password", "fake", benchmark_fixture_memory="8Gi")

    def test_explicit_benchmark_memory_quota_fits_initial_replicas_and_extra_fixtures(self):
        priority = {"metadata": {"name": "fixture-benchmark", "labels": {"task": TASK}},
                    "value": -10, "preemptionPolicy": "Never", "globalDefault": False}
        for extra in ([], ["cell-two-blue", "cell-two-green"]):
            items = render(self.namespace, "synthetic-test-password", "fake", benchmark=True, priority_class=priority,
                           benchmark_fixture_memory="8Gi", extra_fixtures=extra)["items"]
            quota = next(item for item in items if item["kind"] == "ResourceQuota")["spec"]["hard"]
            memory_mi = 0
            for item in items:
                if item["kind"] == "Deployment":
                    for container in item["spec"]["template"]["spec"]["containers"]:
                        value = container["resources"]["requests"]["memory"]
                        memory_mi += int(value[:-2]) * (1024 if value.endswith("Gi") else 1) * item["spec"]["replicas"]
            self.assertEqual(quota["requests.memory"], str((memory_mi + 2047) // 1024) + "Gi")
            self.assertEqual(quota["limits.memory"], quota["requests.memory"])
            self.assertEqual((quota["requests.cpu"], quota["limits.cpu"], quota["pods"]), ("8", "8", "20"))
        with self.assertRaisesRegex(ValueError, "140Gi"):
            render(self.namespace, "synthetic-test-password", "fake", benchmark=True, priority_class=priority,
                   benchmark_fixture_memory="8Gi", extra_fixtures=["cell-" + str(index) for index in range(20)])

    def test_benchmark_without_explicit_class_fails_closed(self):
        with self.assertRaises(ValueError):
            render(self.namespace, "synthetic-test-password", "fake", benchmark=True)

    def test_nonbenchmark_explicit_priority_keeps_small_fixture_resources(self):
        priority = {"metadata": {"name": "test-low", "labels": {"task": TASK}}, "value": -10,
                    "preemptionPolicy": "Never", "globalDefault": False}
        items = render(self.namespace, "synthetic-test-password", "fake", extra_fixtures=["normal-blue"], priority_class=priority)["items"]
        pod = next(item for item in items if item["kind"] == "Deployment" and item["metadata"]["name"] == "fixture-normal-blue")["spec"]["template"]["spec"]
        self.assertEqual(pod["priorityClassName"], "test-low")
        self.assertEqual(pod["containers"][0]["resources"]["requests"], {"cpu": "50m", "memory": "64Mi"})

    def test_network_is_namespace_scoped_and_real_trino_preserves_proxy_urls(self):
        network = next(item for item in self.items if item["kind"] == "NetworkPolicy")["spec"]
        self.assertEqual(network["podSelector"], {})
        self.assertEqual(network["policyTypes"], ["Ingress", "Egress"])
        self.assertEqual(network["ingress"], [{"from": [{"podSelector": {"matchLabels": {"task": TASK}}}]}])
        self.assertEqual(len(network["egress"]), 2)
        for resource in self.items:
            if resource["kind"] == "ConfigMap" and resource["metadata"]["name"].startswith("trino-"):
                self.assertIn("http-server.process-forwarded=true", resource["data"]["config.properties"])

    def test_real_trino_uses_explicit_synthetic_password_authentication(self):
        auth = next(item for item in self.items if item["kind"] == "Secret" and item["metadata"]["name"] == "trino-test-auth")
        self.assertEqual(auth["stringData"]["password.db"], "user:!\n")
        custom = render(self.namespace, "synthetic-test-password", trino_password_hash="dummy-not-a-working-hash")["items"]
        custom_auth = next(item for item in custom if item["metadata"]["name"] == "trino-test-auth")
        self.assertEqual(custom_auth["stringData"]["password.db"], "user:dummy-not-a-working-hash\n")
        self.assertNotEqual(auth["stringData"]["internal-secret-blue"], auth["stringData"]["internal-secret-green"])
        repeated = next(item for item in render(self.namespace, "synthetic-test-password")["items"] if item["metadata"]["name"] == "trino-test-auth")
        self.assertEqual(auth["stringData"], repeated["stringData"])
        for resource in self.items:
            if resource["kind"] == "ConfigMap" and resource["metadata"]["name"].startswith("trino-"):
                self.assertIn("http-server.authentication.type=PASSWORD", resource["data"]["config.properties"])
                self.assertIn("file.password-file=/etc/trino-auth/password.db", resource["data"]["password-authenticator.properties"])

    def test_tls_mounts_only_server_keys_and_verifies_backend_certificates(self):
        files = {name: b"synthetic fixture bytes" for name in ["gateway.p12", "trino-blue.p12", "trino-green.p12", "truststore.p12"]}
        items = render(self.namespace, "synthetic-test-password", tls_files=files)["items"]
        secret = next(item for item in items if item["metadata"]["name"] == "lab-tls")
        self.assertEqual(set(secret["data"]), set(files))
        self.assertEqual(base64.b64decode(secret["data"]["gateway.p12"]), files["gateway.p12"])
        config = json.loads(next(item for item in items if item["metadata"]["name"] == "gateway-config")["stringData"]["config.yaml"])
        for client in ["proxy", "monitor"]:
            self.assertEqual(config["serverConfig"][client + ".http-client.trust-store-path"], "/etc/lab-tls/truststore.p12")
        for item in items:
            if item["kind"] == "Deployment" and item["metadata"]["name"] in {"gateway", "trino-blue", "trino-green"}:
                pod = item["spec"]["template"]["spec"]
                volume = next(volume for volume in pod["volumes"] if volume["name"] == "tls")
                mounted = {entry["key"] for entry in volume["secret"]["items"]}
                name = item["metadata"]["name"]
                self.assertEqual(mounted, {"gateway.p12", "truststore.p12"} if name == "gateway" else {name + ".p12"})

    def test_fault_proxy_is_small_and_separate_from_control_port(self):
        items = render(self.namespace, "synthetic-test-password", proxy_source="fixture source")["items"]
        deployment = next(item for item in items if item["kind"] == "Deployment" and item["metadata"]["name"] == "postgres-fault-proxy")
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(container["resources"]["requests"], {"cpu": "50m", "memory": "64Mi"})
        service = next(item for item in items if item["kind"] == "Service" and item["metadata"]["name"] == "postgres-fault-proxy")
        self.assertEqual({port["port"] for port in service["spec"]["ports"]}, {8080, 15432})

    def test_password_hash_uses_stdin_and_checks_output(self):
        with patch("render.subprocess.run", return_value=CompletedProcess([], 0, "not a hash")) as run:
            with self.assertRaises(ValueError):
                password_hash("dummy-test-input", "/test/htpasswd")
            command = run.call_args.args[0]
            self.assertEqual(command, ["/test/htpasswd", "-niB", "-C", "10", "user"])
            self.assertNotIn("dummy-test-input", command)
            self.assertEqual(run.call_args.kwargs["input"], "dummy-test-input\n")

    def test_fault_only_fixture_omits_real_coordinators_and_keeps_two_gateways(self):
        items = render(self.namespace, "synthetic-test-password", "fake", proxy_source="proxy", include_real_trino=False)["items"]
        self.assertFalse(any(item["metadata"]["name"].startswith("trino-") for item in items))
        deployments = {item["metadata"]["name"]: item for item in items if item["kind"] == "Deployment"}
        self.assertEqual(set(deployments), {"gateway", "postgres", "fixture-blue", "fixture-green", "postgres-fault-proxy"})
        self.assertEqual(deployments["gateway"]["spec"]["replicas"], 2)

    def test_extra_fixture_is_small_namespaced_and_reuses_only_script_config(self):
        items = render(self.namespace, "synthetic-test-password", "fake", extra_fixtures=["cell-two"])["items"]
        resources = [item for item in items if item["metadata"]["name"] == "fixture-cell-two"]
        self.assertEqual({item["kind"] for item in resources}, {"Deployment", "Service"})
        pod = next(item for item in resources if item["kind"] == "Deployment")["spec"]["template"]["spec"]
        self.assertEqual(pod["containers"][0]["resources"]["requests"], {"cpu": "50m", "memory": "64Mi"})
        self.assertEqual(pod["volumes"], [{"name": "source", "configMap": {"name": "fake-backend-source"}}])
        self.assertEqual(pod["containers"][0]["args"][-2:], ["--identity", "cell-two"])
        self.assertFalse(pod["automountServiceAccountToken"])
        for names in [["blue"], ["green"], ["same", "same"], ["escape/namespace"], ["UPPER"], ["trailing-"]]:
            with self.subTest(names=names), self.assertRaises(ValueError):
                render(self.namespace, "synthetic-test-password", "fake", extra_fixtures=names)


if __name__ == "__main__":
    unittest.main()
