import unittest

from render import TASK, render


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

    def test_network_is_namespace_scoped_and_real_trino_preserves_proxy_urls(self):
        network = next(item for item in self.items if item["kind"] == "NetworkPolicy")["spec"]
        self.assertEqual(network["podSelector"], {})
        self.assertEqual(network["policyTypes"], ["Ingress", "Egress"])
        self.assertEqual(network["ingress"], [{"from": [{"podSelector": {"matchLabels": {"task": TASK}}}]}])
        self.assertEqual(len(network["egress"]), 2)
        for resource in self.items:
            if resource["kind"] == "ConfigMap" and resource["metadata"]["name"].startswith("trino-"):
                self.assertIn("http-server.process-forwarded=true", resource["data"]["config.properties"])


if __name__ == "__main__":
    unittest.main()
