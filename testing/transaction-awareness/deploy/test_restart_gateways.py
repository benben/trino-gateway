import unittest

from restart_gateways import validate_running_image, validate_scope, verify_replacement


class RestartSafetyTests(unittest.TestCase):
    def test_requires_exact_dedicated_namespace_confirmation(self):
        validate_scope("explicit-development-context", "gateway-tx-lab-test", "gateway-tx-lab-test")
        for context, namespace, confirmation in [("", "gateway-tx-lab-test", "gateway-tx-lab-test"),
                                                 ("dev", "default", "default"),
                                                 ("dev", "gateway-tx-lab-test", "other")]:
            with self.subTest(namespace=namespace), self.assertRaises(ValueError):
                validate_scope(context, namespace, confirmation)

    def test_requires_all_gateway_processes_replaced_and_dependencies_unchanged(self):
        before = {"gateways": {"old-a", "old-b"}, "dependencies": {"postgres": ("pg", 0), "trino-blue": ("blue", 0)}, "secrets": "digest"}
        after = before | {"gateways": {"new-a", "new-b"}}
        verify_replacement(before, after)
        for changed in [after | {"gateways": {"old-a", "new-b"}},
                        after | {"gateways": {"new-a"}},
                        after | {"dependencies": {"postgres": ("pg", 1), "trino-blue": ("blue", 0)}},
                        after | {"secrets": "different"}]:
            with self.assertRaises(AssertionError):
                verify_replacement(before, changed)

    def test_running_image_requires_digest_and_standard_packaged_jar(self):
        image = "registry.example.test/gateway@sha256:" + "a" * 64
        arguments = ["exec java -Xmx512m -jar /usr/lib/trino-gateway/gateway-ha-jar-with-dependencies.jar /etc/trino-gateway/config.yaml"]
        validate_running_image(image, ["sh", "-c"], arguments)
        for selected_image, command, args in [("registry.example.test/gateway:main", ["sh", "-c"], arguments),
                                               (image, ["sh", "-c"], ["exec java -jar /artifact/launch.jar /etc/trino-gateway/config.yaml"]),
                                               (image, [], [])]:
            with self.subTest(image=selected_image), self.assertRaises(ValueError):
                validate_running_image(selected_image, command, args)

    def test_running_image_and_command_must_stay_unchanged(self):
        before = {"gateways": {"old-a", "old-b"}, "dependencies": {}, "secrets": "same",
                  "image_config": {"image": "digest-one", "command": ["same"]}}
        after = before | {"gateways": {"new-a", "new-b"}}
        verify_replacement(before, after)
        for config in [{"image": "digest-two", "command": ["same"]}, {"image": "digest-one", "command": ["changed"]}]:
            with self.assertRaisesRegex(AssertionError, "image or launch command"):
                verify_replacement(before, after | {"image_config": config})


if __name__ == "__main__":
    unittest.main()
