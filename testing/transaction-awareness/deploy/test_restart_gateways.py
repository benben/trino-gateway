import unittest

from restart_gateways import validate_scope, verify_replacement


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


if __name__ == "__main__":
    unittest.main()
