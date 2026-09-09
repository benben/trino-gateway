import unittest

from transaction_config import configure_transactions


class TransactionConfigTests(unittest.TestCase):
    def test_enable_preserves_database_credentials_and_reuses_keys(self):
        baseline = {"dataStore": {"jdbcUrl": "jdbc:postgresql://postgres:5432/gateway", "user": "test", "password": "synthetic"},
                    "monitor": {"taskDelay": "1s"}}
        enabled = configure_transactions(baseline, enabled=True, terminal_retention=2)
        self.assertNotIn("transactionAwareness", baseline)
        self.assertEqual(enabled["dataStore"], baseline["dataStore"])
        self.assertEqual(enabled["monitor"], baseline["monitor"])
        settings = enabled["transactionAwareness"]
        self.assertTrue(settings["enabled"])
        self.assertGreaterEqual(len(settings["identityKey"].encode()), 32)
        self.assertNotEqual(settings["identityKey"], settings["adminToken"])
        self.assertEqual(settings["terminalRetentionSeconds"], 2)
        disabled = configure_transactions(enabled, enabled=False, terminal_retention=120)
        self.assertFalse(disabled["transactionAwareness"]["enabled"])
        again = configure_transactions(disabled, enabled=True, terminal_retention=2)
        self.assertEqual(again["transactionAwareness"], settings)

    def test_refuses_invalid_persistent_keys_instead_of_rotating(self):
        for value in ["", "short", None, 32]:
            with self.subTest(value=value):
                config = {"dataStore": {"jdbcUrl": "jdbc:postgresql://postgres:5432/gateway"},
                          "transactionAwareness": {"identityKey": value}}
                with self.assertRaises(ValueError):
                    configure_transactions(config, enabled=True, terminal_retention=2)

    def test_requires_postgres_and_valid_retention(self):
        for config, retention in [({}, 2), ({"dataStore": {"jdbcUrl": "jdbc:mysql://postgres/gateway"}}, 2),
                                  ({"dataStore": {"jdbcUrl": "jdbc:postgresql://postgres/gateway"}}, -1)]:
            with self.assertRaises(ValueError):
                configure_transactions(config, enabled=True, terminal_retention=retention)
        for retention in [0, 86401, True, False]:
            with self.subTest(retention=retention), self.assertRaises(ValueError):
                configure_transactions({"dataStore": {"jdbcUrl": "jdbc:postgresql://postgres/gateway"}},
                                       enabled=True, terminal_retention=retention)


if __name__ == "__main__":
    unittest.main()
