import unittest

from load_config import prepare
from render import TASK


class LoadConfigTests(unittest.TestCase):
    def setUp(self):
        self.priority = {"metadata": {"name": "test-low", "labels": {"task": TASK}}, "value": -10,
                         "preemptionPolicy": "Never", "globalDefault": False}
        self.database = {"jdbcUrl": "jdbc:postgresql://database.example.test:5432/gateway?sslmode=verify-full&sslrootcert=/etc/database-ca/ca.pem",
                         "user": "synthetic", "password": "not-a-working-credential", "driver": "org.postgresql.Driver"}

    def test_private_tls_connection_preserves_keys_and_bounds_scaling(self):
        original = {"transactionAwareness": {"identityKey": "preserve"}, "dataStore": {"queryHistoryHoursRetention": 24}}
        config, network, deployment, quota = prepare(original, self.database, ["10.0.0.10"], 100, priority_class=self.priority)
        self.assertEqual(config["transactionAwareness"], original["transactionAwareness"])
        self.assertEqual(config["dataStore"]["queryHistoryHoursRetention"], 24)
        self.assertNotIn("password", original["dataStore"])
        self.assertEqual(network["spec"]["egress"][0]["to"], [{"ipBlock": {"cidr": "10.0.0.10/32"}}])
        self.assertEqual(deployment["spec"]["replicas"], 100)
        self.assertEqual(deployment["spec"]["template"]["spec"]["preemptionPolicy"], "Never")
        self.assertEqual(deployment["spec"]["template"]["spec"]["priorityClassName"], "test-low")
        self.assertEqual(quota["spec"]["hard"]["pods"], "120")

    def test_rejects_insecure_database_and_unbounded_network_or_replicas(self):
        for url in [self.database["jdbcUrl"].replace("verify-full", "require"), self.database["jdbcUrl"].replace("verify-full", "disable"), "jdbc:postgresql://user:secret@database/db",
                    self.database["jdbcUrl"] + "&sslfactory=unsafe", self.database["jdbcUrl"] + "&sslhostnameverifier=unsafe",
                    self.database["jdbcUrl"] + "&sslfactory=", self.database["jdbcUrl"] + "&sslmode=",
                    self.database["jdbcUrl"] + "&connectTimeout=1", self.database["jdbcUrl"] + "&socketTimeout=1",
                    self.database["jdbcUrl"] + "&tcpKeepAlive=true"]:
            with self.subTest(url=url), self.assertRaises(ValueError):
                prepare({}, dict(self.database, jdbcUrl=url), ["10.0.0.10"], 2, priority_class=self.priority)
        for addresses in [[], ["0.0.0.0"], ["127.0.0.1"], ["8.8.8.8"], ["10.0.0.0/8"]]:
            with self.subTest(addresses=addresses), self.assertRaises(ValueError):
                prepare({}, self.database, addresses, 2, priority_class=self.priority)
        for replicas in [1, 101, True]:
            with self.subTest(replicas=replicas), self.assertRaises(ValueError):
                prepare({}, self.database, ["10.0.0.10"], replicas, priority_class=self.priority)

    def test_rejects_missing_priority_class(self):
        with self.assertRaises(ValueError):
            prepare({}, self.database, ["10.0.0.10"], 2)


if __name__ == "__main__":
    unittest.main()
