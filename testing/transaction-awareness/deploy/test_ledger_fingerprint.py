"""Synthetic reference-codec and PostgreSQL checks for aggregate ledger evidence."""

import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
import subprocess
import unittest
import uuid


SQL = Path(__file__).with_name("ledger_fingerprint.sql").read_text()


def reference(rows, fields):
    encoded = bytearray()
    for row in sorted(rows, key=lambda value: value[fields[0]].encode("ascii")):
        for field in fields:
            value = row[field].encode("ascii")
            encoded.extend(str(len(value)).encode("ascii") + b":" + value)
    return {"count": len(rows), "sha256": hashlib.sha256(encoded).hexdigest()}


class FingerprintCodecTests(unittest.TestCase):
    def test_field_lengths_avoid_concatenation_ambiguity(self):
        self.assertNotEqual(reference([{"a": "1", "b": "23"}], ["a", "b"]),
                            reference([{"a": "12", "b": "3"}], ["a", "b"]))

    def test_empty_and_ascii_order_are_deterministic(self):
        self.assertEqual(reference([], ["id"]), {"count": 0, "sha256": hashlib.sha256(b"").hexdigest()})
        self.assertEqual(reference([{"id": "q2"}, {"id": "q10"}], ["id"]),
                         {"count": 2, "sha256": hashlib.sha256(b"3:q102:q2").hexdigest()})

    def test_reference_rejects_non_ascii(self):
        with self.assertRaises(UnicodeEncodeError):
            reference([{"id": "caf\u00e9"}], ["id"])


@unittest.skipUnless(os.getenv("TX_OBSERVER_LOCAL_PG") == "yes", "Requires an explicitly owned local PostgreSQL fixture")
class FingerprintPostgresTests(unittest.TestCase):
    def setUp(self):
        self.assertIn(os.environ.get("PGHOST", ""), ("127.0.0.1", "localhost", "::1"), "Use only the owned local test database")
        self.schema = "fingerprint_test_" + uuid.uuid4().hex
        self.environment = dict(os.environ, PGOPTIONS="-c search_path=" + self.schema, PGAPPNAME="synthetic-fingerprint-test")
        self.psql("CREATE SCHEMA " + self.schema)
        migrations = Path(__file__).resolve().parents[3] / "gateway-ha/src/main/resources/postgresql"
        for version in (5, 6, 7, 8):
            self.psql(next(migrations.glob("V" + str(version) + "__*.sql")).read_text())
        self.first = "00000000-0000-0000-0000-000000000001"
        self.second = "00000000-0000-0000-0000-000000000002"
        self.psql("""
            INSERT INTO transaction_backend(incarnation,name,current_name,backend_url,external_url,routing_group,state)
            VALUES ('00000000-0000-0000-0000-000000000001','first','first','http://first.test','http://first.test','synthetic','ACTIVE'),
                   ('00000000-0000-0000-0000-000000000002','second','second','http://second.test','http://second.test','synthetic','ACTIVE');
            INSERT INTO transaction_binding(transaction_id,owner_hash,incarnation,start_query_id,state)
            VALUES ('tx_1','synthetic-owner','00000000-0000-0000-0000-000000000001','q10','OPEN');
            INSERT INTO transaction_query(query_id,owner_hash,incarnation)
            VALUES ('q2','synthetic-owner','00000000-0000-0000-0000-000000000001'),
                   ('q10','synthetic-owner','00000000-0000-0000-0000-000000000001'),
                   ('q9','synthetic-owner','00000000-0000-0000-0000-000000000002');
            INSERT INTO transaction_admission(admission_id,incarnation,owner_hash,state)
            VALUES ('00000000-0000-0000-0000-000000000010','00000000-0000-0000-0000-000000000001','synthetic-owner','PENDING'),
                   ('00000000-0000-0000-0000-000000000020','00000000-0000-0000-0000-000000000002','synthetic-owner','UNCERTAIN');
        """)

    def tearDown(self):
        self.psql("DROP SCHEMA " + self.schema + " CASCADE")

    def psql(self, sql):
        result = subprocess.run(["psql", "-XqAtw", "-v", "ON_ERROR_STOP=1", "-f", "-"],
                                input=sql, env=self.environment, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.strip()

    def snapshot(self):
        return json.loads(self.psql("BEGIN READ ONLY;\n" + SQL + "\nROLLBACK;"))

    def test_actual_postgres_matches_reference_without_exporting_identifiers(self):
        row = self.snapshot()
        queries = [{"query_id": name, "incarnation": owner, "current_name": backend} for name, owner, backend in
                   [("q2", self.first, "first"), ("q10", self.first, "first"), ("q9", self.second, "second")]]
        admissions = [{"admission_id": "00000000-0000-0000-0000-000000000010", "incarnation": self.first, "state": "PENDING"},
                      {"admission_id": "00000000-0000-0000-0000-000000000020", "incarnation": self.second, "state": "UNCERTAIN"}]
        self.assertTrue(row["ascii_valid"])
        self.assertEqual(row["nonterminal_queries"], reference(queries, ["query_id", "incarnation", "current_name"]))
        self.assertEqual(row["pending_admissions"], reference(admissions, ["admission_id", "incarnation", "state"]))
        self.assertEqual(row["open_transactions"], reference([{"transaction_id": "tx_1", "incarnation": self.first, "state": "OPEN"}], ["transaction_id", "incarnation", "state"]))
        expected_groups = [{"incarnation_hash": hashlib.sha256(owner.encode()).hexdigest(), **reference([query for query in queries if query["incarnation"] == owner], ["query_id", "incarnation", "current_name"])} for owner in (self.first, self.second)]
        self.assertEqual(row["query_groups"], sorted(expected_groups, key=lambda item: item["incarnation_hash"]))
        backend_rows = [{"current_name": "first", "incarnation": self.first}, {"current_name": "second", "incarnation": self.second}]
        self.assertEqual(row["backend_set_sha256"], reference(backend_rows, ["current_name", "incarnation"])["sha256"])
        serialized = json.dumps(row)
        for private_value in ("q10", "q2", "q9", "tx_1", self.first, self.second, "synthetic-owner"):
            self.assertNotIn(private_value, serialized)

    def test_lifecycle_changes_do_not_change_identity_fingerprint(self):
        before = self.snapshot()
        self.psql("UPDATE transaction_backend SET state='DRAINING',generation=generation+1 WHERE current_name='first'")
        after = self.snapshot()
        self.assertEqual(before["backend_set_sha256"], after["backend_set_sha256"])
        self.assertNotEqual(before["backends"], after["backends"])

    def test_observation_timestamp_is_the_statement_snapshot_start(self):
        wrapped = "WITH sample(value) AS MATERIALIZED (" + SQL.rstrip().removesuffix(";") + ") SELECT jsonb_build_object('snapshot', (SELECT value FROM sample), 'statement_start', statement_timestamp());"
        result = json.loads(self.psql("BEGIN READ ONLY; SET LOCAL TIME ZONE 'Pacific/Auckland';\n" + wrapped + "\nROLLBACK;"))
        self.assertNotEqual(datetime.fromisoformat(result["statement_start"]).utcoffset().total_seconds(), 0)
        self.assertTrue(result["snapshot"]["sample_time"].endswith("Z"))
        self.assertEqual(datetime.fromisoformat(result["snapshot"]["sample_time"].replace("Z", "+00:00")),
                         datetime.fromisoformat(result["statement_start"]))

    def test_empty_sets_use_sha256_of_empty_bytes(self):
        self.psql("DELETE FROM transaction_admission; DELETE FROM transaction_query; DELETE FROM transaction_binding;")
        row = self.snapshot()
        self.assertTrue(row["ascii_valid"])
        for name in ("nonterminal_queries", "pending_admissions", "open_transactions"):
            self.assertEqual(row[name], {"count": 0, "sha256": hashlib.sha256(b"").hexdigest()})
        self.assertEqual(row["query_groups"], [])
        self.assertEqual(row["retained_terminal_counts"], [])

    def test_query_and_uncertainty_changes_are_visible(self):
        before = self.snapshot()
        self.psql("UPDATE transaction_query SET terminal=true,retain_until=clock_timestamp()+interval '1 hour' WHERE query_id='q2'; UPDATE transaction_admission SET state='UNCERTAIN' WHERE state='PENDING'")
        after = self.snapshot()
        self.assertNotEqual(before["nonterminal_queries"], after["nonterminal_queries"])
        self.assertEqual(before["pending_admissions"]["count"], after["pending_admissions"]["count"])
        self.assertNotEqual(before["pending_admissions"]["sha256"], after["pending_admissions"]["sha256"])
        self.assertEqual(after["retained_terminal_counts"], [{"current_name": "first", "count": 1}])

    def test_null_historical_name_and_non_ascii_fail_closed(self):
        self.psql("UPDATE transaction_backend SET current_name=NULL WHERE current_name='second'")
        self.assertFalse(self.snapshot()["ascii_valid"])
        self.psql("UPDATE transaction_backend SET current_name='caf\u00e9' WHERE name='second'")
        self.assertFalse(self.snapshot()["ascii_valid"])


if __name__ == "__main__":
    unittest.main()
