import importlib.util
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "postgres_contract", ROOT / "scripts/validate/postgres_contract.py"
)
postgres_contract = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(postgres_contract)


class PostgresContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract = json.loads((ROOT / "contracts/postgres/capture-backfill.json").read_text())
        cls.fixtures = json.loads((ROOT / "fixtures/m0/postgres/capture-backfill.json").read_text())

    def test_complete_contract_and_fixtures_validate(self):
        findings, bundle = postgres_contract.validate()
        self.assertEqual([], findings)
        self.assertEqual(5, len(bundle))

    def test_fixture_to_executor_mapping_is_total(self):
        cases = self.fixtures["cases"]
        self.assertEqual(self.contract["fixture_ids"], [case["fixture_id"] for case in cases])
        self.assertTrue({case["executor_id"] for case in cases} <= set(self.contract["executors"]))
        self.assertTrue(all(case["hook"] and case["expected"]["status_code"] for case in cases))

    def test_creation_floor_requires_slot_and_wal_predicates(self):
        by_id = {case["fixture_id"]: case for case in self.fixtures["cases"]}
        for suffix in ("INVALID-SLOT", "WAL-UNAVAILABLE"):
            case = by_id[f"SCN-M0-PG-CREATION-FLOOR-{suffix}"]
            self.assertIsNone(case["inputs"]["durable_transaction_end_lsn"])
            self.assertEqual("requires_reseed", case["expected"]["state"])
            self.assertEqual("unchanged", case["expected"]["feedback"])

    def test_feedback_service_never_uses_received_or_keepalive_lsn(self):
        forbidden = set(self.contract["feedback"]["forbidden_inputs"])
        self.assertIn("primary keepalive wal_end", forbidden)
        self.assertIn("last_received_lsn", forbidden)
        self.assertEqual(
            "max(requested_lsn, server_confirmed_flush_lsn)",
            self.contract["feedback"]["effective_server_restart"],
        )

    def test_control_grants_are_column_limited(self):
        sql = (ROOT / "contracts/postgres/roles-grants.sql").read_text()
        self.assertIn("GRANT SELECT (id)", sql)
        self.assertIn("GRANT UPDATE (nonce, updated_at)", sql)
        self.assertNotIn("GRANT INSERT", sql)
        self.assertNotIn("GRANT DELETE", sql)

    def test_admitted_ddl_conflicts_with_guard(self):
        rows = self.contract["ddl_matrix"]
        self.assertGreaterEqual(len(rows), 10)
        self.assertTrue(all(row["minimum_lock"] == "ACCESS EXCLUSIVE" for row in rows))
        self.assertTrue(all(row["guard_conflicts"] for row in rows))

    def test_safe_stop_close_does_not_mask_ownership_loss(self):
        policy = self.contract["safe_stop_close"]
        self.assertEqual("ownership loss and nonzero exit", policy["advisory_loss_always"])
        self.assertEqual("forbidden in process", policy["reopen"])
        self.assertLessEqual(
            self.contract["feedback"]["service"]["ownership_probe_interval_ms"],
            self.contract["ownership"]["ownership_deadline_seconds"] * 1000,
        )


if __name__ == "__main__":
    unittest.main()
