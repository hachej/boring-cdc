import copy
import importlib.util
import json
import subprocess
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
        self.assertEqual(6, len(bundle))

    def test_fixture_to_executor_mapping_is_total(self):
        cases = self.fixtures["cases"]
        self.assertEqual(self.contract["fixture_ids"], [case["fixture_id"] for case in cases])
        self.assertEqual({case["executor_id"] for case in cases}, set(self.contract["executors"]))
        self.assertEqual({case["hook"] for case in cases}, set(self.contract["hooks"]))
        self.assertTrue(all(case["inputs"] and case["expected"]["status_code"] for case in cases))

    def test_creation_floor_requires_slot_and_wal_predicates(self):
        by_id = {case["fixture_id"]: case for case in self.fixtures["cases"]}
        for suffix in ("INVALID-SLOT", "WAL-UNAVAILABLE"):
            case = by_id[f"SCN-M0-PG-CREATION-FLOOR-{suffix}"]
            self.assertIsNone(case["inputs"]["durable_transaction_end_lsn"])
            self.assertEqual("requires_reseed", case["expected"]["state"])
            self.assertEqual("unchanged", case["expected"]["feedback"])

    def test_lifecycle_profiles_reject_generic_and_impossible_state(self):
        cases = copy.deepcopy(self.fixtures["cases"])
        before_slot = next(case for case in cases if case["fixture_id"] == "SCN-M0-PG-BEFORE-SLOT-CREATE")
        before_slot["inputs"]["pre_state"] = "fixture_precondition_ready"
        before_slot["inputs"]["fault_action"] = "inject_once_before_named_effect"
        before_slot["inputs"]["exporter_backend_pid"] = 4101
        before_slot["inputs"]["importer_acknowledged"] = 2
        before_slot["inputs"]["importer_expected"] = 2
        findings = []
        postgres_contract.validate_fixture_semantics(cases, findings)
        self.assertEqual(
            {"E_FIXTURE_CASE_DIGEST", "E_FIXTURE_INPUT_DIGEST", "E_FIXTURE_LIFECYCLE", "E_FIXTURE_PROCESS_STATE"},
            {item["code"] for item in findings},
        )

    def test_expected_outcomes_and_preconditions_are_exact(self):
        cases = copy.deepcopy(self.fixtures["cases"])
        guard_loss = next(case for case in cases if case["fixture_id"] == "SCN-M0-PG-GUARD-LOSS")
        guard_loss["expected"]["state"] = "anchor_complete"
        guard_loss["preconditions"] = ["anything"]
        findings = []
        postgres_contract.validate_fixture_semantics(cases, findings)
        self.assertIn("E_FIXTURE_CASE_DIGEST", {item["code"] for item in findings})

    def test_fixture_schema_is_discriminated_by_fixture_id(self):
        fixtures = copy.deepcopy(self.fixtures)
        fixtures["cases"][25]["inputs"]["pre_state"] = "replication_streaming"
        findings = []
        schema = postgres_contract.load(postgres_contract.FIXTURE_SCHEMA)
        postgres_contract.CORE.validate_schema_instance(
            fixtures,
            schema,
            findings,
            base=postgres_contract.FIXTURE_SCHEMA.parent,
            root=schema,
        )
        self.assertTrue(findings)

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

    def test_importers_remain_live_after_exporter_release(self):
        case = next(case for case in self.fixtures["cases"] if case["fixture_id"] == "SCN-M0-PG-EXPORTER-RELEASE-AFTER")
        self.assertNotIn("exporter_backend_pid", case["inputs"])
        self.assertEqual([4103, 4104], case["inputs"]["importer_backend_pids"])
        self.assertEqual(2, case["inputs"]["importer_acknowledged"])

    def test_forged_evidence_source_parent_is_rejected(self):
        from types import SimpleNamespace
        from unittest import mock

        inputs = postgres_contract.validate()[1]
        validator_sha = postgres_contract.hashlib.sha256(postgres_contract.VALIDATOR.read_bytes()).hexdigest()
        for candidate in ("HEAD", subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True).strip()):
            prior = {"inputs": inputs, "validator_sha256": validator_sha, "source_parent_git_commit": candidate}
            _, error = postgres_contract.resolve_source_parent(prior, inputs, validator_sha)
            self.assertIn("canonical full lowercase commit OID", error)
        prior = {"inputs": inputs, "validator_sha256": validator_sha, "source_parent_git_commit": "0" * 40}
        _, error = postgres_contract.resolve_source_parent(prior, inputs, validator_sha)
        self.assertIn("not an existing commit", error)
        candidate = "1" * 40
        prior["source_parent_git_commit"] = candidate
        head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True)
        with mock.patch.object(postgres_contract.subprocess, "check_output", side_effect=[head, candidate + "\n"]), mock.patch.object(postgres_contract.subprocess, "run", return_value=SimpleNamespace(returncode=1)):
            _, error = postgres_contract.resolve_source_parent(prior, inputs, validator_sha)
        self.assertIn("not an ancestor of HEAD", error)

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
