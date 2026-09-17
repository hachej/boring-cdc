import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "fixtures/m0/decisions/boring-cdc-d-archive-durability.json"
POLICY = ROOT / "contracts/m0/failure-policy.json"


class ArchiveDurabilityTests(unittest.TestCase):
    def setUp(self):
        self.fixture = json.loads(FIXTURE.read_text())
        self.audit = self.fixture["confirmed_boundary"]["audit"]
        self.vectors = self.fixture["vectors"]

    def test_budget_boundaries_are_concrete_and_exact(self):
        cases = {
            "byte_budget_boundary": ("bytes", "max_bytes_per_pass"),
            "event_budget_boundary": ("events", "max_events_per_pass"),
            "time_budget_boundary": ("elapsed_ms", "max_milliseconds_per_pass"),
        }
        for case_id, (field, limit) in cases.items():
            with self.subTest(case_id=case_id):
                self.assertEqual(self.audit[limit], self.vectors[case_id]["inputs"][field])
                self.assertEqual("pass", self.vectors[case_id]["expected_outcome"])

    def test_oversized_resume_is_bounded_and_content_stateful(self):
        inputs = self.vectors["oversized_unit_resume"]["inputs"]
        self.assertGreater(inputs["unit_bytes"], self.audit["max_bytes_per_pass"])
        self.assertEqual(self.audit["max_bytes_per_pass"], inputs["pass_bytes"])
        self.assertEqual(inputs["pass_bytes"], inputs["resume_offset"])
        self.assertEqual(1, inputs["hash_state_version"])

    def test_freshness_and_inadequate_service_are_hostile(self):
        expired = self.vectors["freshness_expired"]["inputs"]
        self.assertEqual(self.audit["freshness_seconds"] + 1, expired["last_complete_age_seconds"])
        inadequate = self.vectors["inadequate_service_three_misses"]["inputs"]
        self.assertEqual(self.audit["inadequate_service_after_consecutive_missed_cadences"], inadequate["missed_cadences"])
        self.assertEqual(self.audit["freshness_seconds"], inadequate["missed_cadences"] * inadequate["cadence_seconds"])

    def test_identity_change_cannot_resume_old_cursor(self):
        inputs = self.vectors["identity_change_restart"]["inputs"]
        self.assertNotEqual(inputs["generation"], inputs["new_generation"])
        self.assertNotEqual(inputs["selector_sha256"], inputs["new_selector_sha256"])
        self.assertGreater(inputs["partial_cursor"]["byte_offset"], 0)
        self.assertEqual("pass_restart_round", self.vectors["identity_change_restart"]["expected_outcome"])

    def test_failure_projection_matches_canonical_archive_outputs(self):
        policy = json.loads(POLICY.read_text())
        outputs = {
            row["class"]: row["output"]
            for row in policy["domain_hooks"]["exhaustive_class_component_outputs"]
            if row["component"] == "archive"
        }
        mappings = self.fixture["confirmed_boundary"]["failure_mapping"]
        self.assertEqual(
            {"transient_io", "rate_limited", "configuration", "unsupported", "integrity"},
            {row["class"] for row in mappings.values()},
        )
        for name, row in mappings.items():
            with self.subTest(name=name):
                self.assertEqual(outputs[row["class"]], row["failure_policy_output"])


if __name__ == "__main__":
    unittest.main()
