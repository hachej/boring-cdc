import importlib.util
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "m0_completeness", ROOT / "scripts/validate/m0_completeness.py"
)
m0 = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(m0)


class M0CompletenessTests(unittest.TestCase):
    def test_current_aggregate_manifests_have_complete_unique_owners(self):
        self.assertEqual([], m0.aggregate_findings(ROOT))

    def fixture_root(self):
        temporary = tempfile.TemporaryDirectory()
        target = Path(temporary.name)
        paths = {
            Path("contracts/m0/manifest.json"),
            Path("contracts/m0/artifacts.json"),
            Path("contracts/m0/decisions.json"),
            Path("contracts/m0/expected-artifacts.json"),
            Path(".beads/issues.jsonl"),
        }
        artifacts = json.loads((ROOT / "contracts/m0/artifacts.json").read_text())["artifacts"]
        paths.update(Path(row["path"]) for row in artifacts)
        for _, owner in m0.PRIMARY_ARTIFACTS.values():
            if owner != "boring-cdc-m0-scaffold":
                paths.add(Path("artifacts") / owner / "spec/evidence.json")
        for path in paths:
            destination = target / path
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / path, destination)
        return temporary, target

    def test_duplicate_bead_and_manifest_owner_drift_fail_closed(self):
        temporary, target = self.fixture_root()
        self.addCleanup(temporary.cleanup)
        graph = target / ".beads/issues.jsonl"
        first = graph.read_text().splitlines()[0]
        graph.write_text(graph.read_text() + first + "\n")
        manifest = target / "contracts/m0/manifest.json"
        value = json.loads(manifest.read_text())
        value["artifacts"][0]["owner_bead"] = "boring-cdc-m0-scaffold"
        manifest.write_text(json.dumps(value, sort_keys=True) + "\n")
        findings = m0.aggregate_findings(target)
        self.assertTrue(any(item.startswith("duplicate Bead IDs") for item in findings), findings)
        self.assertTrue(any("primary owner mismatch" in item for item in findings), findings)

    def test_supporting_artifact_cannot_disappear_from_registry(self):
        temporary, target = self.fixture_root()
        self.addCleanup(temporary.cleanup)
        registry = target / "contracts/m0/artifacts.json"
        value = json.loads(registry.read_text())
        value["artifacts"] = [row for row in value["artifacts"] if row["id"] != "ART-M0-PG-VALIDATOR"]
        registry.write_text(json.dumps(value, sort_keys=True) + "\n")
        expected = target / "contracts/m0/expected-artifacts.json"
        expected.write_text(json.dumps([item for item in json.loads(expected.read_text()) if item != "ART-M0-PG-VALIDATOR"]) + "\n")
        findings = m0.aggregate_findings(target)
        self.assertIn("expected artifact inventory digest mismatch", findings)

    def test_evidence_cannot_omit_its_input_inventory(self):
        temporary, target = self.fixture_root()
        self.addCleanup(temporary.cleanup)
        evidence = target / "artifacts/boring-cdc-m0-event-format/spec/evidence.json"
        value = json.loads(evidence.read_text())
        value["inputs"] = {}
        evidence.write_text(json.dumps(value, sort_keys=True) + "\n")
        self.assertIn("boring-cdc-m0-event-format: evidence input provenance mismatch", m0.aggregate_findings(target))

    def test_approved_manifest_rows_cannot_retain_provisional_markers(self):
        temporary, target = self.fixture_root()
        self.addCleanup(temporary.cleanup)
        decisions = target / "contracts/m0/decisions.json"
        value = json.loads(decisions.read_text())
        row = next(row for row in value["decisions"] if row["owner_bead"] == "boring-cdc-d-compose")
        row["provisional_markers"] = ["// M0-PROVISIONAL: boring-cdc-d-compose"]
        decisions.write_text(json.dumps(value, sort_keys=True) + "\n")
        findings = m0.aggregate_findings(target)
        self.assertIn("boring-cdc-d-compose: approved decision retains provisional marker", findings)

    def test_every_decision_domain_validator_is_fail_closed(self):
        expected = {
            "boring-cdc-d-archive-scope": ("scripts/validate/archive_scope.sh",),
            "boring-cdc-d-compose": ("scripts/validate/compose_spec.sh",),
            "boring-cdc-d-failure-policy": ("scripts/validate/failure_policy.sh",),
            "boring-cdc-d-license": ("scripts/validate/license.sh",),
            "boring-cdc-d-owner": ("scripts/fixtures/validate_m0_repository_identity.py",),
            "boring-cdc-d-security": ("scripts/validate/security_exposure.sh",),
            "boring-cdc-d-sqlite": ("scripts/validate/sqlite_durability.sh",),
            "boring-cdc-d-values": ("scripts/validate/supported_values.sh",),
            "boring-cdc-d-wal-cap": ("scripts/validate/wal_cap.sh",),
        }
        self.assertEqual(expected, m0.DECISION_DOMAIN_COMMANDS)
        original_commands = m0.DOMAIN_COMMANDS
        self.addCleanup(setattr, m0, "DOMAIN_COMMANDS", original_commands)
        for owner, required in expected.items():
            with self.subTest(owner=owner, failure="omitted"):
                m0.DOMAIN_COMMANDS = [command for command in original_commands if tuple(command) != required]
                with mock.patch.object(m0, "aggregate_findings", return_value=[]), \
                     mock.patch.object(m0.subprocess, "run", return_value=mock.Mock(returncode=0, stdout="", stderr="")):
                    result = m0.probe()
                self.assertEqual("fail", result["status"])
                self.assertIn(f"{owner}: decision-domain validator must execute exactly once", result["findings"])
            with self.subTest(owner=owner, failure="semantic"):
                m0.DOMAIN_COMMANDS = original_commands

                def run(command, **_kwargs):
                    return mock.Mock(
                        returncode=1 if tuple(command) == required else 0,
                        stdout="semantic failure" if tuple(command) == required else "",
                        stderr="",
                    )

                with mock.patch.object(m0, "aggregate_findings", return_value=[]), \
                     mock.patch.object(m0.subprocess, "run", side_effect=run):
                    result = m0.probe()
                self.assertEqual("fail", result["status"])
                self.assertTrue(any(item.startswith(f"command failed ({' '.join(required)})") for item in result["findings"]), result)


if __name__ == "__main__":
    unittest.main()
