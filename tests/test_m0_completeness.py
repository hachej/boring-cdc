import importlib.util
import json
import shutil
import tempfile
import unittest
from pathlib import Path

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

    def test_provisional_manifest_rows_cannot_claim_approval(self):
        temporary, target = self.fixture_root()
        self.addCleanup(temporary.cleanup)
        decisions = target / "contracts/m0/decisions.json"
        value = json.loads(decisions.read_text())
        row = next(row for row in value["decisions"] if row["owner_bead"] == "boring-cdc-d-compose")
        row["status"] = "approved"
        row["approval"] = {"approved_by": "forged", "approved_at": "now", "value_digest": "0" * 64}
        decisions.write_text(json.dumps(value, sort_keys=True) + "\n")
        findings = m0.aggregate_findings(target)
        self.assertIn("boring-cdc-d-compose: provisional decision state/marker mismatch", findings)


if __name__ == "__main__":
    unittest.main()
