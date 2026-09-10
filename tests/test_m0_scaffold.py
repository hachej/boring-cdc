import json
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def run(*args):
    return subprocess.run(args, cwd=ROOT, text=True, capture_output=True, check=True)

class ScaffoldTests(unittest.TestCase):
    def test_static_scaffold_contract(self):
        out = json.loads(run("scripts/validate/m0_scaffold.sh").stdout)
        self.assertEqual(out["status"], "pass")
        self.assertFalse(out["findings"])

    def test_agent_helpers_are_deterministic_and_non_mutating(self):
        before = run("git", "status", "--porcelain").stdout
        for helper in ("verify", "handoff", "recover", "finish"):
            first = run(f"scripts/agent/{helper}", "boring-cdc-m0-scaffold").stdout
            second = run(f"scripts/agent/{helper}", "boring-cdc-m0-scaffold").stdout
            self.assertEqual(first, second)
            self.assertTrue(json.loads(first).get("read_only", True))
        self.assertEqual(run("git", "status", "--porcelain").stdout, before)

    def test_upstream_reader_implementations_are_not_duplicated(self):
        for helper in ("doctor", "next", "context", "impact"):
            self.assertIn("from agent_context import main", (ROOT / "scripts" / "agent" / helper).read_text())

    def test_compose_is_health_gated_and_has_one_connector(self):
        text = (ROOT / "compose.yaml").read_text()
        self.assertEqual(text.count("  connector:\n"), 1)
        self.assertEqual(text.count("condition: service_healthy"), 2)
        self.assertIn("restart: unless-stopped", text)
        self.assertIn("@sha256:", text)

    def test_all_provisional_authorities_are_explicit(self):
        contract = json.loads((ROOT / "contracts/scaffold/m0-scaffold.json").read_text())
        expected = {f"// M0-PROVISIONAL: boring-cdc-d-{x}" for x in ("security", "values", "keys", "failure-policy", "sqlite", "wal-cap", "compose")}
        self.assertEqual(set(contract["provisional_authorities"]), expected)

if __name__ == "__main__":
    unittest.main()
