import json
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts" / "lib"))
import m0_scaffold

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

    def test_owner_confirmed_contract_has_no_provisional_markers(self):
        paths = (ROOT / "contracts/scaffold/m0-scaffold.json", ROOT / "config/boring-cdc.schema.json")
        self.assertFalse(any("M0-" + "PROVISIONAL" in path.read_text() for path in paths))

    def test_capture_normalization_redacts_host_and_secret_paths(self):
        captured = (
            f"workspace={ROOT}/target/debug/boring-cdc "
            "/var/tmp/m0-scaffold-random/attempt-1/postgres_password "
            "postgresql://user:hunter2@db/source"
        ).encode()
        normalized = m0_scaffold.normalize_capture(captured).decode()
        self.assertNotIn(str(ROOT), normalized)
        self.assertNotIn("/var/tmp/", normalized)
        self.assertNotIn("hunter2", normalized)
        self.assertIn("<workspace>", normalized)
        self.assertIn("<redacted>", normalized)

    def test_evidence_digest_and_path_guards_fail_closed(self):
        evidence_root = ROOT / "artifacts" / "boring-cdc-m0-scaffold" / "scenario"
        self.assertTrue(m0_scaffold.is_sha256("a" * 64))
        self.assertFalse(m0_scaffold.is_sha256("not-a-digest"))
        self.assertTrue(m0_scaffold.contains_sensitive_absolute_path("/home/other/worktree/file"))
        self.assertTrue(m0_scaffold.contains_sensitive_absolute_path("/run/secrets/db-key"))
        self.assertFalse(m0_scaffold.contains_sensitive_absolute_path("/usr/local/bin/boring-cdc"))
        self.assertIsNone(m0_scaffold.evidence_path(evidence_root, "/etc/passwd"))
        self.assertIsNone(m0_scaffold.evidence_path(evidence_root, "Cargo.lock"))

    def test_m1_completion_rejects_reintroduced_provisional_marker(self):
        marker = ROOT / "config" / ".test-provisional-marker"
        marker.write_text("M0-" + "PROVISIONAL")
        try:
            result = subprocess.run(
                ["scripts/acceptance/m1_complete.sh", "--probe"],
                cwd=ROOT,
                text=True,
                capture_output=True,
            )
        finally:
            marker.unlink(missing_ok=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("provisional marker reintroduced", result.stdout)

    def test_m1_completion_rejects_marker_in_root_product_files(self):
        for relative in (".env.example", "rust-toolchain.toml", ".dockerignore"):
            with self.subTest(path=relative):
                path = ROOT / relative
                original = path.read_bytes()
                path.write_bytes(original + b"\nM0-" + b"PROVISIONAL\n")
                try:
                    result = subprocess.run(
                        ["scripts/acceptance/m1_complete.sh", "--probe"],
                        cwd=ROOT,
                        text=True,
                        capture_output=True,
                    )
                finally:
                    path.write_bytes(original)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(f"provisional marker reintroduced: {relative}", result.stdout)

if __name__ == "__main__":
    unittest.main()
