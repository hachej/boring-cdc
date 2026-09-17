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
            "postgresql://user:"
            "hunter2@db/source"
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

    def test_scaffold_rejects_unauthorized_provisional_marker(self):
        marker = ROOT / "config" / ".test-provisional-marker"
        marker.write_text("// M0-" + "PROVISIONAL: boring-cdc-d-values\n")
        try:
            result = subprocess.run(
                ["scripts/validate/m0_scaffold.sh"], cwd=ROOT, text=True, capture_output=True
            )
        finally:
            marker.unlink(missing_ok=True)
        self.assertNotEqual(result.returncode, 0)
        findings = json.loads(result.stdout)["findings"]
        self.assertIn(
            "provisional-marker-unauthorized:config/.test-provisional-marker:boring-cdc-d-values",
            findings,
        )

    def test_scaffold_rejects_malformed_marker_in_authorized_path(self):
        marker = ROOT / "config" / ".test-authorized-marker"
        relative = str(marker.relative_to(ROOT))
        m0_scaffold.PROVISIONAL_AUTHORITIES[relative] = {"boring-cdc-d-values"}
        try:
            for value in (
                "// M0-" + "PROVISIONAL\n",
                "M0-" + "PROVISIONAL: boring-cdc-d-values\n",
                "# M0-" + "PROVISIONAL: boring-cdc-d-values\n",
                "// M0-" + "PROVISIONAL: boring-cdc-d-values/forged\n",
            ):
                marker.write_text(value)
                self.assertEqual(
                    [f"provisional-marker-malformed:{relative}"],
                    m0_scaffold.provisional_marker_errors([marker]),
                )
        finally:
            marker.unlink(missing_ok=True)
            m0_scaffold.PROVISIONAL_AUTHORITIES.pop(relative, None)

    def test_scaffold_rejects_forged_template_suffix(self):
        marker = ROOT / "config" / ".test-template-marker"
        relative = str(marker.relative_to(ROOT))
        m0_scaffold.PROVISIONAL_AUTHORITIES[relative] = {"boring-cdc-d-values"}
        m0_scaffold.PROVISIONAL_TEMPLATE_PATHS.add(relative)
        try:
            marker.write_text("// M0-" + "PROVISIONAL: {decision}/forged\n")
            self.assertEqual(
                [f"provisional-marker-malformed:{relative}"],
                m0_scaffold.provisional_marker_errors([marker]),
            )
        finally:
            marker.unlink(missing_ok=True)
            m0_scaffold.PROVISIONAL_AUTHORITIES.pop(relative, None)
            m0_scaffold.PROVISIONAL_TEMPLATE_PATHS.discard(relative)

    def test_secret_scan_does_not_exempt_mixed_password_file_line(self):
        path = ROOT / "compose.yaml"
        original = path.read_bytes()
        hostile = b"\n# POSTGRES_PASSWORD_FILE: /run/secrets/postgres_password " + b"pass" + b"word='creden" + b"tial'\n"
        path.write_bytes(original + hostile)
        try:
            result = subprocess.run(
                ["scripts/validate/scaffold_secrets.sh"], cwd=ROOT, text=True, capture_output=True
            )
        finally:
            path.write_bytes(original)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("compose.yaml", result.stderr)

    def test_secret_scan_does_not_exempt_modified_synthetic_fixture_lines(self):
        cases = (
            ("tests/test_context.py", "postgres" + "://user:supersecret@example.invalid/db"),
            ("tests/validate_knowledge.py", "postgresql" + "://user:pw@host/db"),
        )
        for relative, token in cases:
            with self.subTest(path=relative):
                path = ROOT / relative
                original = path.read_bytes()
                text = original.decode()
                self.assertIn(token, text)
                path.write_text(text.replace(token, token + " pass" + "word='credential'", 1))
                try:
                    result = subprocess.run(
                        ["scripts/validate/scaffold_secrets.sh"],
                        cwd=ROOT,
                        text=True,
                        capture_output=True,
                    )
                finally:
                    path.write_bytes(original)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(relative, result.stderr)

    def test_package_excludes_internal_metadata_and_evidence(self):
        out = json.loads(run("scripts/validate/scaffold_package.sh").stdout)
        self.assertEqual(out["status"], "pass")
        self.assertFalse(out["sensitive_repository_metadata"])

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
