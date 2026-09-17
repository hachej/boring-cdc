import json
import os
import subprocess
import sys
import unittest
from unittest.mock import patch
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

    def test_clean_pull_verifies_raw_manifests_before_build(self):
        source = (ROOT / "scripts/lib/m0_scaffold.py").read_text()
        invocation = 'run_record(["python3","scripts/validate/compose_manifests.py"],proof,clean_env)'
        verify = source.index(invocation)
        build = source.index('["docker","buildx","create"')
        start = source.index('["docker","compose","-f","compose.yaml","up"')
        self.assertLess(verify, build)
        self.assertLess(verify, start)
        self.assertNotIn(invocation[:-1] + ',78)', source)
        self.assertIn('clean_env=isolated_clean_environment(cli_config,proof_root,author_timestamp,os.environ)', source)
        self.assertIn('"--build-arg",f"SOURCE_DATE_EPOCH={author_timestamp}"', source)

    def test_outer_launcher_environment_ignores_hostile_ambient_values(self):
        hostile = {
            "DOCKER_CONFIG": "/attacker/docker", "DOCKER_HOST": "tcp://attacker",
            "DOCKER_TLS_VERIFY": "1", "DOCKER_CERT_PATH": "/attacker/certs",
            "BUILDKIT_HOST": "tcp://attacker", "COMPOSE_FILE": "attacker.yml",
            "COMPOSE_PROFILES": "attacker", "DOCKER_CONTEXT": "attacker",
            "DOCKER_DEFAULT_PLATFORM": "linux/arm64", "HTTP_PROXY": "http://attacker",
            "HTTPS_PROXY": "http://attacker", "NO_PROXY": "", "ALL_PROXY": "http://attacker",
            "CARGO_HOME": "/attacker/cargo", "RUSTUP_HOME": "/attacker/rustup",
            "RUSTFLAGS": "-C target-cpu=native", "PATH": "/attacker/bin",
        }
        outer = m0_scaffold.outer_launcher_environment(
            Path("/isolated/docker"), Path("/isolated/home"), hostile
        )
        self.assertEqual(m0_scaffold.OUTER_LAUNCH_ENVIRONMENT_KEYS, set(outer))
        self.assertEqual("/isolated/docker", outer["DOCKER_CONFIG"])
        self.assertEqual("/isolated/home", outer["HOME"])
        self.assertEqual("/usr/bin:/bin", outer["PATH"])
        self.assertFalse(set(hostile) - {"DOCKER_CONFIG", "PATH"} & set(outer))
        source = (ROOT / "scripts/lib/m0_scaffold.py").read_text()
        self.assertIn("outer=outer_launcher_environment(cli_config,launcher_home,os.environ)", source)
        self.assertIn("docker=trusted_docker_executable()", source)

    def test_execute_uses_isolated_outer_launcher_under_hostile_ambient(self):
        hostile = {
            "DOCKER_CONFIG": "/attacker/docker", "DOCKER_HOST": "tcp://attacker",
            "DOCKER_TLS_VERIFY": "1", "DOCKER_CERT_PATH": "/attacker/certs",
            "BUILDKIT_HOST": "tcp://attacker", "COMPOSE_FILE": "attacker.yml",
            "HTTP_PROXY": "http://attacker", "HTTPS_PROXY": "http://attacker",
            "CARGO_HOME": "/attacker/cargo", "RUSTUP_HOME": "/attacker/rustup",
            "RUSTFLAGS": "-C target-cpu=native", "PATH": "/attacker/bin",
        }
        observed = {}

        def stop_at_first_launch(argv, **kwargs):
            observed["argv"] = argv
            observed["env"] = kwargs["env"]
            raise RuntimeError("launch observed")

        def fake_git(*args):
            return "a" * 40 if args == ("rev-parse", "HEAD") else "1789654678"

        with patch.dict(os.environ, hostile, clear=True), \
             patch.object(m0_scaffold, "validate", return_value=[]), \
             patch.object(m0_scaffold, "source_snapshot", return_value="source-digest"), \
             patch.object(m0_scaffold, "git", side_effect=fake_git), \
             patch.object(m0_scaffold, "trusted_docker_executable", return_value="/usr/bin/docker"), \
             patch.object(m0_scaffold.subprocess, "run", side_effect=stop_at_first_launch):
            with self.assertRaisesRegex(RuntimeError, "launch observed"):
                m0_scaffold.execute(Path("unused"))

        self.assertEqual(["/usr/bin/docker", "rm", "-f", "m0-scaffold-dind-aaaaaaaaaaaa"], observed["argv"])
        self.assertEqual(m0_scaffold.OUTER_LAUNCH_ENVIRONMENT_KEYS, set(observed["env"]))
        self.assertEqual("/usr/bin:/bin", observed["env"]["PATH"])
        self.assertNotIn("DOCKER_HOST", observed["env"])
        self.assertNotIn("HTTP_PROXY", observed["env"])
        self.assertTrue(observed["env"]["DOCKER_CONFIG"].startswith("/var/tmp/m0-scaffold-"))

    def test_clean_environment_ignores_all_ambient_values(self):
        hostile = {
            "DOCKER_HOST": "tcp://attacker", "COMPOSE_FILE": "attacker.yml",
            "HTTP_PROXY": "http://attacker", "RUSTFLAGS": "-C target-cpu=native",
            "CARGO_HOME": "/attacker/cargo", "RUSTUP_HOME": "/attacker/rustup",
            "PATH": "/attacker/bin", "SOURCE_DATE_EPOCH": "0",
        }
        clean = m0_scaffold.isolated_clean_environment(Path("/isolated/docker"), Path("/isolated/home"), "1789654678", hostile)
        self.assertEqual(m0_scaffold.CLEAN_ENVIRONMENT_KEYS, set(clean))
        self.assertEqual("/usr/bin:/bin", clean["PATH"])
        self.assertEqual("1789654678", clean["SOURCE_DATE_EPOCH"])
        self.assertFalse(set(hostile) - {"PATH", "SOURCE_DATE_EPOCH"} & set(clean))

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
        self.assertIn("provisional-marker-malformed", result.stdout)

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
                self.assertIn(f"provisional-marker-malformed:{relative}", result.stdout)

    def test_m1_completion_rejects_unauthorized_marker_in_artifacts(self):
        """Generated evidence is part of the completion surface and must not
        become a blind spot for unauthorized provisional markers."""
        marker = ROOT / "artifacts" / ".test-provisional-marker"
        marker.write_text("// M0-" + "PROVISIONAL: boring-cdc-d-values\n")
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
        self.assertIn(
            "provisional-marker-unauthorized:artifacts/.test-provisional-marker:boring-cdc-d-values",
            result.stdout,
        )

    def test_m1_completion_rejects_unauthorized_decision_marker(self):
        """A well-formed marker naming a decision the registry does not grant for
        that path must still fail. This is the half of the registry contract that
        a bare-token test cannot reach."""
        path = ROOT / "src" / "m2_spool.rs"
        original = path.read_bytes()
        marker = b"\n// M0-" + b"PROVISIONAL: boring-cdc-d-not-authorized-here\n"
        path.write_bytes(original + marker)
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
        self.assertIn(
            "provisional-marker-unauthorized:src/m2_spool.rs:boring-cdc-d-not-authorized-here",
            result.stdout,
        )

    def test_m1_completion_accepts_registry_authorized_markers(self):
        """The other half: markers the registry DOES grant must not be reported.
        The working tree carries supervisor-authorized M2 markers, so a clean probe
        must report no marker finding at all. Without this, the two rejection tests
        above would still pass if the check rejected every marker unconditionally,
        which is the behaviour this registry deliberately replaced."""
        self.assertIn(
            "M0-" + "PROVISIONAL",
            (ROOT / "src" / "m2_spool.rs").read_text(),
            "fixture precondition: an authorized marker must exist in the tree",
        )
        result = subprocess.run(
            ["scripts/acceptance/m1_complete.sh", "--probe"],
            cwd=ROOT,
            text=True,
            capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout)
        findings = json.loads(result.stdout).get("findings", [])
        self.assertEqual(
            [], [f for f in findings if "provisional-marker" in str(f)], findings
        )

if __name__ == "__main__":
    unittest.main()
