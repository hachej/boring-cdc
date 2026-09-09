#!/usr/bin/env python3
"""Hostile closure tests for the public repository identity fixture."""
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FAIL = '{"code":"PUBLIC_OWNER_FIXTURE_INVALID","outcome":"fail","phase":"validate_spec"}\n'
OWNER_DIR = "artifacts/m0/decisions/boring-cdc-d-owner"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical_write(path, value):
    path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")


class RepositoryIdentityClosureTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="boring-cdc-owner-hostile."))
        self.checkout = self.tmp / "checkout"
        subprocess.run(
            ["git", "worktree", "add", "--detach", str(self.checkout), "HEAD"],
            cwd=ROOT, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

    def tearDown(self):
        subprocess.run(
            ["git", "worktree", "remove", "--force", str(self.checkout)],
            cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_validator(self):
        result = subprocess.run(
            ["scripts/fixtures/validate_m0_repository_identity.py"],
            cwd=self.checkout, text=True, capture_output=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, FAIL)
        self.assertEqual(result.stderr, "")

    def update_artifact_hash(self, artifact_id, path):
        manifest_path = self.checkout / "contracts/m0/artifacts.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        next(row for row in manifest["artifacts"] if row["id"] == artifact_id)["sha256"] = digest(path)
        canonical_write(manifest_path, manifest)

    def test_coherent_probe_and_hash_drift_is_rejected(self):
        probe_path = self.checkout / OWNER_DIR / "fixture-run.jsonl"
        hostile = [
            {"code": "PUBLIC_OWNER_FIXTURE_VALID", "outcome": "pass", "phase": "validate_spec"},
            {"code": "UNRELATED_SUCCESS", "outcome": "pass", "phase": "observe"},
        ]
        probe_path.write_text("".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in hostile), encoding="utf-8")
        spec_path = self.checkout / "fixtures/m0/decisions/boring-cdc-d-owner.json"
        spec = json.loads(spec_path.read_text(encoding="utf-8"))
        spec["execution_probe"]["expected_lines"] = hostile
        spec["execution_probe"]["sha256"] = digest(probe_path)
        canonical_write(spec_path, spec)
        spec_sha = digest(spec_path)

        registry_path = self.checkout / "contracts/agent/stable-ids.json"
        registry = json.loads(registry_path.read_text(encoding="utf-8"))
        registry["source_files"]["fixtures/m0/decisions/boring-cdc-d-owner.json"] = spec_sha
        canonical_write(registry_path, registry)
        provenance_path = self.checkout / "contracts/coverage/plan-to-beads.provenance.json"
        provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
        provenance["source_digests"]["contracts/agent/stable-ids.json"] = digest(registry_path)
        canonical_write(provenance_path, provenance)
        decisions_path = self.checkout / "contracts/m0/decisions.json"
        decisions = json.loads(decisions_path.read_text(encoding="utf-8"))
        next(row for row in decisions["decisions"] if row["id"] == "DEC-PUBLIC-OWNER")["fixture_sha256"] = spec_sha
        canonical_write(decisions_path, decisions)
        self.update_artifact_hash("ART-M0-PUBLIC-OWNER-FIXTURE", spec_path)
        self.update_artifact_hash("ART-M0-PUBLIC-OWNER-PROBE", probe_path)
        evidence_path = self.checkout / OWNER_DIR / "evidence.json"
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
        evidence["input_sha256"] = digest(decisions_path)
        canonical_write(evidence_path, evidence)
        self.update_artifact_hash("ART-M0-PUBLIC-OWNER-VALIDATION", evidence_path)
        self.run_validator()

    def test_non_ancestor_evidence_commit_with_identical_tree_is_rejected(self):
        env = os.environ | {
            "GIT_AUTHOR_NAME": "hostile-test", "GIT_AUTHOR_EMAIL": "hostile@example.invalid",
            "GIT_COMMITTER_NAME": "hostile-test", "GIT_COMMITTER_EMAIL": "hostile@example.invalid",
            "GIT_AUTHOR_DATE": "2000-01-01T00:00:00Z", "GIT_COMMITTER_DATE": "2000-01-01T00:00:00Z",
        }
        tree = subprocess.check_output(["git", "rev-parse", "HEAD^{tree}"], cwd=self.checkout, text=True).strip()
        hostile_sha = subprocess.check_output(
            ["git", "commit-tree", tree, "-m", "non-ancestor identical tree"],
            cwd=self.checkout, env=env, text=True,
        ).strip()
        evidence_path = self.checkout / OWNER_DIR / "evidence.json"
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
        evidence["git_commit"] = hostile_sha
        canonical_write(evidence_path, evidence)
        self.update_artifact_hash("ART-M0-PUBLIC-OWNER-VALIDATION", evidence_path)
        self.run_validator()


if __name__ == "__main__":
    unittest.main()
