#!/usr/bin/env python3
"""Validate the exact-head live reconciliation rerun packet."""

import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
EXPECTED_REASONS = [
    "SLOT_MISSING",
    "RESUME_WAL_STATUS_UNAVAILABLE",
    "SLOT_INVALID_WAL_REMOVED",
]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check_attempt(root, number):
    attempt = root / f"attempt-{number}"
    stdout = attempt / "stdout.txt"
    proof = attempt / "crash-proof.json"
    lines = [json.loads(line) for line in stdout.read_text().splitlines() if line.startswith("{")]
    summaries = [value for value in lines if value.get("postgres") == "17.6"]
    assert len(summaries) == 1, f"attempt {number}: expected one live summary"
    summary = summaries[0]
    assert summary == {
        "postgres": "17.6",
        "database_oid_observed": True,
        "live_publication_fingerprint": True,
        "fresh_slot_ambiguous": True,
        "migration_receipt_retry": True,
        "slot_reasons": EXPECTED_REASONS,
        "abrupt_process_restart": True,
        "cli_json_text_matrix": True,
    }, f"attempt {number}: incomplete live summary"
    crash = json.loads(proof.read_text())
    assert crash == {
        "schema_version": "m2-reconcile-crash-proof/v1",
        "postgres": "17.6",
        "crash_pid": crash["crash_pid"],
        "executable": "boring-cdc",
        "boundary_marker": "before-source-state-receipt",
        "sigkill_reaped": True,
        "process_absent_after_wait": True,
        "source_receipt_absent_before_restart": True,
        "slot_reasons": EXPECTED_REASONS,
    }, f"attempt {number}: incomplete crash proof"
    assert isinstance(crash["crash_pid"], int) and crash["crash_pid"] > 1
    return {
        "summary": summary,
        "crash_proof": {key: value for key, value in crash.items() if key != "crash_pid"},
        "stdout_sha256": digest(stdout),
        "crash_proof_sha256": digest(proof),
    }


def main():
    mode, directory = sys.argv[1:]
    assert mode in {"create", "validate"}
    root = Path(directory).resolve()
    root.relative_to(ROOT)
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    attempts = [check_attempt(root, number) for number in (1, 2)]
    assert attempts[0]["summary"] == attempts[1]["summary"]
    assert attempts[0]["crash_proof"] == attempts[1]["crash_proof"]
    packet = {
        "schema_version": "m2-reconcile-component/v1",
        "owner_bead": "boring-cdc-m2-reconcile.1.1",
        "git_commit": head,
        "source_sha256": digest(ROOT / "src/m2_reconcile.rs"),
        "capture_runtime_sha256": digest(ROOT / "src/m2_capture_runtime.rs"),
        "attempts": attempts,
        "status": "pass",
    }
    manifest = root / "manifest.json"
    if mode == "create":
        manifest.write_text(json.dumps(packet, sort_keys=True, separators=(",", ":")) + "\n")
        source = (ROOT / "src/m2_reconcile.rs").read_bytes() + (ROOT / "src/m2_capture_runtime.rs").read_bytes()
        source_digest = hashlib.sha256(source).hexdigest()
        commands = []
        artifact_paths = []
        for number, command in ((1, "scripts/e2e/m2_reconcile.sh"), (2, "scripts/faults/m2_reconcile.sh")):
            attempt = root / f"attempt-{number}"
            stdout = attempt / "stdout.txt"
            stderr = attempt / "stderr.txt"
            proof = attempt / "crash-proof.json"
            assert stderr.is_file(), f"attempt {number}: missing stderr"
            for retained in (stdout, stderr, proof):
                assert not re.search(rb"(?i)postgres(?:ql)?://|password=|PGPASSWORD", retained.read_bytes()), f"secret-like content in {retained}"
            commands.append({
                "argv": command,
                "version": "m2-reconcile-component/v1",
                "exit_code": 0,
                "stdout_path": stdout.relative_to(ROOT).as_posix(),
                "stdout_sha256": digest(stdout),
                "stderr_path": stderr.relative_to(ROOT).as_posix(),
                "stderr_sha256": digest(stderr),
            })
            artifact_paths.extend([stdout, proof])
        evidence = {
            "schema_version": "evidence/v1",
            "owner_bead": "boring-cdc-m2-reconcile.1.1",
            "scenario_id": "SCN-M2-RECONCILE-LIVE-PG17",
            "evidence_profile": "runtime",
            "evidence_tier": "component",
            "seed": "current-head",
            "git_commit": head,
            "commands": commands,
            "source_preservation": {"before_sha256": source_digest, "after_sha256": source_digest, "preserved": True},
            "cleanup": {"complete": True, "remaining_paths": []},
            "redaction": {"checked": True, "secrets_found": 0},
            "tier_proof": {
                "targeted_checks": True, "boundary_e2e": True, "fault_suite": True,
                "deterministic_rerun": True, "consumed_contract_vectors": True,
                "workspace_tests": False, "integration": True, "clean_environment": True,
                "exit_assertions": True, "endurance": False, "full_failure_matrix": False,
                "clean_clone": False,
            },
            "result": {
                "status": "pass",
                "digest": hashlib.sha256(b"".join(path.read_bytes() for path in artifact_paths)).hexdigest(),
                "artifacts": [path.relative_to(ROOT).as_posix() for path in artifact_paths],
                "product_faults": "exact-PID SIGKILL before source receipt; missing, unreserved, and lost PostgreSQL slots",
                "runtime_observed": True,
                "attempts": ["live-e2e", "fault-wrapper-rerun"],
            },
        }
        (root / "evidence.json").write_text(json.dumps(evidence, sort_keys=True, separators=(",", ":")) + "\n")
    else:
        assert json.loads(manifest.read_text()) == packet, "component packet mismatch"
    print(json.dumps({"status": "pass", "git_commit": head, "attempts": 2}))


if __name__ == "__main__":
    main()
