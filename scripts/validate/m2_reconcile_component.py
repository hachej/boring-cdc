#!/usr/bin/env python3
"""Validate the exact-head live reconciliation rerun packet."""

import hashlib
import json
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
    root = Path(directory)
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
    else:
        assert json.loads(manifest.read_text()) == packet, "component packet mismatch"
    print(json.dumps({"status": "pass", "git_commit": head, "attempts": 2}))


if __name__ == "__main__":
    main()
