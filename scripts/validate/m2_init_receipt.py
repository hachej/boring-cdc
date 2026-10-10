#!/usr/bin/env python3
"""Validate the retained live M2 init receipt against its exact source tree."""

import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
INPUTS = (
    "src/m2_init_recovery.rs",
    "src/m2_capture_runtime.rs",
    "scripts/e2e/m2_init_recovery.sh",
    "scripts/validate/m2_init_receipt.py",
    "contracts/m2/init-recovery-cases.json",
    "scripts/setup/durable_simple_prerequisites.sql",
)
REJECTIONS = {
    "missing-heartbeat-row": "M2_INIT_CONTROL_CARDINALITY_INVALID",
    "missing-fence-row": "M2_INIT_CONTROL_CARDINALITY_INVALID",
    "multiple-heartbeat-rows": "M2_INIT_CONTROL_CARDINALITY_INVALID",
    "wrong-heartbeat-key": "M2_INIT_CONTROL_CARDINALITY_INVALID",
    "weak-fence-lengths": "M2_INIT_CONTROL_LENGTH_CONSTRAINT_INVALID",
    "unvalidated-fence-length": "M2_INIT_CONTROL_LENGTH_CONSTRAINT_INVALID",
    "relation-set": "M2_INIT_PUBLICATION_RELATION_SET_MISMATCH",
    "owner": "M2_INIT_PUBLICATION_OWNER_MISMATCH",
    "publish-insert": "M2_INIT_PUBLICATION_PUBLISH_INSERT_MISMATCH",
    "publish-update": "M2_INIT_PUBLICATION_PUBLISH_UPDATE_MISMATCH",
    "publish-delete": "M2_INIT_PUBLICATION_PUBLISH_DELETE_MISMATCH",
    "publish-truncate": "M2_INIT_PUBLICATION_PUBLISH_TRUNCATE_MISMATCH",
    "privilege": "M2_INIT_CONTROL_PRIVILEGE_EXCESS",
    "heartbeat-insert": "M2_INIT_CONTROL_PRIVILEGE_EXCESS",
    "heartbeat-delete": "M2_INIT_CONTROL_PRIVILEGE_EXCESS",
    "heartbeat-key-update": "M2_INIT_CONTROL_PRIVILEGE_EXCESS",
    "capture_fences-insert": "M2_INIT_CONTROL_PRIVILEGE_EXCESS",
    "capture_fences-delete": "M2_INIT_CONTROL_PRIVILEGE_EXCESS",
    "capture_fences-key-update": "M2_INIT_CONTROL_PRIVILEGE_EXCESS",
    "heartbeat-value-update": "M2_INIT_CONTROL_PRIVILEGE_MISSING",
    "fence-value-update": "M2_INIT_CONTROL_PRIVILEGE_MISSING",
    "membership": "M2_INIT_CONTROL_ROLE_MEMBERSHIP_EXCESS",
}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate(receipt: dict, root: Path = ROOT) -> list[str]:
    findings = []
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    if receipt.get("schema_version") != "m2-init-live-receipt/v1":
        findings.append("E_SCHEMA")
    if receipt.get("git_commit") != head:
        findings.append("E_GIT_COMMIT")
    if receipt.get("postgres_version") != "17.6":
        findings.append("E_POSTGRES_VERSION")
    expected_inputs = {name: sha(root / name) for name in INPUTS}
    if receipt.get("input_sha256") != expected_inputs:
        findings.append("E_INPUTS_STALE")
    cases = json.loads((root / "contracts/m2/init-recovery-cases.json").read_text())["cases"]
    expected_ids = {case["id"] for case in cases} - {"SCN-M2-INIT-AMBIGUOUS"}
    observed_ids = receipt.get("verified_live_case_ids")
    if not isinstance(observed_ids, list) or not all(isinstance(item, str) for item in observed_ids) or len(observed_ids) != len(set(observed_ids)) or set(observed_ids) != expected_ids:
        findings.append("E_CASE_COVERAGE")
    observed_rejections = receipt.get("rejected_cases")
    valid_rejections = isinstance(observed_rejections, list) and all(
        isinstance(item, dict) and isinstance(item.get("case"), str) and isinstance(item.get("condition"), str)
        for item in observed_rejections
    )
    if not valid_rejections or len(observed_rejections) != len(REJECTIONS) or {
        item["case"]: item["condition"] for item in observed_rejections
    } != REJECTIONS:
        findings.append("E_REJECTIONS")
    for key in ("first_init", "idempotent_init", "post_fault_init"):
        result = receipt.get(key)
        if not isinstance(result, dict) or result.get("outcome") != "success" or result.get("logical_slot_exists") is not False or result.get("control_rows") != 2 or not re.fullmatch(r"[0-9a-f]{64}", str(result.get("postcondition_evidence_digest", ""))):
            findings.append("E_INIT_RESULT_" + key.upper())
    if receipt.get("post_bootstrap") != {
        "pgoutput_slot_count": 1,
        "heartbeat_rows": 1,
        "fence_rows": 1,
        "runtime_source_lock_observed": True,
        "peer_init_rejected_by_source_lock": True,
    }:
        findings.append("E_BOOTSTRAP_STATE")
    raw = json.dumps(receipt, sort_keys=True).lower()
    if any(value in raw for value in ("postgresql://", "local-only", "password=", "/tmp/", "/home/")):
        findings.append("E_REDACTION")
    return findings


def comparable_observation(receipt: dict) -> dict:
    """Compare boundary outcomes, excluding per-run postcondition digests."""
    observation = json.loads(json.dumps(receipt))
    for key in ("first_init", "idempotent_init", "post_fault_init"):
        observation[key].pop("postcondition_evidence_digest")
    return observation


def main() -> int:
    if len(sys.argv) not in (2, 3):
        print("usage: scripts/validate/m2_init_receipt.py RECEIPT [RERUN_RECEIPT]", file=sys.stderr)
        return 2
    try:
        receipts = [json.loads(Path(name).read_text()) for name in sys.argv[1:]]
        findings = [item for receipt in receipts for item in validate(receipt)]
        if len(receipts) == 2 and not findings and comparable_observation(receipts[0]) != comparable_observation(receipts[1]):
            findings.append("E_RERUN_DIVERGENCE")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        findings = ["E_RECEIPT: " + str(exc)]
    print(json.dumps({"validator": "m2-init-receipt/v1", "status": "pass" if not findings else "fail", "findings": findings}, sort_keys=True))
    return bool(findings)


if __name__ == "__main__":
    raise SystemExit(main())
