#!/usr/bin/env python3
"""Check the relationships inside a retained M2 init component packet."""

import hashlib
import json
import pathlib
import sys

from m2_init_receipt import comparable_observation, validate

ROOT = pathlib.Path(__file__).resolve().parents[2]
SCENARIO = "SCN-M2-INIT-CLEAN"
SEED = "init-component-v1"


def sha(data):
    return hashlib.sha256(data).hexdigest()


def check(packet):
    findings = []
    try:
        manifest = json.loads((packet / "evidence.json").read_text())
        receipts = [json.loads((packet / f"attempt-{number}/receipt.json").read_text()) for number in (1, 2)]
        findings.extend(item for receipt in receipts for item in validate(receipt))
        if comparable_observation(receipts[0]) != comparable_observation(receipts[1]):
            findings.append("E_RERUN_DIVERGENCE")
        if manifest.get("owner_bead") != "boring-cdc-m2-init-recovery" or manifest.get("scenario_id") != SCENARIO or manifest.get("seed") != SEED:
            findings.append("E_PACKET_IDENTITY")
        if manifest.get("git_commit") != receipts[0]["git_commit"] or receipts[0]["git_commit"] != receipts[1]["git_commit"]:
            findings.append("E_PACKET_COMMIT")
        source = sha((json.dumps(receipts[0]["input_sha256"], sort_keys=True, separators=(",", ":")) + "\n").encode())
        if manifest.get("source_preservation") != {"before_sha256": source, "after_sha256": source, "preserved": True}:
            findings.append("E_PACKET_SOURCE")
        if json.loads((packet / "state/after.json").read_text()) != comparable_observation(receipts[0]):
            findings.append("E_PACKET_STATE")
        timeline = receipts[0]["rejected_cases"] + [{"case": "executing-plan-new-dry-run", "condition": "M2_INIT_RECONCILIATION_REQUIRED"}]
        if json.loads((packet / "fault-timeline.json").read_text()) != timeline:
            findings.append("E_PACKET_FAULT_TIMELINE")
        expected = ["scripts/e2e/m2_init_recovery.sh", "scripts/e2e/m2_init_recovery.sh", "scripts/faults/m2_init_recovery.sh"]
        commands = manifest.get("commands", [])
        if [item.get("argv") for item in commands] != expected or any(item.get("exit_code") != 0 for item in commands):
            findings.append("E_PACKET_COMMANDS")
        for number in (1, 2):
            if b"M2_INIT_RECOVERY_E2E_OK" not in (packet / f"attempt-{number}/stdout.txt").read_bytes():
                findings.append(f"E_PACKET_E2E_OUTPUT_{number}")
        if b"M2_INIT_RECOVERY_FAULTS_OK" not in (packet / "fault-stdout.txt").read_bytes():
            findings.append("E_PACKET_FAULT_OUTPUT")
        files = sorted(path for path in packet.rglob("*") if path.is_file() and path.name != "sha256.txt")
        expected_inventory = "".join(f"{sha(path.read_bytes())}  {path.relative_to(packet)}\n" for path in files)
        if (packet / "sha256.txt").read_text() != expected_inventory:
            findings.append("E_PACKET_INVENTORY")
        forbidden = (b"postgresql://", b"local-only", b"password=", b"/tmp/", b"/home/")
        if any(any(secret in path.read_bytes().lower() for secret in forbidden) for path in files):
            findings.append("E_PACKET_REDACTION")
    except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
        findings.append("E_PACKET_READ: " + str(exc))
    return findings


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: scripts/validate/m2_init_component.py ARTIFACT_ROOT")
    packet = pathlib.Path(sys.argv[1]) / SCENARIO / SEED
    findings = check(packet)
    print(json.dumps({"validator": "m2-init-component/v1", "status": "pass" if not findings else "fail", "findings": findings}, sort_keys=True))
    return bool(findings)


if __name__ == "__main__":
    raise SystemExit(main())
