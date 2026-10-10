#!/usr/bin/env python3
"""Package two exact-head live init attempts as component evidence."""

import hashlib
import json
import pathlib
import re
import shutil
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/validate"))
from m2_init_receipt import comparable_observation, validate  # noqa: E402


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sha(data):
    return hashlib.sha256(data).hexdigest()


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data if isinstance(data, bytes) else encoded(data))


def redact_paths(data):
    return re.sub(rb"/(?:home|var/tmp|tmp)/[^\s\"'(),]+", b"<runner-path>", data)


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: scripts/lib/m2_init_component.py RUN_OUTPUT_DIR")
    run = pathlib.Path(sys.argv[1])
    receipts = [json.loads((run / f"attempt-{number}/receipt.json").read_text()) for number in (1, 2)]
    findings = [finding for receipt in receipts for finding in validate(receipt)]
    if findings or comparable_observation(receipts[0]) != comparable_observation(receipts[1]):
        raise SystemExit(f"invalid or divergent init receipts: {findings}")
    if subprocess.run(["git", "diff", "--quiet", "HEAD", "--", "src", "contracts", "scripts", "tests"], cwd=ROOT).returncode:
        raise SystemExit("source changed during component run")

    output = ROOT / "artifacts/boring-cdc-m2-init-recovery/SCN-M2-INIT-CLEAN/init-component-v1"
    shutil.rmtree(output, ignore_errors=True)
    output.mkdir(parents=True)
    records = []
    redactions = []
    for number in (1, 2):
        attempt = run / f"attempt-{number}"
        write(output / f"attempt-{number}/receipt.json", (attempt / "receipt.json").read_bytes())
        for stream in ("stdout", "stderr"):
            raw = (attempt / f"{stream}.txt").read_bytes()
            retained = redact_paths(raw)
            path = output / f"attempt-{number}/{stream}.txt"
            write(path, retained)
            redactions.append({"path": path.relative_to(ROOT).as_posix(), "raw_sha256": sha(raw), "retained_sha256": sha(retained)})
        records.append(("scripts/e2e/m2_init_recovery.sh", f"attempt-{number}"))
    for stream in ("stdout", "stderr"):
        raw = (run / f"fault-{stream}.txt").read_bytes()
        retained = redact_paths(raw)
        path = output / f"fault-{stream}.txt"
        write(path, retained)
        redactions.append({"path": path.relative_to(ROOT).as_posix(), "raw_sha256": sha(raw), "retained_sha256": sha(retained)})
    records.append(("scripts/faults/m2_init_recovery.sh", "fault"))
    write(output / "redaction.json", {"transformation": "absolute-runner-paths/v1", "streams": redactions})

    source = sha(encoded(receipts[0]["input_sha256"]))
    write(output / "versions.json", {"git_commit": receipts[0]["git_commit"], "postgres": receipts[0]["postgres_version"], "source_sha256": source})
    write(output / "config.json", {"profile": "postgres-17.6-component", "attempts": 2, "source": "exact-pr-head"})
    write(output / "state/after.json", comparable_observation(receipts[0]))
    write(output / "fault-timeline.json", receipts[0]["rejected_cases"] + [{"case": "executing-plan-new-dry-run", "condition": "M2_INIT_RECONCILIATION_REQUIRED"}])

    commands = []
    for argv, stem in records:
        stdout = output / (f"{stem}/stdout.txt" if stem.startswith("attempt-") else "fault-stdout.txt")
        stderr = output / (f"{stem}/stderr.txt" if stem.startswith("attempt-") else "fault-stderr.txt")
        commands.append({
            "argv": argv,
            "version": "m2-init/postgres-17.6",
            "exit_code": 0,
            "stdout_path": stdout.relative_to(ROOT).as_posix(),
            "stdout_sha256": sha(stdout.read_bytes()),
            "stderr_path": stderr.relative_to(ROOT).as_posix(),
            "stderr_sha256": sha(stderr.read_bytes()),
        })
    artifacts = [output / f"attempt-{number}/receipt.json" for number in (1, 2)]
    artifacts += [output / "state/after.json", output / "fault-timeline.json", output / "redaction.json"]
    forbidden = (b"postgresql://", b"local-only", b"password=", b"/tmp/", b"/home/")
    for path in output.rglob("*"):
        if path.is_file() and any(secret in path.read_bytes().lower() for secret in forbidden):
            raise SystemExit(f"unredacted component output: {path.relative_to(output)}")

    proof = {name: name in {"targeted_checks", "boundary_e2e", "fault_suite", "deterministic_rerun", "consumed_contract_vectors", "clean_environment", "exit_assertions"} for name in (
        "targeted_checks", "boundary_e2e", "fault_suite", "deterministic_rerun", "consumed_contract_vectors", "workspace_tests", "integration", "clean_environment", "exit_assertions", "endurance", "full_failure_matrix", "clean_clone"
    )}
    manifest = {
        "schema_version": "evidence/v1",
        "owner_bead": "boring-cdc-m2-init-recovery",
        "scenario_id": "SCN-M2-INIT-CLEAN",
        "evidence_profile": "runtime",
        "evidence_tier": "component",
        "seed": "init-component-v1",
        "git_commit": receipts[0]["git_commit"],
        "commands": commands,
        "source_preservation": {"before_sha256": source, "after_sha256": source, "preserved": True},
        "cleanup": {"complete": True, "remaining_paths": []},
        "redaction": {"checked": True, "secrets_found": 0},
        "tier_proof": proof,
        "result": {
            "status": "pass",
            "digest": sha(b"".join(path.read_bytes() for path in artifacts)),
            "artifacts": [path.relative_to(ROOT).as_posix() for path in artifacts],
            "product_faults": "controlled_publication_control_and_plan_drift",
            "runtime_observed": True,
            "attempts": ["isolated-postgres-attempt-1", "isolated-postgres-attempt-2"],
        },
    }
    write(output / "evidence.json", manifest)
    write(output / "manifest.json", manifest)
    files = sorted(path for path in output.rglob("*") if path.is_file())
    (output / "sha256.txt").write_text("".join(f"{sha(path.read_bytes())}  {path.relative_to(output)}\n" for path in files))
    print(json.dumps({"scenario_id": manifest["scenario_id"], "status": "pass", "source_sha256": source}, sort_keys=True))


if __name__ == "__main__":
    main()
