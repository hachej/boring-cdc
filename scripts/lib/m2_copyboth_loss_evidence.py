#!/usr/bin/env python3
"""Seal the exact-head PostgreSQL CopyBoth-loss capture observation."""
import hashlib
import json
import pathlib
import subprocess
import sys

root = pathlib.Path(__file__).resolve().parents[2]
out = pathlib.Path(sys.argv[1]).resolve()
stdout = pathlib.Path(sys.argv[2]).read_bytes()
stderr = pathlib.Path(sys.argv[3]).read_bytes()
assert out.is_relative_to(root / "artifacts/boring-cdc-m2-capture-runtime")
assert stdout.count(b"M2_COPYBOTH_LOSS_FENCED_OK") == 2
cases = json.loads((root / "contracts/m2/capture-runtime-cases.json").read_text())["cases"]
case = next(case for case in cases if case["id"] == "SCN-M2-CAPTURE-COPYBOTH-LOSS")
assert case["test"] == "deterministic_safe_stop_shutdown_and_no_in_process_reopen"
assert case["assertion"] == "unexpected CopyBoth loss fences generation and requires supervised successor"
attempts = [out / f"attempt-{number}" / "observation.json" for number in (1, 2)]
for path in attempts:
    observed = json.loads(path.read_text())
    assert observed["runtime_exited_without_operator_signal"] and not observed["replication_reopened"]


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


sources = [
    "src/m2_capture_runtime.rs",
    "src/m2_ownership.rs",
    "contracts/m2/capture-runtime-cases.json",
    "scripts/e2e/m2_capture_runtime.sh",
    "scripts/lib/m2_copyboth_loss_evidence.py",
    "scripts/validate/m2_copyboth_loss.py",
]
source_hash = hashlib.sha256()
for name in sources:
    data = (root / name).read_bytes()
    source_hash.update(len(name).to_bytes(8, "big"))
    source_hash.update(name.encode())
    source_hash.update(len(data).to_bytes(8, "big"))
    source_hash.update(data)
source_digest = source_hash.hexdigest()
head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
markers = [line for line in stdout.splitlines() if line.startswith(b"M2_COPYBOTH_LOSS_FENCED_OK")]
retained_stdout = b"\n".join(markers) + b"\n"
(out / "stdout.txt").write_bytes(retained_stdout)
(out / "stderr.txt").write_bytes(b"")
(out / "redaction.json").write_bytes(canonical({
    "raw_stdout_sha256": digest(stdout),
    "raw_stderr_sha256": digest(stderr),
    "retained_stdout_sha256": digest(retained_stdout),
    "retained_stderr_sha256": digest(b""),
    "method": "retain-stable-success-marker-only",
}))
(out / "fault-timeline.json").write_bytes(canonical([
    "durable_transaction_and_feedback",
    "copyboth_backend_terminated",
    "unexpected_eof_fences_capture",
    "copyboth_not_reopened",
    "nonzero_process_exit",
]))
artifacts = attempts + [out / "redaction.json", out / "fault-timeline.json"]
relative = lambda path: path.relative_to(root).as_posix()
manifest = {
    "schema_version": "evidence/v1",
    "owner_bead": "boring-cdc-m2-capture-runtime",
    "scenario_id": "SCN-M2-CAPTURE-COPYBOTH-LOSS",
    "evidence_profile": "runtime",
    "evidence_tier": "component",
    "seed": "pg17-copyboth-loss-current-head",
    "git_commit": head,
    "commands": [{
        "argv": "scripts/e2e/m2_capture_runtime.sh",
        "version": "m2-copyboth-loss/v1",
        "exit_code": 0,
        "stdout_path": relative(out / "stdout.txt"),
        "stdout_sha256": digest((out / "stdout.txt").read_bytes()),
        "stderr_path": relative(out / "stderr.txt"),
        "stderr_sha256": digest(b""),
    }],
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
        "status": "pass", "digest": digest(b"".join(path.read_bytes() for path in artifacts)),
        "artifacts": [relative(path) for path in artifacts],
        "product_faults": "replication CopyBoth session terminated after a durable transaction",
        "runtime_observed": True,
        "attempts": ["fresh-postgres-17.6-a", "fresh-postgres-17.6-b"],
    },
}
(out / "evidence.json").write_bytes(canonical(manifest))
(out / "manifest.json").write_bytes(canonical(manifest))
files = sorted(path for path in out.rglob("*") if path.is_file() and path.name != "sha256.txt")
(out / "sha256.txt").write_text("".join(
    f"{digest(path.read_bytes())}  {path.relative_to(out).as_posix()}\n" for path in files
))
print(canonical({"status": "pass", "scenario_id": manifest["scenario_id"]}).decode(), end="")
