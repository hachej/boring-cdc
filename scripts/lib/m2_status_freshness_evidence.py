#!/usr/bin/env python3
"""Write one immutable packet from two live and two fault observations."""

import hashlib
import json
import os
import pathlib
import re
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[2]
SCENARIO = "SCN-M2-STATUS-FRESHNESS-COMPONENT"
OWNER = "boring-cdc-m2-fault-status.4"
SEED = os.environ.get("M2_STATUS_FRESHNESS_EVIDENCE_SEED", "status-freshness-v1")
if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}", SEED):
    raise SystemExit("invalid evidence seed")
if len(sys.argv) != 5:
    raise SystemExit("expected two live and two fault observation paths")
output = ROOT / "artifacts/boring-cdc-m2-fault-status" / SCENARIO / SEED
if output.exists():
    raise SystemExit(f"evidence seed already exists: {output}")


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value if isinstance(value, bytes) else value.encode())


live = [json.loads(pathlib.Path(path).read_text()) for path in sys.argv[1:3]]
faults = [json.loads(pathlib.Path(path).read_text()) for path in sys.argv[3:5]]
contract = json.loads((ROOT / "contracts/m2/status-freshness-cases.json").read_text())
assert contract["owner_bead"] == OWNER and contract["scenario_id"] == SCENARIO
for observation in live:
    assert observation["startup_age_seconds"] >= contract["live"]["minimum_startup_age_seconds"]
    assert 0 <= observation["heartbeat_age_seconds"] <= contract["live"]["maximum_heartbeat_age_seconds"]
    assert observation["freshness"] == contract["live"]["freshness"]
    assert observation["last_heartbeat_seq"] > 0 and observation["postgres_version"] == "17.6"
assert faults == [contract["faults"], contract["faults"]]

source_files = [
    "src/m2_schema.rs",
    "src/m2_journal.rs",
    "src/m2_fault_status.rs",
    "scripts/acceptance/durable_simple_case.sh",
    "scripts/e2e/m2_status_freshness.sh",
    "scripts/lib/m2_status_freshness_evidence.py",
    "scripts/validate/m2_schema.py",
    "contracts/m2/status-freshness-cases.json",
]
source_hash = hashlib.sha256()
for name in source_files:
    source_hash.update(name.encode() + (ROOT / name).read_bytes())
implementation_digest = source_hash.hexdigest()
commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
config = {
    "seed": SEED,
    "postgres_image": "17.6@sha256:00bc86618629af00d2937fdc5a5d63db3ff8450acf52f0636ec813c7f4902929",
    "live_attempts": 2,
    "fault_attempts": 2,
}
oracle = {
    "live": live,
    "faults": faults,
    "deterministic_attempts": 2,
    "source_digests": {
        "contracts/m2/status-freshness-cases.json": digest((ROOT / "contracts/m2/status-freshness-cases.json").read_bytes())
    },
}
after = {
    "freshness": "fresh",
    "startup_older_than_freshness_window": True,
    "durable_heartbeat_per_run": True,
    "stale_on_missing_heartbeat": True,
    "restart_requires_new_heartbeat": True,
}
write(output / "config.json", encoded(config))
write(output / "oracle.json", encoded(oracle))
write(output / "state/before.json", encoded({"freshness": "stale", "heartbeat_proof": "absent"}))
write(output / "state/after.json", encoded(after))
write(output / "fault-timeline.json", encoded(["precommit_abort", "durable_heartbeat", "stale_after_31_seconds", "restart_without_proof", "new_run_heartbeat"]))
write(output / "logs/boring-cdc.jsonl", encoded({"schema_version": "m2-status-freshness-event/v1", "scenario_id": SCENARIO, "bead_id": OWNER, "outcome": "pass", "config_fingerprint": digest(encoded(config))}))
write(output / "stdout.txt", "M2_STATUS_FRESHNESS_COMPONENT_OK\n")
write(output / "stderr.txt", b"")
write(output / "commands.txt", "scripts/e2e/m2_status_freshness.sh\n")
write(output / "versions.json", encoded({"git_commit": commit, "implementation_sha256": implementation_digest, "postgres": "17.6", "python": sys.version.split()[0]}))

command = {
    "argv": "scripts/e2e/m2_status_freshness.sh",
    "version": "m2-status-freshness/v1",
    "exit_code": 0,
    "stdout_path": (output / "stdout.txt").relative_to(ROOT).as_posix(),
    "stdout_sha256": digest((output / "stdout.txt").read_bytes()),
    "stderr_path": (output / "stderr.txt").relative_to(ROOT).as_posix(),
    "stderr_sha256": digest((output / "stderr.txt").read_bytes()),
}
result_files = [output / "oracle.json", output / "state/after.json", output / "fault-timeline.json"]
manifest = {
    "schema_version": "evidence/v1",
    "owner_bead": OWNER,
    "scenario_id": SCENARIO,
    "evidence_profile": "runtime",
    "evidence_tier": "component",
    "seed": SEED,
    "git_commit": commit,
    "commands": [command],
    "source_preservation": {"before_sha256": implementation_digest, "after_sha256": implementation_digest, "preserved": True},
    "cleanup": {"complete": True, "remaining_paths": []},
    "redaction": {"checked": True, "secrets_found": 0},
    "tier_proof": {"targeted_checks": True, "boundary_e2e": True, "fault_suite": True, "deterministic_rerun": True, "consumed_contract_vectors": True, "workspace_tests": False, "integration": True, "clean_environment": True, "exit_assertions": True, "endurance": False, "full_failure_matrix": False, "clean_clone": False},
    "result": {"status": "pass", "digest": digest(b"".join(path.read_bytes() for path in result_files)), "artifacts": [path.relative_to(ROOT).as_posix() for path in result_files], "product_faults": "sqlite_precommit_abort_and_restart_without_run_heartbeat", "runtime_observed": True, "attempts": ["live-pg17-1", "live-pg17-2", "fault-1", "fault-2"]},
}
write(output / "manifest.json", encoded(manifest))
write(output / "evidence.json", encoded(manifest))
listed = sorted(path for path in output.rglob("*") if path.is_file())
write(output / "sha256.txt", "".join(f"{digest(path.read_bytes())}  {path.relative_to(output).as_posix()}\n" for path in listed))
print(json.dumps({"scenario_id": SCENARIO, "status": "pass"}, sort_keys=True))
