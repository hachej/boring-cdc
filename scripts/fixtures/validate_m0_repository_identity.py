#!/usr/bin/env python3
"""Fail-closed validation for the public-repository decision fixture."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = ROOT / "fixtures/m0/decisions/boring-cdc-d-owner.json"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fail():
    print('{"code":"PUBLIC_OWNER_FIXTURE_INVALID","outcome":"fail","phase":"validate_spec"}')
    raise SystemExit(1)


try:
    spec = json.loads(SPEC.read_text(encoding="utf-8"))
    registry = json.loads((ROOT / "contracts/agent/stable-ids.json").read_text(encoding="utf-8"))
    graph = [json.loads(line) for line in (ROOT / ".beads/issues.jsonl").read_text(encoding="utf-8").splitlines()]
    decisions = json.loads((ROOT / "contracts/m0/decisions.json").read_text(encoding="utf-8"))
    artifacts = json.loads((ROOT / "contracts/m0/artifacts.json").read_text(encoding="utf-8"))
    observation_path = ROOT / spec["source_observation"]["record_path"]
    observation = json.loads(observation_path.read_text(encoding="utf-8"))
    required = ("inputs", "preconditions", "supported_matrix", "deterministic_phase", "expected", "expected_failure", "result_contract", "redaction_assertions", "later_executors")
    if any(not spec.get(field) for field in required):
        fail()
    if spec["fixture_id"] not in {row["id"] for row in registry["entries"]}:
        fail()
    graph_ids = {row["id"] for row in graph}
    if any(executor not in graph_ids for executor in spec["later_executors"]):
        fail()
    for key in ("path", "validator_path"):
        path = ROOT / spec["script"][key]
        digest_key = "sha256" if key == "path" else "validator_sha256"
        if not path.is_file() or sha(path) != spec["script"][digest_key]:
            fail()
    if sha(observation_path) != spec["source_observation"]["record_sha256"]:
        fail()
    output_path = ROOT / observation["output_path"]
    if sha(output_path) != observation["output_sha256"] or json.loads(output_path.read_text(encoding="utf-8")) != observation["output"]:
        fail()
    decision = next(row for row in decisions["decisions"] if row["id"] == spec["decision_id"])
    if decision["fixture_sha256"] != sha(SPEC) or decision["executor_beads"] != spec["later_executors"] or decision["status"] != "approved":
        fail()
    manifest = {row["path"]: row for row in artifacts["artifacts"] if row["id"] != "ART-M0-PUBLIC-OWNER-PROBE"}
    if any(not (ROOT / path).is_file() or sha(ROOT / path) != row["sha256"] for path, row in manifest.items()):
        fail()
except (KeyError, ValueError, OSError, json.JSONDecodeError, StopIteration, TypeError):
    fail()
print('{"code":"PUBLIC_OWNER_FIXTURE_VALID","outcome":"pass","phase":"validate_spec"}')
