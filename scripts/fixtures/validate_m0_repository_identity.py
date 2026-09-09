#!/usr/bin/env python3
"""Fail-closed validation for the public-repository decision fixture."""
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC_REL = "fixtures/m0/decisions/boring-cdc-d-owner.json"
SPEC = ROOT / SPEC_REL
FIXTURE_ID = "SCN-M0-PUBLIC-OWNER-IDENTITY"
OWNER = "boring-cdc-d-owner"
EXECUTOR = "boring-cdc-m0-scaffold"
INTENTION = "d3e8abc3-d2f4-4bc0-8aec-d6ffd7bf2e36"
APPROVER = "Julien Hurault (repository owner)"
APPROVED_AT = "2026-09-09T08:58:19Z"
PROPOSED = "Public repository identity is hachej/boring-cdc with visibility PUBLIC at https://github.com/hachej/boring-cdc."
EXPECTED = {"nameWithOwner": "hachej/boring-cdc", "url": "https://github.com/hachej/boring-cdc", "visibility": "PUBLIC"}
COMMAND = "gh repo view hachej/boring-cdc --json nameWithOwner,visibility,url --jq '{nameWithOwner:.nameWithOwner,visibility:.visibility,url:.url}'"
ACTIVE_RUN = sys.argv[1:] == ["--active-run"]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fail():
    print('{"code":"PUBLIC_OWNER_FIXTURE_INVALID","outcome":"fail","phase":"validate_spec"}')
    raise SystemExit(1)


if sys.argv[1:] not in ([], ["--active-run"]):
    fail()
try:
    spec = json.loads(SPEC.read_text(encoding="utf-8"))
    registry = json.loads((ROOT / "contracts/agent/stable-ids.json").read_text(encoding="utf-8"))
    coverage = json.loads((ROOT / "contracts/coverage/plan-to-beads.json").read_text(encoding="utf-8"))
    graph = [json.loads(line) for line in (ROOT / ".beads/issues.jsonl").read_text(encoding="utf-8").splitlines()]
    decisions = json.loads((ROOT / "contracts/m0/decisions.json").read_text(encoding="utf-8"))
    artifacts = json.loads((ROOT / "contracts/m0/artifacts.json").read_text(encoding="utf-8"))
    observation_path = ROOT / spec["source_observation"]["record_path"]
    observation = json.loads(observation_path.read_text(encoding="utf-8"))
    required = ("inputs", "preconditions", "supported_matrix", "deterministic_phase", "expected", "expected_failure", "result_contract", "redaction_assertions", "later_executors")
    if any(not spec.get(field) for field in required) or spec["approved_boundary"] != {"canonical_url": EXPECTED["url"], "nameWithOwner": EXPECTED["nameWithOwner"], "visibility": EXPECTED["visibility"]}:
        fail()
    if spec["fixture_id"] != FIXTURE_ID or spec["owner_bead"] != OWNER or spec["later_executors"] != [EXECUTOR]:
        fail()
    if spec["approval"] != {"approved_at": APPROVED_AT, "approved_by": APPROVER, "intention_id": INTENTION, "scope": "all 25 M0 proposed/default boundaries as written"}:
        fail()
    stable = next(row for row in registry["entries"] if row["id"] == FIXTURE_ID)
    covered = next(row for row in coverage["assignments"] if row["id"] == FIXTURE_ID)
    if stable["owner_bead"] != OWNER or stable["source"] != SPEC_REL or stable["namespace"] != "SCN":
        fail()
    if covered != {"evidence_status": stable["evidence_status"], "id": FIXTURE_ID, "owner_bead": OWNER, "source": SPEC_REL, "source_digest": stable["source_digest"]}:
        fail()
    if EXECUTOR not in {row["id"] for row in graph}:
        fail()
    for key in ("path", "validator_path"):
        path = ROOT / spec["script"][key]
        digest_key = "sha256" if key == "path" else "validator_sha256"
        if not path.is_file() or sha(path) != spec["script"][digest_key]:
            fail()
    if sha(observation_path) != spec["source_observation"]["record_sha256"]:
        fail()
    output_path = ROOT / observation["output_path"]
    if observation["canonical_observed_at"] != "2026-08-28T00:00:00Z" or observation["reverified_at"] != "2026-09-09T11:43:00Z" or observation["command"] != COMMAND:
        fail()
    if observation["approved_by"] != APPROVER or observation["approval_intention_id"] != INTENTION or sha(output_path) != observation["output_sha256"] or json.loads(output_path.read_text(encoding="utf-8")) != EXPECTED or observation["output"] != EXPECTED:
        fail()
    decision = next(row for row in decisions["decisions"] if row["id"] == "DEC-PUBLIC-OWNER")
    expected_approval = {"approved_at": APPROVED_AT, "approved_by": f"{APPROVER}, intention {INTENTION}", "value_digest": hashlib.sha256(PROPOSED.encode()).hexdigest()}
    if decision != {"id": "DEC-PUBLIC-OWNER", "owner_bead": OWNER, "status": "approved", "proposed_value": PROPOSED, "approval": expected_approval, "fixture_spec": SPEC_REL, "fixture_sha256": sha(SPEC), "executor_beads": [EXECUTOR]}:
        fail()
    skip = {"ART-M0-PUBLIC-OWNER-PROBE"} if ACTIVE_RUN else set()
    manifest = {row["path"]: row for row in artifacts["artifacts"] if row["id"] not in skip}
    if any(not (ROOT / path).is_file() or sha(ROOT / path) != row["sha256"] for path, row in manifest.items()):
        fail()
    if not ACTIVE_RUN:
        evidence = json.loads((ROOT / "artifacts/m0/decisions/boring-cdc-d-owner/evidence.json").read_text(encoding="utf-8"))
        if evidence["status"] != "pass" or evidence["findings"] != [] or evidence["input_sha256"] != sha(ROOT / "contracts/m0/decisions.json"):
            fail()
except (KeyError, ValueError, OSError, json.JSONDecodeError, StopIteration, TypeError):
    fail()
print('{"code":"PUBLIC_OWNER_FIXTURE_VALID","outcome":"pass","phase":"validate_spec"}')
