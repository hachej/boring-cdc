#!/usr/bin/env python3
"""Fail-closed validation for the public-repository decision fixture."""
import hashlib
import json
import sys
import re
import subprocess
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
EXPECTED_PROBE = [
    {"code": "PUBLIC_OWNER_FIXTURE_VALID", "outcome": "pass", "phase": "validate_spec"},
    {"code": "REPOSITORY_IDENTITY_CONFIRMED", "outcome": "pass", "phase": "observe"},
]
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
    anchor = '"fixture_id":"SCN-M0-PUBLIC-OWNER-IDENTITY"'
    anchor_digest = hashlib.sha256(anchor.encode()).hexdigest()
    if stable["owner_bead"] != OWNER or stable["source"] != SPEC_REL or stable["namespace"] != "SCN" or stable["source_anchor"] != anchor or stable["source_excerpt"] != anchor or stable["source_digest"] != anchor_digest:
        fail()
    if SPEC.read_text(encoding="utf-8").count(anchor) != 1 or covered != {"evidence_status": "pending", "id": FIXTURE_ID, "owner_bead": OWNER, "source": SPEC_REL, "source_digest": anchor_digest}:
        fail()
    provenance = json.loads((ROOT / "contracts/coverage/plan-to-beads.provenance.json").read_text(encoding="utf-8"))
    if provenance["generated_digest"] != sha(ROOT / "contracts/coverage/plan-to-beads.json") or provenance["source_digests"]["contracts/agent/stable-ids.json"] != sha(ROOT / "contracts/agent/stable-ids.json"):
        fail()
    if subprocess.run([str(ROOT / "scripts/validate/plan_coverage.sh")], cwd=ROOT, capture_output=True).returncode != 0:
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
    expected_artifacts = {
        "ART-M0-PUBLIC-OWNER-FIXTURE": SPEC_REL,
        "ART-M0-PUBLIC-OWNER-OBSERVATION": "artifacts/m0/decisions/boring-cdc-d-owner/repository-view.json",
        "ART-M0-PUBLIC-OWNER-OBSERVATION-RECORD": "artifacts/m0/decisions/boring-cdc-d-owner/observation.json",
        "ART-M0-PUBLIC-OWNER-PROBE": "artifacts/m0/decisions/boring-cdc-d-owner/fixture-run.jsonl",
        "ART-M0-PUBLIC-OWNER-VALIDATION": "artifacts/m0/decisions/boring-cdc-d-owner/evidence.json",
    }
    owned = {row["id"]: row for row in artifacts["artifacts"] if row.get("owner_bead") == OWNER}
    if set(owned) != set(expected_artifacts):
        fail()
    for artifact_id, path in expected_artifacts.items():
        row = owned[artifact_id]
        if row != {"id": artifact_id, "owner_bead": OWNER, "path": path, "sha256": row["sha256"], "status": "complete"}:
            fail()
        if artifact_id != "ART-M0-PUBLIC-OWNER-PROBE" or not ACTIVE_RUN:
            if not (ROOT / path).is_file() or sha(ROOT / path) != row["sha256"]:
                fail()
    if not ACTIVE_RUN:
        probe = [json.loads(line) for line in (ROOT / expected_artifacts["ART-M0-PUBLIC-OWNER-PROBE"]).read_text(encoding="utf-8").splitlines()]
        if probe != EXPECTED_PROBE or spec["execution_probe"]["expected_lines"] != EXPECTED_PROBE or spec["execution_probe"]["path"] != expected_artifacts["ART-M0-PUBLIC-OWNER-PROBE"] or spec["execution_probe"]["sha256"] != sha(ROOT / expected_artifacts["ART-M0-PUBLIC-OWNER-PROBE"]):
            fail()
        evidence = json.loads((ROOT / expected_artifacts["ART-M0-PUBLIC-OWNER-VALIDATION"]).read_text(encoding="utf-8"))
        if evidence.get("schema_version") != "validation-result/v1" or evidence.get("validator_version") != "core-validators/1.0.0" or evidence.get("owner_bead") != "boring-cdc-m0.1" or evidence.get("status") != "pass" or evidence.get("findings") != [] or evidence.get("input_sha256") != sha(ROOT / "contracts/m0/decisions.json") or not re.fullmatch(r"[0-9a-f]{40}", evidence.get("git_commit", "")):
            fail()
        evidence_sha = evidence["git_commit"]
        if subprocess.run(["git", "cat-file", "-e", evidence_sha + "^{commit}"], cwd=ROOT, capture_output=True).returncode != 0 or subprocess.run(["git", "merge-base", "--is-ancestor", evidence_sha, "HEAD"], cwd=ROOT, capture_output=True).returncode != 0 or subprocess.run(["git", "diff", "--quiet", evidence_sha + "..HEAD", "--", SPEC_REL, "scripts/fixtures", "contracts/agent", "contracts/coverage", "contracts/m0/decisions.json", "artifacts/m0/decisions/boring-cdc-d-owner/observation.json", "artifacts/m0/decisions/boring-cdc-d-owner/repository-view.json"], cwd=ROOT).returncode != 0:
            fail()
except (KeyError, ValueError, OSError, json.JSONDecodeError, StopIteration, TypeError):
    fail()
print('{"code":"PUBLIC_OWNER_FIXTURE_VALID","outcome":"pass","phase":"validate_spec"}')
