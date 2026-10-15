#!/usr/bin/env python3
"""Check the retained Article 2 capture observation against its exact source commit."""

import hashlib
import json
import pathlib
import re
import subprocess


ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKET = ROOT / "evidence/article2/capture"
SOURCE_PATHS = {
    "scenario": "scripts/acceptance/durable_simple_case.sh",
    "configuration": "tests/fixtures/m1_config/representative.toml",
    "compose": "compose.yaml",
    "cargo_lock": "Cargo.lock",
}
CHECKS = {
    "confirmed_flush_bounded_by_durable_journal",
    "distinct_startup_run_ids",
    "journal_sequence_gap_free",
    "restart_lsn_nonregressing",
    "sigkill_exit_137",
    "source_oracle_equals_journal",
    "toast_inheritance",
    "update_key_change_delete",
}
FORBIDDEN_KEYS = {
    "committed_oracle",
    "dsn",
    "feedback_boundaries",
    "final_durable_lsn",
    "first_run_id",
    "password",
    "xid",
}
SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def nested_keys(value: object):
    if isinstance(value, dict):
        for key, item in value.items():
            yield key
            yield from nested_keys(item)
    elif isinstance(value, list):
        for item in value:
            yield from nested_keys(item)


def main() -> None:
    manifest = json.loads((PACKET / "manifest.json").read_text())
    observation_bytes = (PACKET / "observation.json").read_bytes()
    observation = json.loads(observation_bytes)
    require(manifest["schema_version"] == "article2-capture-packet/v1", "manifest version")
    require(manifest["owner_bead"] == "boring-cdc-m2-fault-status.8", "owner Bead")
    require(manifest["source_ci_run"] == "https://github.com/hachej/boring-cdc/actions/runs/38750001126", "CI run")
    require(manifest["source_ci_job"] == "m4_live_journal_convergence", "CI job")
    require(manifest["artifact_name"] == "m4-live-journal-convergence", "artifact name")
    require(manifest["ci_command"] == "scripts/e2e/m4_live_journal_convergence.sh", "CI command")
    require(manifest["capture_command"] == observation["command"], "capture command")
    require(manifest["observation_path"] == "evidence/article2/capture/observation.json", "observation path")
    require(manifest["observation_sha256"] == sha256(observation_bytes), "observation digest")
    require(manifest["evidence_tier"] == "component" and manifest["status"] == "pass", "component verdict")
    require("no M3 anchor" in manifest["scope"] and "no M3 anchor" in observation["scope"], "scope")
    require(observation["schema_version"] == "article2-capture-observation/v1", "observation version")
    commit = manifest["source_commit"]
    require(re.fullmatch(r"[0-9a-f]{40}", commit) is not None, "source commit")
    require(observation["git_commit"] == commit, "observation commit")
    require(observation["status"] == "pass", "observation verdict")
    require(observation["versions"]["postgres"] == "17.6", "PostgreSQL version")
    require(observation["versions"]["postgres_image"].startswith("docker.io/library/postgres:17.6@sha256:"), "PostgreSQL image")
    require(observation["counts"] == {
        "feedback_samples": 5,
        "journal_events_at_oracle": 8,
        "journal_user_transactions": 6,
        "source_user_transactions": 6,
    }, "capture counts")
    require(set(observation["checks"]) == CHECKS and all(observation["checks"].values()), "capture checks")
    require(SHA256.fullmatch(observation["source_oracle_sha256"]) is not None, "oracle digest")
    require(not FORBIDDEN_KEYS.intersection(nested_keys(observation)), "raw identifier or payload key")
    require(set(observation["input_sha256"]) == {*SOURCE_PATHS, "binary"}, "input inventory")
    for name, path in SOURCE_PATHS.items():
        source = subprocess.check_output(["git", "show", f"{commit}:{path}"], cwd=ROOT)
        require(observation["input_sha256"][name] == sha256(source), f"{name} digest")
    require(SHA256.fullmatch(observation["input_sha256"]["binary"]) is not None, "binary digest")
    print("ARTICLE2_CAPTURE_PACKET_OK")


if __name__ == "__main__":
    main()
