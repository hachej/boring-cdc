#!/usr/bin/env python3
"""Verify the retained exact-head Article 2 selector reader probe."""

import hashlib
import json
import pathlib
import re
import subprocess


ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKET = ROOT / "evidence/article2/selector_reader"
INPUTS = {
    "scripts/e2e/article2_anchored_clickhouse.sh",
    "src/m2_capture_runtime.rs",
    "src/m2_event_envelope.rs",
    "src/m3_bootstrap.rs",
    "src/m4_initial_load.rs",
    "src/m4_clickhouse_durability.rs",
    "src/m4_clickhouse_wire.rs",
    "contracts/clickhouse/model.json",
    "contracts/clickhouse/ddl.sql",
    "contracts/clickhouse/canonical-query.sql",
}
CHECKS = {
    "anchor_bound_checkpoint",
    "child_abort_after_remote_before_checkpoint",
    "complete_anchor",
    "independent_source_oracle",
    "reader_selector_probe",
    "restart_adoption",
    "same_stream_m3_snapshot_and_m2_tail",
    "selector_absent_before_probe",
    "snapshot_batch_readback",
}
FORBIDDEN_KEYS = {"dsn", "password", "xid", "final_durable_lsn", "committed_oracle"}


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
    commit = manifest["source_commit"]
    require(manifest["schema_version"] == "article2-selector-reader-packet/v1", "manifest version")
    require(manifest["owner_bead"] == "boring-cdc-m4.1", "owner")
    require(re.fullmatch(r"[0-9a-f]{40}", commit) is not None, "source commit")
    require(manifest["source_ci_run"] == "https://github.com/hachej/boring-cdc/actions/runs/38857713727", "CI run")
    require(manifest["source_ci_job"] == "anchored_candidate", "CI job")
    require(manifest["artifact_name"] == f"article2-anchored-clickhouse-{commit}", "artifact name")
    require(manifest["observation_path"] == "evidence/article2/selector_reader/observation.json", "observation path")
    require(manifest["observation_sha256"] == sha256(observation_bytes), "observation digest")
    require(manifest["evidence_tier"] == "component", "evidence tier")
    require("synthetic selector" in manifest["scope"] and "no production promotion" in manifest["scope"], "scope")
    require(observation["schema_version"] == "article2-anchored-clickhouse-observation/v2", "observation version")
    require(observation["git_commit"] == commit, "observation commit")
    require(observation["command"] == "scripts/e2e/article2_anchored_clickhouse.sh", "command")
    require(observation["status"] == "pass" and observation["runtime_observed"] is True, "verdict")
    require(observation["evidence_tier"] == "component" and observation["repeats"] == 2, "repeats")
    require(observation["versions"] == {"postgres": "17.6", "clickhouse": "25.8.2.29"}, "versions")
    require(set(observation["checks"]) == CHECKS and all(observation["checks"].values()), "checks")
    require("direct test selector" in observation["scope"] and "production promotion" in observation["scope"], "observation scope")
    require(not FORBIDDEN_KEYS.intersection(nested_keys(observation)), "raw identifier key")
    for name in ("oracle_sha256", "candidate_query_sha256"):
        require(len(observation[name]) == 2 and all(re.fullmatch(r"[0-9a-f]{64}", value) for value in observation[name]), name)
    require(set(observation["input_sha256"]) == INPUTS, "input inventory")
    for path in INPUTS:
        source = subprocess.check_output(["git", "show", f"{commit}:{path}"], cwd=ROOT)
        require(observation["input_sha256"][path] == sha256(source), f"{path} digest")
    print("ARTICLE2_SELECTOR_READER_PACKET_OK")


if __name__ == "__main__":
    main()
