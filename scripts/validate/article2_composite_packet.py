#!/usr/bin/env python3
"""Verify the retained exact-head Article 2 same-stream candidate observation."""

import hashlib
import json
import pathlib
import re
import subprocess


ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKET = ROOT / "evidence/article2/composite"
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
    "restart_adoption",
    "same_stream_m3_snapshot_and_m2_tail",
    "selector_absent",
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
    require(manifest["schema_version"] == "article2-composite-packet/v1", "manifest version")
    require(manifest["owner_bead"] == "boring-cdc-m4.1", "owner Bead")
    require(manifest["source_ci_job"] == "anchored_candidate", "CI job")
    require(manifest["source_ci_run"] == "https://github.com/hachej/boring-cdc/actions/runs/38789897606", "CI run")
    require(manifest["observation_path"] == "evidence/article2/composite/observation.json", "observation path")
    require(manifest["observation_sha256"] == sha256(observation_bytes), "observation digest")
    require(manifest["evidence_tier"] == "component", "evidence tier")
    require("candidate" in manifest["scope"] and "no selector promotion" in manifest["scope"], "scope")
    commit = manifest["source_commit"]
    require(re.fullmatch(r"[0-9a-f]{40}", commit) is not None, "source commit")
    require(observation["git_commit"] == commit, "observation commit")
    require(manifest["artifact_name"] == f"article2-anchored-clickhouse-{commit}", "artifact name")
    require(observation["schema_version"] == "article2-anchored-clickhouse-observation/v1", "observation version")
    require(observation["command"] == "scripts/e2e/article2_anchored_clickhouse.sh", "command")
    require(observation["status"] == "pass" and observation["runtime_observed"] is True, "observation verdict")
    require(observation["evidence_tier"] == "component" and observation["repeats"] == 2, "component repeats")
    require(observation["versions"] == {"postgres": "17.6", "clickhouse": "25.8.2.29"}, "versions")
    require(set(observation["checks"]) == CHECKS and all(observation["checks"].values()), "checks")
    require("candidate" in observation["scope"] and "no selector promotion" in observation["scope"], "observation scope")
    for name in ("oracle_sha256", "candidate_query_sha256"):
        values = observation[name]
        require(len(values) == 2 and all(re.fullmatch(r"[0-9a-f]{64}", value) for value in values), name)
    require(not FORBIDDEN_KEYS.intersection(nested_keys(observation)), "raw identifier key")
    require(set(observation["input_sha256"]) == INPUTS, "input inventory")
    for path in INPUTS:
        source = subprocess.check_output(["git", "show", f"{commit}:{path}"], cwd=ROOT)
        require(observation["input_sha256"][path] == sha256(source), f"{path} digest")
    print("ARTICLE2_COMPOSITE_PACKET_OK")


if __name__ == "__main__":
    main()
