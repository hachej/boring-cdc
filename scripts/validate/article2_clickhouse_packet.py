#!/usr/bin/env python3
"""Check the retained live M2-journal-to-ClickHouse component observation."""

import hashlib
import json
import pathlib
import re
import subprocess


ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKET = ROOT / "evidence/article2/clickhouse"
SOURCE_PATHS = {
    "scenario": "scripts/e2e/m4_live_journal_convergence.sh",
    "capture_scenario": "scripts/acceptance/durable_simple_case.sh",
    "clickhouse_ddl": "contracts/clickhouse/ddl.sql",
    "canonical_query": "contracts/clickhouse/canonical-query.sql",
    "clickhouse_model": "contracts/clickhouse/model.json",
}
CHECKS = {
    "delete",
    "event_conflict_rejected",
    "key_change",
    "selector_conflict_rejected",
    "toast_inheritance",
    "typed_oracle",
}
FORBIDDEN_KEYS = {"dsn", "password", "xid", "final_durable_lsn", "committed_oracle"}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def source_bytes(commit: str, path: str) -> bytes:
    return subprocess.check_output(["git", "show", f"{commit}:{path}"], cwd=ROOT)


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
    require(manifest["schema_version"] == "article2-clickhouse-component-packet/v1", "manifest version")
    require(manifest["owner_bead"] == "boring-cdc-m4.2", "owner Bead")
    require(manifest["source_ci_run"] == "https://github.com/hachej/boring-cdc/actions/runs/38737982759", "CI run")
    require(manifest["source_ci_job"] == "m4_live_journal_convergence", "CI job")
    require(manifest["artifact_name"] == "m4-live-journal-convergence", "artifact name")
    require(manifest["observation_path"] == "evidence/article2/clickhouse/observation.json", "observation path")
    require(manifest["observation_sha256"] == sha256(observation_bytes), "observation digest")
    require(manifest["evidence_tier"] == "component", "evidence tier")
    require("synthetic selector" in manifest["scope"] and "no M3 snapshot import" in manifest["scope"], "scope")
    commit = manifest["source_commit"]
    require(re.fullmatch(r"[0-9a-f]{40}", commit) is not None, "source commit")
    require(observation["git_commit"] == commit, "observation commit")
    require(observation["schema_version"] == "m4-live-journal-observation/v1", "observation version")
    require(observation["command"] == "scripts/e2e/m4_live_journal_convergence.sh", "command")
    require(observation["status"] == "pass", "observation verdict")
    require(observation["versions"] == {"postgres": "17.6", "clickhouse": "25.8.2.29"}, "versions")
    require(observation["selected_generations"] == [3, 6], "generations")
    require(observation["final_keys"] == [101, 201, 202, 203, 302], "final keys")
    require(observation["physical_replay_rows"] == 13 and observation["logical_event_ids"] == 12, "replay counts")
    require(set(observation["checks"]) == CHECKS and all(observation["checks"].values()), "checks")
    require("synthetic component probe" in observation["selector"], "selector limit")
    require(re.fullmatch(r"[0-9a-f]{64}", observation["postgres_oracle_sha256"]) is not None, "oracle digest")
    require(not FORBIDDEN_KEYS.intersection(nested_keys(observation)), "raw identifier key")
    require(set(observation["input_sha256"]) == set(SOURCE_PATHS), "input inventory")
    for name, path in SOURCE_PATHS.items():
        require(observation["input_sha256"][name] == sha256(source_bytes(commit, path)), f"{name} digest")
    require(manifest["abort_test"] == "m4_clickhouse_durability::tests::live_m2_copied_transactions_are_admitted_by_m4", "abort test")
    require(manifest["abort_test_source_sha256"] == sha256(source_bytes(commit, "src/m4_clickhouse_durability.rs")), "abort test source")
    print("ARTICLE2_CLICKHOUSE_PACKET_OK")


if __name__ == "__main__":
    main()
