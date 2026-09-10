#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cp "$root/tests/fixtures/m1_config/representative.toml" "$tmp/boring-cdc.toml"
export PG_RUNTIME=redacted PG_CONTROL=redacted PG_ADMIN=redacted CH_RUNTIME=redacted CH_MAINT=redacted
python3 - "$root" "$tmp" <<'PY'
import copy, json, os, subprocess, sys
root, tmp = sys.argv[1:]
base = json.load(open(f"{root}/tests/fixtures/m1_preflight/supported.json"))
cases = json.load(open(f"{root}/contracts/m1/preflight-cases.json"))["cases"]

def put(value, *path):
    def mutate(doc):
        target = doc
        for key in path[:-1]: target = target[key]
        target[path[-1]] = value
    return mutate

mutations = {
"SCN-M1-PREFLIGHT-SCHEMA": put("unsupported", "schema_version"),
"SCN-M1-PREFLIGHT-PROVENANCE": put("mismatch", "config_fingerprint"),
"SCN-M1-PREFLIGHT-PG-VERSION": put(14, "source", "server_major"),
"SCN-M1-PREFLIGHT-WAL": put("replica", "source", "wal_level"),
"SCN-M1-PREFLIGHT-COPYBOTH": put(False, "source", "copy_both_available"),
"SCN-M1-PREFLIGHT-PUBLICATION": put(False, "source", "publication_fingerprint_matches"),
"SCN-M1-PREFLIGHT-PUBLICATION-OWNERSHIP": put(True, "source", "runtime_can_alter_publication"),
"SCN-M1-PREFLIGHT-REPLICATION-ROLE": put(False, "source", "role_has_replication"),
"SCN-M1-PREFLIGHT-SLOT": put("wrong", "source", "slot_plugin"),
"SCN-M1-PREFLIGHT-SLOT-INTENT": put(False, "source", "slot_intent_no_drop_bound"),
"SCN-M1-PREFLIGHT-SLOT-STATE": put(False, "source", "slot_wal_status_safe"),
"SCN-M1-PREFLIGHT-PUBLICATION-SHAPE": put(False, "source", "publication_row_filters_absent"),
"SCN-M1-PREFLIGHT-SOURCE-POLICY": put(False, "source", "source_tls_verified"),
"SCN-M1-PREFLIGHT-TABLES": put(False, "source", "table_contracts_match"),
"SCN-M1-PREFLIGHT-DDL": put(False, "source", "ddl_policy_matches"),
"SCN-M1-PREFLIGHT-KEYS": put(False, "source", "keys_supported"),
"SCN-M1-PREFLIGHT-TYPES": put(False, "source", "types_supported"),
"SCN-M1-PREFLIGHT-PARTITIONS": put(False, "source", "partitions_supported"),
"SCN-M1-PREFLIGHT-REPLICA-IDENTITY": put(False, "source", "replica_identity_complete"),
"SCN-M1-PREFLIGHT-CONTROL-CARDINALITY": put(0, "source", "control_rows_each"),
"SCN-M1-PREFLIGHT-CONTROL-PRIVILEGES": put(True, "source", "control_insert"),
"SCN-M1-PREFLIGHT-GRANTS": put(False, "source", "grants_sufficient"),
"SCN-M1-PREFLIGHT-PROTOCOL-FINGERPRINT": put("streaming=true", "source", "streaming_option"),
"SCN-M1-PREFLIGHT-MEMORY": put(0, "source", "sqlite_writer_staging_bytes"),
"SCN-M1-PREFLIGHT-SPILL-COUNTERS": put(False, "source", "spill_counters_available"),
"SCN-M1-PREFLIGHT-TIMEOUTS": put(30001, "source", "idle_in_transaction_session_timeout_ms"),
"SCN-M1-PREFLIGHT-OWNERSHIP": put(False, "security", "lock_probe_configured"),
"SCN-M1-PREFLIGHT-NO-MUTATION": put("changed", "storage", "after_state_sha256"),
"SCN-M1-PREFLIGHT-SQLITE-PRAGMAS": put("DELETE", "storage", "journal_mode"),
"SCN-M1-PREFLIGHT-WRITER-ATTESTATION": put(2, "current_connection_generation"),
"SCN-M1-PREFLIGHT-FILESYSTEM": put(False, "storage", "disposable_probe_succeeded"),
"SCN-M1-PREFLIGHT-BUDGETS": put(0, "storage", "reader_budget_units"),
"SCN-M1-PREFLIGHT-CLICKHOUSE-MAPPING": put(False, "destination", "clickhouse_mapping_matches"),
"SCN-M1-PREFLIGHT-ARCHIVE-MAPPING": put(False, "destination", "archive_mapping_matches"),
"SCN-M1-PREFLIGHT-DESTINATION-TLS": put(False, "destination", "tls_verified"),
"SCN-M1-PREFLIGHT-SOCKET": put(False, "security", "socket_parent_secure"),
"SCN-M1-PREFLIGHT-READONLY-ROUTES": put(False, "security", "status_read_only"),
"SCN-M1-PREFLIGHT-LISTENERS": put(False, "security", "listeners_match"),
"SCN-M1-PREFLIGHT-REDACTION": put(False, "security", "redaction_probe_passed"),
"SCN-M1-PREFLIGHT-SOURCE-FREE-DISK": put(None, "source", "source_free_disk_bytes"),
"SCN-M1-PREFLIGHT-LIVE-COLLECTOR": lambda doc: None,
}
assert set(mutations) == {case["id"] for case in cases}
for case in cases:
    doc = copy.deepcopy(base)
    mutations[case["id"]](doc)
    with open(f"{tmp}/preflight-observation.json", "w") as out: json.dump(doc, out)
    run = subprocess.run([f"{root}/target/debug/boring-cdc", "check", "--json"], cwd=tmp,
                         text=True, capture_output=True, env=os.environ)
    result = json.loads(run.stdout)
    checks = {check["scenario_id"]: check for check in result["data"]["checks"]}
    actual = checks[case["id"]]["reason"]
    assert actual == case["blocked_or_unknown_reason"], (case["id"], actual)
    expected_exit = 4 if checks[case["id"]]["status"] in {"unverified", "degraded"} else 3
    assert run.returncode == expected_exit, (case["id"], run.returncode)
    assert "redacted" not in run.stdout.lower()
print(f"m1 preflight faults: PASS ({len(cases)}/41 contract cases exercised through public CLI)")
PY
