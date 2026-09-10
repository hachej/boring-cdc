#!/usr/bin/env python3
"""Validate the M0 PostgreSQL capture/backfill contract and executable fixtures."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OWNER = "boring-cdc-m0-pg-contract"
CONTRACT = ROOT / "contracts/postgres/capture-backfill.json"
SCHEMA = ROOT / "contracts/postgres/capture-backfill.schema.json"
FIXTURES = ROOT / "fixtures/m0/postgres/capture-backfill.json"
FIXTURE_SCHEMA = ROOT / "contracts/postgres/capture-backfill-fixtures.schema.json"
SQL = ROOT / "contracts/postgres/roles-grants.sql"
EVIDENCE = ROOT / "artifacts/boring-cdc-m0-pg-contract/spec/evidence.json"
CORE_SPEC = importlib.util.spec_from_file_location("core_validator", ROOT / "scripts/lib/core_validator.py")
CORE = importlib.util.module_from_spec(CORE_SPEC)
CORE_SPEC.loader.exec_module(CORE)


def load(path: Path):
    return json.loads(path.read_text())


def finding(items, code, path, message):
    items.append({"code": code, "path": path, "message": message})


def validate():
    findings = []
    contract, schema, fixtures = load(CONTRACT), load(SCHEMA), load(FIXTURES)
    schema_findings = []
    CORE.validate_schema_instance(contract, schema, schema_findings, base=SCHEMA.parent, root=schema)
    for item in schema_findings:
        finding(findings, "E_SCHEMA", "contract", item["pointer"] + ": " + item["message"])
    fixture_schema = load(FIXTURE_SCHEMA)
    fixture_schema_findings = []
    CORE.validate_schema_instance(fixtures, fixture_schema, fixture_schema_findings, base=FIXTURE_SCHEMA.parent, root=fixture_schema)
    for item in fixture_schema_findings:
        finding(findings, "E_FIXTURE_SCHEMA", "fixtures", item["pointer"] + ": " + item["message"])

    expected_markers = {f"// M0-PROVISIONAL: {item}" for item in (
        "boring-cdc-d-pg-protocol", "boring-cdc-d-publication", "boring-cdc-d-ddl",
        "boring-cdc-d-backfill", "boring-cdc-d-wal-cap", "boring-cdc-d-failure-policy",
        "boring-cdc-d-keys",
    )}
    text = CONTRACT.read_text() + SQL.read_text()
    if set(contract.get("provisional_authorities", [])) != expected_markers:
        finding(findings, "E_PROVISIONAL_INVENTORY", "provisional_authorities", "authority inventory changed")
    for marker in expected_markers:
        if marker not in text:
            finding(findings, "E_PROVISIONAL_MARKER", "contract", marker)

    protocol = contract["protocol"]
    if contract["supported_postgresql"] != [{
        "exact_version": "17.6", "image_manifest": "sha256:00bc86618629af00d2937fdc5a5d63db3ff8450acf52f0636ec813c7f4902929",
        "major": 17, "platform": "linux/amd64", "provisional": "// M0-PROVISIONAL: boring-cdc-d-pg-protocol"
    }]:
        finding(findings, "E_SERVER_MATRIX", "supported_postgresql", "only pinned PostgreSQL 17.6 linux/amd64 is admitted")
    required_options = ["proto_version", "publication_names", "binary", "messages", "streaming", "two_phase", "origin"]
    if protocol["option_order"] != required_options or any(value is not False for value in (protocol["binary"], protocol["streaming"], protocol["two_phase"])) or protocol["origin"] != "any":
        finding(findings, "E_PROTOCOL_OPTIONS", "protocol", "logical-replication options changed")
    if (protocol["transport"]["crate"], protocol["transport"]["version"], protocol["transport"]["feature"]) != ("pgwire-replication", "0.4.0", "tls-rustls,scram"):
        finding(findings, "E_TRANSPORT", "protocol/transport", "CopyBoth transport pin changed")
    if "START_REPLICATION SLOT" not in protocol["start_replication_template"] or "origin 'any'" not in protocol["start_replication_template"]:
        finding(findings, "E_START_REPLICATION", "protocol/start_replication_template", "template is incomplete")

    state_fields = {item["name"]: item for item in contract["source_state_fields"]}
    for name in ("capture_epoch", "durable_transaction_end_lsn", "slot_creation_floor_lsn", "last_feedback_lsn", "server_confirmed_flush_lsn", "wal_status", "safe_wal_size_bytes"):
        if name not in state_fields:
            finding(findings, "E_SOURCE_FIELD", "source_state_fields", name)
    if state_fields.get("durable_transaction_end_lsn", {}).get("constructor") != "atomic journal commit only":
        finding(findings, "E_TYPED_BOUNDARY", "source_state_fields/durable_transaction_end_lsn", "constructor widened")

    feedback = contract["feedback"]
    if feedback["effective_server_restart"] != "max(requested_lsn, server_confirmed_flush_lsn)":
        finding(findings, "E_RESTART_RULE", "feedback", "restart rule changed")
    if set(feedback["forbidden_inputs"]) != {"primary keepalive wal_end", "last_received_lsn", "slot creation floor as durable transaction", "destination checkpoint", "clock-derived LSN"}:
        finding(findings, "E_FEEDBACK_SOURCE", "feedback/forbidden_inputs", "unsafe feedback input set changed")

    publication = contract["publication"]
    if publication["publish"] != ["insert", "update", "delete", "truncate"] or publication["truncate"] != "detection_only_requires_reseed":
        finding(findings, "E_PUBLICATION", "publication", "publication safety operations changed")
    for table in ("heartbeat", "capture_fences"):
        if contract["control_relations"][table]["immutable_key"] != {"id": 1}:
            finding(findings, "E_CONTROL_KEY", f"control_relations/{table}", "fixed key changed")

    sql = SQL.read_text()
    required_sql = (
        "CREATE ROLE boring_cdc_admin NOLOGIN", "CREATE ROLE boring_cdc_capture LOGIN REPLICATION",
        "REVOKE ALL ON ALL TABLES IN SCHEMA boring_cdc_control FROM PUBLIC",
        "GRANT SELECT (id) ON boring_cdc_control.heartbeat, boring_cdc_control.capture_fences",
        "GRANT UPDATE (nonce, updated_at) ON boring_cdc_control.heartbeat",
        "GRANT UPDATE (capture_epoch, generation, table_set_fingerprint, unique_nonce)",
        "publish = 'insert, update, delete, truncate'", "ALTER PUBLICATION boring_cdc OWNER TO boring_cdc_admin",
    )
    for fragment in required_sql:
        if fragment not in sql:
            finding(findings, "E_GRANT_SQL", "contracts/postgres/roles-grants.sql", fragment)
    if re.search(r"GRANT\s+(INSERT|DELETE|ALL).*boring_cdc_control", sql, re.I):
        finding(findings, "E_EXCESS_GRANT", "contracts/postgres/roles-grants.sql", "control writer has excess privilege")

    ddl = contract["ddl_matrix"]
    if len(ddl) < 10 or any(row["minimum_lock"] != "ACCESS EXCLUSIVE" or row["guard_conflicts"] is not True for row in ddl):
        finding(findings, "E_DDL_MATRIX", "ddl_matrix", "finite admitted DDL conflict proof is incomplete")
    if not all(term in contract["bootstrap"]["initial"] for term in ("acquire canonical ACCESS SHARE DDL guard and verify contracts", "each importer SET TRANSACTION SNAPSHOT as first statement", "publish and durably observe unique capture fence")):
        finding(findings, "E_BOOTSTRAP_SEQUENCE", "bootstrap/initial", "required ordered bootstrap phases absent")

    wal = contract["wal_headroom"]
    expected_wal = (68719476736, 300, 30, 30, 120, {"warning": 1800, "action": 900, "critical": 300, "hard": 0})
    observed_wal = (wal["max_slot_wal_keep_size_bytes"], wal["rate_window_seconds"], wal["metric_freshness_seconds"], wal["monitoring_delay_seconds"], wal["reaction_reserve_seconds"], wal["horizons_seconds"])
    if observed_wal != expected_wal:
        finding(findings, "E_WAL_LITERALS", "wal_headroom", "recommended WAL constants changed")

    fixture_ids = contract["fixture_ids"]
    cases = fixtures.get("cases", [])
    case_ids = [case.get("fixture_id") for case in cases]
    if fixture_ids != case_ids or len(set(case_ids)) != len(case_ids):
        finding(findings, "E_FIXTURE_INVENTORY", "fixtures", "ordered contract/fixture inventory differs or duplicates")
    if {case.get("executor_id") for case in cases} - set(contract["executors"]):
        finding(findings, "E_EXECUTOR_MAP", "fixtures", "fixture executor is not declared")
    required_expected = {"state", "exit_code", "checkpoint", "feedback", "external_effect", "status_code", "metric"}
    for index, case in enumerate(cases):
        if set(case.get("expected", {})) != required_expected or not case.get("hook") or not case.get("preconditions") or case.get("seed") != "0x50474344434d305631":
            finding(findings, "E_FIXTURE_SHAPE", f"cases/{index}", "fixture is not mechanically executable")
        if case["expected"]["checkpoint"] not in ("unchanged", "advanced_to_fence_transaction"):
            finding(findings, "E_CHECKPOINT_BOUNDARY", f"cases/{index}", "fixture permits partial checkpoint")
    compound = [case for case in cases if case["fixture_id"] in {
        "SCN-M0-PG-CREATION-FLOOR-NULL", "SCN-M0-PG-CREATION-FLOOR-EQUAL",
        "SCN-M0-PG-CREATION-FLOOR-INVALID-SLOT", "SCN-M0-PG-CREATION-FLOOR-WAL-UNAVAILABLE",
    }]
    if len(compound) != 4 or any(case["inputs"].get("durable_transaction_end_lsn") is not None for case in compound):
        finding(findings, "E_CREATION_FLOOR", "fixtures", "compound creation-floor matrix incomplete")
    rejected = {case["fixture_id"]: case["expected"]["state"] for case in compound}
    if rejected.get("SCN-M0-PG-CREATION-FLOOR-INVALID-SLOT") != "requires_reseed" or rejected.get("SCN-M0-PG-CREATION-FLOOR-WAL-UNAVAILABLE") != "requires_reseed":
        finding(findings, "E_CREATION_FLOOR_PREDICATES", "fixtures", "slot validity and WAL availability are not independently required")

    inputs = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in (CONTRACT, SCHEMA, FIXTURE_SCHEMA, FIXTURES, SQL)}
    return findings, inputs


def main():
    findings, inputs = validate()
    evidence = {
        "schema_version": "m0-postgres-contract-evidence/v1", "owner_bead": OWNER,
        "status": "pass" if not findings else "fail", "validator": "scripts/validate/postgres_contract.py",
        "inputs": inputs, "fixture_count": len(load(FIXTURES)["cases"]), "findings": findings,
        "runtime_observed": False, "product_faults": "fault_not_applicable",
    }
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    print(json.dumps(evidence, sort_keys=True, separators=(",", ":")))
    return 0 if not findings else 1


if __name__ == "__main__":
    raise SystemExit(main())
