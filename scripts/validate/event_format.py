#!/usr/bin/env python3
"""Validate the M0 public event ABI, schema, and golden vectors."""
from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
import re
import struct
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OWNER = "boring-cdc-m0-event-format"
CONTRACT = ROOT / "contracts/event/event-format.json"
SCHEMA = ROOT / "contracts/event/event.schema.json"
VECTORS = ROOT / "fixtures/m0/event-format/golden-vectors.json"
DOC = ROOT / "docs/EVENT_FORMAT.md"
EVIDENCE = ROOT / "artifacts/boring-cdc-m0-event-format/spec/evidence.json"
U64_MAX = (1 << 64) - 1
CORE_SPEC = importlib.util.spec_from_file_location(
    "core_validator", ROOT / "scripts/lib/core_validator.py"
)
CORE = importlib.util.module_from_spec(CORE_SPEC)
CORE_SPEC.loader.exec_module(CORE)


def load(path: Path):
    return json.loads(path.read_text())


def field(value: bytes) -> bytes:
    return len(value).to_bytes(8, "big") + value


def digest(domain: str, fields: list[bytes]) -> str:
    h = hashlib.sha256()
    h.update(field(domain.encode()))
    for value in fields:
        h.update(field(value))
    return h.hexdigest()


def u(value: int, width: int) -> bytes:
    return value.to_bytes(width, "big", signed=False)


def signed(value: int, width: int) -> bytes:
    return value.to_bytes(width, "big", signed=True)


def optional_hash(value: str | None) -> bytes:
    return b"" if value is None else bytes.fromhex(value)


def source_slot_identity(value: dict) -> str:
    return digest(
        "boring-cdc/source-slot/v1",
        [
            u(value["system_identifier"], 8),
            u(value["timeline"], 4),
            value["database_identity"].encode(),
            value["slot"].encode(),
            value["plugin"].encode(),
        ],
    )


def logical_table_id(value: dict) -> str:
    return digest(
        "boring-cdc/logical-table/v1",
        [
            u(value["system_identifier"], 8),
            value["database_identity"].encode(),
            value["schema"].encode(),
            value["table"].encode(),
        ],
    )


def relation_contract_bytes(value: dict) -> bytes:
    out = bytearray()
    out.extend(field(bytes.fromhex(value["logical_table_id"])))
    out.extend(u(value["relation_id"], 4))
    columns = sorted(value["columns"], key=lambda item: item["attnum"])
    out.extend(u(len(columns), 4))
    for column in columns:
        out.extend(signed(column["attnum"], 2))
        out.extend(u(0xFFFFFFFF if column["logical_ordinal"] is None else column["logical_ordinal"], 4))
        out.extend(field(column["name"].encode()))
        out.append(1 if column["dropped"] else 0)
        out.extend(u(column["type_oid"], 4))
        out.extend(signed(column["typmod"], 4))
        out.extend(u(column["collation_oid"], 4))
        out.append(1 if column["nullable"] else 0)
        for name in ("default_expression_hash", "generated_expression_hash", "identity_expression_hash"):
            out.extend(field(optional_hash(column[name])))
    out.append({"default": 0, "nothing": 1, "full": 2, "index": 3}[value["replica_mode"]])
    for name in (
        "replica_index_definition_hash",
        "effective_key_definition_hash",
        "partition_root_logical_table_id",
        "partition_routing_definition_hash",
        "partition_key_hash",
        "partition_bounds_hash",
    ):
        out.extend(field(optional_hash(value[name])))
    out.append(value["publication_action_bits"])
    out.append(1 if value["publication_member"] else 0)
    out.extend(field(optional_hash(value["publication_row_filter_hash"])))
    out.extend(field(optional_hash(value["publication_column_projection_hash"])))
    return bytes(out)


def relation_fingerprint(value: dict) -> str:
    return digest("boring-cdc/relation-schema/v1", [relation_contract_bytes(value)])


def decode_b64url(value: str) -> bytes:
    if not re.fullmatch(r"(?:[A-Za-z0-9_-]{4})*(?:[A-Za-z0-9_-]{2}|[A-Za-z0-9_-]{3})?", value):
        raise ValueError("invalid unpadded base64url")
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def encode_key(key: list[dict]) -> bytes:
    encoded = bytearray(u(len(key), 4))
    tags = {"bool": 1, "int64": 2, "uint64": 3, "bytes": 4, "text": 5}
    for component in key:
        kind, value = component["kind"], component["value"]
        encoded.append(tags[kind])
        encoded.extend(u(component["type_oid"], 4))
        encoded.extend(signed(component["type_modifier"], 4))
        if kind == "bool":
            encoded.append(1 if value else 0)
        elif kind == "int64":
            encoded.extend(signed(int(value), 8))
        elif kind == "uint64":
            encoded.extend(u(int(value), 8))
        else:
            raw = decode_b64url(value) if kind == "bytes" else value.encode()
            encoded.extend(u(len(raw), 8))
            encoded.extend(raw)
    return bytes(encoded)


def key_hash(key: list[dict]) -> str:
    return digest("boring-cdc/physical-key/v1", [encode_key(key)])


def wal_id(value: dict) -> str:
    return digest(
        "boring-cdc/wal-event/v1",
        [
            u(value["capture_epoch"], 8),
            bytes.fromhex(value["source_slot_identity"]),
            u(value["transaction_end_lsn"], 8),
            u(value["row_ordinal"], 8),
            u(value["mutation_ordinal"], 1),
        ],
    )


def snapshot_id(value: dict) -> str:
    return digest(
        "boring-cdc/snapshot-event/v1",
        [
            u(value["capture_epoch"], 8),
            u(value["generation"], 8),
            bytes.fromhex(value["logical_table_id"]),
            u(value["chunk_id"], 8),
            encode_key(value["canonical_key"]),
        ],
    )


def control_payload_hash(value: dict) -> str:
    control = value["control"]
    generation = control.get("generation")
    return digest(
        "boring-cdc/control-payload/v1",
        [
            bytes.fromhex(value["connector_event_id"]),
            control["kind"].encode(),
            u(control["transaction_end_lsn"], 8),
            b"" if generation is None else u(generation, 8),
        ],
    )


def payload_hash(value: dict) -> str:
    states = bytearray(u(len(value["columns"]), 8))
    state_tags = {"absent_for_schema": 0, "explicit_null": 1, "unchanged_toast": 2, "explicit_value": 3}
    for column in value["columns"]:
        states.extend(u(column["column_id"], 4))
        states.append(state_tags[column["state"]])
        if column["state"] == "explicit_value":
            raw = decode_b64url(column["bytes"])
            states.extend(u(column["type_oid"], 4))
            states.extend(signed(column["type_modifier"], 4))
            states.extend(u(len(raw), 8))
            states.extend(raw)
    source_version = value["source_version"]
    before_key = b"" if value.get("before_key") is None else encode_key(value["before_key"])
    return digest(
        "boring-cdc/mutation-payload/v1",
        [
            u(value["capture_epoch"], 8),
            u(source_version["lsn_u64"], 8),
            u(source_version["origin_rank"], 1),
            u(source_version["transaction_ordinal"], 4),
            u(source_version["mutation_ordinal"], 1),
            bytes.fromhex(value["connector_event_id"]),
            bytes.fromhex(value["relation_fingerprint"]),
            encode_key(value["canonical_key"]),
            bytes.fromhex(value["key_hash"]),
            u({"snapshot": 0, "insert": 1, "update": 2, "delete": 3}[value["operation"]], 1),
            before_key,
            u({"delete": 0, "upsert": 1}[value["mutation_kind"]], 1),
            bytes(states),
        ],
    )


def canonical_case_bytes(case: dict) -> bytes:
    kind, value = case["kind"], case["input"]
    if kind == "bool":
        return bytes([1 if value else 0])
    if kind in ("int2", "int4", "int8"):
        return signed(int(value), {"int2": 2, "int4": 4, "int8": 8}[kind])
    if kind == "oid":
        return u(int(value), 4)
    if kind == "float4":
        return bytes.fromhex("7fc00000") if value == "NaN" else struct.pack(">f", float(value))
    if kind == "float8":
        return bytes.fromhex("7ff8000000000000") if value == "NaN" else struct.pack(">d", float(value))
    if kind == "numeric":
        return value.encode()
    if kind == "date":
        return signed(int(value), 4)
    if kind in ("timestamp", "timestamptz"):
        return signed(int(value), 8)
    if kind == "uuid":
        return bytes.fromhex(value.replace("-", ""))
    if kind == "text":
        return value.encode()
    if kind == "bytea":
        return bytes.fromhex(value)
    if kind == "array-int4":
        out = bytearray(u(len(value), 4))
        for item in value:
            out.append(1 if item is None else 3)
            if item is not None:
                raw = signed(int(item), 4)
                out.extend(u(23, 4))
                out.extend(signed(-1, 4))
                out.extend(u(len(raw), 8))
                out.extend(raw)
        return bytes(out)
    raise ValueError(kind)


def fail(findings, code, where, message):
    findings.append({"code": code, "path": where, "message": message})


def event_semantic_findings(event: dict) -> list[str]:
    issues = []
    try:
        if event["source_version"]["connector_event_id"] != event["connector_event_id"]:
            issues.append("source-version tie-breaker mismatch")
        if event["event_type"] == "mutation":
            ids = [column["column_id"] for column in event["columns"]]
            if ids != sorted(set(ids)):
                issues.append("column IDs must be unique and ascending")
            if any(
                component["kind"] in ("bytes", "text")
                and len(decode_b64url(component["value"]) if component["kind"] == "bytes" else component["value"].encode()) > 1024
                for component in event["canonical_key"]
            ):
                issues.append("key component exceeds 1024 decoded bytes")
            operation = event["operation"]
            mutation = event["mutation_kind"]
            origin = event["source_version"]["origin_rank"]
            before = event.get("before_key")
            if operation == "snapshot" and (origin != 0 or mutation != "upsert" or before is not None):
                issues.append("snapshot shape is contradictory")
            if operation != "snapshot" and origin != 1:
                issues.append("WAL operation must have WAL origin rank")
            if operation in ("insert", "delete") and before is not None:
                issues.append("insert/delete before_key must be null")
            if operation == "insert" and mutation != "upsert":
                issues.append("insert must be upsert")
            if operation == "delete" and (mutation != "delete" or event["columns"]):
                issues.append("delete must be an empty-column tombstone")
            if operation == "update" and mutation == "delete" and event["columns"]:
                issues.append("key-change tombstone must have no columns")
        elif event["source_version"]["origin_rank"] != 1:
            issues.append("control event must have WAL origin rank")
    except (KeyError, TypeError, ValueError, OverflowError) as error:
        issues.append(f"not canonically encodable: {error}")
    return issues


def validate() -> tuple[list[dict], dict]:
    findings = []
    contract, schema, vectors = load(CONTRACT), load(SCHEMA), load(VECTORS)
    required_sections = [
        "Envelope and field grammar",
        "Canonical value encodings",
        "Identity and total source order",
        "Relation and row identity",
        "TOAST and schema evolution",
        "Control routing",
        "Golden vectors",
        "Compatibility and versioning",
    ]
    document = DOC.read_text()
    for title in required_sections:
        if f"## {title}" not in document:
            fail(findings, "E_DOC_SECTION", "docs/EVENT_FORMAT.md", title)
    if "M0-" + "PROVISIONAL" in document or "M0-" + "PROVISIONAL" in CONTRACT.read_text():
        fail(findings, "E_PROVISIONAL", "contract", "reconciled artifact contains a provisional marker")

    primitives = vectors["identity_primitives"]
    observed_slot = source_slot_identity(primitives["source_slot"]["input"])
    observed_table = logical_table_id(primitives["logical_table"]["input"])
    relation_input = primitives["relation_fingerprint"]["input"]
    for number, component in enumerate(primitives["relation_components"]):
        observed = digest(
            "boring-cdc/relation-component/v1",
            [component["kind"].encode(), component["definition"].encode()],
        )
        if observed != component["expected_sha256"]:
            fail(findings, "E_RELATION_COMPONENT", f"identity_primitives/relation_components/{number}", "digest mismatch")
    if observed_slot != primitives["source_slot"]["expected_sha256"]:
        fail(findings, "E_SOURCE_SLOT_ID", "identity_primitives/source_slot", "digest mismatch")
    if observed_table != primitives["logical_table"]["expected_sha256"]:
        fail(findings, "E_LOGICAL_TABLE_ID", "identity_primitives/logical_table", "digest mismatch")
    if relation_input["logical_table_id"] != observed_table:
        fail(findings, "E_RELATION_TABLE_ID", "identity_primitives/relation_fingerprint", "table ID mismatch")
    if relation_fingerprint(relation_input) != primitives["relation_fingerprint"]["expected_sha256"]:
        fail(findings, "E_RELATION_FINGERPRINT", "identity_primitives/relation_fingerprint", "digest mismatch")

    ids = set()
    expected_categories = {"identity", "order", "types", "limits", "toast", "schema", "stability", "control_routing"}
    categories = set()

    def check_event(event, case, path):
        try:
            actual = (
                wal_id(case["identity_input"])
                if case["identity_kind"] == "wal"
                else snapshot_id(case["identity_input"])
                if case["identity_kind"] == "snapshot"
                else event["connector_event_id"]
            )
            if event["connector_event_id"] != actual:
                fail(findings, "E_EVENT_ID", path, "connector_event_id mismatch")
            schema_findings = []
            CORE.validate_schema_instance(event, schema, schema_findings, base=SCHEMA.parent, root=schema)
            for item in schema_findings:
                fail(findings, "E_EVENT_SCHEMA", path, item["pointer"] + ": " + item["message"])
            for message in event_semantic_findings(event):
                fail(findings, "E_EVENT_SEMANTICS", path, message)
            if event["event_type"] == "mutation":
                if event["key_hash"] != key_hash(event["canonical_key"]):
                    fail(findings, "E_KEY_HASH", path, "key_hash mismatch")
                if event["payload_hash"] != payload_hash(event):
                    fail(findings, "E_PAYLOAD_HASH", path, "payload_hash mismatch")
            elif event["payload_hash"] != control_payload_hash(event):
                fail(findings, "E_PAYLOAD_HASH", path, "control payload_hash mismatch")
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            fail(findings, "E_EVENT_ENCODING", path, str(error))

    for index, case in enumerate(vectors["vectors"]):
        path = f"vectors/{index}"
        fixture_id = case["fixture_id"]
        if fixture_id in ids:
            fail(findings, "E_DUPLICATE_FIXTURE", path, fixture_id)
        ids.add(fixture_id)
        categories.add(case["category"])
        outcome = case["expected_outcome"]
        if outcome == "emit":
            check_event(case["event"], case, path)
        elif outcome == "emit_sequence":
            if len(case.get("events", [])) != 2 or len(case.get("identity_inputs", [])) != 2:
                fail(findings, "E_SEQUENCE", path, "key change requires exactly two events and identities")
            for number, event in enumerate(case.get("events", [])):
                check_event(event, {**case, "identity_input": case["identity_inputs"][number]}, f"{path}/events/{number}")
            if [event.get("mutation_kind") for event in case.get("events", [])] != ["delete", "upsert"]:
                fail(findings, "E_SEQUENCE", path, "key-change sequence must be delete then upsert")
        elif outcome == "canonicalize":
            observed = {item["name"]: canonical_case_bytes(item).hex() for item in case["cases"]}
            expected = {item["name"]: item["expected_hex"] for item in case["cases"]}
            if observed != expected:
                fail(findings, "E_CANONICAL_VALUE", path, "canonical value corpus mismatch")
        elif outcome == "admit":
            expected_input = {"active_backfill": False, "change": "add_column", "generated": False, "has_default": False, "nullable": True}
            if case.get("input") != expected_input or case.get("expected") != {
                "capture": "continue",
                "older_projection": "absent_for_schema_to_destination_null",
                "relation_fingerprint": "changes",
            }:
                fail(findings, "E_ADDITIVE", path, "compatible additive branch changed")
        elif outcome == "block":
            for key in ("input", "failure_class", "failure_code", "failed_boundary", "checkpoint", "feedback", "recovery"):
                if key not in case:
                    fail(findings, "E_FAILURE_FIELD", path, key)
            if case.get("checkpoint") != "unchanged" or case.get("feedback") != "unchanged" or case.get("failed_boundary") != "before_journal_checkpoint_and_source_feedback":
                fail(findings, "E_FAILURE_BOUNDARY", path, "blocking boundary weakened")
            if fixture_id == "SCN-M0-EVENT-TYPE-UNSUPPORTED" and case.get("input", {}).get("type_oid") in contract["array_type_oids"].values():
                fail(findings, "E_UNSUPPORTED_TYPE", path, "unsupported fixture uses admitted type")
            if fixture_id == "SCN-M0-EVENT-LIMIT-BOUNDARIES":
                expected_limits = {"event_bytes": 8388608, "key_component_bytes": 1024, "row_bytes": 4194304, "scalar_bytes": 1048576}
                if case.get("input", {}).get("inclusive") != expected_limits or case.get("input", {}).get("rejected") != {name: value + 1 for name, value in expected_limits.items()}:
                    fail(findings, "E_LIMIT_VECTOR", path, "near/over limit values changed")
            if fixture_id == "SCN-M0-EVENT-TOAST-KEY-CHANGE-BLOCK":
                value = case.get("input", {})
                if value.get("before_key") == value.get("key") or not any(column.get("state") == "unchanged_toast" for column in value.get("columns", [])):
                    fail(findings, "E_TOAST_BLOCK", path, "fixture no longer combines key change and unchanged TOAST")
            if fixture_id == "SCN-M0-EVENT-ADDITIVE-DURING-BACKFILL" and case.get("input") != {
                "active_backfill": True, "change": "add_column", "generated": False, "has_default": False, "nullable": True
            }:
                fail(findings, "E_ADDITIVE_BLOCK", path, "active-backfill branch changed")
        else:
            fail(findings, "E_OUTCOME", path, f"unknown expected outcome {outcome}")

        if fixture_id == "SCN-M0-EVENT-SAME-KEY-ORDER":
            comparison = case.get("comparison", {})
            left, right = comparison.get("left"), comparison.get("right")
            if comparison.get("grouping") != ["capture_epoch", "logical_table_id", "canonical_key"] or comparison.get("cross_epoch") != "incomparable" or not (left and right and tuple(left) < tuple(right) and comparison.get("expected") == "less"):
                fail(findings, "E_SOURCE_ORDER", path, "source-version comparison fixture changed")
        if fixture_id == "SCN-M0-EVENT-RETRY-STABILITY":
            contexts = case.get("execution_contexts", [])
            if len(contexts) != 2 or contexts[0] == contexts[1] or payload_hash(case["event"]) != case["event"]["payload_hash"] or wal_id(case["identity_input"]) != case["event"]["connector_event_id"]:
                fail(findings, "E_RETRY_STABILITY", path, "volatile-context stability proof changed")

    if categories != expected_categories:
        fail(findings, "E_CATEGORY_COVERAGE", "vectors", str(sorted(expected_categories - categories)))
    if set(contract["fixture_ids"]) != ids:
        fail(findings, "E_FIXTURE_INVENTORY", "contract/fixture_ids", "contract and vector IDs differ")
    limits = contract["limits"]
    if (limits["canonical_key_components"], limits["key_component_bytes"], limits["scalar_bytes"], limits["row_bytes"], limits["event_bytes"]) != (32, 1024, 1048576, 4194304, 8388608):
        fail(findings, "E_LIMIT_LITERAL", "contract/limits", "recommended limits changed")
    if contract["hashing"]["algorithm"] != "SHA-256" or contract["hashing"]["field_framing"] != "u64-be byte length followed by bytes":
        fail(findings, "E_HASH_LITERAL", "contract/hashing", "hash ABI changed")
    relation_terms = ("attnum", "logical ordinal", "dropped", "collation", "default-expression", "generated-expression", "identity-expression", "replica mode", "replica-index", "partition", "publication membership", "column-projection")
    if any(term not in contract["relation_contract_encoding"] for term in relation_terms):
        fail(findings, "E_RELATION_CONTRACT", "contract/relation_contract_encoding", "required relation input absent")
    if schema.get("$id") != "https://boring-cdc.dev/contracts/event/event.schema.json" or any(schema["$defs"][name].get("additionalProperties") is not False for name in ("mutation", "controlEvent")):
        fail(findings, "E_SCHEMA", "schema", "public schema identity/closure changed")

    emitted = next(case["event"] for case in vectors["vectors"] if case["expected_outcome"] == "emit" and case["event"]["event_type"] == "mutation")
    malformed = []
    bad = json.loads(json.dumps(emitted)); bad["source_version"]["lsn_u64"] = U64_MAX + 1; malformed.append(bad)
    bad = json.loads(json.dumps(emitted)); bad["columns"][0]["bytes"] = "A"; malformed.append(bad)
    bad = json.loads(json.dumps(emitted)); bad["columns"].append(dict(bad["columns"][0])); malformed.append(bad)
    bad = json.loads(json.dumps(emitted)); bad["operation"] = "delete"; malformed.append(bad)
    bad = json.loads(json.dumps(emitted)); bad["operation"] = "snapshot"; malformed.append(bad)
    for number, bad in enumerate(malformed):
        schema_findings = []
        CORE.validate_schema_instance(bad, schema, schema_findings, base=SCHEMA.parent, root=schema)
        if not schema_findings and not event_semantic_findings(bad):
            fail(findings, "E_SCHEMA_FAIL_OPEN", f"schema-negative/{number}", "malformed event was accepted")

    bundle = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in (DOC, SCHEMA, CONTRACT, VECTORS)}
    return findings, bundle


def main():
    findings, bundle = validate()
    status = "pass" if not findings else "fail"
    evidence = {
        "schema_version": "m0-event-format-evidence/v1",
        "owner_bead": OWNER,
        "status": status,
        "validator": "scripts/validate/event_format.py",
        "inputs": bundle,
        "fixture_count": len(load(VECTORS)["vectors"]),
        "findings": findings,
        "runtime_observed": False,
        "product_faults": "fault_not_applicable",
    }
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    print(json.dumps(evidence, sort_keys=True, separators=(",", ":")))
    return 0 if not findings else 1


if __name__ == "__main__":
    raise SystemExit(main())
