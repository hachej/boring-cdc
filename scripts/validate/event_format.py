#!/usr/bin/env python3
"""Validate the M0 public event ABI, schema, and golden vectors."""
from __future__ import annotations
import base64, hashlib, json, re, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OWNER = "boring-cdc-m0-event-format"
CONTRACT = ROOT / "contracts/event/event-format.json"
SCHEMA = ROOT / "contracts/event/event.schema.json"
VECTORS = ROOT / "fixtures/m0/event-format/golden-vectors.json"
DOC = ROOT / "docs/EVENT_FORMAT.md"
EVIDENCE = ROOT / "artifacts/boring-cdc-m0-event-format/spec/evidence.json"
HEX32 = re.compile(r"^[0-9a-f]{64}$")


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


def key_hash(key: list[dict]) -> str:
    encoded = bytearray()
    tags = {"bool": 1, "int64": 2, "uint64": 3, "bytes": 4, "text": 5}
    for component in key:
        kind, value = component["kind"], component["value"]
        encoded.append(tags[kind])
        if kind == "bool": encoded.append(1 if value else 0)
        elif kind == "int64": encoded.extend(int(value).to_bytes(8, "big", signed=True))
        elif kind == "uint64": encoded.extend(int(value).to_bytes(8, "big"))
        else:
            raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)) if kind == "bytes" else value.encode()
            encoded.extend(u(len(raw), 8)); encoded.extend(raw)
    return digest("boring-cdc/physical-key/v1", [bytes(encoded)])


def wal_id(v: dict) -> str:
    return digest("boring-cdc/wal-event/v1", [u(v["capture_epoch"],8), bytes.fromhex(v["source_slot_identity"]), u(v["transaction_end_lsn"],8), u(v["row_ordinal"],8), u(v["mutation_ordinal"],1)])


def snapshot_id(v: dict) -> str:
    return digest("boring-cdc/snapshot-event/v1", [u(v["capture_epoch"],8),u(v["generation"],8),bytes.fromhex(v["logical_table_id"]),u(v["chunk_id"],8),bytes.fromhex(v["key_hash"])])


def control_payload_hash(v: dict) -> str:
    control=v["control"]; generation=control.get("generation")
    return digest("boring-cdc/control-payload/v1", [bytes.fromhex(v["connector_event_id"]),control["kind"].encode(),u(control["transaction_end_lsn"],8),b"" if generation is None else u(generation,8)])


def payload_hash(v: dict) -> str:
    states = bytearray(u(len(v["columns"]),8)); state_tags={"absent_for_schema":0,"explicit_null":1,"unchanged_toast":2,"explicit_value":3}
    for col in v["columns"]:
        states.append(state_tags[col["state"]])
        if col["state"] == "explicit_value":
            raw=base64.urlsafe_b64decode(col["bytes"] + "=" * (-len(col["bytes"]) % 4))
            states.extend(u(col["type_oid"],4)); states.extend(int(col["type_modifier"]).to_bytes(4,"big",signed=True)); states.extend(u(len(raw),8)); states.extend(raw)
    sv=v["source_version"]
    return digest("boring-cdc/mutation-payload/v1", [u(sv["capture_epoch"],8),u(sv["commit_lsn"],8),u(sv["origin_rank"],1),u(sv["transaction_id"],4),u(sv["transaction_ordinal"],4),u(sv["mutation_ordinal"],1),bytes.fromhex(v["connector_event_id"]),bytes.fromhex(v["relation_fingerprint"]),bytes.fromhex(v["key_hash"]),u({"delete":0,"upsert":1}[v["mutation_kind"]],1),bytes(states)])


def fail(findings, code, where, message): findings.append({"code":code,"path":where,"message":message})


def validate() -> tuple[list[dict],dict]:
    findings=[]
    contract,schema,vectors=load(CONTRACT),load(SCHEMA),load(VECTORS)
    required_sections=["Envelope and field grammar","Canonical value encodings","Identity and total source order","Relation and row identity","TOAST and schema evolution","Control routing","Golden vectors","Compatibility and versioning"]
    doc=DOC.read_text()
    for title in required_sections:
        if f"## {title}" not in doc: fail(findings,"E_DOC_SECTION","docs/EVENT_FORMAT.md",title)
    for marker in contract["provisional_authorities"]:
        if marker not in doc and marker not in CONTRACT.read_text(): fail(findings,"E_PROVISIONAL","contract",marker)
    ids=set(); expected_categories={"identity","order","types","toast","control_routing"}
    categories=set()
    for i,case in enumerate(vectors["vectors"]):
        p=f"vectors/{i}"; cid=case["fixture_id"]
        if cid in ids: fail(findings,"E_DUPLICATE_FIXTURE",p,cid)
        ids.add(cid); categories.add(case["category"])
        if case["expected_outcome"]=="emit":
            event=case["event"]
            if case["identity_kind"]=="wal": actual=wal_id(case["identity_input"])
            elif case["identity_kind"]=="snapshot": actual=snapshot_id(case["identity_input"])
            else: actual=event["connector_event_id"]
            if event["connector_event_id"] != actual: fail(findings,"E_EVENT_ID",p,"connector_event_id mismatch")
            required={"schema_version","event_type","connector_event_id","capture_epoch","source_version","routing","payload_hash"}
            if not required <= set(event): fail(findings,"E_EVENT_SCHEMA",p,"required envelope field absent")
            if event["event_type"]=="mutation":
                if event["routing"] != "business" or "control" in event: fail(findings,"E_EVENT_SCHEMA",p,"mutation routing/shape invalid")
                actual_key=key_hash(event["canonical_key"])
                if event["key_hash"] != actual_key: fail(findings,"E_KEY_HASH",p,"key_hash mismatch")
                if event["payload_hash"] != payload_hash(event): fail(findings,"E_PAYLOAD_HASH",p,"payload_hash mismatch")
            elif event["event_type"]=="control":
                if event["routing"] != "control" or any(k in event for k in ("canonical_key","columns","key_hash","relation_fingerprint")): fail(findings,"E_EVENT_SCHEMA",p,"control routing/shape invalid")
                if event["payload_hash"] != control_payload_hash(event): fail(findings,"E_PAYLOAD_HASH",p,"control payload_hash mismatch")
            else: fail(findings,"E_EVENT_SCHEMA",p,"unknown event type")
        else:
            for key in ("failure_class","failure_code","failed_boundary","checkpoint","feedback","recovery"):
                if key not in case: fail(findings,"E_FAILURE_FIELD",p,key)
    if categories != expected_categories: fail(findings,"E_CATEGORY_COVERAGE","vectors",str(sorted(expected_categories-categories)))
    declared=set(contract["fixture_ids"])
    if declared != ids: fail(findings,"E_FIXTURE_INVENTORY","contract/fixture_ids","contract and vector IDs differ")
    if contract["limits"]["canonical_key_components"] != 32 or contract["limits"]["key_component_bytes"] != 1024 or contract["limits"]["scalar_bytes"] != 1048576 or contract["limits"]["row_bytes"] != 4194304 or contract["limits"]["event_bytes"] != 8388608: fail(findings,"E_LIMIT_LITERAL","contract/limits","recommended limits changed")
    if contract["hashing"]["algorithm"] != "SHA-256" or contract["hashing"]["field_framing"] != "u64-be byte length followed by bytes": fail(findings,"E_HASH_LITERAL","contract/hashing","hash ABI changed")
    if schema.get("$id") != "https://boring-cdc.dev/contracts/event/event.schema.json" or schema.get("additionalProperties") is not False: fail(findings,"E_SCHEMA","schema","public schema identity/closure changed")
    bundle={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in (DOC,SCHEMA,CONTRACT,VECTORS)}
    return findings,bundle


def main():
    findings,bundle=validate(); status="pass" if not findings else "fail"
    evidence={"schema_version":"m0-event-format-evidence/v1","owner_bead":OWNER,"status":status,"validator":"scripts/validate/event_format.py","inputs":bundle,"fixture_count":len(load(VECTORS)["vectors"]),"findings":findings,"runtime_observed":False,"product_faults":"fault_not_applicable"}
    EVIDENCE.parent.mkdir(parents=True,exist_ok=True); EVIDENCE.write_text(json.dumps(evidence,indent=2,sort_keys=True)+"\n")
    print(json.dumps(evidence,sort_keys=True,separators=(",",":")))
    return 0 if not findings else 1
if __name__ == "__main__": raise SystemExit(main())
