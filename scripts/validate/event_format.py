#!/usr/bin/env python3
"""Validate the M0 public event ABI, schema, and golden vectors."""
from __future__ import annotations
import base64, hashlib, importlib.util, json, re, struct, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OWNER = "boring-cdc-m0-event-format"
CONTRACT = ROOT / "contracts/event/event-format.json"
SCHEMA = ROOT / "contracts/event/event.schema.json"
VECTORS = ROOT / "fixtures/m0/event-format/golden-vectors.json"
DOC = ROOT / "docs/EVENT_FORMAT.md"
EVIDENCE = ROOT / "artifacts/boring-cdc-m0-event-format/spec/evidence.json"
HEX32 = re.compile(r"^[0-9a-f]{64}$")
CORE_SPEC = importlib.util.spec_from_file_location("core_validator", ROOT / "scripts/lib/core_validator.py")
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


def encode_key(key: list[dict]) -> bytes:
    encoded = bytearray(u(len(key), 4))
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
    return bytes(encoded)


def key_hash(key: list[dict]) -> str:
    return digest("boring-cdc/physical-key/v1", [encode_key(key)])


def wal_id(v: dict) -> str:
    return digest("boring-cdc/wal-event/v1", [u(v["capture_epoch"],8), bytes.fromhex(v["source_slot_identity"]), u(v["transaction_end_lsn"],8), u(v["row_ordinal"],8), u(v["mutation_ordinal"],1)])


def snapshot_id(v: dict) -> str:
    return digest("boring-cdc/snapshot-event/v1", [u(v["capture_epoch"],8),u(v["generation"],8),bytes.fromhex(v["logical_table_id"]),u(v["chunk_id"],8),encode_key(v["canonical_key"])])


def control_payload_hash(v: dict) -> str:
    control=v["control"]; generation=control.get("generation")
    return digest("boring-cdc/control-payload/v1", [bytes.fromhex(v["connector_event_id"]),control["kind"].encode(),u(control["transaction_end_lsn"],8),b"" if generation is None else u(generation,8)])


def payload_hash(v: dict) -> str:
    states = bytearray(u(len(v["columns"]),8)); state_tags={"absent_for_schema":0,"explicit_null":1,"unchanged_toast":2,"explicit_value":3}
    for col in v["columns"]:
        states.extend(u(col["column_id"],4))
        states.append(state_tags[col["state"]])
        if col["state"] == "explicit_value":
            raw=base64.urlsafe_b64decode(col["bytes"] + "=" * (-len(col["bytes"]) % 4))
            states.extend(u(col["type_oid"],4)); states.extend(int(col["type_modifier"]).to_bytes(4,"big",signed=True)); states.extend(u(len(raw),8)); states.extend(raw)
    sv=v["source_version"]
    return digest("boring-cdc/mutation-payload/v1", [u(v["capture_epoch"],8),u(sv["lsn_u64"],8),u(sv["origin_rank"],1),u(sv["transaction_ordinal"],4),u(sv["mutation_ordinal"],1),bytes.fromhex(v["connector_event_id"]),bytes.fromhex(v["relation_fingerprint"]),encode_key(v["canonical_key"]),bytes.fromhex(v["key_hash"]),u({"delete":0,"upsert":1}[v["mutation_kind"]],1),bytes(states)])


def canonical_case_bytes(case: dict) -> bytes:
    kind,value=case["kind"],case["input"]
    if kind=="bool": return bytes([1 if value else 0])
    if kind in ("int2","int4","int8"): return int(value).to_bytes({"int2":2,"int4":4,"int8":8}[kind],"big",signed=True)
    if kind=="oid": return int(value).to_bytes(4,"big")
    if kind=="float4": return bytes.fromhex("7fc00000") if value=="NaN" else struct.pack(">f",float(value))
    if kind=="float8": return bytes.fromhex("7ff8000000000000") if value=="NaN" else struct.pack(">d",float(value))
    if kind=="numeric": return value.encode()
    if kind=="date": return int(value).to_bytes(4,"big",signed=True)
    if kind in ("timestamp","timestamptz"): return int(value).to_bytes(8,"big",signed=True)
    if kind=="uuid": return bytes.fromhex(value.replace("-",""))
    if kind=="text": return value.encode()
    if kind=="bytea": return bytes.fromhex(value)
    if kind=="array-int4":
        out=bytearray(u(len(value),4))
        for item in value:
            out.append(1 if item is None else 3)
            if item is not None:
                raw=int(item).to_bytes(4,"big",signed=True);out.extend(u(23,4));out.extend((-1).to_bytes(4,"big",signed=True));out.extend(u(len(raw),8));out.extend(raw)
        return bytes(out)
    raise ValueError(kind)


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
    ids=set(); expected_categories={"identity","order","types","limits","toast","schema","stability","control_routing"}
    categories=set()
    def check_event(event,case,p):
        actual=wal_id(case["identity_input"]) if case["identity_kind"]=="wal" else snapshot_id(case["identity_input"]) if case["identity_kind"]=="snapshot" else event["connector_event_id"]
        if event["connector_event_id"] != actual: fail(findings,"E_EVENT_ID",p,"connector_event_id mismatch")
        if event["source_version"]["connector_event_id"] != actual: fail(findings,"E_SOURCE_VERSION_ID",p,"source-version tie-breaker mismatch")
        schema_findings=[]; CORE.validate_schema_instance(event,schema,schema_findings,base=SCHEMA.parent,root=schema)
        for item in schema_findings: fail(findings,"E_EVENT_SCHEMA",p,item["pointer"]+": "+item["message"])
        if event["event_type"]=="mutation":
            actual_key=key_hash(event["canonical_key"])
            if event["key_hash"] != actual_key: fail(findings,"E_KEY_HASH",p,"key_hash mismatch")
            if event["payload_hash"] != payload_hash(event): fail(findings,"E_PAYLOAD_HASH",p,"payload_hash mismatch")
        elif event["payload_hash"] != control_payload_hash(event): fail(findings,"E_PAYLOAD_HASH",p,"control payload_hash mismatch")
    for i,case in enumerate(vectors["vectors"]):
        p=f"vectors/{i}"; cid=case["fixture_id"]
        if cid in ids: fail(findings,"E_DUPLICATE_FIXTURE",p,cid)
        ids.add(cid); categories.add(case["category"])
        if case["expected_outcome"]=="emit": check_event(case["event"],case,p)
        elif case["expected_outcome"]=="emit_sequence":
            if len(case.get("events",[]))<2: fail(findings,"E_SEQUENCE",p,"sequence requires at least two events")
            for n,event in enumerate(case.get("events",[])): check_event(event,{**case,"identity_input":case["identity_inputs"][n]},f"{p}/events/{n}")
        elif case["expected_outcome"]=="canonicalize":
            observed={item["name"]:canonical_case_bytes(item).hex() for item in case["cases"]}
            expected={item["name"]:item["expected_hex"] for item in case["cases"]}
            if observed != expected: fail(findings,"E_CANONICAL_VALUE",p,"canonical value corpus mismatch")
        elif case["expected_outcome"]=="admit":
            if case.get("expected",{}).get("older_projection") != "absent_for_schema_to_destination_null": fail(findings,"E_ADDITIVE",p,"compatible additive projection absent")
        else:
            if "input" not in case: fail(findings,"E_FAILURE_INPUT",p,"blocking vector requires exact input")
            for key in ("failure_class","failure_code","failed_boundary","checkpoint","feedback","recovery"):
                if key not in case: fail(findings,"E_FAILURE_FIELD",p,key)
    emitted=next(c["event"] for c in vectors["vectors"] if c["expected_outcome"]=="emit" and c["event"]["event_type"]=="mutation")
    invalid=[]
    bad=json.loads(json.dumps(emitted)); bad["columns"][0].pop("bytes"); invalid.append(bad)
    bad=json.loads(json.dumps(emitted)); bad["canonical_key"][0]["value"]={}; invalid.append(bad)
    bad=next(json.loads(json.dumps(c["event"])) for c in vectors["vectors"] if c.get("event",{}).get("event_type")=="control"); bad["logical_table_id"]="0"*64; invalid.append(bad)
    for n,bad in enumerate(invalid):
        local=[];CORE.validate_schema_instance(bad,schema,local,base=SCHEMA.parent,root=schema)
        if not local: fail(findings,"E_SCHEMA_FAIL_OPEN",f"schema-negative/{n}","malformed event was accepted")
    if categories != expected_categories: fail(findings,"E_CATEGORY_COVERAGE","vectors",str(sorted(expected_categories-categories)))
    declared=set(contract["fixture_ids"])
    if declared != ids: fail(findings,"E_FIXTURE_INVENTORY","contract/fixture_ids","contract and vector IDs differ")
    if contract["limits"]["canonical_key_components"] != 32 or contract["limits"]["key_component_bytes"] != 1024 or contract["limits"]["scalar_bytes"] != 1048576 or contract["limits"]["row_bytes"] != 4194304 or contract["limits"]["event_bytes"] != 8388608: fail(findings,"E_LIMIT_LITERAL","contract/limits","recommended limits changed")
    if contract["hashing"]["algorithm"] != "SHA-256" or contract["hashing"]["field_framing"] != "u64-be byte length followed by bytes": fail(findings,"E_HASH_LITERAL","contract/hashing","hash ABI changed")
    relation_terms=("attnum","logical ordinal","dropped","collation","default-expression","generated-expression","identity-expression","replica mode","replica-index","partition","publication membership","column-projection")
    if any(term not in contract["relation_contract_encoding"] for term in relation_terms): fail(findings,"E_RELATION_CONTRACT","contract/relation_contract_encoding","required relation input absent")
    if schema.get("$id") != "https://boring-cdc.dev/contracts/event/event.schema.json" or any(schema["$defs"][name].get("additionalProperties") is not False for name in ("mutation", "controlEvent")): fail(findings,"E_SCHEMA","schema","public schema identity/closure changed")
    bundle={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in (DOC,SCHEMA,CONTRACT,VECTORS)}
    return findings,bundle


def main():
    findings,bundle=validate(); status="pass" if not findings else "fail"
    evidence={"schema_version":"m0-event-format-evidence/v1","owner_bead":OWNER,"status":status,"validator":"scripts/validate/event_format.py","inputs":bundle,"fixture_count":len(load(VECTORS)["vectors"]),"findings":findings,"runtime_observed":False,"product_faults":"fault_not_applicable"}
    EVIDENCE.parent.mkdir(parents=True,exist_ok=True); EVIDENCE.write_text(json.dumps(evidence,indent=2,sort_keys=True)+"\n")
    print(json.dumps(evidence,sort_keys=True,separators=(",",":")))
    return 0 if not findings else 1
if __name__ == "__main__": raise SystemExit(main())
