#!/usr/bin/env python3
import hashlib
import importlib.util
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OWNER = "boring-cdc-m0-archive-model"
C = ROOT / "contracts/archive/archive-model.json"
S = ROOT / "contracts/archive/archive-model.schema.json"
F = ROOT / "fixtures/m0/archive/scenarios.json"
FS = ROOT / "contracts/archive/archive-fixtures.schema.json"
RS = ROOT / "contracts/archive/archive-result.schema.json"
E = ROOT / "artifacts/boring-cdc-m0-archive-model/spec/evidence.json"
M = ROOT / "contracts/m0/manifest.json"
A = ROOT / "contracts/m0/artifacts.json"
V = Path(__file__)
core_spec = importlib.util.spec_from_file_location("core_validator", ROOT / "scripts/lib/core_validator.py")
core = importlib.util.module_from_spec(core_spec)
core_spec.loader.exec_module(core)

def load(path):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"duplicate key {key}")
            result[key] = value
        return result
    return json.loads(path.read_text(), object_pairs_hook=pairs)

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def finding(out, code, path, message):
    out.append({"code": code, "path": path, "message": message})

def validate():
    out = []
    try:
        contract, schema, fixtures, fixture_schema, result_schema = map(load, (C, S, F, FS, RS))
    except Exception as exc:
        return [{"code": "E_JSON", "path": "inputs", "message": str(exc)}], {}
    for instance, spec, code in ((contract, schema, "E_SCHEMA"), (fixtures, fixture_schema, "E_FIXTURE_SCHEMA")):
        failures = []
        core.validate_schema_instance(instance, spec, failures, base=S.parent, root=spec)
        for item in failures:
            finding(out, code, item["pointer"], item["message"])
    text = "\n".join(path.read_text(errors="replace") for path in (C, S, F, FS, RS))
    if "// M0-" + "PROVISIONAL:" in text:
        finding(out, "E_PROVISIONAL", "inputs", "owner-confirmed artifact retains a provisional marker")
    profile = contract["writer_profile"]
    expected_profile = ("2.6", "parquet 57.0.0", "arrow 57.0.0", "zstd 1.5.7", 3, True, True, 0, 65536, 1048576, False)
    observed_profile = (profile["parquet_format_version"], profile["parquet_writer_crate"], profile["arrow_crate"], profile["zstd_library"], profile["zstd_level"], profile["zstd_checksum"], profile["zstd_content_size"], profile["zstd_workers"], profile["row_group_max_rows"], profile["data_page_max_bytes"], profile["dictionary_enabled"])
    if observed_profile != expected_profile:
        finding(out, "E_WRITER_PROFILE", "writer_profile", "deterministic Parquet/Zstd profile changed")
    layout = contract["layout"]
    if layout["allowed_filesystems"] != ["ext4", "xfs"] or layout["root_mode"] != "0700" or layout["file_mode"] != "0600":
        finding(out, "E_FILESYSTEM", "layout", "filesystem allowlist or owner-only modes changed")
    if any(token not in layout["final"] for token in ("{epoch64}", "{generation20}", "{start20}", "{end20}", "{intent64}")):
        finding(out, "E_PATH_GRAMMAR", "layout/final", "canonical final key omits an opaque identity component")
    steps = contract["commit_protocol"]["ordered_steps"]
    required_order = ("pending intent", "write each file", "manifest.json", "rename staging", "SEGMENT_READY", "segment_ready", "fence selector", "promoted generation")
    positions = []
    for phrase in required_order:
        matches = [i for i, step in enumerate(steps) if phrase in step]
        if len(matches) != 1:
            finding(out, "E_COMMIT_STEP", "commit_protocol/ordered_steps", f"expected one {phrase!r} step")
            positions.append(-1)
        else:
            positions.append(matches[0])
    if positions != sorted(positions) or -1 in positions:
        finding(out, "E_COMMIT_ORDER", "commit_protocol/ordered_steps", "durability/publication order is not monotonic")
    hashes = contract["hashes"]
    canonical = json.dumps(hashes["golden_manifest"], sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    if hashlib.sha256(canonical).hexdigest() != hashes["golden_manifest_sha256"]:
        finding(out, "E_GOLDEN_HASH", "hashes/golden_manifest_sha256", "manifest hash does not cover canonical manifest bytes")
    if hashes["file_scope"] != "exact persisted compressed file bytes" or "no trailing newline" not in hashes["manifest_encoding"]:
        finding(out, "E_HASH_SCOPE", "hashes", "file or manifest hash scope is ambiguous")
    policy = contract["consumes"]["failure_policy"]
    if (policy["version"], policy["base_delay_ms"], policy["cap_delay_ms"], policy["maximum_attempts"]) != (1, 250, 30000, 10):
        finding(out, "E_FAILURE_POLICY", "consumes/failure_policy", "owner-confirmed retry literals changed")
    audit = contract["audit"]
    if audit["separate_ranges"] != ["journal_verified_range", "self_consistent_range"] or audit["checkpoint_moves"] is not False:
        finding(out, "E_AUDIT_BOUNDARY", "audit", "audit conflates coverage or moves the live checkpoint")
    budgets = audit["budgets"]
    if any(budgets[key] <= 0 for key in budgets):
        finding(out, "E_AUDIT_BUDGET", "audit/budgets", "every finite audit budget and window must be positive")
    cases = fixtures["cases"]
    ids = [case["fixture_id"] for case in cases]
    if ids != contract["fixture_ids"] or len(ids) != len(set(ids)):
        finding(out, "E_FIXTURE_INVENTORY", "fixtures/cases", "ordered fixture inventory differs or duplicates")
    if {case["executor_id"] for case in cases} != set(contract["executors"]):
        finding(out, "E_EXECUTORS", "fixtures/cases", "executor coverage differs")
    required_cases = {"CRASH-AFTER-INTENT", "CRASH-DURING-FILE", "CRASH-AFTER-MANIFEST-SYNC", "CRASH-AFTER-DIRECTORY-RENAME", "CRASH-BEFORE-READY", "CRASH-AFTER-READY-SYNC", "FINAL-WITHOUT-READY-CORRUPT", "PARQUET-BYTE-RETRY", "JSONL-BYTE-RETRY", "SAME-FENCE-CONFLICT", "EXTERNAL-FENCE-AHEAD", "AUDIT-OVERSIZED-PART-RESUME", "GC-ELIGIBLE", "SYMLINK-RACE", "ENOSPC-BEFORE-RESERVE"}
    suffixes = {item.removeprefix("SCN-M0-ARCHIVE-") for item in ids}
    if not required_cases <= suffixes:
        finding(out, "E_FIXTURE_BRANCHES", "fixtures/cases", "required crash/corruption/audit/GC branches missing")
    for index, case in enumerate(cases):
        pre, action, expected = case["pre_state"], case["action"], case["expected"]
        if action["fault_once"] is not True or not action["phase"] or not action["fault_hook"]:
            finding(out, "E_FIXTURE_ACTION", f"fixtures/cases/{index}", "fault action is not deterministic")
        if pre["requested_range"]["start"] > pre["requested_range"]["end"] or expected["redacted"] is not True or expected["feedback"] != "unaffected":
            finding(out, "E_FIXTURE_EXPECTED", f"fixtures/cases/{index}", "range, redaction, or capture independence invalid")
        if expected["archive_state"].startswith("blocked") and expected["checkpoint"] != "unchanged":
            finding(out, "E_CHECKPOINT_SKIP", f"fixtures/cases/{index}", "blocked fixture advances checkpoint")
    try:
        manifest, artifacts = load(M), load(A)
        rows = [row for row in manifest["artifacts"] if row["owner_bead"] == OWNER]
        if len(rows) != 1:
            finding(out, "E_MANIFEST", "contracts/m0/manifest.json", "expected exactly one archive row")
        else:
            row = rows[0]
            if row["path"] != str(C.relative_to(ROOT)) or row["sha256"] != digest(C) or row["fixture_ids"] != ids or row["executor_ids"] != contract["executors"]:
                finding(out, "E_MANIFEST_BINDING", "contracts/m0/manifest.json", "archive row does not bind contract, fixtures and executors")
        rows = [row for row in artifacts["artifacts"] if row["owner_bead"] == OWNER]
        expected_paths = {str(path.relative_to(ROOT)) for path in (C, S, F, FS, RS, V, E)}
        if {row["path"] for row in rows} != expected_paths:
            finding(out, "E_ARTIFACT_INVENTORY", "contracts/m0/artifacts.json", "archive artifact inventory differs")
        for row in rows:
            path = ROOT / row["path"]
            if not path.is_file() or row["sha256"] != digest(path):
                finding(out, "E_ARTIFACT_HASH", row["path"], "artifact hash mismatch")
    except Exception as exc:
        finding(out, "E_REGISTRY", "contracts/m0", str(exc))
    for secret in ("postgres" + "://", "password" + "=", "BEGIN PRIVATE" + " KEY", "AK" + "IA"):
        if secret in text:
            finding(out, "E_SECRET", "inputs", f"forbidden token {secret}")
    inputs = {str(path.relative_to(ROOT)): digest(path) for path in (C, S, F, FS, RS)}
    return out, inputs

def main():
    findings, inputs = validate()
    previous = load(E) if E.exists() else {}
    parent = previous.get("source_parent_git_commit") or subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    material = "".join(key + "\0" + value + "\n" for key, value in sorted(inputs.items())).encode()
    evidence = {"schema_version": "m0-archive-contract-evidence/v1", "owner_bead": OWNER, "status": "pass" if not findings else "fail", "validator": str(V.relative_to(ROOT)), "validator_sha256": digest(V), "source_parent_git_commit": parent, "input_tree_sha256": hashlib.sha256(material).hexdigest(), "inputs": inputs, "fixture_count": len(load(F)["cases"]), "runtime_observed": False, "product_faults": "fault_not_applicable", "findings": findings}
    E.parent.mkdir(parents=True, exist_ok=True)
    E.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    print(json.dumps(evidence, sort_keys=True, separators=(",", ":")))
    return 0 if not findings else 1

if __name__ == "__main__":
    raise SystemExit(main())
