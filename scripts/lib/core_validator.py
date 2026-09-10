#!/usr/bin/env python3
"""Deterministic, dependency-free validators for the M0 bootstrap contracts."""
from __future__ import annotations
import argparse, hashlib, json, os, re, shutil, subprocess, sys, tempfile
from pathlib import Path

VERSION = "core-validators/1.0.0"
OWNER = "boring-cdc-m0.1"
ROOT = Path(__file__).resolve().parents[2]
SHA256 = re.compile(r"^[0-9a-f]{64}$")
BEAD = re.compile(r"^boring-cdc-[A-Za-z0-9.-]+$")
ID = re.compile(r"^(REQ|INV|DEC|CMD|COND|TRANS|SCN|REL|RISK|RUNBOOK|CLAIM|FINDING|ART)-[A-Z0-9-]+$")
SECRET = re.compile(r"(?i)(password\s*[=:]|api[_-]?key\s*[=:]|secret\s*[=:]|token\s*[=:]|postgres(?:ql)?://[^\s:@]+:[^\s@]+@|-----BEGIN [A-Z ]*PRIVATE KEY-----)")
UNRESOLVED = re.compile(r"(?i)^\s*(tbd|todo|unknown|owner[- ]?pending)\s*$")

class DuplicateKey(ValueError): pass

def unique(pairs):
    out = {}
    for k, v in pairs:
        if k in out: raise DuplicateKey(k)
        out[k] = v
    return out

def digest(data: bytes) -> str: return hashlib.sha256(data).hexdigest()
def read_strict(path: Path):
    raw = path.read_bytes()
    return json.loads(raw, object_pairs_hook=unique), raw

def safe_path(value: object, pointer: str, findings: list, *, must_exist=False):
    if not isinstance(value, str) or not value:
        add(findings, "E_PATH_INVALID", pointer, "path must be a nonempty relative repository path"); return None
    p = Path(value)
    if p.is_absolute() or ".." in p.parts or any(part in ("", ".") for part in p.parts):
        add(findings, "E_PATH_TRAVERSAL", pointer, "absolute, dot, and parent path components are forbidden"); return None
    candidate = ROOT / p
    current = ROOT
    for part in p.parts:
        current /= part
        if current.is_symlink():
            add(findings, "E_PATH_SYMLINK", pointer, "symlink path components are forbidden"); return None
    try:
        candidate.resolve(strict=False).relative_to(ROOT.resolve())
    except (OSError, ValueError):
        add(findings, "E_PATH_TRAVERSAL", pointer, "resolved path must remain inside the repository"); return None
    if must_exist and not candidate.is_file(): add(findings, "E_REFERENCE_MISSING", pointer, f"referenced file does not exist: {value}")
    return candidate

def add(findings, code, pointer, message, owner=OWNER):
    findings.append({"code":code,"pointer":pointer,"owner_bead":owner,"message":message})

def req_obj(obj, fields, findings, pointer=""):
    if not isinstance(obj, dict): add(findings,"E_TYPE",pointer or "/","expected object"); return False
    for k in fields:
        if k not in obj: add(findings,"E_REQUIRED",f"{pointer}/{k}","required field is absent")
    return True

def check_unknown(obj, allowed, findings, pointer=""):
    if isinstance(obj, dict):
        for k in set(obj)-set(allowed): add(findings,"E_UNKNOWN_FIELD",f"{pointer}/{k}","unknown field")

def check_version(obj, expected, findings):
    if obj.get("schema_version") != expected: add(findings,"E_SCHEMA_VERSION","/schema_version",f"expected {expected}")

def check_id(value, pointer, findings, prefix=None):
    if not isinstance(value,str) or not ID.fullmatch(value) or (prefix and not value.startswith(prefix)):
        add(findings,"E_ID_INVALID",pointer,f"invalid {prefix or 'stable '}ID")

def check_owner(value,pointer,findings):
    if not isinstance(value,str) or not BEAD.fullmatch(value): add(findings,"E_OWNER_INVALID",pointer,"owner must be a canonical Bead ID")

def check_unresolved(value,pointer,findings):
    if not isinstance(value,str) or not value.strip() or UNRESOLVED.fullmatch(value): add(findings,"E_UNRESOLVED",pointer,"value is empty or unresolved")

def duplicates(rows,key,findings,pointer,code="E_DUPLICATE_ID"):
    seen=set()
    for i,row in enumerate(rows if isinstance(rows,list) else []):
        if isinstance(row,dict) and isinstance(row.get(key),str):
            val=row[key]
            if val in seen: add(findings,code,f"{pointer}/{i}/{key}",f"duplicate {key}: {val}")
            seen.add(val)

def load(path: Path, findings):
    try: return read_strict(path)
    except FileNotFoundError: add(findings,"E_INPUT_MISSING","/",f"input not found: {path}")
    except DuplicateKey as e: add(findings,"E_DUPLICATE_KEY","/",f"duplicate JSON key: {e}")
    except (json.JSONDecodeError,UnicodeDecodeError) as e: add(findings,"E_JSON_MALFORMED","/",f"malformed JSON: {e}")
    return None,b""

def inventory(path, findings, kind):
    if not path: return None
    obj,_=load(Path(path),findings)
    if not isinstance(obj,list): add(findings,"E_INVENTORY_TYPE",f"/{kind}","inventory must be a JSON array"); return set()
    result=set()
    for index,value in enumerate(obj):
        if not isinstance(value,str) or not value: add(findings,"E_INVENTORY_ITEM",f"/{kind}/{index}","inventory entries must be nonempty strings"); continue
        if value in result: add(findings,"E_DUPLICATE_OWNER" if kind=="owners" else "E_DUPLICATE_INVENTORY",f"/{kind}/{index}",f"duplicate {kind} entry: {value}")
        result.add(value)
    return result

def validate_decisions(obj, findings, args):
    if not req_obj(obj,["schema_version","decisions"],findings): return
    check_version(obj,"m0-decisions/v1",findings); check_unknown(obj,["schema_version","decisions"],findings)
    rows=obj.get("decisions",[])
    if not isinstance(rows,list): add(findings,"E_TYPE","/decisions","expected array"); return
    duplicates(rows,"id",findings,"/decisions"); owners=inventory(args.owners,findings,"owners"); fixtures=inventory(args.fixtures,findings,"fixtures"); executors=inventory(args.executors,findings,"executors")
    allowed=["id","owner_bead","status","proposed_value","approval","fixture_spec","fixture_sha256","executor_beads"]
    for i,r in enumerate(rows):
        p=f"/decisions/{i}";
        if not req_obj(r,["id","owner_bead","status","proposed_value","fixture_spec","fixture_sha256","executor_beads"],findings,p): continue
        check_unknown(r,allowed,findings,p); check_id(r.get("id"),p+"/id",findings,"DEC-"); check_owner(r.get("owner_bead"),p+"/owner_bead",findings); check_unresolved(r.get("proposed_value"),p+"/proposed_value",findings)
        if owners is not None and isinstance(r.get("owner_bead"),str) and r.get("owner_bead") not in owners: add(findings,"E_OWNER_UNKNOWN",p+"/owner_bead","owner absent from supplied inventory")
        if r.get("status") not in ("open","approved","rejected"): add(findings,"E_STATUS",p+"/status","invalid decision status")
        if r.get("status")=="approved":
            a=r.get("approval")
            approval_fields=["approved_by","approved_at","value_digest"]
            if not req_obj(a,approval_fields,findings,p+"/approval"): pass
            else:
                check_unknown(a,approval_fields,findings,p+"/approval")
                check_unresolved(a.get("approved_by"),p+"/approval/approved_by",findings); check_unresolved(a.get("approved_at"),p+"/approval/approved_at",findings)
                if not SHA256.fullmatch(str(a.get("value_digest",""))): add(findings,"E_DIGEST",p+"/approval/value_digest","expected lowercase sha256")
                elif a.get("value_digest") != digest(r.get("proposed_value","").encode()): add(findings,"E_APPROVAL_DIGEST",p+"/approval/value_digest","approval digest must bind the proposed value")
        elif "approval" in r: add(findings,"E_APPROVAL_STATE",p+"/approval","approval is allowed only for approved rows")
        fp=r.get("fixture_spec"); target=safe_path(fp,p+"/fixture_spec",findings)
        expected_hash=r.get("fixture_sha256")
        if not SHA256.fullmatch(str(expected_hash or "")): add(findings,"E_DIGEST",p+"/fixture_sha256","expected lowercase sha256")
        elif target and target.is_file() and digest(target.read_bytes()) != expected_hash: add(findings,"E_HASH_MISMATCH",p+"/fixture_sha256","fixture content hash mismatch")
        if fixtures is not None and isinstance(fp,str) and fp not in fixtures: add(findings,"E_FIXTURE_UNKNOWN",p+"/fixture_spec","fixture absent from supplied inventory")
        ex=r.get("executor_beads")
        if not isinstance(ex,list) or not ex: add(findings,"E_EXECUTOR_REQUIRED",p+"/executor_beads","at least one later executor is required")
        else:
            if len(set(map(str,ex)))!=len(ex): add(findings,"E_DUPLICATE_EXECUTOR",p+"/executor_beads","duplicate executor")
            for j,x in enumerate(ex):
                check_owner(x,f"{p}/executor_beads/{j}",findings)
                if executors is not None and isinstance(x,str) and x not in executors: add(findings,"E_EXECUTOR_UNKNOWN",f"{p}/executor_beads/{j}","executor absent from supplied inventory")
    if args.complete:
        if not rows: add(findings,"E_DECISIONS_EMPTY","/decisions","empty skeleton cannot satisfy aggregate completeness")
        for option in ("owners", "fixtures", "executors", "expected_decisions"):
            if not getattr(args, option): add(findings,"E_INVENTORY_REQUIRED",f"/{option}",f"--complete requires --{option.replace('_','-')} inventory")
        expected=inventory(args.expected_decisions,findings,"expected_decisions") if args.expected_decisions else set()
        actual={r.get("id") for r in rows if isinstance(r,dict) and isinstance(r.get("id"),str)}
        for missing in sorted(expected-actual): add(findings,"E_DECISION_MISSING",f"/decisions/{missing}","required decision is absent")
        for extra in sorted(actual-expected): add(findings,"E_DECISION_EXTRA",f"/decisions/{extra}","decision is absent from expected inventory")
        for i,r in enumerate(rows):
            if isinstance(r,dict) and r.get("status")!="approved": add(findings,"E_DECISION_OPEN",f"/decisions/{i}/status","real closure requires every decision approved")
            if isinstance(r,dict): safe_path(r.get("fixture_spec"),f"/decisions/{i}/fixture_spec",findings,must_exist=True)

def validate_artifacts(obj, findings, args):
    if not req_obj(obj,["schema_version","artifacts"],findings): return
    check_version(obj,"m0-artifacts/v1",findings); check_unknown(obj,["schema_version","artifacts"],findings); rows=obj.get("artifacts",[])
    if not isinstance(rows,list): add(findings,"E_TYPE","/artifacts","expected array"); return
    duplicates(rows,"id",findings,"/artifacts")
    for i,r in enumerate(rows):
        p=f"/artifacts/{i}"
        if not req_obj(r,["id","owner_bead","path","sha256","status"],findings,p): continue
        check_unknown(r,["id","owner_bead","path","sha256","status"],findings,p); check_id(r.get("id"),p+"/id",findings,"ART-"); check_owner(r.get("owner_bead"),p+"/owner_bead",findings)
        target=safe_path(r.get("path"),p+"/path",findings,must_exist=r.get("status")=="complete")
        if not SHA256.fullmatch(str(r.get("sha256",""))): add(findings,"E_DIGEST",p+"/sha256","expected lowercase sha256")
        elif target and target.is_file() and digest(target.read_bytes())!=r.get("sha256"): add(findings,"E_HASH_MISMATCH",p+"/sha256","artifact content hash mismatch")
        if r.get("status") not in ("declared","complete"): add(findings,"E_STATUS",p+"/status","invalid artifact status")
    if args.complete:
        if not rows: add(findings,"E_ARTIFACTS_EMPTY","/artifacts","empty skeleton cannot satisfy aggregate completeness")
        if not args.expected_artifacts: add(findings,"E_INVENTORY_REQUIRED","/expected_artifacts","--complete requires --expected-artifacts inventory")
        expected=inventory(args.expected_artifacts,findings,"expected_artifacts") if args.expected_artifacts else set()
        actual={r.get("id") for r in rows if isinstance(r,dict) and isinstance(r.get("id"),str)}
        for missing in sorted(expected-actual): add(findings,"E_ARTIFACT_MISSING",f"/artifacts/{missing}","required artifact is absent")
        for extra in sorted(actual-expected): add(findings,"E_ARTIFACT_EXTRA",f"/artifacts/{extra}","artifact is absent from expected inventory")
        for i,r in enumerate(rows):
            if isinstance(r,dict) and r.get("status") != "complete": add(findings,"E_ARTIFACT_INCOMPLETE",f"/artifacts/{i}/status","aggregate completeness requires every artifact complete")

def validate_evidence(obj, findings, args):
    fields=["schema_version","owner_bead","scenario_id","evidence_profile","evidence_tier","seed","git_commit","commands","source_preservation","cleanup","redaction","tier_proof","result"]
    if not req_obj(obj,fields,findings): return
    check_version(obj,"evidence/v1",findings); check_unknown(obj,fields,findings); check_owner(obj.get("owner_bead"),"/owner_bead",findings); check_id(obj.get("scenario_id"),"/scenario_id",findings,"SCN-"); check_unresolved(obj.get("seed"),"/seed",findings)
    profile=obj.get("evidence_profile"); tier=obj.get("evidence_tier")
    if profile not in ("runtime","external_managed","documentation"): add(findings,"E_EVIDENCE_PROFILE","/evidence_profile","unknown evidence profile")
    if tier not in ("leaf","component","milestone","release"): add(findings,"E_EVIDENCE_TIER","/evidence_tier","unknown evidence tier")
    commit=str(obj.get("git_commit",""))
    if not re.fullmatch(r"[0-9a-f]{40}",commit): add(findings,"E_GIT_SHA","/git_commit","expected full Git SHA")
    else:
        exists=subprocess.run(["git","cat-file","-e",commit+"^{commit}"],cwd=ROOT,capture_output=True).returncode==0
        ancestor=exists and subprocess.run(["git","merge-base","--is-ancestor",commit,"HEAD"],cwd=ROOT,capture_output=True).returncode==0
        if not exists or not ancestor: add(findings,"E_GIT_OBJECT","/git_commit","commit must exist and be an ancestor of the validating checkout")
        else:
            bound_paths = ["contracts", "scripts", "tests"]
            if obj.get("owner_bead") == "boring-cdc-m2-journal":
                bound_paths += ["src/m2_journal.rs", "src/m2_schema.rs", "src/lib.rs", "examples/m2_journal_component.rs"]
            if subprocess.run(["git","diff","--quiet",commit+"..HEAD","--",*bound_paths],cwd=ROOT).returncode != 0:
                add(findings,"E_EVIDENCE_STALE","/git_commit","owned implementation, contract, fixture, src, or example paths changed after the evidence commit")
    cmds=obj.get("commands")
    if not isinstance(cmds,list) or not cmds: add(findings,"E_COMMANDS_REQUIRED","/commands","at least one command record is required")
    else:
        for i,c in enumerate(cmds):
            p=f"/commands/{i}"; required=["argv","version","exit_code","stdout_path","stdout_sha256","stderr_path","stderr_sha256"]; req_obj(c,required,findings,p)
            if isinstance(c,dict):
                check_unknown(c,required,findings,p); check_unresolved(c.get("argv"),p+"/argv",findings); check_unresolved(c.get("version"),p+"/version",findings)
                if type(c.get("exit_code")) is not int: add(findings,"E_EXIT_CODE",p+"/exit_code","exit_code must be an integer")
                executable=str(c.get("argv","")).split()[0] if str(c.get("argv","")).split() else ""
                if "/" in executable:
                    command_path=safe_path(executable,p+"/argv",findings,must_exist=True)
                    if command_path and command_path.is_file() and not os.access(command_path,os.X_OK): add(findings,"E_COMMAND_NOT_EXECUTABLE",p+"/argv",f"command is not executable: {executable}")
                elif executable:
                    resolved_command=shutil.which(executable)
                    if resolved_command is None: add(findings,"E_COMMAND_MISSING",p+"/argv",f"command executable not found: {executable}")
                    elif not os.access(resolved_command,os.X_OK): add(findings,"E_COMMAND_NOT_EXECUTABLE",p+"/argv",f"command is not executable: {executable}")
                for stream in ("stdout","stderr"):
                    key=stream+"_sha256"; target=safe_path(c.get(stream+"_path"),p+"/"+stream+"_path",findings,must_exist=True)
                    if not SHA256.fullmatch(str(c.get(key,""))): add(findings,"E_DIGEST",p+"/"+key,"expected lowercase sha256")
                    elif target and target.is_file() and digest(target.read_bytes()) != c.get(key): add(findings,"E_HASH_MISMATCH",p+"/"+key,f"{stream} content hash mismatch")
    sp=obj.get("source_preservation"); sp_fields=["before_sha256","after_sha256","preserved"]; req_obj(sp,sp_fields,findings,"/source_preservation")
    if isinstance(sp,dict):
        check_unknown(sp,sp_fields,findings,"/source_preservation")
        for k in ("before_sha256","after_sha256"):
            if not SHA256.fullmatch(str(sp.get(k,""))): add(findings,"E_DIGEST",f"/source_preservation/{k}","expected lowercase sha256")
        if sp.get("preserved") is not True or sp.get("before_sha256")!=sp.get("after_sha256"): add(findings,"E_SOURCE_MUTATED","/source_preservation","before/after source hashes must match and preserved must be true")
    cl=obj.get("cleanup"); cl_fields=["complete","remaining_paths"]; req_obj(cl,cl_fields,findings,"/cleanup")
    if isinstance(cl,dict):
        check_unknown(cl,cl_fields,findings,"/cleanup")
        if cl.get("complete") is not True or cl.get("remaining_paths")!=[]: add(findings,"E_CLEANUP_INCOMPLETE","/cleanup","cleanup must be complete with no remaining paths")
    rd=obj.get("redaction"); rd_fields=["checked","secrets_found"]; req_obj(rd,rd_fields,findings,"/redaction")
    if isinstance(rd,dict):
        check_unknown(rd,rd_fields,findings,"/redaction")
        if rd.get("checked") is not True or rd.get("secrets_found")!=0: add(findings,"E_REDACTION","/redaction","redaction must be checked with zero secrets")
    proof_fields=["targeted_checks","boundary_e2e","fault_suite","deterministic_rerun","consumed_contract_vectors","workspace_tests","integration","clean_environment","exit_assertions","endurance","full_failure_matrix","clean_clone"]
    proof=obj.get("tier_proof"); req_obj(proof,proof_fields,findings,"/tier_proof")
    if isinstance(proof,dict):
        check_unknown(proof,proof_fields,findings,"/tier_proof")
        component=["targeted_checks","boundary_e2e","fault_suite","deterministic_rerun","consumed_contract_vectors"]
        milestone=component+["workspace_tests","integration","clean_environment","exit_assertions"]
        required_by_tier={"leaf":["targeted_checks"],"component":component,"milestone":milestone,"release":milestone+["endurance","full_failure_matrix","clean_clone"]}
        for name in required_by_tier.get(tier,[]):
            if proof.get(name) is not True: add(findings,"E_TIER_PROOF",f"/tier_proof/{name}",f"{tier} evidence requires {name}")
        for name in proof_fields:
            if name in proof and type(proof[name]) is not bool: add(findings,"E_TYPE",f"/tier_proof/{name}","tier proof values must be booleans")
    result=obj.get("result"); result_fields=["status","digest","artifacts","product_faults","runtime_observed","attempts"]; req_obj(result,["status","digest","artifacts","product_faults","runtime_observed"],findings,"/result")
    if isinstance(result,dict):
        check_unknown(result,result_fields,findings,"/result")
        if result.get("status") not in ("pass","fail","unavailable","unsupported"): add(findings,"E_RESULT_STATUS","/result/status","unknown evidence result status")
        artifacts=result.get("artifacts")
        if not isinstance(artifacts,list) or not artifacts: add(findings,"E_RESULT_ARTIFACTS","/result/artifacts","result must name content-bound artifacts")
        else:
            chunks=[]
            for index,value in enumerate(artifacts):
                target=safe_path(value,f"/result/artifacts/{index}",findings,must_exist=True)
                if target and target.is_file(): chunks.append(target.read_bytes())
            if SHA256.fullmatch(str(result.get("digest",""))) and len(chunks)==len(artifacts) and digest(b"".join(chunks)) != result.get("digest"): add(findings,"E_RESULT_DIGEST","/result/digest","result digest must bind listed artifacts in order")
        if not SHA256.fullmatch(str(result.get("digest",""))): add(findings,"E_DIGEST","/result/digest","expected lowercase sha256")
        if type(result.get("runtime_observed")) is not bool: add(findings,"E_TYPE","/result/runtime_observed","runtime_observed must be boolean")
        if profile=="documentation" and (result.get("product_faults")!="fault_not_applicable" or result.get("runtime_observed") is not False): add(findings,"E_FORWARD_RUNTIME_EVIDENCE","/result","documentation evidence cannot claim product runtime evidence")
        if profile=="runtime" and result.get("runtime_observed") is not True: add(findings,"E_RUNTIME_PROVENANCE","/result/runtime_observed","runtime profile requires direct runtime observation")
        if profile=="external_managed" and not result.get("attempts"): add(findings,"E_EXTERNAL_PROVENANCE","/result/attempts","managed evidence requires exact attempts/provenance")
    text=json.dumps(obj,sort_keys=True)
    if SECRET.search(text): add(findings,"E_SECRET","/","secret-like content is forbidden")

def validate_schema_instance(instance, schema, findings, pointer="", base=None, root=None):
    """Validate every assertion keyword published by repository-owned schemas."""
    if not isinstance(schema, dict):
        add(findings, "E_SCHEMA_DEFINITION", pointer or "/", "schema node must be an object")
        return
    root = schema if root is None else root
    if "$ref" in schema:
        try:
            name, _, fragment = schema["$ref"].partition("#")
            document = root if not name else json.loads((base / name).read_text())
            target = document
            if fragment:
                if not fragment.startswith("/"): raise ValueError("invalid fragment")
                for token in fragment[1:].split("/"):
                    target = target[token.replace("~1", "/").replace("~0", "~")]
            validate_schema_instance(instance, target, findings, pointer, base, document)
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            add(findings, "E_SCHEMA_REF", pointer or "/", "schema reference cannot be resolved")
        return
    expected = schema.get("type")
    types = expected if isinstance(expected, list) else [expected] if expected is not None else []
    matches = {
        "object": lambda value: isinstance(value, dict),
        "array": lambda value: isinstance(value, list),
        "string": lambda value: isinstance(value, str),
        "integer": lambda value: type(value) is int,
        "number": lambda value: type(value) in (int, float),
        "boolean": lambda value: type(value) is bool,
        "null": lambda value: value is None,
    }
    if types and not any(kind in matches and matches[kind](instance) for kind in types):
        add(findings, "E_SCHEMA_TYPE", pointer or "/", "instance type does not match schema")
        return
    if "const" in schema and instance != schema["const"]:
        add(findings, "E_SCHEMA_CONST", pointer or "/", "instance does not match required constant")
    if "enum" in schema and instance not in schema["enum"]:
        add(findings, "E_SCHEMA_ENUM", pointer or "/", "instance is not an allowed value")
    for keyword in ("allOf", "anyOf", "oneOf"):
        if keyword in schema:
            branch_findings=[]
            for branch in schema[keyword]:
                local=[]; validate_schema_instance(instance, branch, local, pointer, base, root); branch_findings.append(local)
            passed=sum(not local for local in branch_findings)
            if keyword == "allOf":
                for local in branch_findings: findings.extend(local)
            elif keyword == "anyOf" and passed == 0: add(findings,"E_SCHEMA_ANY_OF",pointer or "/","instance matches no anyOf branch")
            elif keyword == "oneOf" and passed != 1: add(findings,"E_SCHEMA_ONE_OF",pointer or "/","instance must match exactly one oneOf branch")
    if isinstance(instance, str):
        if isinstance(schema.get("minLength"), int) and len(instance) < schema["minLength"]:
            add(findings, "E_SCHEMA_MIN_LENGTH", pointer or "/", "string is shorter than minLength")
        if isinstance(schema.get("maxLength"), int) and len(instance) > schema["maxLength"]:
            add(findings, "E_SCHEMA_MAX_LENGTH", pointer or "/", "string is longer than maxLength")
        if isinstance(schema.get("pattern"), str) and re.search(schema["pattern"], instance) is None:
            add(findings, "E_SCHEMA_PATTERN", pointer or "/", "string does not match required pattern")
    if type(instance) in (int,float):
        if type(schema.get("minimum")) in (int,float) and instance < schema["minimum"]: add(findings,"E_SCHEMA_MINIMUM",pointer or "/","number is below minimum")
        if type(schema.get("maximum")) in (int,float) and instance > schema["maximum"]: add(findings,"E_SCHEMA_MAXIMUM",pointer or "/","number is above maximum")
    if isinstance(instance, list):
        if isinstance(schema.get("minItems"),int) and len(instance)<schema["minItems"]: add(findings,"E_SCHEMA_MIN_ITEMS",pointer or "/","array has fewer than minItems")
        if isinstance(schema.get("maxItems"),int) and len(instance)>schema["maxItems"]: add(findings,"E_SCHEMA_MAX_ITEMS",pointer or "/","array has more than maxItems")
        if schema.get("uniqueItems") is True and len({json.dumps(x,sort_keys=True,separators=(",",":")) for x in instance}) != len(instance): add(findings,"E_SCHEMA_UNIQUE_ITEMS",pointer or "/","array items are not unique")
        if isinstance(schema.get("items"), dict):
            for index, value in enumerate(instance):
                validate_schema_instance(value, schema["items"], findings, f"{pointer}/{index}", base, root)
        if isinstance(schema.get("contains"),dict):
            matches_contains=False
            for index,value in enumerate(instance):
                local=[];validate_schema_instance(value,schema["contains"],local,f"{pointer}/{index}",base,root)
                if not local:matches_contains=True;break
            if not matches_contains:add(findings,"E_SCHEMA_CONTAINS",pointer or "/","array has no matching item")
    if isinstance(instance, dict):
        if isinstance(schema.get("minProperties"),int) and len(instance)<schema["minProperties"]: add(findings,"E_SCHEMA_MIN_PROPERTIES",pointer or "/","object has fewer than minProperties")
        if isinstance(schema.get("maxProperties"),int) and len(instance)>schema["maxProperties"]: add(findings,"E_SCHEMA_MAX_PROPERTIES",pointer or "/","object has more than maxProperties")
        properties = schema.get("properties", {})
        required = schema.get("required", [])
        for key in required if isinstance(required, list) else []:
            if key not in instance:
                add(findings, "E_SCHEMA_REQUIRED", f"{pointer}/{key}", "required field is absent")
        if isinstance(properties, dict):
            for key, value in instance.items():
                child = f"{pointer}/{key}"
                if key in properties:
                    validate_schema_instance(value, properties[key], findings, child, base, root)
                elif schema.get("additionalProperties") is False:
                    add(findings, "E_SCHEMA_ADDITIONAL_PROPERTY", child, "additional property is forbidden")
                elif isinstance(schema.get("additionalProperties"), dict):
                    validate_schema_instance(value, schema["additionalProperties"], findings, child, base, root)
        names = schema.get("propertyNames")
        if isinstance(names, dict) and isinstance(names.get("pattern"), str):
            for key in instance:
                if re.search(names["pattern"], key) is None:
                    add(findings, "E_SCHEMA_PROPERTY_NAME", f"{pointer}/{key}", "property name does not match required pattern")

def validate_runbooks(obj, findings, args):
    if not req_obj(obj,["schema_version","stage","runbooks"],findings): return
    check_version(obj,"runbooks-index/v1",findings); check_unknown(obj,["schema_version","stage","runbooks"],findings)
    if obj.get("stage") not in ("declared","complete"): add(findings,"E_STAGE","/stage","stage must be declared or complete")
    rows=obj.get("runbooks",[])
    if not isinstance(rows,list): add(findings,"E_TYPE","/runbooks","expected array"); return
    duplicates(rows,"id",findings,"/runbooks")
    conditions=set(); actions=set()
    for i,r in enumerate(rows if isinstance(rows,list) else []):
        p=f"/runbooks/{i}"; fields=["id","condition_id","action_id","condition_owner","action_owner","procedure_owner","procedure"]
        if not req_obj(r,fields,findings,p): continue
        check_unknown(r,fields,findings,p); check_id(r.get("id"),p+"/id",findings,"RUNBOOK-"); check_id(r.get("condition_id"),p+"/condition_id",findings,"COND-"); check_id(r.get("action_id"),p+"/action_id",findings,"CMD-")
        for k in ("condition_owner","action_owner","procedure_owner"): check_owner(r.get(k),p+"/"+k,findings)
        for k,bucket,code in (("condition_id",conditions,"E_DUPLICATE_CONDITION"),("action_id",actions,"E_DUPLICATE_ACTION")):
            value=r.get(k)
            if isinstance(value,str):
                if value in bucket: add(findings,code,p+"/"+k,"registry mapping must be unique")
                bucket.add(value)
        if obj.get("stage")=="complete" and not r.get("procedure"): add(findings,"E_PROCEDURE_GAP",p+"/procedure","complete registry requires procedure")
        if obj.get("stage")=="declared" and r.get("procedure") not in (None,""): add(findings,"E_STAGE_INCOMPATIBLE",p+"/procedure","declared rows must not claim completed procedures")
    if args.release and obj.get("stage")!="complete": add(findings,"E_RELEASE_DECLARED_RUNBOOK","/stage","release rejects declared-only runbooks")

def parse_jsonl(path, findings):
    try: raw=path.read_bytes()
    except FileNotFoundError: add(findings,"E_INPUT_MISSING","/",f"input not found: {path}"); return [],b""
    except OSError as e: add(findings,"E_INPUT_UNREADABLE","/",f"input cannot be read: {type(e).__name__}"); return [],b""
    rows=[]
    for i,line in enumerate(raw.splitlines()):
        try: rows.append(json.loads(line,object_pairs_hook=unique))
        except DuplicateKey as e: add(findings,"E_DUPLICATE_KEY",f"/lines/{i}",f"duplicate JSON key: {e}")
        except (json.JSONDecodeError,UnicodeDecodeError) as e: add(findings,"E_JSONL_MALFORMED",f"/lines/{i}",f"malformed JSONL: {e}")
    return rows,raw

def witness(path,findings):
    try:
        raw=path.read_bytes()
        with tempfile.TemporaryDirectory(prefix="boring-cdc-witness-") as td:
            beads=Path(td)/".beads"; beads.mkdir(); (beads/"issues.jsonl").write_bytes(raw)
            cp=subprocess.run(["br","sync","--witness","--json","--no-db"],cwd=td,text=True,capture_output=True,timeout=30)
            if cp.returncode:
                detail=(cp.stderr or cp.stdout).strip().replace(str(ROOT),"<repo>").replace(td,"<tmp>")
                add(findings,"E_WITNESS", "/", "br witness failed: "+detail); return "0"*64
            return json.loads(cp.stdout)["witness"]["root_hash"]
    except Exception as e: add(findings,"E_WITNESS","/",f"br witness unavailable: {type(e).__name__}"); return "0"*64

def validated_edges(by, findings):
    edges=[]
    for issue,r in by.items():
        dependencies=r.get("dependencies",[])
        if not isinstance(dependencies,list): add(findings,"E_TYPE",f"/issues/{issue}/dependencies","dependencies must be an array"); continue
        for j,d in enumerate(dependencies):
            p=f"/issues/{issue}/dependencies/{j}"
            if not isinstance(d,dict): add(findings,"E_EDGE_RECORD",p,"dependency must be an object"); continue
            edge_owner=d.get("issue_id"); target=d.get("depends_on_id"); typ=d.get("type")
            if not isinstance(edge_owner,str): add(findings,"E_ID_INVALID",p+"/issue_id","dependency issue_id must be a string")
            elif edge_owner!=issue: add(findings,"E_EDGE_OWNER",p+"/issue_id","dependency issue_id differs from containing issue")
            if not isinstance(target,str): add(findings,"E_ID_INVALID",p+"/depends_on_id","dependency target ID must be a string")
            elif target not in by: add(findings,"E_EDGE_DANGLING",p+"/depends_on_id",f"missing issue: {target}")
            if not isinstance(typ,str) or typ not in ("blocks","parent-child","related","discovered-from"): add(findings,"E_EDGE_TYPE",p+"/type","unknown dependency type")
            if isinstance(target,str) and isinstance(typ,str): edges.append((issue,target,typ))
    return edges

def validate_graph(path, findings, args):
    rows,raw=parse_jsonl(path,findings); by={}
    for i,r in enumerate(rows):
        if not isinstance(r,dict) or not isinstance(r.get("id"),str): add(findings,"E_GRAPH_RECORD",f"/lines/{i}","issue object with ID required"); continue
        if r["id"] in by: add(findings,"E_DUPLICATE_ID",f"/lines/{i}/id",f"duplicate issue: {r['id']}")
        by[r["id"]]=r
    if args.baseline:
        baseline_rows,_=parse_jsonl(Path(args.baseline),findings); baseline={r.get("id"):r for r in baseline_rows if isinstance(r,dict) and isinstance(r.get("id"),str)}
        for missing in sorted(set(baseline)-set(by)): add(findings,"E_GRAPH_MISSING",f"/issues/{missing}","record missing from captured graph")
        for extra in sorted(set(by)-set(baseline)): add(findings,"E_GRAPH_EXTRA",f"/issues/{extra}","unexpected record in captured graph")
        for stale in sorted(set(by)&set(baseline)):
            if by[stale] != baseline[stale]: add(findings,"E_GRAPH_STALE",f"/issues/{stale}","record differs from baseline across one or more fields")
    edges=validated_edges(by,findings)
    for kind in ("blocks","parent-child"):
        graph={x:[] for x in by}
        for a,b,t in edges:
            if t==kind and b in by: graph[a].append(b)
        visiting=set(); done=set()
        def walk(n):
            if n in visiting: add(findings,"E_GRAPH_CYCLE",f"/issues/{n}",f"{kind} cycle"); return
            if n in done:return
            visiting.add(n)
            for nxt in graph[n]: walk(nxt)
            visiting.remove(n); done.add(n)
        for n in sorted(graph): walk(n)
    for issue,r in by.items():
        if r.get("status")=="closed":
            for a,b,t in edges:
                if a==issue and t=="blocks" and by.get(b,{}).get("status")!="closed": add(findings,"E_PREMATURE_CLOSE",f"/issues/{issue}/status",f"closed with open prerequisite {b}")
                if b==issue and t=="parent-child" and by.get(a,{}).get("status")!="closed": add(findings,"E_PREMATURE_PARENT_CLOSE",f"/issues/{issue}/status",f"closed parent has open child {a}")
    if args.expect_root:
        expected=set(json.loads(Path(args.expect_root).read_text()))
        leaves={x for x in by if not any(b==x and t=="parent-child" for a,b,t in edges)}
        if leaves!=expected: add(findings,"E_LEAF_SET","/","graph leaf set differs from expected inventory")
    captured_sha=digest(raw)
    root=witness(path,findings) if raw else "0"*64
    if args.witness_root and root != args.witness_root: add(findings,"E_WITNESS_MISMATCH","/witness_root","captured witness does not match expected immutable root")
    try:
        if digest(path.read_bytes()) != captured_sha: add(findings,"E_INPUT_MOVED","/","graph input changed during captured operation")
    except FileNotFoundError: add(findings,"E_INPUT_MOVED","/","graph input disappeared during captured operation")
    normalized={"schema_version":"pinned-graph/v1","source_sha256":captured_sha,"witness_root":root,"issues":[by[x] for x in sorted(by)],"edges":[{"issue_id":a,"depends_on_id":b,"type":t} for a,b,t in sorted(edges)]}
    if args.output: Path(args.output).write_text(json.dumps(normalized,sort_keys=True,separators=(",",":"))+"\n")

def parser():
    p=argparse.ArgumentParser(description="Validate Boring CDC M0 bootstrap contracts with deterministic JSON diagnostics.")
    p.add_argument("kind",choices=["decisions","decision","artifacts","artifact","evidence","runbooks","graph","schema"]); p.add_argument("input"); p.add_argument("--schema"); p.add_argument("--complete",action="store_true"); p.add_argument("--release",action="store_true"); p.add_argument("--owners"); p.add_argument("--fixtures"); p.add_argument("--executors"); p.add_argument("--expected-decisions"); p.add_argument("--expected-artifacts"); p.add_argument("--expect-root"); p.add_argument("--baseline"); p.add_argument("--witness-root"); p.add_argument("--output"); return p

def main():
    args=parser().parse_args(); path=Path(args.input); findings=[]; raw=b""
    if args.kind=="graph":
        validate_graph(path,findings,args)
        try: raw=path.read_bytes()
        except OSError: raw=b""
    elif args.kind=="evidence" and path.is_dir():
        manifests=sorted(path.rglob("manifest.json"))
        if not manifests: add(findings,"E_INPUT_MISSING","/","artifact root contains no manifest.json")
        chunks=[]
        for manifest in manifests:
            local=[]; obj,part=load(manifest,local); chunks.append(part)
            if obj is not None: validate_evidence(obj,local,args)
            prefix="/"+manifest.relative_to(path).as_posix()
            for finding in local: finding["pointer"]=prefix+finding["pointer"]
            findings.extend(local)
        raw=b"\n".join(chunks)
    else:
        obj,raw=load(path,findings)
        if obj is not None:
            if args.kind in ("decisions","decision"):
                if args.kind=="decision" and isinstance(obj,dict) and "decisions" not in obj: obj={"schema_version":"m0-decisions/v1","decisions":[obj]}
                validate_decisions(obj,findings,args)
            elif args.kind in ("artifacts","artifact"):
                if args.kind=="artifact" and isinstance(obj,dict) and "artifacts" not in obj: obj={"schema_version":"m0-artifacts/v1","artifacts":[obj]}
                validate_artifacts(obj,findings,args)
            elif args.kind=="evidence": validate_evidence(obj,findings,args)
            elif args.kind=="schema":
                if not args.schema:
                    add(findings,"E_SCHEMA_REQUIRED_OPTION","/schema","schema validation requires --schema")
                else:
                    schema,schema_raw=load(Path(args.schema),findings)
                    if schema is not None: validate_schema_instance(obj,schema,findings,base=Path(args.schema).parent)
                    raw += b"\n" + schema_raw
            else: validate_runbooks(obj,findings,args)
    findings.sort(key=lambda x:(x["pointer"],x["code"],x["message"]))
    result={"schema_version":"validation-result/v1","validator_version":VERSION,"owner_bead":OWNER,"status":"fail" if findings else "pass","input_sha256":digest(raw),"git_commit":subprocess.run(["git","rev-parse","HEAD"],cwd=ROOT,text=True,capture_output=True).stdout.strip(),"findings":findings}
    print(json.dumps(result,sort_keys=True,separators=(",",":"))); return 1 if findings else 0
if __name__=="__main__": raise SystemExit(main())
