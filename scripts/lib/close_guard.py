#!/usr/bin/env python3
"""Fail closed unless a root, all hierarchy descendants, and prerequisites are closed."""
import hashlib, json, sys
from pathlib import Path
from core_validator import validated_edges

OWNER = "boring-cdc-m0.1"

class DuplicateKey(ValueError): pass

def unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result: raise DuplicateKey(key)
        result[key] = value
    return result

def finding(code, pointer, message, owner=OWNER):
    return {"code": code, "pointer": pointer, "owner_bead": owner, "message": message}

def main():
    root, path = sys.argv[1], Path(sys.argv[2])
    findings, rows, seen_ids = [], [], set()
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        raw = b""
        findings.append(finding("E_INPUT_MISSING", "/", f"input not found: {path}"))
    except OSError as exc:
        raw = b""
        findings.append(finding("E_INPUT_UNREADABLE", "/", f"input cannot be read: {type(exc).__name__}"))
    for number, line in enumerate(raw.splitlines()):
        try:
            row = json.loads(line, object_pairs_hook=unique)
            if not isinstance(row, dict) or not isinstance(row.get("id"), str):
                findings.append(finding("E_GRAPH_RECORD", f"/lines/{number}", "issue object with ID required")); continue
            if row["id"] in seen_ids:
                findings.append(finding("E_DUPLICATE_ID", f"/lines/{number}/id", f"duplicate issue: {row['id']}")); continue
            seen_ids.add(row["id"]); rows.append(row)
        except DuplicateKey as exc:
            findings.append(finding("E_DUPLICATE_KEY", f"/lines/{number}", f"duplicate JSON key: {exc}"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            findings.append(finding("E_JSONL_MALFORMED", f"/lines/{number}", f"malformed JSONL: {exc}"))
    by = {row["id"]: row for row in rows}
    if root not in by:
        findings.append(finding("E_ROOT_MISSING", "/root", f"missing root {root}"))
    children = {issue: [] for issue in by}
    prerequisites = {issue: [] for issue in by}
    for issue, target, kind in validated_edges(by, findings):
        if kind == "blocks" and target in by: prerequisites[issue].append(target)
        elif kind == "parent-child" and target in by: children[target].append(issue)
    stack, checked = [root], set()
    while stack:
        issue = stack.pop()
        if issue in checked or issue not in by: continue
        checked.add(issue)
        row = by[issue]
        if row.get("status") != "closed":
            findings.append(finding("E_CLOSE_BLOCKED", f"/issues/{issue}/status", "root, hierarchy child, or blocking prerequisite is not closed", issue))
        stack.extend(children[issue]); stack.extend(prerequisites[issue])
    findings.sort(key=lambda item: (item["pointer"], item["code"], item["message"]))
    out = {"schema_version":"validation-result/v1","validator_version":"core-validators/1.0.0","owner_bead":OWNER,"status":"fail" if findings else "pass","input_sha256":hashlib.sha256(raw).hexdigest(),"findings":findings}
    print(json.dumps(out, sort_keys=True, separators=(",", ":")))
    return 1 if findings else 0

if __name__ == "__main__": raise SystemExit(main())
