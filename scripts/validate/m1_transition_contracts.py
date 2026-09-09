#!/usr/bin/env python3
"""Validate M1 transition schemas and the pinned Bead-ID snapshot."""
import json
from pathlib import Path
from jsonschema import Draft202012Validator

for path in sorted(Path("contracts/m1").glob("*.schema.json")):
    Draft202012Validator.check_schema(json.loads(path.read_text()))
    print(f"PASS {path}")
ids = [
    json.loads(line)["id"]
    for line in Path(".beads/issues.jsonl").read_text().splitlines()
    if line.strip()
]
duplicates = sorted({value for value in ids if ids.count(value) > 1})
print(f"duplicate Bead IDs={len(duplicates)}")
if duplicates:
    raise SystemExit(1)
