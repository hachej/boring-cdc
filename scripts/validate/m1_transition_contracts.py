#!/usr/bin/env python3
"""Validate M1 transition schemas, fixtures, and the pinned Bead-ID snapshot."""
import json
from pathlib import Path
from jsonschema import Draft202012Validator, ValidationError
from referencing import Registry, Resource

schemas = {}
for path in sorted(Path("contracts/m1").glob("*.schema.json")):
    schema = json.loads(path.read_text())
    Draft202012Validator.check_schema(schema)
    schemas[path.name] = (path, schema)
    print(f"PASS schema {path}")

_, fixture_schema = schemas["transition-fixture.schema.json"]
registry = Registry().with_resources(
    (schema["$id"], Resource.from_contents(schema)) for _, schema in schemas.values()
)
validator = Draft202012Validator(fixture_schema, registry=registry)
for path in sorted(Path("tests/fixtures/m1-transition/valid").glob("*.json")):
    validator.validate(json.loads(path.read_text()))
    print(f"PASS valid fixture {path}")
for path in sorted(Path("tests/fixtures/m1-transition/invalid").glob("*.json")):
    try:
        validator.validate(json.loads(path.read_text()))
    except ValidationError:
        print(f"PASS rejected fixture {path}")
    else:
        raise SystemExit(f"invalid fixture accepted: {path}")

ids = [json.loads(line)["id"] for line in Path(".beads/issues.jsonl").read_text().splitlines() if line.strip()]
duplicates = sorted({value for value in ids if ids.count(value) > 1})
print(f"duplicate Bead IDs={len(duplicates)}")
if duplicates:
    raise SystemExit(1)
