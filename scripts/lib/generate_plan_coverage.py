#!/usr/bin/env python3
"""Regenerate the assignment-only coverage view from the stable ID registry."""

import hashlib
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "contracts/agent/stable-ids.json"
COVERAGE = ROOT / "contracts/coverage/plan-to-beads.json"
PROVENANCE = ROOT / "contracts/coverage/plan-to-beads.provenance.json"


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def encoded(value: object) -> bytes:
    return (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode()


def main() -> int:
    if sys.argv[1:] not in ([], ["--check"]):
        raise SystemExit("usage: scripts/lib/generate_plan_coverage.py [--check]")
    registry_bytes = REGISTRY.read_bytes()
    entries = json.loads(registry_bytes)["entries"]
    assignments = [
        {key: entry[key] for key in ("evidence_status", "id", "owner_bead", "source", "source_digest")}
        for entry in entries
    ]
    coverage_bytes = encoded({"assignments": assignments, "schema_version": "plan-to-beads/v1"})
    provenance_bytes = encoded(
        {
            "editable": False,
            "generated_digest": digest(coverage_bytes),
            "generator": "boring-cdc-m0.2",
            "schema_version": "generated-view/v1",
            "source_digests": {"contracts/agent/stable-ids.json": digest(registry_bytes)},
            "sources": ["contracts/agent/stable-ids.json"],
        }
    )
    if sys.argv[1:] == ["--check"]:
        if COVERAGE.read_bytes() != coverage_bytes or PROVENANCE.read_bytes() != provenance_bytes:
            print("generated coverage is stale", file=sys.stderr)
            return 1
        return 0
    COVERAGE.write_bytes(coverage_bytes)
    PROVENANCE.write_bytes(provenance_bytes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
