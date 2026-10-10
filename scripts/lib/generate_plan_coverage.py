#!/usr/bin/env python3
"""Regenerate coverage assignments and M2 case references from canonical sources."""

import hashlib
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "contracts/agent/stable-ids.json"
COVERAGE = ROOT / "contracts/coverage/plan-to-beads.json"
PROVENANCE = ROOT / "contracts/coverage/plan-to-beads.provenance.json"
M2_CASE_FILES = sorted((ROOT / "contracts/m2").glob("*cases.json"))


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def encoded(value: object) -> bytes:
    return (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode()


def main() -> int:
    if sys.argv[1:] not in ([], ["--check"]):
        raise SystemExit("usage: scripts/lib/generate_plan_coverage.py [--check]")
    registry_bytes = REGISTRY.read_bytes()
    entries = json.loads(registry_bytes)["entries"]
    cases = {}
    source_digests = {"contracts/agent/stable-ids.json": digest(registry_bytes)}
    for path in M2_CASE_FILES:
        document = json.loads(path.read_bytes())
        rows = document.get("cases", []) + document.get("scenarios", [])
        if not rows:
            continue
        relative = str(path.relative_to(ROOT))
        source_digests[relative] = digest(path.read_bytes())
        for case in rows:
            cases.setdefault(case["id"], []).append((relative, document["owner_bead"], case))
    assignments = [
        {key: entry[key] for key in ("evidence_status", "id", "owner_bead", "source", "source_digest")}
        for entry in entries
    ]
    for assignment in assignments:
        matching = [item for item in cases.get(assignment["id"], []) if item[1] == assignment["owner_bead"]]
        if matching:
            if len(matching) != 1:
                raise SystemExit("ambiguous M2 case owner: " + assignment["id"])
            source, _, case = matching[0]
            assignment["case_binding"] = {
                "source": source,
                "source_digest": source_digests[source],
                "case_digest": digest(json.dumps(case, sort_keys=True, separators=(",", ":")).encode()),
            }
    missing = sorted(set(cases) - {row["id"] for row in assignments if "case_binding" in row})
    if missing:
        raise SystemExit("unassigned M2 cases: " + ",".join(missing))
    coverage_bytes = encoded({"assignments": assignments, "schema_version": "plan-to-beads/v2"})
    provenance_bytes = encoded(
        {
            "editable": False,
            "generated_digest": digest(coverage_bytes),
            "generator": "boring-cdc-m0.2",
            "schema_version": "generated-view/v1",
            "source_digests": source_digests,
            "sources": sorted(source_digests),
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
