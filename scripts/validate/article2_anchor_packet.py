#!/usr/bin/env python3
"""Validate the copied exact-head M3 anchor component packet."""

import hashlib
import json
import pathlib


ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKET = ROOT / "evidence/article2/anchor"
FILES = {
    "commands.txt",
    "config.json",
    "evidence.json",
    "fault-timeline.json",
    "logs/boring-cdc.jsonl",
    "manifest.json",
    "state/after.json",
    "state/before.json",
    "stderr.txt",
    "stdout.txt",
    "versions.json",
}
COMMIT = "6aaadad690b78197239646d16d7d0c19912621e7"
MANIFEST_SHA256 = "86c535f8869d1a77d87ac2c5c83f909adfbf84441d78b3f244824a4303e72018"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def sha256(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    provenance = json.loads((PACKET / "provenance.json").read_text())
    require(provenance["schema_version"] == "article2-anchor-packet/v1", "packet version")
    require(provenance["owner_bead"] == "boring-cdc-m3.8", "owner Bead")
    require(provenance["source_commit"] == COMMIT, "source commit")
    require(provenance["source_ci_run"] == "https://github.com/hachej/boring-cdc/actions/runs/38748581460", "CI run")
    require(provenance["source_ci_job"] == "m3_bootstrap_writer_handoff", "CI job")
    require(provenance["artifact_name"] == "m3-bootstrap-writer-handoff", "artifact name")
    require(provenance["original_manifest_sha256"] == MANIFEST_SHA256, "manifest digest reference")
    require("no ClickHouse snapshot import" in provenance["scope"], "scope")

    inventory = {}
    for line in (PACKET / "sha256.txt").read_text().splitlines():
        digest, name = line.split("  ", 1)
        require(name not in inventory, "duplicate inventory entry")
        inventory[name] = digest
    require(set(inventory) == FILES, "artifact inventory")
    for name, digest in inventory.items():
        require(sha256(PACKET / name) == digest, f"{name} digest")
    require(inventory["manifest.json"] == MANIFEST_SHA256, "original manifest digest")
    require((PACKET / "manifest.json").read_bytes() == (PACKET / "evidence.json").read_bytes(), "manifest copy")

    evidence = json.loads((PACKET / "evidence.json").read_text())
    versions = json.loads((PACKET / "versions.json").read_text())
    after = json.loads((PACKET / "state/after.json").read_text())
    require(evidence["git_commit"] == versions["git_commit"] == COMMIT, "artifact commit")
    require(evidence["schema_version"] == "evidence/v1", "evidence version")
    require(evidence["evidence_tier"] == "component", "evidence tier")
    require(evidence["result"]["status"] == "pass", "scenario result")
    require(evidence["result"]["attempts"] == ["clean-attempt-1", "clean-attempt-2"], "clean attempts")
    require(evidence["cleanup"] == {"complete": True, "remaining_paths": []}, "cleanup")
    require(evidence["redaction"] == {"checked": True, "secrets_found": 0}, "redaction")
    require(evidence["source_preservation"]["preserved"] is True, "source preservation")
    require(versions["postgres"] == "17.6", "PostgreSQL version")
    require(all(value is True for key, value in after.items() if key not in {"mode", "control_update_rows"}), "anchor checks")
    require(after["mode"] == "e2e" and after["control_update_rows"] == 1, "anchor mode")
    require(after["anchor_complete"] and after["m2_capture_fence_durable"] and after["imported_canonical_schema_proof"], "anchor proof")
    require((PACKET / "stdout.txt").read_text().strip() == "M3_BOOTSTRAP_E2E_OK", "scenario stdout")
    print("ARTICLE2_ANCHOR_PACKET_OK")


if __name__ == "__main__":
    main()
