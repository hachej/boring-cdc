#!/usr/bin/env python3
"""Verify approved OCI index and linux/amd64 child digests without pulling images."""
import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / "contracts/m0/compose.json"


def digest(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def fail(image_key: str) -> None:
    print(json.dumps({"code": "COMPOSE_MANIFEST_INVALID", "image_key": image_key, "outcome": "fail", "phase": "validate_manifest"}, sort_keys=True, separators=(",", ":")))
    raise SystemExit(78)


def main() -> int:
    contract = json.loads(CONTRACT.read_text())
    records = contract["clean_pull_execution"]["manifest_verification"]
    for record in records:
        key = record["image_key"]
        try:
            index = subprocess.check_output(record["index_command"], stderr=subprocess.DEVNULL)
            if digest(index) != record["index_digest"]:
                fail(key)
            descriptors = [
                item["digest"]
                for item in json.loads(index)["manifests"]
                if item.get("platform", {}).get("os") == "linux"
                and item.get("platform", {}).get("architecture") == "amd64"
                and not item.get("platform", {}).get("variant")
            ]
            if descriptors != [record["platform_descriptor_digest"]]:
                fail(key)
            child = subprocess.check_output(record["platform_command"], stderr=subprocess.DEVNULL)
            if digest(child) != record["platform_descriptor_digest"]:
                fail(key)
        except (KeyError, ValueError, json.JSONDecodeError, subprocess.CalledProcessError):
            fail(key)
        print(json.dumps({"code": "COMPOSE_MANIFEST_VALID", "image_key": key, "index_digest": record["index_digest"], "outcome": "pass", "phase": "validate_manifest", "platform_digest": record["platform_descriptor_digest"]}, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
