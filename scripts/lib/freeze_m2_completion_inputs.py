#!/usr/bin/env python3
"""Freeze M2 completion's content-addressed handoff manifests outside live packets."""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COVERAGE = ROOT / "contracts/coverage/m2.json"
PIN_ROOT = ROOT / "artifacts/boring-cdc-m2-complete/pinned-manifests"


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def historical_bytes(path: str, expected: str) -> bytes:
    candidate = ROOT / path
    if candidate.is_file() and sha(candidate.read_bytes()) == expected:
        return candidate.read_bytes()
    commits = subprocess.check_output(
        ["git", "log", "--all", "--format=%H", "--", path], cwd=ROOT, text=True
    ).splitlines()
    for commit in commits:
        run = subprocess.run(
            ["git", "show", f"{commit}:{path}"], cwd=ROOT, capture_output=True
        )
        if run.returncode == 0 and sha(run.stdout) == expected:
            return run.stdout
    raise SystemExit(f"cannot resolve pinned manifest {path} at sha256:{expected}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    coverage = json.loads(COVERAGE.read_text())
    rows = {row["id"]: row for row in (json.loads(line) for line in (ROOT / ".beads/issues.jsonl").read_text().splitlines() if line.strip())}
    for leaf in coverage["required_leaves"]:
        for ref in leaf.get("completion_handoffs", []):
            comments = [comment for comment in rows.get(ref["bead"], {}).get("comments", []) if sha(comment.get("text", "").encode()) == ref["text_sha256"]]
            if len(comments) != 1:
                raise SystemExit(f"cannot resolve immutable handoff {ref['bead']} sha256:{ref['text_sha256']}")
        for item in leaf.get("evidence", []):
            expected = item["sha256"]
            target = PIN_ROOT / f"{expected}.json"
            raw = target.read_bytes() if target.is_file() else b""
            if not args.verify and sha(raw) != expected:
                raw = historical_bytes(item["manifest"], expected)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(raw)
            if sha(raw) != expected:
                raise SystemExit(f"pinned manifest digest mismatch: {target.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
