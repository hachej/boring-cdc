#!/usr/bin/env python3
"""Validate direct PostgreSQL feedback fault receipts and their sealed packet."""
from __future__ import annotations

import hashlib
import json
import pathlib
import re
import sys


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def single(path: pathlib.Path, hook: str) -> dict:
    assert hook in {"before_feedback", "after_feedback"}
    receipt = json.loads((path / "receipt.json").read_text())
    assert receipt["schema_version"] == "m2-feedback-fault-receipt/v1"
    assert receipt["hook"] == hook
    assert receipt["child_command"] == ["target/debug/boring-cdc", "run"]
    assert isinstance(receipt["child_pid"], int) and receipt["child_pid"] > 0
    assert receipt["child_exit_code"] == 134
    assert receipt["postgres_version"].startswith("17.6")
    assert receipt["durable_transaction_count"] == 1
    assert re.fullmatch(r"[0-9A-F]+/[0-9A-F]+", receipt["durable_lsn"])
    assert isinstance(receipt["elapsed_since_runtime_start_ms"], int)
    assert receipt["elapsed_since_runtime_start_ms"] >= 0
    assert receipt["feedback_match_observed"] == (hook == "after_feedback")
    if hook == "after_feedback":
        assert receipt["server_feedback_positions_observed"] == ",".join([receipt["durable_lsn"]] * 3)
    else:
        assert receipt["server_feedback_positions_observed"] != ",".join([receipt["durable_lsn"]] * 3)
    for stem in ("stdout", "stderr"):
        assert receipt[f"{stem}_sha256"] == sha((path / f"runtime.{stem}").read_bytes())
    return receipt


def packet(path: pathlib.Path) -> None:
    seen = []
    for attempt in (1, 2):
        for hook in ("before_feedback", "after_feedback"):
            receipt = single(path / f"attempt-{attempt}" / hook, hook)
            seen.append((hook, receipt["child_exit_code"], receipt["feedback_match_observed"], receipt["durable_transaction_count"]))
    assert seen[:2] == seen[2:]
    manifest = json.loads((path / "manifest.json").read_text())
    assert manifest["owner_bead"] == "boring-cdc-m2-fault-status.1"
    assert manifest["scenario_id"] == "SCN-M2-FEEDBACK-ABORT-RECEIPTS"
    assert manifest["result"]["status"] == "pass"
    assert manifest["result"]["runtime_observed"] is True
    assert manifest["result"]["attempts"] == ["attempt-1", "attempt-2"]
    listed = (path / "sha256.txt").read_text().splitlines()
    actual = sorted(p for p in path.rglob("*") if p.is_file() and p.name != "sha256.txt")
    assert listed == [f"{sha(p.read_bytes())}  {p.relative_to(path).as_posix()}" for p in actual]
    for file in actual:
        raw = file.read_bytes().lower()
        assert b"postgresql://" not in raw and b"password=" not in raw


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "single":
        single(pathlib.Path(sys.argv[2]), sys.argv[3])
    elif len(sys.argv) == 3 and sys.argv[1] == "packet":
        packet(pathlib.Path(sys.argv[2]))
    else:
        raise SystemExit("usage: m2_feedback_receipts.py single DIR HOOK | packet DIR")
