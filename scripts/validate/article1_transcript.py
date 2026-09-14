#!/usr/bin/env python3
"""Validate and normalize the committed Article 1 live reader transcript."""

import argparse
import hashlib
import json
import pathlib
import re
import sys
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parents[2]
EVIDENCE = ROOT / "evidence/article1"
LSN = re.compile(r"^[0-9A-F]+/[0-9A-F]+$")


def fail(message: str) -> None:
    raise ValueError(message)


def load_jsonl(path: pathlib.Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for number, line in enumerate(path.read_text().splitlines(), 1):
        try:
            value = json.loads(line)
        except json.JSONDecodeError as error:
            fail(f"{path}:{number}: invalid JSON: {error}")
        if not isinstance(value, dict):
            fail(f"{path}:{number}: event is not an object")
        rows.append(value)
    return rows


def lsn_value(value: object, label: str) -> int:
    if not isinstance(value, str) or not LSN.fullmatch(value):
        fail(f"{label}: invalid LSN {value!r}")
    high, low = value.split("/")
    return (int(high, 16) << 32) | int(low, 16)


def validate_scenario(rows: list[dict[str, Any]], scenario: str) -> None:
    expected_events = ["BEGIN", "INSERT", "UPDATE", "DELETE", "COMMIT"]
    if [row.get("event") for row in rows] != expected_events:
        fail(f"{scenario}: expected event sequence {expected_events}")

    begin, insert, update, delete, commit = rows
    xid = begin.get("transaction", {}).get("xid")
    if not isinstance(xid, int):
        fail(f"{scenario}: BEGIN xid is absent")
    for ordinal, row in enumerate((insert, update, delete)):
        transaction = row.get("transaction", {})
        if transaction.get("xid") != xid or transaction.get("ordinal") != ordinal:
            fail(f"{scenario}: transaction envelope drift at ordinal {ordinal}")
    begin_time = begin.get("transaction", {}).get("commit_time")
    commit_time = commit.get("transaction", {}).get("commit_time")
    if not isinstance(begin_time, int) or begin_time != commit_time:
        fail(f"{scenario}: BEGIN/COMMIT timestamp relationship drifted")
    if commit.get("row_count") != 3:
        fail(f"{scenario}: COMMIT row_count is not 3")

    for index, row in enumerate(rows):
        start = lsn_value(row.get("wal_start"), f"{scenario}[{index}].wal_start")
        end = lsn_value(row.get("wal_end"), f"{scenario}[{index}].wal_end")
        if start != end:
            fail(f"{scenario}: event WAL start/end relationship drifted")
    event_lsns = [lsn_value(row["wal_start"], scenario) for row in rows]
    if event_lsns != sorted(event_lsns) or not event_lsns[0] == event_lsns[1] < event_lsns[2] < event_lsns[3] < event_lsns[4]:
        fail(f"{scenario}: expected BEGIN/first-row shared LSN followed by increasing row/COMMIT LSNs")
    final_lsn = lsn_value(begin.get("final_lsn"), f"{scenario}.final_lsn")
    commit_lsn = lsn_value(commit.get("commit_lsn"), f"{scenario}.commit_lsn")
    end_lsn = lsn_value(commit.get("end_lsn"), f"{scenario}.end_lsn")
    if final_lsn != commit_lsn or not event_lsns[-2] < commit_lsn < end_lsn:
        fail(f"{scenario}: BEGIN/COMMIT LSN relationship drifted")
    if event_lsns[-1] != end_lsn:
        fail(f"{scenario}: COMMIT WAL/end LSN relationship drifted")

    expected_states = ["absent", "absent", "key"] if scenario == "default" else ["absent", "full", "full"]
    if [insert.get("old_state"), update.get("old_state"), delete.get("old_state")] != expected_states:
        fail(f"{scenario}: replica-identity old-state contract drifted")
    if insert.get("old") is not None:
        fail(f"{scenario}: INSERT unexpectedly has an old tuple")
    if scenario == "default":
        if update.get("old") is not None or delete.get("old") != ["9101", None, None]:
            fail("default: expected absent UPDATE old tuple and key-only DELETE old tuple")
    else:
        if update.get("old") != ["9201", "Article Full", "3"]:
            fail("full: UPDATE does not contain the complete old tuple")
        if delete.get("old") != ["9201", "Article Full Updated", "4"]:
            fail("full: DELETE does not contain the complete old tuple")


def normalized_lines(default_path: pathlib.Path, full_path: pathlib.Path) -> list[str]:
    output: list[str] = []
    for scenario, path in (("default", default_path), ("full", full_path)):
        rows = load_jsonl(path)
        validate_scenario(rows, scenario)
        for row in rows:
            transaction = row.get("transaction", {})
            if "commit_time" in transaction:
                transaction["commit_time"] = 0
            output.append(json.dumps(row, sort_keys=True, separators=(",", ":")))
    return output


def digest(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--default", type=pathlib.Path, default=EVIDENCE / "reader-default.raw.jsonl")
    parser.add_argument("--full", type=pathlib.Path, default=EVIDENCE / "reader-full.raw.jsonl")
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("--skip-manifest", action="store_true")
    args = parser.parse_args()

    lines = normalized_lines(args.default, args.full)
    normalized = "\n".join(lines) + "\n"
    if args.output:
        args.output.write_text(normalized)
    else:
        committed = EVIDENCE / "reader.normalized.jsonl"
        if committed.read_text() != normalized:
            fail("committed normalized transcript does not derive from the raw stdout transcripts")

    if not args.skip_manifest:
        manifest = json.loads((EVIDENCE / "manifest.json").read_text())
        for name, expected in manifest["sha256"].items():
            path = ROOT / name
            actual = digest(path)
            if actual != expected:
                fail(f"digest drift for {name}: expected {expected}, got {actual}")
    print("ARTICLE1_TRANSCRIPT_OK real_stdout=true events=BEGIN,INSERT,UPDATE,DELETE,COMMIT old_states=absent,key,full lsn_relationships=valid")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"ARTICLE1_TRANSCRIPT_VALIDATION_FAILED: {error}", file=sys.stderr)
        raise SystemExit(1)
