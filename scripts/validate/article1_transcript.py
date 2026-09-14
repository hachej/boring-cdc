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
EXPECTED_SHA256 = {
    "config/article1-reader.toml": "50945fe039ad1594d124783ec4f02558649875ed7ddf6633235d797b81b1869c",
    "fixtures/article1/schema-and-seed.sql": "7f58d39e39d26006849c0bd6f4f6e6f63cf7ae81c71ba511fc64a976e4744cf1",
    "fixtures/article1/fixture.json": "6c9f5705efead78c287fe3791aaad096f7bf6446287d6f44f789f623ff023ad1",
    "evidence/article1/reader-default.raw.jsonl": "74a7275851d82121468de916742a8067cf2906062a92cc6da0a066399275d818",
    "evidence/article1/reader-full.raw.jsonl": "de7fac472c6114a004cfc10939fe251ecf8d19a2b9d2f38ffd511137868cfa3e",
    "evidence/article1/reader.normalized.jsonl": "1842c3162593775d9cfbd0be3368769f060c617df9a3871d2101d893787f0915",
}


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
    expected_keys = [
        {"event", "final_lsn", "transaction", "wal_end", "wal_start"},
        {"event", "new", "old", "old_state", "relation_id", "transaction", "wal_end", "wal_start"},
        {"event", "new", "old", "old_state", "relation_id", "transaction", "wal_end", "wal_start"},
        {"event", "new", "old", "old_state", "relation_id", "transaction", "wal_end", "wal_start"},
        {"commit_lsn", "end_lsn", "event", "row_count", "transaction", "wal_end", "wal_start"},
    ]
    if [set(row) for row in rows] != expected_keys:
        fail(f"{scenario}: event field shape drifted")
    if set(begin.get("transaction", {})) != {"commit_time", "xid"} or set(commit.get("transaction", {})) != {"commit_time"}:
        fail(f"{scenario}: transaction boundary shape drifted")
    if any(set(row.get("transaction", {})) != {"ordinal", "xid"} for row in (insert, update, delete)):
        fail(f"{scenario}: row transaction shape drifted")
    if any(row.get("relation_id") != 16385 for row in (insert, update, delete)):
        fail(f"{scenario}: relation identity drifted")

    expected_new = (
        [["9101", "Article Default", "1"], ["9101", "Article Default Updated", "2"], None]
        if scenario == "default"
        else [["9201", "Article Full", "3"], ["9201", "Article Full Updated", "4"], None]
    )
    if [insert.get("new"), update.get("new"), delete.get("new")] != expected_new:
        fail(f"{scenario}: row payload drifted")

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
    parser.add_argument("--manifest", type=pathlib.Path, default=EVIDENCE / "manifest.json")
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
        manifest = json.loads(args.manifest.read_text())
        if set(manifest) != {
            "schema_version", "owner_bead", "capture_code_sha", "reader_command", "capture_binary_sha256",
            "capture_binary_note", "server", "fixture", "normalization", "consumer_row_shape",
            "m4_canonical_destination_row_produced_or_tested", "sha256",
        }:
            fail("manifest field inventory drifted")
        expected_identity = {
            "schema_version": "article1-reader-evidence/v1",
            "owner_bead": "boring-cdc-pci.4",
            "capture_code_sha": "30e9fe4ed6f0796b2aca8497b31d30f517f141c5",
            "reader_command": "BORING_CDC_ARTICLE1_DSN='postgresql://postgres:article1_fixture_only@127.0.0.1:55696/article1?sslmode=disable' target/debug/boring-cdc run",
            "capture_binary_sha256": "3fc5d0fe0652fcbc6b033075c555e651d46efc2f7e0aa7f105a834a6155b84b8",
            "capture_binary_note": "Digest of the exact target/debug/boring-cdc executable used for the committed raw capture; debug binaries built in another absolute checkout can differ.",
        }
        for name, expected in expected_identity.items():
            if manifest.get(name) != expected:
                fail(f"manifest identity drift for {name}")
        if manifest.get("server") != {
            "version": "PostgreSQL 17.6 (Debian 17.6-2.pgdg13+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit",
            "server_version_num": 170006,
            "image": "docker.io/library/postgres:17.6@sha256:00bc86618629af00d2937fdc5a5d63db3ff8450acf52f0636ec813c7f4902929",
            "image_id": "sha256:50903ccdcab597707a1f61c7ae016a06b0b548da53a6f7ad716d56b072bedba0",
            "platform": "linux/amd64",
        }:
            fail("manifest server/image identity drifted")
        if manifest.get("fixture") != {
            "publication": "article1_publication", "slot": "article1_slot", "seed": "workload-v1",
            "seed_rows_sha256": "238b582c56ffa001bcd4e566cfb307d26fbe3804f55a4b0be014ace3ae143e57",
        }:
            fail("manifest fixture identity drifted")
        if manifest.get("normalization") != {
            "normalized_fields": ["transaction.commit_time"], "replacement": 0,
            "serialization": "JSON objects with sorted keys and compact separators; default events then FULL events",
            "retained_unchanged": ["xid", "relation_id", "ordinal", "tuple values", "wal_start", "wal_end", "final_lsn", "commit_lsn", "end_lsn"],
        }:
            fail("manifest normalization contract drifted")
        if manifest.get("consumer_row_shape") != {
            "begin": ["event", "final_lsn", "transaction{xid,commit_time}", "wal_start", "wal_end"],
            "row": ["event", "new", "old", "old_state", "relation_id", "transaction{xid,ordinal}", "wal_start", "wal_end"],
            "commit": ["event", "commit_lsn", "end_lsn", "row_count", "transaction{commit_time}", "wal_start", "wal_end"],
        }:
            fail("manifest consumer row shape drifted")
        if manifest.get("m4_canonical_destination_row_produced_or_tested") is not False:
            fail("manifest M4 boundary drifted")
        if manifest.get("sha256") != EXPECTED_SHA256:
            fail("manifest digest inventory drifted")
        for name, expected in EXPECTED_SHA256.items():
            actual = digest(ROOT / name)
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
