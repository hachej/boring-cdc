#!/usr/bin/env python3
"""Turn source and destination query results into a redacted outage packet."""

import base64
import hashlib
import json
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def decode_value(cell):
    encoded = cell[4]
    raw = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
    return int.from_bytes(raw, "big", signed=True) if int(cell[2]) == 20 else raw.decode()


def main():
    work = Path(sys.argv[1])
    repo = Path(sys.argv[2])
    packet = Path(sys.argv[3])
    stopped_ms = int(sys.argv[4])
    restarted_ms = int(sys.argv[5])
    caught_up_ms = int(sys.argv[6]) if sys.argv[6] else None
    outage_seconds = int(sys.argv[7])
    packet.mkdir(parents=True, exist_ok=False)
    samples = [json.loads(line) for line in (work / "samples.jsonl").read_text().splitlines() if line]
    source_rows = {}
    for line in (work / "source-oracle.tsv").read_text().splitlines():
        key, value = line.split("|", 1)
        source_rows[int(key)] = value
    remote_rows = {}
    for line in (work / "candidate-rows.jsonl").read_text().splitlines():
        row = json.loads(line)
        key = int(json.loads(row["canonical_key"])[0]["value"])
        cells = {int(cell[0]): cell for cell in row["explicit_cells"]}
        remote_rows[key] = decode_value(cells[1])
        if decode_value(cells[0]) != key:
            raise ValueError("candidate key mismatch")
    db = sqlite3.connect(work / "state/boring.db")
    try:
        expected = {row[0] for row in db.execute("SELECT event_id FROM journal_events WHERE control_kind IS NULL")}
        failures = [
            {"component": row[0], "class": row[1], "retry_class": row[2], "attempt": row[3], "armed": bool(row[4])}
            for row in db.execute(
                "SELECT component,failure_class,retry_class,attempt,armed FROM processing_failures "
                "WHERE destination_id='snapshot-probe' ORDER BY component,failure_id"
            )
        ]
    finally:
        db.close()
    remote_ids = (work / "remote-event-ids.txt").read_text().splitlines()
    distinct_remote = set(remote_ids)
    lost = len(expected - distinct_remote)
    extra = len(distinct_remote - expected)
    physical = len(remote_ids)
    duplicates = physical - len(distinct_remote)
    oracle_match = source_rows == remote_rows
    errors = []
    if caught_up_ms is None:
        errors.append("DESTINATION_DID_NOT_CATCH_UP")
    if lost:
        errors.append("LOST_EVENT_IDS")
    if extra:
        errors.append("FOREIGN_EVENT_IDS")
    if not oracle_match:
        errors.append("ORACLE_MISMATCH")
    if any(row["retry_class"] == "exhausted" for row in failures):
        errors.append("DESTINATION_RETRY_EXHAUSTED")
    if samples:
        max_retained = max(sample["slot"]["retained_bytes"] for sample in samples)
        max_lag = max(sample["destination"]["lag_events"] for sample in samples)
    else:
        max_retained = max_lag = 0
        errors.append("NO_SAMPLES")
    observation = {
        "schema_version": "article2-destination-outage/v1",
        "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
        "images": {"postgres": "17.6", "clickhouse": "25.8.2.29"},
        "profile": {"sample_interval_seconds": 5, "workload_interval_seconds": 5, "prelude_transactions": int((work / "prelude-complete").read_text()), "outage_seconds": outage_seconds, "workload_transactions": int((work / "workload-complete").read_text()), "retained_wal_limit_bytes": 268435456},
        "timeline": {"clickhouse_stopped_ms": stopped_ms, "clickhouse_restarted_ms": restarted_ms, "caught_up_ms": caught_up_ms, "catch_up_ms": caught_up_ms - restarted_ms if caught_up_ms is not None else None},
        "samples": samples,
        "outcome": {"oracle_match": oracle_match, "lost_event_ids": lost, "foreign_event_ids": extra, "remote_physical_rows": physical, "remote_distinct_event_ids": len(distinct_remote), "duplicate_rows": duplicates, "operator_steps": []},
        "destination_failures": failures,
        "peak_retained_wal_bytes": max_retained,
        "peak_destination_lag_events": max_lag,
        "failure_codes": errors,
        "result": "fail" if errors else "pass",
    }
    observation_path = packet / "observation.json"
    observation_path.write_text(json.dumps(observation, indent=2, sort_keys=True) + "\n")
    oracle_path = packet / "source-oracle.tsv"
    oracle_path.write_bytes((work / "source-oracle.tsv").read_bytes())
    validator_path = packet / "validate.py"
    shutil.copyfile(repo / "scripts/validate/article2_destination_outage.py", validator_path)
    inputs = [
        "scripts/e2e/article2_destination_outage.sh",
        "scripts/e2e/article2_destination_sample.py",
        "scripts/e2e/article2_destination_packet.py",
        "src/m2_capture_runtime.rs",
        "src/m3_bootstrap.rs",
        "src/m5_clickhouse_loop.rs",
        "contracts/m2/failure-policy-cases.json",
        "contracts/clickhouse/ddl.sql",
    ]
    manifest = {
        "schema_version": "article2-destination-outage-manifest/v1",
        "git_commit": observation["git_commit"],
        "input_sha256": {name: sha256(repo / name) for name in inputs},
        "config_sha256": sha256(work / "boring-cdc.toml"),
        "observation_sha256": sha256(observation_path),
        "oracle_sha256": sha256(oracle_path),
        "validator_sha256": sha256(validator_path),
        "image_digests": {
            "postgres": "sha256:00bc86618629af00d2937fdc5a5d63db3ff8450acf52f0636ec813c7f4902929",
            "clickhouse": "sha256:74c213b4d4cb4854c2497694df0c2d153c041003eadbb0457ae62c28cb8d723f",
        },
    }
    (packet / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    (packet / "README.md").write_text(
        "# Article 2 destination outage\n\n"
        f"Result: **{observation['result']}**. Failure codes: {', '.join(errors) if errors else 'none'}.\n\n"
        "The M3 live fixture used actual PostgreSQL snapshot/import/fence and verified the initial ClickHouse batch. "
        "The shipped `boring-cdc run` process then owned capture and destination delivery. "
        f"ClickHouse was stopped with `docker stop` for {outage_seconds} seconds while PostgreSQL transactions continued. "
        "The candidate keyed query was compared with a separate PostgreSQL query. "
        "Managed Estuary comparison remains unavailable.\n\n"
        "Run `python3 validate.py observation.json` offline. A failing outcome exits nonzero and is retained.\n"
    )
    print(json.dumps({"result": observation["result"], "failure_codes": errors, "peak_retained_wal_bytes": max_retained, "peak_destination_lag_events": max_lag, "catch_up_ms": observation["timeline"]["catch_up_ms"], "oracle_match": oracle_match, "duplicate_rows": duplicates}, sort_keys=True))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
