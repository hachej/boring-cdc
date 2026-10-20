#!/usr/bin/env python3
"""Five-second source, journal, destination, and status sampler for Article 2."""

import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path


def disk_bytes(path):
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file()) if path.exists() else 0


def sample(work, pg_port, slot, binary, start_ns):
    query = (
        "SELECT active,restart_lsn::text,confirmed_flush_lsn::text,"
        "pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)::bigint "
        f"FROM pg_replication_slots WHERE slot_name='{slot}'"
    )
    source = subprocess.check_output(
        ["psql", "-At", "-F", "|", "-h", "127.0.0.1", "-p", str(pg_port), "-U", "boring_cdc", "-d", "boring_cdc", "-c", query],
        text=True,
    ).strip().split("|")
    if len(source) != 4:
        raise RuntimeError("replication slot sample unavailable")
    db_path = work / "state/boring.db"
    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        head = db.execute("SELECT coalesce(durable_journal_seq,0),coalesce(durable_transaction_end_lsn,'0000000000000000') FROM source_state WHERE singleton=1").fetchone()
        checkpoint = db.execute("SELECT journal_seq FROM destination_checkpoints WHERE destination_id='snapshot-probe'").fetchone()
    finally:
        db.close()
    status = json.loads(subprocess.check_output([str(binary), "status", "--json"], cwd=work, text=True))["data"]
    sqlite_bytes = sum(path.stat().st_size for path in (db_path, Path(str(db_path) + "-wal"), Path(str(db_path) + "-shm")) if path.exists())
    checkpoint_seq = checkpoint[0] if checkpoint else 0
    return {
        "elapsed_ms": (time.monotonic_ns() - start_ns) // 1_000_000,
        "slot": {"active": source[0] == "t", "restart_lsn": source[1], "confirmed_flush_lsn": source[2], "retained_bytes": int(source[3])},
        "journal": {"head_seq": head[0], "head_lsn": head[1]},
        "destination": {"checkpoint_seq": checkpoint_seq, "lag_events": head[0] - checkpoint_seq},
        "sizes": {"sqlite_bytes": sqlite_bytes, "spool_bytes": disk_bytes(work / "state/spool")},
        "status": {"overall_health": status["overall_health"], "ownership_state": status["ownership"]["state"], "conditions": [{"reason": row["reason"], "severity": row["severity"]} for row in status["condition_details"]]},
    }


def main():
    work = Path(sys.argv[1])
    pg_port = int(sys.argv[2])
    slot = sys.argv[3]
    if not slot.replace("_", "").isalnum():
        raise ValueError("unsafe slot name")
    binary = Path(sys.argv[4])
    start_ns = int(sys.argv[5])
    output = work / "samples.jsonl"
    due = time.monotonic()
    with output.open("w") as stream:
        while not (work / "stop-sampling").exists():
            reading = sample(work, pg_port, slot, binary, start_ns)
            stream.write(json.dumps(reading, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
            due += 5
            time.sleep(max(0, due - time.monotonic()))


if __name__ == "__main__":
    main()
