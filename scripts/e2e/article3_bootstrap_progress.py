#!/usr/bin/env python3
"""Observe durable copy and capture progress from one read transaction per sample."""

import json
import os
import sqlite3
import sys
import time
from pathlib import Path


def sample(db):
    db.execute("BEGIN")
    try:
        done, total = db.execute(
            "SELECT count(*) FILTER (WHERE state='complete'),count(*) FROM backfill_chunks"
        ).fetchone()
        head, = db.execute("SELECT durable_journal_seq FROM source_state").fetchone()
        events, = db.execute("SELECT count(*) FROM journal_events WHERE control_kind IS NULL").fetchone()
        anchor = db.execute(
            "SELECT snapshot_complete_seq,post_copy_fence_seq FROM bootstrap_anchors WHERE state='complete'"
        ).fetchone()
        checkpoints = db.execute(
            "SELECT c.journal_seq FROM destination_checkpoints c "
            "JOIN destinations d USING(destination_id) WHERE d.kind='clickhouse'"
        ).fetchall()
        return {
            "chunks_done": done,
            "chunks_total": total,
            "journal_head": head,
            "durable_business_events": events,
            "complete_anchor": anchor,
            "clickhouse_checkpoints": [seq for (seq,) in checkpoints],
        }
    finally:
        db.rollback()


def main():
    database, output, pid, *mode = sys.argv[1:]
    deadline = time.monotonic() + (1 if mode else 180)
    incomplete = None
    with sqlite3.connect(f"file:{Path(database).resolve()}?mode=ro", uri=True, timeout=1) as db:
        while time.monotonic() < deadline:
            os.kill(int(pid), 0)
            observed = sample(db)
            Path(output).write_text(json.dumps(observed, indent=2, sort_keys=True) + "\n")
            done = observed["chunks_done"]
            total = observed["chunks_total"]
            events = observed["durable_business_events"]
            anchor = observed["complete_anchor"]
            if mode:
                head = observed["journal_head"]
                caught_up = head is not None and observed["clickhouse_checkpoints"] == [head]
                return 0 if anchor and 0 < done == total and events >= 18 and caught_up else 1
            if 0 < done < total and anchor is None:
                if incomplete is None:
                    incomplete = observed
                elif done > incomplete["chunks_done"] and events > incomplete["durable_business_events"]:
                    Path(output).write_text(json.dumps({"before":incomplete,"after":observed}, indent=2, sort_keys=True) + "\n")
                    return 0
            if anchor is not None:
                print("Anchor completed before overlap was observed", file=sys.stderr)
                return 1
            time.sleep(0.1)
    print("Timed out observing durable WAL events during snapshot copy", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
