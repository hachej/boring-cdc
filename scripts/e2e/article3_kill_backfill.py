#!/usr/bin/env python3
"""Observe durable backfill state around a process interruption."""

import json
import os
import sqlite3
import sys
import time
from pathlib import Path


def sample(db):
    db.execute("BEGIN")
    try:
        generations = db.execute(
            """SELECT g.generation,g.state,
                      count(c.chunk_id) FILTER (WHERE c.state='complete'),count(c.chunk_id)
               FROM backfill_generations g LEFT JOIN backfill_chunks c USING(generation_id)
               GROUP BY g.generation_id ORDER BY g.generation"""
        ).fetchall()
        epoch, head = db.execute(
            "SELECT capture_epoch,durable_journal_seq FROM source_state"
        ).fetchone()
        anchors = db.execute(
            """SELECT generation,snapshot_complete_seq,post_copy_fence_seq
               FROM bootstrap_anchors WHERE capture_epoch=? AND state='complete'""",
            (epoch,),
        ).fetchall()
        checkpoints = db.execute(
            """SELECT c.journal_seq FROM destination_checkpoints c JOIN destinations d USING(destination_id)
               WHERE d.kind='clickhouse' AND c.capture_epoch=?""",
            (epoch,),
        ).fetchall()
        events, = db.execute("SELECT count(*) FROM journal_events WHERE control_kind IS NULL").fetchone()
        return {
            "capture_epoch": epoch,
            "journal_head": head,
            "durable_business_events": events,
            "generations": [
                {"generation": gen, "state": state, "chunks_done": done, "chunks_total": total}
                for gen, state, done, total in generations
            ],
            "complete_anchors": [
                {"generation": gen, "copied_rows": rows, "fence_seq": fence}
                for gen, rows, fence in anchors
            ],
            "clickhouse_checkpoints": [seq for (seq,) in checkpoints],
        }
    finally:
        db.rollback()


def at_midpoint(observed):
    if len(observed["generations"]) != 1 or observed["complete_anchors"]:
        return False
    generation, = observed["generations"]
    done, total = generation["chunks_done"], generation["chunks_total"]
    return (
        generation["state"] == "copying"
        and total > 0
        and 45 * total <= 100 * done <= 55 * total
    )


def write(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def main():
    mode, database, output, *args = sys.argv[1:]
    output = Path(output)
    with sqlite3.connect(f"file:{Path(database).resolve()}?mode=ro", uri=True, timeout=1) as db:
        if mode == "midpoint":
            pid, = map(int, args)
            deadline = time.monotonic() + 600
            while time.monotonic() < deadline:
                os.kill(pid, 0)
                observed = sample(db)
                write(output, observed)
                if at_midpoint(observed):
                    return 0
                if observed["complete_anchors"]:
                    break
                time.sleep(0.05)
            print("No incomplete generation observed near its midpoint", file=sys.stderr)
            return 1

        observed = sample(db)
        if mode == "killed":
            write(output, observed)
            return 0 if at_midpoint(observed) else 1

        before = json.loads(Path(args[0]).read_text())
        original, = before["generations"]
        original_number = original["generation"]
        generations = observed["generations"]
        replacement = next((gen for gen in reversed(generations) if gen["generation"] > original_number), None)
        old = next((gen for gen in generations if gen["generation"] == original_number), None)
        anchor = next(
            (a for a in observed["complete_anchors"] if replacement and a["generation"] == replacement["generation"]),
            None,
        )
        previous_anchor = any(a["generation"] == original_number for a in observed["complete_anchors"])
        head = observed["journal_head"]
        caught_up = head is not None and observed["clickhouse_checkpoints"] == [head]
        if replacement:
            observed["restart_behavior"] = "new_generation"
            observed["chunks_read_in_replacement_generation"] = replacement["chunks_done"]
        elif previous_anchor or (old and old["chunks_done"] > original["chunks_done"]):
            observed["restart_behavior"] = "reused_generation"
        else:
            observed["restart_behavior"] = "replacement_not_observed"
        if anchor:
            observed["elapsed_to_anchor_ms"] = (time.monotonic_ns() - int(args[1])) // 1_000_000
            if output.is_file():
                observed["elapsed_to_anchor_ms"] = json.loads(output.read_text()).get(
                    "elapsed_to_anchor_ms", observed["elapsed_to_anchor_ms"]
                )
        write(output, observed)
        restarted = (
            replacement and old and anchor
            and observed["capture_epoch"] == before["capture_epoch"]
            and old["state"] == "invalidated"
            and not previous_anchor
            and 0 < replacement["chunks_done"] == replacement["chunks_total"]
        )
        if mode == "replacement":
            return 0 if restarted else 1
        expected_events = int(args[2])
        return 0 if (
            restarted and caught_up and expected_events > 0
            and observed["durable_business_events"] == expected_events
        ) else 1


if __name__ == "__main__":
    sys.exit(main())
