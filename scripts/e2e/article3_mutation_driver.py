#!/usr/bin/env python3
"""Drive and verify mutations against completed, claimed and pending snapshot ranges."""

import argparse
import json
import os
import sqlite3
import subprocess
import time
from contextlib import closing
from pathlib import Path

from article3_bootstrap_oracle import read_candidate, read_source

CATEGORIES = ("copied", "copying", "pending")
OPERATIONS = ("update", "delete", "update_then_delete")


def key(bound):
    if len(bound) != 9 or bound[0] != 1:
        raise ValueError("expected a finite bigint key bound")
    return int.from_bytes(bound[1:], "big") - (1 << 63)


def connection(path):
    return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1)


def pick(db, reserved):
    db.execute("BEGIN")
    try:
        copying = db.execute(
            """SELECT c.chunk_id,c.range_start,c.range_end
               FROM m3_chunk_claims q JOIN backfill_chunks c USING(chunk_id,generation_id)
               WHERE c.state='pending' ORDER BY c.chunk_id LIMIT 1"""
        ).fetchone()
        if copying is None or not all(copying[1:]):
            return None
        copied, pending = (
            db.execute(
                f"""SELECT chunk_id,range_start,range_end FROM backfill_chunks
                    WHERE generation_id=(SELECT generation_id FROM backfill_chunks WHERE chunk_id=?)
                    AND state=? AND chunk_id {side} ? AND length(range_start)=9 AND length(range_end)=9
                    ORDER BY chunk_id {order} LIMIT 1""",
                (copying[0], state, copying[0]),
            ).fetchone()
            for state, side, order in (("complete", "<", "DESC"), ("pending", ">", "ASC"))
        )
        if copied is None or pending is None:
            return None
        selected = []
        for category, row in zip(CATEGORIES, (copied, copying, pending), strict=True):
            candidates = (ident for ident in range(key(row[1]) + 10, key(row[2])) if ident not in reserved)
            for operation, ident in zip(OPERATIONS, candidates):
                selected.append({"category": category, "chunk": row[0], "key": ident,
                                 "operation": operation})
        return selected
    finally:
        db.rollback()


def write(db_path, ledger, concurrent_writes):
    reserved = set(range(1, concurrent_writes + 1))
    reserved.update(range(5001, 5001 + concurrent_writes))
    deadline = time.monotonic() + 180
    selected = None
    while time.monotonic() < deadline:
        try:
            with closing(connection(db_path)) as db:
                selected = pick(db, reserved)
        except sqlite3.OperationalError as error:
            if "no such table" not in str(error):
                raise
        if selected and len(selected) == len(CATEGORIES) * len(OPERATIONS):
            break
        time.sleep(0.02)
    else:
        raise RuntimeError("no three usable snapshot ranges observed")

    selected_ms = time.monotonic_ns() // 1_000_000
    statements = ["BEGIN;", "SELECT pg_current_xact_id()::text;"]
    for change in selected:
        ident = change["key"]
        if change["operation"] != "delete":
            statements.append(f"UPDATE public.orders SET payload='mutation-{ident}' WHERE id={ident};")
        if change["operation"] != "update":
            statements.append(f"DELETE FROM public.orders WHERE id={ident};")
    statements.append("COMMIT;")
    result = subprocess.run(
        ["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1"],
        input="\n".join(statements),
        text=True,
        capture_output=True,
        check=True,
        timeout=15,
    )
    acknowledged_ms = time.monotonic_ns() // 1_000_000
    xid = int(result.stdout.strip()) % (1 << 32)
    ledger.write_text(json.dumps({"changes": selected, "xid": xid, "selected_mono_ms": selected_ms,
                                  "commit_returned_mono_ms": acknowledged_ms},
                                indent=2, sort_keys=True) + "\n")


def verify(db_path, ledger, source_path, candidate_path, result_path):
    run = json.loads(ledger.read_text())
    source, candidate = read_source(source_path), read_candidate(candidate_path)
    with closing(connection(db_path)) as db:
        matches = db.execute(
            """SELECT t.end_lsn,t.event_count,
                      json_extract(CAST(e.payload AS TEXT),'$.source_version.lsn_u64')
               FROM source_transactions t JOIN journal_events e
                 ON t.transaction_id=e.transaction_id AND e.journal_seq=t.first_seq
               WHERE t.xid=? AND t.state='committed'""",
            (str(run["xid"]),),
        ).fetchall()
        if len(matches) != 1:
            raise RuntimeError("expected one durable source transaction for mutation XID")
        end_lsn, event_count, commit_lsn = matches[0]
        copy_ms, = db.execute(
            """SELECT max(c.committed_mono_ms)-min(p.started_mono_ms)
               FROM m3_planner_runs p JOIN m3_chunk_commits c USING(generation_id)"""
        ).fetchone()
        copy = {
            chunk: committed for chunk, committed in db.execute(
                "SELECT chunk_id,committed_mono_ms FROM m3_chunk_commits"
            )
        }

    missing, resurrected, stale = 0, 0, 0
    for change in run["changes"]:
        ident = change["key"]
        expected = f"mutation-{ident}" if change["operation"] == "update" else None
        pg_value, ch_value = source.get(ident), candidate.get(ident)
        change.update(postgres=pg_value, clickhouse=ch_value, expected=expected)
        missing += expected is not None and ch_value is None
        resurrected += expected is None and ch_value is not None
        stale += pg_value != expected or (ch_value is not None and expected is not None and ch_value != expected)
    # A completed source response gives an upper bound on the PostgreSQL commit time.
    unfinished_at_commit = all(
        copy[change["chunk"]] > run["commit_returned_mono_ms"]
        for change in run["changes"] if change["category"] != "copied"
    )
    copied_before_mutation = all(
        copy[change["chunk"]] <= run["selected_mono_ms"]
        for change in run["changes"] if change["category"] == "copied"
    )
    minimum_copy_ms = int(os.environ.get("BCDC_ARTICLE3_MUTATION_MIN_COPY_MS", "0"))
    passed = (isinstance(copy_ms, int) and copy_ms >= minimum_copy_ms
              and copied_before_mutation and unfinished_at_commit and event_count == 12
              and isinstance(commit_lsn, int) and not any((missing, resurrected, stale)))
    run.update(commit_lsn_u64=commit_lsn, commit_end_lsn=end_lsn, durable_events=event_count,
               copy_elapsed_ms=copy_ms, minimum_copy_ms=minimum_copy_ms,
               copied_before_mutation=copied_before_mutation,
               unfinished_at_commit=unfinished_at_commit, missing_keys=missing,
               resurrected_deletes=resurrected, stale_values=stale, result="pass" if passed else "fail")
    result_path.write_text(json.dumps(run, indent=2, sort_keys=True) + "\n")
    return 0 if passed else 1


def main():
    parser = argparse.ArgumentParser()
    modes = parser.add_subparsers(dest="mode", required=True)
    for mode, paths in [
        ("write", ("database", "ledger")),
        ("verify", ("database", "ledger", "source", "candidate", "result")),
    ]:
        entry = modes.add_parser(mode)
        for path in paths:
            entry.add_argument(path, type=Path)
        if mode == "write":
            entry.add_argument("--concurrent-writes", type=int, required=True)
    args = parser.parse_args()
    if args.mode == "write":
        return write(args.database, args.ledger, args.concurrent_writes)
    return verify(args.database, args.ledger, args.source, args.candidate, args.result)


if __name__ == "__main__":
    raise SystemExit(main())
