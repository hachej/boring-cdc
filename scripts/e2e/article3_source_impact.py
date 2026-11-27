#!/usr/bin/env python3
"""Measure source load for fresh backfill and plain streaming on sibling fixtures."""

import contextlib
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import statistics
import subprocess
import sys
import threading
import time


READER_INTERVAL = 0.1
WRITER_INTERVAL = 5
SAMPLE_INTERVAL = 1
MAX_SECONDS = 900
DDL_BOUND_MS = 5000
FULL_ROWS = 200000
FULL_BACKFILL_MIN_MS = 180_000
READ_QUERY = "SELECT length(payload) FROM public.orders WHERE id=1;"
PSQL = ["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1"]
METRICS_SQL = """
SELECT json_build_object(
 'backend_xmin_age_xids', (SELECT max(age(backend_xmin)) FROM pg_stat_activity WHERE pid <> pg_backend_pid()),
 'longest_transaction_ms', (SELECT max(extract(epoch FROM (clock_timestamp() - xact_start))*1000)
                            FROM pg_stat_activity WHERE datname=current_database() AND pid <> pg_backend_pid()),
 'snapshot_session_transaction_ms', (SELECT max(extract(epoch FROM (clock_timestamp() - xact_start))*1000)
                                     FROM pg_stat_activity WHERE datname=current_database()
                                     AND backend_xmin IS NOT NULL
                                     AND (usename='boring_cdc_runtime' OR application_name='article3-naive')),
 'dead_tuples_estimate', (SELECT n_dead_tup FROM pg_stat_user_tables WHERE relid='public.orders'::regclass),
 'retained_slot_wal_bytes', (SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
                             FROM pg_replication_slots WHERE slot_name='boring_slot'),
 'slot_active', (SELECT active FROM pg_replication_slots WHERE slot_name='boring_slot'),
 'table_lock_waiters', (SELECT coalesce(json_agg(json_build_object(
     'mode', mode, 'wait_ms', extract(epoch FROM (clock_timestamp()-waitstart))*1000)), '[]'::json)
     FROM pg_locks WHERE relation='public.orders'::regclass AND NOT granted)
)
"""


def save(path, value):
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")


def command(argv, **kwargs):
    return subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, timeout=10, **kwargs)


def app_env():
    return dict(os.environ, PGUSER="boring_cdc_app", PGPASSWORD=os.environ["IMPACT_APP_SECRET"],
                PGOPTIONS="-c statement_timeout=15000")


def psql_client(query, env=None):
    return command(PSQL + ["-c", query], env=env, check=True).stdout


def elapsed_ms(start):
    return (time.monotonic_ns() - start) / 1_000_000


def reader(stop, latencies, errors):
    with subprocess.Popen(PSQL, env=app_env(), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                          stderr=subprocess.DEVNULL, text=True, bufsize=1) as connection:
        while not stop.is_set():
            start = time.monotonic_ns()
            try:
                connection.stdin.write(READ_QUERY + "\n")
                connection.stdin.flush()
                result = connection.stdout.readline()
                if not result.strip().isdecimal():
                    raise ValueError("reader query returned no numeric value")
                latencies.append((time.monotonic_ns(), elapsed_ms(start)))
            except (OSError, ValueError):
                errors.append("E_ARTICLE3_READER")
                return
            stop.wait(max(0, READER_INTERVAL - elapsed_ms(start) / 1000))
        connection.stdin.close()
        connection.wait(timeout=5)


def writer(stop, begin, commits, errors, seed_rows):
    begin.wait()
    for number in range(1, MAX_SECONDS // WRITER_INTERVAL + 3):
        if stop.is_set():
            return
        start = time.monotonic_ns()
        key = 2 + number % (seed_rows - 1)
        scratch = seed_rows + 1
        try:
            psql_client(
                f"BEGIN; UPDATE public.orders SET payload=repeat(md5('{number}'),16) WHERE id={key};"
                f"DELETE FROM public.orders WHERE id={scratch};"
                f"INSERT INTO public.orders VALUES({scratch}, 'scratch'); COMMIT;",
                app_env(),
            )
        except (subprocess.SubprocessError, OSError):
            if not stop.is_set():
                errors.append("E_ARTICLE3_WRITER")
            return
        commits.append(time.monotonic_ns())
        stop.wait(max(0, WRITER_INTERVAL - elapsed_ms(start) / 1000))


def get_cgroup(container):
    try:
        res = command(["docker", "exec", container, "cat", "/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/io.stat"])
    except subprocess.TimeoutExpired:
        return {}
    out = {}
    for line in res.stdout.splitlines():
        if line.startswith("usage_usec "):
            out["source_cpu_usage_usec"] = int(line.split()[1])
        elif re.match(r"^\d+:\d+ ", line):
            for entry in line.split()[1:]:
                field, _, count = entry.partition("=")
                if field in ("rbytes", "wbytes"):
                    name = "source_io_" + field
                    out[name] = out.get(name, 0) + int(count)
    return out


def metrics(container, started):
    pg = json.loads(psql_client(METRICS_SQL))
    return dict(pg, **get_cgroup(container), elapsed_ms=elapsed_ms(started))


def copy_state():
    try:
        with contextlib.closing(sqlite3.connect("file:state/boring.db?mode=ro", uri=True, timeout=0.5)) as db:
            db.execute("BEGIN")
            chunks, total = db.execute(
                "SELECT coalesce(sum(state='complete'),0),count(*) FROM backfill_chunks"
            ).fetchone()
            anchor = db.execute(
                "SELECT snapshot_complete_seq,post_copy_fence_seq FROM bootstrap_anchors WHERE state='complete'"
            ).fetchone()
            return {"complete_chunks": chunks, "total_chunks": total, "complete_anchor": anchor}
    except sqlite3.OperationalError as error:
        if "locked" not in str(error):
            raise
        return None


def latency_stats(values):
    ordered = sorted(values)
    if not ordered:
        return {"count": 0, "p50_ms": None, "p95_ms": None, "p99_ms": None}
    return {
        "count": len(ordered),
        "p50_ms": statistics.median(ordered),
        "p95_ms": ordered[math.ceil(len(ordered) * 0.95) - 1],
        "p99_ms": ordered[math.ceil(len(ordered) * 0.99) - 1],
    }


def curves(latencies, start, end, result):
    selected = [(when, value) for when, value in latencies if start <= when <= end]
    result["latency"] = latency_stats([value for _, value in selected])
    result["reader_samples"] = [{"elapsed_ms": (when - start) / 1_000_000, "latency_ms": value}
                                for when, value in selected]
    buckets = {}
    for when, value in selected:
        second = (when - start) // 1_000_000_000
        buckets.setdefault(second, []).append(value)
    for sample in result["samples"]:
        second = int(sample["elapsed_ms"] // 1000)
        sample["reader_latency_in_second"] = latency_stats(buckets.get(second, []))


def summarize_metrics(result):
    before = result.get("pre_copy_metrics", {})
    after = result.get("end_copy_metrics", {})
    deltas = {}
    for field in ("source_cpu_usage_usec", "source_io_rbytes", "source_io_wbytes"):
        if field in before and field in after and after[field] >= before[field]:
            deltas[field] = after[field] - before[field]
    result["source_counter_delta"] = deltas
    windows = [*result["samples"], after]
    peaks = {}
    for field in ("snapshot_session_transaction_ms", "longest_transaction_ms", "backend_xmin_age_xids",
                  "dead_tuples_estimate", "retained_slot_wal_bytes"):
        values = [sample[field] for sample in windows if sample.get(field) is not None]
        peaks[field] = max(values, default=None)
    result["sampled_peaks"] = peaks


def sanitized_status(binary):
    status = command([binary, "status", "--json"])
    if not status.stdout:
        return {}
    data = json.loads(status.stdout)["data"]["backfill"]
    if data.get("progress"):
        data["progress"].pop("run_id", None)
    sample = data.get("source_impact_last_sample") or {}
    for waiter in sample.get("lock_waiters", []):
        waiter.pop("pid", None)
    return data


def measure(packet, arm, binary, container, rows):
    if rows < 2:
        raise ValueError("E_ARTICLE3_FIXTURE_ROWS")
    stop = threading.Event()
    begin = threading.Event()
    latencies, commits, errors = [], [], []
    consumers = [
        threading.Thread(target=reader, args=(stop, latencies, errors), daemon=True),
        threading.Thread(target=writer, args=(stop, begin, commits, errors, rows), daemon=True),
    ]
    for consumer in consumers:
        consumer.start()
    process = ddl = None
    process_output = Path("connector.log")
    result = {"arm": arm, "samples": [], "result": "fail"}
    start = end = time.monotonic_ns()
    try:
        time.sleep(5)
        if errors or not latencies:
            raise RuntimeError("E_ARTICLE3_WORKLOAD_WARMUP")
        result["pre_copy_metrics"] = metrics(container, time.monotonic_ns())
        start = time.monotonic_ns()
        if arm == "naive":
            with Path("naive.csv").open("wb") as stream:
                process = subprocess.Popen(
                    PSQL + ["-c", "COPY (SELECT * FROM public.orders) TO STDOUT WITH (FORMAT CSV)"],
                    stdout=stream, stderr=subprocess.DEVNULL,
                    env=dict(app_env(), PGOPTIONS="-c statement_timeout=900000", PGAPPNAME="article3-naive"),
                )
        else:
            with process_output.open("wb") as log:
                process = subprocess.Popen([binary, "run", "--bootstrap"], stdout=log, stderr=subprocess.STDOUT)
        begin.set()
        next_sample = 0
        ddl_start = None
        while elapsed_ms(start) < 1000 * MAX_SECONDS:
            now = elapsed_ms(start)
            if errors:
                raise RuntimeError(errors[0])
            if arm == "naive" and process.poll() is not None:
                end = time.monotonic_ns()
                if process.returncode != 0:
                    raise RuntimeError("E_ARTICLE3_NAIVE_COMMAND")
                result["copied_bytes"] = Path("naive.csv").stat().st_size
                with Path("naive.csv").open("rb") as csv:
                    result["copied_rows"] = sum(1 for _ in csv)
                if result["copied_rows"] < rows:
                    raise RuntimeError("E_ARTICLE3_NAIVE_INCOMPLETE")
                break
            if arm == "backfill":
                copy = copy_state()
                if copy and copy["complete_anchor"] is not None:
                    end = time.monotonic_ns()
                    result["copy_state"] = copy
                    if copy["complete_anchor"][0] < rows or len(commits) < 3:
                        raise RuntimeError("E_ARTICLE3_BACKFILL_INCOMPLETE")
                    if rows == FULL_ROWS and (end - start) / 1_000_000 < FULL_BACKFILL_MIN_MS:
                        raise RuntimeError("E_ARTICLE3_BACKFILL_BELOW_WORKLOAD_DURATION")
                    break
            if arm == "ddl" and ddl is None:
                copy = copy_state()
                if copy and 0 < copy["complete_chunks"] < copy["total_chunks"]:
                    ddl_start = time.monotonic_ns()
                    ddl = subprocess.Popen(
                        PSQL + ["-c", "ALTER TABLE public.orders ADD COLUMN note text"],
                        env=dict(os.environ, PGOPTIONS="-c lock_timeout=15000"),
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    )
            if ddl is not None and ddl.poll() is not None and "ddl_duration_ms" not in result:
                result["ddl_duration_ms"] = elapsed_ms(ddl_start)
                result["ddl_exit_code"] = ddl.returncode
            if arm == "ddl" and "ddl_exit_code" in result and process.poll() is not None:
                end = time.monotonic_ns()
                result["connector_exit_code"] = process.returncode
                status = sanitized_status(binary)
                result["backfill"] = status
                wait_seen = any(s["table_lock_waiters"] for s in result["samples"])
                if (result["ddl_exit_code"] != 0 or result["ddl_duration_ms"] < DDL_BOUND_MS
                    or not wait_seen or process.returncode == 0
                    or status.get("failure_code") != "M3_DDL_WAITER"
                    or status.get("guard_liveness") != "released"
                    or status.get("generation_state") != "invalidated"):
                    raise RuntimeError("E_ARTICLE3_DDL_OUTCOME")
                break
            if process.poll() is not None and (arm != "ddl" or ddl is None):
                result["connector_exit_code"] = process.returncode
                raise RuntimeError("E_ARTICLE3_CONNECTOR_EXIT")
            if now >= next_sample:
                result["samples"].append(metrics(container, start))
                next_sample += SAMPLE_INTERVAL * 1000
                while next_sample < elapsed_ms(start):
                    next_sample += SAMPLE_INTERVAL * 1000
            time.sleep(0.05)
        else:
            raise RuntimeError("E_ARTICLE3_IMPACT_TIMEOUT")
        result["end_copy_metrics"] = metrics(container, start)
        if arm == "backfill" and not any(s["slot_active"] for s in result["samples"]):
            raise RuntimeError("E_ARTICLE3_IMPACT_NO_SLOT_SAMPLE")
        result["result"] = "pass"
    except (OSError, ValueError, RuntimeError, KeyError, sqlite3.Error, subprocess.SubprocessError) as error:
        if isinstance(error, RuntimeError):
            result["failure_code"] = str(error)
        else:
            result["failure_code"] = "E_ARTICLE3_IMPACT_" + type(error).__name__.upper()
        end = time.monotonic_ns()
    finally:
        stop.set()
        begin.set()
        for proc in (ddl, process):
            if proc is not None and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
        for consumer in consumers:
            consumer.join(timeout=20)
        result["duration_ms"] = (end - start) / 1_000_000
        result["workload_commits_during_copy"] = sum(start <= when <= end for when in commits)
        curves(latencies, start, end, result)
        summarize_metrics(result)
        if process_output.is_file():
            result["connector_codes"] = sorted(set(re.findall(
                r"\b(?:M[0-9]|BCDC)_[A-Z0-9]+(?:_[A-Z0-9]+)+\b", process_output.read_text(errors="replace"),
            )))
        save(packet / (arm + ".json"), result)
        print(f"{arm}: {result['result']} ({result.get('failure_code', 'measured')})", flush=True)
    return 0 if result["result"] == "pass" else 1


def finalize(packet, stage, exit_code, rows):
    observations = {}
    for arm in ("backfill", "naive", "ddl"):
        path = packet / (arm + ".json")
        if path.is_file():
            result = json.loads(path.read_text())
            observations[arm] = {key: value for key, value in result.items()
                                 if key not in ("samples", "reader_samples")}
    save(packet / "observation.json", {
        "scenario": "article3-source-impact",
        "stage": stage,
        "exit_code": exit_code,
        "result": "pass" if stage == "complete" and exit_code == 0 else "fail",
        "fixture": {"source_rows": rows, "chunk_rows": 50, "writer_interval_s": WRITER_INTERVAL,
                    "reader_interval_s": READER_INTERVAL, "metrics_interval_s": SAMPLE_INTERVAL,
                    "ddl_waiter_bound_ms": DDL_BOUND_MS, "postgres_version": "17.6",
                    "trial_kind": "full" if rows == FULL_ROWS else "diagnostic",
                    "required_full_backfill_duration_ms": FULL_BACKFILL_MIN_MS,
                    "copy_arm_scope": "CLI start to durable initial anchor",
                    "naive_arm_scope": "stream SELECT * with PostgreSQL COPY TO STDOUT, no replication slot",
                    "latency_scope": "persistent psql request/response for indexed key lookup",
                    "cpu_io_scope": "PostgreSQL container cumulative cgroup v2 counters"},
        "arms": observations,
    })
    save(packet / "manifest.json", {
        "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "ci_run_id": os.getenv("GITHUB_RUN_ID"),
        "ci_run_attempt": os.getenv("GITHUB_RUN_ATTEMPT"),
        "command": ["scripts/e2e/article3_source_impact.sh"],
        "files": {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                  for path in sorted(packet.iterdir()) if path.is_file() and path.name != "manifest.json"},
    })


if __name__ == "__main__":
    operation, packet_arg, *args = sys.argv[1:]
    packet = Path(packet_arg)
    if operation == "measure":
        arm, binary, container, rows = args
        sys.exit(measure(packet, arm, binary, container, int(rows)))
    stage, exit_code, rows = args
    finalize(packet, stage, int(exit_code), int(rows))
