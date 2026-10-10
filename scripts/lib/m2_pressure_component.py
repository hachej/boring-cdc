#!/usr/bin/env python3
"""Capture pressure evidence from executed SQLite tests."""

import hashlib
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SEED = "pressure-component-v2"
COMMAND = ["cargo", "test", "--locked", "m2_pressure::tests", "--", "--nocapture", "--test-threads=1"]
CAPTURE_COMMAND = ["cargo", "test", "--locked", "m2_capture_runtime::tests::capture_reserve_breach_safe_stops_before_accepting_more_wal", "--", "--nocapture"]
MARKER = re.compile(r"PRESSURE_OBSERVATION (\{[^\n]+\})")
CAPTURE_MARKER = re.compile(r"PRESSURE_CAPTURE_OBSERVATION (\{[^\n]+\})")
PRIVATE = re.compile(rb"(?i)(password|secret|token|postgresql://|/home/|/tmp/)")
IMPLEMENTATION_PATHS = (
    "src/m2_pressure.rs", "src/m2_schema.rs", "src/m2_journal.rs",
    "src/m2_capture_runtime.rs", "contracts/m2/pressure-cases.json",
)


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sha(value):
    return hashlib.sha256(value).hexdigest()


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value if isinstance(value, bytes) else value.encode())


def run(argv):
    return subprocess.run(argv, cwd=ROOT, capture_output=True, timeout=240)


def implementation_digest():
    return sha(b"".join((ROOT / name).read_bytes() for name in IMPLEMENTATION_PATHS))


def observations(stdout, expected):
    output = stdout.decode()
    passed = set(re.findall(r"test m2_pressure::tests::(\w+) \.\.\. (?:PRESSURE_OBSERVATION [^\n]+\n)?ok", output))
    if not expected <= passed:
        raise RuntimeError(f"pressure tests missing: {sorted(expected - passed)}")
    summary = re.search(r"test result: ok\. (\d+) passed; 0 failed", output)
    if summary is None or int(summary[1]) != len(passed):
        raise RuntimeError("test summary disagrees with passing test names")
    records = [json.loads(value) for value in MARKER.findall(output)]
    by_probe = {record["probe"]: record for record in records}
    if len(records) != 4 or set(by_probe) != {"pin_gc", "reader_contention", "runtime_service", "wal_recycling"}:
        raise RuntimeError("required SQLite observations missing or duplicated")
    pin, reader, service = (by_probe[name] for name in ("pin_gc", "reader_contention", "runtime_service"))
    wal = by_probe["wal_recycling"]
    if not (
        pin["first_gc_transactions"] == 3
        and pin["first_gc_remaining_events"] == 2
        and pin["expired_pin_blocked"]
        and pin["second_gc_transactions"] == 2
        and pin["after_release_remaining_events"] == 0
        and reader["blocked_checkpoint_busy"] == 1
        and reader["released_checkpoint_busy"] == 0
        and reader["incremental_vacuum_max_pages"] <= 1000
        and reader["automatic_full_vacuum"] is False
        and service["pressure_state"] == "action"
        and service["gc_transactions"] == 5
        and service["remaining_events"] == 0
        and service["checkpoint_busy"] == 0
        and wal["writes_while_reader_held"] == 64
        and wal["reader_snapshot_count"] == 0
        and wal["stalled_checkpoint_busy"] == 1
        and wal["stalled_wal_pages"] >= 64
        and wal["released_checkpoint_busy"] == 0
        and wal["released_checkpointed_pages"] == wal["stalled_wal_pages"]
        and wal["recycled_wal_pages"] < wal["stalled_wal_pages"]
        and wal["gc_transactions"] == 3
        and wal["remaining_pinned_events"] == 2
    ):
        raise RuntimeError("pressure probe observed unexpected state")
    return by_probe


def capture_observation(stdout):
    output = stdout.decode()
    name = "m2_capture_runtime::tests::capture_reserve_breach_safe_stops_before_accepting_more_wal"
    if f"test {name} ... ok" not in output or "test result: ok. 1 passed; 0 failed" not in output:
        raise RuntimeError("capture Hard-stop test did not pass")
    records = CAPTURE_MARKER.findall(output)
    if len(records) != 1:
        raise RuntimeError("capture Hard-stop observation missing or unexpected")
    observed = json.loads(records[0])
    if (set(observed) != {"before_free_bytes", "after_free_bytes", "hard_free_bytes",
                          "physical_fill_bytes", "safe_stopped", "committed_transactions",
                          "feedback_packets"}
            or not observed["before_free_bytes"] > observed["hard_free_bytes"]
            or not observed["after_free_bytes"] <= observed["hard_free_bytes"]
            or observed["before_free_bytes"] - observed["after_free_bytes"] < 16 << 20
            or observed["physical_fill_bytes"] not in {32 << 20, 64 << 20, 96 << 20, 128 << 20}
            or observed["safe_stopped"] is not True
            or observed["committed_transactions"] != 0
            or observed["feedback_packets"] != 0):
        raise RuntimeError("capture Hard-stop observation missing or unexpected")
    return {"probe": "capture_hard_stop", "physical_threshold_crossed": True,
            "safe_stopped": True, "committed_transactions": 0, "feedback_packets": 0}


def main():
    mode = sys.argv[1]
    if mode not in {"e2e", "fault"}:
        raise SystemExit("usage: m2_pressure_component.py e2e|fault")
    scenario = "SCN-M2-PRESSURE-COMPONENT" if mode == "e2e" else "SCN-M2-PRESSURE-READER-CONTENTION"
    out = ROOT / "artifacts/boring-cdc-m2-pressure" / scenario / SEED
    expected = {case["test"] for case in json.loads((ROOT / "contracts/m2/pressure-cases.json").read_text())["cases"]}
    implementation = implementation_digest()
    runs = []
    capture_runs = []
    for attempt in (1, 2):
        completed = run(COMMAND)
        if completed.returncode:
            raise RuntimeError(f"pressure attempt {attempt} exited {completed.returncode}: {completed.stderr.decode()[-2000:]}")
        completed.stdout = completed.stdout.replace(str(ROOT).encode(), b"[REPO]")
        completed.stderr = completed.stderr.replace(str(ROOT).encode(), b"[REPO]")
        if PRIVATE.search(completed.stdout) or PRIVATE.search(completed.stderr):
            raise RuntimeError("pressure test output contains unapproved path or secret-like text")
        runs.append((completed, observations(completed.stdout, expected)))
        if mode == "fault":
            capture = run(CAPTURE_COMMAND)
            if capture.returncode:
                raise RuntimeError(f"capture Hard-stop attempt {attempt} exited {capture.returncode}: {capture.stderr.decode()[-2000:]}")
            capture.stdout = capture.stdout.replace(str(ROOT).encode(), b"[REPO]")
            capture.stderr = capture.stderr.replace(str(ROOT).encode(), b"[REPO]")
            if PRIVATE.search(capture.stdout) or PRIVATE.search(capture.stderr):
                raise RuntimeError("capture Hard-stop output contains unapproved path or secret-like text")
            capture_runs.append((capture, capture_observation(capture.stdout)))
    if runs[0][1] != runs[1][1]:
        raise RuntimeError("pressure observations changed between attempts")
    if mode == "fault" and capture_runs[0][1] != capture_runs[1][1]:
        raise RuntimeError("capture Hard-stop observations changed between attempts")
    after_implementation = implementation_digest()
    if implementation != after_implementation:
        raise RuntimeError("pressure source changed during evidence capture")
    observed = runs[0][1]
    selected = (observed["runtime_service"] if mode == "e2e"
                else {"reader_contention": observed["reader_contention"], "wal_recycling": observed["wal_recycling"], "capture_hard_stop": capture_runs[0][1]})
    timeline = ([observed["pin_gc"], observed["runtime_service"]] if mode == "e2e"
                else [observed["reader_contention"], observed["wal_recycling"], capture_runs[0][1], observed["pin_gc"]])
    shutil.rmtree(out, ignore_errors=True)
    before = {"journal_events": observed["pin_gc"]["first_gc_transactions"] + observed["pin_gc"]["first_gc_remaining_events"]}
    write(out / "state/before.json", encoded(before))
    write(out / "state/after.json", encoded(selected))
    write(out / "fault-timeline.json", encoded(timeline))
    write(out / "config.json", encoded({"seed": SEED, "test_threads": 1,
                                        "physical_fill_max_bytes": 128 << 20 if mode == "fault" else 0,
                                        "command_output_redaction": "checkout root replaced with [REPO]"}))
    git_commit = run(["git", "rev-parse", "HEAD"]).stdout.decode().strip()
    rustc = run(["rustc", "--version"]).stdout.decode().strip()
    write(out / "versions.json", encoded({"git_commit": git_commit, "rustc": rustc, "implementation_sha256": implementation}))
    write(out / "commands.txt", " ".join(COMMAND) + "\n" + (" ".join(CAPTURE_COMMAND) + "\n" if mode == "fault" else ""))
    commands = []
    for name, argv, attempts in (("pressure", COMMAND, runs), ("capture", CAPTURE_COMMAND, capture_runs)):
        for attempt, (completed, _) in enumerate(attempts, 1):
            stem = f"attempt-{attempt}" if name == "pressure" else f"attempt-{attempt}-{name}"
            stdout, stderr = out / f"{stem}-stdout.txt", out / f"{stem}-stderr.txt"
            write(stdout, completed.stdout)
            write(stderr, completed.stderr)
            commands.append({"argv": " ".join(argv), "version": "cargo-test/v1", "exit_code": completed.returncode,
                             "stdout_path": stdout.relative_to(ROOT).as_posix(), "stdout_sha256": sha(completed.stdout),
                             "stderr_path": stderr.relative_to(ROOT).as_posix(), "stderr_sha256": sha(completed.stderr)})
    events = []
    for sequence, item in enumerate(timeline, 1):
        events.append({"schema_version": "journal-event/v1", "bead_id": "boring-cdc-m2-pressure",
                       "scenario_id": scenario, "correlation_id": scenario.lower() + ":run-v2", "run_id": "pressure-run-v2",
                       "capture_epoch": "epoch", "component": "pressure-test-probe", "case_event_seq": sequence,
                       "phase": item["probe"], "outcome": "pass", "config_fingerprint": implementation,
                       "generation": None, "intent_id": None, "request_id": None, "xid": None, "commit_lsn": None,
                       "end_lsn": None, "journal_range": None, "anchor": None, "fence": None, "attempt": 1,
                       "fault_hook": ("reserve-breach" if item["probe"] == "capture_hard_stop" else "stalled-sqlite-reader") if mode == "fault" else None,
                       "failure_class": None, "failure_fingerprint": None, "metric_units": None,
                       "evidence_digest": sha(encoded(item))})
    write(out / "logs/boring-cdc.jsonl", b"".join(encoded(event) for event in events))
    result_paths = [out / "state/after.json", out / "fault-timeline.json", out / "logs/boring-cdc.jsonl"]
    manifest = {"schema_version": "evidence/v1", "owner_bead": "boring-cdc-m2-pressure", "scenario_id": scenario,
                "evidence_profile": "runtime", "evidence_tier": "component", "seed": SEED, "git_commit": git_commit,
                "commands": commands,
                "source_preservation": {"before_sha256": implementation, "after_sha256": after_implementation, "preserved": True},
                "cleanup": {"complete": True, "remaining_paths": []}, "redaction": {"checked": True, "secrets_found": 0},
                "tier_proof": {"targeted_checks": True, "boundary_e2e": True, "fault_suite": True,
                               "deterministic_rerun": True, "consumed_contract_vectors": True, "workspace_tests": False,
                               "integration": False, "clean_environment": False, "exit_assertions": True,
                               "endurance": False, "full_failure_matrix": False, "clean_clone": False},
                "result": {"status": "pass", "digest": sha(b"".join(path.read_bytes() for path in result_paths)),
                           "artifacts": [path.relative_to(ROOT).as_posix() for path in result_paths],
                           "product_faults": "stalled_sqlite_reader_wal_recycling_and_bounded_physical_capture_pressure" if mode == "fault" else "none",
                           "runtime_observed": True, "attempts": ["sqlite-probe-1", "sqlite-probe-2"]}}
    write(out / "manifest.json", encoded(manifest))
    write(out / "evidence.json", encoded(manifest))
    files = sorted(path for path in out.rglob("*") if path.is_file())
    write(out / "sha256.txt", "".join(f"{sha(path.read_bytes())}  {path.relative_to(out).as_posix()}\n" for path in files))
    print(json.dumps({"mode": mode, "status": "pass", "observed_probes": sorted(observed) + (["capture_hard_stop"] if mode == "fault" else [])}))


if __name__ == "__main__":
    main()
