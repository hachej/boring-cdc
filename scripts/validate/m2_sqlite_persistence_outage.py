#!/usr/bin/env python3
"""Validate two live SQLite persistence-outage and successor observations."""
import hashlib
import json
import pathlib
import re
import sys

out = pathlib.Path(sys.argv[1])


def lsn(value):
    assert re.fullmatch(r"[0-9A-F]+/[0-9A-F]+", value)
    high, low = value.split("/")
    return (int(high, 16) << 32) | int(low, 16)


for number in (1, 2):
    observed = json.loads((out / f"attempt-{number}" / "observation.json").read_text())
    assert observed["schema_version"] == "m2-sqlite-persistence-outage-observation/v1"
    assert observed["postgres_version"].startswith("17.6")
    durable = observed["durable_lsn_before_outage"]
    successor = observed["successor_durable_lsn"]
    assert 0 < lsn(durable) < lsn(successor)
    assert observed["feedback_before_outage"] == ",".join([durable] * 3)
    assert observed["original_replication_pid"] > 0
    assert observed["runtime_exit_code"] != 0
    assert observed["runtime_failure_code"] == "M2_FAILURE_PERSIST_FAILED"
    assert observed["armed_failure_count_before_release"] == 0
    for field in (
        "slot_confirmed_before_outage",
        "slot_confirmed_while_locked",
        "slot_confirmed_after_exit_before_release",
    ):
        assert lsn(observed[field]) <= lsn(durable)
    assert lsn(observed["slot_confirmed_before_outage"]) <= lsn(observed["slot_confirmed_while_locked"])
    assert lsn(observed["slot_confirmed_while_locked"]) <= lsn(observed["slot_confirmed_after_exit_before_release"])
    assert observed["durable_transaction_count_before_release"] == 1
    assert observed["replication_slot_active_after_exit"] is False
    assert observed["successor_durable_transaction_count"] == 2
    assert observed["successor_feedback"] == ",".join([successor] * 3)
    assert observed["successor_clean_shutdown"] is True
manifest = json.loads((out / "evidence.json").read_text())
assert manifest["scenario_id"] == "SCN-M2-CAPTURE-CRASH-BEFORE-COMMIT"
assert manifest["result"]["status"] == "pass"
files = sorted(path for path in out.rglob("*") if path.is_file() and path.name != "sha256.txt")
actual = [f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(out).as_posix()}" for path in files]
assert (out / "sha256.txt").read_text().splitlines() == actual
for path in files:
    raw = path.read_bytes().lower()
    assert b"postgresql:" + b"//" not in raw and b"password" + b"=" not in raw
print(json.dumps({"status": "pass", "scenario_id": manifest["scenario_id"]}, sort_keys=True))
