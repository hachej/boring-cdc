#!/usr/bin/env python3
"""Check two retained live CopyBoth-session-loss receipts."""
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
    observation = json.loads((out / f"attempt-{number}" / "observation.json").read_text())
    assert observation["schema_version"] == "m2-copyboth-loss-observation/v1"
    assert observation["postgres_version"].startswith("17.6")
    durable = observation["durable_lsn_before_loss"]
    assert lsn(durable) > 0
    assert observation["feedback_before_loss"] == ",".join([durable] * 3)
    assert lsn(observation["slot_confirmed_flush_after_loss"]) <= lsn(durable)
    assert observation["source_advisory_pid_before_loss"] > 0
    assert observation["terminated_replication_pid"] > 0
    assert observation["terminated_replication_pid"] != observation["source_advisory_pid_before_loss"]
    assert observation["runtime_exit_code"] != 0
    assert observation["failure_class_after_loss"] == "transient_source"
    assert observation["durable_transaction_count_after_loss"] == 1
    assert observation["replication_slot_active_after_loss"] is False
    assert observation["replication_reopened"] is False
    assert observation["runtime_exited_without_operator_signal"] is True
manifest = json.loads((out / "evidence.json").read_text())
assert manifest["scenario_id"] == "SCN-M2-CAPTURE-COPYBOTH-LOSS"
assert manifest["result"]["status"] == "pass"
files = sorted(path for path in out.rglob("*") if path.is_file() and path.name != "sha256.txt")
actual = [f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(out).as_posix()}" for path in files]
assert (out / "sha256.txt").read_text().splitlines() == actual
for path in files:
    raw = path.read_bytes().lower()
    assert b"postgresql:" + b"//" not in raw and b"password" + b"=" not in raw
print(json.dumps({"status": "pass", "scenario_id": manifest["scenario_id"]}, sort_keys=True))
