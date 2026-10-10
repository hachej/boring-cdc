#!/usr/bin/env python3
"""Check two retained live source-commit crash and recovery receipts."""
import hashlib
import json
import pathlib
import re
import sys

out = pathlib.Path(sys.argv[1])
hook = sys.argv[2]
scenarios = {
    "before_source_commit": ("SCN-M2-CAPTURE-CRASH-BEFORE-COMMIT", 0),
    "after_source_commit_before_feedback": ("SCN-M2-CAPTURE-CRASH-AFTER-COMMIT", 1),
}
scenario, expected_count = scenarios[hook]
def lsn(value):
    assert re.fullmatch(r"[0-9A-F]+/[0-9A-F]+", value)
    high, low = value.split("/")
    return (int(high, 16) << 32) | int(low, 16)

for number in (1, 2):
    observation = json.loads((out / f"attempt-{number}" / "observation.json").read_text())
    assert observation["schema_version"] == "m2-source-commit-observation/v1"
    assert observation["postgres_version"].startswith("17.6")
    assert observation["fault_hook"] == hook
    assert observation["fault_exit_code"] == 134
    assert observation["durable_transaction_count_after_fault"] == expected_count
    assert observation["slot_confirmed_after_fault"] == observation["slot_confirmed_before_fault"]
    assert lsn(observation["slot_confirmed_after_fault"]) >= 0
    assert observation["durable_transaction_count_after_successor"] == 1
    durable = observation["successor_durable_lsn"]
    assert lsn(durable) > 0
    assert observation["successor_feedback"] == ",".join([durable] * 3)
    assert observation["successor_clean_shutdown"] is True
manifest = json.loads((out / "evidence.json").read_text())
assert manifest["scenario_id"] == scenario
assert manifest["result"]["status"] == "pass"
files = sorted(path for path in out.rglob("*") if path.is_file() and path.name != "sha256.txt")
actual = [f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(out).as_posix()}" for path in files]
assert (out / "sha256.txt").read_text().splitlines() == actual
for path in files:
    raw = path.read_bytes().lower()
    assert b"postgresql:" + b"//" not in raw and b"password" + b"=" not in raw
print(json.dumps({"status": "pass", "scenario_id": manifest["scenario_id"]}, sort_keys=True))
