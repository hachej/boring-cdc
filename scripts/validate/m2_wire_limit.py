#!/usr/bin/env python3
"""Check a retained live CopyData header-limit receipt."""
import hashlib
import json
import pathlib
import re
import sys

out = pathlib.Path(sys.argv[1])
for number in (1, 2):
    observation = json.loads((out / f"attempt-{number}" / "observation.json").read_text())
    assert observation["schema_version"] == "m2-wire-limit-observation/v1"
    assert observation["postgres_version"].startswith("17.6")
    assert re.fullmatch(r"[0-9A-F]+/[0-9A-F]+", observation["durable_lsn_before_limit"])
    assert observation["feedback_before_limit"] == ",".join([observation["durable_lsn_before_limit"]] * 3)
    assert observation["armed_failure"] == "configuration,deterministic"
    assert observation["durable_transaction_count_after_limit"] == 1
    assert observation["replication_slot_active_after_limit"] is False
    assert observation["advisory_lock_held_after_limit"] is True
    assert observation["process_alive_after_limit"] is True
    assert observation["clean_shutdown"] is True
manifest = json.loads((out / "evidence.json").read_text())
assert manifest["scenario_id"] == "SCN-M2-CAPTURE-OVERSIZED"
assert manifest["result"]["status"] == "pass"
files = sorted(path for path in out.rglob("*") if path.is_file() and path.name != "sha256.txt")
actual = [f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(out).as_posix()}" for path in files]
assert (out / "sha256.txt").read_text().splitlines() == actual
for path in files:
    raw = path.read_bytes().lower()
    assert b"postgresql://" not in raw and b"password=" not in raw
print(json.dumps({"status": "pass", "scenario_id": manifest["scenario_id"]}, sort_keys=True))
