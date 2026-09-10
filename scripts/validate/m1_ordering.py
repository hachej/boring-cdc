#!/usr/bin/env python3
import json
import subprocess
from pathlib import Path

root = Path(__file__).resolve().parents[2]
cases = json.loads((root / "contracts/m1/ordering-cases.json").read_text())
source = (root / "src/m1_ordering.rs").read_text()
assert cases["owner_bead"] == "boring-cdc-m1-ordering"
assert cases["evidence_tier"] == "leaf"
ids = [case["scenario_id"] for case in cases["cases"]]
assert len(ids) == len(set(ids)) == 9
assert all(value.startswith("SCN-M1-ORDERING-") for value in ids)
for case in cases["cases"]:
    assert f"fn {case['unit_test']}" in source, case
for marker in ("boring-cdc-d-event-id", "boring-cdc-d-keys"):
    assert f"M0-PROVISIONAL: {marker}" in source
for golden in (
    "06004c4a0b18bedd87c4fabd9102bbaf009e50ffecfea740528edeb68485d548",
    "5a0574e19de8c33923e12953a6c002e16c06dc13c46aa82b8e249f77ffb83fe3",
    "000162f8dd1a1f04d4ec4e8f6357c5202e25db4f0993de3069591621cbc56be3",
    "ddf09f7280c10ad15ffb79c78ce4678a5a99d27a890655098046394736f39adb",
    "69977ac3ca1be0868cf1f6dc35ecaef3cf68e8eace61967a1ad0e24eaf8e0b00",
):
    assert golden in source
wal_body = source[source.index("pub fn wal_connector_event_id"):source.index("pub struct SnapshotIdentityInput")]
for excluded in ("journal_seq", "run_id", "cache_timing", "timestamp"):
    assert excluded not in wal_body
for required in (
    "before_checkpoint_and_feedback", "DifferentCaptureEpoch", "CAPTURE_EPOCH_MISMATCH",
    "ROW_ORDINAL_OUT_OF_RANGE", "SourceSlotIdentity::derive", "LogicalTableIdentity",
):
    assert required in source
subprocess.run(
    ["cargo", "test", "--locked", "m1_ordering::tests"], cwd=root, check=True,
    stdout=subprocess.DEVNULL,
)
print("m1 ordering contract: PASS (9 cases, 10 semantic tests, 5 fixed goldens)")
