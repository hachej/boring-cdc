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
assert len(ids) == len(set(ids)) == 10
assert all(value.startswith("SCN-M1-ORDERING-") for value in ids)
for case in cases["cases"]:
    assert f"fn {case['unit_test']}" in source, case
assert "M0-PROVISIONAL: boring-cdc-d-event-id" in source
assert "M0-PROVISIONAL: boring-cdc-d-keys" not in source
for provisional_literal in (
    "fn canonical_length_bytes(len: usize) -> [u8; 8]",
    "COLUMN_ABSENT_TAG: u8 = 0",
    "COLUMN_NULL_TAG: u8 = 1",
    "COLUMN_UNCHANGED_TOAST_TAG: u8 = 2",
    "COLUMN_VALUE_TAG: u8 = 3",
    "MUTATION_DELETE_TAG: u8 = 0",
    "MUTATION_UPSERT_TAG: u8 = 1",
    "MAX_CANONICAL_KEY_COMPONENTS: usize = 8",
):
    assert provisional_literal in source
for golden in (
    "06004c4a0b18bedd87c4fabd9102bbaf009e50ffecfea740528edeb68485d548",
    "e4a6350855eb0705318a27f6822976320cc869cff47753af9324600165c046a9",
    "4317afc16b609fcbf9d0133dc604a3250d0b18425f37226a9dd16320e4bba187",
    "ddf09f7280c10ad15ffb79c78ce4678a5a99d27a890655098046394736f39adb",
    "fc98f5b0520965efe632181840282500e412a5bab5e9ca2c2a63645e6abf4f4d",
):
    assert golden in source
wal_body = source[source.index("pub fn wal_connector_event_id"):source.index("pub struct SnapshotIdentityInput")]
for excluded in ("journal_seq", "run_id", "cache_timing", "timestamp"):
    assert excluded not in wal_body
for required in (
    "before_checkpoint_and_feedback", "DifferentCaptureEpoch", "CAPTURE_EPOCH_MISMATCH",
    "ROW_ORDINAL_OUT_OF_RANGE", "SourceSlotIdentity::derive", "LogicalTableIdentity",
    "SNAPSHOT_PAYLOAD_IDENTITY_MISMATCH", "KEY_CHANGE_IDENTITIES_EQUAL",
    "SNAPSHOT_ORDINAL_INVALID", "MAX_CANONICAL_KEY_COMPONENTS + 1",
):
    assert required in source
subprocess.run(
    ["cargo", "test", "--locked", "m1_ordering::tests"], cwd=root, check=True,
    stdout=subprocess.DEVNULL,
)
print("m1 ordering contract: PASS (10 cases, 12 semantic tests, 5 fixed goldens)")
