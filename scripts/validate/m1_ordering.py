#!/usr/bin/env python3
import json
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
for excluded in ("journal_seq", "run_id", "cache_timing", "timestamp"):
    wal_body = source[source.index("pub fn wal_connector_event_id"):source.index("pub struct SnapshotIdentityInput")]
    assert excluded not in wal_body
assert "before_checkpoint_and_feedback" in source
assert "DifferentCaptureEpoch" in source
print("m1 ordering contract: PASS (9 cases, provisional literals marked)")
