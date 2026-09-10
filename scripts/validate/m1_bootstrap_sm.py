#!/usr/bin/env python3
"""Validate and execute every M1 bootstrap state-machine leaf vector."""
import json
import re
import subprocess
from pathlib import Path

inventory = json.loads(Path("contracts/m1/bootstrap-sm-cases.json").read_text())
assert inventory["schema_version"] == "boring-cdc/bootstrap-sm-cases/v1"
assert inventory["owner_bead"] == "boring-cdc-m1-bootstrap-sm"
assert inventory["evidence_tier"] == "leaf"
assert inventory["runtime_owner"] == "boring-cdc-m3-bootstrap"
assert inventory["persistence_owners"] == ["boring-cdc-m2-schema", "boring-cdc-m2-reconcile"]
assert inventory["seed"] == 0xB007
cases = inventory["cases"]
ids = [row["scenario_id"] for row in cases]
tests = [row["unit_test"] for row in cases]
assert len(cases) == 17 and len(ids) == len(set(ids)) and len(tests) == len(set(tests))
assert all(re.fullmatch(r"SCN-M1-BOOTSTRAP-[A-Z0-9-]+", value) for value in ids)
source = Path("src/m1_bootstrap_sm.rs").read_text()
for test in tests:
    assert f"fn {test}()" in source, f"missing test {test}"
    subprocess.run(
        ["cargo", "test", "--locked", "--quiet", f"m1_bootstrap_sm::tests::{test}", "--", "--exact"],
        check=True,
        stdout=subprocess.DEVNULL,
    )
required_states = {
    "Prepared", "SnapshotExported", "ImportsPending", "ImportsComplete",
    "ExporterReleasePermitted", "ExporterReleased", "SnapshotUnusable",
    "BootstrapAmbiguousRequiresRestart", "ExistingSlotGenerationRequired",
    "RetainedWalDrained", "FencePending", "AnchorComplete", "FullReseedRequired",
    "FullReseedConfirmed",
}
assert required_states <= set(re.findall(r"^    ([A-Z][A-Za-z]+),$", source, re.MULTILINE))
for prohibited in ("rusqlite", "CREATE_REPLICATION_SLOT", "DROP_REPLICATION_SLOT", "START_REPLICATION"):
    assert prohibited not in source
assert source.count("M0-PROVISIONAL:") == 4
print(f"PASS bootstrap vectors={len(cases)} exact=true live_runtime=false m0_provisional=4")
