#!/usr/bin/env python3
"""Validate and execute every M1 bootstrap state-machine leaf vector."""
import json
import re
import subprocess
from pathlib import Path

inventory = json.loads(Path("contracts/m1/bootstrap-sm-cases.json").read_text())
EXPECTED_OUTCOMES = {'SCN-M1-BOOTSTRAP-ACK-REORDER-STALE': ('ExporterReleasePermitted', 'unchanged', 'stale_ack_rejected'),
 'SCN-M1-BOOTSTRAP-ANCHOR-FENCE': ('AnchorComplete', 'fence=end:150,seq:3', 'matching_fence_durable'),
 'SCN-M1-BOOTSTRAP-BOUNDARY-MONOTONIC': ('ImportsPending', 'durable=end:120,seq:2', 'DURABLE_WAL_REGRESSION'),
 'SCN-M1-BOOTSTRAP-BOUNDS-CANCEL': ('SnapshotUnusable', 'unchanged', 'bounded_session_or_cancel'),
 'SCN-M1-BOOTSTRAP-CAPACITY-ACCOUNTING': ('Prepared', 'unchanged', 'owned_capacity_accounted'),
 'SCN-M1-BOOTSTRAP-CONTINUITY-CAPABILITY': ('FullReseedRequired', 'continuity_unproven', 'identity_or_intent_mismatch'),
 'SCN-M1-BOOTSTRAP-EXPORTER-LOSS-EACH-ACK': ('SnapshotUnusable',
                                             'feedback_unchanged',
                                             'EXPORTER_LOST_BEFORE_IMPORT_ACKS'),
 'SCN-M1-BOOTSTRAP-EXPORTER-RELEASE': ('ExporterReleased', 'unchanged', 'exporter_release_legitimate'),
 'SCN-M1-BOOTSTRAP-FEEDBACK-GATE': ('ExporterReleasePermitted', 'feedback=durable_end:120', 'feedback_gate_released'),
 'SCN-M1-BOOTSTRAP-FEEDBACK-NOT-DECISION': ('ExistingSlotGenerationRequired',
                                            'feedback_position_ignored',
                                            'no_drop_or_reseed_from_feedback'),
 'SCN-M1-BOOTSTRAP-FENCE-CAPABILITY': ('AnchorComplete', 'fence=end:160,seq:4', 'guard_nonce_and_boundary_bound'),
 'SCN-M1-BOOTSTRAP-FIRST-STATEMENT': ('ImportsPending', 'unchanged', 'SNAPSHOT_IMPORT_NOT_FIRST_STATEMENT'),
 'SCN-M1-BOOTSTRAP-FLOOR-CAPTURE': ('ImportsPending', 'start_seq=0;durable_wal=null', 'capture_connection_separate'),
 'SCN-M1-BOOTSTRAP-FULL-RESEED': ('FullReseedConfirmed', 'new_epoch=8;continuity_break=true', 'confirmed_full_reseed'),
 'SCN-M1-BOOTSTRAP-GUARD-LIFECYCLE': ('SnapshotUnusable', 'feedback_gate_released', 'DDL_GUARD_LOST_BEFORE_FENCE'),
 'SCN-M1-BOOTSTRAP-HARNESS-REPLAY': ('SnapshotUnusable', 'unchanged', 'deterministic_seed_45063'),
 'SCN-M1-BOOTSTRAP-IMPORTER-LOSS': ('SnapshotUnusable',
                                    'feedback_gate_released',
                                    'IMPORTER_LOST_BEFORE_ASSIGNED_READS_COMPLETE'),
 'SCN-M1-BOOTSTRAP-INTENT-GUARD-SLOT': ('Prepared', 'unchanged', 'PERMANENT_SLOT_CREATION_NOT_AUTHORIZED'),
 'SCN-M1-BOOTSTRAP-LOST-TOKEN-RECOVERY': ('ImportsPending(existing-slot generation=2)',
                                          'lower_stitch=end:120,seq:2',
                                          'lost_token_retained_capture_recovered'),
 'SCN-M1-BOOTSTRAP-ORIGIN-ORDER': ('not_applicable', 'snapshot_rank=0;wal_rank=1', 'wal_wins_equal_position'),
 'SCN-M1-BOOTSTRAP-RESTART-RESPONSE-WINDOW': ('ExistingSlotGenerationRequired',
                                              'feedback_unchanged',
                                              'bootstrap_ambiguous_then_retained_slot'),
 'SCN-M1-BOOTSTRAP-RETAINED-SLOT-TRANSIENT': ('ImportsPending(existing-slot generation=2)',
                                              'lower_stitch=end:120,seq:2',
                                              'insert_delete_retained'),
 'SCN-M1-BOOTSTRAP-STALE-COMPLETION': ('AnchorComplete', 'fence=end:150,seq:3', 'stale_scope_rejected')}
assert inventory["schema_version"] == "boring-cdc/bootstrap-sm-cases/v1"
assert inventory["owner_bead"] == "boring-cdc-m1-bootstrap-sm"
assert inventory["evidence_tier"] == "leaf"
assert inventory["runtime_owner"] == "boring-cdc-m3-bootstrap"
assert inventory["persistence_owners"] == ["boring-cdc-m2-schema", "boring-cdc-m2-reconcile"]
assert inventory["seed"] == 0xB007
cases = inventory["cases"]
ids = [row["scenario_id"] for row in cases]
tests = [row["unit_test"] for row in cases]
actual_outcomes = {row["scenario_id"]: (row["expected_state"], row["expected_checkpoint"], row["expected_log_outcome"]) for row in cases}
assert actual_outcomes == EXPECTED_OUTCOMES
assert all("asserted_by" not in value and "placeholder" not in value for outcome in actual_outcomes.values() for value in outcome)
assert all(row["expected_state"] and row["expected_checkpoint"] and row["expected_log_outcome"] for row in cases)
assert len(cases) == 23 and len(ids) == len(set(ids)) and len(tests) == len(set(tests))
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
assert source.count("M0-RECONCILED:") == 4
print(f"PASS bootstrap vectors={len(cases)} exact=true live_runtime=false m0_provisional=4")
