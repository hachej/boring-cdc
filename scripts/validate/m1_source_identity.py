#!/usr/bin/env python3
"""Validate the complete M1 source-identity leaf scenario inventory."""
import json
import subprocess
from pathlib import Path

EXPECTED = {
    "SCN-M1-SOURCE-IDENTITY-MISMATCH": ("every_identity_mismatch_blocks_before_position_rules", "block_identity_mismatch_before_position_rules"),
    "SCN-M1-SOURCE-IDENTITY-UNSUPPORTED-PLUGIN": ("matching_unsupported_plugin_fails_closed_before_startup_decisions", "matching_unsupported_plugin_blocks_before_resume"),
    "SCN-M1-SOURCE-IDENTITY-AMBIGUOUS": ("ambiguous_bootstrap_precedes_server_ahead", "bootstrap_ambiguous_requires_restart"),
    "SCN-M1-SOURCE-IDENTITY-RETRY-CREATION": ("prepared_bootstrap_without_remote_slot_retries_creation", "retry_prepared_bootstrap_slot_creation"),
    "SCN-M1-SOURCE-IDENTITY-FLOOR": ("creation_floor_null_and_equal_are_not_durable_progress", "request_verified_creation_floor_without_durable_progress"),
    "SCN-M1-SOURCE-IDENTITY-FLOOR-SAFETY": ("creation_floor_still_requires_valid_slot_and_available_wal", "requires_reseed_if_slot_invalid_or_resume_wal_unavailable"),
    "SCN-M1-SOURCE-IDENTITY-SERVER-AHEAD": ("server_ahead_requires_reseed_before_generic_slot_checks", "requires_reseed_server_ahead"),
    "SCN-M1-SOURCE-IDENTITY-SLOT-WAL": ("invalid_slot_and_missing_wal_require_reseed", "requires_reseed_if_slot_invalid_or_resume_wal_unavailable"),
    "SCN-M1-SOURCE-IDENTITY-LOCAL-AHEAD": ("local_ahead_requests_durable_end_and_expects_duplicates", "request_durable_end_and_tolerate_duplicates"),
    "SCN-M1-SOURCE-IDENTITY-COMPATIBLE": ("compatible_positions_apply_postgresql_effective_max_rule", "request_durable_end_and_apply_effective_max"),
    "SCN-M1-SOURCE-IDENTITY-FEEDBACK": ("feedback_uses_only_durable_transaction_end_for_all_three_positions", "write_flush_apply_equal_durable_transaction_end"),
    "SCN-M1-SOURCE-IDENTITY-COMMIT-END": ("commit_and_end_lsn_roles_remain_distinct", "source_version_uses_commit_and_durable_boundary_uses_end"),
    "SCN-M1-SOURCE-IDENTITY-EPOCH": ("source_versions_never_compare_across_capture_epochs", "different_capture_epoch_not_ordered"),
    "SCN-M1-SOURCE-IDENTITY-ROW": ("table_schema_and_row_identities_are_distinct_dimensions", "canonical_table_schema_and_physical_key_dimensions"),
    "SCN-M1-SOURCE-IDENTITY-CONSTRUCTION": ("invalid_local_progress_relationships_fail_closed", "invalid_progress_relationships_block"),
    "SCN-M1-SOURCE-IDENTITY-LOCK": ("identity_validation_and_lock_derivation_are_canonical", "canonical_lock_and_protocol_fingerprints"),
}

inventory = json.loads(Path("contracts/m1/source-identity-cases.json").read_text())
assert inventory["schema_version"] == "boring-cdc/source-identity-cases/v1"
assert inventory["owner_bead"] == "boring-cdc-m1-source-identity"
assert inventory["evidence_tier"] == "leaf"
assert inventory["persistence_owners"] == ["boring-cdc-m2-schema", "boring-cdc-m2-reconcile"]
actual = {
    row["scenario_id"]: (row["unit_test"], row["expected"])
    for row in inventory["cases"]
}
assert actual == EXPECTED
source = Path("src/m1_source_identity.rs").read_text()
for test, _ in EXPECTED.values():
    assert f"fn {test}()" in source, f"missing unit test: {test}"
    subprocess.run(
        [
            "cargo",
            "test",
            "--locked",
            "--quiet",
            f"m1_source_identity::tests::{test}",
            "--",
            "--exact",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )
lower = source.lower()
assert "rusqlite" not in lower and "create table" not in lower and "sqlite::" not in lower
print(f"PASS source identity vectors={len(EXPECTED)} exact=true sqlite=false")
