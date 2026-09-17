#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
cargo test --locked failure_policy::tests::deterministic_schedule_caps_jitters_and_ignores_clock_rollback
cargo test --locked failure_policy::tests::stale_epoch_generation_attempt_and_fingerprint_completions_cannot_clear
cargo test --locked failure_policy::tests::non_transient_rearm_is_persistable_and_stale_operations_are_rejected
cargo test --locked failure_policy::tests::shared_bounded_harness_replays_policy_vectors_deterministically
cargo test --locked failure_policy::tests::prepared_adapter_persists_reopens_and_clears_via_supplied_transaction
cargo test --locked failure_policy::tests::fingerprint_changes_only_for_allowed_canonical_inputs_and_contains_no_raw_data
cargo test --locked failure_policy::tests::complete_policy_vector_inventory_uses_bounded_harness_schedules
cargo test --locked failure_policy::tests::persistence_cas_projections_execute_inside_bounded_harness
