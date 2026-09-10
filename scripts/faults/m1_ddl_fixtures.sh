#!/bin/sh
set -eu
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/faults/m1_ddl_fixtures.sh [SEED]'; exit 0; }
seed=${1:-m1-ddl-fault-v1}; [ "$seed" = m1-ddl-fault-v1 ] || { echo 'E_SEED: expected m1-ddl-fault-v1' >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
faults=$(mktemp)
trap 'rm -f "$faults"' EXIT HUP INT TERM
cat >"$faults" <<'EOF'
catalog_poll_fingerprint_detects_idle_ddl_and_only_safe_addition_is_admitted|catalog_poll_fingerprint|RELATION_CONTRACT_CHANGED
all_contract_changes_invalidate_active_generation|active_generation_change|ACTIVE_GENERATION_SCHEMA_DRIFT
guard_spans_all_copy_boundaries_until_durable_fence|guard_lifecycle|DDL_GUARD_CATALOG_FINGERPRINT_MISMATCH
guard_loss_invalidates_and_releases_feedback_gate|guard_session_lost|DDL_GUARD_SESSION_LOST
conflicting_waiter_at_bound_invalidates_and_releases|ddl_waiter_bound|BACKFILL_DDL_WAITER
changed_relation_synchronously_blocks_following_dml_and_feedback|changed_relation_dml|DML_BEFORE_RELATION_VALIDATION
noncanonical_contracts_and_guard_order_fail_closed|noncanonical_contract|RELATION_CONTRACT_NON_CANONICAL
every_relation_contract_dimension_changes_the_fingerprint|full_fingerprint|RELATION_CONTRACT_CHANGED
changed_type_or_removed_replica_identity_blocks_update_delete_safety|key_delete_safety|RELATION_CONTRACT_CHANGED
stale_or_nonmatching_durable_fence_cannot_release_newer_guard|stale_durable_fence|DDL_GUARD_FENCE_MISMATCH
malformed_catalog_validation_and_actual_decoder_row_remain_fail_closed|decoded_relation_admission|RELATION_CONTRACT_NON_CANONICAL
EOF
while IFS='|' read -r test hook fingerprint; do
 cargo test --locked "m1_ddl_fixtures::tests::$test" -- --exact >/dev/null 2>&1
 printf 'fault hook=%s fingerprint=%s outcome=pass_fail_closed\n' "$hook" "$fingerprint"
done <"$faults"
# This test derives all 11 fingerprints from actual return values and compares the timeline.
cargo test --locked "m1_ddl_fixtures::tests::fault_timeline_matches_failures_exercised_from_code" -- --exact >/dev/null 2>&1
scripts/validate/m1_ddl_fixtures.py >/dev/null
printf 'm1 ddl fault pass seed=%s hooks=11 checkpoint=unchanged feedback=blocked-or-released-on-invalidation cleanup=trap\n' "$seed"
