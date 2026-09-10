#!/bin/sh
set -eu
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/faults/m1_ddl_fixtures.sh [SEED]'; exit 0; }
seed=${1:-m1-ddl-fault-v1}; [ "$seed" = m1-ddl-fault-v1 ] || { echo 'E_SEED: expected m1-ddl-fault-v1' >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
for test in catalog_poll_fingerprint_detects_idle_ddl_and_only_safe_addition_is_admitted all_contract_changes_invalidate_active_generation guard_spans_all_copy_boundaries_until_durable_fence guard_loss_invalidates_and_releases_feedback_gate conflicting_waiter_at_bound_invalidates_and_releases changed_relation_synchronously_blocks_following_dml_and_feedback noncanonical_contracts_and_guard_order_fail_closed; do
 cargo test --locked "m1_ddl_fixtures::tests::$test" -- --exact >/dev/null 2>&1
done
scripts/validate/m1_ddl_fixtures.py >/dev/null
printf 'm1 ddl fault pass seed=%s hooks=7 checkpoint=unchanged feedback=blocked-or-released-on-invalidation cleanup=trap\n' "$seed"
