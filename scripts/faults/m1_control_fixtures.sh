#!/bin/sh
set -eu
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/faults/m1_control_fixtures.sh [SEED]'; exit 0; }
seed=${1:-m1-control-fault-v1}; [ "$seed" = m1-control-fault-v1 ] || { echo 'E_SEED: expected m1-control-fault-v1' >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
for test in \
 publication_fingerprint_is_exact_and_order_independent \
 heartbeat_cannot_feedback_before_durable_commit \
 zero_or_multiple_control_rows_block forbidden_control_shapes_block \
 unbound_fence_fails_closed truncate_is_detection_only \
 table_membership_change_has_no_live_path slot_guard_accepts_only_bound_configured_export \
 administration_credential_is_gone_before_exporter source_timeline_publication_slot_mismatch_blocks_startup; do
 cargo test --locked "m1_control_fixtures::tests::$test" -- --exact >/dev/null
done
printf 'm1 control fault pass seed=%s hooks=10 checkpoint=unchanged cleanup=trap\n' "$seed"
