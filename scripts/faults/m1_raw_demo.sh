#!/bin/sh
set -eu
export TMPDIR=${TMPDIR:-/var/tmp}
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/faults/m1_raw_demo.sh [raw-demo-v1]'; exit 0; }
seed=${1:-raw-demo-v1}; [ "$seed" = raw-demo-v1 ] || { echo E_SEED >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
out="$TMPDIR/m1rawfault${$}"; transcript="$out/transcript.txt"; rm -rf "$out"; mkdir -p "$out"; trap 'rm -rf "$out"' EXIT HUP INT TERM
run() {
 test_log="$out/test.log"
 cargo test --locked "$1" -- --exact --quiet --nocapture >"$test_log" 2>"$out/test.err"
 observed=$(grep -E "^CASE $2 state=[^ ]+ checkpoint=[^ ]+ log=[^ ]+$" "$test_log")
 [ "$(printf '%s\n' "$observed" | grep -c .)" -eq 1 ]
 printf 'ASSERT %s test=%s exit=0 product_observation=true\n' "$2" "$1"
 printf '%s\n' "$observed"
}
{
 echo 'scenario=SCN-M1-FAULT-MATRIX seed=raw-demo-v1'
 run m1_decoder::tests::golden_transaction_preserves_row_only_ordinals_and_origin SCN-M1-RAW-FIXED-SEED
 run m1_decoder::tests::reconnect_metadata_does_not_need_transaction_or_ordinal SCN-M1-RAW-COPYBOTH-RESTART
 run m1_control_fixtures::tests::heartbeat_is_monotonic_durable_noop SCN-M1-RAW-HEARTBEAT
 run m1_bootstrap_sm::tests::feedback_is_gated_until_all_import_acks_then_uses_durable_wal_only SCN-M1-RAW-BOOTSTRAP
 run m1_raw_demo::tests::fixed_seed_exact_set_oracle_smoke_passes SCN-M1-RAW-EXACT-SET
 run m1_bootstrap_sm::tests::failed_continuity_requires_confirmed_new_epoch_full_reseed SCN-M1-RAW-FULL-RESEED
 run m1_control_fixtures::tests::truncate_is_detection_only SCN-M1-RAW-TRUNCATE
 run m1_control_fixtures::tests::publication_fingerprint_is_exact_and_order_independent SCN-M1-RAW-PUBLICATION-DRIFT
 run m1_ddl_fixtures::tests::every_relation_contract_dimension_changes_the_fingerprint SCN-M1-RAW-IDLE-DDL
 run m1_ddl_fixtures::tests::changed_relation_synchronously_blocks_following_dml_and_feedback SCN-M1-RAW-IMMEDIATE-DDL
 run m1_decoder::tests::unsupported_messages_and_binary_truncate_fail_closed SCN-M1-RAW-UNSUPPORTED-PROTOCOL
 run m1_ddl_fixtures::tests::selected_types_keys_delete_and_destination_compatibility_fail_independently SCN-M1-RAW-UNSUPPORTED-TABLE
 run m1_ddl_fixtures::tests::changed_type_or_removed_replica_identity_blocks_update_delete_safety SCN-M1-RAW-UNSUPPORTED-TYPE
 run m1_ordering::tests::same_id_same_hash_is_duplicate_and_different_hash_is_conflict SCN-M1-RAW-IDENTITY-CONFLICT
 run m1_control_fixtures::tests::table_membership_change_has_no_live_path SCN-M1-RAW-NO-ONLINE-TABLE-ADD
 cargo test --locked m1_raw_demo::tests -- --quiet >/dev/null 2>&1
 echo 'PASS m1_fault_matrix cases=15 blocked_actions=status,recover_reseed cleanup=trap'
} > "$transcript"
cat "$transcript"
scripts/validate/m1_raw_demo.py seal fault "$transcript" >/dev/null
echo 'PASS m1 raw fault evidence sealed'
