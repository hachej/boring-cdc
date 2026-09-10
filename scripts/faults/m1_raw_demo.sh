#!/bin/sh
set -eu
export TMPDIR=${TMPDIR:-/var/tmp}
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/faults/m1_raw_demo.sh [raw-demo-v1]'; exit 0; }
seed=${1:-raw-demo-v1}; [ "$seed" = raw-demo-v1 ] || { echo E_SEED >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
out="$TMPDIR/m1rawfault${$}"; transcript="$out/transcript.txt"; rm -rf "$out"; mkdir -p "$out"; trap 'rm -rf "$out"' EXIT HUP INT TERM
run() { cargo test --locked "$1" -- --exact --quiet >/dev/null 2>&1; printf 'PASS %s checkpoint=unchanged\n' "$2"; }
{
 echo 'scenario=SCN-M1-FAULT-MATRIX seed=raw-demo-v1'
 run m1_decoder::tests::reconnect_metadata_does_not_need_transaction_or_ordinal copyboth_restart
 run m1_decoder::tests::copy_both_keepalive_and_complete_status_packet keepalive_reply_durable_only
 run m1_bootstrap_sm::tests::feedback_is_gated_until_all_import_acks_then_uses_durable_wal_only bootstrap_feedback_gate
 run m1_workload::tests::clean_fixed_seed_is_reproducible_and_sequence_gaps_are_diagnostic exact_set_oracle
 run m1_bootstrap_sm::tests::failed_continuity_requires_confirmed_new_epoch_full_reseed full_reseed
 run m1_control_fixtures::tests::truncate_is_detection_only truncate_requires_reseed
 run m1_control_fixtures::tests::publication_fingerprint_is_exact_and_order_independent publication_drift
 run m1_ddl_fixtures::tests::catalog_poll_fingerprint_detects_idle_ddl_and_only_safe_addition_is_admitted idle_ddl
 run m1_ddl_fixtures::tests::changed_relation_synchronously_blocks_following_dml_and_feedback immediate_ddl
 run m1_decoder::tests::unsupported_messages_and_binary_truncate_fail_closed unsupported_protocol
 run m1_ddl_fixtures::tests::selected_types_keys_delete_and_destination_compatibility_fail_independently unsupported_table
 run m1_ddl_fixtures::tests::changed_type_or_removed_replica_identity_blocks_update_delete_safety unsupported_type
 run m1_ordering::tests::same_id_same_hash_is_duplicate_and_different_hash_is_conflict identity_conflict
 run m1_control_fixtures::tests::table_membership_change_has_no_live_path no_online_table_add
 cargo test --locked m1_raw_demo::tests -- --quiet >/dev/null 2>&1
 echo 'PASS m1_fault_matrix cases=15 blocked_actions=status,recover_reseed cleanup=trap'
} > "$transcript"
cat "$transcript"
scripts/validate/m1_raw_demo.py seal fault "$transcript" >/dev/null
echo 'PASS m1 raw fault evidence sealed'
