#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR=/var/tmp; ulimit -c 0
work=$(mktemp -d /var/tmp/m2-fault-hooks.XXXXXX); trap 'rm -rf "$work"' EXIT INT TERM
expect_test_abort(){ local hook=$1 test=$2; set +e; BORING_CDC_M2_FAULT_HOOK="$hook" cargo test --quiet --locked "$test" -- --exact >"$work/$hook.out" 2>"$work/$hook.err"; local rc=$?; set -e; [[ $rc -eq 101 ]]; grep -q 'SIGABRT' "$work/$hook.err"; }
expect_live_abort(){ local hook=$1; set +e; BORING_CDC_M2_FAULT_HOOK="$hook" timeout 240 scripts/e2e/m2_capture_runtime.sh >"$work/$hook.out" 2>"$work/$hook.err"; local rc=$?; set -e; [[ $rc -ne 0 ]]; grep -q 'Aborted' "$work/$hook.err"; }
expect_test_abort before_source_commit m2_journal::tests::atomic_commit_publishes_complete_transaction_and_durable_end
expect_test_abort after_source_commit_before_feedback m2_journal::tests::atomic_commit_publishes_complete_transaction_and_durable_end
expect_live_abort before_feedback
expect_live_abort after_feedback
expect_test_abort bootstrap_intent_durable m2_reconcile::tests::ambiguous_bootstrap_is_transitioned_and_persisted
expect_test_abort spool_created m2_spool::tests::incremental_overflow_has_one_iterator_and_no_whole_transaction_collection
expect_test_abort spool_synced m2_spool::tests::incremental_overflow_has_one_iterator_and_no_whole_transaction_collection
for hook in archive_intent_durable archive_file_synced archive_directory_synced checkpoint_before_commit checkpoint_after_commit; do expect_test_abort "$hook" m2_jsonl::tests::deterministic_commit_and_exact_range_retry_are_byte_identical; done
expect_test_abort lease_fenced m2_leases::tests::same_generation_identity_cycle_permanently_fences_old_worker
expect_test_abort promotion_before_selector m2_leases::tests::promotion_intent_allocates_increasing_fence_and_stale_token_cannot_prepare
expect_test_abort promotion_after_selector m2_leases::tests::promotion_after_selector_hook_crosses_live_dispatch_boundary
expect_test_abort ownership_lost m2_capture_runtime::tests::ownership_lost_hook_crosses_runtime_boundary
# Clean same-code and pinned PG17 recovery rerun after all abrupt processes.
cargo test --quiet --locked m2_fault_status::tests
scripts/e2e/m2_fault_status.sh
printf 'M2_FAULT_STATUS_FAULTS_OK rerun=true product_boundaries_aborted=16\n'
