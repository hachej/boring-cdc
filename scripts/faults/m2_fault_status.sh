#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR=/var/tmp
hooks='before_source_commit after_source_commit_before_feedback before_feedback after_feedback bootstrap_intent_durable spool_created spool_synced archive_intent_durable archive_file_synced archive_directory_synced lease_fenced checkpoint_before_commit checkpoint_after_commit promotion_before_selector promotion_after_selector ownership_lost'
work=$(mktemp -d /var/tmp/m2-fault-hooks.XXXXXX); trap 'rm -rf "$work"' EXIT INT TERM
for hook in $hooks; do
  set +e
  BORING_CDC_M2_FAULT_HOOK="$hook" cargo run --quiet --locked --example m2_fault_hook_probe -- "$hook" >"$work/$hook.out" 2>"$work/$hook.err"
  rc=$?
  set -e
  [[ $rc -eq 134 ]]
done
# Same-code paths execute normally with hooks disarmed; each test crosses the wired boundaries.
cargo test --quiet --locked m2_journal::tests::atomic_commit_publishes_complete_transaction_and_durable_end
cargo test --quiet --locked m2_spool::tests::incremental_overflow_has_one_iterator_and_no_whole_transaction_collection
cargo test --quiet --locked m2_jsonl::tests::deterministic_commit_and_exact_range_retry_are_byte_identical
cargo test --quiet --locked m2_leases::tests::promotion_intent_allocates_increasing_fence_and_stale_token_cannot_prepare
cargo test --quiet --locked m2_reconcile::tests::ambiguous_bootstrap_is_transitioned_and_persisted
# Re-run the pinned PostgreSQL exact-process crash/status integration.
scripts/e2e/m2_fault_status.sh
printf 'M2_FAULT_STATUS_FAULTS_OK rerun=true hooks_executed=16\n'
