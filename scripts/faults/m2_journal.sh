#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
export TMPDIR="${TMPDIR:-/var/tmp}"
[ "$TMPDIR" != /tmp ] || { echo "TMPDIR=/tmp is forbidden" >&2; exit 2; }
cargo test --locked m2_journal::tests::crash_before_and_after_commit_is_absent_or_complete
cargo test --locked m2_journal::tests::positional_duplicate_is_idempotent_but_conflict_blocks
cargo test --locked m2_journal::tests::relation_schema_conflict_rolls_back_whole_source_commit
cargo test --locked m2_journal::tests::slow_storage_hold_bound_rolls_back_without_partial_visibility
cargo test --locked m2_journal::tests::saturated_real_writer_service_bounds_slow_capture_and_reserved_work
scratch=$(mktemp -d /var/tmp/boring-cdc-m2-journal-fault.XXXXXX)
trap 'rm -rf "$scratch"' EXIT INT TERM
python3 scripts/lib/m2_journal_component.py fault
cp -a artifacts/boring-cdc-m2-journal/SCN-M2-JOURNAL-CRASH-BOUNDARY/. "$scratch"/
python3 scripts/lib/m2_journal_component.py fault
diff -ru "$scratch" artifacts/boring-cdc-m2-journal/SCN-M2-JOURNAL-CRASH-BOUNDARY
python3 scripts/validate/m2_journal.py
