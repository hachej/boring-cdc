#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
export TMPDIR="${TMPDIR:-/var/tmp}"
[ "$TMPDIR" != /tmp ] || { echo "TMPDIR=/tmp is forbidden" >&2; exit 2; }
python3 scripts/validate/m2_journal.py
cargo test --locked m2_journal::tests::atomic_commit_publishes_complete_transaction_and_durable_end
cargo test --locked m2_journal::tests::feedback_token_exists_only_after_commit_return
cargo test --locked m2_journal::tests::bounded_range_copies_complete_transactions_and_releases_reader
cargo test --locked m2_journal::tests::capture_priority_reserves_bounded_service_and_overload
scratch=$(mktemp -d /var/tmp/boring-cdc-m2-journal-e2e.XXXXXX)
trap 'rm -rf "$scratch"' EXIT INT TERM
python3 scripts/lib/m2_journal_component.py e2e
cp -a artifacts/boring-cdc-m2-journal/SCN-M2-JOURNAL-COMPONENT/. "$scratch"/
python3 scripts/lib/m2_journal_component.py e2e
diff -ru "$scratch" artifacts/boring-cdc-m2-journal/SCN-M2-JOURNAL-COMPONENT
