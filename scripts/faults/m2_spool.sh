#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
export TMPDIR="${TMPDIR:-/var/tmp}"
[ "$TMPDIR" = /var/tmp ] || { echo "TMPDIR must be /var/tmp" >&2; exit 2; }
cargo test --locked m2_spool::tests::filesystem_reserve_rejects_before_write_and_preserves_emergency_space
cargo test --locked m2_spool::tests::checksum_failure_is_detected_by_commit_iterator
cargo test --locked m2_spool::tests::startup_removes_owned_dead_spools_and_quarantines_malformed_or_contradictory
scratch=$(mktemp -d /var/tmp/boring-cdc-m2-spool-fault.XXXXXX)
trap 'rm -rf "$scratch"' EXIT INT TERM
python3 scripts/lib/m2_spool_component.py fault
cp -a artifacts/boring-cdc-m2-spool/SCN-M2-SPOOL-FAULTS "$scratch/expected"
python3 scripts/lib/m2_spool_component.py fault
diff -ru "$scratch/expected" artifacts/boring-cdc-m2-spool/SCN-M2-SPOOL-FAULTS
python3 scripts/validate/m2_spool.py fault
