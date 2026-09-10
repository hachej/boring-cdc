#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
export TMPDIR="${TMPDIR:-/var/tmp}"
[ "$TMPDIR" = /var/tmp ] || { echo "TMPDIR must be /var/tmp" >&2; exit 2; }
cargo test --locked m2_spool::tests
scratch=$(mktemp -d /var/tmp/boring-cdc-m2-spool-e2e.XXXXXX)
trap 'rm -rf "$scratch"' EXIT INT TERM
export BORING_CDC_WORKSPACE_TEST_STDOUT="$scratch/workspace-tests-stdout.txt"
export BORING_CDC_WORKSPACE_TEST_STDERR="$scratch/workspace-tests-stderr.txt"
if cargo test --locked --workspace --all-targets >"$BORING_CDC_WORKSPACE_TEST_STDOUT" 2>"$BORING_CDC_WORKSPACE_TEST_STDERR"; then export BORING_CDC_WORKSPACE_TEST_EXIT_CODE=0; else code=$?; cat "$BORING_CDC_WORKSPACE_TEST_STDOUT"; cat "$BORING_CDC_WORKSPACE_TEST_STDERR" >&2; exit "$code"; fi
python3 scripts/lib/m2_spool_component.py e2e
cp -a artifacts/boring-cdc-m2-spool/SCN-M2-SPOOL-COMPONENT "$scratch/expected"
python3 scripts/lib/m2_spool_component.py e2e
diff -ru "$scratch/expected" artifacts/boring-cdc-m2-spool/SCN-M2-SPOOL-COMPONENT
python3 scripts/validate/m2_spool.py e2e
scripts/validate/evidence.sh artifacts/boring-cdc-m2-spool/SCN-M2-SPOOL-COMPONENT
