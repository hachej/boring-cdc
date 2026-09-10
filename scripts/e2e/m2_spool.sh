#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
export TMPDIR="${TMPDIR:-/var/tmp}"
[ "$TMPDIR" = /var/tmp ] || { echo "TMPDIR must be /var/tmp" >&2; exit 2; }
cargo test --locked m2_spool::tests
scratch=$(mktemp -d /var/tmp/boring-cdc-m2-spool-e2e.XXXXXX)
trap 'rm -rf "$scratch"' EXIT INT TERM
python3 scripts/lib/m2_spool_component.py e2e
cp -a artifacts/boring-cdc-m2-spool/SCN-M2-SPOOL-COMPONENT "$scratch/expected"
python3 scripts/lib/m2_spool_component.py e2e
diff -ru "$scratch/expected" artifacts/boring-cdc-m2-spool/SCN-M2-SPOOL-COMPONENT
python3 scripts/validate/m2_spool.py e2e
