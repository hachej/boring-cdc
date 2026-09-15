#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp
cargo test --locked m2_pressure::tests::pin_lifecycle_and_gc_preserve_whole_transactions
cargo test --locked m2_pressure::tests::maintenance_is_bounded_and_never_full_vacuum
python3 scripts/lib/m2_pressure_component.py fault
scratch=$(mktemp -d /var/tmp/m2-pressure-fault.XXXXXX); trap 'rm -rf "$scratch"' EXIT INT TERM
cp -a artifacts/boring-cdc-m2-pressure/SCN-M2-PRESSURE-READER-CONTENTION/. "$scratch"/
python3 scripts/lib/m2_pressure_component.py fault
diff -ru "$scratch" artifacts/boring-cdc-m2-pressure/SCN-M2-PRESSURE-READER-CONTENTION
python3 scripts/validate/m2_pressure.py fault
