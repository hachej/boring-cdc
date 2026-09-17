#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp
cargo test --locked m2_pressure::tests
python3 scripts/lib/m2_pressure_component.py e2e
scratch=$(mktemp -d /var/tmp/m2-pressure-e2e.XXXXXX); trap 'rm -rf "$scratch"' EXIT INT TERM
cp -a artifacts/boring-cdc-m2-pressure/SCN-M2-PRESSURE-COMPONENT/. "$scratch"/
python3 scripts/lib/m2_pressure_component.py e2e
diff -ru "$scratch" artifacts/boring-cdc-m2-pressure/SCN-M2-PRESSURE-COMPONENT
python3 scripts/validate/m2_pressure.py e2e
