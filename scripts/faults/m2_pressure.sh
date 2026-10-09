#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
cargo test --locked m2_pressure::tests::pin_lifecycle_and_gc_preserve_whole_transactions
cargo test --locked m2_pressure::tests::maintenance_is_bounded_and_never_full_vacuum
python3 scripts/lib/m2_pressure_component.py fault
python3 scripts/validate/m2_pressure.py fault
