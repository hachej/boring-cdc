#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
cargo test --locked m2_pressure::tests
python3 scripts/lib/m2_pressure_component.py e2e
python3 scripts/validate/m2_pressure.py e2e
