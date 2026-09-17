#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
python3 scripts/validate/m2_schema.py
cargo test --locked m2_schema::tests
