#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp
cargo test --locked m2_reconcile::tests
cargo test --locked m2_reconcile::tests
python3 scripts/validate/m2_reconcile.py
