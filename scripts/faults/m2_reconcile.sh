#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp
cargo test --locked m2_reconcile::tests::server_ahead_requires_reseed
cargo test --locked m2_reconcile::tests::creation_floor_compound_safety_precedes_floor_resume
cargo test --locked m2_reconcile::tests::ambiguous_bootstrap_is_transitioned_and_persisted
python3 scripts/validate/m2_reconcile.py
