#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
cargo test --locked m2_schema::tests::failed_migration_rolls_back_atomically_and_upgrade_is_idempotent
cargo test --locked m2_schema::tests::immutable_rows_fences_and_anchor_proof_fail_closed
