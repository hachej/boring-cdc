#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR=/var/tmp
# The e2e scenario includes an injected SQLite source-receipt abort after migrations and proves
# retry from durable table state against PostgreSQL 17.6.
scripts/e2e/m2_reconcile.sh
cargo test --locked m2_reconcile::tests::creation_floor_compound_safety_precedes_floor_resume
cargo test --locked m2_reconcile::tests::fresh_journal_with_preexisting_slot_is_ambiguous_without_provenance
cargo test --locked m2_reconcile::tests::invalid_slot_and_missing_wal_require_reseed
python3 scripts/validate/m2_reconcile.py
