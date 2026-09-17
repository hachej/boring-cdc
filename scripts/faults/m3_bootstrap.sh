#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../..";export TMPDIR=/var/tmp
cargo test --locked m3_bootstrap::tests
scripts/e2e/m3_bootstrap.sh
for test in invalidation_atomically_releases_feedback_gate lost_response_never_recreates_or_drops_slot ambiguous_continuity_requires_full_reseed;do cargo test --locked "m3_bootstrap::tests::$test";done
python3 scripts/lib/m3_bootstrap_evidence.py faults
scripts/validate/evidence.sh artifacts/boring-cdc-m3-bootstrap/SCN-M3-BOOTSTRAP-FAULTS/bootstrap-pg17-v1/evidence.json
