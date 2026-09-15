#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR="${TMPDIR:-/var/tmp}"; [[ "$TMPDIR" == /var/tmp ]]
# Product functions execute real SQLite crash boundaries; the PostgreSQL boundary is exercised by the paired e2e suite.
cargo test --locked m2_capture_runtime::tests::crash_boundaries_never_create_a_feedback_token -- --exact
cargo test --locked m2_capture_runtime::tests::deterministic_safe_stop_shutdown_and_no_in_process_reopen -- --exact
cargo test --locked m2_ownership::tests::two_state_paths_same_source_fail_closed -- --exact
python3 scripts/lib/m2_capture_runtime_evidence.py fault
scripts/validate/evidence.sh artifacts/boring-cdc-m2-capture-runtime/SCN-M2-CAPTURE-RUNTIME-FAULTS/capture-runtime-component-v1/evidence.json
echo 'M2_CAPTURE_RUNTIME_FAULTS_OK pre_commit=no_feedback post_commit=reconciled ownership=fenced'
