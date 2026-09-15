#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR=/var/tmp
cargo test --locked --workspace --all-targets
cargo test --locked m2_fault_status::tests
scripts/e2e/m2_fault_status.sh
scripts/faults/m2_fault_status.sh
scripts/validate/m2_fault_status.py
scripts/validate/runbook_registry.sh contracts/runbooks/index.json
printf 'M2_ACCEPTANCE_OK durable_before_feedback=true\n'
