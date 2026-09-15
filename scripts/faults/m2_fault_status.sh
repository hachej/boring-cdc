#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR=/var/tmp
# A second clean isolated run is the deterministic rerun required by the milestone contract.
scripts/e2e/m2_fault_status.sh
TMPDIR=/var/tmp cargo test --quiet --locked m2_fault_status::tests
printf 'M2_FAULT_STATUS_FAULTS_OK rerun=true hooks=16\n'
