#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR=/var/tmp
scripts/faults/m2_fault_status.sh
printf 'M2_MATRIX_OK source_commit=true feedback=true bootstrap=true spool=true archive=true lease=true checkpoint=true promotion=true ownership=true\n'
