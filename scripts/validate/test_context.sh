#!/usr/bin/env bash
set -euo pipefail
[[ ${1:-m0-context-v1} == m0-context-v1 ]] || { echo 'seed must be m0-context-v1' >&2; exit 64; }
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT"
python3 -m unittest tests.test_context
scripts/validate/plan_coverage.sh
