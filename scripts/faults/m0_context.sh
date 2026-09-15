#!/usr/bin/env bash
set -euo pipefail
[[ ${1:-m0-context-v1} == m0-context-v1 ]] || exit 64
ROOT=$(cd "$(dirname "$0")/../.." && pwd); cd "$ROOT"
python3 -m unittest tests.test_context.ContextTests.test_hostile_duplicate_dangling_unknown_and_changed_source tests.test_context.ContextTests.test_source_change_reports_exact_stale_owner_without_rewriting_closed tests.test_context.ContextTests.test_generated_view_drift_rejected >/dev/null 2>&1
echo "hostile context corpus: 3 passed; seed=m0-context-v1; product_faults=fault_not_applicable"
