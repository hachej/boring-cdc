#!/bin/sh
set -eu
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/faults/m0_validation_core.sh [SEED]'; exit 0; }
seed=${1:-m0-core-v1}; [ "$seed" = m0-core-v1 ] || { echo 'E_SEED: expected m0-core-v1' >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
expect() { code=$1; shift; tmp=$(mktemp); if "$@" >"$tmp" 2>/dev/null; then echo "expected failure $code" >&2; rm -f "$tmp"; exit 1; fi; grep -q '"code":"'"$code"'"' "$tmp" || { cat "$tmp" >&2; rm -f "$tmp"; exit 1; }; rm -f "$tmp"; }
expect E_DUPLICATE_ID scripts/validate/m0_decisions.sh tests/fixtures/m0-core/invalid/decisions-duplicate.json
expect E_PATH_TRAVERSAL scripts/validate/m0_artifact.sh tests/fixtures/m0-core/invalid/artifact-traversal.json
expect E_PROCEDURE_GAP scripts/validate/runbook_registry.sh tests/fixtures/m0-core/invalid/runbook-gap.json
expect E_GRAPH_CYCLE scripts/validate/beads_snapshot.sh tests/fixtures/m0-core/invalid/graph-cycle.jsonl
expect E_DUPLICATE_KEY scripts/validate/m0_artifact.sh tests/fixtures/m0-core/invalid/duplicate-key.json
expect E_DECISIONS_EMPTY scripts/validate/m0_decisions.sh contracts/m0/decisions.json --complete
expect E_CLOSE_BLOCKED scripts/validate/close_guard.sh boring-cdc-m0 tests/fixtures/m0-core/valid/graph.jsonl
printf 'm0 core hostile corpus pass seed=%s product_faults=fault_not_applicable\n' "$seed"
