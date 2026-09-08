#!/bin/sh
set -eu
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/e2e/m0_validation_core.sh [SEED]'; exit 0; }
seed=${1:-m0-core-v1}; [ "$seed" = m0-core-v1 ] || { echo 'E_SEED: expected m0-core-v1' >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/boring-cdc-m0-core.XXXXXX"); trap 'rm -rf "$tmp"' EXIT HUP INT TERM
before=$(sha256sum tests/fixtures/m0-core/valid/* contracts/m0/*.json | sha256sum | cut -d' ' -f1)
scripts/validate/m0_decision.sh tests/fixtures/m0-core/valid/decisions.json --complete --owners tests/fixtures/m0-core/valid/owners.json --fixtures tests/fixtures/m0-core/valid/fixtures.json --executors tests/fixtures/m0-core/valid/executors.json >/dev/null
scripts/validate/m0_artifact.sh tests/fixtures/m0-core/valid/artifacts.json --complete >/dev/null
scripts/validate/m0_decisions.sh tests/fixtures/m0-core/valid/decisions.json --complete --owners tests/fixtures/m0-core/valid/owners.json --fixtures tests/fixtures/m0-core/valid/fixtures.json --executors tests/fixtures/m0-core/valid/executors.json >/dev/null
scripts/validate/runbook_registry.sh tests/fixtures/m0-core/valid/runbooks.json --release >/dev/null
scripts/validate/beads_snapshot.sh .beads/issues.jsonl --output "$tmp/normalized.json" >/dev/null
# A DB-free captured JSONL round-trips through an isolated br store with exact
# semantic equality across every issue and dependency field.
mkdir -p "$tmp/roundtrip/.beads"; cp .beads/issues.jsonl "$tmp/original.jsonl"; cp .beads/issues.jsonl "$tmp/roundtrip/.beads/issues.jsonl"
(
 cd "$tmp/roundtrip"
 br init --prefix synthetic --db .beads/beads.db --force >/dev/null
 br sync --import-only --db .beads/beads.db >/dev/null
 br sync --flush-only --db .beads/beads.db >/dev/null
)
python3 - "$tmp/original.jsonl" "$tmp/roundtrip/.beads/issues.jsonl" "$tmp/normalized.json" <<'PY'
import json,sys
load=lambda p:{x['id']:x for x in map(json.loads,open(p))}
assert load(sys.argv[1]) == load(sys.argv[2])
x=json.load(open(sys.argv[3])); assert x['schema_version']=='pinned-graph/v1'; assert len(x['issues'])>100; assert len(x['witness_root'])==64
PY
after=$(sha256sum tests/fixtures/m0-core/valid/* contracts/m0/*.json | sha256sum | cut -d' ' -f1)
[ "$before" = "$after" ] || { echo E_SOURCE_MUTATED >&2; exit 1; }
printf 'm0 core e2e pass seed=%s source_sha256=%s cleanup=trap\n' "$seed" "$before"
