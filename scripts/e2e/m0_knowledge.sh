#!/bin/sh
set -eu
[ "${1:-}" != --help ] || { echo 'Usage: scripts/e2e/m0_knowledge.sh [SEED]'; exit 0; }
seed=${1:-m0-knowledge-v1}; [ "$seed" = m0-knowledge-v1 ] || { echo E_SEED >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd);cd "$root";tmp=$(mktemp -d "${TMPDIR:-/tmp}/boring-cdc-knowledge.XXXXXX");trap 'rm -rf "$tmp"' EXIT HUP INT TERM
before=$(sha256sum tests/fixtures/m0-knowledge/valid/* contracts/agent/claims.schema.json contracts/agent/claim-index.schema.json contracts/agent/claim-compatibility.schema.json contracts/agent/handoff.schema.json contracts/knowledge/findings.schema.json evidence/findings.jsonl|sha256sum|cut -d' ' -f1)
python3 - "$tmp" <<'PY'
import json,sys
from pathlib import Path
r=Path('tests/fixtures/m0-knowledge/valid');t=Path(sys.argv[1]);claims=json.loads((r/'claims.json').read_text());index=json.loads((r/'claim-index.json').read_text())
for i,name in enumerate(('exact','compatible')):
 (t/f'{name}.json').write_text(json.dumps({'schema_version':'claims/v1','claims':[claims['claims'][i]]}));(t/f'{name}-index.json').write_text(json.dumps({'schema_version':'claim-index/v1','entries':[index['entries'][i]]}))
PY
scripts/validate/claims.sh "$tmp/exact.json" --index "$tmp/exact-index.json" --baseline-index "$tmp/exact-index.json" --owners tests/fixtures/m0-knowledge/valid/owners.json --actual tests/fixtures/m0-knowledge/valid/actual-exact.json --compatibility tests/fixtures/m0-knowledge/valid/compatibility.json >/dev/null
scripts/validate/claims.sh "$tmp/compatible.json" --index "$tmp/compatible-index.json" --baseline-index "$tmp/compatible-index.json" --owners tests/fixtures/m0-knowledge/valid/owners.json --actual tests/fixtures/m0-knowledge/valid/actual-compatible.json --compatibility tests/fixtures/m0-knowledge/valid/compatibility.json >/dev/null
scripts/validate/findings.sh evidence/findings.jsonl --baseline evidence/findings.jsonl >/dev/null
scripts/validate/handoff.sh tests/fixtures/m0-knowledge/valid/handoff.json >/dev/null
after=$(sha256sum tests/fixtures/m0-knowledge/valid/* contracts/agent/claims.schema.json contracts/agent/claim-index.schema.json contracts/agent/claim-compatibility.schema.json contracts/agent/handoff.schema.json contracts/knowledge/findings.schema.json evidence/findings.jsonl|sha256sum|cut -d' ' -f1)
[ "$before" = "$after" ] || { echo E_SOURCE_MUTATED >&2;exit 1; }
printf 'm0 knowledge e2e pass seed=%s source_sha256=%s cleanup=trap\n' "$seed" "$before"
