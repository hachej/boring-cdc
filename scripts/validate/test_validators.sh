#!/bin/sh
set -eu
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/validate/test_validators.sh [SEED]'; exit 0; }
seed=${1:-m0-validator-v1}
[ "$seed" = m0-validator-v1 ] || { echo 'E_SEED: expected m0-validator-v1' >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/boring-cdc-m0-validator-unit.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM

# Preserve each leaf's complete corpus as the source of mechanism-level truth.
scripts/validate/test_core_validators.sh m0-core-v1 >"$tmp/core.log" 2>&1
scripts/validate/test_context.sh m0-context-v1 >"$tmp/context.log" 2>&1
scripts/validate/test_knowledge.sh m0-knowledge-v1 >"$tmp/knowledge.log" 2>&1

python3 - "$tmp" <<'PY'
import json, subprocess, sys
from pathlib import Path
root=Path.cwd(); tmp=Path(sys.argv[1])
required={
 'core.log':('Ran 14 tests','OK'),
 'context.log':('context unit corpus: 17 passed','plan-coverage/v1'),
 'knowledge.log':('tests=12',),
}
for name,needles in required.items():
 text=(tmp/name).read_text()
 for needle in needles:
  if needle not in text: raise SystemExit(f'E_LEAF_CORPUS:{name}:{needle}')
# The aggregate owns composition only: all leaf CLIs remain the executors.
for rel in ('scripts/validate/test_core_validators.sh','scripts/validate/test_context.sh','scripts/validate/test_knowledge.sh'):
 if not (root/rel).is_file(): raise SystemExit('E_LEAF_EXECUTOR:'+rel)
# Stay state-neutral: decisions may legitimately close or attach evidence after
# A+B while this independent aggregate gate is running.
rows=[json.loads(line) for line in (root/'.beads/issues.jsonl').read_text().splitlines() if line]
if len([row for row in rows if row.get('issue_type')=='decision']) < 25:
 raise SystemExit('E_DECISION_INVENTORY')
assignments=json.loads((root/'contracts/coverage/plan-to-beads.json').read_text())['assignments']
stable_ids=json.loads((root/'contracts/agent/stable-ids.json').read_text())['entries']
assignment_ids=[row.get('id') for row in assignments]; expected_ids={row.get('id') for row in stable_ids}
if len(assignment_ids)!=len(set(assignment_ids)) or set(assignment_ids)!=expected_ids:
 raise SystemExit('E_ASSIGNMENT_INVENTORY')
if not all(isinstance(row.get('owner_bead'),str) for row in assignments):
 raise SystemExit('E_ASSIGNMENT_OWNER')
PY
assignments=$(python3 -c 'import json; print(len(json.load(open("contracts/coverage/plan-to-beads.json"))["assignments"]))')
printf 'm0 aggregate validator corpus pass seed=%s leaves=3 state=neutral assignments=%s\n' "$seed" "$assignments"
