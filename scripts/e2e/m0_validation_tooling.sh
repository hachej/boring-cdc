#!/bin/sh
set -eu
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/e2e/m0_validation_tooling.sh [SEED]'; exit 0; }
seed=${1:-m0-validator-v1}
[ "$seed" = m0-validator-v1 ] || { echo 'E_SEED: expected m0-validator-v1' >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/boring-cdc-m0-validator-e2e.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
export BORING_AGENT_NOW=2026-01-01T00:00:00Z
source_digest() { git ls-files -z contracts scripts tests/fixtures evidence .beads/issues.jsonl | xargs -0 sha256sum | sha256sum | cut -d' ' -f1; }
before=$(source_digest)

for round in 1 2; do
 scripts/e2e/m0_validation_core.sh m0-core-v1 >"$tmp/core.$round" 2>&1
 scripts/e2e/m0_context.sh m0-context-v1 >"$tmp/context.$round" 2>&1
 scripts/e2e/m0_knowledge.sh m0-knowledge-v1 >"$tmp/knowledge.$round" 2>&1
 scripts/agent/context boring-cdc-m0-validation-tooling --profile handoff --observed-at "$BORING_AGENT_NOW" >"$tmp/pack.$round"
done
for suite in core context knowledge pack; do cmp "$tmp/$suite.1" "$tmp/$suite.2" >/dev/null || { echo "E_NONDETERMINISTIC:$suite" >&2; exit 1; }; done

# Bind an actual generated context world/effective contract to the leaf-owned
# immutable-claim and handoff validators, then exercise staged graph readiness.
python3 - "$tmp" <<'PY'
import copy, hashlib, json, subprocess, sys
from pathlib import Path
root=Path.cwd(); tmp=Path(sys.argv[1])
canon=lambda x:json.dumps(x,sort_keys=True,separators=(',',':'))
sha=lambda x:hashlib.sha256(canon(x).encode()).hexdigest()
pack=json.loads((tmp/'pack.1').read_text())
names={a['name'] for a in pack['attachments']}
if not {'selected_bead','manifest','dependency_outputs','consumed_canonical_rows','handoff_state'} <= names:
 raise SystemExit('E_STAGE_COMPOSITION')
deps=next(a['content'] for a in pack['attachments'] if a['name']=='dependency_outputs')
dep_ids={x['id'] for x in deps}
if not {'boring-cdc-m0.1','boring-cdc-m0.2','boring-cdc-m0.3'} <= dep_ids or dep_ids-{'boring-cdc-m0','boring-cdc-m0.1','boring-cdc-m0.2','boring-cdc-m0.3'}:
 raise SystemExit('E_STAGE_LEAF_SET')

valid=root/'tests/fixtures/m0-knowledge/valid'
claims=json.loads((valid/'claims.json').read_text()); claim=copy.deepcopy(claims['claims'][0])
actual=json.loads((valid/'actual-exact.json').read_text())
world_digest=sha(pack['world_state']); effective_digest=sha(pack['effective_contract'])
for obj in (claim['bindings'],actual):
 obj['graph_digest']=pack['world_state']['beads_snapshot_digest']
 obj['effective_contract_digest']=effective_digest
 obj['git_commit']=pack['world_state']['git_commit']
claim_doc={'schema_version':'claims/v1','claims':[claim]}
index={'schema_version':'claim-index/v1','entries':[{'claim_id':claim['claim_id'],'claim_sha256':sha(claim),'owner_bead':claim['owner_bead']}]}
for name,obj in [('claims.json',claim_doc),('index.json',index),('actual.json',actual)]: (tmp/name).write_text(canon(obj)+'\n')
cp=subprocess.run([str(root/'scripts/validate/claims.sh'),str(tmp/'claims.json'),'--index',str(tmp/'index.json'),'--baseline-index',str(valid/'claim-index-baseline.json'),'--owners',str(valid/'owners.json'),'--claim-id',claim['claim_id'],'--actual',str(tmp/'actual.json'),'--compatibility',str(valid/'compatibility.json')],text=True,capture_output=True)
if cp.returncode: raise SystemExit('E_CONTEXT_CLAIM:'+cp.stdout+cp.stderr)

handoff=json.loads((valid/'handoff.json').read_text()); state=next(a['content'] for a in pack['attachments'] if a['name']=='handoff_state')
handoff.update({'bead_id':'boring-cdc-m0-validation-tooling','world_state_digest':world_digest,'base_sha':state['implementation_range']['base_sha'],'head_sha':state['implementation_range']['head_sha'],'changed_paths':state['changed_paths'],'facts':['generated context, immutable claim index, and handoff agree'],'observations':['aggregate composition is deterministic'],'hypotheses':['owner review remains downstream']})
(tmp/'handoff.json').write_text(canon(handoff)+'\n')
cp=subprocess.run([str(root/'scripts/validate/handoff.sh'),str(tmp/'handoff.json')],text=True,capture_output=True)
if cp.returncode: raise SystemExit('E_CONTEXT_HANDOFF:'+cp.stdout+cp.stderr)

# A+B unblock decisions; C and this aggregate remain independent blockers of M0 completion.
rows={
 'A':{'status':'closed','deps':[]},'B':{'status':'closed','deps':[]},
 'C':{'status':'open','deps':[]},'aggregate':{'status':'open','deps':['A','B','C']},
 'decision':{'status':'open','deps':['A','B']},
 'complete':{'status':'open','deps':['A','B','C','aggregate']},
}
ready=lambda key: rows[key]['status']=='open' and all(rows[d]['status']=='closed' for d in rows[key]['deps'])
if not ready('decision') or ready('aggregate') or ready('complete'): raise SystemExit('E_STAGED_READINESS')
rows['C']['status']='closed'
if not ready('aggregate') or ready('complete'): raise SystemExit('E_AGGREGATE_BARRIER')
rows['aggregate']['status']='closed'
if not ready('complete'): raise SystemExit('E_COMPLETION_BARRIER')
PY

after=$(source_digest)
[ "$before" = "$after" ] || { echo E_SOURCE_MUTATED >&2; exit 1; }
printf 'm0 aggregate e2e pass seed=%s reruns=2 leaf_set=A+B+C claim_index=validated readiness=staged source_sha256=%s cleanup=trap\n' "$seed" "$before"
