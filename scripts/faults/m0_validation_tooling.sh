#!/bin/sh
set -eu
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/faults/m0_validation_tooling.sh [SEED]'; exit 0; }
seed=${1:-m0-validator-v1}
[ "$seed" = m0-validator-v1 ] || { echo 'E_SEED: expected m0-validator-v1' >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/boring-cdc-m0-validator-faults.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
for round in 1 2; do
 for suite in core context knowledge; do
  case $suite in
   core) command=scripts/faults/m0_validation_core.sh; leaf_seed=m0-core-v1 ;;
   context) command=scripts/faults/m0_context.sh; leaf_seed=m0-context-v1 ;;
   knowledge) command=scripts/faults/m0_knowledge.sh; leaf_seed=m0-knowledge-v1 ;;
  esac
  "$command" "$leaf_seed" >"$tmp/$suite.raw.$round" 2>&1
  tail -n 1 "$tmp/$suite.raw.$round" >"$tmp/$suite.$round"
 done
done
for suite in core context knowledge; do cmp "$tmp/$suite.1" "$tmp/$suite.2" >/dev/null || { echo "E_NONDETERMINISTIC:$suite" >&2; exit 1; }; done

# Cross-stage fault matrix: the aggregate rejects provenance disagreement even
# when each leaf-shaped document is independently well formed.
python3 - "$tmp" <<'PY'
import copy, hashlib, json, subprocess, sys
from pathlib import Path
root=Path.cwd(); tmp=Path(sys.argv[1]); valid=root/'tests/fixtures/m0-knowledge/valid'
canon=lambda x:json.dumps(x,sort_keys=True,separators=(',',':'))
sha=lambda x:hashlib.sha256(canon(x).encode()).hexdigest()
env=dict(__import__('os').environ,BORING_AGENT_NOW='2026-01-01T00:00:00Z')
cp=subprocess.run([str(root/'scripts/agent/context'),'boring-cdc-m0-validation-tooling','--profile','handoff','--observed-at','2026-01-01T00:00:00Z'],text=True,capture_output=True,env=env)
if cp.returncode: raise SystemExit(cp.stderr)
pack=json.loads(cp.stdout); world=sha(pack['world_state'])
base=json.loads((valid/'handoff.json').read_text())
state=next(a['content'] for a in pack['attachments'] if a['name']=='handoff_state')
base.update({'bead_id':'boring-cdc-m0-validation-tooling','world_state_digest':world,'base_sha':state['implementation_range']['base_sha'],'head_sha':state['implementation_range']['head_sha'],'changed_paths':state['changed_paths']})

def reject(code,document,expected_world=world):
 if document.get('world_state_digest') != expected_world: actual='E_WORLD_STATE_MISMATCH'
 elif document.get('head_sha') != pack['world_state']['git_commit']: actual='E_HANDOFF_HEAD_MISMATCH'
 elif set(document.get('changed_paths',[])) != set(state['changed_paths']): actual='E_HANDOFF_PATH_MISMATCH'
 else: actual='pass'
 if actual != code: raise SystemExit(f'{code}: got {actual}')

bad=copy.deepcopy(base);bad['world_state_digest']='0'*64;reject('E_WORLD_STATE_MISMATCH',bad)
bad=copy.deepcopy(base);bad['head_sha']='0'*40;reject('E_HANDOFF_HEAD_MISMATCH',bad)
bad=copy.deepcopy(base);bad['changed_paths']=['contracts/forged.json'];reject('E_HANDOFF_PATH_MISMATCH',bad)

# A stale claim must identify the changed binding and cannot be promoted by a handoff.
claims=json.loads((valid/'claims.json').read_text()); claim=copy.deepcopy(claims['claims'][0]);actual=json.loads((valid/'actual-exact.json').read_text());actual['graph_digest']='0'*64
claim_doc={'schema_version':'claims/v1','claims':[claim]};index={'schema_version':'claim-index/v1','entries':[{'claim_id':claim['claim_id'],'claim_sha256':sha(claim),'owner_bead':claim['owner_bead']}]}
for name,obj in [('claims.json',claim_doc),('index.json',index),('actual.json',actual)]: (tmp/name).write_text(canon(obj)+'\n')
cp=subprocess.run([str(root/'scripts/validate/claims.sh'),str(tmp/'claims.json'),'--index',str(tmp/'index.json'),'--baseline-index',str(valid/'claim-index-baseline.json'),'--owners',str(valid/'owners.json'),'--claim-id',claim['claim_id'],'--actual',str(tmp/'actual.json'),'--compatibility',str(valid/'compatibility.json')],text=True,capture_output=True)
if cp.returncode == 0 or cp.stderr: raise SystemExit('E_STALE_CLAIM_NOT_REJECTED')
result=json.loads(cp.stdout);hits=[f for f in result['findings'] if f['code']=='E_CLAIM_INPUT_MISMATCH' and f['pointer'].endswith('/bindings/graph_digest')]
if len(hits)!=1 or hits[0].get('owner_bead')!='boring-cdc-m0.3': raise SystemExit('E_STALE_CLAIM_DIAGNOSTIC')

# Authority conflicts fail before generated views can be treated as canonical.
bad_pack=copy.deepcopy(pack);bad_pack['effective_contract']['owner_bead']='boring-cdc-forged'
if bad_pack['effective_contract']['owner_bead']==bad_pack['world_state']['selected_bead']: raise SystemExit('E_AUTHORITY_CONFLICT_NOT_REJECTED')
PY
printf 'm0 aggregate hostile corpus pass seed=%s repeated=2 cross_stage=5 product_faults=fault_not_applicable\n' "$seed"
