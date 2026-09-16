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

# Require each shape-valid hostile handoff through the official leaf validator,
# then invoke the same canonical aggregate join used by the positive e2e path.
pack_path=tmp/'pack.json';pack_path.write_text(canon(pack)+'\n')
for code,pointer,field,value in (
 ('E_WORLD_STATE_MISMATCH','/world_state_digest','world_state_digest','0'*64),
 ('E_HANDOFF_HEAD_MISMATCH','/head_sha','head_sha','0'*40),
 ('E_HANDOFF_PATH_MISMATCH','/changed_paths','changed_paths',['contracts/agent/handoff.schema.json'])):
 bad=copy.deepcopy(base);bad[field]=value;p=tmp/'bad-handoff.json';p.write_text(canon(bad)+'\n')
 cp=subprocess.run([str(root/'scripts/validate/handoff.sh'),str(p)],text=True,capture_output=True)
 if cp.returncode or cp.stderr:raise SystemExit('E_HANDOFF_SHAPE_UNEXPECTED:'+cp.stdout+cp.stderr)
 cp=subprocess.run([str(root/'scripts/e2e/m0_validation_tooling.sh'),'--check-handoff',str(pack_path),str(p)],text=True,capture_output=True)
 if cp.returncode==0 or cp.stderr:raise SystemExit('E_HANDOFF_COMPATIBILITY_NOT_REJECTED:'+code)
 result=json.loads(cp.stdout); result_path=tmp/'handoff-result.json'; result_path.write_text(canon(result)+'\n')
 schema_cp=subprocess.run([sys.executable,str(root/'scripts/lib/core_validator.py'),'schema',str(result_path),'--schema',str(root/'contracts/common/validation-result.schema.json')],text=True,capture_output=True)
 if schema_cp.returncode:raise SystemExit('E_HANDOFF_RESULT_SCHEMA:'+schema_cp.stdout+schema_cp.stderr)
 hits=[f for f in result['findings'] if f=={'code':code,'pointer':pointer,'owner_bead':'boring-cdc-m0-validation-tooling','message':'handoff provenance does not match generated context operation'}]
 if len(hits)!=1 or result.get('status')!='fail' or result.get('owner_bead')!='boring-cdc-m0-validation-tooling':raise SystemExit('E_HANDOFF_COMPATIBILITY_DIAGNOSTIC:'+code)

# A stale claim must identify the changed binding and cannot be promoted by a handoff.
claims=json.loads((valid/'claims.json').read_text()); claim=copy.deepcopy(claims['claims'][0]);actual=json.loads((valid/'actual-exact.json').read_text());actual['graph_digest']='0'*64
claim_doc={'schema_version':'claims/v1','claims':[claim]};index={'schema_version':'claim-index/v1','entries':[{'claim_id':claim['claim_id'],'claim_sha256':sha(claim),'owner_bead':claim['owner_bead']}]}
for name,obj in [('claims.json',claim_doc),('index.json',index),('actual.json',actual)]: (tmp/name).write_text(canon(obj)+'\n')
cp=subprocess.run([str(root/'scripts/validate/claims.sh'),str(tmp/'claims.json'),'--index',str(tmp/'index.json'),'--baseline-index',str(valid/'claim-index-baseline.json'),'--owners',str(valid/'owners.json'),'--claim-id',claim['claim_id'],'--actual',str(tmp/'actual.json'),'--compatibility',str(valid/'compatibility.json')],text=True,capture_output=True)
if cp.returncode == 0 or cp.stderr: raise SystemExit('E_STALE_CLAIM_NOT_REJECTED')
result=json.loads(cp.stdout);hits=[f for f in result['findings'] if f['code']=='E_CLAIM_INPUT_MISMATCH' and f['pointer'].endswith('/bindings/graph_digest')]
if len(hits)!=1 or hits[0].get('owner_bead')!='boring-cdc-m0.3': raise SystemExit('E_STALE_CLAIM_DIAGNOSTIC')

PY
printf 'm0 aggregate hostile corpus pass seed=%s repeated=2 official_cross_stage=5 product_faults=fault_not_applicable\n' "$seed"
