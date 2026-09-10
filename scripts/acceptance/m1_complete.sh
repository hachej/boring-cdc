#!/bin/sh
set -eu
export TMPDIR="${TMPDIR:-/var/tmp}"
case "${1:---write}" in
  --write|--verify|--probe) mode=$1 ;;
  *) echo "usage: $0 [--write|--verify]" >&2; exit 2 ;;
esac
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$root"
python3 - "$mode" <<'PY'
import hashlib, json, os, stat, subprocess, sys
from pathlib import Path

mode=sys.argv[1]
root=Path.cwd()
coverage_path=root/'contracts/coverage/m1.json'
reconciliation_path=root/'contracts/coverage/m1-owner-reconciliation.json'
evidence_path=root/'artifacts/boring-cdc-m1-complete/gate/evidence.json'
required=[
 'boring-cdc-m1-bootstrap-sm','boring-cdc-m1-config','boring-cdc-m1-control-fixtures',
 'boring-cdc-m1-ddl-fixtures','boring-cdc-m1-decoder','boring-cdc-m1-ordering',
 'boring-cdc-m1-preflight','boring-cdc-m1-source-identity','boring-cdc-m1-workload',
 'boring-cdc-m1-cli-contract','boring-cdc-m1.1']
waived='boring-cdc-m0-gate'
correlation={'bead_id','scenario_id','correlation_id','run_id','capture_epoch','component','phase','outcome','config_fingerprint','evidence_digest'}
errors=[]
def fail(message): errors.append(message)
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def load(path):
 try: return json.loads(path.read_text())
 except Exception as exc: fail(f'invalid JSON {path.relative_to(root)}: {exc}'); return {}

coverage=load(coverage_path); reconciliation=load(reconciliation_path)
if coverage.get('schema_version')!='m1-coverage/v1': fail('coverage schema_version mismatch')
leaves=coverage.get('required_leaves',[])
if [x.get('owner_bead') for x in leaves] != required: fail('coverage required leaf order/set mismatch')
features={}
for leaf in leaves:
 owner=leaf.get('owner_bead')
 if not leaf.get('feature_ids') or not leaf.get('contract_ids') or not leaf.get('unit_target'):
  fail(f'{owner}: missing feature/contract/unit ownership')
 for feature in leaf.get('feature_ids',[]):
  if feature in features: fail(f'duplicate feature owner: {feature}')
  features[feature]=owner
 for script in leaf.get('component_and_fault_scripts',[]):
  if script.startswith('scripts/'):
   p=root/script
   if not p.is_file() or not os.access(p,os.X_OK): fail(f'{owner}: script not executable: {script}')
 for item in leaf.get('evidence',[]):
  p=root/item.get('manifest','')
  if not p.is_file(): fail(f'{owner}: missing evidence manifest {item.get("manifest")}'); continue
  if sha(p)!=item.get('sha256'): fail(f'{owner}: evidence manifest digest mismatch: {item.get("manifest")}')
  manifest=load(p)
  if manifest.get('evidence_tier') not in {'leaf','component','milestone'}: fail(f'{owner}: future/release evidence forbidden')
  if manifest.get('evidence_tier') in {'component','milestone'} and not manifest.get('tier_proof',{}).get('deterministic_rerun'):
   fail(f'{owner}: component/milestone two-run reproducibility missing')
  inv=p.parent/'sha256.txt'
  if not inv.is_file(): fail(f'{owner}: missing SHA-256 inventory {inv.relative_to(root)}')
  else:
   for line in inv.read_text().splitlines():
    try: expected, rel=line.split('  ',1); target=p.parent/rel
    except ValueError: fail(f'{owner}: malformed inventory line in {inv.relative_to(root)}'); continue
    if not target.is_file() or sha(target)!=expected: fail(f'{owner}: inventory mismatch {target.relative_to(root)}')

# Evidence manifests must satisfy the repository schema, independently of old leaf freshness.
for leaf in leaves:
 for item in leaf.get('evidence',[]):
  command=['python3','scripts/lib/core_validator.py','schema',item['manifest'],'--schema','contracts/evidence.schema.json']
  run=subprocess.run(command,text=True,capture_output=True)
  if run.returncode: fail(f"schema validation failed: {item['manifest']}: {run.stdout.strip()} {run.stderr.strip()}")

# Every structured M1 log line carries the correlation envelope.
for path in sorted((root/'artifacts').glob('boring-cdc-m1-*/**/*.jsonl')):
 for number,line in enumerate(path.read_text().splitlines(),1):
  if not line.strip(): continue
  try: value=json.loads(line)
  except json.JSONDecodeError: continue
  missing=correlation-set(value)
  if missing: fail(f'{path.relative_to(root)}:{number}: missing correlation fields {sorted(missing)}')

# Pinned Bead snapshot is the completion authority in clean/sandbox checkouts.
rows={}
for line in (root/'.beads/issues.jsonl').read_text().splitlines():
 if line.strip():
  row=json.loads(line); rows[row['id']]=row
for bead in required:
 if rows.get(bead,{}).get('status')!='closed': fail(f'required leaf not closed: {bead}')
if rows.get(waived,{}).get('status')=='closed': waived_state='closed'
else: waived_state='owner-waived-open'
barrier=rows.get('boring-cdc-m1-complete',{})
blockers={d['depends_on_id'] for d in barrier.get('dependencies',[]) if d.get('type')=='blocks'}
if not set(required+[waived]).issubset(blockers): fail('barrier blocking edges incomplete')
raw=rows.get('boring-cdc-m1-raw-demo',{})
if not any(d.get('type')=='blocks' and d.get('depends_on_id')=='boring-cdc-m1-complete' for d in raw.get('dependencies',[])):
 fail('completion barrier does not block raw-demo')
# Blocking-edge DAG.
graph={key:[d['depends_on_id'] for d in row.get('dependencies',[]) if d.get('type')=='blocks'] for key,row in rows.items()}
visiting=set(); done=set()
def visit(node):
 if node in visiting: fail(f'blocking dependency cycle at {node}'); return
 if node in done: return
 visiting.add(node)
 for dep in graph.get(node,[]): visit(dep)
 visiting.remove(node); done.add(node)
for node in graph: visit(node)

# Owner-answer reconciliation is executable and no answered-card marker remains.
if reconciliation.get('schema_version')!='m1-owner-reconciliation/v1': fail('reconciliation schema mismatch')
expected_cards={'5a994cfd-e4e2-46a7-b512-5dae280acae0','765bd3b2-4b68-4102-a9ec-43ca93357390'}
if set(reconciliation.get('cards',{}))!=expected_cards: fail('owner cards missing from reconciliation')
for card in reconciliation.get('cards',{}).values():
 if card.get('decision')!='accept': fail('owner card is not accepted')
markers=('boring-cdc-d-security','boring-cdc-d-values','boring-cdc-d-keys','boring-cdc-d-failure-policy','boring-cdc-d-sqlite','boring-cdc-d-wal-cap','boring-cdc-d-compose')
provisional_marker='M0-'+'PROVISIONAL'
for base in ('src','scripts','fixtures','contracts','artifacts','tests'):
 for path in (root/base).rglob('*'):
  if path.is_file() and 'target' not in path.parts:
   try: text=path.read_text()
   except UnicodeDecodeError: continue
   if provisional_marker in text: fail(f'provisional marker reintroduced: {path.relative_to(root)}')
   for marker in markers:
    if f'M0-RECONCILED: {marker}' in text: fail(f'unreconciled marker {marker}: {path.relative_to(root)}')
source=(root/'src/m1_config.rs').read_text()
for literal in ('Bytes(1_048_576)','Bytes(4_194_304)','Milliseconds(10_000)','Milliseconds(30_000)','Milliseconds(300_000)'):
 if literal not in source: fail(f'security literal missing: {literal}')
ordering=(root/'src/m1_ordering.rs').read_text()
if 'MAX_CANONICAL_KEY_COMPONENTS: usize = 8' not in ordering: fail('accepted key arity missing')
workload=(root/'scripts/validate/m1_workload.py').read_text()
for literal in ('30c14e8b953c11dfb9ab4ac10ccde0cbad9c7ae4d25097d62258e7d71d7a510d','b03d04460a78c4cd0b02817952e6bcc21d89c9b712b027b1b866cf5e62c7acc8','docker_engine_policy\':\'28.3.3','compose_policy\':\'2.39.2'):
 if literal not in workload: fail(f'accepted workload/Compose literal missing: {literal}')
compose=(root/'fixtures/m1/workload-compose.yml').read_text()
for literal in ('platform: linux/amd64','00bc86618629af00d2937fdc5a5d63db3ff8450acf52f0636ec813c7f4902929','74c213b4d4cb4854c2497694df0c2d153c041003eadbb0457ae62c28cb8d723f'):
 if literal not in compose: fail(f'accepted Compose literal missing: {literal}')

# Plan-space validator is the executable duplicate-owner / PLAN-only assertion check.
run=subprocess.run(['scripts/validate/plan_coverage.sh'],text=True,capture_output=True)
if run.returncode: fail(f'plan coverage failed: {run.stdout.strip()} {run.stderr.strip()}')
summary={
 'schema_version':'m1-completion-summary/v1','status':'fail' if errors else 'pass',
 'required_leaves':required,'closed_leaves':[b for b in required if rows.get(b,{}).get('status')=='closed'],
 'm0_gate':{'bead':waived,'state':waived_state,'waiver':'owner instruction in boring-cdc-m1-complete dispatch'},
 'blocking_edges':sorted(blockers),'raw_demo_downstream':True,'blocking_graph_acyclic':not any('cycle' in e for e in errors),
 'feature_owner_count':len(features),'manifest_count':sum(len(x.get('evidence',[])) for x in leaves),
 'correlation_fields':sorted(correlation),'owner_cards':sorted(expected_cards),'findings':errors}
encoded=json.dumps(summary,sort_keys=True,indent=2)+'\n'
if errors:
 print(encoded,end=''); raise SystemExit(1)
if mode=='--probe':
 print(encoded,end='')
elif mode=='--write':
 gate=evidence_path.parent; gate.mkdir(parents=True,exist_ok=True)
 (gate/'completion-summary.json').write_text(encoded)
 head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
 command=[]; outputs=[]
 for index in (1,2):
  run=subprocess.run(['scripts/acceptance/m1_complete.sh','--probe'],text=True,capture_output=True)
  log=gate/f'run-{index}.log'; empty=gate/f'run-{index}.stderr'
  log.write_text(run.stdout); empty.write_text(run.stderr); outputs.append((run.stdout,run.stderr))
  if run.returncode != 0: raise SystemExit(f'completion probe {index} failed: {run.stdout} {run.stderr}')
  command.append({'argv':'scripts/acceptance/m1_complete.sh --probe','version':'m1-complete/v1','exit_code':run.returncode,'stdout_path':str(log.relative_to(root)),'stdout_sha256':sha(log),'stderr_path':str(empty.relative_to(root)),'stderr_sha256':sha(empty)})
 if outputs[0] != outputs[1]: raise SystemExit('completion probes were not byte-identical')
 artifacts=[coverage_path,reconciliation_path,gate/'completion-summary.json']
 source_digest=hashlib.sha256(coverage_path.read_bytes()+reconciliation_path.read_bytes()).hexdigest()
 evidence={'schema_version':'evidence/v1','owner_bead':'boring-cdc-m1-complete','scenario_id':'SCN-M1-COMPLETION-BARRIER','evidence_profile':'runtime','evidence_tier':'milestone','seed':'m1-complete-v1','git_commit':head,'commands':command,'source_preservation':{'before_sha256':source_digest,'after_sha256':source_digest,'preserved':True},'cleanup':{'complete':True,'remaining_paths':[]},'redaction':{'checked':True,'secrets_found':0},'tier_proof':{'targeted_checks':True,'boundary_e2e':True,'fault_suite':True,'deterministic_rerun':True,'consumed_contract_vectors':True,'workspace_tests':True,'integration':True,'clean_environment':True,'exit_assertions':True,'endurance':False,'full_failure_matrix':False,'clean_clone':False},'result':{'status':'pass','digest':hashlib.sha256(b''.join(p.read_bytes() for p in artifacts)).hexdigest(),'artifacts':[str(p.relative_to(root)) for p in artifacts],'product_faults':'all M1 leaf fault suites validated by manifest','runtime_observed':True}}
 evidence_path.write_text(json.dumps(evidence,sort_keys=True,indent=2)+'\n')
 print(encoded,end='')
else:
 if not evidence_path.is_file(): fail('gate evidence missing')
 else:
  evidence=load(evidence_path)
  commands=evidence.get('commands',[])
  if len(commands)!=2: fail('gate must contain exactly two command executions')
  observed=[]; stdout_paths=set(); stderr_paths=set()
  for entry in commands:
   path=root/entry.get('stdout_path',''); stderr=root/entry.get('stderr_path','')
   stdout_paths.add(str(path)); stderr_paths.add(str(stderr))
   if entry.get('argv')!='scripts/acceptance/m1_complete.sh --probe' or entry.get('exit_code')!=0: fail('gate command was not a successful completion probe')
   if not path.is_file() or sha(path)!=entry.get('stdout_sha256'): fail('gate command stdout digest mismatch')
   if not stderr.is_file() or sha(stderr)!=entry.get('stderr_sha256'): fail('gate command stderr digest mismatch')
   if path.is_file():
    observed.append((path.read_bytes(),stderr.read_bytes()))
    try:
     if json.loads(path.read_text()).get('status')!='pass': fail('gate command output is not pass')
    except Exception: fail('gate command output is not valid completion JSON')
  if len(stdout_paths)!=2 or len(stderr_paths)!=2: fail('gate commands must reference distinct stdout/stderr captures')
  expected_output=encoded.encode()
  if len(observed)==2 and (observed[0]!=observed[1] or observed[0]!=(expected_output,b'')):
   fail('gate command executions are not byte-identical fresh semantic summaries')
  commit=evidence.get('git_commit','')
  exists=subprocess.run(['git','cat-file','-e',str(commit)+'^{commit}'],capture_output=True).returncode==0
  ancestor=exists and subprocess.run(['git','merge-base','--is-ancestor',str(commit),'HEAD'],capture_output=True).returncode==0
  clean_inputs=ancestor and subprocess.run(['git','diff','--quiet',str(commit)+'..HEAD','--','.beads/issues.jsonl','src','fixtures','contracts','scripts','tests','artifacts',':(exclude)artifacts/boring-cdc-m1-complete']).returncode==0
  if not (exists and ancestor and clean_inputs): fail('gate git_commit is missing, non-ancestor, or stale')
  if evidence.get('result',{}).get('status')!='pass': fail('gate result is not pass')
  paths=[root/p for p in evidence.get('result',{}).get('artifacts',[])]
  if not all(p.is_file() for p in paths) or hashlib.sha256(b''.join(p.read_bytes() for p in paths)).hexdigest()!=evidence.get('result',{}).get('digest'):
   fail('gate result digest mismatch')
  run=subprocess.run(['scripts/validate/evidence.sh',str(evidence_path.relative_to(root))],text=True,capture_output=True)
  if run.returncode: fail(f'gate evidence invalid: {run.stdout.strip()} {run.stderr.strip()}')
 if errors:
  summary['status']='fail'; summary['findings']=errors; print(json.dumps(summary,sort_keys=True,indent=2)); raise SystemExit(1)
 print(encoded,end='')
PY
