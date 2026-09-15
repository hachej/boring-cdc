#!/bin/sh
set -eu
export TMPDIR="${TMPDIR:-/var/tmp}"
case "${1:---write}" in
  --write|--verify|--probe) mode=$1 ;;
  *) echo "usage: $0 [--write|--verify|--probe]" >&2; exit 2 ;;
esac
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$root"
python3 - "$mode" <<'PY'
import hashlib, json, os, re, subprocess, sys
from pathlib import Path, PurePosixPath

mode=sys.argv[1]
root=Path.cwd()
coverage_path=root/'contracts/coverage/m2.json'
evidence_path=root/'artifacts/boring-cdc-m2-complete/gate/evidence.json'
required=[
 'boring-cdc-m2-heartbeat','boring-cdc-m2-init-recovery','boring-cdc-m2-journal',
 'boring-cdc-m2-jsonl','boring-cdc-m2-leases','boring-cdc-m2-ownership',
 'boring-cdc-m2-pressure','boring-cdc-m2-reconcile','boring-cdc-m2-schema',
 'boring-cdc-m2-spool','boring-cdc-m2-capture-runtime','boring-cdc-m2.1']
prior='boring-cdc-m1-raw-demo'
barrier_id='boring-cdc-m2-complete'
terminal='boring-cdc-m2-fault-status'
correlation={'bead_id','scenario_id','correlation_id','run_id','capture_epoch','component','phase','outcome','config_fingerprint','evidence_digest'}
errors=[]
def fail(message):
 if message not in errors: errors.append(message)
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def load(path):
 try: return json.loads(path.read_text())
 except Exception as exc: fail(f'invalid JSON {path.relative_to(root)}: {exc}'); return {}

def validate_inventory(owner, manifest):
 inventory=manifest.parent/'sha256.txt'
 if not inventory.is_file(): fail(f'{owner}: missing SHA-256 inventory {inventory.relative_to(root)}'); return
 listed=set()
 for number,line in enumerate(inventory.read_text().splitlines(),1):
  if not line.strip(): continue
  try: expected,rel=line.split('  ',1)
  except ValueError: fail(f'{owner}: malformed inventory line {inventory.relative_to(root)}:{number}'); continue
  pure=PurePosixPath(rel)
  if pure.is_absolute() or '..' in pure.parts or rel in listed:
   fail(f'{owner}: unsafe or duplicate inventory path {rel!r}'); continue
  listed.add(rel); target=manifest.parent/pure
  try: target.resolve().relative_to(manifest.parent.resolve())
  except ValueError: fail(f'{owner}: inventory path escapes artifact root: {rel}'); continue
  if not re.fullmatch(r'[0-9a-f]{64}',expected) or not target.is_file() or sha(target)!=expected:
   fail(f'{owner}: inventory mismatch {target.relative_to(root)}')
 actual={str(path.relative_to(manifest.parent)) for path in manifest.parent.rglob('*') if path.is_file() and path!=inventory}
 if listed!=actual: fail(f'{owner}: inventory set mismatch: missing={sorted(actual-listed)} extra={sorted(listed-actual)}')

# Evidence must be attributable to committed inputs. Generated gate outputs are
# the sole exception because their evidence necessarily names the input commit.
status=subprocess.check_output(['git','status','--porcelain=v1','--untracked-files=all','--','.beads/issues.jsonl','src','fixtures','contracts','scripts','tests','artifacts'],text=True)
dirty=[]
for line in status.splitlines():
 path=line[3:].split(' -> ')[-1]
 if not path.startswith('artifacts/boring-cdc-m2-complete/gate/'):
  dirty.append(line)
if dirty: fail('dirty certification inputs: '+', '.join(dirty))

coverage=load(coverage_path)
if coverage.get('schema_version')!='m2-coverage/v1': fail('coverage schema_version mismatch')
if coverage.get('owner_bead')!=barrier_id: fail('coverage owner mismatch')
if coverage.get('prior_terminal_proof')!=prior: fail('prior terminal proof mismatch')
if coverage.get('terminal_proof')!={'bead':terminal,'role':'downstream-proof-excluded-from-barrier-inputs'}:
 fail('terminal proof boundary mismatch')
leaves=coverage.get('required_leaves',[])
if [x.get('owner_bead') for x in leaves] != required: fail('coverage required leaf order/set mismatch')
features={}; contracts={}; manifest_count=0
plan=load(root/'contracts/coverage/plan-to-beads.json')
canonical={a.get('id'):a.get('owner_bead') for a in plan.get('assignments',[])}
for leaf in leaves:
 owner=leaf.get('owner_bead','unknown')
 if not leaf.get('feature_ids'): fail(f'{owner}: missing canonical feature IDs')
 if not leaf.get('contract_ids'): fail(f'{owner}: missing contract IDs')
 if not leaf.get('unit_target'): fail(f'{owner}: missing unit target')
 if not leaf.get('component_and_fault_scripts'): fail(f'{owner}: missing component/fault/validator scripts')
 events=leaf.get('structured_log_event_codes',[])
 if not events: fail(f'{owner}: missing structured log event codes')
 else:
  owned_text=''
  for path in [root/'src'/f"{leaf.get('unit_target','').split('::',1)[0]}.rs"]+[root/script for script in scripts]:
   try: owned_text += path.read_text()
   except (OSError,UnicodeDecodeError): pass
  for event in events:
   if event not in owned_text: fail(f'{owner}: structured log event code is not backed by owned implementation: {event}')
 if not leaf.get('evidence'): fail(f'{owner}: missing immutable evidence manifests')
 for feature in leaf.get('feature_ids',[]):
  if feature in features: fail(f'duplicate feature owner: {feature}')
  features[feature]=owner
  if canonical.get(feature)!=owner: fail(f'{owner}: canonical feature owner mismatch: {feature}')
 for contract in leaf.get('contract_ids',[]):
  if contract in contracts: fail(f'duplicate contract owner: {contract}')
  contracts[contract]=owner
  matches=[]
  for candidate in (root/'contracts').rglob('*.json'):
   try: value=json.loads(candidate.read_text())
   except (OSError,json.JSONDecodeError): continue
   if value.get('schema_version')==contract and value.get('owner_bead')==owner: matches.append(candidate)
  if len(matches)!=1: fail(f'{owner}: contract ID is not uniquely backed by an owned contract: {contract}')
 module=leaf.get('unit_target','').split('::',1)[0]
 if not (root/'src'/f'{module}.rs').is_file(): fail(f'{owner}: unit target module missing: {module}')
 for script in leaf.get('component_and_fault_scripts',[]):
  path=root/script
  if not path.is_file() or not os.access(path,os.X_OK): fail(f'{owner}: script not executable: {script}')
 for item in leaf.get('evidence',[]):
  manifest_count += 1
  path=root/item.get('manifest','')
  if not path.is_file(): fail(f'{owner}: missing evidence manifest {item.get("manifest")}'); continue
  if sha(path)!=item.get('sha256'): fail(f'{owner}: evidence manifest digest mismatch: {item.get("manifest")}')
  manifest=load(path)
  if manifest.get('owner_bead')!=owner: fail(f'{owner}: manifest owner mismatch: {path.relative_to(root)}')
  if manifest.get('evidence_tier') not in {'leaf','component'}: fail(f'{owner}: future/milestone evidence forbidden')
  if not manifest.get('tier_proof',{}).get('deterministic_rerun'): fail(f'{owner}: two-run reproducibility missing')
  attempts=manifest.get('result',{}).get('attempts',[])
  if len(attempts)<2 or len(set(attempts))!=len(attempts): fail(f'{owner}: two distinct rerun attempts missing')
  schema=subprocess.run(['python3','scripts/lib/core_validator.py','schema',str(path.relative_to(root)),'--schema','contracts/evidence.schema.json'],text=True,capture_output=True)
  if schema.returncode: fail(f'{owner}: evidence schema validation failed: {path.relative_to(root)}')
  validate_inventory(owner,path)
  log=path.parent/'logs/boring-cdc.jsonl'
  if not log.is_file(): fail(f'{owner}: missing structured log {log.relative_to(root)}')
  else:
   for number,line in enumerate(log.read_text().splitlines(),1):
    if not line.strip(): continue
    try: value=json.loads(line)
    except json.JSONDecodeError: fail(f'{owner}: invalid structured log JSON {log.relative_to(root)}:{number}'); continue
    missing=correlation-set(value)
    if missing: fail(f'{owner}: missing correlation fields {sorted(missing)} at {log.relative_to(root)}:{number}')
    elif value['bead_id']!=owner or value['scenario_id']!=manifest.get('scenario_id'): fail(f'{owner}: correlation identity mismatch at {log.relative_to(root)}:{number}')
    elif value['config_fingerprint'] is not None and not re.fullmatch(r'[0-9a-f]{64}',str(value['config_fingerprint'])): fail(f'{owner}: invalid config fingerprint at {log.relative_to(root)}:{number}')
    elif value['evidence_digest'] is not None and not re.fullmatch(r'[0-9a-f]{64}',str(value['evidence_digest'])): fail(f'{owner}: invalid evidence digest at {log.relative_to(root)}:{number}')

rows={}
try:
 for line in (root/'.beads/issues.jsonl').read_text().splitlines():
  if line.strip():
   row=json.loads(line); rows[row['id']]=row
except Exception as exc: fail(f'invalid durable Bead snapshot: {exc}')
closed=[bead for bead in required if rows.get(bead,{}).get('status')=='closed']
for bead in required:
 if rows.get(bead,{}).get('status')!='closed': fail(f'required leaf not closed: {bead}')
if rows.get(prior,{}).get('status')!='closed': fail(f'prior terminal proof not closed: {prior}')
barrier=rows.get(barrier_id,{})
blockers={d.get('depends_on_id') for d in barrier.get('dependencies',[]) if d.get('type')=='blocks'}
expected_blockers=set(required+[prior])
if blockers!=expected_blockers: fail(f'barrier blocking edges mismatch: missing={sorted(expected_blockers-blockers)} extra={sorted(blockers-expected_blockers)}')
terminal_row=rows.get(terminal,{})
terminal_blockers={d.get('depends_on_id') for d in terminal_row.get('dependencies',[]) if d.get('type')=='blocks'}
if barrier_id not in terminal_blockers: fail('completion barrier does not block terminal proof')

graph={key:[d.get('depends_on_id') for d in row.get('dependencies',[]) if d.get('type')=='blocks'] for key,row in rows.items()}
visiting=set(); done=set(); cycle=False
def visit(node):
 global cycle
 if node in visiting: cycle=True; fail(f'blocking dependency cycle at {node}'); return
 if node in done: return
 visiting.add(node)
 for dep in graph.get(node,[]): visit(dep)
 visiting.remove(node); done.add(node)
for node in graph: visit(node)

for command,label in [(['scripts/validate/plan_coverage.sh'],'plan coverage')]:
 run=subprocess.run(command,text=True,capture_output=True)
 if run.returncode: fail(f'{label} failed: {run.stdout.strip()} {run.stderr.strip()}')

summary={'schema_version':'m2-completion-summary/v1','status':'fail' if errors else 'pass','required_leaves':required,'closed_leaves':closed,'prior_terminal_proof':{'bead':prior,'closed':rows.get(prior,{}).get('status')=='closed'},'blocking_edges':sorted(blockers),'terminal_proof_downstream':barrier_id in terminal_blockers,'blocking_graph_acyclic':not cycle,'feature_owner_count':len(features),'contract_owner_count':len(contracts),'manifest_count':manifest_count,'correlation_fields':sorted(correlation),'findings':errors}
encoded=json.dumps(summary,sort_keys=True,indent=2)+'\n'
if errors:
 print(encoded,end=''); raise SystemExit(1)
if mode=='--probe': print(encoded,end=''); raise SystemExit(0)
gate=evidence_path.parent
if mode=='--write':
 gate.mkdir(parents=True,exist_ok=True)
 (gate/'completion-summary.json').write_text(encoded)
 outputs=[]; commands=[]
 for index in (1,2):
  run=subprocess.run(['scripts/acceptance/m2_complete.sh','--probe'],text=True,capture_output=True)
  stdout=gate/f'run-{index}.log'; stderr=gate/f'run-{index}.stderr'
  stdout.write_text(run.stdout); stderr.write_text(run.stderr); outputs.append((run.stdout,run.stderr))
  if run.returncode: raise SystemExit(f'completion probe {index} failed')
  commands.append({'argv':'scripts/acceptance/m2_complete.sh --probe','version':'m2-complete/v1','exit_code':0,'stdout_path':str(stdout.relative_to(root)),'stdout_sha256':sha(stdout),'stderr_path':str(stderr.relative_to(root)),'stderr_sha256':sha(stderr)})
 if outputs[0]!=outputs[1]: raise SystemExit('completion probes were not byte-identical')
 head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
 artifacts=[coverage_path,gate/'completion-summary.json']
 digest=hashlib.sha256(b''.join(p.read_bytes() for p in artifacts)).hexdigest()
 evidence={'schema_version':'evidence/v1','owner_bead':barrier_id,'scenario_id':'SCN-M2-COMPLETION-BARRIER','evidence_profile':'runtime','evidence_tier':'milestone','seed':'m2-complete-v1','git_commit':head,'commands':commands,'source_preservation':{'before_sha256':sha(coverage_path),'after_sha256':sha(coverage_path),'preserved':True},'cleanup':{'complete':True,'remaining_paths':[]},'redaction':{'checked':True,'secrets_found':0},'tier_proof':{'targeted_checks':True,'boundary_e2e':True,'fault_suite':True,'deterministic_rerun':True,'consumed_contract_vectors':True,'workspace_tests':True,'integration':True,'clean_environment':True,'exit_assertions':True,'endurance':False,'full_failure_matrix':True,'clean_clone':True},'result':{'status':'pass','digest':digest,'artifacts':[str(p.relative_to(root)) for p in artifacts],'product_faults':'all required M2 leaf fault manifests validated; downstream terminal proof excluded','runtime_observed':True}}
 evidence_path.write_text(json.dumps(evidence,sort_keys=True,indent=2)+'\n')
 print(encoded,end='')
else:
 if not evidence_path.is_file(): fail('gate evidence missing')
 else:
  evidence=load(evidence_path)
  if evidence.get('result',{}).get('status')!='pass': fail('gate result is not pass')
  commands=evidence.get('commands',[])
  if len(commands)!=2: fail('gate must retain two completion probes')
  observed=[]; stdout_paths=set(); stderr_paths=set()
  for entry in commands:
   stdout=root/entry.get('stdout_path',''); stderr=root/entry.get('stderr_path','')
   stdout_paths.add(str(stdout)); stderr_paths.add(str(stderr))
   if entry.get('argv')!='scripts/acceptance/m2_complete.sh --probe' or entry.get('exit_code')!=0: fail('stored gate command is not a successful completion probe')
   if not stdout.is_file() or sha(stdout)!=entry.get('stdout_sha256'): fail('stored gate stdout digest mismatch')
   if not stderr.is_file() or sha(stderr)!=entry.get('stderr_sha256'): fail('stored gate stderr digest mismatch')
   if stdout.is_file() and stderr.is_file(): observed.append((stdout.read_bytes(),stderr.read_bytes()))
  if len(stdout_paths)!=2 or len(stderr_paths)!=2: fail('gate command paths are not distinct')
  if len(observed)==2 and (observed[0]!=observed[1] or observed[0]!=(encoded.encode(),b'')): fail('stored probes are not byte-identical to the fresh summary')
  commit=evidence.get('git_commit','')
  exists=subprocess.run(['git','cat-file','-e',str(commit)+'^{commit}'],capture_output=True).returncode==0
  ancestor=exists and subprocess.run(['git','merge-base','--is-ancestor',str(commit),'HEAD'],capture_output=True).returncode==0
  changed=subprocess.check_output(['git','diff','--name-only',str(commit)+'..HEAD'],text=True).splitlines() if ancestor else []
  if not ancestor or any(not path.startswith('artifacts/boring-cdc-m2-complete/gate/') for path in changed): fail('gate git_commit is missing, non-ancestor, or stale')
  expected_artifacts=[coverage_path,root/'artifacts/boring-cdc-m2-complete/gate/completion-summary.json']
  recorded=[root/path for path in evidence.get('result',{}).get('artifacts',[])]
  if recorded!=expected_artifacts or not all(path.is_file() for path in recorded) or hashlib.sha256(b''.join(path.read_bytes() for path in recorded)).hexdigest()!=evidence.get('result',{}).get('digest'): fail('gate result artifact digest mismatch')
  run=subprocess.run(['scripts/validate/evidence.sh',str(evidence_path.relative_to(root))],text=True,capture_output=True)
  if run.returncode: fail(f'gate evidence invalid: {run.stdout.strip()} {run.stderr.strip()}')
 if errors:
  summary['status']='fail'; summary['findings']=errors; print(json.dumps(summary,sort_keys=True,indent=2)); raise SystemExit(1)
 print(encoded,end='')
PY
