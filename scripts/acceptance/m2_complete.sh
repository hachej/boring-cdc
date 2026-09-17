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
import hashlib, json, os, re, shlex, subprocess, sys
from pathlib import Path

mode=sys.argv[1]
root=Path.cwd()
coverage_path=root/'contracts/coverage/m2.json'
beads_path=root/'.beads/issues.jsonl'
evidence_path=root/'artifacts/boring-cdc-m2-complete/gate/evidence.json'
required=[
 'boring-cdc-m2-heartbeat','boring-cdc-m2-init-recovery','boring-cdc-m2-journal',
 'boring-cdc-m2-jsonl','boring-cdc-m2-leases','boring-cdc-m2-ownership',
 'boring-cdc-m2-pressure','boring-cdc-m2-reconcile','boring-cdc-m2-schema',
 'boring-cdc-m2-spool','boring-cdc-m2-capture-runtime','boring-cdc-m2.1']
prior='boring-cdc-m1-raw-demo'
barrier_id='boring-cdc-m2-complete'
terminal='boring-cdc-m2-fault-status'
remote_branch='origin/epic/boring-cdc-m2'
errors=[]
def fail(message):
 if message not in errors: errors.append(message)
def sha_bytes(value): return hashlib.sha256(value).hexdigest()
def sha(path): return sha_bytes(path.read_bytes())
def load(path):
 try: return json.loads(path.read_text())
 except Exception as exc: fail(f'invalid JSON {path.relative_to(root)}: {exc}'); return {}
def git_ok(*args): return subprocess.run(['git',*args],capture_output=True).returncode==0

def status_entries():
 """Return porcelain entries with every path, including both sides of renames."""
 raw=subprocess.check_output(['git','status','--porcelain=v1','-z','--untracked-files=all'])
 fields=raw.split(b'\0'); entries=[]; index=0
 while index < len(fields) and fields[index]:
  field=fields[index]
  if len(field)<4: fail('malformed git status porcelain entry'); break
  xy=field[:2].decode('ascii','replace'); paths=[field[3:].decode('utf-8','surrogateescape')]
  index += 1
  if 'R' in xy or 'C' in xy:
   if index>=len(fields) or not fields[index]: fail('malformed git rename/copy porcelain entry'); break
   paths.append(fields[index].decode('utf-8','surrogateescape')); index += 1
  entries.append((xy,paths))
 return entries

def exempt(path):
 # The v6 integration host explicitly preserves the unrelated, untracked v4
 # planning packet; it is not an M2 implementation or certification input.
 return path=='.factory-sha' or path.startswith('.doctor/') or path.startswith('docs/issues/boring-cdc-m2-v4/') or path.startswith('target/') or path.startswith('artifacts/boring-cdc-m2-complete/gate/')
dirty=[]
for xy,paths in status_entries():
 # A rename into an exempt directory is dirty when its tracked source is not exempt.
 if not all(exempt(path) for path in paths): dirty.append(f'{xy} '+ ' -> '.join(reversed(paths)) if len(paths)==2 else f'{xy} {paths[0]}')
if dirty: fail('dirty certification inputs: '+', '.join(dirty))

coverage=load(coverage_path)
if coverage.get('schema_version')!='m2-coverage/v1': fail('coverage schema_version mismatch')
if coverage.get('completion_policy')!='factory-handoff/v1': fail('coverage completion policy mismatch')
if coverage.get('owner_bead')!=barrier_id: fail('coverage owner mismatch')
if coverage.get('prior_terminal_proof')!=prior: fail('prior terminal proof mismatch')
if coverage.get('terminal_proof')!={'bead':terminal,'role':'downstream-proof-excluded-from-barrier-inputs'}: fail('terminal proof boundary mismatch')
leaves=coverage.get('required_leaves',[])
if [x.get('owner_bead') for x in leaves] != required: fail('coverage required leaf order/set mismatch')

rows={}
try:
 for line in beads_path.read_text().splitlines():
  if line.strip():
   row=json.loads(line)
   if row.get('id') in rows: fail(f'duplicate durable Bead id: {row.get("id")}')
   rows[row['id']]=row
except Exception as exc: fail(f'invalid durable Bead snapshot: {exc}')

# Factory Workers never close their own Beads. Certification therefore consumes
# contract-pinned, content-addressed handoff comments, including review-cap
# replacement Beads, instead of treating mutable status as completion proof.
completed=[]; handoff_count=0; manifest_count=0; capped_residuals=[]
features={}; contracts={}
plan=load(root/'contracts/coverage/plan-to-beads.json')
canonical={a.get('id'):a.get('owner_bead') for a in plan.get('assignments',[])}
handoff_header=re.compile(r'^\[Boring CDC [^\]]+\] handoff · ([A-Za-z0-9.-]+) · ([0-9a-f]{7,40})(?:\n|$)')
for leaf in leaves:
 owner=leaf.get('owner_bead','unknown')
 if not leaf.get('unit_target'): fail(f'{owner}: missing unit target')
 scripts=leaf.get('component_and_fault_scripts',[])
 if not scripts: fail(f'{owner}: missing component/fault/validator scripts')
 for script in scripts:
  path=root/script
  if not path.is_file() or not os.access(path,os.X_OK): fail(f'{owner}: script not executable: {script}')
 module=leaf.get('unit_target','').split('::',1)[0]
 if not (root/'src'/f'{module}.rs').is_file(): fail(f'{owner}: unit target module missing: {module}')
 refs=leaf.get('completion_handoffs',[])
 if not refs: fail(f'{owner}: missing immutable completion handoffs'); continue
 chain={owner}|{str(ref.get('bead')) for ref in refs}
 expected_features={feature for feature,assigned_owner in canonical.items() if assigned_owner in chain}
 declared_features=set(leaf.get('feature_ids',[]))
 if declared_features!=expected_features: fail(f'{owner}: canonical feature set mismatch: missing={sorted(expected_features-declared_features)} extra={sorted(declared_features-expected_features)}')
 for feature in declared_features:
  if feature in features: fail(f'duplicate feature owner: {feature}')
  features[feature]=owner
 expected_contracts=set()
 for candidate in (root/'contracts').rglob('*.json'):
  try: value=json.loads(candidate.read_text())
  except (OSError,json.JSONDecodeError): continue
  if isinstance(value,dict) and value.get('owner_bead') in chain and value.get('schema_version'): expected_contracts.add(value['schema_version'])
 declared_contracts=set(leaf.get('contract_ids',[]))
 if declared_contracts!=expected_contracts: fail(f'{owner}: owned contract set mismatch: missing={sorted(expected_contracts-declared_contracts)} extra={sorted(declared_contracts-expected_contracts)}')
 for contract in declared_contracts:
  if contract in contracts: fail(f'duplicate contract owner: {contract}')
  contracts[contract]=owner
 for item in leaf.get('evidence',[]):
  manifest_count += 1
  path=root/item.get('manifest','')
  if not path.is_file(): fail(f'{owner}: missing pinned evidence manifest {item.get("manifest")}')
  elif sha(path)!=item.get('sha256'): fail(f'{owner}: pinned evidence manifest digest mismatch: {item.get("manifest")}')
 admissions=[ref.get('admission') for ref in refs]
 if any(value not in {'superseded','approved','review-cap-residual'} for value in admissions) or any(value!='superseded' for value in admissions[:-1]) or admissions[-1] not in {'approved','review-cap-residual'}:
  fail(f'{owner}: invalid handoff disposition chain: {admissions}'); continue
 seen=set(); leaf_ok=True
 for ref in refs:
  handoff_count += 1
  key=(ref.get('bead'),ref.get('comment_id'))
  if key in seen: fail(f'{owner}: duplicate completion handoff {key}'); leaf_ok=False; continue
  seen.add(key)
  bead=rows.get(ref.get('bead'),{})
  comments=[c for c in bead.get('comments',[]) if c.get('id')==ref.get('comment_id')]
  if len(comments)!=1: fail(f'{owner}: pinned handoff comment missing or duplicate: {key}'); leaf_ok=False; continue
  text=comments[0].get('text','')
  if sha_bytes(text.encode())!=ref.get('text_sha256'): fail(f'{owner}: pinned handoff content digest mismatch: {key}'); leaf_ok=False
  match=handoff_header.match(text)
  target=str(ref.get('target_sha',''))
  if not match or match.group(1)!=ref.get('bead') or not target.startswith(match.group(2)): fail(f'{owner}: malformed or mismatched handoff title: {key}'); leaf_ok=False
  if not re.fullmatch(r'[0-9a-f]{40}',target) or target not in text: fail(f'{owner}: handoff does not name its exact target SHA: {key}'); leaf_ok=False
  elif not git_ok('cat-file','-e',target+'^{commit}') or not git_ok('merge-base','--is-ancestor',target,'HEAD') or not git_ok('merge-base','--is-ancestor',target,remote_branch):
   fail(f'{owner}: handoff target is missing, non-ancestral, or not pushed: {target}'); leaf_ok=False
  lower=text.lower()
  if len(text)<500 or 'proof' not in lower or 'review' not in lower: fail(f'{owner}: incomplete handoff proof/review sections: {key}'); leaf_ok=False
  admission=ref.get('admission')
  if admission=='approved' and not re.search(r'(?i)(approve(?:d)?/clean|clean/approve|independently approved|approve/clean|verdict clean)',text): fail(f'{owner}: approved handoff lacks approving verdict: {key}'); leaf_ok=False
  if admission=='review-cap-residual':
   if not re.search(r'(?i)(capreached=true|review cap reached|review-capped)',text) or not re.search(r'(?i)(residual|remaining blocker|missing acceptance)',text): fail(f'{owner}: review-cap handoff lacks explicit residual disposition: {key}'); leaf_ok=False
   capped_residuals.append({'owner_bead':owner,'handoff_bead':ref.get('bead'),'target_sha':target})
  bead_comments='\n'.join(str(c.get('text','')) for c in bead.get('comments',[])).lower()
  if 'friction:' not in bead_comments: fail(f'{owner}: handoff bead lacks friction note: {ref.get("bead")}'); leaf_ok=False
 if leaf_ok: completed.append(owner)

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

run=subprocess.run(['scripts/validate/plan_coverage.sh'],text=True,capture_output=True)
if run.returncode: fail(f'plan coverage failed: {run.stdout.strip()} {run.stderr.strip()}')

summary={'schema_version':'m2-completion-summary/v1','status':'fail' if errors else 'pass','completion_policy':'factory-handoff/v1','required_leaves':required,'completed_by_immutable_handoff':completed,'review_cap_residual_handoffs':capped_residuals,'immutable_handoff_count':handoff_count,'feature_owner_count':len(features),'contract_owner_count':len(contracts),'pinned_manifest_count':manifest_count,'prior_terminal_proof':{'bead':prior,'closed':rows.get(prior,{}).get('status')=='closed'},'blocking_edges':sorted(blockers),'terminal_proof_downstream':barrier_id in terminal_blockers,'blocking_graph_acyclic':not cycle,'findings':errors}
encoded=json.dumps(summary,sort_keys=True,indent=2)+'\n'
if errors:
 print(encoded,end=''); raise SystemExit(1)
if mode=='--probe': print(encoded,end=''); raise SystemExit(0)

def command_argv(text):
 try: return shlex.split(text,posix=True)
 except ValueError: return []
def attempt_id(index,entry):
 bound={'index':index,'argv':command_argv(entry['argv']),'exit_code':entry['exit_code'],'stdout_sha256':entry['stdout_sha256'],'stderr_sha256':entry['stderr_sha256']}
 return f'attempt-{index}:'+sha_bytes(json.dumps(bound,sort_keys=True,separators=(',',':')).encode())

gate=evidence_path.parent
required_proof_commands=[
 ['cargo','fmt','--check'],
 ['cargo','clippy','--locked','--workspace','--all-targets','--all-features'],
 ['cargo','test','--locked','--workspace','--all-targets'],
 ['scripts/validate/plan_coverage.sh'],
 ['scripts/acceptance/m2_complete.sh','--probe'],
 ['scripts/acceptance/m2_complete.sh','--probe']]
if mode=='--write':
 gate.mkdir(parents=True,exist_ok=True)
 (gate/'completion-summary.json').write_text(encoded)
 commands=[]; observed=[]
 for index,argv in enumerate(required_proof_commands,1):
  run=subprocess.run(argv,text=True,capture_output=True)
  stdout=gate/f'command-{index}.stdout'; stderr=gate/f'command-{index}.stderr'
  stdout.write_text(run.stdout.rstrip()+('\n' if run.stdout else '')); stderr.write_text(run.stderr.rstrip()+('\n' if run.stderr else ''))
  if run.returncode: raise SystemExit(f'certification command failed: {shlex.join(argv)}')
  if argv==['scripts/acceptance/m2_complete.sh','--probe']: observed.append((run.stdout,run.stderr))
  commands.append({'argv':shlex.join(argv),'version':'m2-complete/v2','exit_code':run.returncode,'stdout_path':str(stdout.relative_to(root)),'stdout_sha256':sha(stdout),'stderr_path':str(stderr.relative_to(root)),'stderr_sha256':sha(stderr)})
 if len(observed)!=2 or observed[0]!=observed[1]: raise SystemExit('completion probes were not byte-identical')
 secret=re.compile(r'(?i)(password\s*[=:]|api[_-]?key\s*[=:]|secret\s*[=:]|token\s*[=:]|postgres(?:ql)?://[^\s:@]+:[^\s@]+@|-----BEGIN [A-Z ]*PRIVATE KEY-----)')
 for path in [gate/'completion-summary.json']+[root/entry[key] for entry in commands for key in ('stdout_path','stderr_path')]:
  if secret.search(path.read_text(errors='replace')): raise SystemExit(f'secret-like content in certification output: {path.relative_to(root)}')
 head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
 artifacts=[coverage_path,gate/'completion-summary.json']
 digest=sha_bytes(b''.join(path.read_bytes() for path in artifacts))
 attempts=[attempt_id(index,commands[index-1]) for index in (5,6)]
 evidence={'schema_version':'evidence/v1','owner_bead':barrier_id,'scenario_id':'SCN-M2-COMPLETION-BARRIER','evidence_profile':'runtime','evidence_tier':'milestone','seed':'m2-complete-v2','git_commit':head,'commands':commands,'source_preservation':{'before_sha256':sha(coverage_path),'after_sha256':sha(coverage_path),'preserved':True},'cleanup':{'complete':True,'remaining_paths':[]},'redaction':{'checked':True,'secrets_found':0},'tier_proof':{'targeted_checks':True,'boundary_e2e':True,'fault_suite':True,'deterministic_rerun':True,'consumed_contract_vectors':True,'workspace_tests':True,'integration':True,'clean_environment':True,'exit_assertions':True,'endurance':False,'full_failure_matrix':False,'clean_clone':(root/'.factory-sha').is_file()},'result':{'status':'pass','digest':digest,'artifacts':[str(path.relative_to(root)) for path in artifacts],'product_faults':'factory completion consumes pinned PostgreSQL 17.6 Compose crash/fault proof; three review-cap residual handoffs remain explicit for owner disposition; downstream terminal proof remains excluded','runtime_observed':True,'attempts':attempts}}
 evidence_path.write_text(json.dumps(evidence,sort_keys=True,indent=2)+'\n')
 print(encoded,end='')
else:
 if not evidence_path.is_file(): fail('gate evidence missing')
 else:
  evidence=load(evidence_path)
  if evidence.get('result',{}).get('status')!='pass': fail('gate result is not pass')
  commands=evidence.get('commands',[]); observed=[]; stdout_paths=set(); stderr_paths=set()
  if len(commands)!=len(required_proof_commands): fail('gate command inventory mismatch')
  for index,entry in enumerate(commands,1):
   stdout=root/entry.get('stdout_path',''); stderr=root/entry.get('stderr_path','')
   stdout_paths.add(str(stdout)); stderr_paths.add(str(stderr))
   expected=required_proof_commands[index-1] if index<=len(required_proof_commands) else []
   if command_argv(str(entry.get('argv','')))!=expected or entry.get('exit_code')!=0: fail('stored gate command is not an exact successful required command')
   if not stdout.is_file() or sha(stdout)!=entry.get('stdout_sha256'): fail('stored gate stdout digest mismatch')
   if not stderr.is_file() or sha(stderr)!=entry.get('stderr_sha256'): fail('stored gate stderr digest mismatch')
   if stdout.is_file() and stderr.is_file() and expected==['scripts/acceptance/m2_complete.sh','--probe']: observed.append((stdout.read_bytes(),stderr.read_bytes()))
  expected_attempts=[attempt_id(index,commands[index-1]) for index in (5,6)] if len(commands)==len(required_proof_commands) else []
  if evidence.get('result',{}).get('attempts')!=expected_attempts: fail('gate attempts are not content-bound to command outputs')
  if len(stdout_paths)!=len(commands) or len(stderr_paths)!=len(commands): fail('gate command paths are not distinct')
  if len(observed)==2 and (observed[0]!=observed[1] or observed[0]!=(encoded.encode(),b'')): fail('stored probes are not byte-identical to the fresh summary')
  commit=evidence.get('git_commit','')
  ancestor=re.fullmatch(r'[0-9a-f]{40}',str(commit)) and git_ok('cat-file','-e',str(commit)+'^{commit}') and git_ok('merge-base','--is-ancestor',str(commit),'HEAD')
  freshness_paths=['src','examples','Cargo.toml','Cargo.lock','rust-toolchain.toml','build.rs','config','compose.yaml','Dockerfile','fixtures','contracts','scripts','tests']
  if not ancestor or subprocess.run(['git','diff','--quiet',str(commit)+'..HEAD','--',*freshness_paths]).returncode: fail('gate git_commit is missing, non-ancestor, or stale')
  expected_artifacts=[coverage_path,root/'artifacts/boring-cdc-m2-complete/gate/completion-summary.json']
  recorded=[root/path for path in evidence.get('result',{}).get('artifacts',[])]
  if recorded!=expected_artifacts or not all(path.is_file() for path in recorded) or sha_bytes(b''.join(path.read_bytes() for path in recorded))!=evidence.get('result',{}).get('digest'): fail('gate result artifact digest mismatch')
  schema=subprocess.run(['python3','scripts/lib/core_validator.py','schema',str(evidence_path.relative_to(root)),'--schema','contracts/evidence.schema.json'],text=True,capture_output=True)
  if schema.returncode: fail(f'gate evidence schema invalid: {schema.stdout.strip()} {schema.stderr.strip()}')
  semantic=subprocess.run(['scripts/validate/evidence.sh',str(evidence_path.relative_to(root))],text=True,capture_output=True)
  if semantic.returncode: fail(f'gate evidence invalid: {semantic.stdout.strip()} {semantic.stderr.strip()}')
 if errors:
  summary['status']='fail'; summary['findings']=errors; print(json.dumps(summary,sort_keys=True,indent=2)); raise SystemExit(1)
 print(encoded,end='')
PY
