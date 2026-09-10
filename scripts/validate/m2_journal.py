#!/usr/bin/env python3
import hashlib,json,re,subprocess,sys
from pathlib import Path
root=Path(__file__).resolve().parents[2]; src=(root/'src/m2_journal.rs').read_text(); contract=json.loads((root/'contracts/m2/journal-cases.json').read_text()); errors=[]
def sha(data): return hashlib.sha256(data).hexdigest()
def implementation_digest():
 paths=['src/m2_journal.rs','src/m2_schema.rs','src/lib.rs','examples/m2_journal_component.rs','contracts/m2/journal-cases.json','scripts/lib/m2_journal_component.py','scripts/validate/m2_journal.py','scripts/lib/core_validator.py','scripts/e2e/m2_journal.sh','scripts/faults/m2_journal.sh']
 h=hashlib.sha256()
 for name in paths:
  raw=(root/name).read_bytes(); h.update(len(name).to_bytes(8,'big'));h.update(name.encode());h.update(len(raw).to_bytes(8,'big'));h.update(raw)
 return h.hexdigest()
ids=[c['id'] for c in contract['cases']]
if len(ids)!=len(set(ids)): errors.append('duplicate scenario IDs')
for case in contract['cases']:
 if not re.search(r'fn\s+'+re.escape(case['test'])+r'\s*\(',src): errors.append('missing test '+case['test'])
 assertion=case.get('assertion','')
 if len(assertion)<40 or 'journal-owned projection executes or fail-closes' in assertion: errors.append('non-specific assertion '+case['id'])
coverage=json.loads((root/'contracts/coverage/plan-to-beads.json').read_text())
required={a['id'] for a in coverage['assignments'] if a.get('owner_bead')=='boring-cdc-m2-journal'}
missing=required-set(ids)
if missing: errors.append('missing canonical assignments: '+','.join(sorted(missing)))
command_test='journal_command_boundaries_are_read_only_bounded_and_asserted'
for cid in ('CMD-JOURNAL-GC-DRY-RUN','CMD-JOURNAL-INSPECT-EVENT-ID-ID-EXPLAIN-JSON','CMD-JOURNAL-VERIFY'):
 rows=[c for c in contract['cases'] if c['id']==cid]
 if len(rows)!=1 or rows[0]['test']!=command_test: errors.append('command lacks executable boundary '+cid)
for symbol in ('journal_inspect_event','journal_verify','journal_gc_dry_run','complete_range_peak_accounts_for_simultaneous_live_data','saturated_real_writer_service_bounds_slow_capture_and_reserved_work'):
 if symbol not in src: errors.append('missing journal boundary '+symbol)
if 'WRITER_BUSY_TIMEOUT: Duration = Duration::from_secs(5)' not in (root/'src/m2_schema.rs').read_text(): errors.append('confirmed sqlite writer timeout changed')
head=subprocess.run(['git','rev-parse','HEAD'],cwd=root,text=True,capture_output=True).stdout.strip(); impl=implementation_digest()
scenario_filter=sys.argv[1] if len(sys.argv)>1 else 'all'
scenarios={'e2e':('SCN-M2-JOURNAL-COMPONENT',),'fault':('SCN-M2-JOURNAL-CRASH-BOUNDARY',),'all':('SCN-M2-JOURNAL-COMPONENT','SCN-M2-JOURNAL-CRASH-BOUNDARY')}
if scenario_filter not in scenarios: errors.append('unknown scenario filter '+scenario_filter); selected=()
else: selected=scenarios[scenario_filter]
for scenario in selected:
 packet=root/'artifacts/boring-cdc-m2-journal'/scenario/'journal-component-v1'
 if packet.exists():
  manifest=json.loads((packet/'manifest.json').read_text()); versions=json.loads((packet/'versions.json').read_text())
  evidence_commit=manifest['git_commit']
  ancestor=subprocess.run(['git','merge-base','--is-ancestor',evidence_commit,head],cwd=root).returncode==0
  bound=['src/m2_journal.rs','src/m2_schema.rs','src/lib.rs','examples/m2_journal_component.rs','contracts/m2/journal-cases.json','scripts/lib/m2_journal_component.py','scripts/validate/m2_journal.py','scripts/lib/core_validator.py','scripts/e2e/m2_journal.sh','scripts/faults/m2_journal.sh']
  source_unchanged=ancestor and subprocess.run(['git','diff','--quiet',evidence_commit+'..'+head,'--',*bound],cwd=root).returncode==0
  if not source_unchanged: errors.append(f'{scenario} not bound to reviewed implementation')
  if manifest['source_preservation']['before_sha256']!=impl or manifest['source_preservation']['after_sha256']!=impl: errors.append(f'{scenario} implementation digest mismatch')
  if versions.get('git_commit')!=evidence_commit or versions.get('implementation_sha256')!=impl: errors.append(f'{scenario} binary provenance source mismatch')
  binary=root/'target/debug/examples/m2_journal_component'
  if binary.exists() and versions.get('binary_sha256')!=sha(binary.read_bytes()): errors.append(f'{scenario} binary hash mismatch')
  events=[json.loads(x) for x in (packet/'logs/boring-cdc.jsonl').read_text().splitlines()]
  for i,event in enumerate(events,1):
   for field in ('schema_version','case_event_seq','bead_id','scenario_id','correlation_id','run_id','capture_epoch','component','phase','outcome','config_fingerprint'):
    if field not in event or event[field] in ('',None): errors.append(f'{scenario} event {i} missing {field}')
   forbidden=json.dumps(event).lower()
   for token in ('password=','postgres://','postgresql://','canonical_key','dsn'):
    if token in forbidden: errors.append(f'{scenario} event {i} contains forbidden {token}')
for script,mode in [('scripts/e2e/m2_journal.sh','e2e'),('scripts/faults/m2_journal.sh','fault')]:
 text=(root/script).read_text()
 if text.count(f'm2_journal_component.py {mode}')!=2: errors.append(f'{script} must run component probe twice')
print(json.dumps({'schema_version':'validation-result/v1','validator':'m2-journal/v2','valid':not errors,'findings':errors},sort_keys=True,separators=(',',':')))
raise SystemExit(bool(errors))
