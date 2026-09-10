#!/usr/bin/env python3
import hashlib,json,re,subprocess,sys
from pathlib import Path
root=Path(__file__).resolve().parents[2];src=(root/'src/m2_spool.rs').read_text();contract=json.loads((root/'contracts/m2/spool-cases.json').read_text());errors=[]
def digest():
 names=['src/m2_spool.rs','src/lib.rs','examples/m2_spool_component.rs','contracts/m2/spool-cases.json','scripts/lib/m2_spool_component.py','scripts/validate/m2_spool.py','scripts/e2e/m2_spool.sh','scripts/faults/m2_spool.sh'];h=hashlib.sha256()
 for name in names:
  raw=(root/name).read_bytes();h.update(len(name).to_bytes(8,'big'));h.update(name.encode());h.update(len(raw).to_bytes(8,'big'));h.update(raw)
 return h.hexdigest()
ids=[x['id'] for x in contract['cases']]
if len(ids)!=len(set(ids)):errors.append('duplicate spool case IDs')
for case in contract['cases']:
 test=case.get('test')
 if test and not re.search(r'fn\s+'+re.escape(test)+r'\s*\(',src):errors.append('missing test '+case['id'])
 if len(case.get('assertion',''))<50:errors.append('weak assertion '+case['id'])
for symbol in ('MemoryBudget','DiskAdmission','TxnBuffer','CommitIter','classify_startup_spools','failure_observation'):
 if symbol not in src:errors.append('missing production boundary '+symbol)
if '// M0-PROVISIONAL: boring-cdc-d-admission' not in src:errors.append('spool format provisional marker missing')
reconciliation=json.loads((root/'contracts/m2/m0-provisional-reconciliation.json').read_text())
if reconciliation['confirmed']['failure_policy']['version']!='failure-policy-v1' or reconciliation['confirmed']['sqlite_writer_busy_timeout_ms']!=5000:errors.append('confirmed M0 inputs changed')
head=subprocess.run(['git','rev-parse','HEAD'],cwd=root,text=True,capture_output=True).stdout.strip();implementation=digest();selected={'e2e':['SCN-M2-SPOOL-COMPONENT'],'fault':['SCN-M2-SPOOL-FAULTS'],'all':['SCN-M2-SPOOL-COMPONENT','SCN-M2-SPOOL-FAULTS']}.get(sys.argv[1] if len(sys.argv)>1 else 'all',[])
for scenario in selected:
 packet=root/'artifacts/boring-cdc-m2-spool'/scenario/'spool-component-v1'
 if not packet.exists():errors.append('missing packet '+scenario);continue
 manifest=json.loads((packet/'manifest.json').read_text());versions=json.loads((packet/'versions.json').read_text());evidence=manifest['git_commit']
 unchanged=subprocess.run(['git','merge-base','--is-ancestor',evidence,head],cwd=root).returncode==0 and subprocess.run(['git','diff','--quiet',evidence+'..'+head,'--','src/m2_spool.rs','src/lib.rs','examples/m2_spool_component.rs','contracts/m2/spool-cases.json','scripts/lib/m2_spool_component.py','scripts/validate/m2_spool.py','scripts/e2e/m2_spool.sh','scripts/faults/m2_spool.sh'],cwd=root).returncode==0
 if not unchanged:errors.append('packet not bound to unchanged implementation '+scenario)
 if manifest['source_preservation']['before_sha256']!=implementation or versions.get('implementation_sha256')!=implementation:errors.append('implementation digest mismatch '+scenario)
 binary=root/'target/debug/examples/m2_spool_component'
 if not binary.is_file() or versions.get('binary_sha256')!=hashlib.sha256(binary.read_bytes()).hexdigest():errors.append('binary provenance mismatch '+scenario)
 for line in (packet/'logs/boring-cdc.jsonl').read_text().splitlines():
  event=json.loads(line)
  for field in ('schema_version','case_event_seq','bead_id','scenario_id','correlation_id','run_id','capture_epoch','component','phase','outcome','config_fingerprint'):
   if event.get(field) in (None,''):errors.append(f'{scenario} missing {field}')
  text=json.dumps(event).lower()
  if any(x in text for x in ('postgres://','password=','canonical_key','dsn')):errors.append('redaction failure '+scenario)
for script,mode in [('scripts/e2e/m2_spool.sh','e2e'),('scripts/faults/m2_spool.sh','fault')]:
 if (root/script).read_text().count(f'm2_spool_component.py {mode}')!=2:errors.append(script+' must execute deterministic rerun')
print(json.dumps({'schema_version':'validation-result/v1','validator':'m2-spool/v1','valid':not errors,'findings':errors},sort_keys=True,separators=(',',':')));raise SystemExit(bool(errors))
