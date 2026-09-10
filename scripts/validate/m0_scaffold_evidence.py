#!/usr/bin/env python3
import hashlib,json,re,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"scripts"/"lib"))
from m0_scaffold import contains_sensitive_absolute_path,evidence_path,is_sha256
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def fail(msg):print(json.dumps({"status":"fail","finding":msg},sort_keys=True));raise SystemExit(1)
base=Path(sys.argv[1]) if len(sys.argv)>1 else ROOT/'artifacts/boring-cdc-m0-scaffold'
if not base.is_absolute():base=ROOT/base
fixtures=json.loads((ROOT/'fixtures/m0/scaffold/scenarios.json').read_text())['scenarios']; expected={x['id']:x for x in fixtures}
dirs={p.parent.parent.name:p.parent for p in base.glob('*/*/evidence.json')}
if set(dirs)!=set(expected):fail(f"scenario set mismatch: {sorted(dirs)}")
for ident,root in dirs.items():
 evidence=json.loads((root/'evidence.json').read_text());manifest=json.loads((root/'manifest.json').read_text());inventory=json.loads((root/'sha256.json').read_text());spec=expected[ident]
 if evidence['scenario_id']!=ident or manifest['scenario_id']!=ident:fail(f"scenario identity mismatch: {ident}")
 if manifest['specified_exit']!=spec['expected_exit'] or manifest['specified_outcome']!=spec['expected_status'] or manifest['observed_probe_exit']!=spec['expected_exit']:fail(f"unobserved expected outcome: {ident}")
 probes=[c for c in manifest['commands'] if ident in c['argv']]
 if len(probes)!=2 or any(p['exit_code']!=spec['expected_exit'] for p in probes):fail(f"probe command mismatch: {ident}")
 reruns=manifest.get('rerun_digests',[])
 if len(reruns)!=2 or reruns[0]!=reruns[1] or evidence['result'].get('attempts')!=reruns:fail(f"rerun mismatch: {ident}")
 binary=manifest.get('binary_sha256',''); cargo_lock=manifest.get('cargo_lock_sha256','')
 if not is_sha256(binary):fail(f"binary SHA-256 malformed: {ident}")
 if cargo_lock!=sha(ROOT/'Cargo.lock'):fail(f"Cargo.lock digest mismatch: {ident}")
 for command in evidence.get('commands',[]):
  for stream in ('stdout','stderr'):
   capture=evidence_path(root,command.get(f'{stream}_path'))
   if capture is None:fail(f"command {stream} path escape: {ident}")
   if not is_sha256(command.get(f'{stream}_sha256')) or sha(capture)!=command[f'{stream}_sha256']:
    fail(f"command {stream} digest mismatch: {ident}")
 binary_commands=[c for c in evidence.get('commands',[]) if c.get('argv')=='docker run --rm --entrypoint sha256sum <tested-connector-image> /usr/local/bin/boring-cdc']
 if len(binary_commands)!=2:fail(f"tested binary command mismatch: {ident}")
 for command in binary_commands:
  output=evidence_path(root,command.get('stdout_path'))
  if output is None or output.read_text().split()[0:1]!=[binary]:fail(f"tested binary digest mismatch: {ident}")
 exclusions=set(manifest.get('inventory_exclusions',[]))
 if exclusions!={'sha256.json','manifest.json','evidence.json'}:fail(f"inventory exclusion contract mismatch: {ident}")
 actual={str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}-exclusions
 if set(inventory)!=actual or manifest.get('artifact_hashes')!=inventory:fail(f"inventory completeness mismatch: {ident}")
 for rel,digest in inventory.items():
  p=(root/rel).resolve()
  try:p.relative_to(root.resolve())
  except (ValueError,TypeError):fail(f"inventory path escape: {ident}/{rel}")
  if not p.is_file() or sha(p)!=digest:fail(f"inventory mismatch: {ident}/{rel}")
 log=json.loads((root/'logs/boring-cdc.jsonl').read_text());required={'schema_version','case_event_seq','bead_id','scenario_id','correlation_id','run_id','capture_epoch','component','phase','outcome','config_fingerprint','evidence_digest'}
 if not required<=set(log) or log['scenario_id']!=ident or log['bead_id']!='boring-cdc-m0-scaffold' or log['outcome']!=spec['expected_status'] or log['run_id']!='00000000-0000-0000-0000-000000000001' or log['correlation_id']!='scaffold-component' or log['config_fingerprint']!=manifest['config_fingerprint'] or log['evidence_digest']!=sha(ROOT/'contracts/scaffold/m0-scaffold.json'):fail(f"log correlation mismatch: {ident}")
 text='\n'.join(p.read_text(errors='replace') for p in root.rglob('*') if p.is_file())
 if re.search(r'postgres(?:ql)?://[^\s:@]+:[^\s@]+@|-----BEGIN [A-Z ]*PRIVATE KEY-----|BORING_CDC_SOURCE_DSN|BORING_CDC_POSTGRES_PASSWORD_FILE',text,re.I) or contains_sensitive_absolute_path(text):fail(f"redaction failure: {ident}")
 if evidence['source_preservation']['before_sha256']!=evidence['source_preservation']['after_sha256'] or not evidence['cleanup']['complete']:fail(f"provenance claim mismatch: {ident}")
manifest_registry=json.loads((ROOT/'contracts/m0/manifest.json').read_text())['artifacts'];row=next(r for r in manifest_registry if r['id']=='ART-M0-SCAFFOLD');contract=json.loads((ROOT/'contracts/scaffold/m0-scaffold.json').read_text())
if len(row['executor_ids'])!=len(set(row['executor_ids'])) or row['executor_ids']!=contract['executors']:fail('executor registry mismatch')
beads={json.loads(x)['id'] for x in (ROOT/'.beads/issues.jsonl').read_text().splitlines() if x.strip()}
if not set(row['executor_ids'])<=beads:fail('dangling executor')
print(json.dumps({"status":"pass","scenarios":len(dirs),"inventories":"valid","reruns":"matched","redaction":"pass","executors":"resolved"},sort_keys=True))
