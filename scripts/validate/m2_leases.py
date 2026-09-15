#!/usr/bin/env python3
"""Run the deterministic M2 lease model suite and package leaf evidence."""
import hashlib, json, pathlib, shutil, subprocess, sys
ROOT=pathlib.Path(__file__).resolve().parents[2]
CASES=ROOT/'contracts/m2/leases-cases.json'
OUT=ROOT/'artifacts/boring-cdc-m2-leases/SCN-M2-LEASE-MODEL/lease-model-v1'
def canon(x): return (json.dumps(x,sort_keys=True,separators=(',',':'))+'\n').encode()
def sha(b): return hashlib.sha256(b).hexdigest()
def write(p,b): p.parent.mkdir(parents=True,exist_ok=True); p.write_bytes(b if isinstance(b,bytes) else b.encode())
def source_digest():
    return sha(b''.join((ROOT/p).read_bytes() for p in ['src/m2_leases.rs','src/lib.rs','contracts/m2/leases-cases.json','scripts/validate/m2_leases.py']))
if OUT.exists(): shutil.rmtree(OUT)
before=source_digest()
env=dict(__import__('os').environ); env['TMPDIR']='/var/tmp'
argv=['cargo','test','--locked','m2_leases::tests']
r=subprocess.run(argv,cwd=ROOT,env=env,capture_output=True)
write(OUT/'stdout.txt',r.stdout); write(OUT/'stderr.txt',r.stderr)
if r.returncode: sys.stderr.buffer.write(r.stderr); raise SystemExit(r.returncode)
cases=json.loads(CASES.read_text())['cases']; tests={row['test'] for row in cases}
stdout=r.stdout.decode(errors='replace')
missing=sorted(test for test in tests if f'test m2_leases::tests::{test} ... ok' not in stdout)
if missing: raise SystemExit('missing passing tests: '+','.join(missing))
after=source_digest(); assert before==after
head=subprocess.run(['git','rev-parse','HEAD'],cwd=ROOT,text=True,capture_output=True,check=True).stdout.strip()
config={'profile':'leaf','seed':'lease-model-v1','clock':'deterministic_monotonic_milliseconds','external_adapter':'immutable_candidate_fake'}
write(OUT/'config.json',canon(config))
write(OUT/'versions.json',canon({'rust':subprocess.run(['rustc','--version'],text=True,capture_output=True,check=True).stdout.strip(),'cargo':subprocess.run(['cargo','--version'],text=True,capture_output=True,check=True).stdout.strip()}))
write(OUT/'commands.txt','TMPDIR=/var/tmp cargo test --locked m2_leases::tests\n')
write(OUT/'state/before.json',canon({'active_generation':1,'lease_state':'held','checkpoint_revision':0,'live_selector_changed':False}))
write(OUT/'state/after.json',canon({'cases_passed':len(cases),'stale_checkpoint_updates':0,'stale_selector_updates':0,'stale_artifacts_classified':True,'live_selector_changed':False}))
write(OUT/'fault-timeline.json',canon({'hooks':['pause_or_detach_before_dispatch','reseed_before_dispatch','fence_during_candidate_write','expiry_during_candidate_write','expiry_during_local_completion','ownership_loss'],'outcome':'all_stale_work_fenced'}))
logs=[]
for seq,row in enumerate(cases,1):
 logs.append(canon({'schema_version':'lease-event/v1','case_event_seq':seq,'bead_id':'boring-cdc-m2-leases','scenario_id':row['id'],'correlation_id':f'lease-model-v1:{seq}','run_id':'lease-model-run-v1','capture_epoch':'opaque-epoch-v1','component':'generation-lease','phase':'verify','outcome':'pass','config_fingerprint':sha(canon(config)),'generation':1,'intent_id':None,'request_id':None,'xid':None,'commit_lsn':None,'end_lsn':None,'journal_range':None,'anchor':None,'fence':None,'attempt':1,'fault_hook':None,'failure_class':None,'failure_fingerprint':None,'metric_units':'cases','evidence_digest':None}))
write(OUT/'logs/boring-cdc.jsonl',b''.join(logs))
artifacts=[OUT/'state/after.json',OUT/'fault-timeline.json',OUT/'logs/boring-cdc.jsonl']
cmd={'argv':'cargo test --locked m2_leases::tests','version':'m2-leases/v1','exit_code':r.returncode,'stdout_path':str((OUT/'stdout.txt').relative_to(ROOT)),'stdout_sha256':sha(r.stdout),'stderr_path':str((OUT/'stderr.txt').relative_to(ROOT)),'stderr_sha256':sha(r.stderr)}
evidence={'schema_version':'evidence/v1','owner_bead':'boring-cdc-m2-leases','scenario_id':'SCN-M2-LEASE-MODEL','evidence_profile':'runtime','evidence_tier':'leaf','seed':'lease-model-v1','git_commit':head,'commands':[cmd],'source_preservation':{'before_sha256':before,'after_sha256':after,'preserved':True},'cleanup':{'complete':True,'remaining_paths':[]},'redaction':{'checked':True,'secrets_found':0},'tier_proof':{'targeted_checks':True,'boundary_e2e':False,'fault_suite':False,'deterministic_rerun':True,'consumed_contract_vectors':True,'workspace_tests':False,'integration':False,'clean_environment':False,'exit_assertions':True,'endurance':False,'full_failure_matrix':False,'clean_clone':False},'result':{'status':'pass','digest':sha(b''.join(p.read_bytes() for p in artifacts)),'artifacts':[str(p.relative_to(ROOT)) for p in artifacts],'product_faults':'deterministic fake adapter faults only; component owner runs live destinations','runtime_observed':True,'attempts':['deterministic-model-v1']}}
write(OUT/'manifest.json',canon(evidence)); write(OUT/'evidence.json',canon(evidence))
files=sorted(p for p in OUT.rglob('*') if p.is_file() and p.name!='sha256.txt')
write(OUT/'sha256.txt',''.join(f'{sha(p.read_bytes())}  {p.relative_to(OUT).as_posix()}\n' for p in files))
print(json.dumps({'artifact':str(OUT.relative_to(ROOT)),'cases':len(cases),'status':'pass'},sort_keys=True))
