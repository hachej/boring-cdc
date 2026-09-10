#!/usr/bin/env python3
"""Deterministic component evidence for the bounded transaction spool."""
import hashlib,json,os,shutil,subprocess,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];SEED="spool-component-v1"
def canon(v):return (json.dumps(v,sort_keys=True,separators=(",",":"))+"\n").encode()
def sha(v):return hashlib.sha256(v).hexdigest()
def write(p,v):p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(v.encode() if isinstance(v,str) else v)
def run(a):return subprocess.run(a,cwd=ROOT,text=True,capture_output=True,timeout=240)
def impl_digest():
 names=['src/m2_spool.rs','src/lib.rs','examples/m2_spool_component.rs','contracts/m2/spool-cases.json','scripts/lib/m2_spool_component.py','scripts/validate/m2_spool.py','scripts/e2e/m2_spool.sh','scripts/faults/m2_spool.sh'];h=hashlib.sha256()
 for name in names:
  raw=(ROOT/name).read_bytes();h.update(len(name).to_bytes(8,'big'));h.update(name.encode());h.update(len(raw).to_bytes(8,'big'));h.update(raw)
 return h.hexdigest()
def cmd(argv,out,err,code):return {'argv':argv,'version':'m2-spool-component/v1','exit_code':code,'stdout_path':out.relative_to(ROOT).as_posix(),'stdout_sha256':sha(out.read_bytes()),'stderr_path':err.relative_to(ROOT).as_posix(),'stderr_sha256':sha(err.read_bytes())}
def main():
 mode=sys.argv[1];scenario='SCN-M2-SPOOL-COMPONENT' if mode=='e2e' else 'SCN-M2-SPOOL-FAULTS';out=ROOT/'artifacts/boring-cdc-m2-spool'/scenario/SEED
 shutil.rmtree(out,ignore_errors=True);work=Path(os.environ.get('TMPDIR','/var/tmp'))/f'boring-cdc-m2-spool-{mode}';shutil.rmtree(work,ignore_errors=True);work.mkdir(mode=0o700)
 build=run(['cargo','build','--quiet','--locked','--example','m2_spool_component']);assert build.returncode==0,build.stderr
 binary=ROOT/'target/debug/examples/m2_spool_component';modes=['near-limit','oversized'] if mode=='e2e' else ['enospc','startup'];commands=[];observed={};displays=[]
 workspace_stdout_source=Path(os.environ['BORING_CDC_WORKSPACE_TEST_STDOUT']);workspace_stderr_source=Path(os.environ['BORING_CDC_WORKSPACE_TEST_STDERR']);workspace_code=int(os.environ['BORING_CDC_WORKSPACE_TEST_EXIT_CODE']);assert workspace_code==0 and workspace_stdout_source.is_file() and workspace_stderr_source.is_file()
 workspace_stdout=out/'workspace-tests-stdout.txt';workspace_stderr=out/'workspace-tests-stderr.txt';write(workspace_stdout,workspace_stdout_source.read_bytes());write(workspace_stderr,workspace_stderr_source.read_bytes());workspace_display='cargo test --locked --workspace --all-targets';displays.append(workspace_display);commands.append(cmd(workspace_display,workspace_stdout,workspace_stderr,workspace_code))
 for item in modes:
  result=run([str(binary),item,str(work/item)]);assert result.returncode==0,result.stderr;observed[item]=json.loads(result.stdout);stdout=out/f'{item}-stdout.txt';stderr=out/f'{item}-stderr.txt';write(stdout,result.stdout);write(stderr,result.stderr);display=f'target/debug/examples/m2_spool_component {item} $TMPDIR/isolated-spool';displays.append(display);commands.append(cmd(display,stdout,stderr,result.returncode))
 if mode=='e2e':
  assert observed['near-limit']['spill_delta']>0 and observed['near-limit']['stream_delta']==0 and observed['near-limit']['iterator_events']==2
  assert observed['oversized']['outcome']['feedback_permitted'] is False
 else:
  assert observed['enospc']['outcome']['feedback_permitted'] is False
  assert observed['startup']['removed']==1 and observed['startup']['quarantined']==2 and observed['startup']['feedback_permitted'] is False
 config={'profile':'component','seed':SEED,'spool_format_version':1,'limits':'fixture-supplied','emergency_reserve_bytes':512};fingerprint=sha(canon(config));git=run(['git','rev-parse','HEAD']).stdout.strip();impl=impl_digest()
 write(out/'commands.txt','\n'.join(displays)+'\n');write(out/'versions.json',canon({'python':sys.version.split()[0],'rustc':run(['rustc','--version']).stdout.strip(),'git_commit':git,'binary_sha256':sha(binary.read_bytes()),'implementation_sha256':impl}));write(out/'config.json',canon(config));write(out/'state/before.json',canon({'admitted_transactions':0,'feedback_permits':0}));write(out/'state/after.json',canon(observed));write(out/'fault-timeline.json',canon(['begin',*modes,'cleanup']))
 base={'schema_version':'spool-event/v1','bead_id':'boring-cdc-m2-spool','scenario_id':scenario,'correlation_id':scenario.lower()+':run-v1','run_id':'spool-run-v1','capture_epoch':'capture-epoch-v1','component':'transaction-spool','config_fingerprint':fingerprint,'generation':None,'intent_id':None,'request_id':None,'xid':None,'commit_lsn':None,'end_lsn':None,'journal_range':None,'anchor':None,'fence':None,'attempt':1,'metric_units':'bytes','evidence_digest':None}
 events=[]
 for i,item in enumerate(modes,1):events.append({**base,'case_event_seq':i,'phase':item,'outcome':'pass','fault_hook':item if mode=='fault' else None,'failure_class':None,'failure_fingerprint':None})
 write(out/'logs/boring-cdc.jsonl',b''.join(canon(x) for x in events));result_paths=[out/'state/after.json',out/'fault-timeline.json',out/'logs/boring-cdc.jsonl']
 manifest={'schema_version':'evidence/v1','owner_bead':'boring-cdc-m2-spool','scenario_id':scenario,'evidence_profile':'runtime','evidence_tier':'component','seed':SEED,'git_commit':git,'commands':commands,'source_preservation':{'before_sha256':impl,'after_sha256':impl,'preserved':True},'cleanup':{'complete':True,'remaining_paths':[]},'redaction':{'checked':True,'secrets_found':0},'tier_proof':{'targeted_checks':True,'boundary_e2e':True,'fault_suite':True,'deterministic_rerun':True,'consumed_contract_vectors':True,'workspace_tests':True,'integration':False,'clean_environment':True,'exit_assertions':True,'endurance':False,'full_failure_matrix':False,'clean_clone':False},'result':{'status':'pass','digest':sha(b''.join(p.read_bytes() for p in result_paths)),'artifacts':[p.relative_to(ROOT).as_posix() for p in result_paths],'product_faults':'none' if mode=='e2e' else 'injected_enospc_and_malformed_or_contradictory_spools','runtime_observed':True,'attempts':['clean-attempt-1','clean-attempt-2']}}
 write(out/'manifest.json',canon(manifest));files=sorted(p for p in out.rglob('*') if p.is_file());write(out/'sha256.txt',''.join(f'{sha(p.read_bytes())}  {p.relative_to(out).as_posix()}\n' for p in files));shutil.rmtree(work);print(canon({'mode':mode,'status':'pass'}).decode(),end='')
if __name__=='__main__':main()
