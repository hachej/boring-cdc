import hashlib,json,os,subprocess,tempfile,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]; CLI=ROOT/'scripts/lib/core_validator.py'; F=ROOT/'tests/fixtures/m0-core'; ZERO=hashlib.sha256(b'').hexdigest()
def run(*args): return subprocess.run(['python3',str(CLI),*map(str,args)],cwd=ROOT,text=True,capture_output=True)
def evidence(**overrides):
 d={'schema_version':'evidence/v1','owner_bead':'boring-cdc-m0.1','scenario_id':'SCN-M0-CORE','evidence_profile':'documentation','evidence_tier':'component','seed':'m0-core-v1','git_commit':'a'*40,'commands':[{'argv':'one','version':'1','exit_code':0,'stdout_sha256':ZERO,'stderr_sha256':ZERO},{'argv':'two','version':'1','exit_code':0,'stdout_sha256':ZERO,'stderr_sha256':ZERO}],'source_preservation':{'before_sha256':'b'*64,'after_sha256':'b'*64,'preserved':True},'cleanup':{'complete':True,'remaining_paths':[]},'redaction':{'checked':True,'secrets_found':0},'result':{'status':'pass','digest':'c'*64,'product_faults':'fault_not_applicable'}}
 d.update(overrides); return d
class Core(unittest.TestCase):
 def assertCode(self,cp,code):
  self.assertNotEqual(cp.returncode,0,cp.stdout+cp.stderr); self.assertIn(code,[x['code'] for x in json.loads(cp.stdout)['findings']])
 def test_valid_contracts(self):
  v=F/'valid'
  cases=[('decisions',v/'decisions.json','--complete','--owners',v/'owners.json','--fixtures',v/'fixtures.json','--executors',v/'executors.json'),('artifacts',v/'artifacts.json','--complete'),('runbooks',v/'runbooks.json','--release'),('graph',v/'graph.jsonl','--output',v/'normalized.tmp.json')]
  try:
   for c in cases:
    cp=run(*c); self.assertEqual(cp.returncode,0,cp.stdout+cp.stderr); self.assertEqual(json.loads(cp.stdout)['status'],'pass')
  finally: (v/'normalized.tmp.json').unlink(missing_ok=True)
 def test_empty_skeletons_valid_but_not_complete(self):
  self.assertEqual(run('decisions',ROOT/'contracts/m0/decisions.json').returncode,0)
  self.assertCode(run('decisions',ROOT/'contracts/m0/decisions.json','--complete'),'E_DECISIONS_EMPTY')
  self.assertEqual(run('artifacts',ROOT/'contracts/m0/artifacts.json').returncode,0)
  self.assertCode(run('artifacts',ROOT/'contracts/m0/artifacts.json','--complete'),'E_ARTIFACTS_EMPTY')
 def test_invalid_reason_codes_and_determinism(self):
  bad=F/'invalid'; cases=[(('decisions',bad/'decisions-duplicate.json'),'E_DUPLICATE_ID'),(('artifacts',bad/'artifact-traversal.json'),'E_PATH_TRAVERSAL'),(('runbooks',bad/'runbook-gap.json'),'E_PROCEDURE_GAP'),(('graph',bad/'graph-cycle.jsonl'),'E_GRAPH_CYCLE'),(('artifacts',bad/'duplicate-key.json'),'E_DUPLICATE_KEY')]
  for argv,code in cases:
   before=hashlib.sha256(Path(argv[1]).read_bytes()).hexdigest(); a=run(*argv); b=run(*argv); self.assertCode(a,code); self.assertEqual(a.stdout,b.stdout); self.assertEqual(before,hashlib.sha256(Path(argv[1]).read_bytes()).hexdigest())
 def test_evidence_profiles_tiers_redaction_and_cleanup(self):
  with tempfile.TemporaryDirectory(dir=ROOT/'tests') as td:
   p=Path(td)/'manifest.json'
   p.write_text(json.dumps(evidence())); self.assertEqual(run('evidence',p).returncode,0)
   bad=[(evidence(evidence_profile='future'),'E_EVIDENCE_PROFILE'),(evidence(cleanup={'complete':False,'remaining_paths':['x']}),'E_CLEANUP_INCOMPLETE'),(evidence(result={'status':'pass','digest':'c'*64,'product_faults':'tested'}),'E_FORWARD_RUNTIME_EVIDENCE'),(evidence(seed='password=bad'),'E_SECRET'),(evidence(evidence_tier='release',commands=[]),'E_COMMANDS_REQUIRED')]
   for doc,code in bad: p.write_text(json.dumps(doc)); self.assertCode(run('evidence',p),code)
 def test_graph_baseline_and_witness_mismatch(self):
  self.assertCode(run('graph',F/'invalid/graph-stale.jsonl','--baseline',F/'valid/graph.jsonl'),'E_GRAPH_MISSING')
  self.assertCode(run('graph',F/'invalid/graph-stale.jsonl','--baseline',F/'valid/graph.jsonl'),'E_GRAPH_EXTRA')
  self.assertCode(run('graph',F/'invalid/graph-stale.jsonl','--baseline',F/'valid/graph.jsonl'),'E_GRAPH_STALE')
  self.assertCode(run('graph',F/'valid/graph.jsonl','--witness-root','0'*64),'E_WITNESS_MISMATCH')
  lines=(F/'valid/graph.jsonl').read_text().splitlines()
  with tempfile.NamedTemporaryFile('w',dir=ROOT/'tests',delete=False) as f: f.write('\n'.join(reversed(lines))+'\n'); name=f.name
  try: self.assertEqual(run('graph',name).returncode,0)
  finally: Path(name).unlink()
 def test_real_decision_closure_fails_without_approvals(self):
  self.assertCode(run('decisions',F/'invalid/decisions-duplicate.json','--complete'),'E_DECISION_OPEN')
if __name__=='__main__': unittest.main()
