import contextlib,hashlib,io,json,os,subprocess,tempfile,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
import sys;sys.path.insert(0,str(ROOT/'scripts/lib'))
import agent_context as ac
class ContextTests(unittest.TestCase):
 def cli(self,*a,ok=True,env=None):
  p=subprocess.run(a,cwd=ROOT,text=True,capture_output=True,env={**os.environ,**(env or {})})
  if ok:self.assertEqual(p.returncode,0,p.stderr)
  return p,json.loads(p.stdout)
 def test_registry_complete_unique_pending_and_owned(self):
  reg=json.loads((ROOT/'contracts/agent/stable-ids.json').read_text()); es=reg['entries']; ids=[e['id'] for e in es]
  self.assertEqual(len(ids),len(set(ids))); self.assertEqual(ac.validate(),[])
  counts={n:sum(e['namespace']==n for e in es) for n in set(e['namespace'] for e in es)}
  self.assertEqual(counts,{'REQ':144,'INV':20,'DEC':25,'CMD':26,'COND':6,'TRANS':6,'SCN':120,'REL':33,'RISK':38})
  self.assertTrue(all(e['evidence_status']=='pending' for e in es));self.assertNotIn('pass_digest',json.dumps(reg))
  self.assertTrue({'RUNBOOK','CLAIM','FINDING'} <= set(json.loads((ROOT/'contracts/agent/stable-ids.schema.json').read_text())['properties']['entries']['items']['properties']['namespace']['enum']))
 def test_all_interfaces_help_and_read_only(self):
  before={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in [ROOT/'.beads/issues.jsonl',ROOT/'docs/PLAN.md',ROOT/'docs/REQUIREMENTS.md']}
  for tool,args in [('doctor',[]),('next',[]),('context',['boring-cdc-m0.2']),('impact',['docs/PLAN.md'])]:
   p,x=self.cli(str(ROOT/'scripts/agent'/tool),*args,'--observed-at','2026-01-01T00:00:00Z');self.assertTrue(x['read_only'] if 'read_only' in x else True)
   hp=subprocess.run([str(ROOT/'scripts/agent'/tool),'--help'],cwd=ROOT,text=True,capture_output=True);self.assertEqual(hp.returncode,0)
  after={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in before};self.assertEqual(before,after)
 def test_context_complete_attachment_and_expansion(self):
  _,x=self.cli(str(ROOT/'scripts/agent/context'),'boring-cdc-m0.2','--observed-at','2026-01-01T00:00:00Z')
  self.assertLessEqual(x['summary_bytes'],16384);self.assertGreater(x['total_bytes'],x['summary_bytes']);self.assertTrue(x['attachments'][0]['complete']);self.assertIn('acceptance_criteria',x['attachments'][0]['content']);self.assertEqual(len(x['omitted_ids']),len(x['expansion_plan']))
  _,y=self.cli(str(ROOT/'scripts/agent/context'),'boring-cdc-m0.2','--expand','all','--observed-at','2026-01-01T00:00:00Z');self.assertEqual(y['attachments'][-1]['name'],'expansion')
 def test_no_decisions_closed_and_claim_index_absent(self):
  rows=ac.rows();dec=[r for r in rows if r.get('issue_type')=='decision'];self.assertEqual(len(dec),25);self.assertTrue(all(r['status']=='open' for r in dec))
  _,x=self.cli(str(ROOT/'scripts/agent/doctor'),'--observed-at','2026-01-01T00:00:00Z');self.assertEqual(x['claim_index'],'pending_unavailable');self.assertFalse(x['claim_reuse'])
 def test_source_change_reports_exact_stale_owner_without_rewriting_closed(self):
  old=ac.REG;self.addCleanup(setattr,ac,'REG',old)
  with tempfile.TemporaryDirectory() as td:
   p=Path(td)/'reg.json';r=ac.registry();r['source_files']['docs/PLAN.md']='0'*64;changed=next(e for e in r['entries'] if e['source']=='docs/PLAN.md' and e['owner_bead']=='boring-cdc-d-owner');changed['source_anchor']='synthetic changed canonical row';p.write_text(json.dumps(r));ac.REG=p
   out=io.StringIO();ns=type('N',(),{'target':'docs/PLAN.md'})()
   with contextlib.redirect_stdout(out):ac.cmd_impact(ns)
   x=json.loads(out.getvalue());self.assertEqual(x['stale_open_or_in_progress'],['boring-cdc-d-owner']);self.assertEqual(x['affected_ids'],[changed['id']]);self.assertIsInstance(x['historical_closed'],list);self.assertEqual(x['conflicts'][0]['owner_bead'],'boring-cdc-m0.2')
  ac.REG=old
 def test_hostile_duplicate_dangling_unknown_and_changed_source(self):
  old=ac.REG;self.addCleanup(setattr,ac,'REG',old)
  with tempfile.TemporaryDirectory() as td:
   p=Path(td)/'reg.json';r=ac.registry();r['schema_version']='unknown';r['entries'].append(dict(r['entries'][0]));r['entries'][-1]['owner_bead']='boring-cdc-missing';r['source_files']['docs/PLAN.md']='f'*64;p.write_text(json.dumps(r));ac.REG=p
   codes={x[0] for x in ac.validate()};self.assertTrue({'E_SCHEMA_VERSION','E_ID_DUPLICATE','E_OWNER_DANGLING','E_SOURCE_CONFLICT'}<=codes)
  ac.REG=old
 def test_profiles_are_distinct_and_budget_exact(self):
  names={}
  for profile in ['orient','implement','review','handoff']:
   _,x=self.cli(str(ROOT/'scripts/agent/context'),'boring-cdc-m0.2','--profile',profile,'--observed-at','2026-01-01T00:00:00Z');names[profile]={a['name'] for a in x['attachments']};self.assertEqual(x['total_bytes'],len(ac.canonical(x).encode()));self.assertEqual(x['token_estimate'],(x['total_bytes']+3)//4)
  self.assertIn('orientation',names['orient']);self.assertIn('dependency_outputs',names['implement']);self.assertIn('review_diff',names['review']);self.assertIn('handoff_state',names['handoff'])
 def test_world_state_binds_dirty_content_and_impact_rejects_escape(self):
  with tempfile.TemporaryDirectory(dir=ROOT/'tests') as td:
   p=Path(td)/'dirty';p.write_text('one');a=ac.world()['working_tree_digest'];p.write_text('two');b=ac.world()['working_tree_digest'];self.assertNotEqual(a,b)
  ns=type('N',(),{'target':'../../etc/passwd'})()
  with self.assertRaisesRegex(SystemExit,'E_PATH_TRAVERSAL'):ac.cmd_impact(ns)
 def test_deleted_registry_row_and_unknown_expansion_fail(self):
  old=ac.REG;self.addCleanup(setattr,ac,'REG',old)
  with tempfile.TemporaryDirectory() as td:
   p=Path(td)/'reg.json';r=ac.registry();r['entries'].pop();p.write_text(json.dumps(r));ac.REG=p
   self.assertIn('E_COVERAGE_INCOMPLETE',{x[0] for x in ac.validate()})
  ac.REG=old
  ns=type('N',(),{'bead':'boring-cdc-m0.2','profile':'implement','expand':'REQ-NOT-REAL','observed_at':'2026-01-01T00:00:00Z'})()
  with self.assertRaisesRegex(SystemExit,'E_EXPANSION_UNKNOWN'):ac.cmd_context(ns)
 def test_every_source_fragment_is_digest_bound_and_present(self):
  for e in ac.registry()['entries']:
   self.assertEqual(ac.digest_bytes(e['source_anchor'].encode()),e['source_digest']);self.assertIn(e['source_excerpt'],(ROOT/e['source']).read_text())
 def test_generated_view_drift_rejected(self):
  p,x=self.cli(str(ROOT/'scripts/validate/plan_coverage.sh'));self.assertTrue(x['valid'])
 def test_above_and_below_16k_beads_are_complete(self):
  old=ac.GRAPH
  with tempfile.TemporaryDirectory() as td:
   p=Path(td)/'issues.jsonl'; data=ac.rows(); base=dict(data[0]);base.update(id='boring-cdc-test-large',title='large',status='open',description='x'*20000,dependencies=[]);small=dict(base);small.update(id='boring-cdc-test-small',description='tiny')
   p.write_text('\n'.join(json.dumps(x) for x in data+[base,small])+'\n');ac.GRAPH=p
   for bead,size in [('boring-cdc-test-large',20000),('boring-cdc-test-small',4)]:
    out=io.StringIO();ns=type('N',(),{'bead':bead,'profile':'implement','expand':None,'observed_at':'2026-01-01T00:00:00Z'})()
    with contextlib.redirect_stdout(out):ac.cmd_context(ns)
    x=json.loads(out.getvalue());self.assertLessEqual(x['summary_bytes'],16384);self.assertEqual(len(x['attachments'][0]['content']['description']),size)
  ac.GRAPH=old
 def test_secret_bearing_bead_fails_closed(self):
  old=ac.GRAPH
  with tempfile.TemporaryDirectory() as td:
   p=Path(td)/'issues.jsonl';data=ac.rows();bad=dict(data[0]);bad.update(id='boring-cdc-test-secret',description='postgres://user:supersecret@example.invalid/db',dependencies=[]);p.write_text('\n'.join(json.dumps(x) for x in data+[bad])+'\n');ac.GRAPH=p
   ns=type('N',(),{'bead':'boring-cdc-test-secret','profile':'implement','expand':None,'observed_at':'2026-01-01T00:00:00Z'})()
   with self.assertRaisesRegex(SystemExit,'E_SECRET_DETECTED'):ac.cmd_context(ns)
  ac.GRAPH=old
 def test_deterministic_outputs(self):
  cmd=[str(ROOT/'scripts/agent/context'),'boring-cdc-m0.2','--observed-at','2026-01-01T00:00:00Z'];a=subprocess.run(cmd,cwd=ROOT,capture_output=True).stdout;b=subprocess.run(cmd,cwd=ROOT,capture_output=True).stdout;self.assertEqual(a,b)
if __name__=='__main__':unittest.main()
