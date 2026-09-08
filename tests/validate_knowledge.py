import copy,hashlib,json,subprocess,tempfile,unittest
from pathlib import Path
R=Path(__file__).resolve().parents[1]; C=R/'scripts/lib/knowledge_validator.py'; F=R/'tests/fixtures/m0-knowledge/valid'
def run(kind,path,*args):return subprocess.run(['python3',str(C),kind,str(path),*map(str,args)],cwd=R,text=True,capture_output=True)
def codes(cp):return [x['code'] for x in json.loads(cp.stdout)['findings']]
class Knowledge(unittest.TestCase):
 def assertCode(self,cp,code):self.assertNotEqual(cp.returncode,0,cp.stdout+cp.stderr);self.assertIn(code,codes(cp));self.assertFalse(cp.stderr)
 def claim(self,actual='actual-exact.json',index=True,compat=True):
  a=['--actual',F/actual,'--index',F/'claim-index.json','--baseline-index',F/'claim-index-baseline.json','--owners',F/'owners.json','--claim-id','CLAIM-M0-KNOWLEDGE-EXACT','--observed-at','2026-01-01T00:00:00Z']
  if not index:
   del a[a.index('--index'):a.index('--index')+2]
  if compat:a+=['--compatibility',F/'compatibility.json']
  return run('claims',F/'claims.json',*a)
 def test_valid_exact_and_compatible(self):
  # each mode is evaluated against an input it explicitly admits
  d=json.loads((F/'claims.json').read_text())
  with tempfile.TemporaryDirectory(dir=R/'tests') as td:
   p=Path(td)/'one.json';idx=json.loads((F/'claim-index.json').read_text())
   for n,actual in ((0,'actual-exact.json'),(1,'actual-compatible.json')):
    p.write_text(json.dumps({'schema_version':'claims/v1','claims':[d['claims'][n]]}));ip=Path(td)/'idx.json';ip.write_text(json.dumps({'schema_version':'claim-index/v1','entries':[idx['entries'][n]]}))
    cp=run('claims',p,'--index',ip,'--baseline-index',F/'claim-index-baseline.json','--owners',F/'owners.json','--claim-id',d['claims'][n]['claim_id'],'--actual',F/actual,'--compatibility',F/'compatibility.json');self.assertEqual(cp.returncode,0,cp.stdout)
 def test_absent_index_and_exact_mismatch(self):
  self.assertCode(self.claim(index=False),'E_CLAIM_INDEX_REQUIRED');self.assertCode(self.claim('actual-compatible.json'),'E_CLAIM_INPUT_MISMATCH')
 def test_forged_owner_unowned_range_hash_and_provenance(self):
  doc=json.loads((F/'claims.json').read_text());idx=json.loads((F/'claim-index.json').read_text());comp=json.loads((F/'compatibility.json').read_text())
  with tempfile.TemporaryDirectory(dir=R/'tests') as td:
   p=Path(td)/'x.json';q=Path(td)/'i.json';k=Path(td)/'c.json'
   idx['entries'][0]['owner_bead']='boring-cdc-forged';q.write_text(json.dumps(idx));self.assertCode(run('claims',F/'claims.json','--index',q,'--baseline-index',F/'claim-index-baseline.json','--owners',F/'owners.json','--claim-id','CLAIM-M0-KNOWLEDGE-EXACT','--actual',F/'actual-exact.json','--compatibility',F/'compatibility.json'),'E_CLAIM_OWNER_FORGED')
   comp['predicates']=[];k.write_text(json.dumps(comp));self.assertCode(run('claims',F/'claims.json','--index',F/'claim-index.json','--baseline-index',F/'claim-index-baseline.json','--owners',F/'owners.json','--claim-id','CLAIM-M0-KNOWLEDGE-COMPATIBLE','--actual',F/'actual-compatible.json','--compatibility',k),'E_COMPATIBILITY_UNOWNED')
   doc['claims'][0]['bindings'].pop('artifact_digest');p.write_text(json.dumps(doc));self.assertCode(run('claims',p,'--index',F/'claim-index.json','--baseline-index',F/'claim-index-baseline.json','--owners',F/'owners.json','--claim-id','CLAIM-M0-KNOWLEDGE-EXACT','--actual',F/'actual-exact.json','--compatibility',F/'compatibility.json'),'E_REQUIRED')
 def test_superseded_freshness_and_rewritten_history(self):
  doc=json.loads((F/'claims.json').read_text());doc['claims'][1]['supersedes']=['CLAIM-M0-KNOWLEDGE-EXACT'];doc['claims'][0]['applicability']={'mode':'exact','freshness':'fresh_until','fresh_until':'2025-01-01T00:00:00Z'}
  idx=json.loads((F/'claim-index.json').read_text())
  for i,row in enumerate(doc['claims']):idx['entries'][i]['claim_sha256']=hashlib.sha256(json.dumps(row,sort_keys=True,separators=(',',':')).encode()).hexdigest()
  actual=json.loads((F/'actual-compatible.json').read_text())
  with tempfile.TemporaryDirectory(dir=R/'tests') as td:
   p=Path(td)/'x.json';a=Path(td)/'a.json';ip=Path(td)/'index.json';p.write_text(json.dumps(doc));a.write_text(json.dumps(actual));ip.write_text(json.dumps(idx))
   successor=run('claims',p,'--index',ip,'--baseline-index',F/'claim-index-baseline.json','--owners',F/'owners.json','--claim-id','CLAIM-M0-KNOWLEDGE-COMPATIBLE','--actual',a,'--compatibility',F/'compatibility.json','--observed-at','2026-01-01T00:00:00Z');self.assertEqual(successor.returncode,0,successor.stdout)
   old=run('claims',p,'--index',ip,'--baseline-index',F/'claim-index-baseline.json','--owners',F/'owners.json','--claim-id','CLAIM-M0-KNOWLEDGE-EXACT','--actual',a,'--compatibility',F/'compatibility.json','--observed-at','2026-01-01T00:00:00Z');self.assertCode(old,'E_CLAIM_SUPERSEDED');self.assertCode(old,'E_CLAIM_STALE')
   actual['git_ancestry']=False;a.write_text(json.dumps(actual));self.assertCode(run('claims',p,'--index',ip,'--baseline-index',F/'claim-index-baseline.json','--owners',F/'owners.json','--claim-id','CLAIM-M0-KNOWLEDGE-COMPATIBLE','--actual',a,'--compatibility',F/'compatibility.json'),'E_HISTORY_REWRITTEN')
 def test_findings_append_only_and_hypothesis_separation(self):
  self.assertEqual(run('findings',F/'findings.jsonl','--baseline',F/'findings.jsonl').returncode,0)
  row=json.loads((F/'findings.jsonl').read_text());row['kind']='hypothesis';row['hypothesis']='guess';row['observation']='promoted guess'
  with tempfile.TemporaryDirectory(dir=R/'tests') as td:
   p=Path(td)/'x.jsonl';p.write_text(json.dumps(row)+'\n'+json.dumps(row)+'\n');cp=run('findings',p,'--baseline',F/'findings.jsonl');self.assertCode(cp,'E_HYPOTHESIS_PROMOTION');self.assertCode(cp,'E_FINDING_REWRITE')
 def test_handoff_redaction_ambiguity_and_separation(self):
  self.assertEqual(run('handoff',F/'handoff.json').returncode,0)
  h=json.loads((F/'handoff.json').read_text());h['facts']=h['hypotheses'];h['intents']['ambiguous']=['external write unknown'];h['unsafe_repeats']=[];h['observations']=['postgresql://user:pw@host/db']
  with tempfile.TemporaryDirectory(dir=R/'tests') as td:
   p=Path(td)/'x.json';p.write_text(json.dumps(h));cp=run('handoff',p)
   for c in ('E_HYPOTHESIS_PROMOTION','E_UNSAFE_REPEAT_REQUIRED','E_SECRET'):self.assertCode(cp,c)
 def test_cross_input_mismatch_matrix(self):
  actual=json.loads((F/'actual-exact.json').read_text())
  with tempfile.TemporaryDirectory(dir=R/'tests') as td:
   p=Path(td)/'actual.json'
   for field in ('capture_epoch_digest','fixture_digest','config_digest','image_digest','environment_digest'):
    bad=dict(actual);bad[field]='0'*64;p.write_text(json.dumps(bad));cp=run('claims',F/'claims.json','--index',F/'claim-index.json','--baseline-index',F/'claim-index-baseline.json','--owners',F/'owners.json','--claim-id','CLAIM-M0-KNOWLEDGE-EXACT','--actual',p,'--compatibility',F/'compatibility.json');self.assertCode(cp,'E_CLAIM_INPUT_MISMATCH');self.assertTrue(any(x['pointer'].endswith('/bindings/'+field) for x in json.loads(cp.stdout)['findings']))
 def test_index_and_finding_baseline_rewrites(self):
  idx=json.loads((F/'claim-index.json').read_text());idx['entries'][0]['claim_sha256']='0'*64
  row=json.loads((F/'findings.jsonl').read_text());row['observation']='rewritten history'
  with tempfile.TemporaryDirectory(dir=R/'tests') as td:
   i=Path(td)/'index.json';i.write_text(json.dumps(idx));self.assertCode(run('claims',F/'claims.json','--index',i,'--baseline-index',F/'claim-index.json','--owners',F/'owners.json','--claim-id','CLAIM-M0-KNOWLEDGE-EXACT','--actual',F/'actual-exact.json','--compatibility',F/'compatibility.json'),'E_INDEX_REWRITTEN')
   f=Path(td)/'findings.jsonl';f.write_text(json.dumps(row)+'\n');self.assertCode(run('findings',f,'--baseline',F/'findings.jsonl'),'E_FINDING_REWRITE')
 def test_malformed_check_buckets_and_redaction_bypasses(self):
  base=json.loads((F/'handoff.json').read_text())
  with tempfile.TemporaryDirectory(dir=R/'tests') as td:
   p=Path(td)/'handoff.json'
   for bucket,result in (('completed','fail'),('failed','pass'),('stale','pass')):
    h=copy.deepcopy(base);h['checks'][bucket]=[{'command':'x','result':result,'digest':'1'*64}];p.write_text(json.dumps(h));self.assertCode(run('handoff',p),'E_CHECK_BUCKET')
   for leak in ('/etc/boring/config','config=/etc/boring/private.json','inspect(/etc/boring/private.json)','root is /','tab\t/etc/private','mysql://host/db','raw payload bytes','driver error detail'):
    h=copy.deepcopy(base);h['observations']=[leak];p.write_text(json.dumps(h));self.assertCode(run('handoff',p),'E_SECRET')
 def test_malformed_owned_documents_never_crash(self):
  with tempfile.TemporaryDirectory(dir=R/'tests') as td:
   p=Path(td)/'bad.json';p.write_text('[]')
   for args in (("claims",F/'claims.json','--index',p,'--baseline-index',F/'claim-index-baseline.json','--owners',F/'owners.json','--claim-id','CLAIM-M0-KNOWLEDGE-EXACT','--actual',F/'actual-exact.json'),("claims",F/'claims.json','--index',F/'claim-index.json','--baseline-index',F/'claim-index-baseline.json','--owners',F/'owners.json','--claim-id','CLAIM-M0-KNOWLEDGE-COMPATIBLE','--actual',F/'actual-compatible.json','--compatibility',p),("handoff",p)):
    cp=run(*args);self.assertNotEqual(cp.returncode,0);json.loads(cp.stdout);self.assertFalse(cp.stderr)
   for field,value in (("hypotheses",7),("facts",7),("log_references",{}),("intents",[])):
    h=json.loads((F/'handoff.json').read_text());h[field]=value;p.write_text(json.dumps(h));cp=run('handoff',p);self.assertNotEqual(cp.returncode,0);json.loads(cp.stdout);self.assertFalse(cp.stderr)
 def test_canonical_schema_hostile_inputs(self):
  with tempfile.TemporaryDirectory(dir=R/'tests') as td:
   p=Path(td)/'hostile.json'
   cases=[]
   idx=json.loads((F/'claim-index.json').read_text());idx['entries'][0]['status']='current';cases.append(('claims',idx,'--index'))
   baseline=json.loads((F/'claim-index-baseline.json').read_text());baseline['entries']=7;cases.append(('baseline',baseline,'--baseline-index'))
   compat=json.loads((F/'compatibility.json').read_text());compat['predicates'][0]['status']='current';cases.append(('compatibility-field',compat,'--compatibility'))
   compat_empty=json.loads((F/'compatibility.json').read_text());compat_empty['predicates'][0]['allowed_values']['git_commit']=[];cases.append(('allowed-values',compat_empty,'--compatibility'))
   for name,document,option in cases:
    p.write_text(json.dumps(document));args=['claims',F/'claims.json','--index',F/'claim-index.json','--baseline-index',F/'claim-index-baseline.json','--owners',F/'owners.json','--claim-id','CLAIM-M0-KNOWLEDGE-COMPATIBLE','--actual',F/'actual-compatible.json','--compatibility',F/'compatibility.json'];args[args.index(option)+1]=p
    cp=run(*args);self.assertNotEqual(cp.returncode,0,name);json.loads(cp.stdout);self.assertFalse(cp.stderr,name);self.assertTrue(any(c.startswith('E_SCHEMA_') for c in codes(cp)),cp.stdout)
   finding=json.loads((F/'findings.jsonl').read_text());finding['owner_bead']='not/a/bead';p.write_text(json.dumps(finding)+'\n');cp=run('findings',p,'--baseline',p);self.assertCode(cp,'E_SCHEMA_PATTERN')
 def test_hostile_paths_and_determinism(self):
  for kind,name in (('handoff','handoff.json'),('findings','findings.jsonl')):
   raw=(F/name).read_text().replace('synthetic validator','/home/alice/private') if kind=='handoff' else (F/name).read_text().replace('Validator fails','token=abc Validator fails')
   with tempfile.TemporaryDirectory(dir=R/'tests') as td:
    p=Path(td)/name;p.write_text(raw);a=run(kind,p,*(['--baseline',F/'findings.jsonl'] if kind=='findings' else []));b=run(kind,p,*(['--baseline',F/'findings.jsonl'] if kind=='findings' else []));self.assertCode(a,'E_SECRET');self.assertEqual(a.stdout,b.stdout);self.assertEqual(hashlib.sha256(raw.encode()).hexdigest(),hashlib.sha256(p.read_bytes()).hexdigest())
if __name__=='__main__':unittest.main()
