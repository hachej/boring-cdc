import importlib.util, json, sqlite3, tempfile, unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('storage_contract',ROOT/'scripts/validate/storage_contract.py'); m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
class StorageContractTests(unittest.TestCase):
 def test_contract(self): self.assertEqual([],m.validate()[0])
 def test_fixture_inventory(self):
  c=m.load(m.C); f=m.load(m.F); self.assertEqual(c['fixture_ids'],[x['fixture_id'] for x in f['cases']]); self.assertEqual(36,len(f['cases']))
 def test_memory_equation(self):
  c=m.load(m.C); a=c['admission']; x=a['runtime_memory']; self.assertEqual(x['required_runtime_memory_bytes'],x['fixed_runtime_overhead_budget']+x['max_copyboth_receive_buffer_bytes']+x['max_decoder_queue_bytes']+x['max_in_memory_event_staging_bytes']+x['max_sqlite_writer_staging_bytes']+a['limits']['backfill_worker_count']*a['limits']['max_backfill_worker_memory_bytes']+sum(x['destination_workers'].values())+x['telemetry_and_command_headroom_bytes'])
 def test_schema_executes_and_connection_attests(self):
  with tempfile.TemporaryDirectory() as td:
   con=sqlite3.connect(Path(td)/'x.db');
   for q in ('PRAGMA page_size=4096','PRAGMA auto_vacuum=INCREMENTAL','PRAGMA journal_mode=WAL','PRAGMA synchronous=FULL','PRAGMA foreign_keys=ON','PRAGMA trusted_schema=OFF','PRAGMA wal_autocheckpoint=0','PRAGMA temp_store=FILE','PRAGMA journal_size_limit=268435456','PRAGMA mmap_size=0','PRAGMA secure_delete=FAST'): con.execute(q)
   con.executescript(m.Q.read_text()); self.assertEqual(('wal',2,2,1,0,0,1,268435456,0,2),(con.execute('PRAGMA journal_mode').fetchone()[0],con.execute('PRAGMA synchronous').fetchone()[0],con.execute('PRAGMA auto_vacuum').fetchone()[0],con.execute('PRAGMA foreign_keys').fetchone()[0],con.execute('PRAGMA trusted_schema').fetchone()[0],con.execute('PRAGMA wal_autocheckpoint').fetchone()[0],con.execute('PRAGMA temp_store').fetchone()[0],con.execute('PRAGMA journal_size_limit').fetchone()[0],con.execute('PRAGMA mmap_size').fetchone()[0],con.execute('PRAGMA secure_delete').fetchone()[0]))
 def test_bootstrap_floor_is_distinct_from_durable_progress(self):
  with tempfile.TemporaryDirectory() as td:
   con=sqlite3.connect(Path(td)/'x.db')
   for q in ('PRAGMA page_size=4096','PRAGMA auto_vacuum=INCREMENTAL','PRAGMA journal_mode=WAL','PRAGMA synchronous=FULL','PRAGMA foreign_keys=ON','PRAGMA trusted_schema=OFF','PRAGMA wal_autocheckpoint=0','PRAGMA temp_store=FILE','PRAGMA journal_size_limit=268435456','PRAGMA mmap_size=0','PRAGMA secure_delete=FAST'): con.execute(q)
   con.executescript(m.Q.read_text())
   con.execute("INSERT INTO source_state VALUES(1,'source','slot','epoch',x'0000000000000001',NULL,NULL,'config')")
   self.assertEqual((None,None,bytes.fromhex('0000000000000001')),con.execute('SELECT durable_lsn,durable_seq,slot_creation_floor_lsn FROM source_state').fetchone())
   con.execute("INSERT INTO destinations(destination_id,kind,state,generation,highest_external_fence) VALUES('archive','archive','ready',1,41)")
   self.assertEqual(41,con.execute("SELECT highest_external_fence FROM destinations WHERE destination_id='archive'").fetchone()[0])
   con.execute("DELETE FROM source_state WHERE id=1")
   with self.assertRaisesRegex(sqlite3.IntegrityError,'durable_lsn IS NULL AND durable_seq IS NULL'): con.execute("INSERT INTO source_state VALUES(1,'source','slot','epoch',NULL,x'0000000000000002',NULL,'config')")
   with self.assertRaisesRegex(sqlite3.IntegrityError,'durable_lsn IS NULL AND durable_seq IS NULL'): con.execute("INSERT INTO source_state VALUES(1,'source','slot','epoch',NULL,NULL,2,'config')")
   con.execute("INSERT INTO writer_attestations VALUES('run',1,'backend','wal',2,2,1,0,0,1,268435456,0,2,1)")
   self.assertEqual((1,268435456,0,2),con.execute("SELECT temp_store,journal_size_limit,mmap_size,secure_delete FROM writer_attestations").fetchone())
 def test_frozen_corpus_and_manifest_bindings(self):
  import hashlib
  self.assertEqual(m.EXPECTED_CONTRACT_SHA256,hashlib.sha256(m.C.read_bytes()).hexdigest())
  self.assertEqual(m.EXPECTED_FIXTURES_SHA256,hashlib.sha256(m.F.read_bytes()).hexdigest())
  self.assertFalse([x for x in m.validate()[0] if x['code'].startswith('E_MANIFEST') or x['code'].startswith('E_ARTIFACT')])
 def test_evidence_parent_is_canonical_and_input_bound(self):
  inputs=m.validate()[1]; validator_sha=m.hashlib.sha256(m.V.read_bytes()).hexdigest()
  _,error=m.resolve_source_parent({'inputs':inputs,'validator_sha256':validator_sha,'source_parent_git_commit':'HEAD'},inputs,validator_sha)
  self.assertIn('canonical full lowercase commit OID',error)
  parent,error=m.resolve_source_parent({'inputs':{},'validator_sha256':validator_sha,'source_parent_git_commit':'0'*40},inputs,validator_sha)
  self.assertIsNone(error); self.assertEqual(m.subprocess.check_output(['git','rev-parse','HEAD'],cwd=m.ROOT,text=True).strip(),parent)
 def test_evidence_parent_hashes_recorded_commit_blobs(self):
  evidence=m.load(m.E); candidate=evidence['source_parent_git_commit']
  def commit_sha(path):
   blob=m.subprocess.check_output(['git','show',f'{candidate}:{path}'],cwd=m.ROOT)
   return m.hashlib.sha256(blob).hexdigest()
  inputs={path:commit_sha(path) for path in evidence['inputs']}
  validator_path=str(m.V.relative_to(m.ROOT)); validator_sha=commit_sha(validator_path)
  prior={'inputs':inputs,'validator_sha256':validator_sha,'source_parent_git_commit':candidate}
  parent,error=m.resolve_source_parent(prior,inputs,validator_sha)
  self.assertIsNone(error); self.assertEqual(candidate,parent)
  old=m.subprocess.check_output(['git','rev-parse',candidate+'^'],cwd=m.ROOT,text=True).strip()
  prior['source_parent_git_commit']=old
  _,error=m.resolve_source_parent(prior,inputs,validator_sha)
  self.assertRegex(error,r'(missing recorded blob|blob hash differs)')
  missing={**inputs,'contracts/storage/not-present.json':'0'*64}
  _,error=m.resolve_source_parent({'inputs':missing,'validator_sha256':validator_sha,'source_parent_git_commit':candidate},missing,validator_sha)
  self.assertIn('missing recorded blob contracts/storage/not-present.json',error)
 def test_provisional_inventory_is_card_bound(self):
  c=m.load(m.C); marker=lambda decision:f'// M0-PROVISIONAL: {decision}'
  self.assertEqual(c['authority']['owner_cards'],['59a63169'])
  self.assertEqual(set(c['provisional_markers']),{marker(x) for x in ('boring-cdc-d-admission','boring-cdc-d-compose')})
  self.assertNotIn(marker('boring-cdc-d-wal-cap'),c['admission']['provisional'])
  self.assertNotIn('provisional',c['sqlite'])
  self.assertEqual(c['ownership_commands']['provisional'],marker('boring-cdc-d-compose'))
  self.assertNotIn(marker('boring-cdc-d-sqlite'),m.Q.read_text())
 def test_accepted_sqlite_boundary_is_complete(self):
  c=m.load(m.C)['sqlite']; d=m.load(ROOT/'fixtures/m0/decisions/boring-cdc-d-sqlite.json')['confirmed_boundary']
  self.assertEqual({'busy_timeout_ms':d['pragmas']['busy_timeout_ms'],'foreign_keys':d['pragmas']['foreign_keys'],'journal_size_limit_bytes':d['pragmas']['journal_size_limit_bytes'],'mmap_size_bytes':d['pragmas']['mmap_size_bytes'],'secure_delete':d['pragmas']['secure_delete'],'synchronous':d['pragmas']['synchronous'],'temp_store':d['pragmas']['temp_store'],'trusted_schema':'OFF','wal_autocheckpoint_pages':d['pragmas']['wal_autocheckpoint_pages']},c['connection_pragmas'])
  self.assertEqual(d['attestation'],c['filesystem_attestation'])
  self.assertEqual({'auto_vacuum':2,'foreign_keys':1,'journal_mode':'wal','journal_size_limit':268435456,'mmap_size':0,'secure_delete':2,'synchronous':2,'temp_store':1,'trusted_schema':0,'wal_autocheckpoint':0},c['actual_connection_attestation']['required_observed_values'])
 def test_no_secrets(self):
  text=''.join(p.read_text() for p in (m.C,m.Q,m.F,m.FS,m.R)); self.assertNotIn('postgres'+'://',text); self.assertNotIn('password'+'=',text)
if __name__=='__main__': unittest.main()
