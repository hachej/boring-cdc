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
   for q in ('PRAGMA page_size=4096','PRAGMA auto_vacuum=INCREMENTAL','PRAGMA journal_mode=WAL','PRAGMA synchronous=FULL','PRAGMA foreign_keys=ON','PRAGMA trusted_schema=OFF','PRAGMA wal_autocheckpoint=0','PRAGMA temp_store=FILE'): con.execute(q)
   con.executescript(m.Q.read_text()); self.assertEqual(('wal',2,2,1,0,0,1),(con.execute('PRAGMA journal_mode').fetchone()[0],con.execute('PRAGMA synchronous').fetchone()[0],con.execute('PRAGMA auto_vacuum').fetchone()[0],con.execute('PRAGMA foreign_keys').fetchone()[0],con.execute('PRAGMA trusted_schema').fetchone()[0],con.execute('PRAGMA wal_autocheckpoint').fetchone()[0],con.execute('PRAGMA temp_store').fetchone()[0]))
 def test_bootstrap_floor_is_distinct_from_durable_progress(self):
  with tempfile.TemporaryDirectory() as td:
   con=sqlite3.connect(Path(td)/'x.db')
   for q in ('PRAGMA page_size=4096','PRAGMA auto_vacuum=INCREMENTAL','PRAGMA journal_mode=WAL','PRAGMA synchronous=FULL','PRAGMA foreign_keys=ON','PRAGMA trusted_schema=OFF','PRAGMA wal_autocheckpoint=0','PRAGMA temp_store=FILE'): con.execute(q)
   con.executescript(m.Q.read_text())
   con.execute("INSERT INTO source_state VALUES(1,'source','slot','epoch',x'0000000000000001',NULL,NULL,'config')")
   self.assertEqual((None,None,bytes.fromhex('0000000000000001')),con.execute('SELECT durable_lsn,durable_seq,slot_creation_floor_lsn FROM source_state').fetchone())
   con.execute("INSERT INTO destinations(destination_id,kind,state,generation,highest_external_fence) VALUES('archive','archive','ready',1,41)")
   self.assertEqual(41,con.execute("SELECT highest_external_fence FROM destinations WHERE destination_id='archive'").fetchone()[0])
 def test_frozen_corpus_and_manifest_bindings(self):
  import hashlib
  self.assertEqual(m.EXPECTED_CONTRACT_SHA256,hashlib.sha256(m.C.read_bytes()).hexdigest())
  self.assertEqual(m.EXPECTED_FIXTURES_SHA256,hashlib.sha256(m.F.read_bytes()).hexdigest())
  self.assertFalse([x for x in m.validate()[0] if x['code'].startswith('E_MANIFEST') or x['code'].startswith('E_ARTIFACT')])
 def test_no_secrets(self):
  text=''.join(p.read_text() for p in (m.C,m.Q,m.F,m.FS,m.R)); self.assertNotIn('postgres://',text); self.assertNotIn('password=',text)
if __name__=='__main__': unittest.main()
