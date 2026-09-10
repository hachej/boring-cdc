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
   for q in ('PRAGMA page_size=4096','PRAGMA auto_vacuum=INCREMENTAL','PRAGMA journal_mode=WAL','PRAGMA synchronous=FULL','PRAGMA foreign_keys=ON','PRAGMA trusted_schema=OFF','PRAGMA wal_autocheckpoint=0'): con.execute(q)
   con.executescript(m.Q.read_text()); self.assertEqual(('wal',2,2,1,0,0),(con.execute('PRAGMA journal_mode').fetchone()[0],con.execute('PRAGMA synchronous').fetchone()[0],con.execute('PRAGMA auto_vacuum').fetchone()[0],con.execute('PRAGMA foreign_keys').fetchone()[0],con.execute('PRAGMA trusted_schema').fetchone()[0],con.execute('PRAGMA wal_autocheckpoint').fetchone()[0]))
 def test_no_secrets(self):
  text=''.join(p.read_text() for p in (m.C,m.Q,m.F,m.R)); self.assertNotIn('postgres://',text); self.assertNotIn('password=',text)
if __name__=='__main__': unittest.main()
