import copy, importlib.util, unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]; spec=importlib.util.spec_from_file_location('ch',ROOT/'scripts/validate/clickhouse_contract.py'); m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
class ClickHouseContractTests(unittest.TestCase):
 def test_contract(self): self.assertEqual([],m.validate()[0])
 def test_golden_semantics(self):
  f=m.load(m.F); self.assertEqual(f['golden_vectors']['expected_current_rows'],m.simulate(f['golden_vectors']['events']))
 def test_conflicting_id_blocks(self):
  es=copy.deepcopy(m.load(m.F)['golden_vectors']['events']); es.append({**es[0],'hash':'f'*64})
  with self.assertRaisesRegex(ValueError,'event conflict'): m.simulate(es)
 def test_missing_toast_predecessor_blocks(self):
  es=[{'capture_epoch':1,'generation':7,'logical_table_id':'8'*64,'journal_seq':100,'batch_id':'7'*64,'id':'1'*64,'hash':'a'*64,'key':'k','op':'update','mutation_kind':'upsert','version':[1,1,0,0,'1'*64],'cells':[[1,'unchanged_toast',25,-1,'']]}]
  with self.assertRaisesRegex(ValueError,'missing predecessor'): m.simulate(es)
 def test_absent_for_schema_projects_explicit_null(self):
  event={'capture_epoch':1,'generation':7,'logical_table_id':'8'*64,'journal_seq':100,'batch_id':'7'*64,'id':'1'*64,'hash':'a'*64,'key':'k','op':'insert','mutation_kind':'upsert','version':[1,1,0,0,'1'*64],'cells':[[3,'absent_for_schema',25,-1,'']]}
  self.assertEqual('explicit_null',m.simulate([event])[0]['columns'][0]['state'])
 def test_fixture_rows_are_ddl_complete_and_fault_pointers_resolve(self):
  f=m.load(m.F); required={'capture_epoch','generation','logical_table_id','journal_seq','batch_id','before_key','cells','hash','id','key','key_hash','mutation_kind','op','relation_schema_fingerprint','version'}
  marker_required={'capture_epoch','generation','batch_id','first_journal_seq','last_journal_seq','event_count','ordered_event_digest','object_fingerprint','finalized_at_unix_ms'}
  for case in f['cases']:
   rows=case['execution']['setup']['history_events']; markers=case['execution']['setup']['batch_markers']
   self.assertTrue(all(set(row)==required for row in rows))
   self.assertTrue(all(set(marker)==marker_required for marker in markers))
   self.assertTrue(all(marker['batch_id'] in {row['batch_id'] for row in rows} for marker in markers))
   m.resolve_pointer(case,case['execution']['fault']['arguments']['setup_pointer'])
 def test_marker_boundaries_and_counts_reject_mismatches(self):
  original=m.load(m.F)['cases'][0]
  for field,value in (('first_journal_seq',101),('last_journal_seq',103),('event_count',5)):
   with self.subTest(field=field):
    case=copy.deepcopy(original); case['execution']['setup']['batch_markers'][0][field]=value; findings=[]
    m.validate_marker_oracles(case,0,findings)
    self.assertIn('E_BATCH_MARKER_BOUNDARY',{finding['code'] for finding in findings})
 def test_checkpoint_oracle_rejects_nonfinalized_endpoint(self):
  original=m.load(m.F)['cases'][0]
  for checkpoint in ('advance_to_102','unchanged','invalid'):
   with self.subTest(checkpoint=checkpoint):
    case=copy.deepcopy(original); case['execution']['oracle']['checkpoint']=checkpoint; case['expected']['checkpoint']=checkpoint; findings=[]
    m.validate_marker_oracles(case,0,findings)
    self.assertIn('E_CHECKPOINT_ORACLE',{finding['code'] for finding in findings})
 def test_fixture_scope(self):
  c=m.load(m.C); f=m.load(m.F); self.assertEqual(41,len(f['cases'])); self.assertEqual(c['fixture_ids'],[x['fixture_id'] for x in f['cases']])
 def test_exact_provisional_inventory(self):
  self.assertEqual([],m.load(m.C)['provisional_markers'])
 def test_capture_epoch_is_direct_u64(self):
  self.assertEqual(3,m.DDL.read_text().count('capture_epoch UInt64'))
  self.assertNotIn('capture_epoch FixedString',m.DDL.read_text())
  self.assertIn('{capture_epoch:UInt64}',m.R.read_text())
  self.assertNotIn('capture_epoch:FixedString',m.R.read_text())
 def test_image_identity_matches_compose_index_and_platform(self):
  pins=m.load(m.C)['pins']; compose=m.load(m.ROOT/'contracts/m0/compose.json')['images']['clickhouse']
  self.assertEqual(f"{compose['repository']}:{compose['tag']}@{compose['index_digest']}",pins['image'])
  self.assertEqual(compose['platform_digest'],pins['image_platform_digest'])
if __name__=='__main__':unittest.main()
