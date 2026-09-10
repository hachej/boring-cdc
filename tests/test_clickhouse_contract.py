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
  es=[{'id':'1'*64,'hash':'a'*64,'key':'k','op':'update','version':[1,1,0,0,0,0,0],'cells':[[1,'unchanged_toast',25,-1,'']]}]
  with self.assertRaisesRegex(ValueError,'missing predecessor'): m.simulate(es)
 def test_fixture_scope(self):
  c=m.load(m.C); f=m.load(m.F); self.assertEqual(41,len(f['cases'])); self.assertEqual(c['fixture_ids'],[x['fixture_id'] for x in f['cases']])
 def test_no_provisional(self): self.assertNotIn('M0-'+'PROVISIONAL',''.join(p.read_text() for p in [m.C,m.S,m.F,m.FS,m.RS,m.DDL,m.Q,m.R,m.D]))
if __name__=='__main__':unittest.main()
