import importlib.util,json,tempfile,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('m4',ROOT/'scripts/validate/m4_clickhouse_ddl.py'); m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
class M4ClickHouseDdlEvidenceTests(unittest.TestCase):
 def test_sealed_real_run(self): self.assertEqual([],m.validate())
 def test_validator_rejects_merge_drift(self):
  original=m.E
  try:
   with tempfile.TemporaryDirectory() as d:
    value=json.loads(original.read_text()); value['merge_invariant']['after_sha256']='0'*64
    m.E=Path(d)/'evidence.json'; m.E.write_text(json.dumps(value))
    self.assertIn('E_MERGE_INVARIANT',m.validate())
  finally: m.E=original
if __name__=='__main__': unittest.main()
