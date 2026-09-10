import hashlib, importlib.util, json, unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('archive_contract',ROOT/'scripts/validate/archive_contract.py'); m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
class ArchiveContractTests(unittest.TestCase):
 def test_contract_is_valid(self): self.assertEqual([],m.validate()[0])
 def test_fixture_inventory_and_branches(self):
  c=m.load(m.C); f=m.load(m.F); self.assertEqual(c['fixture_ids'],[x['fixture_id'] for x in f['cases']]); self.assertEqual(41,len(f['cases']))
 def test_golden_manifest_hash_scope(self):
  h=m.load(m.C)['hashes']; wire=json.dumps(h['golden_manifest'],sort_keys=True,separators=(',',':'),ensure_ascii=True).encode(); self.assertEqual(h['golden_manifest_sha256'],hashlib.sha256(wire).hexdigest())
 def test_commit_order(self):
  steps=m.load(m.C)['commit_protocol']['ordered_steps']; terms=['pending intent','write each file','segment-manifest.json','rename staging','SEGMENT_READY','segment_ready']; positions=[next(i for i,x in enumerate(steps) if term in x) for term in terms]; self.assertEqual(sorted(positions),positions); promotion=m.load(m.C)['commit_protocol']['generation_promotion_steps']; pterms=['candidate_complete','generation-manifest.json','GENERATION_READY','greater-fence generation selector','promoted generation']; ppos=[next(i for i,x in enumerate(promotion) if term in x) for term in pterms]; self.assertEqual(sorted(ppos),ppos)
 def test_blocked_cases_never_advance(self):
  for case in m.load(m.F)['cases']:
   if case['expected']['archive_state'].startswith('blocked'): self.assertEqual('unchanged',case['expected']['checkpoint'],case['fixture_id'])
 def test_path_and_selector_are_opaque_and_monotonic(self):
  c=m.load(m.C); self.assertIn('{intent64}',c['layout']['final']); self.assertIn('{fence20}',c['layout']['selector']); self.assertEqual('no-op',c['promotion']['lower_fence']); self.assertEqual('corruption block',c['promotion']['same_fence_different_state'])
 def test_no_provisional_markers_or_secrets(self):
  text=''.join(p.read_text() for p in (m.C,m.S,m.F,m.FS,m.RS)); self.assertNotIn('// M0-' + 'PROVISIONAL:',text); self.assertNotIn('postgres'+'://',text)
if __name__=='__main__': unittest.main()
