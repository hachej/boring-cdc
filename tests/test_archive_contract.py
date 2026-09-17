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
 def test_manifest_identity_semantics_reject_cross_field_mismatch(self):
  import copy
  manifest=copy.deepcopy(m.load(m.C)['hashes']['golden_manifest']); manifest['files'][1]['logical_table_id']='9'*64
  self.assertTrue(m.validate_segment_manifest_semantics(manifest))
  manifest=copy.deepcopy(m.load(m.C)['hashes']['golden_manifest']); manifest['files'][1]['format']='jsonl-zstd'
  self.assertTrue(m.validate_segment_manifest_semantics(manifest))
 def test_provisional_inventory_is_card_bound(self):
  c=m.load(m.C); marker=lambda decision:f'// M0-PROVISIONAL: {decision}'
  self.assertEqual(['59a63169'],c['authority']['owner_cards'])
  self.assertEqual(set(),set(c['provisional_markers']))
  self.assertEqual('59a63169',c['consumes']['durability']['approval_card'])
  self.assertNotIn('provisional',c['layout'])
  self.assertNotIn('provisional',c['writer_profile'])
 def test_consumed_digests_are_current(self):
  c=m.load(m.C)
  for name in ('event_contract','archive_scope','failure_policy','promotion','durability'):
   item=c['consumes'][name]; path=item.get('path') or item.get('confirmed_projection_source'); expected=item.get('sha256') or item.get('source_sha256'); self.assertEqual(expected,m.digest(m.ROOT/path),name)
 def test_forged_evidence_source_parent_is_rejected(self):
  from unittest import mock
  from types import SimpleNamespace
  inputs=m.validate()[1]; validator_sha=m.digest(m.V)
  for candidate in ('HEAD',m.subprocess.check_output(['git','rev-parse','--short','HEAD'],cwd=m.ROOT,text=True).strip()):
   _,error=m.resolve_source_parent({'inputs':inputs,'validator_sha256':validator_sha,'source_parent_git_commit':candidate},inputs,validator_sha); self.assertIn('canonical full lowercase commit OID',error)
  _,error=m.resolve_source_parent({'inputs':inputs,'validator_sha256':validator_sha,'source_parent_git_commit':'0'*40},inputs,validator_sha); self.assertIn('not an existing commit',error)
  candidate='1'*40
  with mock.patch.object(m.subprocess,'check_output',side_effect=[m.subprocess.check_output(['git','rev-parse','HEAD'],cwd=m.ROOT,text=True),candidate+'\n']), mock.patch.object(m.subprocess,'run',return_value=SimpleNamespace(returncode=1)):
   _,error=m.resolve_source_parent({'inputs':inputs,'validator_sha256':validator_sha,'source_parent_git_commit':candidate},inputs,validator_sha); self.assertIn('not an ancestor of HEAD',error)
 def test_no_secrets(self):
  text=''.join(p.read_text() for p in (m.C,m.S,m.F,m.FS,m.RS)); self.assertNotIn('postgres'+'://',text)
if __name__=='__main__': unittest.main()
