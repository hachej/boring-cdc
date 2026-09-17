import copy, importlib.util, json, unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
S=importlib.util.spec_from_file_location('postgres_protocol',ROOT/'scripts/validate/postgres_protocol.py'); M=importlib.util.module_from_spec(S); S.loader.exec_module(M)
class ProtocolTests(unittest.TestCase):
 def test_canonical_contract_validates(self): self.assertEqual([],M.validate())
 def test_fixture_is_exhaustive(self):
  c=json.loads(M.C.read_text()); f=json.loads(M.F.read_text()); self.assertEqual({x['cause'] for x in c['failure_policy']['mapping']},{x['input']['cause'] for x in f['cases']})
 def test_admission_literals_are_not_owned_here(self):
  w=json.loads(M.C.read_text())['wire']; self.assertEqual('boring-cdc-d-admission',w['admission_owner']); self.assertIn('limits.max_wire_frame_bytes',w['admission_inputs'])
 def test_status_packet_uses_one_durable_boundary(self):
  s=json.loads(M.C.read_text())['wire']['standby_status']; self.assertEqual(34,s['payload_bytes']); self.assertIn('write_lsn == flush_lsn == apply_lsn',s['boundary']); self.assertIn('never copy keepalive wal_end',s['requested_reply'])
 def test_failure_projection_never_advances(self):
  rows=json.loads(M.C.read_text())['failure_policy']['mapping']; self.assertTrue(all(x['checkpoint']=='unchanged' and x['feedback']=='unchanged' for x in rows))
if __name__=='__main__': unittest.main()
