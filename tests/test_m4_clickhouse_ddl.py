import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('m4', ROOT / 'scripts/validate/m4_clickhouse_ddl.py')
m4 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m4)


class M4ClickHouseDdlEvidenceTests(unittest.TestCase):
    def test_sealed_real_run(self):
        self.assertEqual([], m4.validate())

    def assert_rejected_change(self, change, code):
        original = m4.E
        try:
            with tempfile.TemporaryDirectory() as directory:
                evidence = json.loads(original.read_text())
                change(evidence)
                m4.E = Path(directory) / 'evidence.json'
                m4.E.write_text(json.dumps(evidence))
                self.assertIn(code, m4.validate())
        finally:
            m4.E = original

    def test_validator_rejects_merge_drift(self):
        self.assert_rejected_change(
            lambda evidence: evidence['merge_invariant'].update(after_sha256='0' * 64),
            'E_MERGE_INVARIANT',
        )

    def test_validator_rejects_stale_inputs(self):
        self.assert_rejected_change(
            lambda evidence: evidence.update(input_sha256={'src/m4_clickhouse_schema.rs': '0' * 64}),
            'E_INPUTS_STALE',
        )

    def test_validator_rejects_unrelated_fingerprint(self):
        self.assert_rejected_change(
            lambda evidence: evidence.update(object_fingerprint='0' * 64),
            'E_OBJECT_FINGERPRINT',
        )

    def test_validator_requires_history_proof(self):
        self.assert_rejected_change(
            lambda evidence: evidence.pop('history_interface', None),
            'E_HISTORY_INTERFACE',
        )


if __name__ == '__main__':
    unittest.main()
