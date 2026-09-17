import copy
import importlib.util
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("core", ROOT / "scripts/lib/core_validator.py")
core = importlib.util.module_from_spec(spec)
spec.loader.exec_module(core)


class ComposeExecutionSchemaTests(unittest.TestCase):
    def setUp(self):
        self.schema = json.loads((ROOT / "contracts/m0/compose-execution-result.schema.json").read_text())
        self.valid = json.loads((ROOT / "fixtures/m0/compose-execution-result/valid.json").read_text())

    def findings(self, value):
        findings = []
        core.validate_schema_instance(value, self.schema, findings, base=ROOT / "contracts/m0", root=self.schema)
        return findings

    def test_valid_fixture_attests_all_approved_images_and_tools(self):
        self.assertEqual([], self.findings(self.valid))
        self.assertEqual({"builder", "clickhouse", "postgres", "runtime"}, set(self.valid["images"]))

    def test_wrong_tool_or_image_digest_is_rejected(self):
        for path, value in ((('tool_versions', 'docker_engine'), 'wrong'), (('images', 'postgres', 'index_digest'), 'sha256:' + '0' * 64)):
            with self.subTest(path=path):
                hostile = copy.deepcopy(self.valid)
                target = hostile
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = value
                self.assertTrue(self.findings(hostile))

    def test_case_outcome_effects_and_metrics_are_exact(self):
        for field, value in (("outcome", "fail"), ("external_effects", ["fabricated"]), ("metrics", [])):
            with self.subTest(field=field):
                hostile = copy.deepcopy(self.valid)
                hostile[field] = value
                self.assertTrue(self.findings(hostile))


if __name__ == "__main__":
    unittest.main()
