import importlib.util
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("core_validator", ROOT / "scripts/lib/core_validator.py")
CORE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CORE)


class EvidenceFreshnessScopeTests(unittest.TestCase):
    def test_same_milestone_and_shared_paths_remain_binding(self):
        self.assertTrue(CORE.freshness_path_applies("boring-cdc-m2-complete", "contracts/coverage/m2.json"))
        self.assertTrue(CORE.freshness_path_applies("boring-cdc-m2-heartbeat", "scripts/e2e/m2_heartbeat.sh"))
        self.assertTrue(CORE.freshness_path_applies("boring-cdc-m2-heartbeat", "scripts/validate/evidence.sh"))
        self.assertFalse(CORE.freshness_path_applies("boring-cdc-m2-heartbeat", "scripts/acceptance/m2_complete.sh"))
        self.assertTrue(CORE.freshness_path_applies("boring-cdc-m2-complete", "scripts/acceptance/m2_complete.sh"))
        self.assertFalse(CORE.freshness_path_applies("boring-cdc-m2-complete", "scripts/lib/core_validator.py"))

    def test_later_milestone_paths_do_not_stale_earlier_evidence(self):
        self.assertFalse(CORE.freshness_path_applies("boring-cdc-m2-complete", "contracts/m3/planner-cases.json"))
        self.assertFalse(CORE.freshness_path_applies("boring-cdc-m1-complete", "scripts/lib/m3_planner_evidence.py"))

    def test_unknown_owner_keeps_global_freshness(self):
        self.assertTrue(CORE.freshness_path_applies("other-owner", "contracts/m3/planner-cases.json"))


if __name__ == "__main__":
    unittest.main()
