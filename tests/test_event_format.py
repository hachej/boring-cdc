import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("event_format", ROOT / "scripts/validate/event_format.py")
event_format = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(event_format)


class EventFormatContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract = json.loads((ROOT / "contracts/event/event-format.json").read_text())
        cls.vectors = json.loads((ROOT / "fixtures/m0/event-format/golden-vectors.json").read_text())

    def test_complete_contract_and_goldens_validate(self):
        findings, bundle = event_format.validate()
        self.assertEqual([], findings)
        self.assertEqual(4, len(bundle))

    def test_every_required_branch_has_an_exact_fixture(self):
        categories = {case["category"] for case in self.vectors["vectors"]}
        self.assertEqual({"identity", "order", "types", "toast", "control_routing"}, categories)
        self.assertEqual(set(self.contract["fixture_ids"]), {case["fixture_id"] for case in self.vectors["vectors"]})

    def test_golden_hashes_are_content_sensitive(self):
        wal = self.vectors["vectors"][0]
        self.assertEqual(wal["event"]["connector_event_id"], event_format.wal_id(wal["identity_input"]))
        changed = json.loads(json.dumps(wal["event"]))
        changed["columns"][0]["bytes"] = "AAAAAAAAACs"
        self.assertNotEqual(wal["event"]["payload_hash"], event_format.payload_hash(changed))
        self.assertEqual(wal["event"]["connector_event_id"], changed["connector_event_id"])

    def test_key_component_boundaries_are_unambiguous(self):
        left = [{"kind":"bytes","value":"YWI"},{"kind":"bytes","value":"Yw"}]
        right = [{"kind":"bytes","value":"YQ"},{"kind":"bytes","value":"YmM"}]
        self.assertNotEqual(event_format.key_hash(left), event_format.key_hash(right))
        self.assertNotEqual(event_format.key_hash([{"kind":"int64","value":1}]), event_format.key_hash([{"kind":"text","value":"1"}]))

    def test_control_events_are_not_business_payloads(self):
        controls = [c["event"] for c in self.vectors["vectors"] if c["category"] == "control_routing"]
        self.assertEqual(2, len(controls))
        for event in controls:
            self.assertEqual("control", event["routing"])
            self.assertNotIn("canonical_key", event)
            self.assertEqual(event["payload_hash"], event_format.control_payload_hash(event))


if __name__ == "__main__":
    unittest.main()
