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
        self.assertEqual(
            {
                "identity",
                "order",
                "types",
                "limits",
                "toast",
                "schema",
                "stability",
                "control_routing",
            },
            categories,
        )
        self.assertEqual(set(self.contract["fixture_ids"]), {case["fixture_id"] for case in self.vectors["vectors"]})

    def test_foundational_identity_inputs_are_derived(self):
        primitives = self.vectors["identity_primitives"]
        self.assertEqual(
            primitives["source_slot"]["expected_sha256"],
            event_format.source_slot_identity(primitives["source_slot"]["input"]),
        )
        self.assertEqual(
            primitives["logical_table"]["expected_sha256"],
            event_format.logical_table_id(primitives["logical_table"]["input"]),
        )
        self.assertEqual(
            primitives["relation_fingerprint"]["expected_sha256"],
            event_format.relation_fingerprint(primitives["relation_fingerprint"]["input"]),
        )

    def test_golden_hashes_are_content_sensitive(self):
        wal = self.vectors["vectors"][0]
        self.assertEqual(wal["event"]["connector_event_id"], event_format.wal_id(wal["identity_input"]))
        changed = json.loads(json.dumps(wal["event"]))
        changed["columns"][0]["bytes"] = "AAAAAAAAACs"
        self.assertNotEqual(wal["event"]["payload_hash"], event_format.payload_hash(changed))
        self.assertEqual(wal["event"]["connector_event_id"], changed["connector_event_id"])

        key_change = next(
            case for case in self.vectors["vectors"] if case["fixture_id"] == "SCN-M0-EVENT-KEY-CHANGE-ORDER"
        )["events"][1]
        for field, value in (("operation", "insert"), ("before_key", [{"kind": "int64", "type_oid": 20, "type_modifier": -1, "value": 40}])):
            changed = json.loads(json.dumps(key_change))
            changed[field] = value
            self.assertNotEqual(key_change["payload_hash"], event_format.payload_hash(changed))

    def test_key_component_boundaries_are_unambiguous(self):
        left = [{"kind":"bytes","type_oid":17,"type_modifier":-1,"value":"YWI"},{"kind":"bytes","type_oid":17,"type_modifier":-1,"value":"Yw"}]
        right = [{"kind":"bytes","type_oid":17,"type_modifier":-1,"value":"YQ"},{"kind":"bytes","type_oid":17,"type_modifier":-1,"value":"YmM"}]
        self.assertNotEqual(event_format.key_hash(left), event_format.key_hash(right))
        self.assertNotEqual(event_format.key_hash([{"kind":"int64","type_oid":20,"type_modifier":-1,"value":1}]), event_format.key_hash([{"kind":"text","type_oid":25,"type_modifier":-1,"value":"1"}]))

    def test_control_events_are_not_business_payloads(self):
        controls = [c["event"] for c in self.vectors["vectors"] if c["category"] == "control_routing"]
        self.assertEqual(2, len(controls))
        for event in controls:
            self.assertEqual("control", event["routing"])
            self.assertNotIn("canonical_key", event)
            self.assertEqual(event["payload_hash"], event_format.control_payload_hash(event))


if __name__ == "__main__":
    unittest.main()
