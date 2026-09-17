import importlib.util
import json
import subprocess
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

    def test_evidence_source_parent_rejects_mutable_or_missing_revisions(self):
        inputs = event_format.validate()[1]
        validator_sha = event_format.hashlib.sha256(event_format.VALIDATOR.read_bytes()).hexdigest()
        for candidate in ("HEAD", subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True).strip()):
            prior = {"inputs": inputs, "validator_sha256": validator_sha, "source_parent_git_commit": candidate}
            _, error = event_format.resolve_source_parent(prior, inputs, validator_sha)
            self.assertIn("canonical full lowercase commit OID", error)
        prior = {"inputs": inputs, "validator_sha256": validator_sha, "source_parent_git_commit": "0" * 40}
        _, error = event_format.resolve_source_parent(prior, inputs, validator_sha)
        self.assertIn("not an existing commit", error)

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

    def test_relation_component_derivation_is_bound_transitively(self):
        vectors = json.loads(json.dumps(self.vectors))
        component = vectors["identity_primitives"]["relation_components"][0]
        component["definition"] = "primary key (other_id)"
        component["expected_sha256"] = event_format.digest(
            "boring-cdc/relation-component/v1",
            [component["kind"].encode(), component["definition"].encode()],
        )
        with tempfile.TemporaryDirectory(dir=ROOT / "fixtures/m0/event-format") as directory:
            vector_path = Path(directory) / "vectors.json"
            vector_path.write_text(json.dumps(vectors))
            original = event_format.VECTORS
            try:
                event_format.VECTORS = vector_path
                findings, _ = event_format.validate()
            finally:
                event_format.VECTORS = original
        self.assertIn("E_RELATION_COMPONENT_BINDING", {finding["code"] for finding in findings})

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

    def test_numeric_key_payload_rejects_noncanonical_spellings(self):
        invalid = [b"+\x00\x00\x0201", b"+\x00\x00\x01x", b"-\x00\x00\x010", b"+\x00\x00\x0200", b"+\x00\x13\x011"]
        for raw in invalid:
            component = {
                "kind": "bytes",
                "type_oid": 1700,
                "type_modifier": -1,
                "value": event_format.base64.urlsafe_b64encode(raw).decode().rstrip("="),
            }
            with self.assertRaises(ValueError):
                event_format.key_component_bytes(component)

    def test_empty_text_and_bytea_keys_are_representable(self):
        schema = json.loads(event_format.SCHEMA.read_text())
        emitted = json.loads(json.dumps(self.vectors["vectors"][0]["event"]))
        for component in (
            {"kind": "text", "type_oid": 25, "type_modifier": -1, "value": ""},
            {"kind": "bytes", "type_oid": 17, "type_modifier": -1, "value": ""},
        ):
            event = json.loads(json.dumps(emitted))
            event["canonical_key"] = [component]
            event["key_hash"] = event_format.key_hash(event["canonical_key"])
            event["payload_hash"] = event_format.payload_hash(event)
            findings = []
            event_format.CORE.validate_schema_instance(event, schema, findings, base=event_format.SCHEMA.parent, root=schema)
            self.assertEqual([], findings)

    def test_key_canonicalization_rejects_hostile_boundaries(self):
        emitted = json.loads(json.dumps(self.vectors["vectors"][0]["event"]))
        hostile = []
        bad = json.loads(json.dumps(emitted))
        bad["canonical_key"] = [{"kind": "bytes", "type_oid": 17, "type_modifier": -1, "value": "AB"}]
        noncanonical_base64 = bad
        hostile.append(bad)
        bad = json.loads(json.dumps(emitted))
        bad["canonical_key"] = [{"kind": "bytes", "type_oid": 2950, "type_modifier": -1, "value": "AA"}]
        hostile.append(bad)
        bad = json.loads(json.dumps(emitted))
        bad["operation"] = "update"
        bad["before_key"] = [{"kind": "text", "type_oid": 25, "type_modifier": -1, "value": "é" * 513}]
        hostile.append(bad)
        for event in hostile:
            self.assertTrue(event_format.event_semantic_findings(event), event)
        noncanonical_column = json.loads(json.dumps(emitted))
        noncanonical_column["columns"][0]["bytes"] = "AB"
        schema = json.loads(event_format.SCHEMA.read_text())
        for event in (noncanonical_base64, noncanonical_column):
            schema_findings = []
            event_format.CORE.validate_schema_instance(
                event,
                schema,
                schema_findings,
                base=event_format.SCHEMA.parent,
                root=schema,
            )
            self.assertTrue(schema_findings, "schema accepted noncanonical base64url trailing bits")

    def test_control_events_are_not_business_payloads(self):
        controls = [c["event"] for c in self.vectors["vectors"] if c["category"] == "control_routing"]
        self.assertEqual(2, len(controls))
        for event in controls:
            self.assertEqual("control", event["routing"])
            self.assertNotIn("canonical_key", event)
            self.assertEqual(event["payload_hash"], event_format.control_payload_hash(event))


if __name__ == "__main__":
    unittest.main()
