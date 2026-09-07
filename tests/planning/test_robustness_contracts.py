"""Static ownership/projection regressions, not product-runtime evidence.

Run with: python3 -m unittest discover -s tests/planning -v
"""
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
PREFIX = "boring-cdc-"


class RobustnessContracts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows = {r["id"]: r for r in map(json.loads,
                    (ROOT / ".beads/issues.jsonl").read_text().splitlines())}
        cls.plan = (ROOT / "docs/PLAN.md").read_text()
        cls.agent = (ROOT / "docs/AGENT_SYSTEM.md").read_text()
        cls.requirements = (ROOT / "docs/REQUIREMENTS.md").read_text()

    def text(self, suffix):
        row = self.rows[PREFIX + suffix]
        return "\n".join(row.get(f, "") for f in
                         ("description", "design", "acceptance_criteria"))

    def blockers(self, suffix):
        return {d["depends_on_id"].removeprefix(PREFIX)
                for d in self.rows[PREFIX + suffix].get("dependencies", [])
                if d["type"] == "blocks"}

    def closure(self, suffix):
        seen, pending = set(), list(self.blockers(suffix))
        while pending:
            node = pending.pop()
            if node not in seen:
                seen.add(node)
                pending.extend(self.blockers(node))
        return seen

    def test_action_authorization_has_upstream_owner_not_status_back_edge(self):
        for text in (self.plan, self.agent, self.requirements):
            self.assertIn("action-relevant control revisions", text)
        self.assertNotIn("Any state revision or bound fingerprint change invalidates", self.plan)
        ownership = self.text("m2-ownership")
        self.assertIn("unrelated snapshot progress", ownership)
        self.assertIn("nonce issuance", ownership)
        self.assertIn("Atomically validate current predicates", ownership)
        self.assertNotIn("Revision derivation and projection belong to", ownership)
        self.assertNotIn("m2-fault-status", self.closure("m2-ownership"))
        self.assertNotIn("m2-journal", self.closure("m2-ownership"))
        self.assertIn("actual queue integration", ownership)
        self.assertNotIn("expire them when state changes", self.text("m5-ops-cli"))
        self.assertIn("failed current safety predicates", self.text("m5-ops-cli"))

    def test_expected_close_is_not_an_ownership_loss_bypass(self):
        for bead in ("d-failure-policy", "m2-capture-runtime"):
            text = self.text(bead)
            self.assertIn("delayed", text)
            self.assertIn("advisory", text)
        self.assertIn("expected-close token", self.text("m2-capture-runtime"))
        self.assertIn("Any advisory-lock or unexpected CopyBoth connection loss", self.text("m2-capture-runtime"))
        self.assertIn("persistence failure", self.text("m2-capture-runtime"))
        self.assertIn("token cannot suppress a later connection generation", self.plan)

    def test_actual_writer_evidence_is_not_observer_pragma(self):
        for bead in ("d-sqlite", "m1-preflight", "m2-schema", "m2-fault-status"):
            text = self.text(bead)
            self.assertIn("writer", text)
            self.assertIn("attestation", text)
        self.assertIn("synchronous` is connection-local", self.plan)
        self.assertIn("writer OFF versus observer FULL", self.text("m1-preflight"))
        self.assertNotIn("m2-schema", self.closure("m1-preflight"))
        self.assertIn("synthetic live-attestation", self.text("m1-preflight"))

    def test_reader_and_scheduler_owners_do_not_require_later_consumers(self):
        self.assertIn("reader handle/deadline", self.text("m2-schema"))
        self.assertIn("logical range-pin lifecycle", self.text("m2-pressure"))
        self.assertIn("fair bounded service scheduling", self.text("m2-journal"))
        self.assertIn("typed control/destination/GC doubles", self.text("m2-journal"))
        self.assertNotIn("m2-pressure", self.closure("m2-journal"))
        self.assertNotIn("m2-capture-runtime", self.closure("m2-journal"))
        self.assertIn("actual queue integration", self.text("m2-ownership"))
        self.assertIn("startup integrity checks remain mandatory", self.plan)
        for bead in ("m2-jsonl", "m4-durability", "m5-segment-set", "m5-loops"):
            self.assertIn("release physical SQLite snapshots", self.text(bead))

    def test_kernel_is_after_m0_and_before_domain_types(self):
        self.assertEqual(self.blockers("m1.1"), {"m0-gate"})
        for bead in ("m1-source-identity", "m1-ordering", "m1-bootstrap-sm", "m1-complete"):
            self.assertIn("m1.1", self.blockers(bead))
        self.assertNotIn("m1.1", self.closure("m0-gate"))
        text = self.text("m1.1")
        self.assertIn("not domain transitions", text)
        self.assertIn("compile-fail", text)
        self.assertIn("test-only", text)
        self.assertIn("production transition functions", self.text("m6-failure-matrix"))

    def test_audit_rounds_preserve_units_freshness_and_domain_ownership(self):
        for bead in ("m4-durability", "m5-segment-set"):
            text = self.text(bead)
            for phrase in ("audit-round executor", "target complete checkpoint", "bounded resumable",
                           "later rounds catch corruption", "expired subranges"):
                self.assertIn(phrase, text, bead)
        self.assertIn("m5-loops only invokes this API", self.text("m5-segment-set"))
        self.assertIn("per-subrange check timestamps", self.text("m4-durability"))
        self.assertIn("separate coverage meanings", self.text("d-archive-durability"))

    def test_recovery_measurements_do_not_block_their_forecast_implementation(self):
        self.assertNotIn("m6-endurance", self.closure("m6-metrics"))
        self.assertNotIn("m6-endurance", self.closure("d-scale"))
        self.assertIn("synthetic compatible profile manifests", self.text("m6-metrics"))
        self.assertIn("No finite estimate for nonpositive headroom", self.text("m6-metrics"))
        self.assertIn("actual recoverability-envelope measurements", self.text("m6-endurance"))
        self.assertIn("m6-endurance", self.closure("m7-docs"))

    def test_explain_has_one_variant_owner_and_no_early_runtime_gate(self):
        self.assertEqual(self.blockers("m5.1"), {"m2-reconcile", "m5-ops-cli", "m5-gc"})
        self.assertIn("m5.1", self.blockers("m5-complete"))
        for bead in ("m1-raw-demo", "m2-fault-status", "m4-bench", "m5-ops-cli"):
            self.assertNotIn("m5.1", self.closure(bead))
        self.assertIn("base journal inspect", self.text("m2-reconcile"))
        self.assertIn("distinct operation variant owned solely by m5.1", self.text("m1-cli-contract"))
        self.assertIn("Missing, expired and GC-removed evidence is unavailable", self.text("m5.1"))
        self.assertIn("no source reread", self.text("m5.1"))

    def test_embedded_scenario_and_release_projections_are_current(self):
        pairs = [("m6-failure-matrix", "## 15. Required test matrix", "## 16. Milestones"),
                 ("m6-endurance", "## 20. Main risks and mitigations", None),
                 ("m7-release", "## 17. v0.1 release acceptance", "## 18. Explicitly out of scope")]
        for bead, start, end in pairs:
            canonical = self.plan.split(start, 1)[1]
            if end:
                canonical = canonical.split(end, 1)[0]
            canonical = (start + canonical).strip()
            self.assertIn(canonical, self.rows[PREFIX + bead]["description"], bead)
        inventory = self.plan.split("### 12.1 Commands", 1)[1].split("```", 2)[1]
        self.assertIn(inventory, self.rows[PREFIX + "m1-cli-contract"]["description"])


if __name__ == "__main__":
    unittest.main()
