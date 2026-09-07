"""Planning regressions only; not the M0 validator or product-runtime proof.

Run: python3 -m unittest discover -s tests/planning -v
Reads the Git interchange snapshot; use br sync --status/doctor separately for
live-store health. Semantic assertions below guard the reviewed repair seams,
not arbitrary natural-language correctness or completion of future contracts.
"""
import json
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[2]
CORE = "boring-cdc-m0.1"
CONTEXT = "boring-cdc-m0.2"
KNOWLEDGE = "boring-cdc-m0.3"
TOOLING = "boring-cdc-m0-validation-tooling"
POLICY = "boring-cdc-m2.1"
SERIES = "boring-cdc-v01.5"
REPAIR = "boring-cdc-v01.4"
TERMINALS = [
    "boring-cdc-m0-gate", "boring-cdc-m1-raw-demo",
    "boring-cdc-m2-fault-status", "boring-cdc-m3-faults",
    "boring-cdc-m4-bench", "boring-cdc-m5-faults",
    "boring-cdc-m6-endurance", "boring-cdc-m7-release",
]
FIELDS = ("description", "design", "acceptance_criteria", "notes")


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def load_rows():
    return [json.loads(line, object_pairs_hook=unique_object)
            for line in (ROOT / ".beads/issues.jsonl").read_text().splitlines()]


class KickoffContracts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows = load_rows()
        cls.closures = {}
        cls.by_id = {row["id"]: row for row in cls.rows}
        cls.plan = (ROOT / "docs/PLAN.md").read_text()
        cls.agent = (ROOT / "docs/AGENT_SYSTEM.md").read_text()
        cls.series = (ROOT / "docs/SERIES_EXECUTION.md").read_text()

    def blockers(self, bead):
        return {dep["depends_on_id"] for dep in self.by_id[bead].get("dependencies", [])
                if dep["type"] == "blocks"}

    def closure(self, bead, kind="blocks", active=()):
        if bead in active:
            self.fail(f"{kind} cycle: {active + (bead,)}")
        if (bead, kind) in self.closures:
            return self.closures[(bead, kind)]
        result = set()
        for dep in self.by_id[bead].get("dependencies", []):
            if dep["type"] == kind:
                target = dep["depends_on_id"]
                self.assertIn(target, self.by_id)
                result.add(target)
                result.update(self.closure(target, kind, active + (bead,)))
        self.closures[(bead, kind)] = result
        return result

    def text(self, bead):
        return "\n".join(self.by_id[bead].get(field, "") for field in FIELDS)

    def test_unique_valid_references_and_acyclic_graph(self):
        self.assertEqual(len(self.rows), len(self.by_id))
        for row in self.rows:
            for dep in row.get("dependencies", []):
                self.assertEqual(dep["issue_id"], row["id"])
                self.assertIn(dep["depends_on_id"], self.by_id)
            self.closure(row["id"])
            self.closure(row["id"], "parent-child")

    def test_exact_completion_barriers_and_terminal_order(self):
        for milestone, terminal in enumerate(TERMINALS):
            barrier = f"boring-cdc-m{milestone}-complete"
            description = self.by_id[barrier]["description"]
            match = re.search(r"Required leaves \((\d+)\)\n((?:- [^\n]+\n)+)", description)
            self.assertIsNotNone(match, barrier)
            leaves = match[2].splitlines()
            declared = {line[2:] for line in leaves}
            self.assertEqual(len(leaves), len(declared), barrier)
            self.assertEqual(int(match[1]), len(declared), barrier)
            expected = declared | ({TERMINALS[milestone - 1]} if milestone else set())
            self.assertEqual(self.blockers(barrier), expected, barrier)
            self.assertIn(barrier, self.blockers(terminal))
            self.assertNotIn(terminal, self.closure(barrier))
            actual_leaves = {
                row["id"] for row in self.rows
                if row["issue_type"] != "epic"
                and row["id"] not in {barrier, terminal}
                and any(dep["type"] == "parent-child"
                        and dep["depends_on_id"] == f"boring-cdc-m{milestone}"
                        for dep in row.get("dependencies", []))
            }
            self.assertEqual(declared, actual_leaves, barrier)

    def test_release_covers_every_open_executing_task(self):
        covered = self.closure(TERMINALS[-1]) | {TERMINALS[-1]}
        for row in self.rows:
            if row["status"] != "closed" and row["issue_type"] != "epic" and row["id"] != REPAIR:
                self.assertIn(row["id"], covered, row["id"])
        self.assertIn(SERIES, covered)

    def test_bootstrap_stages_have_no_forward_approval_dependency(self):
        self.assertFalse(self.blockers(CORE))
        self.assertEqual(self.blockers(CONTEXT), {CORE})
        self.assertEqual(self.blockers(KNOWLEDGE), {CORE})
        self.assertEqual(self.blockers(TOOLING), {CORE, CONTEXT, KNOWLEDGE})
        decisions = {row["id"] for row in self.rows if row["issue_type"] == "decision"}
        self.assertEqual(len(decisions), 25)
        for bead in decisions | {"boring-cdc-m0-decisions"}:
            closure = self.closure(bead)
            self.assertTrue({CORE, CONTEXT} <= closure, bead)
            self.assertFalse({TOOLING, KNOWLEDGE} & closure, bead)
        for gate in ["boring-cdc-m0-complete", "boring-cdc-m0-gate"]:
            self.assertTrue({CORE, CONTEXT, KNOWLEDGE, TOOLING} <= self.blockers(gate))
        self.assertIn("explicit bootstrap exception", self.plan)
        self.assertIn("explicitly authorized before M0 decisions close", self.agent)

    def test_policy_precedes_consumers_without_live_integration_cycle(self):
        self.assertEqual(self.blockers(POLICY), {"boring-cdc-m2-schema"})
        for bead in ["boring-cdc-m2-spool", "boring-cdc-m2-reconcile", "boring-cdc-m2-jsonl",
                     "boring-cdc-m2-capture-runtime", "boring-cdc-m4-durability", "boring-cdc-m5-loops"]:
            self.assertIn(POLICY, self.blockers(bead))
            self.assertNotIn(bead, self.closure(POLICY))
        self.assertIn(POLICY, self.blockers("boring-cdc-m2-complete"))
        capture = self.by_id["boring-cdc-m2-capture-runtime"]["acceptance_criteria"]
        self.assertIn("explicit typed destination test doubles", capture)
        self.assertNotIn("run unchanged against capture, ClickHouse, and archive adapters", capture)
        spool = self.by_id["boring-cdc-m2-spool"]["acceptance_criteria"]
        self.assertIn("not prerequisites of this spool leaf", spool)
        self.assertIn("real restart", self.text("boring-cdc-m2-capture-runtime"))

    def test_oracle_requires_independent_business_events(self):
        self.assertTrue({"boring-cdc-d-keys", "boring-cdc-d-values"}
                        <= self.blockers("boring-cdc-d-oracle"))
        self.assertNotIn("exact set difference still reports the missing mutation ID", self.plan)
        for bead in ["boring-cdc-d-oracle", "boring-cdc-m1-workload",
                     "boring-cdc-m3-oracle", "boring-cdc-m7-estuary"]:
            text = self.text(bead)
            self.assertIn("drop only an overwritten business event", text, bead)
            self.assertIn("drop only its ledger", text, bead)
            self.assertIn("pre-baseline", text, bead)
            self.assertIn("unavailable", text, bead)

    def test_oracle_counterexample_is_not_a_ledger_difference(self):
        # Witness why independent observation is necessary, not production oracle code.
        expected_events = [("u1", "order1", 10), ("u2", "order1", 20)]
        observed_events = expected_events[1:]
        expected_ledger = {"u1": "hash1", "u2": "hash2"}
        observed_ledger = expected_ledger.copy()
        self.assertEqual(expected_ledger, observed_ledger)
        self.assertEqual({key: value for _, key, value in expected_events},
                         {key: value for _, key, value in observed_events})
        self.assertEqual({event[0] for event in expected_events}
                         - {event[0] for event in observed_events}, {"u1"})

    def test_actual_payload_hash_audit_and_compound_floor_predicate(self):
        for bead in ["boring-cdc-d-ch-accept", "boring-cdc-m0-ch-model", "boring-cdc-m4-durability"]:
            text = self.text(bead)
            self.assertIn("recompute the canonical payload hash", text)
            self.assertIn("preserves event IDs, stored hash columns and batch markers", text)
        predicate = "the slot is valid and required resume WAL is available"
        self.assertIn(predicate, self.plan)
        self.assertIn(predicate, self.text("boring-cdc-m2-reconcile"))
        for bead in ["boring-cdc-d-pg-protocol", "boring-cdc-m0-pg-contract",
                     "boring-cdc-m1-source-identity", "boring-cdc-m2-reconcile"]:
            self.assertIn("Compound creation-floor safety fixture", self.text(bead))

    def test_ddl_and_m4_evidence_boundaries(self):
        ddl = self.by_id["boring-cdc-m1-ddl-fixtures"]["acceptance_criteria"]
        self.assertNotIn("either conflicts with guard or", ddl)
        self.assertIn("every admitted contract-changing DDL operation must conflict", ddl)
        toast = self.by_id["boring-cdc-m4-toast"]["acceptance_criteria"]
        self.assertIn("not an M4 closure prerequisite", toast)
        self.assertNotIn("is discoverable through `destination list`", toast)
        promotion = self.by_id["boring-cdc-m4-promotion"]["acceptance_criteria"]
        self.assertNotIn("cannot move either destination selector backwards", promotion)
        self.assertIn("never an M4 closure prerequisite", promotion)
        self.assertIn("real destination list command", self.text("boring-cdc-m5-ops-cli"))
        self.assertIn("real archive greatest-fence", self.text("boring-cdc-m5-segment-set"))

    def test_compose_freezes_build_inputs_not_future_output(self):
        text = self.text("boring-cdc-d-compose")
        self.assertNotIn("PostgreSQL/ClickHouse/connector/base image versions", text)
        self.assertIn("No future connector binary/container digest is an M0 literal prerequisite", text)
        self.assertIn("m7-artifacts owns", text)

    def test_series_preparation_is_not_a_product_bootstrap_gate(self):
        self.assertFalse(self.blockers(SERIES))
        for terminal in TERMINALS[:-1]:
            self.assertNotIn(SERIES, self.closure(terminal))
        self.assertIn(SERIES, self.blockers("boring-cdc-m7-estuary"))
        self.assertIn(SERIES, self.blockers("boring-cdc-m7-docs"))
        for article in range(1, 6):
            self.assertIn(f"| {article}.", self.series)
        self.assertIn("M2 JSONL candidate segments are not live output", self.series)
        self.assertIn("no additional benchmark vendors", self.series)
        self.assertIn("originating prompts", self.series)
        self.assertIn("no publication or m0-completion date", self.series.lower())

    def test_document_bead_links_resolve(self):
        for name in ["docs/PLAN.md", "docs/AGENT_SYSTEM.md", "docs/SERIES_EXECUTION.md"]:
            text = (ROOT / name).read_text()
            for match in re.findall(r"(?<![/\w])boring-cdc-[a-z0-9][a-z0-9.-]*", text):
                bead = match.rstrip(".")
                if bead.endswith("-"):  # prose family prefix, e.g. d-*
                    continue
                self.assertTrue(bead in self.by_id, (name, bead))


if __name__ == "__main__":
    unittest.main()
