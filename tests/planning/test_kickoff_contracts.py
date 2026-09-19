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
            acceptance = self.by_id[barrier]["acceptance_criteria"]
            counts = re.findall(r"All (\d+) required M\d+ leaves", acceptance)
            if milestone:  # M0 names its approved contract set without a numeral.
                self.assertEqual(len(counts), 1, barrier)
            for count in counts:
                self.assertEqual(int(count), len(declared), barrier)
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

    def test_article_one_uses_shipped_teaching_view_without_article_four_evidence(self):
        rows = [line for line in self.series.splitlines()
                if line.startswith("| 1. How Postgres CDC Works |")]
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0],
            "| 1. How Postgres CDC Works | `boring-cdc-m1-raw-demo`: protocol, "
            "workload, decode and fail-closed fixtures | Shipped PR #3 evidence: "
            "[real raw `pgoutput` events](../evidence/article1/README.md) plus the "
            "same-stream, process-local, non-durable `article1_row_view` **TEACHING VIEW** "
            "showing insert/current row, update/overwrite and delete/removal; explain the tested "
            "topology, keys, source-risk, replica-identity and oracle limits. This is not "
            "destination evidence: ClickHouse, durability, checkpoints and exactly-once are "
            "deferred to Article 4/M4 | Relevant M7 external decode/observation evidence where "
            "available; otherwise clearly label unavailable visibility. Debezium explanation only |",
        )

    def test_series_preparation_record_has_per_article_boundaries(self):
        checkpoints = {
            1: ("boring-cdc-m1-raw-demo", "boring-cdc-pci.6", "verified local demo",
                "scripts/acceptance/article1.sh", "evidence/article1/manifest.json",
                "fixtures/article1/compose.yml", "derives `PGPASSWORD`"),
            2: ("boring-cdc-m2-fault-status", "docs/durable-simple-operator.md",
                "bounded local command available", "scripts/acceptance/durable_simple_case.sh",
                "six-transaction source-to-durable-SQLite simple case", "article composite unavailable"),
            3: ("boring-cdc-m3-faults", "boring-cdc-m3-oracle",
                "component routes exist, article result unavailable", "scripts/e2e/m3_bootstrap.sh",
                "scripts/e2e/m3_planner.sh", "scripts/e2e/m3_oracle.sh"),
            4: ("boring-cdc-m4-bench", "boring-cdc-m5-table-add",
                "component routes exist, article result unavailable", "scripts/e2e/m4_clickhouse_ddl.sh",
                "scripts/e2e/m4_durability.sh", "scripts/e2e/m4_bench.sh"),
            5: ("boring-cdc-m5-faults", "canonical M5 owners", "**unavailable:**",
                "scripts/acceptance/m5.sh", "scripts/e2e/m5_faults.sh", "reconstruct/verify"),
        }
        for article, required in checkpoints.items():
            rows = [line for line in self.series.splitlines()
                    if line.startswith(f"| {article} | `boring-cdc-")]
            self.assertEqual(len(rows), 1, article)
            row = rows[0]
            self.assertEqual(len(row.split("|")), 7, article)
            story_state = ("restricted contemporaneous provenance recovered"
                           if article == 1 else "current bundle **unavailable**")
            for phrase in required + ("Agent-story owner", story_state,
                                      "single command-capture owner", "boring-cdc-m7-estuary",
                                      "**unavailable**"):
                self.assertIn(phrase, row, (article, phrase))
            for phrase in ("disclosure", "publication approval"):
                self.assertIn(phrase, row.lower(), (article, phrase))
        self.assertIn("python3 scripts/validate/article1_transcript.py", self.series)
        self.assertIn("current-master and provenance refresh", self.series)
        self.assertIn("`origin/master` `bc892f0`", self.series)
        self.assertIn("M7 consumes only measurements", self.series)

    def test_article_one_recovery_binds_authentic_sessions_without_publishing_raw_content(self):
        recovered = {
            "1595a1a0-6bf2-4cde-8397-e77fab858ecf": (
                "boring-cdc-pci.1", "16:15:56–16:31:58",
                "0aaf87846c40d14f94c54cb7585a46f037c003fe",
                "e4a7654e50cd3cc481efcfb9e634caf1c4a572e52eaf0f47f608857b9819e9db",
                "6ff0a855d4ae0aa8c709a592d58e17d0695a641a5458f9fcf0078ebb9936106f",
                "7da438832294bfbb927f04e5210a789ec5d1f12540b43beb4a6522b858e4bc18"),
            "ca68a5cd-862d-4979-a60d-f0403a0b453e": (
                "boring-cdc-pci.2", "16:15:56–16:30:55",
                "974a2b02f2adbd13405208637c3e834eba66382e",
                "59e8ab80c5dbdf40e808788c1fee1f748c6e34ca30886ede14179861c51300d7",
                "dddfa4b2b6c993a574d9c8aa22d52d1f50aac44c8d3a0cbc13431f57ed2b6dc3",
                "cf247ff6fc88f56e6861e90b2b8131b70bb54bb099d8f2f0801448fd37588e48"),
            "664a61c7-6d9b-47a3-9eca-b9f987249a84": (
                "boring-cdc-pci.1.1", "16:44:08–17:37:56",
                "411ee95db38969a144dbdd97d16c08b1ef9b784f",
                "f029560bc05507fde78aacc815734a9f55243b1578bd42b2ed987493ce081148",
                "f65fdf52bdb734672d75510555ce02dc002d4ae6b3b7975155f1c1342748de21",
                "18bc85f3a53b9c884842edc5a3885313eaccd289ff3a2a7d2a223f9350558411"),
            "beac1b09-e095-43f5-81c2-b173c819251b": (
                "boring-cdc-pci.1.1.1", "17:40:15–18:11:18",
                "39625f8b992a795d1597359acd9ffe6e465a7d40",
                "2d0d15b1d7aa6b56bb4aaa12676461b0c89740de0be0a715a92dd9449dd03b9f",
                "78d65270c79127fb5ff2123e5a6b28683f5118784b2c91ee566629467ad79049",
                "870bc0775e8e379c1558ef1866749d93300d75c82adfebf00cb1d1c2fc5286ef"),
            "5c6891cf-9b6c-4282-8d58-624cec18ac17": (
                "boring-cdc-pci.3", "18:12:13–18:54:06",
                "94854205b556a85e26304299d7595f8b3828cccd",
                "7bf3ff89fcd6521bb0c162a487672017b68efd686d368a7538a4c7d04d2c95bb",
                "6d18055bc9b1969000ce9915dabc0740d34c0f73ef7be77a24a8ebbc00471da2",
                "81a93b40f4ced809abc22b81fe981a55136def71e134911703c72b7bec3e2d85"),
            "30c7e3ab-a33e-4d78-8eb7-7a72c021c597": (
                "boring-cdc-pci.4", "18:56:09–19:33:21",
                "5d3792c8cd853fd3f34e3baef744b2ba2adedd1c",
                "f23683ce2d6dc4c3110783a688712466ab33b734bfdf180acd9e5c24bde604f6",
                "703c9a2074237ec4dee28846565cd50c8cd84c8d0bec2ac66de71b0cdd7a919f",
                "d4321b0d8c0963808ac5316d6e25cb18c05434dc327547c5ecfb9159b3ad1662"),
            "0b7e681c-8c45-406a-9a6f-6172f115ec40": (
                "boring-cdc-pci.6", "19:34:16–20:01:18",
                "bd93cbea964f623ba82e40adc43451bd30fff184",
                "cd10a72534328811d588c022c365fc66122b174bf226517216003f9400736f89",
                "99ff79feb7ce91d69bdcce530853aebfa3b2c53b7c0695fb377b069962b4fd00",
                "abe9360dd3c880e6f242295ac6244073e5ea50d3f0f22580d79a973bf9a5a102"),
            "59853991-0e93-4d9b-8e91-2fcedc5fbf73": (
                "boring-cdc-pci.5", "20:02:05–20:26:22",
                "b420456f1a1350857c07b330943250543055224a",
                "eb180bd21b5e7c1bde5c1e6458c17601582f2c39aa10f3336069be6fa52102ca",
                "fbf44d6be61ac61f5d0fd0634ecd8f47c8d83f4c8c59b6a50a1e05f194016056",
                "4274deb886c623aa05b1c453534820788de46ee0f14a6ebb8514e938f563d67c"),
            "c8fa8e9f-4fa9-401a-836c-6c5622e4621b": (
                "boring-cdc-pci.7", "20:27:41–20:31:29",
                "726cd7ef58e6f8545e560d62cea5568bc15babc8",
                "d81281afcc0b3a556f2ccdbc4aba8a3747aceb09bf15ac3f0ff55fa7d1967726",
                "3b448721024569f4b558daa10eded03422c5e42729be5a3a9ed55f0a27ef0530",
                "6359b42ddb1192239232c4802881fdacdc3797507be8ef77b94690ca156fe8d5"),
        }
        self.assertEqual(len(recovered), 9)
        for session, values in recovered.items():
            rows = [line for line in self.series.splitlines()
                    if line.startswith("| `boring-cdc-pci.") and f"`{session}`" in line]
            self.assertEqual(len(rows), 1, session)
            for value in (session,) + values:
                self.assertIn(value, rows[0], (session, value))
        for phrase in ("UTF-8 bytes of canonical JSON", "`ensure_ascii=true`",
                       "9 orchestrator, 48 reviewer and 52 worker records",
                       "window contained 1, 18 and 10 respectively",
                       "blocked transport attempt and authority escalation; no runtime implementation",
                       "tracker-only handoff", "no Bead handoff by owner override",
                       "owner override forbade a metadata/handoff commit",
                       "BORING_AGENT_SESSION_ROOT", "Raw locations remain restricted",
                       "do **not** establish a separate human-chat correction chronology",
                       "Publication limitation requiring owner decision before 2026-09-24",
                       "Repository-local extraction and validation tooling can now establish provenance",
                       "paired with its restricted retained source",
                       "trusted whole-file digest from the ratified table",
                       "re-derives every redacted field and source hash",
                       "rejects missing sources, digest mismatches or any exact-derivation mismatch",
                       "treats Unix-style absolute paths as private regardless of root",
                       "fabricated narrative with dummy hashes",
                       "has not received a hardened-validator receipt",
                       "rather than editorial review or publication approval",
                       "no reviewed, approved, publication-safe repository `agent-story/` bundle",
                       "must not be copied, paraphrased as dialogue"):
            self.assertIn(phrase, self.series)

    def test_article_one_candidate_custody_does_not_imply_publication_readiness(self):
        for phrase in ("gitignored, repository-local candidate bundle",
                       "9 unreviewed extracts of owner prompts",
                       "Every record carries a source SHA-256",
                       "all 9 source hashes match whole retained session files",
                       "all 9 entries match the ratified table",
                       "independent scan found zero leaks",
                       "did not inspect the candidate path or raw private content",
                       "must not be committed, published, exposed or copied"):
            self.assertIn(phrase, self.series)
        self.assertIn("The publication verdict is therefore **not ready**", self.series)
        self.assertIn("Managed Estuary access and results are unavailable", self.series)
        self.assertIn("exact PostgreSQL protocol literals remain pending", self.series)
        self.assertIn("Negative, failed and unfavorable results must remain", self.series)

    def test_series_separates_baseline_event_delivery_and_convergence(self):
        for phrase in ("Source baseline/current state", "Ledger delivery",
                       "Independently observed business-event delivery",
                       "Final-state convergence", "business-only loss",
                       "event delivery as **unavailable**"):
            self.assertIn(phrase, self.series)
        self.assertIn("Never downgrade the local oracle", self.series)

    def test_series_retains_redacted_story_without_reconstruction(self):
        for artifact in ("originating-prompt.redacted", "first-output.redacted",
                         "failures.jsonl", "human-corrections.jsonl",
                         "interventions.jsonl"):
            self.assertIn(artifact, self.series)
        self.assertIn("record it as unavailable—do not reconstruct", self.series)
        self.assertIn("no secrets", self.series.lower())

    def test_series_access_and_editorial_states_fail_closed(self):
        self.assertIn("1c99e72d-3878-4f3b-9cdc-663f97657b3e", self.series)
        for phrase in ("single pending request", "managed access and every managed result are **unavailable**",
                       "requested details remain **unverified**", "`verified`, `manual`, or `unavailable`",
                       "None is currently publication-ready", "no approval is automatic",
                       "no predetermined winner"):
            self.assertIn(phrase, self.series)
        self.assertIn("No managed resource", self.series)

    def test_series_records_unsafe_content_checkout_blocker(self):
        self.assertIn("335ada9", self.series)
        self.assertIn("62c02e13110820d3289fc36932c80dc42ced774c721ce237a868df1b3d872ff4",
                      self.series)
        self.assertIn("checkout was therefore unsafe and was left untouched", self.series)
        self.assertIn("keeps `boring-cdc-v01.5` open", self.series)
        self.assertIn("no comparison may be commissioned", self.series)

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
