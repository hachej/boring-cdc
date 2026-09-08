import hashlib, json, os, subprocess, tempfile, unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / "scripts/lib/core_validator.py"
CLOSE = ROOT / "scripts/validate/close_guard.sh"
F = ROOT / "tests/fixtures/m0-core"
ZERO = hashlib.sha256(b"").hexdigest()
HEAD = subprocess.run(["git","rev-parse","HEAD"],cwd=ROOT,text=True,capture_output=True,check=True).stdout.strip()
PROOF_FIELDS = ["targeted_checks","boundary_e2e","fault_suite","deterministic_rerun","consumed_contract_vectors","workspace_tests","integration","clean_environment","exit_assertions","endurance","full_failure_matrix","clean_clone"]

def run(*args):
    return subprocess.run(["python3", str(CLI), *map(str, args)], cwd=ROOT, text=True, capture_output=True)

def close(root, path):
    return subprocess.run([str(CLOSE), root, str(path)], cwd=ROOT, text=True, capture_output=True)

def evidence(**overrides):
    proof = {name: False for name in PROOF_FIELDS}
    for name in ("targeted_checks","boundary_e2e","fault_suite","deterministic_rerun","consumed_contract_vectors"): proof[name] = True
    doc = {
        "schema_version":"evidence/v1", "owner_bead":"boring-cdc-m0.1", "scenario_id":"SCN-M0-CORE",
        "evidence_profile":"documentation", "evidence_tier":"component", "seed":"m0-core-v1", "git_commit":HEAD,
        "commands":[{"argv":"python3 --version","version":"1","exit_code":0,"stdout_path":"tests/fixtures/m0-core/valid/empty.txt","stdout_sha256":ZERO,"stderr_path":"tests/fixtures/m0-core/valid/empty.txt","stderr_sha256":ZERO},{"argv":"br --version","version":"1","exit_code":0,"stdout_path":"tests/fixtures/m0-core/valid/empty.txt","stdout_sha256":ZERO,"stderr_path":"tests/fixtures/m0-core/valid/empty.txt","stderr_sha256":ZERO}],
        "source_preservation":{"before_sha256":"b"*64,"after_sha256":"b"*64,"preserved":True},
        "cleanup":{"complete":True,"remaining_paths":[]}, "redaction":{"checked":True,"secrets_found":0},
        "tier_proof":proof,
        "result":{"status":"pass","digest":ZERO,"artifacts":["tests/fixtures/m0-core/valid/empty.txt"],"product_faults":"fault_not_applicable","runtime_observed":False},
    }
    doc.update(overrides); return doc

class Core(unittest.TestCase):
    def assertCode(self, cp, code):
        self.assertNotEqual(cp.returncode, 0, cp.stdout + cp.stderr)
        payload = json.loads(cp.stdout)
        self.assertIn(code, [item["code"] for item in payload["findings"]])

    def test_valid_contracts(self):
        valid = F / "valid"
        cases = [
            ("decisions",valid/"decisions.json","--complete","--owners",valid/"owners.json","--fixtures",valid/"fixtures.json","--executors",valid/"executors.json","--expected-decisions",valid/"expected-decisions.json"),
            ("artifacts",valid/"artifacts.json","--complete","--expected-artifacts",valid/"expected-artifacts.json"), ("runbooks",valid/"runbooks.json","--release"),
            ("graph",valid/"graph.jsonl","--output",valid/"normalized.tmp.json"),
        ]
        try:
            for case in cases:
                cp = run(*case); self.assertEqual(cp.returncode, 0, cp.stdout + cp.stderr); self.assertEqual(json.loads(cp.stdout)["status"], "pass")
        finally: (valid/"normalized.tmp.json").unlink(missing_ok=True)

    def test_empty_skeletons_valid_but_not_complete(self):
        self.assertEqual(run("decisions",ROOT/"contracts/m0/decisions.json").returncode, 0)
        self.assertCode(run("decisions",ROOT/"contracts/m0/decisions.json","--complete"), "E_DECISIONS_EMPTY")
        self.assertEqual(run("artifacts",ROOT/"contracts/m0/artifacts.json").returncode, 0)
        self.assertCode(run("artifacts",ROOT/"contracts/m0/artifacts.json","--complete"), "E_ARTIFACTS_EMPTY")

    def test_complete_rejects_missing_inventories_declared_artifacts_and_hash_mismatch(self):
        valid = F/"valid"
        self.assertCode(run("decisions",valid/"decisions.json","--complete"), "E_INVENTORY_REQUIRED")
        with tempfile.TemporaryDirectory(dir=ROOT/"tests") as td:
            p = Path(td)/"artifact.json"
            p.write_text(json.dumps({"schema_version":"m0-artifacts/v1","artifacts":[{"id":"ART-X","owner_bead":"boring-cdc-x","path":"missing","sha256":"a"*64,"status":"declared"}]}))
            self.assertCode(run("artifacts",p,"--complete"), "E_ARTIFACT_INCOMPLETE")
            d = json.loads((valid/"decisions.json").read_text()); d["decisions"][0]["fixture_sha256"] = "0"*64; p.write_text(json.dumps(d))
            self.assertCode(run("decisions",p,"--owners",valid/"owners.json","--fixtures",valid/"fixtures.json","--executors",valid/"executors.json"), "E_HASH_MISMATCH")
            p.write_text('["boring-cdc-d-synthetic","boring-cdc-d-synthetic"]')
            self.assertCode(run("decisions",valid/"decisions.json","--owners",p,"--fixtures",valid/"fixtures.json","--executors",valid/"executors.json"), "E_DUPLICATE_OWNER")
            p.write_text('[]')
            self.assertCode(run("decisions",valid/"decisions.json","--owners",valid/"owners.json","--fixtures",valid/"fixtures.json","--executors",p), "E_EXECUTOR_UNKNOWN")
            p.write_text('["DEC-SYNTHETIC","DEC-OMITTED"]')
            self.assertCode(run("decisions",valid/"decisions.json","--complete","--owners",valid/"owners.json","--fixtures",valid/"fixtures.json","--executors",valid/"executors.json","--expected-decisions",p), "E_DECISION_MISSING")
            approval=json.loads((valid/"decisions.json").read_text()); approval["decisions"][0]["approval"].update({"approved_by":"","approved_at":"","extra":"x"}); p.write_text(json.dumps(approval))
            cp=run("decisions",p,"--owners",valid/"owners.json","--fixtures",valid/"fixtures.json","--executors",valid/"executors.json")
            self.assertCode(cp,"E_UNRESOLVED"); self.assertCode(cp,"E_UNKNOWN_FIELD")
            p.write_text('{"schema_version":"m0-decisions/v1","decisions":[{}]}')
            cp=run("decisions",p,"--complete","--owners",valid/"owners.json","--fixtures",valid/"fixtures.json","--executors",valid/"executors.json","--expected-decisions",valid/"expected-decisions.json")
            self.assertCode(cp,"E_REQUIRED"); self.assertCode(cp,"E_DECISION_MISSING")
            p.write_text('{"schema_version":"m0-artifacts/v1","artifacts":[{}]}')
            cp=run("artifacts",p,"--complete","--expected-artifacts",valid/"expected-artifacts.json")
            self.assertCode(cp,"E_REQUIRED"); self.assertCode(cp,"E_ARTIFACT_MISSING")

    def test_invalid_reason_codes_and_determinism(self):
        bad=F/"invalid"; cases=[(("decisions",bad/"decisions-duplicate.json"),"E_DUPLICATE_ID"),(("artifacts",bad/"artifact-traversal.json"),"E_PATH_TRAVERSAL"),(("runbooks",bad/"runbook-gap.json"),"E_PROCEDURE_GAP"),(("graph",bad/"graph-cycle.jsonl"),"E_GRAPH_CYCLE"),(("artifacts",bad/"duplicate-key.json"),"E_DUPLICATE_KEY")]
        for argv, code in cases:
            before=hashlib.sha256(Path(argv[1]).read_bytes()).hexdigest(); a=run(*argv); b=run(*argv)
            self.assertCode(a,code); self.assertEqual(a.stdout,b.stdout); self.assertEqual(before,hashlib.sha256(Path(argv[1]).read_bytes()).hexdigest())

    def test_non_scalar_ids_and_inventory_entries_fail_as_stable_json(self):
        valid=F/"valid"
        decision_args=("--complete","--owners",valid/"owners.json","--fixtures",valid/"fixtures.json","--executors",valid/"executors.json","--expected-decisions",valid/"expected-decisions.json")
        with tempfile.TemporaryDirectory(dir=ROOT/"tests") as td:
            p=Path(td)/"hostile.json"
            for value in ([], {}):
                decisions=json.loads((valid/"decisions.json").read_text()); decisions["decisions"][0]["id"]=value; p.write_text(json.dumps(decisions))
                first=run("decisions",p,*decision_args); second=run("decisions",p,*decision_args)
                self.assertCode(first,"E_ID_INVALID"); self.assertEqual(first.stdout,second.stdout); self.assertFalse(first.stderr)
                for field,code in (("owner_bead","E_OWNER_INVALID"),("fixture_spec","E_PATH_INVALID")):
                    hostile=json.loads((valid/"decisions.json").read_text()); hostile["decisions"][0][field]=value; p.write_text(json.dumps(hostile))
                    first=run("decisions",p,*decision_args); second=run("decisions",p,*decision_args)
                    self.assertCode(first,code); self.assertEqual(first.stdout,second.stdout); self.assertFalse(first.stderr)
                hostile=json.loads((valid/"decisions.json").read_text()); hostile["decisions"][0]["executor_beads"]=[value]; p.write_text(json.dumps(hostile))
                first=run("decisions",p,*decision_args); second=run("decisions",p,*decision_args)
                self.assertCode(first,"E_OWNER_INVALID"); self.assertEqual(first.stdout,second.stdout); self.assertFalse(first.stderr)

                artifacts=json.loads((valid/"artifacts.json").read_text()); artifacts["artifacts"][0]["id"]=value; p.write_text(json.dumps(artifacts))
                first=run("artifacts",p,"--complete","--expected-artifacts",valid/"expected-artifacts.json"); second=run("artifacts",p,"--complete","--expected-artifacts",valid/"expected-artifacts.json")
                self.assertCode(first,"E_ID_INVALID"); self.assertEqual(first.stdout,second.stdout); self.assertFalse(first.stderr)

                runbooks=json.loads((valid/"runbooks.json").read_text())
                for field in ("id","condition_id","action_id"):
                    hostile=json.loads(json.dumps(runbooks)); hostile["runbooks"][0][field]=value; p.write_text(json.dumps(hostile))
                    first=run("runbooks",p,"--release"); second=run("runbooks",p,"--release")
                    self.assertCode(first,"E_ID_INVALID"); self.assertEqual(first.stdout,second.stdout); self.assertFalse(first.stderr)

            for option in ("owners","fixtures","executors","expected-decisions"):
                p.write_text(json.dumps([[],{}]))
                args=list(decision_args); args[args.index("--"+option.replace("_","-"))+1]=p
                first=run("decisions",valid/"decisions.json",*args); second=run("decisions",valid/"decisions.json",*args)
                self.assertCode(first,"E_INVENTORY_ITEM"); self.assertEqual(first.stdout,second.stdout); self.assertFalse(first.stderr)

    def test_non_scalar_graph_dependency_ids_fail_as_stable_json(self):
        with tempfile.TemporaryDirectory(dir=ROOT/"tests") as td:
            p=Path(td)/"graph.jsonl"
            for value in ([], {}):
                rows=[{"id":"root","status":"open","dependencies":[{"issue_id":"root","depends_on_id":value,"type":"blocks"}]}]
                p.write_text("\n".join(map(json.dumps,rows))+"\n")
                for invoke in (lambda: run("graph",p), lambda: close("root",p)):
                    first=invoke(); second=invoke()
                    self.assertCode(first,"E_ID_INVALID"); self.assertEqual(first.stdout,second.stdout); self.assertFalse(first.stderr)

    def test_safety_schemas_require_transition_ownership_and_log_correlation(self):
        cases = [
            ({"schema_version":"plan-to-beads/v1","assignments":[{"id":"TRANS-SYNTHETIC","source":"synthetic","source_digest":"0000000000000000000000000000000000000000000000000000000000000000","evidence_status":"pending"}]}, ROOT/"contracts/coverage/plan-to-beads.schema.json", "/assignments/0/owner_bead"),
            ({"schema_version":"structured-log/v1","level":"error","code":"E_SYNTHETIC","message":"redacted"}, ROOT/"contracts/common/structured-log.schema.json", "/correlation_id"),
        ]
        with tempfile.TemporaryDirectory(dir=ROOT/"tests") as td:
            instance=Path(td)/"hostile.json"
            for document,schema,pointer in cases:
                instance.write_text(json.dumps(document))
                first=run("schema",instance,"--schema",schema); second=run("schema",instance,"--schema",schema)
                self.assertCode(first,"E_SCHEMA_REQUIRED"); self.assertEqual(first.stdout,second.stdout); self.assertFalse(first.stderr)
                findings=json.loads(first.stdout)["findings"]
                self.assertEqual([(item["code"],item["pointer"]) for item in findings], [("E_SCHEMA_REQUIRED",pointer)])

    def test_symlink_parent_escape_and_missing_graph_are_stable_failures(self):
        with tempfile.TemporaryDirectory() as outside, tempfile.TemporaryDirectory(dir=ROOT/"tests") as td:
            outside_file=Path(outside)/"secret"; outside_file.write_text("secret")
            link=Path(td)/"escape"; link.symlink_to(outside, target_is_directory=True)
            rel=link.relative_to(ROOT)/"secret"; manifest=Path(td)/"artifact.json"
            manifest.write_text(json.dumps({"schema_version":"m0-artifacts/v1","artifacts":[{"id":"ART-X","owner_bead":"boring-cdc-x","path":str(rel),"sha256":hashlib.sha256(b"secret").hexdigest(),"status":"complete"}]}))
            self.assertCode(run("artifacts",manifest,"--complete"), "E_PATH_SYMLINK")
        cp=run("graph",ROOT/"tests/definitely-not-present.jsonl"); self.assertCode(cp,"E_INPUT_MISSING"); self.assertFalse(cp.stderr)

    def test_evidence_profiles_tiers_redaction_cleanup_and_types(self):
        with tempfile.TemporaryDirectory(dir=ROOT/"tests") as td:
            p=Path(td)/"manifest.json"; p.write_text(json.dumps(evidence())); self.assertEqual(run("evidence",p).returncode,0)
            cases=[(evidence(evidence_profile="future"),"E_EVIDENCE_PROFILE"),(evidence(cleanup={"complete":False,"remaining_paths":["x"]}),"E_CLEANUP_INCOMPLETE"),(evidence(result={"status":"pass","digest":"c"*64,"product_faults":"tested","runtime_observed":True}),"E_FORWARD_RUNTIME_EVIDENCE"),(evidence(seed="password=bad"),"E_SECRET")]
            for doc,code in cases: p.write_text(json.dumps(doc)); self.assertCode(run("evidence",p),code)
            bogus=evidence(evidence_profile="runtime", evidence_tier="release")
            bogus["commands"][0]["exit_code"]="0"; bogus["commands"][0]["argv"]="definitely-not-a-command"; bogus["git_commit"]="a"*40; bogus["source_preservation"]={"before_sha256":"bogus","after_sha256":"bogus","preserved":True}; bogus["result"]={"status":"invented","digest":"c"*64,"artifacts":["tests/fixtures/m0-core/valid/empty.txt"],"product_faults":"pass","runtime_observed":False}
            p.write_text(json.dumps(bogus)); cp=run("evidence",p)
            for code in ("E_EXIT_CODE","E_COMMAND_MISSING","E_GIT_OBJECT","E_DIGEST","E_RESULT_STATUS","E_RESULT_DIGEST","E_RUNTIME_PROVENANCE","E_TIER_PROOF"): self.assertCode(cp,code)
            external=evidence(evidence_profile="external_managed"); p.write_text(json.dumps(external)); self.assertCode(run("evidence",p),"E_EXTERNAL_PROVENANCE")
            stale=evidence(git_commit="0b034abb0bd02144df8785305ec63caacd655c2a"); p.write_text(json.dumps(stale)); self.assertCode(run("evidence",p),"E_EVIDENCE_STALE")
            not_executable=evidence(); not_executable["commands"][0]["argv"]="tests/fixtures/m0-core/valid/empty.txt"; p.write_text(json.dumps(not_executable)); self.assertCode(run("evidence",p),"E_COMMAND_NOT_EXECUTABLE")
            release=evidence(evidence_tier="release"); release["tier_proof"].update({name:True for name in PROOF_FIELDS}); release["tier_proof"]["boundary_e2e"]=False
            p.write_text(json.dumps(release)); self.assertCode(run("evidence",p),"E_TIER_PROOF")

    def test_runbook_unknown_stage_and_graph_leaf_orientation(self):
        with tempfile.TemporaryDirectory(dir=ROOT/"tests") as td:
            p=Path(td)/"runbooks.json"; p.write_text('{"schema_version":"runbooks-index/v1","stage":"nonsense","runbooks":[]}')
            self.assertCode(run("runbooks",p),"E_STAGE")
            expected=Path(td)/"leaves.json"; expected.write_text('["synthetic-leaf"]')
            cp=run("graph",F/"valid/graph.jsonl","--expect-root",expected); self.assertEqual(cp.returncode,0,cp.stdout+cp.stderr)

    def test_close_guard_traverses_hierarchy_children_and_blockers(self):
        with tempfile.TemporaryDirectory(dir=ROOT/"tests") as td:
            p=Path(td)/"graph.jsonl"
            rows=[{"id":"root","status":"closed","dependencies":[]},{"id":"child","status":"open","dependencies":[{"issue_id":"child","depends_on_id":"root","type":"parent-child"}]}]
            p.write_text("\n".join(map(json.dumps,rows))+"\n"); self.assertCode(close("root",p),"E_CLOSE_BLOCKED")
            rows[1]["status"]="closed"; p.write_text("\n".join(map(json.dumps,rows))+"\n"); self.assertEqual(close("root",p).returncode,0)
            p.write_text('{"id":"root","status":"open","status":"closed","dependencies":[]}\n'); self.assertCode(close("root",p),"E_DUPLICATE_KEY")

    def test_graph_baseline_witness_and_source_isolation(self):
        self.assertCode(run("graph",F/"invalid/graph-stale.jsonl","--baseline",F/"valid/graph.jsonl"),"E_GRAPH_MISSING")
        self.assertCode(run("graph",F/"invalid/graph-stale.jsonl","--baseline",F/"valid/graph.jsonl"),"E_GRAPH_EXTRA")
        self.assertCode(run("graph",F/"invalid/graph-stale.jsonl","--baseline",F/"valid/graph.jsonl"),"E_GRAPH_STALE")
        self.assertCode(run("graph",F/"valid/graph.jsonl","--witness-root","0"*64),"E_WITNESS_MISMATCH")
        lines=(F/"valid/graph.jsonl").read_text().splitlines()
        with tempfile.NamedTemporaryFile("w",dir=ROOT/"tests",delete=False) as file: file.write("\n".join(reversed(lines))+"\n"); name=file.name
        dirty=ROOT/"tests/.dirty-unrelated"
        try:
            dirty.write_text("moving remote and dirty worktree cannot hydrate captured input")
            before=hashlib.sha256(Path(name).read_bytes()).hexdigest(); self.assertEqual(run("graph",name).returncode,0); self.assertEqual(before,hashlib.sha256(Path(name).read_bytes()).hexdigest())
        finally: Path(name).unlink(); dirty.unlink(missing_ok=True)

    def test_owned_schemas_close_nested_object_shapes(self):
        for path in (ROOT/"contracts/evidence.schema.json",ROOT/"contracts/graph/pinned.schema.json",ROOT/"contracts/common/validation-result.schema.json"):
            schema=json.loads(path.read_text())
            def walk(node):
                if isinstance(node,dict):
                    if node.get("type")=="object": self.assertIs(node.get("additionalProperties"),False,path)
                    for value in node.values(): walk(value)
                elif isinstance(node,list):
                    for value in node: walk(value)
            walk(schema)

if __name__ == "__main__": unittest.main()
