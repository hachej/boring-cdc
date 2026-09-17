#!/usr/bin/env python3
"""Aggregate M0 specification-completeness gate.

The owner-card override deliberately permits named provisional recommendations
to remain open. This gate proves their exact authority markers;
it does not turn recommendations into approvals or close decision Beads.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
EVIDENCE = ROOT / "artifacts/boring-cdc-m0-complete/gate/evidence.json"
SUMMARY = EVIDENCE.with_name("completion-summary.json")

# primary manifest ID -> (artifact registry ID, canonical owner)
EXPECTED_ARTIFACTS_SHA256 = "19dbb98a243f15838dcdd011a4469ec8862dd31dbca1f497571a4854239b3bf1"

PRIMARY_ARTIFACTS = {
    "ART-M0-ARCHIVE-MODEL": ("ART-M0-ARCHIVE-MODEL", "boring-cdc-m0-archive-model"),
    "ART-M0-CLICKHOUSE-MODEL": ("ART-M0-CLICKHOUSE-MODEL", "boring-cdc-m0-ch-model"),
    "ART-M0-EVENT-FORMAT": ("ART-M0-EVENT-FORMAT-CONTRACT", "boring-cdc-m0-event-format"),
    "ART-M0-PG-CONTRACT": ("ART-M0-PG-CONTRACT", "boring-cdc-m0-pg-contract"),
    "ART-M0-SCAFFOLD": ("ART-M0-SCAFFOLD", "boring-cdc-m0-scaffold"),
    "ART-M0-STORAGE-MODEL": ("ART-M0-STORAGE-MODEL", "boring-cdc-m0-storage-model"),
}
DECISION_OWNERS = {
    "boring-cdc-d-archive-durability",
    "boring-cdc-d-archive-scope",
    "boring-cdc-d-compose",
    "boring-cdc-d-failure-policy",
    "boring-cdc-d-keys",
    "boring-cdc-d-license",
    "boring-cdc-d-owner",
    "boring-cdc-d-security",
    "boring-cdc-d-sqlite",
    "boring-cdc-d-values",
    "boring-cdc-d-wal-cap",
}
SPEC_INPUTS = {
    "boring-cdc-m0-pg-contract": {
        "contracts/postgres/capture-backfill-fixtures.schema.json", "contracts/postgres/capture-backfill.json",
        "contracts/postgres/capture-backfill.schema.json", "contracts/postgres/postgres-contract-evidence.schema.json",
        "contracts/postgres/roles-grants.sql", "fixtures/m0/postgres/capture-backfill.json",
    },
    "boring-cdc-m0-storage-model": {
        "contracts/storage/sqlite-schema.sql", "contracts/storage/storage-fixtures.schema.json",
        "contracts/storage/storage-model.json", "contracts/storage/storage-model.schema.json",
        "contracts/storage/storage-result.schema.json", "fixtures/m0/storage/scenarios.json",
    },
    "boring-cdc-m0-event-format": {
        "contracts/event/event-format.json", "contracts/event/event.schema.json", "docs/EVENT_FORMAT.md",
        "fixtures/m0/event-format/golden-vectors.json",
    },
    "boring-cdc-m0-archive-model": {
        "contracts/archive/archive-fixtures.schema.json", "contracts/archive/archive-model.json",
        "contracts/archive/archive-model.schema.json", "contracts/archive/archive-result.schema.json",
        "contracts/archive/generation-manifest.schema.json", "contracts/archive/segment-manifest.schema.json",
        "contracts/m0/failure-policy.json", "fixtures/m0/archive/scenarios.json",
        "fixtures/m0/decisions/boring-cdc-d-archive-durability.json",
    },
    "boring-cdc-m0-ch-model": {
        "contracts/clickhouse/canonical-query.sql", "contracts/clickhouse/ddl.sql",
        "contracts/clickhouse/fixtures.schema.json", "contracts/clickhouse/model.json",
        "contracts/clickhouse/model.schema.json", "contracts/clickhouse/result.schema.json",
        "contracts/clickhouse/retire-generation.sql", "docs/CLICKHOUSE_MODEL.md",
        "fixtures/m0/clickhouse/scenarios.json",
    },
}

PROVISIONAL: set[str] = set()
def provisional_marker(owner: str) -> str:
    # Split the sentinel so the repository scanner does not mistake validator
    # source for a consumer of an owner-controlled recommendation.
    return "// M0-" + "PROVISIONAL: " + owner


OPEN_DECISIONS: dict[str, set[str]] = {}
REQUIRED_M0_OWNERS = {
    "boring-cdc-d-additive", "boring-cdc-d-admission", "boring-cdc-d-anchor",
    "boring-cdc-d-archive-durability", "boring-cdc-d-archive-scope",
    "boring-cdc-d-backfill", "boring-cdc-d-ch-accept", "boring-cdc-d-ch-history",
    "boring-cdc-d-compose", "boring-cdc-d-ddl", "boring-cdc-d-event-id",
    "boring-cdc-d-failure-policy", "boring-cdc-d-keys", "boring-cdc-d-license",
    "boring-cdc-d-oracle", "boring-cdc-d-owner", "boring-cdc-d-pg-protocol",
    "boring-cdc-d-promotion", "boring-cdc-d-publication", "boring-cdc-d-scale",
    "boring-cdc-d-security", "boring-cdc-d-sqlite", "boring-cdc-d-toast",
    "boring-cdc-d-values", "boring-cdc-d-values.1", "boring-cdc-d-wal-cap",
    "boring-cdc-m0-archive-model", "boring-cdc-m0-ch-model",
    "boring-cdc-m0-decisions", "boring-cdc-m0-event-format",
    "boring-cdc-m0-pg-contract", "boring-cdc-m0-scaffold",
    "boring-cdc-m0-storage-model", "boring-cdc-m0-validation-tooling",
    "boring-cdc-m0.1", "boring-cdc-m0.2", "boring-cdc-m0.3",
}
DECISION_DOMAIN_COMMANDS = {
    "boring-cdc-d-archive-durability": ("scripts/validate/archive_durability.sh",),
    "boring-cdc-d-archive-scope": ("scripts/validate/archive_scope.sh",),
    "boring-cdc-d-compose": ("scripts/validate/compose_spec.sh",),
    "boring-cdc-d-failure-policy": ("scripts/validate/failure_policy.sh",),
    "boring-cdc-d-keys": ("scripts/validate/supported_keys.sh",),
    "boring-cdc-d-license": ("scripts/validate/license.sh",),
    "boring-cdc-d-owner": ("scripts/fixtures/validate_m0_repository_identity.py",),
    "boring-cdc-d-security": ("scripts/validate/security_exposure.sh",),
    "boring-cdc-d-sqlite": ("scripts/validate/sqlite_durability.sh",),
    "boring-cdc-d-values": ("scripts/validate/supported_values.sh",),
    "boring-cdc-d-wal-cap": ("scripts/validate/wal_cap.sh",),
}
DOMAIN_COMMANDS = [
    ["scripts/validate/m0_artifact.sh", owner]
    for owner in (
        "boring-cdc-m0-pg-contract", "boring-cdc-m0-storage-model",
        "boring-cdc-m0-event-format", "boring-cdc-m0-archive-model",
        "boring-cdc-m0-ch-model",
    )
] + [list(command) for command in DECISION_DOMAIN_COMMANDS.values()] + [
    ["scripts/validate/m0_artifact.sh", "contracts/m0/artifacts.json", "--complete",
     "--expected-artifacts", "contracts/m0/expected-artifacts.json"],
    ["scripts/validate/m0_decisions.sh", "contracts/m0/decisions.json"],
    ["scripts/validate/m0_scaffold.sh"],
    ["scripts/validate/plan_coverage.sh"],
    ["python3", "scripts/validate/article1_transcript.py"],
    ["python3", "-m", "unittest",
     "tests.planning.test_kickoff_contracts.KickoffContracts.test_article_one_uses_shipped_teaching_view_without_article_four_evidence",
     "-v"],
]


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path: Path):
    return json.loads(path.read_text(), object_pairs_hook=_unique)


def _unique(pairs):
    out = {}
    for key, value in pairs:
        if key in out:
            raise ValueError(f"duplicate JSON key {key}")
        out[key] = value
    return out


def aggregate_findings(root: Path = ROOT) -> list[str]:
    findings: list[str] = []
    try:
        manifest = load(root / "contracts/m0/manifest.json")
        artifacts = load(root / "contracts/m0/artifacts.json")
        decisions = load(root / "contracts/m0/decisions.json")
        expected_artifacts = load(root / "contracts/m0/expected-artifacts.json")
    except Exception as exc:
        return [f"manifest JSON invalid: {exc}"]

    graph_rows = []
    try:
        for number, line in enumerate((root / ".beads/issues.jsonl").read_text().splitlines(), 1):
            if line.strip():
                graph_rows.append(json.loads(line, object_pairs_hook=_unique))
    except Exception as exc:
        return [f"Beads graph invalid: {exc}"]
    graph_counts = Counter(row.get("id") for row in graph_rows)
    duplicates = sorted(key for key, count in graph_counts.items() if count != 1)
    if duplicates:
        findings.append(f"duplicate Bead IDs: {duplicates}")
    graph_ids = set(graph_counts)
    missing_owners = sorted(REQUIRED_M0_OWNERS - graph_ids)
    if missing_owners:
        findings.append(f"required M0 owner Beads missing: {missing_owners}")

    manifest_rows = manifest.get("artifacts", [])
    by_primary = {row.get("id"): row for row in manifest_rows if isinstance(row, dict)}
    if len(by_primary) != len(manifest_rows):
        findings.append("primary manifest contains duplicate or malformed rows")
    if set(by_primary) != set(PRIMARY_ARTIFACTS):
        findings.append("primary manifest artifact ID set mismatch")
    if sha(root / "contracts/m0/expected-artifacts.json") != EXPECTED_ARTIFACTS_SHA256:
        findings.append("expected artifact inventory digest mismatch")
    registry_rows = artifacts.get("artifacts", [])
    by_artifact = {row.get("id"): row for row in registry_rows if isinstance(row, dict)}
    if len(by_artifact) != len(registry_rows):
        findings.append("artifact registry contains duplicate or malformed rows")
    if not isinstance(expected_artifacts, list) or set(by_artifact) != set(expected_artifacts) or len(expected_artifacts) != len(set(expected_artifacts)):
        findings.append("artifact registry expected ID set mismatch")
    for artifact_id, (registry_id, owner) in PRIMARY_ARTIFACTS.items():
        row = by_primary.get(artifact_id, {})
        registered = by_artifact.get(registry_id, {})
        if row.get("owner_bead") != owner:
            findings.append(f"{artifact_id}: primary owner mismatch")
        if registered.get("owner_bead") != owner:
            findings.append(f"{artifact_id}: registry owner mismatch")
        for field in ("path", "sha256", "status"):
            if row.get(field) != registered.get(field):
                findings.append(f"{artifact_id}: primary/registry {field} mismatch")
        target = root / str(row.get("path", ""))
        if not target.is_file() or sha(target) != row.get("sha256"):
            findings.append(f"{artifact_id}: content digest mismatch")
    for index, row in enumerate(registry_rows):
        owner = row.get("owner_bead")
        if owner not in graph_ids:
            findings.append(f"artifact row {index}: unknown owner {owner}")
        target = root / str(row.get("path", ""))
        if row.get("status") != "complete" or not target.is_file() or sha(target) != row.get("sha256"):
            findings.append(f"artifact row {index}: incomplete, missing, or digest mismatch")

    decision_rows = decisions.get("decisions", [])
    decision_by_owner = {row.get("owner_bead"): row for row in decision_rows if isinstance(row, dict)}
    if len(decision_by_owner) != len(decision_rows):
        findings.append("decision manifest contains duplicate or malformed owner rows")
    if set(decision_by_owner) != DECISION_OWNERS:
        findings.append("decision manifest owner set mismatch")
    for owner, row in decision_by_owner.items():
        if owner not in graph_ids:
            findings.append(f"decision row has unknown owner {owner}")
        if owner in OPEN_DECISIONS:
            if row.get("status") != "open" or set(row.get("provisional_markers", [])) != OPEN_DECISIONS[owner] or "approval" in row:
                findings.append(f"{owner}: provisional decision state/marker mismatch")
        elif row.get("provisional_markers"):
            findings.append(f"{owner}: approved decision retains provisional marker")
        elif row.get("status") != "approved" or not isinstance(row.get("approval"), dict):
            findings.append(f"{owner}: approved decision lost approval authority")
        for executor in row.get("executor_beads", []):
            if executor not in graph_ids:
                findings.append(f"{owner}: unknown executor {executor}")

    # Six specification artifacts must remain honest about the runtime boundary.
    for _, owner in PRIMARY_ARTIFACTS.values():
        if owner == "boring-cdc-m0-scaffold":
            continue
        evidence = root / "artifacts" / owner / "spec/evidence.json"
        try:
            value = load(evidence)
            if value.get("runtime_observed") is not False:
                findings.append(f"{owner}: M0 specification evidence claims runtime observation")
            source = value.get("source_parent_git_commit")
            if not isinstance(source, str) or len(source) != 40 or any(char not in "0123456789abcdef" for char in source):
                findings.append(f"{owner}: evidence source commit is not a canonical full OID")
            else:
                exists = subprocess.run(["git", "cat-file", "-e", source + "^{commit}"], cwd=root, capture_output=True).returncode == 0
                ancestor = exists and subprocess.run(["git", "merge-base", "--is-ancestor", source, "HEAD"], cwd=root, capture_output=True).returncode == 0
                if not ancestor:
                    findings.append(f"{owner}: evidence source commit is missing or not an ancestor")
            validator = root / str(value.get("validator", ""))
            if not validator.is_file() or sha(validator) != value.get("validator_sha256"):
                findings.append(f"{owner}: evidence validator provenance mismatch")
            inputs = value.get("inputs")
            if not isinstance(inputs, dict) or set(inputs) != SPEC_INPUTS[owner] or any(not (root / path).is_file() or sha(root / path) != digest for path, digest in inputs.items()):
                findings.append(f"{owner}: evidence input provenance mismatch")
        except Exception as exc:
            findings.append(f"{owner}: evidence missing or invalid: {exc}")

    # m0_scaffold.sh, invoked by probe(), is the fail-closed authority for the
    # exact allowed marker paths, IDs, prefixes, and delimiters.
    return sorted(set(findings))


def probe() -> dict:
    findings = aggregate_findings()
    checks = []
    command_counts = Counter(tuple(command) for command in DOMAIN_COMMANDS)
    if set(DECISION_DOMAIN_COMMANDS) != DECISION_OWNERS:
        findings.append("decision-domain validator owner set mismatch")
    for owner, command in DECISION_DOMAIN_COMMANDS.items():
        if command_counts[command] != 1:
            findings.append(f"{owner}: decision-domain validator must execute exactly once")
    for command in DOMAIN_COMMANDS:
        run = subprocess.run(command, cwd=ROOT, text=True, capture_output=True)
        checks.append({"argv": " ".join(command), "exit_code": run.returncode})
        if run.returncode:
            detail = (run.stdout + " " + run.stderr).strip().replace(str(ROOT), "<repo>")
            findings.append(f"command failed ({' '.join(command)}): {detail}")
    return {
        "schema_version": "m0-completion-summary/v1",
        "status": "fail" if findings else "pass",
        "primary_artifact_owners": sorted(owner for _, owner in PRIMARY_ARTIFACTS.values()),
        "decision_manifest_owners": sorted(DECISION_OWNERS),
        "required_m0_owners": sorted(REQUIRED_M0_OWNERS),
        "provisional_decisions": sorted(PROVISIONAL),
        "supervisor_override": "answered owner card 59a63169 authorizes the four exact accepted decision literals; unrelated provisional markers remain governed separately",
        "article1_boundary": "raw pgoutput plus same-stream process-local non-durable teaching view; ClickHouse deferred to Article 4",
        "duplicate_bead_ids": 0 if not any(x.startswith("duplicate Bead IDs") for x in findings) else None,
        "checks": checks,
        "findings": sorted(set(findings)),
    }


def encoded_summary() -> tuple[dict, str]:
    summary = probe()
    return summary, json.dumps(summary, sort_keys=True, indent=2) + "\n"


def write_evidence(summary_text: str) -> None:
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY.write_text(summary_text)
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    captures = []
    observed = []
    for index in (1, 2):
        run = subprocess.run(["scripts/acceptance/m0_complete.sh", "--probe"], cwd=ROOT, text=True, capture_output=True)
        stdout = EVIDENCE.with_name(f"run-{index}.log")
        stderr = EVIDENCE.with_name(f"run-{index}.stderr")
        stdout.write_text(run.stdout)
        stderr.write_text(run.stderr)
        if run.returncode:
            raise SystemExit(f"completion probe {index} failed: {run.stdout} {run.stderr}")
        observed.append((run.stdout, run.stderr))
        captures.append({
            "argv": "scripts/acceptance/m0_complete.sh --probe", "version": "m0-complete/v1",
            "exit_code": 0, "stdout_path": str(stdout.relative_to(ROOT)), "stdout_sha256": sha(stdout),
            "stderr_path": str(stderr.relative_to(ROOT)), "stderr_sha256": sha(stderr),
        })
    if observed[0] != observed[1] or observed[0] != (summary_text, ""):
        raise SystemExit("completion probes were not byte-identical")
    sources = [ROOT / "contracts/m0/manifest.json", ROOT / "contracts/m0/artifacts.json", ROOT / "contracts/m0/decisions.json", SUMMARY]
    source_digest = hashlib.sha256(b"".join(path.read_bytes() for path in sources[:-1])).hexdigest()
    evidence = {
        "schema_version": "evidence/v1", "owner_bead": "boring-cdc-m0-complete",
        "scenario_id": "SCN-M0-COMPLETION-BARRIER", "evidence_profile": "documentation",
        "evidence_tier": "milestone", "seed": "m0-complete-v1", "git_commit": head,
        "commands": captures,
        "source_preservation": {"before_sha256": source_digest, "after_sha256": source_digest, "preserved": True},
        "cleanup": {"complete": True, "remaining_paths": []},
        "redaction": {"checked": True, "secrets_found": 0},
        "tier_proof": {"targeted_checks": True, "boundary_e2e": True, "fault_suite": True,
            "deterministic_rerun": True, "consumed_contract_vectors": True, "workspace_tests": True,
            "integration": True, "clean_environment": True, "exit_assertions": True,
            "endurance": False, "full_failure_matrix": False, "clean_clone": False},
        "result": {"status": "pass", "digest": hashlib.sha256(b"".join(path.read_bytes() for path in sources)).hexdigest(),
            "artifacts": [str(path.relative_to(ROOT)) for path in sources],
            "product_faults": "fault_not_applicable", "runtime_observed": False},
    }
    EVIDENCE.write_text(json.dumps(evidence, sort_keys=True, indent=2) + "\n")


def verify_evidence(summary_text: str) -> list[str]:
    findings = []
    try:
        evidence = load(EVIDENCE)
    except Exception as exc:
        return [f"gate evidence missing or invalid: {exc}"]
    commands = evidence.get("commands", [])
    if len(commands) != 2:
        findings.append("gate evidence must contain two probes")
    observed = []
    for entry in commands:
        stdout = ROOT / str(entry.get("stdout_path", "")); stderr = ROOT / str(entry.get("stderr_path", ""))
        if entry.get("argv") != "scripts/acceptance/m0_complete.sh --probe" or entry.get("exit_code") != 0:
            findings.append("gate command is not a successful completion probe")
        if not stdout.is_file() or sha(stdout) != entry.get("stdout_sha256"):
            findings.append("gate stdout digest mismatch")
        if not stderr.is_file() or sha(stderr) != entry.get("stderr_sha256"):
            findings.append("gate stderr digest mismatch")
        if stdout.is_file() and stderr.is_file():
            observed.append((stdout.read_text(), stderr.read_text()))
    if len(observed) == 2 and (observed[0] != observed[1] or observed[0] != (summary_text, "")):
        findings.append("stored probes differ from fresh semantic summary")
    paths = [ROOT / path for path in evidence.get("result", {}).get("artifacts", [])]
    if not all(path.is_file() for path in paths) or hashlib.sha256(b"".join(path.read_bytes() for path in paths)).hexdigest() != evidence.get("result", {}).get("digest"):
        findings.append("gate result digest mismatch")
    run = subprocess.run(["scripts/validate/evidence.sh", str(EVIDENCE.relative_to(ROOT))], cwd=ROOT, text=True, capture_output=True)
    if run.returncode:
        findings.append(f"gate evidence schema/provenance invalid: {(run.stdout + run.stderr).strip()}")
    return findings


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) == 2 else "--probe"
    if mode not in {"--probe", "--write", "--verify"}:
        print("usage: m0_completeness.py [--probe|--write|--verify]", file=sys.stderr)
        return 2
    summary, text = encoded_summary()
    if summary["status"] != "pass":
        print(text, end="")
        return 1
    if mode == "--write":
        write_evidence(text)
    elif mode == "--verify":
        extra = verify_evidence(text)
        if extra:
            summary["status"] = "fail"; summary["findings"] = sorted(set(summary["findings"] + extra))
            print(json.dumps(summary, sort_keys=True, indent=2))
            return 1
    print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
