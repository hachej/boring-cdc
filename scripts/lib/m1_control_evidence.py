#!/usr/bin/env python3
"""Deterministic integrity checks for the M1 control component evidence packet."""
import hashlib
import json
from pathlib import Path

VERSION = "m1-control-evidence/1.0.0"
SCHEMA = "m1-control-command-evidence/v1"
CANONICAL = Path("artifacts/boring-cdc-m1-control-fixtures/SCN-M1-CONTROL-COMPONENT/m1-control-v1")
EXPECTED_VALIDATORS = {
    "scripts/validate/evidence.sh artifacts/boring-cdc-m1-control-fixtures": "core-validators/1.0.0",
    "scripts/validate/m1_control_fixtures.py": "m1-control-fixtures/1.0.0",
}
FORBIDDEN = ("TBD", "TODO", "FIXME", "<unresolved>", "postgresql://", "capture_fixture_only", "control_fixture_only", "application_fixture_only", "/home/")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate(artifact):
    artifact = Path(artifact)
    findings = []

    def fail(code, detail):
        findings.append({"code": code, "detail": detail})

    def load(name):
        try:
            return json.loads((artifact / name).read_text())
        except (OSError, ValueError) as exc:
            fail("E_DOCUMENT", f"{name}: {type(exc).__name__}")
            return {}

    manifest = load("manifest.json")
    record = load("validator-evidence.json")
    record_fields = {"schema_version", "owner_bead", "manifest_path", "manifest_sha256", "validators"}
    if set(record) != record_fields:
        fail("E_RECORD_FIELDS", "validator-evidence.json")
    if record.get("schema_version") != SCHEMA:
        fail("E_SCHEMA_VERSION", "validator-evidence.json")
    if record.get("owner_bead") != "boring-cdc-m1.4":
        fail("E_OWNER", "validator-evidence.json")
    if record.get("manifest_path") != str(CANONICAL / "manifest.json"):
        fail("E_MANIFEST_PATH", "validator-evidence.json")
    try:
        manifest_sha = sha(artifact / "manifest.json")
    except OSError:
        manifest_sha = None
    if record.get("manifest_sha256") != manifest_sha:
        fail("E_MANIFEST_HASH", "validator-evidence.json")

    validators = record.get("validators")
    if not isinstance(validators, list) or len(validators) != len(EXPECTED_VALIDATORS):
        fail("E_VALIDATOR_SET", "expected exactly generic and M1 validators")
        validators = []
    seen = set()
    for index, row in enumerate(validators):
        if not isinstance(row, dict):
            fail("E_VALIDATOR_ROW", str(index)); continue
        expected_fields = {"argv", "version", "exit_code", "stdout_path", "stdout_sha256", "stderr_path", "stderr_sha256"}
        if set(row) != expected_fields:
            fail("E_VALIDATOR_FIELDS", str(index))
        argv = row.get("argv")
        if argv in seen or argv not in EXPECTED_VALIDATORS:
            fail("E_VALIDATOR_ARGV", str(argv)); continue
        seen.add(argv)
        if row.get("version") != EXPECTED_VALIDATORS[argv]:
            fail("E_VALIDATOR_VERSION", argv)
        if row.get("exit_code") != 0:
            fail("E_VALIDATOR_EXIT", argv)
        for stream in ("stdout", "stderr"):
            value = row.get(f"{stream}_path")
            try:
                stored = Path(value)
                relative = stored.relative_to(CANONICAL)
                target = artifact / relative
                if not target.is_file() or sha(target) != row.get(f"{stream}_sha256"):
                    fail("E_VALIDATOR_STREAM", f"{argv}:{stream}")
            except (TypeError, ValueError):
                fail("E_VALIDATOR_PATH", f"{argv}:{stream}")
    if seen != set(EXPECTED_VALIDATORS):
        fail("E_VALIDATOR_SET", "missing validator")

    try:
        generic = json.loads((artifact / "stdout/validator-generic.txt").read_text())
        expected_generic = {
            "schema_version": "validation-result/v1",
            "validator_version": "core-validators/1.0.0",
            "owner_bead": "boring-cdc-m0.1",
            "status": "pass",
            "input_sha256": manifest_sha,
            "findings": [],
        }
        if any(generic.get(key) != value for key, value in expected_generic.items()) or set(generic) != set(expected_generic) | {"git_commit"}:
            fail("E_GENERIC_RESULT", "generic validator output")
        commit = generic.get("git_commit", "")
        if not isinstance(commit, str) or len(commit) != 40 or any(char not in "0123456789abcdef" for char in commit):
            fail("E_GENERIC_RESULT", "generic validator git commit")
    except (OSError, ValueError, TypeError):
        fail("E_GENERIC_RESULT", "malformed generic validator output")
    for name in ("stderr/validator-generic.txt", "stderr/validator-specific.txt"):
        try:
            if (artifact / name).read_bytes() != b"":
                fail("E_VALIDATOR_STDERR", name)
        except OSError:
            fail("E_VALIDATOR_STDERR", name)

    try:
        inventory = {}
        for line in (artifact / "sha256.txt").read_text().splitlines():
            digest, name = line.split("  ", 1)
            if name in inventory:
                fail("E_INVENTORY_DUPLICATE", name)
            inventory[name] = digest
        expected = {
            str(path.relative_to(artifact)): sha(path)
            for path in artifact.rglob("*")
            if path.is_file() and path.name not in {"manifest.json", "sha256.txt", "validator-evidence.json"}
        }
        if inventory != expected:
            fail("E_INVENTORY", "SHA-256 inventory mismatch")
    except (OSError, ValueError) as exc:
        fail("E_INVENTORY", type(exc).__name__)

    for stem in ("e2e", "faults"):
        for stream in ("stdout", "stderr"):
            try:
                if (artifact / stream / f"{stem}-1.txt").read_bytes() != (artifact / stream / f"{stem}-2.txt").read_bytes():
                    fail("E_RERUN", f"{stream}/{stem}")
            except OSError:
                fail("E_RERUN", f"{stream}/{stem}:missing")

    required_log = {"schema_version", "case_event_seq", "bead_id", "scenario_id", "correlation_id", "run_id", "capture_epoch", "component", "phase", "outcome", "config_fingerprint", "evidence_digest"}
    try:
        rows = [json.loads(line) for line in (artifact / "logs/boring-cdc.jsonl").read_text().splitlines()]
        if [row.get("case_event_seq") for row in rows] != list(range(1, len(rows) + 1)) or not all(required_log <= row.keys() and row["bead_id"] == "boring-cdc-m1-control-fixtures" for row in rows):
            fail("E_LOG", "structured log sequence or fields")
    except (OSError, ValueError, TypeError):
        fail("E_LOG", "malformed structured log")

    expected_specific = (
        f"PASS m1 control fixture scenarios=12 pg_majors=3 unresolved=0\n"
        f"PASS artifact inventory={len(inventory)} logs={len(rows) if 'rows' in locals() else 0} "
        f"deterministic_rerun=1 redaction=1 command_evidence={VERSION}\n"
    )
    try:
        if (artifact / "stdout/validator-specific.txt").read_text() != expected_specific:
            fail("E_SPECIFIC_RESULT", "specific validator output")
    except OSError:
        fail("E_SPECIFIC_RESULT", "missing specific validator output")

    cleanup = manifest.get("cleanup")
    if cleanup != {"complete": True, "remaining_paths": []}:
        fail("E_CLEANUP", "manifest cleanup")
    redaction = manifest.get("redaction")
    if redaction != {"checked": True, "secrets_found": 0}:
        fail("E_REDACTION", "manifest redaction")
    try:
        corpus = "\n".join(path.read_text(errors="replace") for path in artifact.rglob("*") if path.is_file())
        for token in FORBIDDEN:
            if token in corpus:
                fail("E_UNRESOLVED" if token in FORBIDDEN[:4] else "E_REDACTION", token)
    except OSError:
        fail("E_CORPUS", "artifact read")
    return sorted(findings, key=lambda item: (item["code"], item["detail"]))
