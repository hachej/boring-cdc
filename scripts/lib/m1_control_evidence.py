#!/usr/bin/env python3
"""Independent deterministic checks for the M1 control evidence packet."""
import hashlib
import json
import re
from pathlib import Path

VERSION = "m1-control-evidence/1.1.0"
SCHEMA = "m1-control-command-evidence/v1"
CANONICAL = Path("artifacts/boring-cdc-m1-control-fixtures/SCN-M1-CONTROL-COMPONENT/m1-control-v1")
EXPECTED_VALIDATORS = {
    "scripts/validate/capture_validation_result.py -- scripts/validate/evidence.sh artifacts/boring-cdc-m1-control-fixtures": "core-validators/1.0.0",
    "scripts/validate/m1_control_fixtures.py": "m1-control-fixtures/1.0.0",
}
FORBIDDEN = ("TBD", "TODO", "FIXME", "<unresolved>", "postgresql://", "capture_fixture_only", "control_fixture_only", "application_fixture_only", "/home/")
SHA = re.compile(r"^[0-9a-f]{64}$")
TRANSCRIPT_PATHS = {"stdout/validator-generic.txt", "stderr/validator-generic.txt", "stdout/validator-specific.txt", "stderr/validator-specific.txt"}
SEALED_PAYLOAD_ROOT = "dfbcb28574aeb6f97a2ead1876e863ff4dfd2a26a9a26f66ae27d7499fe65ac3"
SEALED_INVENTORY_ROOT = "7f544e0de2885f0f901e6d0fe18c4412a202b1775066034c31c4199fd51b7cd6"


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load(artifact, name, findings):
    try:
        value = json.loads((artifact / name).read_text())
        if not isinstance(value, dict):
            raise ValueError("object required")
        return value
    except (OSError, ValueError, TypeError) as exc:
        findings.append({"code": "E_DOCUMENT", "detail": f"{name}: {type(exc).__name__}"})
        return {}


def validate_payload(artifact):
    """Validate packet content without reading validator transcripts."""
    artifact = Path(artifact)
    findings = []
    fail = lambda code, detail: findings.append({"code": code, "detail": detail})
    manifest = _load(artifact, "manifest.json", findings)
    try:
        resolved_root = artifact.resolve(strict=True)
        for path in artifact.rglob("*"):
            if path.is_symlink():
                fail("E_PATH_SYMLINK", str(path.relative_to(artifact)))
            else:
                path.resolve(strict=True).relative_to(resolved_root)
    except (OSError, ValueError):
        fail("E_PATH_CONTAINMENT", "artifact payload")
    try:
        inventory = {}
        for line in (artifact / "sha256.txt").read_text().splitlines():
            digest, name = line.split("  ", 1)
            if name in inventory or not SHA.fullmatch(digest):
                fail("E_INVENTORY_DUPLICATE" if name in inventory else "E_INVENTORY_DIGEST", name)
            inventory[name] = digest
        expected = {
            str(path.relative_to(artifact)): sha(path)
            for path in artifact.rglob("*")
            if path.is_file() and not path.is_symlink()
            and path.relative_to(artifact).as_posix() not in TRANSCRIPT_PATHS
            and path.name not in {"manifest.json", "sha256.txt", "validator-evidence.json"}
        }
        if inventory != expected:
            fail("E_INVENTORY", "SHA-256 inventory mismatch")
    except (OSError, ValueError) as exc:
        inventory = {}
        fail("E_INVENTORY", type(exc).__name__)

    for stem in ("e2e", "faults"):
        for stream in ("stdout", "stderr"):
            try:
                if (artifact / stream / f"{stem}-1.txt").read_bytes() != (artifact / stream / f"{stem}-2.txt").read_bytes():
                    fail("E_RERUN", f"{stream}/{stem}")
            except OSError:
                fail("E_RERUN", f"{stream}/{stem}:missing")

    required = {"schema_version", "case_event_seq", "bead_id", "scenario_id", "correlation_id", "run_id", "capture_epoch", "component", "phase", "outcome", "config_fingerprint", "evidence_digest"}
    try:
        rows = [json.loads(line) for line in (artifact / "logs/boring-cdc.jsonl").read_text().splitlines()]
        if len(rows) != 12 or [row.get("case_event_seq") for row in rows] != list(range(1, 13)) or not all(isinstance(row, dict) and required <= row.keys() and row["bead_id"] == "boring-cdc-m1-control-fixtures" for row in rows):
            fail("E_LOG", "structured log sequence or fields")
    except (OSError, ValueError, TypeError, AttributeError):
        rows = []
        fail("E_LOG", "malformed structured log")

    if manifest.get("cleanup") != {"complete": True, "remaining_paths": []}:
        fail("E_CLEANUP", "manifest cleanup")
    if manifest.get("redaction") != {"checked": True, "secrets_found": 0}:
        fail("E_REDACTION", "manifest redaction")
    try:
        corpus = "\n".join(
            path.read_text(errors="replace") for path in artifact.rglob("*")
            if path.is_file()
            and path.relative_to(artifact).as_posix() not in TRANSCRIPT_PATHS
            and path.name != "validator-evidence.json"
        )
        for token in FORBIDDEN:
            if token in corpus:
                fail("E_UNRESOLVED" if token in FORBIDDEN[:4] else "E_REDACTION", token)
    except OSError:
        fail("E_CORPUS", "artifact read")
    return sorted(findings, key=lambda item: (item["code"], item["detail"])), len(inventory), len(rows)


def validate(artifact):
    """Independently validate payload plus captured validator command evidence."""
    artifact = Path(artifact)
    findings = []
    fail = lambda code, detail: findings.append({"code": code, "detail": detail})
    if artifact.is_symlink():
        fail("E_ROOT_SYMLINK", "artifact root")
    excluded = {"manifest.json", "sha256.txt", "validator-evidence.json", "versions.json"} | TRANSCRIPT_PATHS
    payload_hash = hashlib.sha256()
    try:
        for path in sorted(item for item in artifact.rglob("*") if item.is_file() and item.relative_to(artifact).as_posix() not in excluded):
            relative = path.relative_to(artifact).as_posix()
            if path.is_symlink():
                fail("E_PATH_SYMLINK", relative)
                continue
            payload_hash.update(relative.encode() + b"\0" + path.read_bytes())
        if payload_hash.hexdigest() != SEALED_PAYLOAD_ROOT:
            fail("E_PAYLOAD_SEAL", "immutable component payload")
        inventory_rows = []
        for line in (artifact / "sha256.txt").read_text().splitlines():
            digest, name = line.split("  ", 1)
            if name != "versions.json": inventory_rows.append((name, digest))
        inventory_hash = hashlib.sha256()
        for name, digest in sorted(inventory_rows):
            inventory_hash.update(name.encode() + b"\0" + digest.encode())
        if inventory_hash.hexdigest() != SEALED_INVENTORY_ROOT:
            fail("E_INVENTORY_SEAL", "immutable SHA-256 inventory")
    except (OSError, ValueError):
        fail("E_PAYLOAD_SEAL", "malformed sealed payload")
    manifest = _load(artifact, "manifest.json", findings)
    if manifest.get("cleanup") != {"complete": True, "remaining_paths": []}:
        fail("E_CLEANUP", "manifest cleanup")
    if manifest.get("redaction") != {"checked": True, "secrets_found": 0}:
        fail("E_REDACTION", "manifest redaction")
    record = _load(artifact, "validator-evidence.json", findings)
    fields = {"schema_version", "owner_bead", "manifest_path", "manifest_sha256", "validators"}
    if set(record) != fields: fail("E_RECORD_FIELDS", "validator-evidence.json")
    if record.get("schema_version") != SCHEMA: fail("E_SCHEMA_VERSION", "validator-evidence.json")
    if record.get("owner_bead") != "boring-cdc-m1.4": fail("E_OWNER", "validator-evidence.json")
    if record.get("manifest_path") != str(CANONICAL / "manifest.json"): fail("E_MANIFEST_PATH", "validator-evidence.json")
    try: manifest_sha = sha(artifact / "manifest.json")
    except OSError: manifest_sha = None
    if record.get("manifest_sha256") != manifest_sha: fail("E_MANIFEST_HASH", "validator-evidence.json")

    validators = record.get("validators")
    if not isinstance(validators, list) or len(validators) != 2:
        fail("E_VALIDATOR_SET", "expected exactly generic and M1 validators"); validators = []
    seen = set()
    for index, row in enumerate(validators):
        if not isinstance(row, dict): fail("E_VALIDATOR_ROW", str(index)); continue
        expected_fields = {"argv", "version", "exit_code", "stdout_path", "stdout_sha256", "stderr_path", "stderr_sha256"}
        if set(row) != expected_fields: fail("E_VALIDATOR_FIELDS", str(index))
        argv = row.get("argv")
        if not isinstance(argv, str) or argv in seen or argv not in EXPECTED_VALIDATORS:
            fail("E_VALIDATOR_ARGV", repr(argv)); continue
        seen.add(argv)
        if row.get("version") != EXPECTED_VALIDATORS[argv]: fail("E_VALIDATOR_VERSION", argv)
        if type(row.get("exit_code")) is not int or row.get("exit_code") != 0: fail("E_VALIDATOR_EXIT", argv)
        for stream in ("stdout", "stderr"):
            value = row.get(f"{stream}_path")
            if not isinstance(value, str): fail("E_VALIDATOR_PATH", f"{argv}:{stream}"); continue
            stored = Path(value)
            try: relative = stored.relative_to(CANONICAL)
            except ValueError: fail("E_VALIDATOR_PATH", f"{argv}:{stream}"); continue
            if ".." in relative.parts or relative.is_absolute(): fail("E_VALIDATOR_PATH", f"{argv}:{stream}"); continue
            target = artifact / relative
            try:
                target.resolve(strict=True).relative_to(artifact.resolve(strict=True))
                if not target.is_file() or not SHA.fullmatch(str(row.get(f"{stream}_sha256", ""))) or sha(target) != row.get(f"{stream}_sha256"):
                    fail("E_VALIDATOR_STREAM", f"{argv}:{stream}")
            except (OSError, ValueError): fail("E_VALIDATOR_PATH", f"{argv}:{stream}")
    if seen != set(EXPECTED_VALIDATORS): fail("E_VALIDATOR_SET", "missing validator")

    try:
        generic = json.loads((artifact / "stdout/validator-generic.txt").read_text())
        expected = {"schema_version": "validation-result/v1", "validator_version": "core-validators/1.0.0", "owner_bead": "boring-cdc-m0.1", "status": "pass", "input_sha256": manifest_sha, "findings": []}
        canonical = json.dumps(expected, sort_keys=True, separators=(",", ":")) + "\n"
        if generic != expected or (artifact / "stdout/validator-generic.txt").read_text() != canonical:
            fail("E_GENERIC_RESULT", "canonical normalized generic validator output")
    except (OSError, ValueError, TypeError): fail("E_GENERIC_RESULT", "malformed generic validator output")
    for name in ("stderr/validator-generic.txt", "stderr/validator-specific.txt"):
        try:
            if (artifact / name).read_bytes(): fail("E_VALIDATOR_STDERR", name)
        except OSError: fail("E_VALIDATOR_STDERR", name)
    expected_specific = "PASS m1 control fixture scenarios=12 pg_majors=3 unresolved=0\nPASS artifact inventory=20 logs=12 deterministic_rerun=1 redaction=1\n"
    try:
        if (artifact / "stdout/validator-specific.txt").read_text() != expected_specific: fail("E_SPECIFIC_RESULT", "specific validator output")
    except OSError: fail("E_SPECIFIC_RESULT", "missing specific validator output")
    return sorted(findings, key=lambda item: (item["code"], item["detail"]))
