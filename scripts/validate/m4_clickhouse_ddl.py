#!/usr/bin/env python3
import hashlib
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
E = ROOT / 'artifacts/boring-cdc-m4-ddl/SCN-M4-CH-MERGE-INVARIANT/evidence.json'
INPUT_PATHS = (
    'Cargo.lock',
    'compose.yaml',
    'contracts/clickhouse/canonical-query.sql',
    'contracts/clickhouse/ddl.sql',
    'contracts/clickhouse/model.json',
    'docs/CLICKHOUSE_MODEL.md',
    'examples/m4_clickhouse_schema_probe.rs',
    'scripts/e2e/m4_clickhouse_ddl.sh',
    'scripts/validate/m4_clickhouse_ddl.py',
    'src/m4_clickhouse_schema.rs',
)


def input_hashes():
    return {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in INPUT_PATHS}


def current_object_fingerprint():
    probe = subprocess.run(
        ['cargo', 'run', '--quiet', '--locked', '--example', 'm4_clickhouse_schema_probe'],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    if probe.returncode:
        return None
    fingerprint = probe.stdout.strip()
    return fingerprint if re.fullmatch(r'[0-9a-f]{64}', fingerprint) else None


def validate():
    if not E.is_file():
        return ['E_EVIDENCE_MISSING']
    evidence = json.loads(E.read_text())
    errors = []
    if evidence.get('schema_version') != 'm4-clickhouse-ddl-evidence/v1' or evidence.get('status') != 'pass':
        errors.append('E_EVIDENCE_SCHEMA')
    source_commit = evidence.get('git_commit', '')
    if not re.fullmatch(r'[0-9a-f]{40}', source_commit):
        errors.append('E_SOURCE_COMMIT')
    elif subprocess.run(
        ['git', 'merge-base', '--is-ancestor', source_commit, 'HEAD'],
        cwd=ROOT, capture_output=True, check=False,
    ).returncode:
        errors.append('E_SOURCE_COMMIT_STALE')
    if evidence.get('input_sha256') != input_hashes():
        errors.append('E_INPUTS_STALE')
    expected_fingerprint = current_object_fingerprint()
    if expected_fingerprint is None:
        errors.append('E_CURRENT_FINGERPRINT_UNAVAILABLE')
    elif evidence.get('object_fingerprint') != expected_fingerprint:
        errors.append('E_OBJECT_FINGERPRINT')
    if evidence.get('images') != {'postgres': '17.6', 'clickhouse': '25.8.2.29'}:
        errors.append('E_IMAGE_PIN')
    versions = evidence.get('observed_versions', {})
    if versions.get('clickhouse') != '25.8.2.29' or not versions.get('postgres', '').startswith('17.6'):
        errors.append('E_REAL_VERSION')
    if evidence.get('objects_verified') != 6:
        errors.append('E_OBJECTS')
    if evidence.get('ordinary_runtime') != {
        'ddl_denied': True, 'alter_denied': True, 'canonical_select_allowed': True,
    }:
        errors.append('E_PRIVILEGES')
    if evidence.get('history_interface') != {'rows': 4, 'ordered_without_final': True}:
        errors.append('E_HISTORY_INTERFACE')
    merge = evidence.get('merge_invariant', {})
    digests = [merge.get(key) for key in ('before_sha256', 'stopped_sha256', 'during_sha256', 'after_sha256')]
    if not merge.get('system_merges_observed') or len(set(digests)) != 1 or not re.fullmatch(r'[0-9a-f]{64}', digests[0] or ''):
        errors.append('E_MERGE_INVARIANT')
    if evidence.get('credentials_recorded') is not False or any(
        token in E.read_text().lower() for token in ('password=', 'postgresql://', 'clickhouse://')
    ):
        errors.append('E_SECRET')
    return errors


if __name__ == '__main__':
    findings = validate()
    print(json.dumps({'validator': 'm4-clickhouse-ddl', 'status': 'fail' if findings else 'pass', 'findings': findings}, sort_keys=True))
    raise SystemExit(bool(findings))
