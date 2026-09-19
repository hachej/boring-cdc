#!/usr/bin/env python3
"""Fail-closed validator for a candidate agent-story bundle produced by
scripts/series/redact_agent_story.py.

This is an INDEPENDENT check: it does not import or trust the redactor's
own regexes. It re-derives its own leak detectors so a bug in one tool is
not automatically invisible to the other.

FAILS on:
  - any surviving absolute filesystem path
  - any credential-shaped string
  - any email address
  - any IPv4/IPv6 address
  - any DSN / URI-scheme string
  - any extracted item that is present but lacks a well-formed source
    SHA-256
  - any field value that is not traceable to a recorded source hash
    (a "present" item whose companion hash is missing, malformed, or
    reused from an unrelated item in a way that breaks 1:1 traceability)

Usage:
    python3 scripts/validate/agent_story_bundle.py <bundle.json> [<bundle.json> ...]
Exits non-zero and prints a JSON {"status":"fail","findings":[...]} on any
violation; prints {"status":"pass", ...} and exits 0 otherwise.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

_HEX64_RE = re.compile(r'^[0-9a-f]{64}$')

_ABS_PATH_RE = re.compile(
    r'(?<![:/\w])/(?:home|var|tmp|root|run|etc|opt|usr|data|mnt|srv)(?:/[^\s"\'`)>,;:]*)+'
)
_CREDENTIAL_RE = re.compile(
    r'(?i)(postgres(?:ql)?://[^\s:@]+:[^\s@]+@'
    r'|-----BEGIN [A-Z ]*PRIVATE KEY-----'
    r'|(?:password|api[_-]?key|secret|token)\s*[=:]\s*["\'][^"\']{4,})'
)
_EMAIL_RE = re.compile(r'[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}')
_IPV4_RE = re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b')
_IPV6_RE = re.compile(r'\b(?:[0-9a-fA-F]{1,4}:){2,7}[0-9a-fA-F]{1,4}\b')
_DSN_RE = re.compile(r'\b[a-zA-Z][a-zA-Z0-9+.\-]{1,15}://\S')
_UUID_RE = re.compile(
    r'\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b'
)
_CONTAINER_RE = re.compile(r'\bboring-(?:worker|reviewer|orchestrator)--[0-9a-fA-F]{6,}\b')


def scan_text_for_leaks(text: str, where: str, findings: list) -> None:
    if not isinstance(text, str):
        return
    checks = (
        ('absolute-path', _ABS_PATH_RE),
        ('credential', _CREDENTIAL_RE),
        ('email', _EMAIL_RE),
        ('ipv4', _IPV4_RE),
        ('ipv6', _IPV6_RE),
        ('dsn', _DSN_RE),
        ('session-uuid', _UUID_RE),
        ('container-id', _CONTAINER_RE),
    )
    for kind, pattern in checks:
        m = pattern.search(text)
        if m:
            findings.append(f'{where}: surviving {kind} ({m.group(0)[:40]!r})')


def walk_all_strings(node, path, findings):
    """Leak-scan every string in the document, whatever the schema shape,
    as a blanket safety net in addition to the schema-aware checks below."""
    if isinstance(node, dict):
        for k, v in node.items():
            walk_all_strings(v, f'{path}.{k}', findings)
    elif isinstance(node, list):
        for i, v in enumerate(node):
            walk_all_strings(v, f'{path}[{i}]', findings)
    elif isinstance(node, str):
        scan_text_for_leaks(node, path, findings)


def is_hex64(value) -> bool:
    return isinstance(value, str) and bool(_HEX64_RE.match(value))


def check_first_agent_turn(item: dict, where: str, findings: list, known_hashes: set) -> None:
    """first_agent_turn uses turn_found/has_text_content rather than the
    generic present/text convention: a first turn very often carries no
    text block at all (thinking + tool call only), which is a real state,
    not a missing field. Still require: if a turn was found, its
    source_sha256 must be well-formed (provenance for the turn exists even
    when there is no publishable text); text must be non-null iff
    has_text_content is true; and no fabricated text may appear alongside
    has_text_content=false or turn_found=false."""
    turn_found = item.get('turn_found')
    has_text = item.get('has_text_content')
    text = item.get('text')
    hash_val = item.get('source_sha256')

    if turn_found:
        if not is_hex64(hash_val):
            findings.append(f'{where}: turn_found but source_sha256 is not a well-formed sha256 hex digest')
        else:
            known_hashes.add(hash_val)
        if has_text:
            if text is None:
                findings.append(f'{where}: has_text_content but text is null')
        else:
            if text is not None:
                findings.append(f'{where}: has_text_content is false but text is not null (looks fabricated)')
    else:
        if text is not None or has_text or hash_val is not None:
            findings.append(f'{where}: turn_found is false but text/has_text_content/source_sha256 is set')


def check_hashed_item(item: dict, where: str, text_field: str, hash_field: str,
                       findings: list, known_hashes: set) -> None:
    present = item.get('present')
    text = item.get(text_field)
    hash_val = item.get(hash_field)

    if present:
        if text is None:
            findings.append(f'{where}: marked present but {text_field} is null')
        if not is_hex64(hash_val):
            findings.append(f'{where}: marked present but {hash_field} is not a well-formed sha256 hex digest')
        else:
            known_hashes.add(hash_val)
    else:
        if text is not None:
            findings.append(f'{where}: marked absent but {text_field} is not null (looks fabricated)')
        if hash_val is not None:
            findings.append(f'{where}: marked absent but carries a {hash_field} (not traceable to an absent field)')


def validate_bundle(bundle, source_desc: str) -> list:
    findings = []

    if bundle.get('bundle_kind') != 'candidate-agent-story-bundle':
        findings.append(f'{source_desc}: unexpected or missing bundle_kind')

    records = bundle.get('records')
    if not isinstance(records, list) or not records:
        findings.append(f'{source_desc}: no records present')
        records = []

    for i, record in enumerate(records):
        where = f'{source_desc}.records[{i}]'
        bead_id = record.get('bead_id')
        if not bead_id or not isinstance(bead_id, str):
            findings.append(f'{where}: missing bead_id')

        record_hash = record.get('source_record_sha256')
        if not is_hex64(record_hash):
            findings.append(f'{where}: missing or malformed source_record_sha256')

        known_hashes = set()
        if is_hex64(record_hash):
            known_hashes.add(record_hash)

        dp = record.get('dispatch_prompt') or {}
        check_hashed_item(dp, f'{where}.dispatch_prompt', 'text', 'source_sha256', findings, known_hashes)

        fat = record.get('first_agent_turn') or {}
        check_first_agent_turn(fat, f'{where}.first_agent_turn', findings, known_hashes)

        rf = record.get('review_feedback') or {}
        check_hashed_item(rf, f'{where}.review_feedback', 'text', 'source_sha256', findings, known_hashes)

        ff = record.get('fix_forward') or {}
        check_hashed_item(ff, f'{where}.fix_forward', 'text', 'source_line_sha256', findings, known_hashes)

        tool_outcomes = record.get('tool_outcomes')
        if not isinstance(tool_outcomes, list):
            findings.append(f'{where}: tool_outcomes is not a list')
            tool_outcomes = []
        seen_tool_hashes = set()
        for j, outcome in enumerate(tool_outcomes):
            owhere = f'{where}.tool_outcomes[{j}]'
            h = outcome.get('source_line_sha256')
            if not is_hex64(h):
                findings.append(f'{owhere}: missing or malformed source_line_sha256')
            else:
                known_hashes.add(h)
                if h in seen_tool_hashes:
                    findings.append(f'{owhere}: source_line_sha256 duplicated within the same record '
                                     f'(not traceable to a distinct source line)')
                seen_tool_hashes.add(h)
            if 'tool_name' not in outcome or not outcome.get('tool_name'):
                findings.append(f'{owhere}: missing tool_name')
            if 'is_error' not in outcome:
                findings.append(f'{owhere}: missing is_error')

        # Every present item's hash must be distinct from the whole-record
        # hash (a field-level claim copy-pasted from the file-level hash is
        # not traceable to that specific field).
        for field_name, item, presence_key in (
            ('dispatch_prompt', dp, 'present'),
            ('first_agent_turn', fat, 'turn_found'),
            ('review_feedback', rf, 'present'),
            ('fix_forward', ff, 'present'),
        ):
            h = item.get('source_sha256') or item.get('source_line_sha256')
            if item.get(presence_key) and is_hex64(h) and is_hex64(record_hash) and h == record_hash:
                findings.append(f'{where}.{field_name}: source hash equals the whole-record hash; '
                                 f'not traceable to this specific field')

    # Blanket leak scan across the entire document, independent of schema.
    walk_all_strings(bundle, source_desc, findings)

    return findings


def main(argv=None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    if not argv:
        print('usage: agent_story_bundle.py <bundle.json> [<bundle.json> ...]', file=sys.stderr)
        return 2

    all_findings = []
    for arg in argv:
        path = Path(arg)
        try:
            bundle = json.loads(path.read_text())
        except Exception as exc:  # noqa: BLE001
            all_findings.append(f'{arg}: could not parse JSON: {exc}')
            continue
        all_findings.extend(validate_bundle(bundle, str(path)))

    if all_findings:
        print(json.dumps({'status': 'fail', 'findings': all_findings}, indent=2))
        return 1

    print(json.dumps({'status': 'pass', 'bundles_checked': len(argv)}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
