#!/usr/bin/env python3
"""Fail-closed validation for a redacted candidate agent-story bundle.

Validation has two independent layers:

* every string is scanned for publication leaks using detectors defined here,
  not imported from the redactor; and
* every candidate record is re-derived from its explicitly supplied retained
  JSONL source.  The retained bytes never enter output.  Their whole-file,
  message-content, and source-line hashes plus every redacted candidate field
  must exactly match this independent derivation.

A bundle without retained-source inputs is therefore not provenance-verified.
The validator never prints candidate text, retained text, source paths, or a
matched leak.

Usage:
    python3 scripts/validate/agent_story_bundle.py \\
      --record BEAD_ID=EXPECTED_SHA256=RETAINED_SESSION.jsonl [--record ...] BUNDLE.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")

# Root-agnostic by design. URI double slashes are excluded; URI-shaped values
# are handled separately by _DSN_RE.
_ABS_PATH_RE = re.compile(
    r"(?<![/\w])/(?!/)[^\r\n\"'`<>]+"
)
_CREDENTIAL_RE = re.compile(
    r"(?i)(postgres(?:ql)?://[^\s:@]+:[^\s@]+@"
    r"|-----BEGIN [A-Z ]*PRIVATE KEY-----"
    r"|(?:password|api[_-]?key|secret|token)\s*[=:]\s*[\"'][^\"']{4,})"
)
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
_IPV4_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
_IPV6_RE = re.compile(r"\b(?:[0-9a-fA-F]{1,4}:){2,7}[0-9a-fA-F]{1,4}\b")
_DSN_RE = re.compile(r"\b[a-zA-Z][a-zA-Z0-9+.\-]{1,15}://\S")
_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
_CONTAINER_RE = re.compile(r"\bboring-(?:worker|reviewer|orchestrator)--[0-9a-fA-F]{6,}\b")
_OUTCOME_SIGNAL_LINE_RE = re.compile(
    r"(?i)\b(pass(?:ed|ing)?|fail(?:ed|ing)?|error(?:\[|:)?|warning:|test result|"
    r"tests? run|exit code|returncode|return code|status\s*[:=]|compiling|finished|"
    r"running \d|success(?:ful)?|✓|✗|ok\b|\bpass\b|clippy|scaffold_secrets|"
    r"cargo (?:test|build|check|clippy|fmt)|assert|no findings|0 findings)\b"
)
_MAX_FIELD_CHARS = 4000
_MAX_TOOL_OUTCOME_CHARS = 600
_TRUNCATION_MARKER = "...<truncated-by-redactor>"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_content_hash(content) -> str:
    data = json.dumps(content, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256_bytes(data)


def scan_text_for_leaks(text: str, where: str, findings: list[str]) -> None:
    if not isinstance(text, str):
        return
    checks = (
        ("absolute-path", _ABS_PATH_RE),
        ("credential", _CREDENTIAL_RE),
        ("email", _EMAIL_RE),
        ("ipv4", _IPV4_RE),
        ("ipv6", _IPV6_RE),
        ("dsn", _DSN_RE),
        ("session-uuid", _UUID_RE),
        ("container-id", _CONTAINER_RE),
    )
    for kind, pattern in checks:
        if pattern.search(text):
            # Never echo a matched secret/path/transcript fragment.
            findings.append(f"{where}: surviving {kind}")


def walk_all_strings(node, path: str, findings: list[str]) -> None:
    if isinstance(node, dict):
        for index, (key, value) in enumerate(node.items()):
            # Keys are untrusted candidate content too. Scan them, but use only
            # an opaque index in findings so a malicious/private key is never
            # reflected into validator output.
            if isinstance(key, str):
                scan_text_for_leaks(key, f"{path}.key[{index}]", findings)
            walk_all_strings(value, f"{path}.value[{index}]", findings)
    elif isinstance(node, list):
        for index, value in enumerate(node):
            walk_all_strings(value, f"{path}[{index}]", findings)
    elif isinstance(node, str):
        scan_text_for_leaks(node, path, findings)


def is_hex64(value) -> bool:
    return isinstance(value, str) and bool(_HEX64_RE.fullmatch(value))


def extract_text_blocks(content) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    return "\n".join(
        block.get("text", "")
        for block in content
        if isinstance(block, dict) and block.get("type") == "text" and block.get("text")
    )


def summarize_tool_call(content) -> str:
    if not isinstance(content, list):
        return ""
    names = [
        str(block.get("toolName") or block.get("name") or "tool")
        for block in content
        if isinstance(block, dict) and block.get("type") == "toolCall"
    ]
    return ", ".join(names)


def filter_outcome_signal_lines(text: str, max_lines: int = 8) -> str:
    kept = []
    for line in text.splitlines():
        if _OUTCOME_SIGNAL_LINE_RE.search(line):
            kept.append(line)
            if len(kept) >= max_lines:
                break
    return "\n".join(kept)


def independently_redact(value: str, max_chars: int = _MAX_FIELD_CHARS) -> str:
    """Independent publication transform used only by this validator."""
    text = _DSN_RE.sub("<DSN>", value)
    text = _ABS_PATH_RE.sub("<ABS-PATH>", text)
    text = _CREDENTIAL_RE.sub("<CREDENTIAL>", text)
    text = _CONTAINER_RE.sub("<CONTAINER-ID>", text)
    text = _UUID_RE.sub("<SESSION-ID>", text)
    text = _EMAIL_RE.sub("<EMAIL>", text)
    text = _IPV6_RE.sub("<IP>", text)
    text = _IPV4_RE.sub("<IP>", text)
    if len(text) > max_chars:
        text = text[:max_chars] + _TRUNCATION_MARKER
        text = _DSN_RE.sub("<DSN>", text)
        text = _ABS_PATH_RE.sub("<ABS-PATH>", text)
    return text


class RetainedRecord:
    """Restricted source parsed in memory; values are never rendered."""

    def __init__(self, path: Path):
        raw = path.read_bytes()
        self.record_sha256 = sha256_bytes(raw)
        self.events = []
        for index, line in enumerate(line for line in raw.decode("utf-8").splitlines() if line.strip()):
            obj = json.loads(line)
            if obj.get("type") == "message" and isinstance(obj.get("message"), dict):
                self.events.append((index, line, obj["message"]))


def derive_record(bead_id: str, source: RetainedRecord) -> dict:
    user_events = [(i, line, msg) for i, line, msg in source.events if msg.get("role") == "user"]
    if user_events:
        dispatch_index, _, dispatch_message = user_events[0]
        dispatch_content = dispatch_message.get("content")
        dispatch_text = extract_text_blocks(dispatch_content)
        dispatch = {
            "present": True,
            "text": independently_redact(dispatch_text) if dispatch_text else None,
            "source_sha256": canonical_content_hash(dispatch_content),
        }
    else:
        dispatch_index = None
        dispatch = {"present": False, "text": None, "source_sha256": None}

    first_index = None
    first_turn = {"turn_found": False, "has_text_content": False, "text": None, "source_sha256": None}
    for index, _, message in source.events:
        if dispatch_index is not None and index <= dispatch_index:
            continue
        if message.get("role") == "assistant":
            first_index = index
            content = message.get("content")
            text = extract_text_blocks(content)
            first_turn = {
                "turn_found": True,
                "has_text_content": bool(text),
                "text": independently_redact(text) if text else None,
                "source_sha256": canonical_content_hash(content),
            }
            break

    outcomes = []
    for index, line, message in source.events:
        role = message.get("role")
        if role == "toolResult":
            text = extract_text_blocks(message.get("content"))
            signal = filter_outcome_signal_lines(text) if text else ""
            outcomes.append({
                "index": index,
                "tool_name": message.get("toolName") or "unknown",
                "is_error": bool(message.get("isError", False)),
                "summary": independently_redact(signal, _MAX_TOOL_OUTCOME_CHARS) if signal else "",
                "source_line_sha256": sha256_bytes(line.encode("utf-8")),
            })
        elif role == "assistant":
            summary = summarize_tool_call(message.get("content"))
            if summary:
                outcomes.append({
                    "index": index,
                    "tool_name": summary,
                    "is_error": False,
                    "summary": "<tool call dispatched; see following toolResult entries>",
                    "source_line_sha256": sha256_bytes(line.encode("utf-8")),
                })

    if len(user_events) <= 1:
        feedback = {
            "present": False,
            "text": None,
            "source_sha256": None,
            "note": "retained transcript has a single dispatch user turn; "
                    "no separate review-feedback turn exists to extract",
        }
    else:
        _, _, message = user_events[1]
        content = message.get("content")
        text = extract_text_blocks(content)
        feedback = {
            "present": True,
            "text": independently_redact(text) if text else None,
            "source_sha256": canonical_content_hash(content),
            "note": "second user turn in retained transcript",
        }

    last = None
    for index, line, message in source.events:
        if message.get("role") != "assistant" or index == first_index:
            continue
        text = extract_text_blocks(message.get("content"))
        if text:
            last = (line, text)
    if last is None:
        fix_forward = {"present": False, "text": None, "source_line_sha256": None}
    else:
        line, text = last
        fix_forward = {
            "present": True,
            "text": independently_redact(text),
            "source_line_sha256": sha256_bytes(line.encode("utf-8")),
        }

    return {
        "bead_id": bead_id,
        "source_record_sha256": source.record_sha256,
        "dispatch_prompt": dispatch,
        "first_agent_turn": first_turn,
        "tool_outcomes": outcomes,
        "review_feedback": feedback,
        "fix_forward": fix_forward,
    }


def validate_bundle(
    bundle,
    source_desc: str,
    source_paths: dict[str, tuple[str, Path]] | None = None,
) -> list[str]:
    findings: list[str] = []
    if not isinstance(bundle, dict):
        return [f"{source_desc}: document is not an object"]
    if bundle.get("bundle_kind") != "candidate-agent-story-bundle":
        findings.append(f"{source_desc}: unexpected or missing bundle_kind")
    if bundle.get("bundle_version") != 1:
        findings.append(f"{source_desc}: unexpected or missing bundle_version")
    if set(bundle) != {"bundle_kind", "bundle_version", "records"}:
        findings.append(f"{source_desc}: unexpected or missing top-level fields")

    records = bundle.get("records")
    if not isinstance(records, list) or not records:
        findings.append(f"{source_desc}: no records present")
        records = []

    sources = source_paths or {}
    seen_beads = set()
    for index, record in enumerate(records):
        where = f"{source_desc}.records[{index}]"
        if not isinstance(record, dict):
            findings.append(f"{where}: record is not an object")
            continue
        bead_id = record.get("bead_id")
        if not isinstance(bead_id, str) or not bead_id:
            findings.append(f"{where}: missing bead_id")
            continue
        if bead_id in seen_beads:
            findings.append(f"{where}: duplicate bead_id")
        seen_beads.add(bead_id)

        source_binding = sources.get(bead_id)
        if source_binding is None:
            findings.append(f"{where}: retained source not supplied; provenance is unverified")
            continue
        expected_sha256, source_path = source_binding
        try:
            retained = RetainedRecord(source_path)
            expected = derive_record(bead_id, retained)
        except Exception:  # noqa: BLE001 -- do not expose private source/path details
            findings.append(f"{where}: retained source could not be parsed")
            continue
        if not is_hex64(expected_sha256) or retained.record_sha256 != expected_sha256:
            findings.append(f"{where}: retained bytes do not match independently supplied expected digest")
        if record != expected:
            findings.append(f"{where}: candidate does not exactly match independent retained-byte derivation")

        record_hash = record.get("source_record_sha256")
        if not is_hex64(record_hash):
            findings.append(f"{where}: missing or malformed source_record_sha256")

    # Blanket leak scan is independent of provenance equality and covers extra
    # schema fields as well. Findings intentionally omit matched values.
    walk_all_strings(bundle, source_desc, findings)
    return findings


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--record", action="append", default=[], metavar="BEAD_ID=SHA256=SESSION_PATH",
        help="Repeatable trusted digest and retained source binding. Paths are never printed.",
    )
    parser.add_argument("bundles", nargs="+", metavar="BUNDLE.json")
    return parser.parse_args(argv)


def parse_source_specs(specs: list[str]) -> tuple[dict[str, tuple[str, Path]], list[str]]:
    sources: dict[str, tuple[str, Path]] = {}
    findings = []
    for spec in specs:
        parts = spec.split("=", 2)
        if len(parts) != 3:
            findings.append("retained source binding has invalid syntax")
            continue
        bead_id, expected_sha256, raw_path = parts
        if (not bead_id or not is_hex64(expected_sha256) or not raw_path
                or bead_id in sources):
            findings.append("retained source binding is invalid or duplicated")
            continue
        sources[bead_id] = (expected_sha256, Path(raw_path))
    if not sources:
        findings.append("no retained source bindings supplied; provenance is unverified")
    return sources, findings


def main(argv=None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    sources, all_findings = parse_source_specs(args.record)
    for index, arg in enumerate(args.bundles):
        source_desc = f"bundle[{index}]"
        try:
            bundle = json.loads(Path(arg).read_text())
        except Exception:  # noqa: BLE001 -- never expose private path or parser content
            all_findings.append(f"{source_desc}: could not parse JSON")
            continue
        all_findings.extend(validate_bundle(bundle, source_desc, sources))

    if all_findings:
        print(json.dumps({"status": "fail", "findings": all_findings}, indent=2))
        return 1
    print(json.dumps({"status": "pass", "bundles_checked": len(args.bundles)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
