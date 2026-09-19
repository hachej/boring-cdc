#!/usr/bin/env python3
"""Build a redacted, provenance-bound candidate agent-story bundle from
retained Factory JSONL session transcripts.

This tool extracts ONLY:
  - the originating dispatch prompt (first user turn)
  - the first recorded agent turn (assistant text only; chain-of-thought
    "thinking" blocks are never extracted)
  - command/tool outcomes (tool name, success/failure, a redacted and
    length-capped excerpt of the result)
  - review feedback, if a retained transcript actually contains a second
    (post-dispatch) user turn carrying review/correction context
  - fix-forward activity (the final assistant text turn, when distinct
    from the first turn)

Every extracted item carries the SHA-256 of the *raw* source bytes it was
derived from. Provenance is established only when the independent validator
is also given the retained sources and exactly re-derives every candidate
field without printing those private bytes. The record-level hash, the
dispatch-prompt hash and the
first-agent-turn hash use the canonical-JSON convention documented in
docs/SERIES_EXECUTION.md ("Article 1 retained-session recovery"): the
whole raw file's SHA-256 for the record, and
sha256(json.dumps(content, sort_keys=True, separators=(",", ":"))) of the
first user/assistant message.content for the prompt/first-turn hashes.

This tool NEVER paraphrases, summarizes, or reconstructs. A missing field
is emitted as explicitly absent (present: false) rather than invented.

Nothing this tool writes is publication-ready by itself: run
scripts/validate/agent_story_bundle.py with one
`--record BEAD_ID=TRUSTED_SOURCE_SHA256=SESSION_PATH` for every candidate
record. The trusted digest comes from the ratified provenance table, not from
the candidate. A passing provenance/leak check still requires
explicit owner sign-off before anything is copied anywhere public.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

MAX_FIELD_CHARS = 4000
# Tool/command outcomes get a much tighter cap than narrative fields
# (dispatch prompt, agent turns): a toolResult can be a raw dump of an
# entire file's contents, which is out of scope for "command/tool
# outcomes" and is exactly the kind of blob most likely to carry an
# incidental credential-/DSN-/path-shaped substring buried deep inside.
MAX_TOOL_OUTCOME_CHARS = 600
TRUNCATION_MARKER = "...<truncated-by-redactor>"

# ---------------------------------------------------------------------------
# Redaction pipeline
#
# Field selection above is the primary (allowlist) control: we only ever
# look at a handful of named fields out of an entire raw transcript. These
# regexes are a second, defense-in-depth pass applied to the text of those
# already-selected fields, because free text (a dispatch prompt, a bash
# command, a tool result) can still carry paths/credentials/etc. inside it.
# ---------------------------------------------------------------------------

# Reuses the credential-shape regex from scripts/validate/scaffold_secrets.sh
# verbatim (case-insensitive), so the two tools agree on what a credential
# looks like.
_CREDENTIAL_RE = re.compile(
    r'(?i)(postgres(?:ql)?://[^\s:@]+:[^\s@]+@'
    r'|-----BEGIN [A-Z ]*PRIVATE KEY-----'
    r'|(?:password|api[_-]?key|secret|token)\s*[=:]\s*["\'][^"\']{4,})'
)

# Any URI-scheme string (postgres://, mysql://, redis://, http(s)://, ...).
# Deliberately broad: "any DSN" must not survive, including a degenerate
# empty-body literal like "postgresql://" immediately followed by a
# closing quote — so the body class is any run of non-whitespace
# (quotes/parens included), not just "non-punctuation".
_DSN_RE = re.compile(r'\b[a-zA-Z][a-zA-Z0-9+.\-]{1,15}://\S+')

# Treat every Unix-style absolute path as private, regardless of its root.
# A root allowlist is unsafe: common workspaces also live below /workspace,
# /Users, /private/var, and arbitrary mount points.  URI double slashes are
# excluded so the DSN pass above remains responsible for URI-shaped values.
_ABS_PATH_RE = re.compile(
    r'(?<![/\w])/(?!/)[^\r\n"\'`<>]+'
)

_EMAIL_RE = re.compile(r'[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}')

_IPV4_RE = re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b')
_IPV6_RE = re.compile(r'\b(?:[0-9a-fA-F]{1,4}:){2,7}[0-9a-fA-F]{1,4}\b')

# Session/container UUIDs (v4-shaped, but match any dash-grouped hex UUID).
_UUID_RE = re.compile(
    r'\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b'
)

# Factory container/session-directory names, e.g. boring-worker--<hex>.
_CONTAINER_RE = re.compile(r'\bboring-(?:worker|reviewer|orchestrator)--[0-9a-fA-F]{6,}\b')

# A tool result is very often a raw dump of unrelated content: full source
# files, git diffs, git log history, grep matches, internal review verdict
# text quoting Bead IDs and file:line references, internal provisional/
# governance markers, and so on. None of that is a "command/tool outcome"
# in the sense this tool is scoped to (pass/fail signal for a command the
# agent ran) — it is internal repo/session context that must not be
# republished verbatim just because it happened to scroll past in a tool
# result. So tool-outcome extraction keeps ONLY the lines that look like an
# outcome/status signal, discards everything else, and never falls back to
# "just include the whole thing" even after redaction and truncation.
_OUTCOME_SIGNAL_LINE_RE = re.compile(
    r'(?i)\b(pass(?:ed|ing)?|fail(?:ed|ing)?|error(?:\[|:)?|warning:|test result|'
    r'tests? run|exit code|returncode|return code|status\s*[:=]|compiling|finished|'
    r'running \d|success(?:ful)?|✓|✗|ok\b|\bpass\b|clippy|scaffold_secrets|'
    r'cargo (?:test|build|check|clippy|fmt)|assert|no findings|0 findings)\b'
)


def filter_outcome_signal_lines(text: str, max_lines: int = 8) -> str:
    """Keep only lines that look like a command/tool outcome signal.
    Verbatim line selection, never rewritten — this is filtering, not
    paraphrasing."""
    kept = []
    for line in text.splitlines():
        if _OUTCOME_SIGNAL_LINE_RE.search(line):
            kept.append(line)
            if len(kept) >= max_lines:
                break
    return '\n'.join(kept)


def redact_text(value: str, max_chars: int = MAX_FIELD_CHARS) -> str:
    """Apply the full redaction pipeline to a single string value.

    Truncation runs LAST, after every substitution, and is re-applied once
    more after truncation in case the cut point landed inside something
    that itself needs redacting (e.g. a path chopped mid-string).
    """
    text = value
    text = _DSN_RE.sub('<DSN>', text)
    text = _ABS_PATH_RE.sub('<ABS-PATH>', text)
    text = _CREDENTIAL_RE.sub('<CREDENTIAL>', text)
    text = _CONTAINER_RE.sub('<CONTAINER-ID>', text)
    text = _UUID_RE.sub('<SESSION-ID>', text)
    text = _EMAIL_RE.sub('<EMAIL>', text)
    text = _IPV6_RE.sub('<IP>', text)
    text = _IPV4_RE.sub('<IP>', text)
    if len(text) > max_chars:
        text = text[:max_chars] + TRUNCATION_MARKER
        # Re-scan once more: truncation can never introduce a leak that
        # wasn't already ruled out above, but run the cheap patterns again
        # defensively in case a substitution above produced something new
        # near the cut boundary.
        text = _DSN_RE.sub('<DSN>', text)
        text = _ABS_PATH_RE.sub('<ABS-PATH>', text)
    return text


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_content_hash(content) -> str:
    """SHA-256 of canonical JSON (sort_keys=True, compact separators) of a
    raw message.content value. Matches the convention documented in
    docs/SERIES_EXECUTION.md for the 'prompt' and 'first' table hashes."""
    encoded = json.dumps(content, sort_keys=True, separators=(',', ':')).encode('utf-8')
    return sha256_bytes(encoded)


# ---------------------------------------------------------------------------
# Session parsing
# ---------------------------------------------------------------------------

class SessionRecord:
    def __init__(self, path: Path):
        self.path = path
        raw = path.read_bytes()
        self.record_sha256 = sha256_bytes(raw)
        self.lines = [ln for ln in raw.decode('utf-8').splitlines() if ln.strip()]
        self.messages = []  # list of (line_index, raw_line_str, parsed_obj)
        for idx, line in enumerate(self.lines):
            obj = json.loads(line)
            self.messages.append((idx, line, obj))

    def message_events(self):
        """Yield (line_index, raw_line, message_dict) for type == 'message'."""
        for idx, line, obj in self.messages:
            if obj.get('type') == 'message':
                yield idx, line, obj['message']


def extract_text_blocks(content) -> str:
    """Concatenate only 'text' content blocks. Thinking blocks and raw
    tool-call argument blobs are deliberately excluded from published
    text — thinking is internal chain-of-thought, never intended for
    publication, and frequently an opaque encrypted blob anyway."""
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ''
    parts = []
    for block in content:
        if isinstance(block, dict) and block.get('type') == 'text':
            text = block.get('text', '')
            if text:
                parts.append(text)
    return '\n'.join(parts)


def summarize_tool_call(content) -> str:
    """Best-effort redacted one-line-ish summary of a toolCall content
    block list, without dumping raw tool-call argument JSON."""
    if not isinstance(content, list):
        return ''
    names = []
    for block in content:
        if isinstance(block, dict) and block.get('type') == 'toolCall':
            names.append(str(block.get('toolName') or block.get('name') or 'tool'))
    return ', '.join(names)


def build_dispatch_prompt(record: SessionRecord):
    for idx, line, msg in record.message_events():
        if msg.get('role') == 'user':
            content = msg.get('content')
            text = extract_text_blocks(content)
            return {
                'present': True,
                'text': redact_text(text) if text else None,
                'source_sha256': canonical_content_hash(content),
            }, idx
    return {'present': False, 'text': None, 'source_sha256': None}, None


def build_first_agent_turn(record: SessionRecord, after_idx):
    """The first assistant turn after the dispatch prompt. This is treated
    separately from the generic present/text/hash convention used
    elsewhere: a first turn is very often a tool call with no narrative
    text at all (thinking + toolCall, nothing of type 'text'). That is a
    real, honestly-reported state — not a missing field — so it gets its
    own 'turn_found' / 'has_text_content' pair instead of forcing
    has-no-text into 'absent'. The source_sha256 always binds to the raw
    first-turn content once a turn is found, regardless of whether that
    turn carried publishable text, so the (redacted) text value published,
    if any, is always traceable back to a recorded source hash."""
    for idx, line, msg in record.message_events():
        if after_idx is not None and idx <= after_idx:
            continue
        if msg.get('role') == 'assistant':
            content = msg.get('content')
            text = extract_text_blocks(content)
            return {
                'turn_found': True,
                'has_text_content': bool(text),
                'text': redact_text(text) if text else None,
                'source_sha256': canonical_content_hash(content),
            }, idx
    return {'turn_found': False, 'has_text_content': False, 'text': None, 'source_sha256': None}, None


def build_tool_outcomes(record: SessionRecord):
    outcomes = []
    for idx, line, msg in record.message_events():
        role = msg.get('role')
        if role == 'toolResult':
            content = msg.get('content')
            text = extract_text_blocks(content)
            signal = filter_outcome_signal_lines(text) if text else ''
            outcomes.append({
                'index': idx,
                'tool_name': msg.get('toolName') or 'unknown',
                'is_error': bool(msg.get('isError', False)),
                'summary': redact_text(signal, MAX_TOOL_OUTCOME_CHARS) if signal else '',
                'source_line_sha256': sha256_bytes(line.encode('utf-8')),
            })
        elif role == 'assistant':
            summary = summarize_tool_call(msg.get('content'))
            if summary:
                outcomes.append({
                    'index': idx,
                    'tool_name': summary,
                    'is_error': False,
                    'summary': '<tool call dispatched; see following toolResult entries>',
                    'source_line_sha256': sha256_bytes(line.encode('utf-8')),
                })
    return outcomes


def build_review_feedback(record: SessionRecord, first_dispatch_idx):
    """A retained worker record has exactly one dispatch user turn in every
    admitted Article-1 session observed so far. If a session ever does
    carry a second user turn, treat it as review/correction context and
    extract it; otherwise emit explicitly absent rather than inventing a
    review-feedback narrative."""
    user_turns = [(idx, msg) for idx, line, msg in record.message_events() if msg.get('role') == 'user']
    if len(user_turns) <= 1:
        return {
            'present': False,
            'text': None,
            'source_sha256': None,
            'note': 'retained transcript has a single dispatch user turn; '
                    'no separate review-feedback turn exists to extract',
        }
    idx, msg = user_turns[1]
    content = msg.get('content')
    text = extract_text_blocks(content)
    return {
        'present': True,
        'text': redact_text(text) if text else None,
        'source_sha256': canonical_content_hash(content),
        'note': 'second user turn in retained transcript',
    }


def build_fix_forward(record: SessionRecord, first_turn_idx):
    """Final assistant turn carrying text, if any, distinct from the first
    agent turn — this is where a fix-forward summary/push report lives."""
    last = None
    for idx, line, msg in record.message_events():
        if msg.get('role') != 'assistant':
            continue
        if first_turn_idx is not None and idx == first_turn_idx:
            continue
        text = extract_text_blocks(msg.get('content'))
        if text:
            last = (idx, line, text)
    if last is None:
        return {'present': False, 'text': None, 'source_line_sha256': None}
    idx, line, text = last
    return {
        'present': True,
        'text': redact_text(text),
        'source_line_sha256': sha256_bytes(line.encode('utf-8')),
    }


def build_record(bead_id: str, path: Path) -> dict:
    record = SessionRecord(path)
    dispatch_prompt, dispatch_idx = build_dispatch_prompt(record)
    first_agent_turn, first_turn_idx = build_first_agent_turn(record, dispatch_idx)
    return {
        'bead_id': bead_id,
        'source_record_sha256': record.record_sha256,
        'dispatch_prompt': dispatch_prompt,
        'first_agent_turn': first_agent_turn,
        'tool_outcomes': build_tool_outcomes(record),
        'review_feedback': build_review_feedback(record, dispatch_idx),
        'fix_forward': build_fix_forward(record, first_turn_idx),
    }


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--record', action='append', default=[], metavar='BEAD_ID=SESSION_PATH',
        help='Repeatable. One admitted (bead id, session file) pair to extract.',
    )
    parser.add_argument('--out', required=True, help='Output bundle JSON path.')
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    if not args.record:
        print('error: at least one --record BEAD_ID=SESSION_PATH is required', file=sys.stderr)
        return 2

    out_path = Path(args.out)
    if 'docs/' in out_path.as_posix() or out_path.as_posix().startswith('docs/'):
        print('error: refusing to write into docs/', file=sys.stderr)
        return 2

    records = []
    for spec in args.record:
        if '=' not in spec:
            print(f'error: --record must be BEAD_ID=SESSION_PATH, got {spec!r}', file=sys.stderr)
            return 2
        bead_id, session_path = spec.split('=', 1)
        path = Path(session_path)
        if not path.is_file():
            print(f'error: session file not found: {path}', file=sys.stderr)
            return 2
        records.append(build_record(bead_id, path))

    bundle = {
        'bundle_kind': 'candidate-agent-story-bundle',
        'bundle_version': 1,
        'records': records,
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(bundle, indent=2, sort_keys=True) + '\n')
    print(json.dumps({'status': 'written', 'out': str(out_path), 'records': len(records)}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
