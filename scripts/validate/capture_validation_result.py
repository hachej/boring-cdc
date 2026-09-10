#!/usr/bin/env python3
"""Run a validator, preserve its exit, and normalize passing JSON provenance."""
import json
import subprocess
import sys

argv = sys.argv[1:]
if argv[:1] == ["--"]:
    argv = argv[1:]
if not argv:
    print("Usage: capture_validation_result.py -- VALIDATOR [ARG ...]", file=sys.stderr)
    raise SystemExit(2)
completed = subprocess.run(argv, text=True, capture_output=True)
sys.stderr.write(completed.stderr)
if completed.returncode != 0:
    sys.stdout.write(completed.stdout)
    raise SystemExit(completed.returncode)
try:
    result = json.loads(completed.stdout)
    if not isinstance(result, dict) or result.get("status") != "pass" or result.get("findings") != []:
        raise ValueError("validator did not pass")
    result.pop("git_commit", None)
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
except (ValueError, TypeError, json.JSONDecodeError) as exc:
    print(f"E_VALIDATION_RESULT: {exc}", file=sys.stderr)
    raise SystemExit(1)
