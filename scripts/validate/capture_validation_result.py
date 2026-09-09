#!/usr/bin/env python3
"""Canonicalize a passing validation result by removing checkout-local provenance."""
import json
import sys
try:
    result = json.load(sys.stdin)
    if not isinstance(result, dict) or result.get("status") != "pass" or result.get("findings") != []:
        raise ValueError("validator did not pass")
    result.pop("git_commit", None)
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
except (ValueError, TypeError, json.JSONDecodeError) as exc:
    print(f"E_VALIDATION_RESULT: {exc}", file=sys.stderr)
    raise SystemExit(1)
