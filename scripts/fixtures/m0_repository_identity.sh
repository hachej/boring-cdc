#!/bin/sh
set -eu

output_path=${1:-artifacts/m0/decisions/boring-cdc-d-owner/repository-view.json}
observed_input=${2:-}
mkdir -p "$(dirname "$output_path")"
tmp="${output_path}.tmp.$$"
trap 'rm -f "$tmp"' EXIT HUP INT TERM

scripts/fixtures/validate_m0_repository_identity.py --active-run
if [ -n "$observed_input" ]; then
  if ! cp -f "$observed_input" "$tmp" 2>/dev/null; then
    printf '%s\n' '{"code":"REPOSITORY_IDENTITY_MISMATCH","outcome":"fail","phase":"observe"}'
    exit 1
  fi
else
  if ! gh repo view hachej/boring-cdc \
    --json nameWithOwner,visibility,url \
    --jq '{nameWithOwner:.nameWithOwner,visibility:.visibility,url:.url}' >"$tmp" 2>/dev/null; then
    printf '%s\n' '{"code":"REPOSITORY_IDENTITY_MISMATCH","outcome":"fail","phase":"observe"}'
    exit 1
  fi
fi

python3 - "$tmp" <<'PY'
import json
import sys

try:
    with open(sys.argv[1], encoding="utf-8") as stream:
        actual = json.load(stream)
except (OSError, UnicodeError, json.JSONDecodeError):
    print('{"code":"REPOSITORY_IDENTITY_MISMATCH","outcome":"fail","phase":"observe"}')
    raise SystemExit(1)
expected = {
    "nameWithOwner": "hachej/boring-cdc",
    "url": "https://github.com/hachej/boring-cdc",
    "visibility": "PUBLIC",
}
if actual != expected:
    print('{"code":"REPOSITORY_IDENTITY_MISMATCH","outcome":"fail","phase":"observe"}')
    raise SystemExit(1)
print('{"code":"REPOSITORY_IDENTITY_CONFIRMED","outcome":"pass","phase":"observe"}')
PY

mv -f "$tmp" "$output_path"
trap - EXIT HUP INT TERM
