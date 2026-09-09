#!/bin/sh
set -eu

output_path=${1:-artifacts/m0/decisions/boring-cdc-d-owner/repository-view.json}
observed_input=${2:-}
mkdir -p "$(dirname "$output_path")"
tmp="${output_path}.tmp.$$"
trap 'rm -f "$tmp"' EXIT HUP INT TERM

if [ -n "$observed_input" ]; then
  cp -f "$observed_input" "$tmp"
else
  gh repo view hachej/boring-cdc \
    --json nameWithOwner,visibility,url \
    --jq '{nameWithOwner:.nameWithOwner,visibility:.visibility,url:.url}' >"$tmp"
fi

python3 - "$tmp" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    actual = json.load(stream)
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
