#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cp "$root/tests/fixtures/m1_config/representative.toml" "$tmp/boring-cdc.toml"
cp "$root/tests/fixtures/m1_preflight/supported.json" "$tmp/preflight-observation.json"
export PG_RUNTIME=redacted PG_CONTROL=redacted PG_ADMIN=redacted CH_RUNTIME=redacted CH_MAINT=redacted
set +e
(cd "$tmp" && "$root/target/debug/boring-cdc" check --json > result.json)
rc=$?
set -e
test "$rc" -eq 4
python3 - "$tmp/result.json" <<'PY'
import json,sys
r=json.load(open(sys.argv[1])); assert r['code']=='PREFLIGHT_DEGRADED'; assert r['outcome']=='degraded'
assert any(x['reason']=='PREFLIGHT_LIVE_COLLECTION_UNVERIFIED' for x in r['data']['checks'])
assert 'redacted' not in json.dumps(r).lower()
PY
echo 'm1 preflight e2e: PASS'
