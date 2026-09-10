#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
cp "$root/tests/fixtures/m1_config/representative.toml" "$tmp/boring-cdc.toml"
export PG_RUNTIME=redacted PG_CONTROL=redacted PG_ADMIN=redacted CH_RUNTIME=redacted CH_MAINT=redacted
python3 - "$root/tests/fixtures/m1_preflight/supported.json" "$tmp/preflight-observation.json" <<'PY'
import json,sys
x=json.load(open(sys.argv[1])); x['source']['streaming_option']='streaming=true'; x['storage']['after_state_sha256']='changed'; x['security']['status_read_only']=False
json.dump(x,open(sys.argv[2],'w'))
PY
set +e
(cd "$tmp" && "$root/target/debug/boring-cdc" check --json > result.json)
rc=$?
set -e
test "$rc" -eq 3
python3 - "$tmp/result.json" <<'PY'
import json,sys
r=json.load(open(sys.argv[1])); assert r['code']=='PREFLIGHT_BLOCKED'
reasons={x['reason'] for x in r['data']['checks']}
assert {'PREFLIGHT_PROTOCOL_FINGERPRINT_MISMATCH','PREFLIGHT_STATE_MUTATED','PREFLIGHT_MUTATING_OBSERVABILITY_ROUTE'} <= reasons
PY
echo 'm1 preflight faults: PASS'
