#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
(cd "$root" && cargo build --locked --quiet)
config="$root/tests/fixtures/m1_config/representative.toml"
observation="$root/tests/fixtures/m1_preflight/supported.json"
cp "$config" "$tmp/boring-cdc.toml"
cp "$observation" "$tmp/preflight-observation.json"
export PG_RUNTIME=redacted PG_CONTROL=redacted PG_ADMIN=redacted CH_RUNTIME=redacted CH_MAINT=redacted
run_check() {
  local output=$1
  set +e
  (cd "$tmp" && "$root/target/debug/boring-cdc" check --json > "$output")
  RUN_RC=$?
  set -e
}
assert_failure() {
  local output=$1 expected_scenario=$2 expected_reason=$3
  test "$RUN_RC" -eq 3
  python3 - "$output" "$expected_scenario" "$expected_reason" <<'PY'
import json,sys
r=json.load(open(sys.argv[1])); assert r['code']=='PREFLIGHT_BLOCKED'
assert r['data']['checks'][0]['scenario_id']==sys.argv[2]
assert r['data']['checks'][0]['reason']==sys.argv[3]
assert r['data']['checks'][0]['status']=='blocked'
assert 'CLI_HANDLER_UNAVAILABLE' not in json.dumps(r)
PY
}
run_check "$tmp/result.json"
test "$RUN_RC" -eq 4
python3 - "$tmp/result.json" <<'PY'
import json,sys
r=json.load(open(sys.argv[1])); assert r['code']=='PREFLIGHT_DEGRADED'; assert r['outcome']=='degraded'
assert any(x['reason']=='PREFLIGHT_LIVE_COLLECTION_UNVERIFIED' for x in r['data']['checks'])
assert 'redacted' not in json.dumps(r).lower()
PY
rm -f "$tmp/preflight-observation.json"
run_check "$tmp/missing-observation.json"
assert_failure "$tmp/missing-observation.json" SCN-M1-PREFLIGHT-OBSERVATION-INPUT PREFLIGHT_OBSERVATION_UNAVAILABLE
mkdir "$tmp/preflight-observation.json"
run_check "$tmp/unreadable-observation.json"
assert_failure "$tmp/unreadable-observation.json" SCN-M1-PREFLIGHT-OBSERVATION-INPUT PREFLIGHT_OBSERVATION_UNAVAILABLE
rm -rf "$tmp/preflight-observation.json"
printf '{' > "$tmp/preflight-observation.json"
run_check "$tmp/malformed-observation.json"
assert_failure "$tmp/malformed-observation.json" SCN-M1-PREFLIGHT-SCHEMA PREFLIGHT_OBSERVATION_SCHEMA_UNSUPPORTED
rm -f "$tmp/boring-cdc.toml"
run_check "$tmp/missing-config.json"
assert_failure "$tmp/missing-config.json" SCN-M1-PREFLIGHT-CONFIG-INPUT PREFLIGHT_CONFIG_UNAVAILABLE
mkdir "$tmp/boring-cdc.toml"
run_check "$tmp/unreadable-config.json"
assert_failure "$tmp/unreadable-config.json" SCN-M1-PREFLIGHT-CONFIG-INPUT PREFLIGHT_CONFIG_UNAVAILABLE
rm -rf "$tmp/boring-cdc.toml"
printf 'not valid toml = [' > "$tmp/boring-cdc.toml"
run_check "$tmp/invalid-config.json"
assert_failure "$tmp/invalid-config.json" SCN-M1-PREFLIGHT-CONFIG-INPUT CONFIG_INVALID_TOML_OR_UNKNOWN_FIELD
echo 'm1 preflight e2e: PASS (supported plus six input-failure paths)'
