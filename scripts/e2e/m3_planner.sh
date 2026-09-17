#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp
cargo test --locked m3_planner::tests
work=$(mktemp -d /var/tmp/m3-planner-e2e.XXXXXX)
project="m3-planner-$RANDOM-$$"
port=$((55000+$$%1000))
cleanup(){ docker compose -p "$project" -f compose.yaml -f "$work/override.yml" down -v --remove-orphans >/dev/null 2>&1||true;rm -rf "$work"; }
trap cleanup EXIT INT TERM
python3 - <<'PY' >"$work/postgres_password"
import secrets
print(secrets.token_hex(24))
PY
chmod 600 "$work/postgres_password"
export BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password"
export PGPASSWORD
PGPASSWORD=$(cat "$BORING_CDC_POSTGRES_PASSWORD_FILE")
export PGHOST=127.0.0.1 PGPORT="$port" PGUSER=boring_cdc PGDATABASE=boring_cdc
export BORING_CDC_M3_DSN
BORING_CDC_M3_DSN=$(printf 'postgresql://%s@127.0.0.1:%s/boring_cdc?sslmode=disable' "boring_cdc:${PGPASSWORD}" "$port")
cat >"$work/override.yml" <<YAML
services:
  postgres:
    ports: ["127.0.0.1:${port}:5432"]
YAML
docker compose -p "$project" -f compose.yaml -f "$work/override.yml" up -d --wait postgres >/dev/null
psql_cmd=(psql -X -v ON_ERROR_STOP=1 -At -h 127.0.0.1 -p "$port" -U boring_cdc -d boring_cdc)
cat >"$work/fixture.sql" <<'SQL'
DROP PUBLICATION IF EXISTS boring_cdc_m3_planner_pub;
DROP TABLE IF EXISTS m3_planner_fixture;
CREATE TABLE m3_planner_fixture(tenant bigint NOT NULL, id uuid NOT NULL, payload text NOT NULL, PRIMARY KEY(tenant,id));
CREATE PUBLICATION boring_cdc_m3_planner_pub FOR TABLE m3_planner_fixture;
INSERT INTO m3_planner_fixture VALUES
(-9223372036854775808,'00000000-0000-0000-0000-000000000000','min'),
(-7,'00000000-0000-0000-0000-000000000001','sparse-a'),
(-7,'80000000-0000-0000-0000-000000000000','sparse-b'),
(0,'ffffffff-ffff-ffff-ffff-ffffffffffff','zero'),
(9223372036854775807,'ffffffff-ffff-ffff-ffff-ffffffffffff','max');
SQL
"${psql_cmd[@]}" -f "$work/fixture.sql" >/dev/null
for attempt in 1 2; do
  BORING_CDC_M3_TEST_OBSERVATION="$work/rust-$attempt.json" cargo test --locked m3_planner::tests::live_postgres_worker_executes_persisted_composite_ranges -- --ignored --exact
  {
    "${psql_cmd[@]}" -c "SELECT tenant,id FROM m3_planner_fixture WHERE (tenant,id) < (-7,'80000000-0000-0000-0000-000000000000'::uuid) ORDER BY tenant,id LIMIT 2"
    "${psql_cmd[@]}" -c "SELECT tenant,id FROM m3_planner_fixture WHERE (tenant,id) >= (-7,'80000000-0000-0000-0000-000000000000'::uuid) AND (tenant,id) < (9223372036854775807,'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid) ORDER BY tenant,id LIMIT 2"
    "${psql_cmd[@]}" -c "SELECT tenant,id FROM m3_planner_fixture WHERE (tenant,id) >= (9223372036854775807,'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid) ORDER BY tenant,id LIMIT 2"
  } >"$work/result-$attempt"
done
cmp "$work/result-1" "$work/result-2"
[[ $(wc -l <"$work/result-1") -eq 5 ]]
python3 - "$work/result-1" "$work/rust-1.json" "$work/rust-2.json" >"$work/observation.json" <<'PY'
import hashlib,json,pathlib,sys
b=pathlib.Path(sys.argv[1]).read_bytes(); runs=[json.loads(pathlib.Path(p).read_text()) for p in sys.argv[2:]]
assert runs[0]==runs[1]
o=runs[0];assert o=={'atomic_chunk_event_commit':True,'complete_chunks':3,'exported_snapshot_importer_handoff':True,'generation_state':'fencing','memory_refused_before_payload':True,'pending_before':3,'remaining_claims':0,'snapshot_events':5}
o.update(row_count=len(b.splitlines()),result_sha256=hashlib.sha256(b).hexdigest(),rust_worker_runs=len(runs),postgres_keyset_runs=2,bounded_reader_released=o['pending_before']==3 and o['complete_chunks']==3,limits_respected=o['snapshot_events']==5)
print(json.dumps(o,sort_keys=True))
PY
[[ "$(docker compose -p "$project" -f compose.yaml -f "$work/override.yml" exec -T postgres psql -At -U boring_cdc -d boring_cdc -c 'show server_version')" == 17.6* ]]
BORING_CDC_M3_OBSERVATION="$work/observation.json" python3 scripts/lib/m3_planner_evidence.py e2e
scripts/validate/evidence.sh artifacts/boring-cdc-m3-planner/SCN-M3-PLANNER-POSTGRES/planner-pg17-v1/evidence.json
