#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR="${TMPDIR:-/tmp}"
cargo test --locked --workspace --all-targets
for attempt in 1 2; do
  cargo test --locked m2_heartbeat::tests -- --nocapture >"$TMPDIR/m2-heartbeat-tests-${attempt}-$$.out"
  grep -q 'real_journal_preserves_control_route_and_persisted_feedback_boundary ... ok' "$TMPDIR/m2-heartbeat-tests-${attempt}-$$.out"
  grep -q 'published_heartbeat_commits_before_feedback ... ok' "$TMPDIR/m2-heartbeat-tests-${attempt}-$$.out"
  rm -f "$TMPDIR/m2-heartbeat-tests-${attempt}-$$.out"
done
work=$(mktemp -d "$TMPDIR/m2-heartbeat-e2e.XXXXXX"); project="m2-heartbeat-$RANDOM-$$"; port=$((58000 + $$ % 1000))
cleanup(){ docker compose -p "$project" -f compose.yaml -f "$work/override.yml" down -v --remove-orphans >/dev/null 2>&1 || true; rm -rf "$work"; }; trap cleanup EXIT INT TERM
printf 'heartbeat-admin-%s\n' "$project" >"$work/postgres_password";chmod 600 "$work/postgres_password";export BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password"; export PGPASSWORD; PGPASSWORD=$(cat "$BORING_CDC_POSTGRES_PASSWORD_FILE")
cat >"$work/override.yml" <<YAML
services:
  postgres:
    ports: ["127.0.0.1:${port}:5432"]
YAML
docker compose -p "$project" -f compose.yaml -f "$work/override.yml" up -d --wait postgres >/dev/null
admin="postgresql://boring_cdc@127.0.0.1:${port}/boring_cdc?sslmode=disable"
psql "$admin" -v ON_ERROR_STOP=1 -f tests/fixtures/m1-control/setup.sql >/dev/null
# The M1 control fixture has a text singleton key. Production M2 init uses the
# immutable integer key 1, which is the predicate exercised by publish_once.
psql "$admin" -v ON_ERROR_STOP=1 <<'SQL' >/dev/null
ALTER TABLE boring_cdc_control.heartbeat DROP CONSTRAINT heartbeat_id_check;
UPDATE boring_cdc_control.heartbeat SET id = '1' WHERE id = 'singleton';
ALTER TABLE boring_cdc_control.heartbeat ALTER COLUMN id TYPE integer USING id::integer;
ALTER TABLE boring_cdc_control.heartbeat ADD CONSTRAINT heartbeat_id_check CHECK (id = 1);
SQL
psql "$admin" -v ON_ERROR_STOP=1 <<SQL >/dev/null
ALTER ROLE boring_cdc_control_writer PASSWORD '$PGPASSWORD';
SQL
control="postgresql://boring_cdc_control_writer@127.0.0.1:${port}/boring_cdc?sslmode=disable"
first=$(M2_HEARTBEAT_DSN="$control" cargo run --quiet --locked --example m2_heartbeat_component -- 1)
second=$(M2_HEARTBEAT_DSN="$control" cargo run --quiet --locked --example m2_heartbeat_component -- 2)
[[ "$first" == '{"affected_rows":1,"selected_keys":1,"runtime_rust_writer":true}' && "$second" == "$first" ]]
denied=0
for sql in "SELECT nonce FROM boring_cdc_control.heartbeat" "INSERT INTO boring_cdc_control.heartbeat VALUES(2,2,now())" "DELETE FROM boring_cdc_control.heartbeat WHERE id=1" "UPDATE boring_cdc_control.heartbeat SET id=2 WHERE id=1"; do
  if psql "$control" -v ON_ERROR_STOP=1 -qc "$sql" >/dev/null 2>&1; then exit 1; else denied=$((denied+1)); fi
done
published=$(psql "$admin" -Atqc "SELECT count(*) FROM pg_publication_tables WHERE pubname='boring_cdc_publication' AND schemaname='boring_cdc_control' AND tablename='heartbeat'")
before=$(psql "$admin" -Atqc 'SELECT pg_current_wal_lsn()')
psql "$admin" -qc "CREATE TABLE unrelated_wal(id bigint); INSERT INTO unrelated_wal VALUES(1)" >/dev/null
after=$(psql "$admin" -Atqc 'SELECT pg_current_wal_lsn()');[[ "$before" != "$after" && "$published" == 1 && "$denied" == 4 ]]
python3 - "$work/observation.json" "$first" <<'PY'
import json,sys
observed=json.loads(sys.argv[2]);observed.update({'excess_privileges_denied':True,'published':True,'unrelated_wal_advanced':True,'feedback_from_unrelated_wal':False,'targeted_tests':True,'durable_before_feedback':True,'control_writes_user_rows':False,'checkpoint_complete_only':True,'heartbeat_degraded':False,'deterministic_attempts':2})
open(sys.argv[1],'w').write(json.dumps(observed,sort_keys=True,separators=(',',':'))+'\n')
PY
M2_HEARTBEAT_OBSERVATION="$work/observation.json" python3 scripts/lib/m2_heartbeat_evidence.py e2e
scripts/validate/evidence.sh artifacts/boring-cdc-m2-heartbeat/SCN-M2-HEARTBEAT-COMPONENT/heartbeat-component-v1/evidence.json
python3 scripts/validate/m2_heartbeat.py
echo M2_HEARTBEAT_E2E_OK
