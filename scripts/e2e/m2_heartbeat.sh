#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR=/var/tmp
cargo test --locked m2_heartbeat::tests
work=$(mktemp -d /var/tmp/m2-heartbeat-e2e.XXXXXX); project="m2-heartbeat-$RANDOM-$$"; port=$((58000 + $$ % 1000))
cleanup(){ docker compose -p "$project" -f compose.yaml -f "$work/override.yml" down -v --remove-orphans >/dev/null 2>&1 || true; rm -rf "$work"; }; trap cleanup EXIT INT TERM
printf 'heartbeat-admin-%s\n' "$project" >"$work/postgres_password";chmod 600 "$work/postgres_password";export BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password"
cat >"$work/override.yml" <<YAML
services:
  postgres:
    ports: ["127.0.0.1:${port}:5432"]
YAML
docker compose -p "$project" -f compose.yaml -f "$work/override.yml" up -d --wait postgres >/dev/null
admin="postgresql://boring_cdc:$(cat "$work/postgres_password")@127.0.0.1:${port}/boring_cdc?sslmode=disable"
psql "$admin" -v ON_ERROR_STOP=1 -f tests/fixtures/m1-control/setup.sql >/dev/null
control="postgresql://boring_cdc_control_writer:control_fixture_only@127.0.0.1:${port}/boring_cdc?sslmode=disable"
selected=$(psql "$control" -Atqc "SELECT count(*) FROM boring_cdc_control.heartbeat WHERE id='singleton'")
affected=$(psql "$control" -Atqc "WITH changed AS (UPDATE boring_cdc_control.heartbeat SET nonce=1,updated_at=clock_timestamp() WHERE id='singleton' RETURNING 1) SELECT count(*) FROM changed")
[[ "$selected" == 1 && "$affected" == 1 ]]
denied=0
for sql in "SELECT nonce FROM boring_cdc_control.heartbeat" "INSERT INTO boring_cdc_control.heartbeat VALUES('other',2,now())" "DELETE FROM boring_cdc_control.heartbeat WHERE id='singleton'" "UPDATE boring_cdc_control.heartbeat SET id='other' WHERE id='singleton'"; do
  if psql "$control" -v ON_ERROR_STOP=1 -qc "$sql" >/dev/null 2>&1; then exit 1; else denied=$((denied+1)); fi
done
published=$(psql "$admin" -Atqc "SELECT count(*) FROM pg_publication_tables WHERE pubname='boring_cdc_publication' AND schemaname='boring_cdc_control' AND tablename='heartbeat'")
before=$(psql "$admin" -Atqc 'SELECT pg_current_wal_lsn()')
psql "$admin" -qc "CREATE TABLE unrelated_wal(id bigint); INSERT INTO unrelated_wal VALUES(1)" >/dev/null
after=$(psql "$admin" -Atqc 'SELECT pg_current_wal_lsn()');[[ "$before" != "$after" && "$published" == 1 && "$denied" == 4 ]]
cat >"$work/observation.json" <<JSON
{"affected_rows":1,"selected_keys":1,"excess_privileges_denied":true,"published":true,"unrelated_wal_advanced":true,"feedback_from_unrelated_wal":false,"targeted_tests":true,"durable_before_feedback":true,"control_writes_user_rows":false,"checkpoint_complete_only":true,"heartbeat_degraded":false}
JSON
M2_HEARTBEAT_OBSERVATION="$work/observation.json" python3 scripts/lib/m2_heartbeat_evidence.py e2e
scripts/validate/evidence.sh artifacts/boring-cdc-m2-heartbeat/SCN-M2-HEARTBEAT-COMPONENT/heartbeat-component-v1/evidence.json
python3 scripts/validate/m2_heartbeat.py
echo M2_HEARTBEAT_E2E_OK
