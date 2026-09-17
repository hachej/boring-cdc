#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR=/var/tmp
work=$(mktemp -d /var/tmp/m2-init-e2e.XXXXXX); project="m2-init-$RANDOM-$$"; port=$((58000 + $$ % 1000))
cleanup(){ docker compose -p "$project" -f compose.yaml -f "$work/override.yml" down -v --remove-orphans >/dev/null 2>&1 || true; rm -rf "$work"; }; trap cleanup EXIT INT TERM
printf 'm2-init-password-%s\n' "$project" >"$work/postgres_password"; chmod 600 "$work/postgres_password"; export BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password"; export PGPASSWORD; PGPASSWORD=$(cat "$BORING_CDC_POSTGRES_PASSWORD_FILE")
cat >"$work/override.yml" <<YAML
services:
  postgres:
    ports: ["127.0.0.1:${port}:5432"]
YAML
docker compose -p "$project" -f compose.yaml -f "$work/override.yml" up -d --wait postgres >/dev/null
psqlc(){ docker compose -p "$project" -f compose.yaml -f "$work/override.yml" exec -T postgres psql -X -v ON_ERROR_STOP=1 -U boring_cdc -d boring_cdc "$@"; }
[[ "$(psqlc -Atqc 'show server_version')" == 17.6* ]]

# This is the documented empty-database prerequisite command, using the checked-in SQL verbatim.
admin_password='admin-local-only'; runtime_password='runtime-local-only'; control_password='control-local-only'; application_password='application-local-only'
psqlc -v admin_password="$admin_password" -v runtime_password="$runtime_password" -v control_password="$control_password" -v application_password="$application_password" \
  < scripts/setup/durable_simple_prerequisites.sql >/dev/null
cargo build --quiet --locked --bin boring-cdc
mkdir -p "$work/run/state/spool"; chmod 700 "$work/run/state" "$work/run/state/spool"; cp tests/fixtures/m1_config/representative.toml "$work/run/boring-cdc.toml"
admin_dsn="postgresql://boring_cdc_admin:${admin_password}@127.0.0.1:${port}/boring_cdc?sslmode=disable"
export PG_ADMIN="$admin_dsn" CH_MAINT='https://unused.invalid'; unset PG_RUNTIME PG_CONTROL CH_RUNTIME || true
binary="$PWD/target/debug/boring-cdc"
init_dry_run(){ (cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE -u PG_ADMIN -u CH_MAINT "$binary" init --dry-run --json); }
confirm_token(){ python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["data"]["confirm_token"])' "$1"; }
reset_local_state(){ rm -rf "$work/run/state"; mkdir -p "$work/run/state/spool"; chmod 700 "$work/run/state" "$work/run/state/spool"; }
expect_init_failure(){
  local expected=$1 label=$2
  reset_local_state
  init_dry_run >"$work/${label}-dry.json"
  local token; token=$(confirm_token "$work/${label}-dry.json")
  if (cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$binary" init --confirm --confirm-token "$token" --json) >"$work/${label}.out" 2>"$work/${label}.err"; then
    echo "E_INIT_DRIFT_ACCEPTED $label" >&2; exit 1
  fi
  grep -q "$expected" "$work/${label}.err"
  ! grep -q 'postgresql://' "$work/${label}.err"
}

# Exercise the documented init --dry-run -> init --confirm order from empty local state.
init_dry_run >"$work/dry.json"
token=$(confirm_token "$work/dry.json")
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$binary" init --confirm --confirm-token "$token" --json) >"$work/first.json"
if (cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$binary" init --confirm --confirm-token "$token" --json) >/dev/null 2>&1; then echo E_INIT_TOKEN_REPLAY >&2; exit 1; fi
init_dry_run >"$work/dry2.json"; token2=$(confirm_token "$work/dry2.json"); [[ "$token" != "$token2" ]]
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$binary" init --confirm --confirm-token "$token2" --json) >"$work/second.json"
[[ "$(psqlc -Atqc "select count(*) from pg_replication_slots where slot_name='boring_slot'")" == 0 ]]
[[ "$(psqlc -Atqc 'select count(*) from boring_cdc_control.heartbeat')" == 1 ]]
[[ "$(psqlc -Atqc 'select count(*) from boring_cdc_control.capture_fences')" == 1 ]]
[[ "$(psqlc -Atqc "select count(*) from pg_publication_tables where pubname='boring_publication'")" == 3 ]]
[[ "$(python3 -c 'import sqlite3,sys; print(sqlite3.connect(sys.argv[1]).execute("select count(*) from source_state").fetchone()[0])' "$work/run/state/boring.db")" == 1 ]]
python3 - "$work/first.json" "$work/second.json" <<'PY'
import json,sys
for p in sys.argv[1:]:
 x=json.load(open(p)); assert x['outcome']=='success' and x['data']['logical_slot_exists'] is False and x['data']['control_rows']==2 and x['mutation_trace'] and x['postcondition_evidence_digest']
 s=open(p).read(); assert 'postgresql://' not in s and 'local-only' not in s
PY

# Every publication component and the control privilege boundary reports its own check.
psqlc -qc 'ALTER PUBLICATION boring_publication DROP TABLE public.orders'
expect_init_failure M2_INIT_PUBLICATION_RELATION_SET_MISMATCH relation-set
psqlc -qc 'ALTER PUBLICATION boring_publication ADD TABLE public.orders'
psqlc -qc 'ALTER PUBLICATION boring_publication OWNER TO boring_cdc'
expect_init_failure M2_INIT_PUBLICATION_OWNER_MISMATCH owner
psqlc -qc 'ALTER PUBLICATION boring_publication OWNER TO boring_cdc_admin'
for spec in \
  'update,delete,truncate:M2_INIT_PUBLICATION_PUBLISH_INSERT_MISMATCH:insert' \
  'insert,delete,truncate:M2_INIT_PUBLICATION_PUBLISH_UPDATE_MISMATCH:update' \
  'insert,update,truncate:M2_INIT_PUBLICATION_PUBLISH_DELETE_MISMATCH:delete' \
  'insert,update,delete:M2_INIT_PUBLICATION_PUBLISH_TRUNCATE_MISMATCH:truncate'
do
  IFS=: read -r publish expected label <<<"$spec"
  psqlc -qc "ALTER PUBLICATION boring_publication SET (publish='${publish}')"
  expect_init_failure "$expected" "publish-${label}"
done
psqlc -qc "ALTER PUBLICATION boring_publication SET (publish='insert,update,delete,truncate')"
psqlc -qc 'GRANT SELECT ON boring_cdc_control.heartbeat TO boring_cdc_control_writer'
expect_init_failure M2_INIT_CONTROL_PRIVILEGE_EXCESS privilege

[[ "$(psqlc -Atqc "select count(*) from pg_replication_slots where slot_name='boring_slot'")" == 0 ]]
echo 'M2_INIT_RECOVERY_E2E_OK postgres=17.6 empty_database=true documented_sql=true idempotent=true replay=blocked relation_set=diagnosed owner=diagnosed publish_flags=diagnosed privilege=diagnosed no_slot=true'
