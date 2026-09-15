#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR=/var/tmp
work=$(mktemp -d /var/tmp/m2-init-e2e.XXXXXX); project="m2-init-$RANDOM-$$"; port=$((58000 + $$ % 1000))
cleanup(){ docker compose -p "$project" -f compose.yaml -f "$work/override.yml" down -v --remove-orphans >/dev/null 2>&1 || true; rm -rf "$work"; }; trap cleanup EXIT INT TERM
printf 'm2-init-password-%s\n' "$project" >"$work/postgres_password"; chmod 600 "$work/postgres_password"; export BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password"
cat >"$work/override.yml" <<YAML
services:
  postgres:
    ports: ["127.0.0.1:${port}:5432"]
YAML
docker compose -p "$project" -f compose.yaml -f "$work/override.yml" up -d --wait postgres >/dev/null
psqlc(){ docker compose -p "$project" -f compose.yaml -f "$work/override.yml" exec -T postgres psql -v ON_ERROR_STOP=1 -U boring_cdc -d boring_cdc "$@"; }
[[ "$(psqlc -Atqc 'show server_version')" == 17.6* ]]
psqlc -qc 'CREATE TABLE customers(id bigint primary key); CREATE TABLE order_items(id bigint primary key); CREATE TABLE orders(id bigint primary key); CREATE TABLE products(id bigint primary key)' >/dev/null
cargo build --quiet --locked --bin boring-cdc
mkdir -p "$work/run/state/spool"; chmod 700 "$work/run/state" "$work/run/state/spool"; cp tests/fixtures/m1_config/representative.toml "$work/run/boring-cdc.toml"
password=$(cat "$work/postgres_password"); dsn="postgresql://boring_cdc:${password}@127.0.0.1:${port}/boring_cdc?sslmode=disable"; export PG_ADMIN="$dsn" CH_MAINT='https://unused.invalid'; unset PG_RUNTIME PG_CONTROL CH_RUNTIME || true
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" init --dry-run --json) >"$work/dry.json"
token=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["data"]["confirm_token"])' "$work/dry.json")
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" init --confirm --confirm-token "$token" --json) >"$work/first.json"
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" init --confirm --confirm-token "$token" --json) >"$work/second.json"
[[ "$(psqlc -Atqc "select count(*) from pg_replication_slots where slot_name='boring_slot'")" == 0 ]]
[[ "$(psqlc -Atqc 'select count(*) from boring_cdc_control.heartbeat')" == 1 ]]
[[ "$(psqlc -Atqc 'select count(*) from boring_cdc_control.capture_fences')" == 1 ]]
[[ "$(psqlc -Atqc "select count(*) from pg_publication_tables where pubname='boring_publication'")" == 3 ]]
[[ "$(python3 -c 'import sqlite3,sys; print(sqlite3.connect(sys.argv[1]).execute("select count(*) from source_state").fetchone()[0])' "$work/run/state/boring.db")" == 1 ]]
python3 - "$work/first.json" "$work/second.json" <<'PY'
import json,sys
for p in sys.argv[1:]:
 x=json.load(open(p)); assert x['outcome']=='success' and x['data']['logical_slot_exists'] is False and x['data']['control_rows']==2
 s=open(p).read(); assert 'postgresql://' not in s and 'm2-init-password' not in s
PY
echo 'M2_INIT_RECOVERY_E2E_OK postgres=17.6 idempotent=true no_slot=true'
