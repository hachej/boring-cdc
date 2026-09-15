#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR="${TMPDIR:-/var/tmp}"; [[ "$TMPDIR" == /var/tmp ]]
cargo test --locked --workspace --all-targets
cargo test --locked m2_capture_runtime::tests
work=$(mktemp -d /var/tmp/m2-capture-e2e.XXXXXX); project="m2-capture-$RANDOM-$$"; port=$((56000 + $$ % 2000))
cleanup(){ docker compose -p "$project" -f compose.yaml -f "$work/override.yml" down -v --remove-orphans >/dev/null 2>&1 || true; rm -rf "$work"; }; trap cleanup EXIT INT TERM
printf 'm2-component-password-%s\n' "$project" >"$work/postgres_password"; chmod 600 "$work/postgres_password"; export BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password"; export PGPASSWORD; PGPASSWORD=$(cat "$BORING_CDC_POSTGRES_PASSWORD_FILE")
cat >"$work/override.yml" <<YAML
services:
  postgres:
    ports: ["127.0.0.1:${port}:5432"]
YAML
docker compose -p "$project" -f compose.yaml -f "$work/override.yml" up -d --wait postgres >/dev/null
psqlc(){ docker compose -p "$project" -f compose.yaml -f "$work/override.yml" exec -T postgres psql -v ON_ERROR_STOP=1 -U boring_cdc -d boring_cdc "$@"; }
psqlc <<'SQL' >/dev/null
CREATE TABLE customers(id bigint primary key,name text,tier int); CREATE TABLE order_items(id bigint primary key); CREATE TABLE orders(id bigint primary key); CREATE TABLE products(id bigint primary key);
CREATE PUBLICATION article1_publication FOR TABLE customers,order_items,orders,products WITH (publish='insert,update,delete');
SELECT * FROM pg_create_logical_replication_slot('article1_slot','pgoutput');
SQL
cargo build --quiet --locked --example m2_capture_runtime_component
mkdir -p "$work/state/spool"; dsn="postgresql://boring_cdc@127.0.0.1:${port}/boring_cdc?sslmode=disable"
target/debug/examples/m2_capture_runtime_component "$dsn" "$work/state/journal.sqlite" "$work/state/spool" >"$work/runtime.out" 2>"$work/runtime.err" & pid=$!
deadline=$((SECONDS+30)); until [[ "$(psqlc -Atqc "SELECT active::int FROM pg_replication_slots WHERE slot_name='article1_slot'")" == 1 ]]; do (( SECONDS < deadline )) || { cat "$work/runtime.err" >&2; exit 1; }; sleep .1; done
psqlc -c "BEGIN; INSERT INTO customers VALUES(1,'one',1); UPDATE customers SET name='two' WHERE id=1; COMMIT" >/dev/null
wait "$pid"; grep -q '"status":"pass"' "$work/runtime.out"; [[ ! -s "$work/runtime.err" ]]
python3 - "$work/state/journal.sqlite" <<'PY'
import sqlite3,sys
c=sqlite3.connect(sys.argv[1]); tx=c.execute('select count(*),max(end_lsn) from source_transactions').fetchone(); ev=c.execute('select count(*) from journal_events').fetchone()[0]
assert tx[0]==1 and ev==2 and tx[1] is not None
print('{"journal_transactions":1,"journal_events":2,"feedback_bounded":true}')
PY
version=$(psqlc -Atqc 'show server_version'); [[ "$version" == 17.6* ]]
export M2_RUNTIME_OUTPUT="$work/runtime.out" M2_POSTGRES_VERSION="$version"
python3 scripts/lib/m2_capture_runtime_evidence.py e2e
scripts/validate/evidence.sh artifacts/boring-cdc-m2-capture-runtime/SCN-M2-CAPTURE-RUNTIME-E2E/capture-runtime-component-v1/evidence.json
echo "M2_CAPTURE_RUNTIME_E2E_OK postgres=$version durable_before_feedback=true"
