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
cargo build --quiet --locked --bin boring-cdc
mkdir -p "$work/run/state/spool"; chmod 700 "$work/run/state" "$work/run/state/spool"; cp tests/fixtures/m1_config/representative.toml "$work/run/boring-cdc.toml"
sed -i 's/publication = "boring_publication"/publication = "article1_publication"/; s/slot = "boring_slot"/slot = "article1_slot"/; s#sqlite_path = "state/boring.db"#sqlite_path = "state/journal.sqlite"#' "$work/run/boring-cdc.toml"
dsn="postgresql://boring_cdc@127.0.0.1:${port}/boring_cdc?sslmode=disable"
export PG_RUNTIME="$dsn" PG_CONTROL="${dsn}&application_name=control" PG_ADMIN="${dsn}&application_name=admin"
export CH_RUNTIME='https://runtime:runtime-only@127.0.0.1:8443' CH_MAINT='https://maint:maint-only@127.0.0.1:8443'
(
  cd "$work/run"
  exec env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" run >"$work/runtime.out" 2>"$work/runtime.err"
) & pid=$!
deadline=$((SECONDS+30)); until [[ "$(psqlc -Atqc "SELECT active::int FROM pg_replication_slots WHERE slot_name='article1_slot'")" == 1 ]]; do (( SECONDS < deadline )) || { cat "$work/runtime.err" >&2; exit 1; }; sleep .1; done
python3 - "$work/run/state/journal.sqlite" "$work/sqlite-locked" <<'PY2' & lock_pid=$!
import pathlib,sqlite3,sys,time
c=sqlite3.connect(sys.argv[1]); c.execute('BEGIN IMMEDIATE'); pathlib.Path(sys.argv[2]).touch(); time.sleep(2); c.commit()
PY2
deadline=$((SECONDS+10)); until [[ -e "$work/sqlite-locked" ]]; do (( SECONDS < deadline )); sleep .02; done
psqlc -c "BEGIN; INSERT INTO customers VALUES(1,'one',1); UPDATE customers SET name='two' WHERE id=1; COMMIT" >/dev/null
sleep .25
blocked_transactions=$(python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
print(sqlite3.connect(sys.argv[1]).execute('select count(*) from source_transactions').fetchone()[0])
PY2
)
blocked_feedback=$(psqlc -Atqc "SELECT coalesce(write_lsn::text,'0/0')||','||coalesce(flush_lsn::text,'0/0')||','||coalesce(replay_lsn::text,'0/0') FROM pg_stat_replication ORDER BY pid LIMIT 1")
[[ "$blocked_transactions" == 0 && "$blocked_feedback" == '0/0,0/0,0/0' ]]
wait "$lock_pid"
deadline=$((SECONDS+30)); until [[ "$(python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
try: print(sqlite3.connect(sys.argv[1]).execute('select count(*) from source_transactions').fetchone()[0])
except Exception: print(0)
PY2
)" == 1 ]]; do (( SECONDS < deadline )) || { cat "$work/runtime.err" >&2; exit 1; }; sleep .1; done
feedback=$(psqlc -Atqc "SELECT coalesce(write_lsn::text,'')||','||coalesce(flush_lsn::text,'')||','||coalesce(replay_lsn::text,'') FROM pg_stat_replication WHERE application_name='' OR application_name IS NOT NULL ORDER BY pid LIMIT 1")
[[ "$feedback" =~ ^[^,]+,[^,]+,[^,]+$ ]]
kill -TERM "$pid"; wait "$pid"; [[ ! -s "$work/runtime.err" ]]
python3 - "$work/run/state/journal.sqlite" "$feedback" <<'PY2'
import sqlite3,sys
c=sqlite3.connect(sys.argv[1]); tx=c.execute('select count(*),max(end_lsn) from source_transactions').fetchone(); ev=c.execute('select count(*) from journal_events').fetchone()[0]
assert tx[0]==1 and ev==2 and tx[1] is not None
print('{"journal_transactions":1,"journal_events":2,"feedback_bounded":true,"server_feedback_positions":"%s"}'%sys.argv[2])
PY2
version=$(psqlc -Atqc 'show server_version'); [[ "$version" == 17.6* ]]
printf '{"command":"CMD-RUN","exit":0,"postgres":"%s","journal_transactions":1,"journal_events":2,"durable_before_feedback":true,"blocked_sqlite_transactions":0,"blocked_server_feedback_positions":"0/0,0/0,0/0","server_feedback_positions":"%s"}\n' "$version" "$feedback" >"$work/observation.json"
export M2_RUNTIME_OUTPUT="$work/runtime.out" M2_RUNTIME_OBSERVATION="$work/observation.json" M2_POSTGRES_VERSION="$version"
python3 scripts/lib/m2_capture_runtime_evidence.py e2e
scripts/validate/evidence.sh artifacts/boring-cdc-m2-capture-runtime/SCN-M2-CAPTURE-RUNTIME-E2E/capture-runtime-production-v1/evidence.json
echo "M2_CAPTURE_RUNTIME_E2E_OK postgres=$version durable_before_feedback=true"
