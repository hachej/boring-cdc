#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR=/var/tmp
work=$(mktemp -d /var/tmp/m2-reconcile-e2e.XXXXXX); project="m2-reconcile-$RANDOM-$$"; port=$((57000 + $$ % 1000))
cleanup(){ if [[ "${KEEP:-0}" == 1 ]]; then return; fi; docker compose -p "$project" -f compose.yaml -f "$work/override.yml" down -v --remove-orphans >/dev/null 2>&1 || true; rm -rf "$work"; }; trap cleanup EXIT INT TERM
printf 'm2-reconcile-admin-%s\n' "$project" >"$work/postgres_password"; chmod 600 "$work/postgres_password"; export BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password"; export PGPASSWORD; PGPASSWORD=$(cat "$BORING_CDC_POSTGRES_PASSWORD_FILE")
printf 'm2-reconcile-runtime-%s\n' "$project" >"$work/runtime_password"; chmod 600 "$work/runtime_password"
cat >"$work/override.yml" <<YAML
services:
  postgres:
    ports: ["127.0.0.1:${port}:5432"]
    command: ["postgres", "-c", "wal_level=logical", "-c", "max_slot_wal_keep_size=1MB", "-c", "checkpoint_timeout=30min"]
YAML
docker compose -p "$project" -f compose.yaml -f "$work/override.yml" up -d --wait postgres >/dev/null
psqlc(){ docker compose -p "$project" -f compose.yaml -f "$work/override.yml" exec -T postgres psql -v ON_ERROR_STOP=1 -U boring_cdc -d boring_cdc "$@"; }
runtime_password=$(cat "$work/runtime_password")
psqlc -v runtime_password="$runtime_password" <<'SQL' >/dev/null
CREATE TABLE orders(id bigint primary key);
CREATE PUBLICATION article1_publication FOR TABLE orders WITH (publish='insert,update,delete,truncate');
SELECT * FROM pg_create_logical_replication_slot('article1_slot','pgoutput');
CREATE ROLE cdc_runtime LOGIN REPLICATION PASSWORD :'runtime_password';
GRANT CONNECT ON DATABASE boring_cdc TO cdc_runtime;
GRANT USAGE ON SCHEMA public TO cdc_runtime;
GRANT SELECT ON orders TO cdc_runtime;
SQL
[[ "$(psqlc -Atqc 'show server_version')" == 17.6* ]]
cargo build --quiet --locked --bin boring-cdc
mkdir -p "$work/run/state/spool"; chmod 700 "$work/run/state" "$work/run/state/spool"; cp tests/fixtures/m1_config/representative.toml "$work/run/boring-cdc.toml"
sed -i 's/publication = "boring_publication"/publication = "article1_publication"/; s/slot = "boring_slot"/slot = "article1_slot"/' "$work/run/boring-cdc.toml"
runtime_dsn="postgresql://cdc_runtime:${runtime_password}@127.0.0.1:${port}/boring_cdc?sslmode=disable"
admin_dsn="postgresql://boring_cdc:${PGPASSWORD}@127.0.0.1:${port}/boring_cdc?sslmode=disable"
export PG_RUNTIME="$runtime_dsn" PG_CONTROL="$admin_dsn" PG_ADMIN="$admin_dsn" CH_RUNTIME='https://unused.invalid' CH_MAINT='https://unused.invalid'
run_connector(){ (cd "$work/run"; exec env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" run); }
# A pre-existing slot beside a fresh durable journal is never silently adopted.
if run_connector >"$work/fresh.out" 2>"$work/fresh.err"; then echo E_FRESH_SLOT_ADMITTED >&2; exit 1; fi
grep -q M2_STARTUP_BLOCKED "$work/fresh.err" || { cat "$work/fresh.err" >&2; exit 1; }
database_oid=$(psqlc -Atqc "select oid from pg_database where datname=current_database()")
python3 - "$work/run/state/boring.db" "$database_oid" <<'PY'
import hashlib,json,sqlite3,sys
c=sqlite3.connect(sys.argv[1])
source=c.execute('select database_id,publication_fingerprint,observed_restart_lsn from source_state').fetchone()
canonical={'name':'article1_publication','owner_role':'boring_cdc','relations':['public.orders'],'operations':['delete','insert','truncate','update']}
expected=hashlib.sha256(json.dumps(canonical,separators=(',',':')).encode()).hexdigest()
assert source[0]==sys.argv[2] and source[1]==expected and source[2]
assert c.execute('select outcome,reason_code from startup_reconciliations order by reconciliation_id desc limit 1').fetchone()==('bootstrap_ambiguous_requires_restart','BOOTSTRAP_PROVENANCE_AMBIGUOUS')
PY
# Existing migrated schema with absent source_state retries by durable state, not file existence.
python3 - "$work/run/state/boring.db" <<'PY'
import sqlite3,sys
c=sqlite3.connect(sys.argv[1]); c.execute('pragma foreign_keys=on')
c.execute('delete from reseed_intents'); c.execute('delete from startup_reconciliations'); c.execute('delete from source_state')
c.execute("create trigger fail_source_receipt before insert on source_state begin select raise(abort,'fault after migration'); end")
c.commit()
PY
if run_connector >"$work/fault.out" 2>"$work/fault.err"; then echo E_SOURCE_RECEIPT_FAULT_ADMITTED >&2; exit 1; fi
[[ "$(python3 -c 'import sqlite3,sys; print(sqlite3.connect(sys.argv[1]).execute("select count(*) from source_state").fetchone()[0])' "$work/run/state/boring.db")" == 0 ]]
python3 - "$work/run/state/boring.db" <<'PY'
import sqlite3,sys
c=sqlite3.connect(sys.argv[1]); c.execute('drop trigger fail_source_receipt'); c.commit()
PY
# Abruptly terminate a real connector process while a migrated empty database cannot yet accept
# its source-state receipt, then release the lock and prove restart writes exactly one receipt.
python3 - "$work/run/state/boring.db" "$work/sqlite-locked" "$work/release-lock" <<'PY' & lock_pid=$!
import pathlib,sqlite3,sys,time
c=sqlite3.connect(sys.argv[1]); c.execute('begin immediate'); pathlib.Path(sys.argv[2]).touch()
while not pathlib.Path(sys.argv[3]).exists(): time.sleep(.02)
c.rollback()
PY
deadline=$((SECONDS+10)); until [[ -e "$work/sqlite-locked" ]]; do (( SECONDS < deadline )); sleep .02; done
(
  cd "$work/run"
  exec env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" run >"$work/crash.out" 2>"$work/crash.err"
) & crash_pid=$!
deadline=$((SECONDS+10)); until [[ "$(readlink "/proc/$crash_pid/exe" 2>/dev/null || true)" == */boring-cdc ]]; do
  (( SECONDS < deadline )) || { echo E_CRASH_PROCESS_NOT_EXEC >&2; exit 1; }
  sleep .02
done
kill -KILL "$crash_pid"; wait "$crash_pid" 2>/dev/null || true
[[ ! -e "/proc/$crash_pid" ]]
touch "$work/release-lock"; wait "$lock_pid"
[[ "$(python3 -c 'import sqlite3,sys; print(sqlite3.connect(sys.argv[1]).execute("select count(*) from source_state").fetchone()[0])' "$work/run/state/boring.db")" == 0 ]]
if run_connector >"$work/retry.out" 2>"$work/retry.err"; then echo E_UNPROVEN_RETRY_ADMITTED >&2; exit 1; fi
[[ "$(python3 -c 'import sqlite3,sys; print(sqlite3.connect(sys.argv[1]).execute("select count(*) from source_state").fetchone()[0])' "$work/run/state/boring.db")" == 1 ]]
# A missing live slot is distinct from an invalid slot and from unavailable resume WAL.
psqlc -qc "select pg_drop_replication_slot('article1_slot')" >/dev/null
if run_connector >"$work/missing.out" 2>"$work/missing.err"; then echo E_MISSING_SLOT_ADMITTED >&2; exit 1; fi
python3 - "$work/run/state/boring.db" <<'PY'
import sqlite3,sys
c=sqlite3.connect(sys.argv[1]); assert c.execute('select reason_code from startup_reconciliations order by reconciliation_id desc limit 1').fetchone()[0]=='SLOT_MISSING'
PY
# Recreate a provenance-bound slot, then drive PostgreSQL through its real unreserved and lost
# states. This observes wal_status and invalidation_reason rather than fabricating probe values.
psqlc -qc "select * from pg_create_logical_replication_slot('article1_slot','pgoutput')" >/dev/null
creation_floor=$(psqlc -Atqc "select confirmed_flush_lsn from pg_replication_slots where slot_name='article1_slot'")
creation_hex=$(python3 - "$creation_floor" <<'PY'
import sys
h,l=sys.argv[1].split('/'); print(f'{(int(h,16)<<32)+int(l,16):016X}')
PY
)
python3 - "$work/run/state/boring.db" "$creation_hex" <<'PY'
import sqlite3,sys
c=sqlite3.connect(sys.argv[1]); row=c.execute('select capture_epoch,source_system_id,database_id,slot_name from source_state').fetchone()
c.execute("insert into bootstrap_intents(intent_id,capture_epoch,source_system_id,database_id,slot_name,creation_floor_lsn,state,revision,created_at) values('live-slot',?,?,?,?,?,'slot_created',0,'component')",(*row,sys.argv[2]))
c.execute("update source_state set slot_creation_floor_lsn=?,slot_creation_intent_id='live-slot',control_revision=control_revision+1 where singleton=1",(sys.argv[2],)); c.commit()
PY
psqlc -qc 'create table wal_filler(data text)' >/dev/null
for batch in 1 2 3 4 5 6 7 8; do
  psqlc -qc "insert into wal_filler select repeat(md5((i+${batch}*10000)::text),100) from generate_series(1,10000) i" >/dev/null
  [[ "$(psqlc -Atqc "select wal_status from pg_replication_slots where slot_name='article1_slot'")" == unreserved ]] && break
done
[[ "$(psqlc -Atqc "select wal_status from pg_replication_slots where slot_name='article1_slot'")" == unreserved ]]
if run_connector >"$work/unreserved.out" 2>"$work/unreserved.err"; then echo E_UNRESERVED_WAL_ADMITTED >&2; exit 1; fi
python3 - "$work/run/state/boring.db" <<'PY'
import sqlite3,sys
c=sqlite3.connect(sys.argv[1]); assert c.execute('select reason_code from startup_reconciliations order by reconciliation_id desc limit 1').fetchone()[0]=='RESUME_WAL_STATUS_UNAVAILABLE'
PY
psqlc -qc 'checkpoint' >/dev/null
[[ "$(psqlc -Atqc "select wal_status||','||invalidation_reason from pg_replication_slots where slot_name='article1_slot'")" == 'lost,wal_removed' ]]
if run_connector >"$work/lost.out" 2>"$work/lost.err"; then echo E_INVALIDATED_SLOT_ADMITTED >&2; exit 1; fi
python3 - "$work/run/state/boring.db" <<'PY'
import sqlite3,sys
c=sqlite3.connect(sys.argv[1]); assert c.execute('select reason_code from startup_reconciliations order by reconciliation_id desc limit 1').fetchone()[0]=='SLOT_INVALID_WAL_REMOVED'
PY
# Exercise the live read-only CLI in JSON and text modes against the component journal.
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE -u PGPASSWORD -u PG_RUNTIME -u PG_CONTROL -u PG_ADMIN -u CH_RUNTIME -u CH_MAINT "$OLDPWD/target/debug/boring-cdc" journal verify --json) >"$work/journal.json"
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE -u PGPASSWORD -u PG_RUNTIME -u PG_CONTROL -u PG_ADMIN -u CH_RUNTIME -u CH_MAINT "$OLDPWD/target/debug/boring-cdc" journal verify) >"$work/journal.txt"
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE -u PGPASSWORD -u PG_RUNTIME -u PG_CONTROL -u PG_ADMIN -u CH_RUNTIME -u CH_MAINT "$OLDPWD/target/debug/boring-cdc" recover inspect --json) >"$work/recover.json"
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE -u PGPASSWORD -u PG_RUNTIME -u PG_CONTROL -u PG_ADMIN -u CH_RUNTIME -u CH_MAINT "$OLDPWD/target/debug/boring-cdc" recover inspect) >"$work/recover.txt"
python3 - "$work/journal.json" "$work/recover.json" "$work/journal.txt" "$work/recover.txt" <<'PY'
import json,sys
j=json.load(open(sys.argv[1])); r=json.load(open(sys.argv[2]))
assert j['outcome']=='success' and j['data']['integrity']=='ok'
assert r['outcome']=='success' and r['data']['latest_reason_code']=='SLOT_INVALID_WAL_REMOVED'
for p in sys.argv[3:]:
 s=open(p).read(); assert 'postgresql://' not in s and 'm2-reconcile-' not in s and s.strip()
PY
printf '{"postgres":"17.6","database_oid_observed":true,"live_publication_fingerprint":true,"fresh_slot_ambiguous":true,"migration_receipt_retry":true,"slot_reasons":["SLOT_MISSING","RESUME_WAL_STATUS_UNAVAILABLE","SLOT_INVALID_WAL_REMOVED"],"abrupt_process_restart":true,"cli_json_text_matrix":true}\n'
