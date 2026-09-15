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
run_connector(){ (cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" run); }
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
if run_connector >"$work/retry.out" 2>"$work/retry.err"; then echo E_UNPROVEN_RETRY_ADMITTED >&2; exit 1; fi
[[ "$(python3 -c 'import sqlite3,sys; print(sqlite3.connect(sys.argv[1]).execute("select count(*) from source_state").fetchone()[0])' "$work/run/state/boring.db")" == 1 ]]
# A missing live slot is distinct from an invalid slot and from unavailable resume WAL.
psqlc -qc "select pg_drop_replication_slot('article1_slot')" >/dev/null
if run_connector >"$work/missing.out" 2>"$work/missing.err"; then echo E_MISSING_SLOT_ADMITTED >&2; exit 1; fi
python3 - "$work/run/state/boring.db" <<'PY'
import sqlite3,sys
c=sqlite3.connect(sys.argv[1]); assert c.execute('select reason_code from startup_reconciliations order by reconciliation_id desc limit 1').fetchone()[0]=='SLOT_MISSING'
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
assert r['outcome']=='success' and r['data']['latest_reason_code']=='SLOT_MISSING'
for p in sys.argv[3:]:
 s=open(p).read(); assert 'postgresql://' not in s and 'm2-reconcile-' not in s and s.strip()
PY
printf '{"postgres":"17.6","database_oid_observed":true,"live_publication_fingerprint":true,"fresh_slot_ambiguous":true,"migration_receipt_retry":true,"slot_reason":"SLOT_MISSING","cli_json_text_matrix":true}\n'
