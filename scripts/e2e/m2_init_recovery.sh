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
psqlc(){ docker compose -p "$project" -f compose.yaml -f "$work/override.yml" exec -T postgres psql -v ON_ERROR_STOP=1 -U boring_cdc -d boring_cdc "$@"; }
[[ "$(psqlc -Atqc 'show server_version')" == 17.6* ]]
psqlc -qc 'CREATE TABLE customers(id bigint primary key); CREATE TABLE order_items(id bigint primary key); CREATE TABLE orders(id bigint primary key); CREATE TABLE products(id bigint primary key)' >/dev/null
cargo build --quiet --locked --bin boring-cdc
mkdir -p "$work/run/state/spool"; chmod 700 "$work/run/state" "$work/run/state/spool"; cp tests/fixtures/m1_config/representative.toml "$work/run/boring-cdc.toml"
dsn="postgresql://boring_cdc@127.0.0.1:${port}/boring_cdc?sslmode=disable"; export PG_ADMIN="$dsn" CH_MAINT='https://unused.invalid'; unset PG_RUNTIME PG_CONTROL CH_RUNTIME || true
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" init --dry-run --json) >"$work/dry.json"
token=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["data"]["confirm_token"])' "$work/dry.json")
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" init --confirm --confirm-token "$token" --json) >"$work/first.json"
if (cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" init --confirm --confirm-token "$token" --json) >/dev/null 2>&1; then echo E_INIT_TOKEN_REPLAY >&2; exit 1; fi
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" init --dry-run --json) >"$work/dry2.json"
token2=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["data"]["confirm_token"])' "$work/dry2.json")
[[ "$token" != "$token2" ]]
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" init --confirm --confirm-token "$token2" --json) >"$work/second.json"
[[ "$(psqlc -Atqc "select count(*) from pg_replication_slots where slot_name='boring_slot'")" == 0 ]]
[[ "$(psqlc -Atqc 'select count(*) from boring_cdc_control.heartbeat')" == 1 ]]
[[ "$(psqlc -Atqc 'select count(*) from boring_cdc_control.capture_fences')" == 1 ]]
[[ "$(psqlc -Atqc "SELECT count(*) FROM pg_constraint WHERE conrelid='boring_cdc_control.capture_fences'::regclass AND pg_get_expr(conbin,conrelid) IN ('(octet_length(table_set_fingerprint) = 32)','(octet_length(unique_nonce) = 16)')")" == 2 ]]
[[ "$(psqlc -Atqc "select count(*) from pg_publication_tables where pubname='boring_publication'")" == 3 ]]
[[ "$(python3 -c 'import sqlite3,sys; print(sqlite3.connect(sys.argv[1]).execute("select count(*) from source_state").fetchone()[0])' "$work/run/state/boring.db")" == 1 ]]
python3 - "$work/first.json" "$work/second.json" <<'PY'
import json,sys
for p in sys.argv[1:]:
 x=json.load(open(p)); assert x['outcome']=='success' and x['data']['logical_slot_exists'] is False and x['data']['control_rows']==2 and x['mutation_trace'] and x['postcondition_evidence_digest']
 s=open(p).read(); assert 'postgresql://' not in s and 'm2-init-password' not in s
PY
# An existing lookalike table with weak byte-length checks must not pass CREATE IF NOT EXISTS.
psqlc -qc 'ALTER TABLE boring_cdc_control.capture_fences DROP CONSTRAINT capture_fences_table_set_fingerprint_check, DROP CONSTRAINT capture_fences_unique_nonce_check; ALTER TABLE boring_cdc_control.capture_fences ADD CONSTRAINT weak_table_length CHECK(octet_length(table_set_fingerprint)>=16), ADD CONSTRAINT weak_nonce_length CHECK(octet_length(unique_nonce)>=8)'
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" init --dry-run --json) >"$work/dry3.json"
token3=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["data"]["confirm_token"])' "$work/dry3.json")
if (cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" init --confirm --confirm-token "$token3" --json) >"$work/length-drift.out" 2>"$work/length-drift.err"; then echo E_LENGTH_DRIFT_ACCEPTED >&2; exit 1; fi
grep -q M2_INIT_CONTROL_LENGTH_CONSTRAINT_INVALID "$work/length-drift.err"; ! grep -q 'postgresql://' "$work/length-drift.err"
# Repair the source and resume the exact nonterminal plan before exercising the next independent drift.
psqlc -qc 'ALTER TABLE boring_cdc_control.capture_fences DROP CONSTRAINT weak_table_length, DROP CONSTRAINT weak_nonce_length; ALTER TABLE boring_cdc_control.capture_fences ADD CONSTRAINT capture_fences_table_set_fingerprint_check CHECK(octet_length(table_set_fingerprint)=32), ADD CONSTRAINT capture_fences_unique_nonce_check CHECK(octet_length(unique_nonce)=16)'
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" init --confirm --confirm-token "$token3" --json) >"$work/recovered.json"
# The exact expression is still insufficient when PostgreSQL has not validated existing rows.
psqlc -qc 'ALTER TABLE boring_cdc_control.capture_fences DROP CONSTRAINT capture_fences_unique_nonce_check; ALTER TABLE boring_cdc_control.capture_fences ADD CONSTRAINT capture_fences_unique_nonce_check CHECK(octet_length(unique_nonce)=16) NOT VALID'
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" init --dry-run --json) >"$work/dry4.json"
token4=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["data"]["confirm_token"])' "$work/dry4.json")
if (cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" init --confirm --confirm-token "$token4" --json) >"$work/unvalidated.out" 2>"$work/unvalidated.err"; then echo E_UNVALIDATED_LENGTH_ACCEPTED >&2; exit 1; fi
grep -q M2_INIT_CONTROL_LENGTH_CONSTRAINT_INVALID "$work/unvalidated.err"
psqlc -qc 'ALTER TABLE boring_cdc_control.capture_fences VALIDATE CONSTRAINT capture_fences_unique_nonce_check'
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" init --confirm --confirm-token "$token4" --json) >"$work/validated.json"
# Existing excess privilege is detected, never silently repaired.
psqlc -qc 'GRANT SELECT ON boring_cdc_control.heartbeat TO boring_cdc_control_writer'
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" init --dry-run --json) >"$work/dry5.json"
token5=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["data"]["confirm_token"])' "$work/dry5.json")
if (cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" init --confirm --confirm-token "$token5" --json) >"$work/drift.out" 2>"$work/drift.err"; then echo E_PRIVILEGE_DRIFT_ACCEPTED >&2; exit 1; fi
grep -q M2_INIT_CONTROL_PRIVILEGE_EXCESS "$work/drift.err"; ! grep -q 'postgresql://' "$work/drift.err"
[[ "$(psqlc -Atqc "select count(*) from pg_replication_slots where slot_name='boring_slot'")" == 0 ]]
echo 'M2_INIT_RECOVERY_E2E_OK postgres=17.6 idempotent=true replay=blocked privilege_drift=blocked exact_fence_lengths=true validated_fence_lengths=true no_slot=true'
