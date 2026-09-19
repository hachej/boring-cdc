#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp

started=$SECONDS
work=$(mktemp -d /var/tmp/schema-change-live.XXXXXX)
project="schema-change-$RANDOM-$$"
port=$((55000 + $$ % 500))
runtime_pid=
bootstrap_pid=
cleanup() {
  [[ -z "$runtime_pid" ]] || kill -KILL "$runtime_pid" >/dev/null 2>&1 || true
  [[ -z "$bootstrap_pid" ]] || kill -KILL "$bootstrap_pid" >/dev/null 2>&1 || true
  docker compose -p "$project" -f compose.yaml -f "$work/override.yml" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT INT TERM
stop_bounded() {
  local child=$1 signal=$2 label=$3 deadline
  kill -"$signal" "$child"
  deadline=$((SECONDS+10))
  while kill -0 "$child" 2>/dev/null; do
    if (( SECONDS >= deadline )); then
      kill -KILL "$child" >/dev/null 2>&1 || true
      wait "$child" 2>/dev/null || true
      echo "E_PROCESS_SHUTDOWN_TIMEOUT $label" >&2
      return 1
    fi
    sleep .1
  done
  wait "$child"
}

printf 'schema-change-postgres-%s\n' "$project" >"$work/postgres_password"
chmod 600 "$work/postgres_password"
export BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password"
export PGPASSWORD
PGPASSWORD=$(cat "$BORING_CDC_POSTGRES_PASSWORD_FILE")
cat >"$work/override.yml" <<YAML
services:
  postgres:
    ports: ["127.0.0.1:${port}:5432"]
YAML
docker compose -p "$project" -f compose.yaml -f "$work/override.yml" up -d --wait postgres >/dev/null
psqlc() { docker compose -p "$project" -f compose.yaml -f "$work/override.yml" exec -T postgres psql -X -v ON_ERROR_STOP=1 -U boring_cdc -d boring_cdc "$@"; }
version=$(psqlc -Atqc 'show server_version')
[[ "$version" == 17.6* ]]

admin_credential='admin-schema-change'
runtime_credential='runtime-schema-change'
control_credential='control-schema-change'
application_credential='application-schema-change'
cargo build --quiet --locked --bin boring-cdc
binary="$PWD/target/debug/boring-cdc"
admin_dsn=postgresql:"//boring_cdc_admin:${admin_credential}@127.0.0.1:${port}/boring_cdc?sslmode=disable"
export PG_RUNTIME=postgresql:"//boring_cdc_runtime:${runtime_credential}@127.0.0.1:${port}/boring_cdc?sslmode=disable"
export PG_CONTROL=postgresql:"//boring_cdc_control_writer:${control_credential}@127.0.0.1:${port}/boring_cdc?sslmode=disable"
export CH_RUNTIME='https://unused.invalid'

reset_source() {
  local scenario=$1
  [[ -z "$runtime_pid" ]] || { stop_bounded "$runtime_pid" TERM "$scenario-runtime" || true; runtime_pid=; }
  psqlc -qc "select pg_drop_replication_slot('boring_slot') where exists (select 1 from pg_replication_slots where slot_name='boring_slot')"
  psqlc -qc 'drop publication if exists boring_publication; drop schema if exists boring_cdc_control cascade; drop table if exists public.orders cascade'
  rm -rf "$work/run"
  mkdir -p "$work/run/state/spool" "$work/run/state/tmp" "$work/run/archive/root"
  chmod 700 "$work/run/state" "$work/run/state/spool" "$work/run/archive" "$work/run/archive/root"
  cp tests/fixtures/m1_config/representative.toml "$work/run/boring-cdc.toml"
  psqlc -v "admin_password=$admin_credential" -v "runtime_password=$runtime_credential" -v "control_password=$control_credential" -v "application_password=$application_credential" \
    < scripts/setup/durable_simple_prerequisites.sql >"$work/${scenario}-prerequisites.out"

  export PG_ADMIN="$admin_dsn" CH_MAINT='https://unused.invalid'
  (cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE -u PG_ADMIN -u CH_MAINT "$binary" init --dry-run --json) >"$work/${scenario}-init-dry-run.json"
  token=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["data"]["confirm_token"])' "$work/${scenario}-init-dry-run.json")
  (cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$binary" init --confirm --confirm-token "$token" --json) >"$work/${scenario}-init-confirm.json"
  unset PG_ADMIN CH_MAINT

  (cd "$work/run"; exec env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$binary" run --bootstrap >"$work/${scenario}-bootstrap.out" 2>"$work/${scenario}-bootstrap.err") &
  bootstrap_pid=$!
  deadline=$((SECONDS+30))
  until [[ "$(psqlc -Atqc "select count(*) from pg_replication_slots where slot_name='boring_slot' and plugin='pgoutput'")" == 1 ]]; do
    (( SECONDS < deadline )) || { cat "$work/${scenario}-bootstrap.err" >&2; exit 1; }
    kill -0 "$bootstrap_pid" 2>/dev/null || { wait "$bootstrap_pid"; exit 1; }
    sleep .1
  done
  sleep 1
  stop_bounded "$bootstrap_pid" INT "$scenario-bootstrap"
  bootstrap_pid=
}

journal="$work/run/state/boring.db"
start_runtime() {
  local scenario=$1
  (cd "$work/run"; exec env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$binary" run >"$work/${scenario}-runtime.out" 2>"$work/${scenario}-runtime.err") &
  runtime_pid=$!
  deadline=$((SECONDS+30))
  until [[ "$(psqlc -Atqc "select active::int from pg_replication_slots where slot_name='boring_slot'")" == 1 ]]; do
    (( SECONDS < deadline )) || { cat "$work/${scenario}-runtime.err" >&2; exit 1; }
    kill -0 "$runtime_pid" 2>/dev/null || { wait "$runtime_pid"; exit 1; }
    sleep .1
  done
}
commit_order() {
  local key=$1 value=$2 extra=${3:-}
  psqlc -qc "begin; set local role boring_cdc_app; insert into public.orders(id,total${extra:+,note}) values(${key},${value}${extra:+,${extra}}); commit"
}
wait_for_journal_key() {
  local key=$1 deadline=$((SECONDS+30))
  until python3 - "$journal" "$key" <<'PY'
import json,sqlite3,sys
with sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True) as db:
    payloads=db.execute("select cast(payload as text) from journal_events order by journal_seq").fetchall()
keys=[]
for (text,) in payloads:
    row=json.loads(text)
    if row.get('kind')=='insert' and row.get('new'):
        keys.append(int(bytes(row['new'][0]['bytes']).decode()))
raise SystemExit(0 if int(sys.argv[2]) in keys else 1)
PY
  do
    (( SECONDS < deadline )) || exit 1
    sleep .1
  done
}
capture_feedback_boundary() {
  local scenario=$1 deadline=$((SECONDS+30)) confirmed durable
  while true; do
    confirmed=$(psqlc -Atqc "select confirmed_flush_lsn::text from pg_replication_slots where slot_name='boring_slot'")
    durable=$(python3 - "$journal" <<'PY'
import sqlite3,sys
with sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True) as db:
    print(db.execute("select durable_transaction_end_lsn from source_state where singleton=1").fetchone()[0])
PY
)
    python3 - "$confirmed" "$durable" <<'PY' && break
import sys
hi,lo=sys.argv[1].split('/')
raise SystemExit(0 if (int(hi,16)<<32)+int(lo,16) == int(sys.argv[2],16) else 1)
PY
    (( SECONDS < deadline )) || { echo "E_FEEDBACK_DID_NOT_REACH_DURABLE_BOUNDARY $scenario confirmed=$confirmed durable=$durable" >&2; exit 1; }
    sleep .1
  done
  printf '%s|%s\n' "$confirmed" "$durable" >"$work/${scenario}-accepted-boundary.txt"
}
wait_for_schema_stop() {
  local scenario=$1 deadline=$((SECONDS+30))
  until grep -q '^M2_RELATION_SCHEMA_CHANGE_UNSUPPORTED detail=relation_contract_mismatch recovery=confirmed_reseed$' "$work/${scenario}-runtime.err"; do
    (( SECONDS < deadline )) || { cat "$work/${scenario}-runtime.err" >&2; exit 1; }
    kill -0 "$runtime_pid" 2>/dev/null || { wait "$runtime_pid"; exit 1; }
    sleep .1
  done
}
verify_fail_closed() {
  local scenario=$1 rejected_key=$2
  confirmed=$(psqlc -Atqc "select confirmed_flush_lsn::text from pg_replication_slots where slot_name='boring_slot'")
  psqlc -Atqc "select xmin::text||'|'||id::text from public.orders order by id" >"$work/${scenario}-full-oracle.txt"
  python3 - "$journal" "$work/${scenario}-accepted-oracle.txt" "$work/${scenario}-full-oracle.txt" "$work/${scenario}-accepted-boundary.txt" "$confirmed" "$rejected_key" "$work/${scenario}-result.json" "$version" "$scenario" <<'PY'
import json,sqlite3,sys
journal,accepted_oracle_path,full_oracle_path,boundary_path,confirmed,rejected_key,result_path,version,scenario=sys.argv[1:]
accepted_oracle={tuple(line.strip().split('|')) for line in open(accepted_oracle_path) if line.strip()}
full_oracle={tuple(line.strip().split('|')) for line in open(full_oracle_path) if line.strip()}
accepted_confirmed,accepted_durable=open(boundary_path).read().strip().split('|')
with sqlite3.connect(f"file:{journal}?mode=ro", uri=True) as db:
    transactions=db.execute("select transaction_id,xid,end_lsn from source_transactions where state='committed'").fetchall()
    events=db.execute("select transaction_id,cast(payload as text) from journal_events order by journal_seq").fetchall()
    durable=db.execute("select durable_transaction_end_lsn from source_state where singleton=1").fetchone()[0]
    failure=db.execute("select failure_class,retry_class,armed from processing_failures where component='capture'").fetchone()
xids={transaction_id:xid for transaction_id,xid,_ in transactions}
journal_set=set()
keys=[]
for transaction_id,text in events:
    payload=json.loads(text)
    if payload.get('kind')=='insert' and payload.get('new'):
        key=str(int(bytes(payload['new'][0]['bytes']).decode()))
        keys.append(int(key)); journal_set.add((xids[transaction_id],key))
assert journal_set==accepted_oracle, {'accepted_postgres':sorted(accepted_oracle),'journal':sorted(journal_set)}
full_keys={int(key) for _,key in full_oracle}
accepted_keys={int(key) for _,key in accepted_oracle}
# ALTER TYPE may rewrite pre-existing rows and therefore change their xmin. Keys remain
# the stable source-owned identity for proving that exactly the rejected row committed.
assert full_keys==accepted_keys|{int(rejected_key)}, {'accepted':sorted(accepted_oracle),'full':sorted(full_oracle)}
assert int(rejected_key) not in keys
assert failure==('unsupported','deterministic',1), failure
assert confirmed==accepted_confirmed, (confirmed,accepted_confirmed)
assert durable==accepted_durable, (durable,accepted_durable)
hi,lo=confirmed.split('/')
assert (int(hi,16)<<32)+int(lo,16) == int(durable,16), (confirmed,durable)
result={'scenario':scenario,'postgres_version':version,'accepted_postgres_oracle_equals_read_only_journal':True,'rejected_postgres_key_committed':int(rejected_key),'rejected_key_absent_from_journal':True,'named_error':'M2_RELATION_SCHEMA_CHANGE_UNSUPPORTED','recovery':'confirmed_reseed','failure_class':failure[0],'retry_class':failure[1],'confirmed_flush_lsn':confirmed,'durable_journal_lsn':durable,'feedback_did_not_advance_past_accepted_boundary':True}
with open(result_path,'w') as f: json.dump(result,f,sort_keys=True); f.write('\n')
print(json.dumps(result,sort_keys=True))
PY
}
verify_restart_blocked() {
  local scenario=$1 stopped_status restart_status
  set +e
  stop_bounded "$runtime_pid" TERM "$scenario-runtime"
  stopped_status=$?
  set -e
  runtime_pid=
  [[ "$stopped_status" == 4 ]]
  set +e
  (cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$binary" run >"$work/${scenario}-restart.out" 2>"$work/${scenario}-restart.err")
  restart_status=$?
  set -e
  [[ "$restart_status" != 0 ]]
  grep -q '^M2_EXPLICIT_REARM_REQUIRED: capture ownership unavailable$' "$work/${scenario}-restart.err"
}

run_nullable() {
  reset_source nullable
  start_runtime nullable
  commit_order 101 101.25
  wait_for_journal_key 101
  capture_feedback_boundary nullable
  psqlc -Atqc "select xmin::text||'|'||id::text from public.orders order by id" >"$work/nullable-accepted-oracle.txt"
  psqlc -qc 'set role boring_cdc_admin; alter table public.orders add column note text null'
  commit_order 102 102.25 "'nullable-value'"
  wait_for_schema_stop nullable
  verify_fail_closed nullable 102
  verify_restart_blocked nullable
}
run_incompatible() {
  reset_source incompatible
  start_runtime incompatible
  commit_order 201 201.25
  wait_for_journal_key 201
  capture_feedback_boundary incompatible
  psqlc -Atqc "select xmin::text||'|'||id::text from public.orders order by id" >"$work/incompatible-accepted-oracle.txt"
  psqlc -qc 'set role boring_cdc_admin; alter table public.orders alter column total type text using total::text'
  commit_order 202 "'incompatible-value'"
  wait_for_schema_stop incompatible
  verify_fail_closed incompatible 202
  verify_restart_blocked incompatible
}

run_nullable
run_incompatible
elapsed=$((SECONDS-started))
echo "SCHEMA_CHANGE_LIVE_OK postgres=$version nullable=fail-closed incompatible=fail-closed error=M2_RELATION_SCHEMA_CHANGE_UNSUPPORTED recovery=confirmed-reseed feedback=frozen-at-accepted-boundary restart=M2_EXPLICIT_REARM_REQUIRED oracle=accepted-set-equality elapsed_seconds=$elapsed"
