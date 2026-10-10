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
  if [[ -n ${M2_SCHEMA_SAFE_STOP_OUT:-} || -n ${M2_SCHEMA_UNMATCHED_EOF_OUT:-} ]]; then
    sed -i 's/heartbeat_cadence_ms = 5000/heartbeat_cadence_ms = 300000/' "$work/run/boring-cdc.toml"
  fi
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
  if [[ -n ${M2_SCHEMA_SAFE_STOP_OUT:-} || -n ${M2_SCHEMA_UNMATCHED_EOF_OUT:-} ]]; then
    replication_pid=$(psqlc -Atqc 'SELECT pid FROM pg_stat_replication ORDER BY pid')
    advisory_pid=$(psqlc -Atqc "SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted AND pid <> pg_backend_pid() ORDER BY pid")
    [[ "$replication_pid" =~ ^[0-9]+$ && "$advisory_pid" =~ ^[0-9]+$ && "$replication_pid" != "$advisory_pid" ]] ||
      { echo 'E_SCHEMA_STOP_OWNERSHIP_AMBIGUOUS' >&2; exit 1; }
  fi
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
  python3 - "$journal" "$work/${scenario}-oracle.txt" "$confirmed" "$rejected_key" "$work/${scenario}-result.json" "$version" "$scenario" <<'PY'
import json,sqlite3,sys
journal,oracle_path,confirmed,rejected_key,result_path,version,scenario=sys.argv[1:]
oracle={tuple(line.strip().split('|')) for line in open(oracle_path) if line.strip()}
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
assert journal_set==oracle, {'postgres':sorted(oracle),'journal':sorted(journal_set)}
assert int(rejected_key) not in keys
assert failure==('unsupported','deterministic',1), failure
hi,lo=confirmed.split('/')
assert (int(hi,16)<<32)+int(lo,16) <= int(durable,16), (confirmed,durable)
result={'scenario':scenario,'postgres_version':version,'postgres_oracle_equals_read_only_journal':True,'rejected_key_absent_from_journal':True,'named_error':'M2_RELATION_SCHEMA_CHANGE_UNSUPPORTED','recovery':'confirmed_reseed','failure_class':failure[0],'retry_class':failure[1],'confirmed_flush_lsn':confirmed,'durable_journal_lsn':durable,'feedback_bounded':True}
with open(result_path,'w') as f: json.dump(result,f,sort_keys=True); f.write('\n')
print(json.dumps(result,sort_keys=True))
PY
}
verify_stable_stop() {
  local scenario=$1 key=$2 value=$3 extra=${4:-} before after confirmed state target_lsn sent_lsn deadline
  [[ -n ${M2_SCHEMA_SAFE_STOP_OUT:-} || -n ${M2_SCHEMA_UNMATCHED_EOF_OUT:-} ]] || return 0
  before=$(python3 - "$journal" <<'PY'
import json,sqlite3,sys
connection=sqlite3.connect(sys.argv[1])
print(json.dumps({
 'transactions':connection.execute('SELECT count(*) FROM source_transactions').fetchone()[0],
 'failure':connection.execute("SELECT failure_id,fingerprint,failure_class,retry_class,attempt,armed,failed_final_lsn FROM processing_failures WHERE component='capture' AND armed=1").fetchone(),
 'durable_lsn':connection.execute('SELECT durable_transaction_end_lsn FROM source_state WHERE singleton=1').fetchone()[0],
},sort_keys=True))
PY
)
  commit_order "$key" "$value" "$extra"
  target_lsn=$(psqlc -Atqc 'SELECT pg_current_wal_lsn()::text')
  deadline=$((SECONDS+10))
  while true; do
    sent_lsn=$(psqlc -Atqc "SELECT coalesce(sent_lsn::text,'0/0') FROM pg_stat_replication WHERE pid=$replication_pid")
    if python3 - "$target_lsn" "$sent_lsn" <<'PY'
import sys
def lsn(value):
    high,low=value.split('/')
    return (int(high,16)<<32)|int(low,16)
raise SystemExit(0 if lsn(sys.argv[2])>=lsn(sys.argv[1]) else 1)
PY
    then break; fi
    (( SECONDS < deadline )) || { echo 'E_SCHEMA_STOP_SOURCE_NOT_SENT' >&2; exit 1; }
    sleep .05
  done
  sleep .2
  state=$(ps -o stat= -p "$runtime_pid" 2>/dev/null || true)
  [[ -n "$state" && "$state" != Z* ]] || { echo 'E_SCHEMA_STOP_PROCESS_EXITED' >&2; exit 1; }
  [[ "$(psqlc -Atqc 'SELECT pid FROM pg_stat_replication ORDER BY pid')" == "$replication_pid" ]] ||
    { echo 'E_SCHEMA_STOP_REPLICATION_REOPENED' >&2; exit 1; }
  [[ "$(psqlc -Atqc "SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted AND pid <> pg_backend_pid() ORDER BY pid")" == "$advisory_pid" ]] ||
    { echo 'E_SCHEMA_STOP_OWNERSHIP_LOST' >&2; exit 1; }
  after=$(python3 - "$journal" <<'PY'
import json,sqlite3,sys
connection=sqlite3.connect(sys.argv[1])
print(json.dumps({
 'transactions':connection.execute('SELECT count(*) FROM source_transactions').fetchone()[0],
 'failure':connection.execute("SELECT failure_id,fingerprint,failure_class,retry_class,attempt,armed,failed_final_lsn FROM processing_failures WHERE component='capture' AND armed=1").fetchone(),
 'durable_lsn':connection.execute('SELECT durable_transaction_end_lsn FROM source_state WHERE singleton=1').fetchone()[0],
},sort_keys=True))
PY
)
  [[ "$before" == "$after" ]] || { echo 'E_SCHEMA_STOP_STATE_ADVANCED' >&2; exit 1; }
  confirmed=$(psqlc -Atqc "SELECT coalesce(write_lsn::text,'0/0')||','||coalesce(flush_lsn::text,'0/0')||','||coalesce(replay_lsn::text,'0/0') FROM pg_stat_replication WHERE pid=$replication_pid")
  python3 - "$before" "$confirmed" "$scenario" "$replication_pid" "$advisory_pid" "$version" "$work/${scenario}-safe-stop.json" <<'PY'
import json,sys
state=json.loads(sys.argv[1]); positions=sys.argv[2].split(',')
def lsn(value):
    high,low=value.split('/')
    return (int(high,16)<<32)|int(low,16)
assert len(positions)==3 and all(lsn(value)<=int(state['durable_lsn'],16) for value in positions)
assert state['failure'] is not None and state['failure'][2:6]==['unsupported','deterministic',1,1]
with open(sys.argv[7],'w') as output:
    json.dump({'schema_version':'m2-schema-safe-stop-observation/v1','scenario':sys.argv[3],
      'postgres_version':sys.argv[6],'replication_pid':int(sys.argv[4]),'advisory_pid':int(sys.argv[5]),
      'durable_transaction_count':state['transactions'],'durable_lsn':state['durable_lsn'],
      'failure_class':state['failure'][2],'retry_class':state['failure'][3],
      'failure_armed':True,'process_alive_after_later_commit':True,
      'replication_pid_stable':True,'advisory_pid_stable':True,
      'feedback_positions':sys.argv[2]},output,sort_keys=True)
    output.write('\n')
PY
}

run_nullable() {
  reset_source nullable
  start_runtime nullable
  commit_order 101 101.25
  wait_for_journal_key 101
  psqlc -Atqc "select xmin::text||'|'||id::text from public.orders order by id" >"$work/nullable-oracle.txt"
  psqlc -qc 'set role boring_cdc_admin; alter table public.orders add column note text null'
  commit_order 102 102.25 "'nullable-value'"
  wait_for_schema_stop nullable
  verify_fail_closed nullable 102
  verify_stable_stop nullable 103 103.25 "'later-value'"
}
run_incompatible() {
  reset_source incompatible
  start_runtime incompatible
  commit_order 201 201.25
  wait_for_journal_key 201
  psqlc -Atqc "select xmin::text||'|'||id::text from public.orders order by id" >"$work/incompatible-oracle.txt"
  psqlc -qc 'set role boring_cdc_admin; alter table public.orders alter column total type text using total::text'
  commit_order 202 "'incompatible-value'"
  wait_for_schema_stop incompatible
  verify_fail_closed incompatible 202
  verify_stable_stop incompatible 203 "'later-value'"
}

if [[ -z ${M2_SCHEMA_UNMATCHED_EOF_OUT:-} ]]; then
  run_nullable
fi
run_incompatible
if [[ -n ${M2_SCHEMA_UNMATCHED_EOF_OUT:-} ]]; then
  artifact=$M2_SCHEMA_UNMATCHED_EOF_OUT
  [[ ! -e "$artifact" ]] || { echo 'E_SCHEMA_EOF_ARTIFACT_EXISTS' >&2; exit 1; }
  [[ "$(psqlc -Atqc "SELECT pg_terminate_backend($replication_pid)::int")" == 1 ]] ||
    { echo 'E_SCHEMA_EOF_TERMINATE_FAILED' >&2; exit 1; }
  deadline=$((SECONDS+15))
  reopened=0
  while true; do
    state=$(ps -o stat= -p "$runtime_pid" 2>/dev/null || true)
    [[ -z "$state" || "$state" == Z* ]] && break
    current_replication=$(psqlc -Atqc 'SELECT pid FROM pg_stat_replication ORDER BY pid')
    [[ -z "$current_replication" || "$current_replication" == "$replication_pid" ]] || reopened=1
    current_advisory=$(psqlc -Atqc "SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted AND pid <> pg_backend_pid() ORDER BY pid")
    [[ -z "$current_advisory" || "$current_advisory" == "$advisory_pid" ]] || reopened=1
    (( SECONDS < deadline )) || { echo 'E_SCHEMA_EOF_EXIT_TIMEOUT' >&2; exit 1; }
    sleep .05
  done
  child=$runtime_pid; set +e; wait "$runtime_pid"; rc=$?; set -e; runtime_pid=
  [[ $rc -ne 0 && $reopened -eq 0 ]] || { echo 'E_SCHEMA_EOF_REOPEN_OR_SUCCESS' >&2; exit 1; }
  grep -q 'M2_COPYBOTH_UNEXPECTED_LOSS' "$work/incompatible-runtime.err" ||
    { echo 'E_SCHEMA_EOF_FAILURE_CODE' >&2; exit 1; }
  [[ "$(psqlc -Atqc "SELECT active::int FROM pg_replication_slots WHERE slot_name='boring_slot'")" == 0 ]] ||
    { echo 'E_SCHEMA_EOF_SLOT_ACTIVE' >&2; exit 1; }
  [[ "$(psqlc -Atqc 'SELECT count(*) FROM pg_stat_replication')" == 0 ]] ||
    { echo 'E_SCHEMA_EOF_REPLICATION_ACTIVE' >&2; exit 1; }
  [[ "$(psqlc -Atqc "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND granted")" == 0 ]] ||
    { echo 'E_SCHEMA_EOF_ADVISORY_ACTIVE' >&2; exit 1; }
  confirmed_after_eof=$(psqlc -Atqc "SELECT coalesce(confirmed_flush_lsn::text,'0/0') FROM pg_replication_slots WHERE slot_name='boring_slot'")
  mkdir -p "$artifact"
  python3 - "$journal" "$work/incompatible-safe-stop.json" "$artifact/observation.json" "$version" "$rc" "$confirmed_after_eof" <<'PY'
import json,sqlite3,sys
before=json.load(open(sys.argv[2]))
connection=sqlite3.connect(sys.argv[1])
count=connection.execute('SELECT count(*) FROM source_transactions').fetchone()[0]
durable=connection.execute('SELECT durable_transaction_end_lsn FROM source_state WHERE singleton=1').fetchone()[0]
failure=connection.execute("SELECT failure_class,retry_class,armed FROM processing_failures WHERE component='capture' AND armed=1").fetchall()
assert count==before['durable_transaction_count']==1 and durable==before['durable_lsn']
assert failure==[('unsupported','deterministic',1)],failure
high,low=sys.argv[6].split('/')
assert ((int(high,16)<<32)|int(low,16)) <= int(durable,16)
with open(sys.argv[3],'w') as output:
    json.dump({'schema_version':'m2-schema-unmatched-eof-observation/v1',
      'postgres_version':sys.argv[4],'runtime_exit_code':int(sys.argv[5]),
      'prior_durable_lsn':durable,'slot_confirmed_after_eof':sys.argv[6],
      'durable_transaction_count_after_eof':count,
      'original_replication_pid':before['replication_pid'],
      'original_advisory_pid':before['advisory_pid'],
      'active_failure_class':failure[0][0],'active_retry_class':failure[0][1],
      'active_failure_count':len(failure),'replication_reopened':False,
      'advisory_reacquired':False,'replication_slot_active_after_exit':False},output,sort_keys=True)
    output.write('\n')
PY
  echo "M2_SCHEMA_UNMATCHED_EOF_FENCED_OK postgres=$version"
  exit 0
fi
if [[ -n ${M2_SCHEMA_SAFE_STOP_OUT:-} ]]; then
  [[ ! -e "$M2_SCHEMA_SAFE_STOP_OUT" ]] || { echo 'E_SCHEMA_STOP_ARTIFACT_EXISTS' >&2; exit 1; }
  mkdir -p "$M2_SCHEMA_SAFE_STOP_OUT"
  cp -f "$work/nullable-safe-stop.json" "$work/incompatible-safe-stop.json" "$M2_SCHEMA_SAFE_STOP_OUT/"
fi
set +e
stop_bounded "$runtime_pid" TERM incompatible-runtime
stopped_status=$?
set -e
runtime_pid=
[[ "$stopped_status" == 4 ]]
elapsed=$((SECONDS-started))
echo "SCHEMA_CHANGE_LIVE_OK postgres=$version nullable=fail-closed incompatible=fail-closed error=M2_RELATION_SCHEMA_CHANGE_UNSUPPORTED recovery=confirmed-reseed feedback=bounded oracle=set-equality elapsed_seconds=$elapsed"
