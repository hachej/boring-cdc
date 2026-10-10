#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR="${TMPDIR:-/var/tmp}"; [[ "$TMPDIR" == /var/tmp ]]
if [[ ${M2_WIRE_LIMIT_PROOF:-0} != 1 && ${M2_ADVISORY_LOSS_PROOF:-0} != 1 && ${M2_COPYBOTH_LOSS_PROOF:-0} != 1 && ${M2_SOURCE_COMMIT_PROOF:-0} != 1 && ${M2_SQLITE_CONTENTION_PROOF:-0} != 1 && ${M2_SQLITE_PERSISTENCE_OUTAGE_PROOF:-0} != 1 ]]; then
  cargo test --locked --workspace --all-targets
fi
if [[ ${M2_SOURCE_COMMIT_PROOF:-0} != 1 && ${M2_SQLITE_CONTENTION_PROOF:-0} != 1 && ${M2_SQLITE_PERSISTENCE_OUTAGE_PROOF:-0} != 1 ]]; then
  cargo test --locked m2_capture_runtime::tests
fi
work=$(mktemp -d /var/tmp/m2-capture-e2e.XXXXXX); project="m2-capture-$RANDOM-$$"; port=$((56000 + $$ % 2000)); bootstrap_pid=; pid=; contention_pid=
record_feedback_abort() {
  local phase=$1 child=$2 rc=$3 observed_feedback=$4 elapsed_ms
  [[ -n ${M2_FEEDBACK_RECEIPT_DIR:-} ]] || return 0
  if [[ ${BORING_CDC_M2_FAULT_HOOK:-} != "$phase" || $rc -ne 134 ]]; then
    printf 'E_FEEDBACK_FAULT_NOT_REACHED phase=%s exit=%s\n' "$phase" "$rc" >&2
    grep -Eo 'E_[A-Z0-9_]+|M2_[A-Z0-9_]+' "$work/runtime.err" | tail -n 15 >&2 || true
    return 1
  fi
  elapsed_ms=$(( $(date +%s%3N) - runtime_started_ms ))
  mkdir -p "$M2_FEEDBACK_RECEIPT_DIR"
  cp -f "$work/runtime.out" "$M2_FEEDBACK_RECEIPT_DIR/runtime.stdout"
  cp -f "$work/runtime.err" "$M2_FEEDBACK_RECEIPT_DIR/runtime.stderr"
  python3 - "$M2_FEEDBACK_RECEIPT_DIR" "$phase" "$child" "$rc" "$version" "$durable_lsn" "$observed_feedback" "$elapsed_ms" <<'PY2'
import hashlib,json,pathlib,sys
out=pathlib.Path(sys.argv[1]); phase=sys.argv[2]
assert phase in ('before_feedback','after_feedback')
stdout=(out/'runtime.stdout').read_bytes(); stderr=(out/'runtime.stderr').read_bytes()
receipt={'schema_version':'m2-feedback-fault-receipt/v1','hook':phase,
         'child_command':['target/debug/boring-cdc','run'],'child_pid':int(sys.argv[3]),
         'child_exit_code':int(sys.argv[4]),'postgres_version':sys.argv[5],
         'durable_transaction_count':1,'durable_lsn':sys.argv[6],
         'server_feedback_positions_observed':sys.argv[7],
         'feedback_match_observed':phase=='after_feedback',
         'elapsed_since_runtime_start_ms':int(sys.argv[8]),
         'stdout_sha256':hashlib.sha256(stdout).hexdigest(),
         'stderr_sha256':hashlib.sha256(stderr).hexdigest()}
(out/'receipt.json').write_text(json.dumps(receipt,sort_keys=True,indent=2)+'\n')
PY2
}
cleanup_child() {
  local child=$1 rc state
  state=$(ps -o stat= -p "$child" 2>/dev/null || true)
  if [[ -n "$state" && "$state" != Z* ]]; then
    kill -KILL "$child" >/dev/null 2>&1 || true
  fi
  set +e
  wait "$child" 2>/dev/null
  rc=$?
  set -e
  [[ $rc -eq 134 ]] && echo "Aborted runtime child $child" >&2
  return 0
}
cleanup(){ [[ -z "$bootstrap_pid" ]] || cleanup_child "$bootstrap_pid"; [[ -z "$pid" ]] || cleanup_child "$pid"; [[ -z "$contention_pid" ]] || cleanup_child "$contention_pid"; docker compose -p "$project" -f compose.yaml -f "$work/override.yml" down -v --remove-orphans >/dev/null 2>&1 || true; rm -rf "$work"; }; trap cleanup EXIT INT TERM
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
printf 'm2-component-password-%s\n' "$project" >"$work/postgres_password"; chmod 600 "$work/postgres_password"; export BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password"; export PGPASSWORD; PGPASSWORD=$(cat "$BORING_CDC_POSTGRES_PASSWORD_FILE")
cat >"$work/override.yml" <<YAML
services:
  postgres:
    ports: ["127.0.0.1:${port}:5432"]
YAML
docker compose -p "$project" -f compose.yaml -f "$work/override.yml" up -d --wait postgres >/dev/null
psqlc(){ docker compose -p "$project" -f compose.yaml -f "$work/override.yml" exec -T postgres psql -v ON_ERROR_STOP=1 -U boring_cdc -d boring_cdc "$@"; }
version=$(psqlc -Atqc 'show server_version'); [[ "$version" == 17.6* ]]
admin_credential='admin-runtime-proof'; runtime_credential='runtime-runtime-proof'; control_credential='control-runtime-proof'; application_credential='application-runtime-proof'
psqlc -v "admin_password=$admin_credential" -v "runtime_password=$runtime_credential" -v "control_password=$control_credential" -v "application_password=$application_credential" \
  < scripts/setup/durable_simple_prerequisites.sql >/dev/null
if [[ ${M2_WIRE_LIMIT_PROOF:-0} == 1 ]]; then
  psqlc -c 'ALTER TABLE public.orders ADD COLUMN payload text' >/dev/null
fi
cargo build --quiet --locked --bin boring-cdc
mkdir -p "$work/run/state/spool" "$work/run/state/tmp" "$work/run/archive/root"; chmod 700 "$work/run/state" "$work/run/state/spool" "$work/run/archive" "$work/run/archive/root"; cp tests/fixtures/m1_config/representative.toml "$work/run/boring-cdc.toml"
sed -i 's/publication = "boring_publication"/publication = "RuntimePublication"/; s/slot = "boring_slot"/slot = "runtime_slot"/; s#sqlite_path = "state/boring.db"#sqlite_path = "state/journal.sqlite"#' "$work/run/boring-cdc.toml"
if [[ ${M2_SQLITE_CONTENTION_PROOF:-0} == 1 || ${M2_SQLITE_PERSISTENCE_OUTAGE_PROOF:-0} == 1 ]]; then
  sed -i 's/heartbeat_cadence_ms = 5000/heartbeat_cadence_ms = 300000/' "$work/run/boring-cdc.toml"
fi
binary="$PWD/target/debug/boring-cdc"
admin_dsn=postgresql:"//boring_cdc_admin:${admin_credential}@127.0.0.1:${port}/boring_cdc?sslmode=disable"
export PG_ADMIN="$admin_dsn" CH_MAINT='https://unused.invalid'; unset PG_RUNTIME PG_CONTROL CH_RUNTIME || true
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE -u PG_ADMIN -u CH_MAINT "$binary" init --dry-run --json) >"$work/init-dry.json"
token=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["data"]["confirm_token"])' "$work/init-dry.json")
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$binary" init --confirm --confirm-token "$token" --json) >"$work/init-confirm.json"
unset PG_ADMIN CH_MAINT
export PG_RUNTIME=postgresql:"//boring_cdc_runtime:${runtime_credential}@127.0.0.1:${port}/boring_cdc?sslmode=disable"
export PG_CONTROL=postgresql:"//boring_cdc_control_writer:${control_credential}@127.0.0.1:${port}/boring_cdc?sslmode=disable"
export CH_RUNTIME='https://unused.invalid'
(cd "$work/run"; exec env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$binary" run --bootstrap >"$work/bootstrap.out" 2>"$work/bootstrap.err") & bootstrap_pid=$!
deadline=$((SECONDS+30)); until [[ "$(psqlc -Atqc "SELECT count(*) FROM pg_replication_slots WHERE slot_name='runtime_slot' AND plugin='pgoutput'")" == 1 ]]; do
  (( SECONDS < deadline )) || { cat "$work/bootstrap.err" >&2; exit 1; }
  kill -0 "$bootstrap_pid" 2>/dev/null || { wait "$bootstrap_pid"; exit 1; }
  sleep .1
done
sleep 1; stop_bounded "$bootstrap_pid" INT bootstrap; bootstrap_pid=
(
  cd "$work/run"
  if [[ ${BORING_CDC_M2_FAULT_HOOK:-} == after_feedback ]]; then
    export M2_FEEDBACK_ABORT_RELEASE_FILE="$work/feedback-observed"
  fi
  exec env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" run >"$work/runtime.out" 2>"$work/runtime.err"
) & pid=$!
runtime_started_ms=$(date +%s%3N)
deadline=$((SECONDS+30)); until [[ "$(psqlc -Atqc "SELECT active::int FROM pg_replication_slots WHERE slot_name='runtime_slot'")" == 1 ]]; do (( SECONDS < deadline )) || { cat "$work/runtime.err" >&2; exit 1; }; sleep .1; done
if [[ ${M2_SOURCE_COMMIT_PROOF:-0} == 1 ]]; then
  ulimit -c 0
  [[ ${M2_SOURCE_COMMIT_HOOK:-} == before_source_commit || ${M2_SOURCE_COMMIT_HOOK:-} == after_source_commit_before_feedback ]] ||
    { echo 'E_SOURCE_COMMIT_HOOK_INVALID' >&2; exit 1; }
  [[ ${BORING_CDC_M2_FAULT_HOOK:-} == "$M2_SOURCE_COMMIT_HOOK" ]] ||
    { echo 'E_SOURCE_COMMIT_HOOK_MISMATCH' >&2; exit 1; }
  pre_fault_confirmed=$(psqlc -Atqc "SELECT coalesce(confirmed_flush_lsn::text,'0/0') FROM pg_replication_slots WHERE slot_name='runtime_slot'")
fi
python3 - "$work/run/state/journal.sqlite" "$work/sqlite-locked" <<'PY2' & lock_pid=$!
import pathlib,sqlite3,sys,time
c=sqlite3.connect(sys.argv[1]); c.execute('BEGIN IMMEDIATE'); pathlib.Path(sys.argv[2]).touch(); time.sleep(2); c.commit()
PY2
deadline=$((SECONDS+10)); until [[ -e "$work/sqlite-locked" ]]; do (( SECONDS < deadline )); sleep .02; done
psqlc -c "BEGIN; INSERT INTO orders(id) VALUES(1); UPDATE orders SET id=id WHERE id=1; COMMIT" >/dev/null
sleep .25
blocked_transactions=$(python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
print(sqlite3.connect(sys.argv[1]).execute('select count(*) from source_transactions').fetchone()[0])
PY2
)
blocked_feedback=$(psqlc -Atqc "SELECT coalesce(write_lsn::text,'0/0')||','||coalesce(flush_lsn::text,'0/0')||','||coalesce(replay_lsn::text,'0/0') FROM pg_stat_replication ORDER BY pid LIMIT 1")
[[ "$blocked_transactions" == 0 && "$blocked_feedback" == '0/0,0/0,0/0' ]]
wait "$lock_pid"
if [[ ${M2_SOURCE_COMMIT_PROOF:-0} == 1 ]]; then
  artifact=${M2_SOURCE_COMMIT_OUT:?set M2_SOURCE_COMMIT_OUT for the live source-commit proof}
  [[ ! -e "$artifact" ]] || { echo 'E_SOURCE_COMMIT_ARTIFACT_EXISTS' >&2; exit 1; }
  deadline=$((SECONDS+15))
  while true; do
    state=$(ps -o stat= -p "$pid" 2>/dev/null || true)
    [[ -z "$state" || "$state" == Z* ]] && break
    (( SECONDS < deadline )) || { cat "$work/runtime.err" >&2; exit 1; }
    sleep .05
  done
  child=$pid; set +e; wait "$pid"; rc=$?; set -e; pid=
  [[ $rc -eq 134 ]] || { echo "E_SOURCE_COMMIT_ABORT_EXIT $rc" >&2; exit 1; }
  fault_count=$(python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
print(sqlite3.connect(sys.argv[1]).execute('SELECT count(*) FROM source_transactions').fetchone()[0])
PY2
)
  expected_count=0
  [[ "$M2_SOURCE_COMMIT_HOOK" == after_source_commit_before_feedback ]] && expected_count=1
  [[ "$fault_count" == "$expected_count" ]] || { echo 'E_SOURCE_COMMIT_ATOMICITY' >&2; exit 1; }
  post_fault_confirmed=$(psqlc -Atqc "SELECT coalesce(confirmed_flush_lsn::text,'0/0') FROM pg_replication_slots WHERE slot_name='runtime_slot'")
  [[ "$post_fault_confirmed" == "$pre_fault_confirmed" ]] || { echo 'E_SOURCE_COMMIT_FEEDBACK_ADVANCED' >&2; exit 1; }
  [[ "$(psqlc -Atqc "SELECT active::int FROM pg_replication_slots WHERE slot_name='runtime_slot'")" == 0 ]]
  (cd "$work/run"; exec env -u BORING_CDC_M2_FAULT_HOOK -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" run >"$work/successor.out" 2>"$work/successor.err") & pid=$!
  deadline=$((SECONDS+30))
  while true; do
    count=$(python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
try: print(sqlite3.connect(sys.argv[1]).execute('SELECT count(*) FROM source_transactions').fetchone()[0])
except Exception: print(0)
PY2
)
    [[ "$count" == 1 ]] && break
    kill -0 "$pid" 2>/dev/null || { cat "$work/successor.err" >&2; exit 1; }
    (( SECONDS < deadline )) || { cat "$work/successor.err" >&2; exit 1; }
    sleep .1
  done
  successor_hex=$(python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
print(sqlite3.connect(sys.argv[1]).execute('SELECT durable_transaction_end_lsn FROM source_state WHERE singleton=1').fetchone()[0])
PY2
)
  successor_lsn=$(python3 - "$successor_hex" <<'PY2'
import sys
v=int(sys.argv[1],16); print(f'{v>>32:X}/{v&0xffffffff:X}')
PY2
)
  deadline=$((SECONDS+15))
  while true; do
    successor_feedback=$(psqlc -Atqc "SELECT coalesce(write_lsn::text,'')||','||coalesce(flush_lsn::text,'')||','||coalesce(replay_lsn::text,'') FROM pg_stat_replication ORDER BY pid LIMIT 1")
    [[ "$successor_feedback" == "$successor_lsn,$successor_lsn,$successor_lsn" ]] && break
    kill -0 "$pid" 2>/dev/null || { cat "$work/successor.err" >&2; exit 1; }
    (( SECONDS < deadline )) || { cat "$work/successor.err" >&2; exit 1; }
    sleep .05
  done
  stop_bounded "$pid" TERM source-commit-successor; pid=
  mkdir -p "$artifact"
  python3 - "$artifact" "$version" "$M2_SOURCE_COMMIT_HOOK" "$rc" "$fault_count" "$pre_fault_confirmed" "$post_fault_confirmed" "$successor_lsn" "$successor_feedback" <<'PY2'
import json,pathlib,sys
out=pathlib.Path(sys.argv[1])
(out/'observation.json').write_text(json.dumps({
 'schema_version':'m2-source-commit-observation/v1','postgres_version':sys.argv[2],
 'fault_hook':sys.argv[3],'fault_exit_code':int(sys.argv[4]),
 'durable_transaction_count_after_fault':int(sys.argv[5]),
 'slot_confirmed_before_fault':sys.argv[6],'slot_confirmed_after_fault':sys.argv[7],
 'durable_transaction_count_after_successor':1,'successor_durable_lsn':sys.argv[8],
 'successor_feedback':sys.argv[9],'successor_clean_shutdown':True,
 },sort_keys=True,indent=2)+'\n')
PY2
  printf 'M2_SOURCE_COMMIT_CRASH_OK hook=%s postgres=%s\n' "$M2_SOURCE_COMMIT_HOOK" "$version"
  exit 0
fi
deadline=$((SECONDS+30)); until [[ "$(python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
try: print(sqlite3.connect(sys.argv[1]).execute('select count(*) from source_transactions').fetchone()[0])
except Exception: print(0)
PY2
)" == 1 ]]; do (( SECONDS < deadline )) || { cat "$work/runtime.err" >&2; exit 1; }; sleep .1; done
durable_hex=$(python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
print(sqlite3.connect(sys.argv[1]).execute('select durable_transaction_end_lsn from source_state where singleton=1').fetchone()[0])
PY2
)
durable_lsn=$(python3 - "$durable_hex" <<'PY2'
import sys
v=int(sys.argv[1],16); print(f'{v>>32:X}/{v&0xffffffff:X}')
PY2
)
deadline=$((SECONDS+10))
while true; do
  feedback=$(psqlc -Atqc "SELECT coalesce(write_lsn::text,'')||','||coalesce(flush_lsn::text,'')||','||coalesce(replay_lsn::text,'') FROM pg_stat_replication ORDER BY pid LIMIT 1")
  [[ "$feedback" == "$durable_lsn,$durable_lsn,$durable_lsn" ]] && break
  state=$(ps -o stat= -p "$pid" 2>/dev/null || true)
  if [[ -z "$state" || "$state" == Z* ]]; then
    child=$pid; set +e; wait "$pid" 2>/dev/null; rc=$?; set -e; pid=
    record_feedback_abort before_feedback "$child" "$rc" "$feedback"
    [[ $rc -eq 134 ]] && echo "Aborted runtime before feedback" >&2
    cat "$work/runtime.err" >&2
    exit 1
  fi
  (( SECONDS < deadline )) || { cat "$work/runtime.err" >&2; exit 1; }
  sleep .05
done
if [[ ${BORING_CDC_M2_FAULT_HOOK:-} == after_feedback ]]; then
  touch "$work/feedback-observed"
fi
sleep .1
if [[ ${BORING_CDC_M2_FAULT_HOOK:-} == after_feedback && -n ${M2_FEEDBACK_RECEIPT_DIR:-} ]]; then
  deadline=$((SECONDS+5))
  while kill -0 "$pid" 2>/dev/null && [[ "$(ps -o stat= -p "$pid" 2>/dev/null || true)" != Z* ]]; do
    (( SECONDS < deadline )) || { echo 'E_AFTER_FEEDBACK_ABORT_NOT_REACHED' >&2; exit 1; }
    sleep .05
  done
fi
if [[ ${M2_SQLITE_CONTENTION_PROOF:-0} == 1 || ${M2_SQLITE_PERSISTENCE_OUTAGE_PROOF:-0} == 1 ]]; then
  printf 'M2_SQLITE_CONTENTION_PHASE=baseline_durable\n'
  if [[ ${M2_SQLITE_PERSISTENCE_OUTAGE_PROOF:-0} == 1 ]]; then
    artifact=${M2_SQLITE_PERSISTENCE_OUTAGE_OUT:?set M2_SQLITE_PERSISTENCE_OUTAGE_OUT for the live persistence outage proof}
    lock_seconds=25
  else
    artifact=${M2_SQLITE_CONTENTION_OUT:?set M2_SQLITE_CONTENTION_OUT for the live SQLite contention proof}
    lock_seconds=7
  fi
  [[ ! -e "$artifact" ]] || { echo 'E_SQLITE_CONTENTION_ARTIFACT_EXISTS' >&2; exit 1; }
  replication_pid=$(psqlc -Atqc 'SELECT pid FROM pg_stat_replication ORDER BY pid')
  [[ "$replication_pid" =~ ^[0-9]+$ ]] || { echo 'E_SQLITE_CONTENTION_REPLICATION_PID' >&2; exit 1; }
  pre_confirmed=$(psqlc -Atqc "SELECT coalesce(confirmed_flush_lsn::text,'0/0') FROM pg_replication_slots WHERE slot_name='runtime_slot'")
  python3 - "$work/run/state/journal.sqlite" "$work/sqlite-contention-locked" "$lock_seconds" <<'PY2' & contention_pid=$!
import pathlib,sqlite3,sys,time
connection=sqlite3.connect(sys.argv[1]); connection.execute('BEGIN IMMEDIATE')
pathlib.Path(sys.argv[2]).touch()
time.sleep(int(sys.argv[3]))
connection.commit()
PY2
  deadline=$((SECONDS+10)); until [[ -e "$work/sqlite-contention-locked" ]]; do (( SECONDS < deadline )) || { echo 'E_SQLITE_CONTENTION_LOCK_TIMEOUT' >&2; exit 1; }; sleep .02; done
  printf 'M2_SQLITE_CONTENTION_PHASE=writer_locked\n'
  psqlc -c 'INSERT INTO orders(id) VALUES(2)' >/dev/null
  printf 'M2_SQLITE_CONTENTION_PHASE=second_source_committed\n'
  blocked_count=$(python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
print(sqlite3.connect(sys.argv[1]).execute('SELECT count(*) FROM source_transactions').fetchone()[0])
PY2
)
  blocked_confirmed=$(psqlc -Atqc "SELECT coalesce(confirmed_flush_lsn::text,'0/0') FROM pg_replication_slots WHERE slot_name='runtime_slot'")
  [[ "$blocked_count" == 1 ]] || { echo 'E_SQLITE_CONTENTION_DURABILITY' >&2; exit 1; }
  python3 - "$pre_confirmed" "$blocked_confirmed" "$durable_lsn" <<'PY2'
import sys
def lsn(value):
    high,low=value.split('/')
    return (int(high,16)<<32)|int(low,16)
assert lsn(sys.argv[1])<=lsn(sys.argv[2])<=lsn(sys.argv[3])
PY2
  if [[ ${M2_SQLITE_PERSISTENCE_OUTAGE_PROOF:-0} != 1 ]]; then
    wait "$contention_pid"; contention_pid=
    printf 'M2_SQLITE_CONTENTION_PHASE=writer_released\n'
  fi
  deadline=$((SECONDS+15))
  while true; do
    state=$(ps -o stat= -p "$pid" 2>/dev/null || true)
    [[ -z "$state" || "$state" == Z* ]] && break
    (( SECONDS < deadline )) || { cat "$work/runtime.err" >&2; exit 1; }
    sleep .05
  done
  child=$pid; set +e; wait "$pid"; rc=$?; set -e; pid=
  printf 'M2_SQLITE_CONTENTION_PHASE=original_exited\n'
  [[ $rc -ne 0 ]] || { echo 'E_SQLITE_CONTENTION_EXIT' >&2; exit 1; }
  if [[ ${M2_SQLITE_PERSISTENCE_OUTAGE_PROOF:-0} == 1 ]]; then
    grep -q 'M2_FAILURE_PERSIST_FAILED' "$work/runtime.err" || { echo 'E_SQLITE_OUTAGE_FAILURE_CODE' >&2; exit 1; }
    python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
connection=sqlite3.connect(sys.argv[1],timeout=0.1)
try:
    connection.execute('BEGIN IMMEDIATE')
except sqlite3.OperationalError as error:
    assert error.sqlite_errorcode == sqlite3.SQLITE_BUSY
else:
    connection.rollback()
    raise AssertionError('E_SQLITE_OUTAGE_LOCK_LOST')
PY2
    [[ "$(psqlc -Atqc "SELECT active::int FROM pg_replication_slots WHERE slot_name='runtime_slot'")" == 0 ]] || { echo 'E_SQLITE_OUTAGE_SLOT_ACTIVE' >&2; exit 1; }
    [[ "$(psqlc -Atqc 'SELECT count(*) FROM pg_stat_replication')" == 0 ]] || { echo 'E_SQLITE_OUTAGE_REPLICATION_ACTIVE' >&2; exit 1; }
    outage_failure_count=$(python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
connection=sqlite3.connect(sys.argv[1])
print(connection.execute("SELECT count(*) FROM processing_failures WHERE component='capture' AND armed=1").fetchone()[0])
PY2
)
    [[ "$outage_failure_count" == 0 ]] || { echo 'E_SQLITE_OUTAGE_FALSE_FAILURE_RECORD' >&2; exit 1; }
    outage_count=$(python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
print(sqlite3.connect(sys.argv[1]).execute('SELECT count(*) FROM source_transactions').fetchone()[0])
PY2
)
    [[ "$outage_count" == 1 ]] || { echo 'E_SQLITE_OUTAGE_DURABILITY' >&2; exit 1; }
    outage_confirmed=$(psqlc -Atqc "SELECT coalesce(confirmed_flush_lsn::text,'0/0') FROM pg_replication_slots WHERE slot_name='runtime_slot'")
    [[ "$outage_confirmed" == "$blocked_confirmed" ]] || { echo 'E_SQLITE_OUTAGE_FEEDBACK_ADVANCED' >&2; exit 1; }
    printf 'M2_SQLITE_CONTENTION_PHASE=outage_fenced_before_release\n'
    wait "$contention_pid"; contention_pid=
    printf 'M2_SQLITE_CONTENTION_PHASE=writer_released\n'
  fi
  if [[ ${M2_SQLITE_PERSISTENCE_OUTAGE_PROOF:-0} != 1 ]]; then
    grep -q 'M2_CAPTURE_FAILED' "$work/runtime.err" || { cat "$work/runtime.err" >&2; exit 1; }
    [[ "$(psqlc -Atqc "SELECT active::int FROM pg_replication_slots WHERE slot_name='runtime_slot'")" == 0 ]]
    [[ "$(psqlc -Atqc 'SELECT count(*) FROM pg_stat_replication')" == 0 ]]
    failure=$(python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
row=sqlite3.connect(sys.argv[1]).execute("SELECT failure_class,retry_class,attempt,next_retry_at FROM processing_failures WHERE component='capture' AND armed=1").fetchone()
assert row is not None and row[0:3]==('transient_io','transient',1) and row[3].startswith('unix-ms:')
print(','.join((row[0],row[1],str(row[2]),row[3])))
PY2
)
  else
    failure=''
  fi
  post_confirmed=$(psqlc -Atqc "SELECT coalesce(confirmed_flush_lsn::text,'0/0') FROM pg_replication_slots WHERE slot_name='runtime_slot'")
  python3 - "$blocked_confirmed" "$post_confirmed" "$durable_lsn" <<'PY2'
import sys
def lsn(value):
    high,low=value.split('/')
    return (int(high,16)<<32)|int(low,16)
assert lsn(sys.argv[1])<=lsn(sys.argv[2])<=lsn(sys.argv[3])
PY2
  [[ "$(python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
print(sqlite3.connect(sys.argv[1]).execute('SELECT count(*) FROM source_transactions').fetchone()[0])
PY2
)" == 1 ]]
  if [[ ${M2_SQLITE_PERSISTENCE_OUTAGE_PROOF:-0} != 1 ]]; then
    printf 'M2_SQLITE_CONTENTION_PHASE=retry_persisted\n'
    retry_at=${failure##*,unix-ms:}
    wait_ms=$(python3 - "$retry_at" <<'PY2'
import sys,time
print(max(0,int(sys.argv[1])-time.time_ns()//1000000+100))
PY2
)
    sleep "$(python3 - "$wait_ms" <<'PY2'
import sys
print(int(sys.argv[1])/1000)
PY2
)"
  fi
  (cd "$work/run"; exec env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" run >"$work/successor.out" 2>"$work/successor.err") & pid=$!
  printf 'M2_SQLITE_CONTENTION_PHASE=successor_started\n'
  deadline=$((SECONDS+30))
  while true; do
    successor_count=$(python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
print(sqlite3.connect(sys.argv[1]).execute('SELECT count(*) FROM source_transactions').fetchone()[0])
PY2
)
    [[ "$successor_count" == 2 ]] && break
    kill -0 "$pid" 2>/dev/null || { echo "E_SQLITE_CONTENTION_SUCCESSOR_EXIT count=$successor_count" >&2; grep -Eo 'E_[A-Z0-9_]+|M2_[A-Z0-9_]+' "$work/successor.out" "$work/successor.err" >&2 || true; exit 1; }
    (( SECONDS < deadline )) || { echo "E_SQLITE_CONTENTION_SUCCESSOR_TX_TIMEOUT count=$successor_count" >&2; grep -Eo 'E_[A-Z0-9_]+|M2_[A-Z0-9_]+' "$work/successor.out" "$work/successor.err" >&2 || true; exit 1; }
    sleep .1
  done
  printf 'M2_SQLITE_CONTENTION_PHASE=successor_two_transactions\n'
  successor_hex=$(python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
print(sqlite3.connect(sys.argv[1]).execute('SELECT durable_transaction_end_lsn FROM source_state WHERE singleton=1').fetchone()[0])
PY2
)
  successor_lsn=$(python3 - "$successor_hex" <<'PY2'
import sys
value=int(sys.argv[1],16); print(f'{value>>32:X}/{value&0xffffffff:X}')
PY2
)
  deadline=$((SECONDS+15))
  while true; do
    successor_feedback=$(psqlc -Atqc "SELECT coalesce(write_lsn::text,'')||','||coalesce(flush_lsn::text,'')||','||coalesce(replay_lsn::text,'') FROM pg_stat_replication ORDER BY pid LIMIT 1")
    [[ "$successor_feedback" == "$successor_lsn,$successor_lsn,$successor_lsn" ]] && break
    (( SECONDS < deadline )) || { echo "E_SQLITE_CONTENTION_SUCCESSOR_FEEDBACK_TIMEOUT durable=$successor_lsn feedback=$successor_feedback" >&2; grep -Eo 'E_[A-Z0-9_]+|M2_[A-Z0-9_]+' "$work/successor.out" "$work/successor.err" >&2 || true; exit 1; }
    sleep .05
  done
  printf 'M2_SQLITE_CONTENTION_PHASE=successor_feedback_equal\n'
  stop_bounded "$pid" TERM sqlite-contention-successor; pid=
  printf 'M2_SQLITE_CONTENTION_PHASE=successor_converged\n'
  mkdir -p "$artifact"
  if [[ ${M2_SQLITE_PERSISTENCE_OUTAGE_PROOF:-0} == 1 ]]; then
    python3 - "$artifact" "$version" "$durable_lsn" "$feedback" "$replication_pid" "$rc" "$pre_confirmed" "$blocked_confirmed" "$outage_confirmed" "$successor_lsn" "$successor_feedback" <<'PY2'
import json,pathlib,sys
out=pathlib.Path(sys.argv[1])
(out/'observation.json').write_text(json.dumps({
 'schema_version':'m2-sqlite-persistence-outage-observation/v1','postgres_version':sys.argv[2],
 'durable_lsn_before_outage':sys.argv[3],'feedback_before_outage':sys.argv[4],
 'original_replication_pid':int(sys.argv[5]),'runtime_exit_code':int(sys.argv[6]),
 'runtime_failure_code':'M2_FAILURE_PERSIST_FAILED','armed_failure_count_before_release':0,
 'slot_confirmed_before_outage':sys.argv[7],
 'slot_confirmed_while_locked':sys.argv[8],'slot_confirmed_after_exit_before_release':sys.argv[9],
 'durable_transaction_count_before_release':1,'replication_slot_active_after_exit':False,
 'successor_durable_transaction_count':2,'successor_durable_lsn':sys.argv[10],
 'successor_feedback':sys.argv[11],'successor_clean_shutdown':True,
 },sort_keys=True,indent=2)+'\n')
PY2
    printf 'M2_SQLITE_PERSISTENCE_OUTAGE_REPLAY_OK postgres=%s\n' "$version"
    exit 0
  fi
  python3 - "$artifact" "$version" "$durable_lsn" "$feedback" "$replication_pid" "$rc" "$failure" "$pre_confirmed" "$blocked_confirmed" "$post_confirmed" "$successor_lsn" "$successor_feedback" <<'PY2'
import json,pathlib,sys
out=pathlib.Path(sys.argv[1])
(out/'observation.json').write_text(json.dumps({
 'schema_version':'m2-sqlite-contention-observation/v1','postgres_version':sys.argv[2],
 'durable_lsn_before_contention':sys.argv[3],'feedback_before_contention':sys.argv[4],
 'original_replication_pid':int(sys.argv[5]),'runtime_exit_code':int(sys.argv[6]),
 'persisted_failure':sys.argv[7],'slot_confirmed_before_contention':sys.argv[8],
 'slot_confirmed_while_locked':sys.argv[9],'slot_confirmed_after_failure':sys.argv[10],
 'durable_transaction_count_after_failure':1,'replication_slot_active_after_failure':False,
 'successor_durable_transaction_count':2,'successor_durable_lsn':sys.argv[11],
 'successor_feedback':sys.argv[12],'successor_clean_shutdown':True,
 },sort_keys=True,indent=2)+'\n')
PY2
  printf 'M2_SQLITE_CONTENTION_RETRY_OK postgres=%s\n' "$version"
  exit 0
fi
if [[ ${M2_COPYBOTH_LOSS_PROOF:-0} == 1 ]]; then
  artifact=${M2_COPYBOTH_LOSS_OUT:?set M2_COPYBOTH_LOSS_OUT for the live CopyBoth-loss proof}
  [[ ! -e "$artifact" ]] || { echo 'E_COPYBOTH_ARTIFACT_EXISTS' >&2; exit 1; }
  owner_pid=$(psqlc -Atqc "SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted AND pid <> pg_backend_pid() ORDER BY pid")
  replication_pid=$(psqlc -Atqc "SELECT pid FROM pg_stat_replication ORDER BY pid")
  [[ "$owner_pid" =~ ^[0-9]+$ && "$replication_pid" =~ ^[0-9]+$ && "$owner_pid" != "$replication_pid" ]] ||
    { echo 'E_COPYBOTH_BACKEND_AMBIGUOUS' >&2; exit 1; }
  [[ "$(psqlc -Atqc "SELECT pg_terminate_backend($replication_pid)::int")" == 1 ]]
  deadline=$((SECONDS+15))
  reopened=0
  while true; do
    state=$(ps -o stat= -p "$pid" 2>/dev/null || true)
    [[ -z "$state" || "$state" == Z* ]] && break
    current_replication=$(psqlc -Atqc "SELECT pid FROM pg_stat_replication ORDER BY pid")
    [[ -z "$current_replication" || "$current_replication" == "$replication_pid" ]] || reopened=1
    (( SECONDS < deadline )) || { cat "$work/runtime.err" >&2; exit 1; }
    sleep .05
  done
  child=$pid; set +e; wait "$pid"; rc=$?; set -e; pid=
  [[ $rc -ne 0 && $reopened -eq 0 ]]
  grep -q 'M2_COPYBOTH_UNEXPECTED_LOSS' "$work/runtime.err"
  [[ "$(psqlc -Atqc "SELECT active::int FROM pg_replication_slots WHERE slot_name='runtime_slot'")" == 0 ]]
  [[ "$(python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
print(sqlite3.connect(sys.argv[1]).execute('SELECT count(*) FROM source_transactions').fetchone()[0])
PY2
)" == 1 ]]
  failure=$(python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
row=sqlite3.connect(sys.argv[1]).execute("SELECT failure_class FROM processing_failures WHERE component='capture' AND armed=1").fetchone()
print(row[0] if row else '')
PY2
)
  [[ "$failure" == transient_source ]]
  confirmed=$(psqlc -Atqc "SELECT coalesce(confirmed_flush_lsn::text,'0/0') FROM pg_replication_slots WHERE slot_name='runtime_slot'")
  python3 - "$confirmed" "$durable_lsn" <<'PY2'
import sys
def lsn(s):
    high,low=s.split('/')
    return (int(high,16)<<32)|int(low,16)
assert lsn(sys.argv[1])<=lsn(sys.argv[2])
PY2
  mkdir -p "$artifact"
  python3 - "$artifact" "$version" "$durable_lsn" "$feedback" "$owner_pid" "$replication_pid" "$rc" "$confirmed" <<'PY2'
import json,pathlib,sys
out=pathlib.Path(sys.argv[1])
(out/'observation.json').write_text(json.dumps({
 'schema_version':'m2-copyboth-loss-observation/v1','postgres_version':sys.argv[2],
 'durable_lsn_before_loss':sys.argv[3],'feedback_before_loss':sys.argv[4],
 'source_advisory_pid_before_loss':int(sys.argv[5]),
 'terminated_replication_pid':int(sys.argv[6]),'runtime_exit_code':int(sys.argv[7]),
 'slot_confirmed_flush_after_loss':sys.argv[8],
 'failure_class_after_loss':'transient_source','durable_transaction_count_after_loss':1,
 'replication_slot_active_after_loss':False,'replication_reopened':False,
 'runtime_exited_without_operator_signal':True,
 },sort_keys=True,indent=2)+'\n')
PY2
  printf 'M2_COPYBOTH_LOSS_FENCED_OK postgres=%s\n' "$version"
  exit 0
fi
if [[ ${M2_ADVISORY_LOSS_PROOF:-0} == 1 ]]; then
  artifact=${M2_ADVISORY_LOSS_OUT:?set M2_ADVISORY_LOSS_OUT for the live advisory-loss proof}
  [[ ! -e "$artifact" ]] || { echo 'E_ADVISORY_ARTIFACT_EXISTS' >&2; exit 1; }
  owner_pid=$(psqlc -Atqc "SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted AND pid <> pg_backend_pid() ORDER BY pid")
  [[ "$owner_pid" =~ ^[0-9]+$ ]] || { echo 'E_ADVISORY_OWNER_AMBIGUOUS' >&2; exit 1; }
  [[ "$(psqlc -Atqc "SELECT pg_terminate_backend($owner_pid)::int")" == 1 ]]
  deadline=$((SECONDS+15))
  reacquired=0
  while true; do
    state=$(ps -o stat= -p "$pid" 2>/dev/null || true)
    [[ -z "$state" || "$state" == Z* ]] && break
    [[ "$(psqlc -Atqc "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND granted")" == 0 ]] || reacquired=1
    (( SECONDS < deadline )) || { cat "$work/runtime.err" >&2; exit 1; }
    sleep .05
  done
  child=$pid; set +e; wait "$pid"; rc=$?; set -e; pid=
  [[ $rc -ne 0 && $reacquired -eq 0 ]]
  grep -q 'M2_OWNERSHIP_LOST' "$work/runtime.err"
  [[ "$(psqlc -Atqc "SELECT active::int FROM pg_replication_slots WHERE slot_name='runtime_slot'")" == 0 ]]
  [[ "$(psqlc -Atqc "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND granted")" == 0 ]]
  [[ "$(python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
print(sqlite3.connect(sys.argv[1]).execute('SELECT count(*) FROM source_transactions').fetchone()[0])
PY2
)" == 1 ]]
  confirmed=$(psqlc -Atqc "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name='runtime_slot'")
  python3 - "$confirmed" "$durable_lsn" <<'PY2'
import sys
def lsn(s):
    high,low=s.split('/')
    return (int(high,16)<<32)|int(low,16)
assert lsn(sys.argv[1])<=lsn(sys.argv[2])
PY2
  mkdir -p "$artifact"
  python3 - "$artifact" "$version" "$durable_lsn" "$feedback" "$owner_pid" "$rc" "$confirmed" <<'PY2'
import json,pathlib,sys
out=pathlib.Path(sys.argv[1])
(out/'observation.json').write_text(json.dumps({
 'schema_version':'m2-advisory-loss-observation/v1','postgres_version':sys.argv[2],
 'durable_lsn_before_loss':sys.argv[3],'feedback_before_loss':sys.argv[4],
 'terminated_advisory_pid':int(sys.argv[5]),'runtime_exit_code':int(sys.argv[6]),
 'slot_confirmed_flush_after_loss':sys.argv[7],
 'durable_transaction_count_after_loss':1,'replication_slot_active_after_loss':False,
 'advisory_lock_reacquired':False,'runtime_exited_without_operator_signal':True,
 },sort_keys=True,indent=2)+'\n')
PY2
  printf 'M2_ADVISORY_LOSS_FENCED_OK postgres=%s\n' "$version"
  exit 0
fi
if [[ ${M2_WIRE_LIMIT_PROOF:-0} == 1 ]]; then
  artifact=${M2_WIRE_LIMIT_OUT:?set M2_WIRE_LIMIT_OUT for the live wire-limit proof}
  [[ ! -e "$artifact" ]] || { echo 'E_WIRE_LIMIT_ARTIFACT_EXISTS' >&2; exit 1; }
  psqlc -c "INSERT INTO orders(id,payload) SELECT 2,string_agg(md5(g::text),'') FROM generate_series(1,65536) AS series(g)" >/dev/null
  deadline=$((SECONDS+30))
  while true; do
    failure=$(python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
row=sqlite3.connect(sys.argv[1]).execute("SELECT failure_class||','||retry_class FROM processing_failures WHERE component='capture' AND armed=1").fetchone()
print(row[0] if row else '')
PY2
)
    [[ "$failure" == 'configuration,deterministic' ]] && break
    kill -0 "$pid" 2>/dev/null || { cat "$work/runtime.err" >&2; exit 1; }
    (( SECONDS < deadline )) || { cat "$work/runtime.err" >&2; exit 1; }
    sleep .1
  done
  [[ "$(psqlc -Atqc "SELECT active::int FROM pg_replication_slots WHERE slot_name='runtime_slot'")" == 0 ]]
  [[ "$(python3 - "$work/run/state/journal.sqlite" <<'PY2'
import sqlite3,sys
print(sqlite3.connect(sys.argv[1]).execute('SELECT count(*) FROM source_transactions').fetchone()[0])
PY2
)" == 1 ]]
  [[ "$(psqlc -Atqc "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND granted")" -gt 0 ]]
  grep -q 'M2_CAPTURE_RESOURCE_LIMIT kind=wire_frame' "$work/runtime.err"
  kill -0 "$pid"
  stop_bounded "$pid" TERM wire-limit-safe-stop; pid=
  mkdir -p "$artifact"
  python3 - "$artifact" "$version" "$durable_lsn" "$feedback" <<'PY2'
import json,pathlib,sys
out=pathlib.Path(sys.argv[1]); (out/'observation.json').write_text(json.dumps({
 'schema_version':'m2-wire-limit-observation/v1','postgres_version':sys.argv[2],
 'durable_lsn_before_limit':sys.argv[3],'feedback_before_limit':sys.argv[4],
 'armed_failure':'configuration,deterministic','durable_transaction_count_after_limit':1,
 'replication_slot_active_after_limit':False,'advisory_lock_held_after_limit':True,
 'process_alive_after_limit':True,'clean_shutdown':True},sort_keys=True,indent=2)+'\n')
PY2
  printf 'M2_WIRE_LIMIT_SAFE_STOP_OK postgres=%s\n' "$version"
  exit 0
fi
state=$(ps -o stat= -p "$pid" 2>/dev/null || true)
if [[ -z "$state" || "$state" == Z* ]]; then
  child=$pid; set +e; wait "$pid" 2>/dev/null; rc=$?; set -e; pid=
  record_feedback_abort after_feedback "$child" "$rc" "$feedback"
  [[ $rc -eq 134 ]] && echo "Aborted runtime after feedback" >&2
  cat "$work/runtime.err" >&2
  exit 1
fi
stop_bounded "$pid" TERM runtime; pid=; [[ ! -s "$work/runtime.err" ]]
python3 - "$work/run/state/journal.sqlite" "$feedback" <<'PY2'
import sqlite3,sys
c=sqlite3.connect(sys.argv[1]); tx=c.execute('select count(*),max(end_lsn) from source_transactions').fetchone(); ev=c.execute('select count(*) from journal_events').fetchone()[0]
assert tx[0]==1 and ev==2 and tx[1] is not None
print('{"journal_transactions":1,"journal_events":2,"feedback_bounded":true,"server_feedback_positions":"%s"}'%sys.argv[2])
PY2
printf '{"command":"CMD-RUN","exit":0,"postgres":"%s","journal_transactions":1,"journal_events":2,"durable_before_feedback":true,"blocked_sqlite_transactions":0,"blocked_server_feedback_positions":"0/0,0/0,0/0","durable_sqlite_lsn":"%s","server_feedback_positions":"%s"}\n' "$version" "$durable_lsn" "$feedback" >"$work/observation.json"
export M2_RUNTIME_OUTPUT="$work/runtime.out" M2_RUNTIME_OBSERVATION="$work/observation.json" M2_POSTGRES_VERSION="$version"
python3 scripts/lib/m2_capture_runtime_evidence.py e2e
scripts/validate/evidence.sh artifacts/boring-cdc-m2-capture-runtime/SCN-M2-CAPTURE-RUNTIME-E2E/capture-runtime-production-v1/evidence.json
echo "M2_CAPTURE_RUNTIME_E2E_OK postgres=$version durable_before_feedback=true"
