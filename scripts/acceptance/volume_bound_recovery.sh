#!/usr/bin/env bash
# PostgreSQL 17.6 source-to-journal spool-limit recovery acceptance.
set -euo pipefail
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp

started=$SECONDS
work=$(mktemp -d /var/tmp/volume-bound-probe.XXXXXX)
project="volume-bound-probe-$RANDOM-$$"
port=$((54000 + $$ % 1000))
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

printf 'volume-bound-probe-postgres-%s\n' "$project" >"$work/postgres_password"
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

admin_credential='admin-durable-simple'
runtime_credential='runtime-durable-simple'
control_credential='control-durable-simple'
application_credential='application-durable-simple'
psqlc -v "admin_password=$admin_credential" -v "runtime_password=$runtime_credential" -v "control_password=$control_credential" -v "application_password=$application_credential" \
  < scripts/setup/durable_simple_prerequisites.sql >"$work/prerequisites.out"

cargo build --quiet --locked --bin boring-cdc
binary="$PWD/target/debug/boring-cdc"
mkdir -p "$work/run/state/spool" "$work/run/state/tmp" "$work/run/archive/root"
chmod 700 "$work/run/state" "$work/run/state/spool" "$work/run/archive" "$work/run/archive/root"
cp tests/fixtures/m1_config/representative.toml "$work/run/boring-cdc.toml"
python3 - "$work/run/boring-cdc.toml" <<'PYC'
from pathlib import Path
import sys
p=Path(sys.argv[1]); s=p.read_text().replace('reserved_free_bytes = 100000000','reserved_free_bytes = 1000000').replace('capture_spool_bytes = 200000000','capture_spool_bytes = 2000000'); p.write_text(s)
PYC

admin_dsn=postgresql:"//boring_cdc_admin:${admin_credential}@127.0.0.1:${port}/boring_cdc?sslmode=disable"
export PG_ADMIN="$admin_dsn" CH_MAINT='https://unused.invalid'
unset PG_RUNTIME PG_CONTROL CH_RUNTIME || true
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE -u PG_ADMIN -u CH_MAINT "$binary" init --dry-run --json) >"$work/init-dry-run.json"
token=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["data"]["confirm_token"])' "$work/init-dry-run.json")
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$binary" init --confirm --confirm-token "$token" --json) >"$work/init-confirm.json"
unset PG_ADMIN CH_MAINT
export PG_RUNTIME=postgresql:"//boring_cdc_runtime:${runtime_credential}@127.0.0.1:${port}/boring_cdc?sslmode=disable"
export PG_CONTROL=postgresql:"//boring_cdc_control_writer:${control_credential}@127.0.0.1:${port}/boring_cdc?sslmode=disable"
export CH_RUNTIME='https://unused.invalid'

(cd "$work/run"; exec env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$binary" run --bootstrap >"$work/bootstrap.out" 2>"$work/bootstrap.err") &
bootstrap_pid=$!
deadline=$((SECONDS+30))
until [[ "$(psqlc -Atqc "select count(*) from pg_replication_slots where slot_name='boring_slot' and plugin='pgoutput'")" == 1 ]]; do
  (( SECONDS < deadline )) || { cat "$work/bootstrap.err" >&2; exit 1; }
  kill -0 "$bootstrap_pid" 2>/dev/null || { wait "$bootstrap_pid"; exit 1; }
  sleep .1
done
sleep 1
stop_bounded "$bootstrap_pid" INT bootstrap
bootstrap_pid=

journal="$work/run/state/boring.db"
journal_transactions() {
  python3 - "$journal" <<'PY'
import sqlite3,sys
with sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True) as db:
    print(db.execute("select count(*) from source_transactions where state='committed'").fetchone()[0])
PY
}
wait_for_transactions() {
  local expected=$1
  deadline=$((SECONDS+90))
  until [[ "$(journal_transactions)" == "$expected" ]]; do
    (( SECONDS < deadline )) || { echo "E_JOURNAL_CATCH_UP expected=$expected observed=$(journal_transactions)" >&2; cat "$work/runtime-restart.err" "$work/runtime-first.err" 2>/dev/null >&2 || true; exit 1; }
    sleep .1
  done
}
wait_for_key() {
  local key=$1 deadline=$((SECONDS+180))
  until python3 - "$journal" "$key" <<'PY'
import json,sqlite3,sys
with sqlite3.connect(f"file:{sys.argv[1]}?mode=ro",uri=True) as db:
    events=db.execute("select cast(payload as text) from journal_events where control_kind is null").fetchall()
for (text,) in events:
    payload=json.loads(text)
    if payload.get('kind')=='insert' and payload.get('new'):
        if int(bytes(payload['new'][0]['bytes']).decode())==int(sys.argv[2]):
            raise SystemExit(0)
raise SystemExit(1)
PY
  do
    (( SECONDS < deadline )) || { echo "E_JOURNAL_KEY_TIMEOUT key=$key" >&2; cat "$work/runtime-restart.err" 2>/dev/null >&2 || true; exit 1; }
    kill -0 "$runtime_pid" 2>/dev/null || { cat "$work/runtime-restart.err" >&2; exit 1; }
    sleep .2
  done
}
wait_for_active_slot() {
  deadline=$((SECONDS+30))
  until [[ "$(psqlc -Atqc "select active::int from pg_replication_slots where slot_name='boring_slot'")" == 1 ]]; do
    (( SECONDS < deadline )) || return 1
    kill -0 "$runtime_pid" 2>/dev/null || return 1
    sleep .1
  done
}
sample_feedback_boundary() {
  local stage=$1 confirmed durable restart
  confirmed=$(psqlc -Atqc "select coalesce(confirmed_flush_lsn::text,'0/0') from pg_replication_slots where slot_name='boring_slot'")
  # restart_lsn is the WAL-retention signal: PostgreSQL cannot recycle WAL segments older than
  # this LSN. A connector that safe-stops but stays attached freezes restart_lsn while the slot
  # still reports active=true, so unbounded WAL accrues with no INACTIVE-slot alert ever firing.
  # Sampling it here during normal healthy streaming lets the final assertion prove it advances.
  restart=$(psqlc -Atqc "select coalesce(restart_lsn::text,'0/0') from pg_replication_slots where slot_name='boring_slot'")
  durable=$(python3 - "$journal" <<'PY'
import sqlite3,sys
with sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True) as db:
    print(db.execute("select coalesce(durable_transaction_end_lsn,'0000000000000000') from source_state where singleton=1").fetchone()[0])
PY
)
  python3 - "$stage" "$confirmed" "$durable" "$restart" "$work/feedback-boundaries.jsonl" <<'PY'
import json,sys
stage,confirmed,durable,restart,path=sys.argv[1:]
hi,lo=confirmed.split('/')
confirmed_value=(int(hi,16)<<32)+int(lo,16)
durable_value=int(durable,16)
assert confirmed_value <= durable_value, (stage,confirmed,durable)
rhi,rlo=restart.split('/')
restart_value=(int(rhi,16)<<32)+int(rlo,16)
assert restart_value <= confirmed_value, (stage,restart,confirmed)
with open(path,'a') as f:
    f.write(json.dumps({'stage':stage,'confirmed_flush_lsn':confirmed,'durable_journal_lsn':durable,'restart_lsn':restart,'restart_lsn_value':restart_value,'bounded':True},sort_keys=True)+'\n')
PY
}
commit_order() {
  local key=$1
  psqlc -qc "begin; set local role boring_cdc_app; insert into public.orders(id,total) values(${key},${key}.25); commit"
}

(cd "$work/run"; exec env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$binary" run >"$work/runtime-first.out" 2>"$work/runtime-first.err") &
runtime_pid=$!
wait_for_active_slot || { cat "$work/runtime-first.err" >&2; exit 1; }

commit_order 101
wait_for_transactions 1
sample_feedback_boundary before-limit
python3 - "$work/large.sql" <<'PYC'
import random,sys
r=random.Random(1729)
with open(sys.argv[1],'w') as f:
    f.write('begin; set local role boring_cdc_app;\n')
    for key in range(201,801):
        digits=''.join(r.choices('0123456789',k=12000))
        f.write(f'insert into public.orders(id,total) values ({key}, {key}.{digits});\n')
    f.write('commit;\n')
PYC
psqlc < "$work/large.sql" > "$work/large.out"
deadline=$((SECONDS+30))
until python3 - "$journal" <<'PYC'
import sqlite3,sys
with sqlite3.connect(f'file:{sys.argv[1]}?mode=ro',uri=True) as db:
    failure=db.execute("select failure_class,retry_class,armed from processing_failures where component='capture'").fetchone()
raise SystemExit(0 if failure==('configuration','deterministic',1) else 1)
PYC
do
    (( SECONDS < deadline )) || { cat "$work/runtime-first.err" >&2; exit 1; }
    sleep .2
done
sample_feedback_boundary after-limit
grep -q '^M2_CAPTURE_RESOURCE_LIMIT kind=spool_disk_reserve recovery=changed_limit_required$' "$work/runtime-first.err"
[[ "$(journal_transactions)" == 1 ]]
read -r active restart confirmed < <(psqlc -Atqc "select active::int||' '||restart_lsn::text||' '||confirmed_flush_lsn::text from pg_replication_slots where slot_name='boring_slot'")
[[ "$active" == 1 ]]
rss_kb=$(awk '/^VmHWM:/ {print $2}' "/proc/$runtime_pid/status")
[[ -n "$rss_kb" ]]
(( rss_kb * 1024 <= 268435456 ))
stop_bounded "$runtime_pid" TERM first || true
runtime_pid=
# Neither an unchanged configuration nor an unrelated edit may re-arm the stopped stream.
set +e
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$binary" run >"$work/unchanged.out" 2>"$work/unchanged.err")
unchanged_status=$?
set -e
[[ "$unchanged_status" != 0 ]]
grep -q 'M2_EXPLICIT_REARM_REQUIRED' "$work/unchanged.err"
python3 - "$work/run/boring-cdc.toml" <<'PYC'
from pathlib import Path
import sys
p=Path(sys.argv[1]); s=p.read_text().replace('schedule_ms = 60000','schedule_ms = 60001',1); p.write_text(s)
PYC
set +e
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$binary" run >"$work/unrelated.out" 2>"$work/unrelated.err")
unrelated_status=$?
set -e
[[ "$unrelated_status" != 0 ]]
grep -q 'M2_RECOVERY_UNRELATED_CONFIG_CHANGE' "$work/unrelated.err"
python3 - "$work/run/boring-cdc.toml" <<'PYC'
from pathlib import Path
import sys
p=Path(sys.argv[1]); s=p.read_text().replace('schedule_ms = 60001','schedule_ms = 60000',1).replace('capture_spool_bytes = 2000000','capture_spool_bytes = 100000000'); p.write_text(s)
PYC
(cd "$work/run"; exec env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$binary" run >"$work/runtime-restart.out" 2>"$work/runtime-restart.err") &
runtime_pid=$!
wait_for_active_slot || { cat "$work/runtime-restart.err" >&2; exit 1; }
wait_for_key 800
sample_feedback_boundary after-recovery

python3 - "$work/bulk.sql" <<'PY'
import random,sys
r=random.Random(1730)
with open(sys.argv[1],'w') as f:
    for key in range(1001,4001):
        digits=''.join(r.choices('0123456789',k=12000))
        f.write(f'begin; set local role boring_cdc_app; insert into public.orders(id,total) values ({key}, {key}.{digits}); commit;\n')
PY
psqlc < "$work/bulk.sql" > "$work/bulk.out"
wait_for_key 4000
psqlc -Atqc 'checkpoint' >/dev/null

source_restart=$restart
deadline=$((SECONDS+120))
while true; do
  confirmed_now=$(psqlc -Atqc "select confirmed_flush_lsn::text from pg_replication_slots where slot_name='boring_slot'")
  restart_now=$(psqlc -Atqc "select restart_lsn::text from pg_replication_slots where slot_name='boring_slot'")
  ready=$(python3 - "$journal" "$confirmed_now" "$restart_now" "$source_restart" <<'PY'
import json,sqlite3,sys
journal,confirmed,restart,baseline=sys.argv[1:]
def lsn(text):
    hi,lo=text.split('/')
    return (int(hi,16)<<32)+int(lo,16)
with sqlite3.connect(f'file:{journal}?mode=ro',uri=True) as db:
    rows=db.execute("select e.transaction_id,cast(e.payload as text) from journal_events e where e.control_kind is null order by e.journal_seq desc limit 100").fetchall()
    target=None
    for transaction_id,text in rows:
        row=json.loads(text)
        if row.get('kind')=='insert' and row.get('new') and int(bytes(row['new'][0]['bytes']).decode())==4000:
            target=db.execute('select end_lsn from source_transactions where transaction_id=?',(transaction_id,)).fetchone()[0]
            break
assert target is not None
print(int(lsn(confirmed)>=int(target,16) and lsn(restart)>lsn(baseline)))
PY
)
  [[ "$ready" == 1 ]] && break
  (( SECONDS < deadline )) || { echo "E_WAL_RELEASE_TIMEOUT confirmed=$confirmed_now restart=$restart_now baseline=$source_restart" >&2; exit 1; }
  psqlc -Atqc 'checkpoint' >/dev/null
  sleep 1
done
sample_feedback_boundary after-wal-release
rss_recovery_kb=$(awk '/^VmHWM:/ {print $2}' "/proc/$runtime_pid/status")
(( rss_recovery_kb * 1024 <= 268435456 ))
psqlc -Atqc "select xmin::text||'|'||id::text from public.orders order by id" >"$work/source-oracle.txt"
python3 - "$journal" "$work/source-oracle.txt" "$work/result.json" "$version" "$rss_kb" "$rss_recovery_kb" "$((SECONDS-started))" "$confirmed" "$confirmed_now" "$restart" "$restart_now" <<'PY'
import json,sqlite3,sys
journal,oracle_path,result_path,version,rss_first,rss_recovery,elapsed,confirmed_before,confirmed_after,restart_before,restart_after=sys.argv[1:]
oracle={tuple(line.strip().split('|')) for line in open(oracle_path) if line.strip()}
with sqlite3.connect(f'file:{journal}?mode=ro',uri=True) as db:
    transactions=db.execute("select transaction_id,xid from source_transactions where state='committed'").fetchall()
    events=db.execute("select journal_seq,transaction_id,cast(payload as text) from journal_events order by journal_seq").fetchall()
    capture_epoch,durable=db.execute("select capture_epoch,durable_transaction_end_lsn from source_state where singleton=1").fetchone()
    receipt=db.execute("select capture_epoch,runtime_fingerprint from capture_configuration_receipts where singleton=1").fetchone()
assert len({row[0] for row in transactions})==len(transactions)
assert len({row[1] for row in transactions})==len(transactions)
assert [row[0] for row in events]==list(range(1,len(events)+1))
assert receipt[0]==capture_epoch and receipt[1]!=capture_epoch
xids=dict(transactions)
journal_rows=set()
for _,transaction_id,text in events:
    row=json.loads(text)
    if row.get('kind')=='insert' and row.get('new'):
        journal_rows.add((xids[transaction_id],str(int(bytes(row['new'][0]['bytes']).decode()))))
assert len(journal_rows)==3601, len(journal_rows)
assert journal_rows==oracle, {'missing':sorted(oracle-journal_rows)[:5],'extra':sorted(journal_rows-oracle)[:5]}
def lsn(text):
    hi,lo=text.split('/')
    return (int(hi,16)<<32)+int(lo,16)
assert lsn(confirmed_before)<lsn(confirmed_after)<=int(durable,16)
assert lsn(restart_before)<lsn(restart_after)<=lsn(confirmed_after)
result={'postgres_version':version,'source_oracle_equals_read_only_journal':True,'user_rows':len(journal_rows),'journal_sequence_gap_free':True,'unique_transaction_ids':True,'unique_source_xids':True,'capture_epoch_preserved':True,'unchanged_and_unrelated_configs_rejected':True,'durable_end_lsn':durable,'confirmed_flush_lsn_before':confirmed_before,'confirmed_flush_lsn_after':confirmed_after,'restart_lsn_before':restart_before,'restart_lsn_after':restart_after,'rss_high_water_first_kb':int(rss_first),'rss_high_water_recovery_kb':int(rss_recovery),'elapsed_seconds':int(elapsed)}
with open(result_path,'w') as f: json.dump(result,f,sort_keys=True);f.write('\n')
print(json.dumps(result,sort_keys=True))
PY
stop_bounded "$runtime_pid" TERM recovery-runtime
runtime_pid=
printf 'VOLUME_BOUND_RECOVERY_OK postgres=%s user_rows=3601 named_error=M2_CAPTURE_RESOURCE_LIMIT oracle=set-equality feedback=bounded restart_lsn=advanced elapsed_seconds=%s\n' "$version" "$((SECONDS-started))"
