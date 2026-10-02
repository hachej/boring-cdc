#!/usr/bin/env bash
# Diagnostic for boring-cdc-ckoi.6. This succeeds when the current recovery gap is
# reproduced; it is not the volume acceptance or a convergence claim.
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
  deadline=$((SECONDS+30))
  until [[ "$(journal_transactions)" == "$expected" ]]; do
    (( SECONDS < deadline )) || { cat "$work/runtime-restart.err" "$work/runtime-first.err" 2>/dev/null >&2 || true; exit 1; }
    sleep .1
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
raise SystemExit(0 if failure==('integrity','integrity_mismatch',1) else 1)
PYC
do
    (( SECONDS < deadline )) || { cat "$work/runtime-first.err" >&2; exit 1; }
    sleep .2
done
sample_feedback_boundary after-limit
[[ "$(journal_transactions)" == 1 ]]
read -r active restart confirmed < <(psqlc -Atqc "select active::int||' '||restart_lsn::text||' '||confirmed_flush_lsn::text from pg_replication_slots where slot_name='boring_slot'")
[[ "$active" == 1 ]]
rss_kb=$(awk '/^VmHWM:/ {print $2}' "/proc/$runtime_pid/status")
[[ -n "$rss_kb" ]]
(( rss_kb * 1024 <= 268435456 ))
stop_bounded "$runtime_pid" TERM first || true
runtime_pid=
python3 - "$work/run/boring-cdc.toml" <<'PYC'
from pathlib import Path
import sys
p=Path(sys.argv[1]); s=p.read_text().replace('capture_spool_bytes = 2000000','capture_spool_bytes = 20000000'); p.write_text(s)
PYC
set +e
(cd "$work/run"; env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$binary" run >"$work/runtime-restart.out" 2>"$work/runtime-restart.err")
restart_status=$?
set -e
[[ "$restart_status" != 0 ]]
grep -q 'M2_EXPLICIT_REARM_REQUIRED' "$work/runtime-restart.err"
printf 'VOLUME_BOUND_RECOVERY_GAP_REPRODUCED postgres=%s committed_before_limit=1 post_limit_journal_commits=%s failure=integrity/resource-limit changed_budget_restart=M2_EXPLICIT_REARM_REQUIRED slot_active_before_shutdown=%s restart_lsn=%s confirmed_flush_lsn=%s rss_high_water_kb=%s process_memory_budget_bytes=268435456 elapsed_seconds=%s\n' "$version" "$(journal_transactions)" "$active" "$restart" "$confirmed" "$rss_kb" "$((SECONDS-started))"
