#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR="${TMPDIR:-/var/tmp}"; [[ "$TMPDIR" == /var/tmp ]]
cargo test --locked --workspace --all-targets
cargo test --locked m2_capture_runtime::tests
work=$(mktemp -d /var/tmp/m2-capture-e2e.XXXXXX); project="m2-capture-$RANDOM-$$"; port=$((56000 + $$ % 2000)); bootstrap_pid=; pid=
cleanup(){ [[ -z "$bootstrap_pid" ]] || kill -KILL "$bootstrap_pid" >/dev/null 2>&1 || true; [[ -z "$pid" ]] || kill -KILL "$pid" >/dev/null 2>&1 || true; docker compose -p "$project" -f compose.yaml -f "$work/override.yml" down -v --remove-orphans >/dev/null 2>&1 || true; rm -rf "$work"; }; trap cleanup EXIT INT TERM
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
cargo build --quiet --locked --bin boring-cdc
mkdir -p "$work/run/state/spool" "$work/run/state/tmp" "$work/run/archive/root"; chmod 700 "$work/run/state" "$work/run/state/spool" "$work/run/archive" "$work/run/archive/root"; cp tests/fixtures/m1_config/representative.toml "$work/run/boring-cdc.toml"
sed -i 's/publication = "boring_publication"/publication = "RuntimePublication"/; s/slot = "boring_slot"/slot = "runtime_slot"/; s#sqlite_path = "state/boring.db"#sqlite_path = "state/journal.sqlite"#' "$work/run/boring-cdc.toml"
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
  exec env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$OLDPWD/target/debug/boring-cdc" run >"$work/runtime.out" 2>"$work/runtime.err"
) & pid=$!
deadline=$((SECONDS+30)); until [[ "$(psqlc -Atqc "SELECT active::int FROM pg_replication_slots WHERE slot_name='runtime_slot'")" == 1 ]]; do (( SECONDS < deadline )) || { cat "$work/runtime.err" >&2; exit 1; }; sleep .1; done
python3 - "$work/run/state/journal.sqlite" "$work/sqlite-locked" <<'PY2' & lock_pid=$!
import pathlib,sqlite3,sys,time
c=sqlite3.connect(sys.argv[1]); c.execute('BEGIN IMMEDIATE'); pathlib.Path(sys.argv[2]).touch(); time.sleep(2); c.commit()
PY2
deadline=$((SECONDS+10)); until [[ -e "$work/sqlite-locked" ]]; do (( SECONDS < deadline )); sleep .02; done
psqlc -c "BEGIN; INSERT INTO orders VALUES(1); UPDATE orders SET id=id WHERE id=1; COMMIT" >/dev/null
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
feedback=$(psqlc -Atqc "SELECT coalesce(write_lsn::text,'')||','||coalesce(flush_lsn::text,'')||','||coalesce(replay_lsn::text,'') FROM pg_stat_replication ORDER BY pid LIMIT 1")
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
[[ "$feedback" == "$durable_lsn,$durable_lsn,$durable_lsn" ]]
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
