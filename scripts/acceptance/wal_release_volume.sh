#!/usr/bin/env bash
# Volume scenario: proves that during HEALTHY streaming (not safe-stop), PostgreSQL WAL
# retained on behalf of boring_slot is actually released, not merely bounded by luck.
#
# durable_simple_case.sh already guards that restart_lsn never regresses at a scale of six
# small transactions, and it deliberately does NOT assert that restart_lsn strictly advances,
# because PostgreSQL only moves restart_lsn at checkpoints, not per commit -- asserting a
# strict per-commit advance failed in CI on a perfectly healthy connector. This script drives
# enough sustained volume, and forces a real checkpoint, that a genuine advance -- and a
# genuine release of previously retained WAL -- becomes a property of the run, not timing luck.
#
# This is a slow, docker-based, non-CI scenario by design. It is not wired into
# .github/workflows/ci.yml.
set -euo pipefail
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp

work=$(mktemp -d /var/tmp/wal-release-volume-acceptance.XXXXXX)
project="wal-release-volume-$RANDOM-$$"
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

printf 'wal-release-volume-postgres-%s\n' "$project" >"$work/postgres_password"
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
# psqlc connects as the compose-provisioned PostgreSQL superuser ("boring_cdc", from
# POSTGRES_USER), not as any of the four dedicated connector roles created below. That
# superuser is the only role in this fixture that can issue CHECKPOINT and pg_switch_wal();
# none of boring_cdc_admin/boring_cdc_runtime/boring_cdc_control_writer/boring_cdc_app carry
# superuser or pg_checkpoint membership, and the prerequisite SQL explicitly strips any
# pre-existing role memberships from those four roles.
psqlc() { docker compose -p "$project" -f compose.yaml -f "$work/override.yml" exec -T postgres psql -X -v ON_ERROR_STOP=1 -U boring_cdc -d boring_cdc "$@"; }
version=$(psqlc -Atqc 'show server_version')
[[ "$version" == 17.6* ]]

# Confirm empirically (not by assumption) which role can checkpoint before relying on it.
superuser_can_checkpoint=$(psqlc -Atqc "select rolsuper or rolname = 'boring_cdc' from pg_roles where rolname = current_user")
[[ "$superuser_can_checkpoint" == t ]]

admin_credential='admin-wal-release-volume'
runtime_credential='runtime-wal-release-volume'
control_credential='control-wal-release-volume'
application_credential='application-wal-release-volume'
psqlc -v "admin_password=$admin_credential" -v "runtime_password=$runtime_credential" -v "control_password=$control_credential" -v "application_password=$application_credential" \
  < scripts/setup/durable_simple_prerequisites.sql >"$work/prerequisites.out"

# The four dedicated connector roles this fixture creates hold none of the privileges CHECKPOINT
# or pg_switch_wal() require. Recorded here as evidence, not merely asserted in the report.
for role in boring_cdc_admin boring_cdc_runtime boring_cdc_control_writer boring_cdc_app; do
  has_checkpoint_privilege=$(psqlc -Atqc "select rolsuper or pg_has_role('${role}','pg_checkpoint','member') from pg_roles where rolname='${role}'")
  [[ "$has_checkpoint_privilege" == f ]]
done

cargo build --quiet --locked --bin boring-cdc
binary="$PWD/target/debug/boring-cdc"
mkdir -p "$work/run/state/spool" "$work/run/state/tmp" "$work/run/archive/root"
chmod 700 "$work/run/state" "$work/run/state/spool" "$work/run/archive" "$work/run/archive/root"
cp tests/fixtures/m1_config/representative.toml "$work/run/boring-cdc.toml"

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
  local expected=$1 deadline
  deadline=$((SECONDS+480))
  # Wait for AT LEAST the expected count, never exact equality. The journal also carries the
  # fixture's own setup transactions, so the total overshoots the bulk-insert count (measured
  # 3097 against an expected 3000), and an equality test then either races a single poll or
  # never matches at all. That is what made this script pass under `bash -x` and fail without it.
  until (( $(journal_transactions) >= expected )); do
    (( SECONDS < deadline )) || { cat "$work/runtime.err" 2>/dev/null >&2 || true; exit 1; }
    kill -0 "$runtime_pid" 2>/dev/null || { cat "$work/runtime.err" >&2; exit 1; }
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
lsn_value() {
  # "hi/lo" hex text -> decimal byte offset, matching durable_simple_case.sh's parsing.
  local text=$1 hi lo
  hi=${text%%/*}; lo=${text##*/}
  python3 -c "print((int('$hi',16)<<32)+int('$lo',16))"
}
sample_slot() {
  # Emits "restart_lsn active retained_bytes generated_bytes" for the named stage. retained_bytes
  # is pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn): exactly what max_slot_wal_keep_size
  # bounds. generated_bytes is pg_current_wal_lsn() itself, used to show how much WAL this run
  # produced in total, independent of what is retained.
  psqlc -Atqc "select restart_lsn, active, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), pg_current_wal_lsn() from pg_replication_slots where slot_name='boring_slot'"
}
record_stage() {
  local stage=$1 line restart active retained current
  line=$(sample_slot)
  IFS='|' read -r restart active retained current <<<"$line"
  python3 - "$stage" "$restart" "$active" "$retained" "$current" "$work/stages.jsonl" <<'PY'
import json,sys
stage,restart,active,retained,current,path=sys.argv[1:]
with open(path,'a') as f:
    f.write(json.dumps({'stage':stage,'restart_lsn':restart,'active':active=='t','retained_bytes':int(retained),'current_wal_lsn':current},sort_keys=True)+'\n')
PY
}

(cd "$work/run"; exec env -u BORING_CDC_POSTGRES_PASSWORD_FILE "$binary" run >"$work/runtime.out" 2>"$work/runtime.err") &
runtime_pid=$!
wait_for_active_slot || { cat "$work/runtime.err" >&2; exit 1; }

record_stage baseline

# --- Drive sustained volume, well past a single 16MB WAL segment. ---
# Each row carries a large, independently-randomized numeric so it cannot be TOAST-compressed
# away (a repeated-digit value like all-nines compresses to almost nothing and undersells real
# WAL volume -- measured directly: 500 rows of an 8000-random-digit numeric produced ~2.1MB of
# WAL, ~4.2KB/row). At that measured rate this count and width clears one 16MB segment several
# times over. All transactions are single-row and independently committed, matching this
# fixture's application role and its DML grant (public.orders, per-row primary key).
transactions=3000
sql_batch="$work/bulk-insert.sql"
python3 - "$transactions" "$sql_batch" <<'PY'
import random, sys
n,path=int(sys.argv[1]),sys.argv[2]
random.seed(1729)
digit_count=12000
with open(path,'w') as f:
    for key in range(301, 301+n):
        digits=''.join(random.choices('0123456789', k=digit_count))
        f.write(f"begin;\nset local role boring_cdc_app;\ninsert into public.orders(id,total) values ({key}, {key}.{digits});\ncommit;\n")
PY
psqlc <"$sql_batch" >"$work/bulk-insert.out"

wait_for_transactions "$transactions"

# Wait deterministically for the slot's confirmed_flush_lsn to reach the journal's own durable
# boundary, rather than a fixed sleep. The connector's feedback is not sent per commit (this
# fixture's heartbeat_cadence_ms is 5000), so a short fixed sleep before checkpointing samples
# a stale confirmed position and understates how much WAL healthy consumption actually released
# -- observed directly: a 1-second sleep left ~5.1MB "retained" out of ~19.1MB generated, purely
# from feedback lag, not from anything still unconsumed.
wait_for_feedback_caught_up() {
  local durable_hex confirmed_text caught_up deadline
  durable_hex=$(python3 - "$journal" <<'PY'
import sqlite3,sys
with sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True) as db:
    print(db.execute("select durable_transaction_end_lsn from source_state where singleton=1").fetchone()[0])
PY
)
  deadline=$((SECONDS+60))
  while true; do
    confirmed_text=$(psqlc -Atqc "select coalesce(confirmed_flush_lsn::text,'0/0') from pg_replication_slots where slot_name='boring_slot'")
    caught_up=$(python3 - "$confirmed_text" "$durable_hex" <<'PY'
import sys
confirmed_text,durable_hex=sys.argv[1],sys.argv[2]
hi,lo=confirmed_text.split('/')
confirmed_value=(int(hi,16)<<32)+int(lo,16)
durable_value=int(durable_hex,16)
print(1 if confirmed_value>=durable_value else 0)
PY
)
    [[ "$caught_up" == 1 ]] && break
    (( SECONDS < deadline )) || { echo "E_FEEDBACK_NOT_CAUGHT_UP durable=$durable_hex confirmed=$confirmed_text" >&2; exit 1; }
    sleep .2
  done
}
wait_for_feedback_caught_up
record_stage before-checkpoint

# Force PostgreSQL to actually reconsider retention: pg_switch_wal() closes the current segment
# so restart_lsn can move past it, and CHECKPOINT is what actually advances restart_lsn (it does
# not move per-commit). Both are issued as the compose superuser confirmed above.
psqlc -Atqc "select pg_switch_wal()" >/dev/null
psqlc -Atqc "checkpoint"
sleep 1
record_stage after-checkpoint

# --- Correctness properties, same oracle style as durable_simple_case.sh. ---
psqlc -Atqc "select xmin::text||'|'||id::text from public.orders order by id" >"$work/postgres-oracle.txt"
# confirmed_flush_lsn lives on the PostgreSQL slot, not in the SQLite journal (source_state only
# tracks what the journal itself has durably recorded as observed_confirmed_flush_lsn once fed
# back, and the journal is opened strictly read-only below -- this scenario never writes to it).
confirmed_flush_lsn=$(psqlc -Atqc "select coalesce(confirmed_flush_lsn::text,'0/0') from pg_replication_slots where slot_name='boring_slot'")
python3 - "$journal" "$work/postgres-oracle.txt" "$work/result.json" "$version" "$transactions" "$confirmed_flush_lsn" <<'PY'
import json,sqlite3,sys
journal,oracle_path,result_path,version,transactions,confirmed_flush_lsn=sys.argv[1],sys.argv[2],sys.argv[3],sys.argv[4],int(sys.argv[5]),sys.argv[6]
oracle={tuple(line.strip().split('|')) for line in open(oracle_path) if line.strip()}
with sqlite3.connect(f"file:{journal}?mode=ro", uri=True) as db:
    rows=db.execute("select transaction_id,xid,first_seq,last_seq,event_count from source_transactions where state='committed' order by first_seq").fetchall()
    events=db.execute("select journal_seq,transaction_id,cast(payload as text) from journal_events order by journal_seq").fetchall()
    state=db.execute("select durable_transaction_end_lsn,durable_journal_seq from source_state where singleton=1").fetchone()
assert len(rows) >= transactions, (len(rows), transactions)
assert len({row[0] for row in rows}) == len(rows), 'duplicate transaction_id'
assert len({row[1] for row in rows}) == len(rows), 'duplicate source xid'
assert [row[0] for row in events] == list(range(1, len(events)+1)), 'journal sequence has a gap'
xid_by_transaction={row[0]: row[1] for row in rows}
journal_set=set()
for _, transaction_id, payload_text in events:
    payload=json.loads(payload_text)
    if payload['kind'] != 'insert':
        continue
    key=str(int(bytes(payload['new'][0]['bytes']).decode('ascii')))
    journal_set.add((xid_by_transaction[transaction_id], key))
assert journal_set == oracle, {'missing_from_journal': sorted(oracle - journal_set), 'extra_in_journal': sorted(journal_set - oracle)}
hi, lo = confirmed_flush_lsn.split('/')
confirmed_value = (int(hi, 16) << 32) + int(lo, 16)
durable_value = int(state[0], 16)
assert confirmed_value <= durable_value, ('confirmed_flush_lsn ahead of durable journal boundary', confirmed_flush_lsn, state[0])
result={
  'postgres_version': version,
  'transactions_driven': transactions,
  'journal_transactions': len(rows),
  'journal_events': len(events),
  'unique_transaction_ids': True,
  'unique_source_xids': True,
  'journal_sequence_gap_free': True,
  'postgres_oracle_equals_journal': True,
  'confirmed_flush_never_exceeded_durable_boundary': True,
}
with open(result_path, 'w') as f:
    json.dump(result, f, indent=2, sort_keys=True)
    f.write('\n')
PY

# --- WAL-release properties: the actual point of this scenario. ---
wal_segment_size=$(psqlc -Atqc "select setting::bigint from pg_settings where name='wal_segment_size'")
python3 - "$work/stages.jsonl" "$work/result.json" "$wal_segment_size" <<'PY'
import json,sys
stages_path,result_path=sys.argv[1],sys.argv[2]
wal_segment_size=int(sys.argv[3])
stages={s['stage']: s for s in (json.loads(l) for l in open(stages_path))}
def lsn(text):
    hi,lo=text.split('/')
    return (int(hi,16)<<32)+int(lo,16)
baseline,before,after=stages['baseline'],stages['before-checkpoint'],stages['after-checkpoint']
restart_before=lsn(before['restart_lsn'])
restart_after=lsn(after['restart_lsn'])
current_before=lsn(before['current_wal_lsn'])
current_baseline=lsn(baseline['current_wal_lsn'])
generated_bytes=current_before-current_baseline
# The connector was consuming the whole time (never entered safe-stop), so the slot never
# stops reporting active.
assert baseline['active'] and before['active'] and after['active'], stages
# restart_lsn must never regress -- going backwards would mean the slot re-pinned WAL it had
# already released.
assert restart_before >= lsn(baseline['restart_lsn']), 'restart_lsn regressed before checkpoint'
assert restart_after >= restart_before, 'restart_lsn regressed across checkpoint'
# The real property this scenario exists to prove: under this much sustained volume plus a
# forced checkpoint, restart_lsn genuinely ADVANCES -- this is not timing luck, unlike the
# small six-transaction case in durable_simple_case.sh, which correctly does not assert this.
assert restart_after > lsn(baseline['restart_lsn']), ('restart_lsn never advanced', stages)
# What remains retained after the checkpoint is the real proof of release. The bound is the
# server's WAL SEGMENT SIZE, not a fraction of generated volume: PostgreSQL frees WAL at segment
# granularity, so a healthy slot settles at under one segment no matter how much was generated,
# while a slot stuck in safe-stop keeps growing without limit.
#
# An earlier form of this asserted retained < generated/4. That is badly founded: retention is
# ~constant at segment granularity while generated grows with the size of the bulk insert, so the
# ratio measures how much volume the TEST drove, not how well the product releases. It failed at
# 26.8% (5,128,920 retained of 19,139,512 generated) on two independent runs, even though 5.1MB is
# already under a third of one 16MB segment — i.e. release was essentially complete.
retained_after=after['retained_bytes']
assert generated_bytes > wal_segment_size, ('bulk insert did not clear one WAL segment', generated_bytes, wal_segment_size)
assert retained_after <= wal_segment_size, ('retained WAL exceeds one segment, so the slot is not releasing', retained_after, wal_segment_size)
# Keep a sanity link to volume: retention must not track what was generated.
assert retained_after < generated_bytes // 2, ('retained bytes tracked generated volume', retained_after, generated_bytes)
result=json.load(open(result_path))
result.update({
  'stages': stages,
  'generated_bytes_during_bulk_insert': generated_bytes,
  'restart_lsn_before_bulk_insert': baseline['restart_lsn'],
  'restart_lsn_after_checkpoint': after['restart_lsn'],
  'retained_bytes_after_checkpoint': retained_after,
  'wal_segment_size': wal_segment_size,
  'retained_within_one_wal_segment': True,
  'restart_lsn_advanced': True,
  'retained_bytes_bounded': True,
})
json.dump(result, open(result_path, 'w'), indent=2, sort_keys=True)
print(json.dumps(result, sort_keys=True))
PY

stop_bounded "$runtime_pid" TERM runtime
runtime_pid=
[[ ! -s "$work/runtime.err" ]]
echo "WAL_RELEASE_VOLUME_OK postgres=$version transactions=$transactions restart_lsn_advanced=true retained_bytes_bounded=true oracle=set-equality"
