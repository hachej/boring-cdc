#!/usr/bin/env bash
set -euo pipefail
ulimit -c 0

repo=$(cd "$(dirname "$0")/../.." && pwd)
packet_dir=${BCDC_ARTICLE2_PACKET_DIR:-$repo/evidence/article2/destination_outage/run-$(date -u +%Y%m%dT%H%M%SZ)-$$}
outage_seconds=${BCDC_ARTICLE2_OUTAGE_SECONDS:-120}
workload_transactions=${BCDC_ARTICLE2_WORKLOAD_TRANSACTIONS:-24}
recovery_wait_seconds=${BCDC_ARTICLE2_RECOVERY_WAIT_SECONDS:-600}
work=$(mktemp -d /var/tmp/article2-destination-outage.XXXXXX)
project="article2-outage-$RANDOM-$$"
slot="boring_cdc_article2_${RANDOM}_$$"
clickhouse_container="article2-outage-ch-$RANDOM-$$"
pg_port=$((52000 + $$ % 1000))
ch_https_port=$((54000 + $$ % 1000))
ch_http_port=$((55000 + $$ % 1000))
connector_pid=""
cleanup() {
  if [[ "${BCDC_ARTICLE2_DEBUG_KEEP:-0}" == 1 ]]; then
    echo "Article 2 debug workspace: $work" >&2
    return
  fi
  if [[ -n "$connector_pid" ]]; then kill "$connector_pid" 2>/dev/null || true; fi
  docker rm -f "$clickhouse_container" >/dev/null 2>&1 || true
  BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password" docker compose -p "$project" -f "$repo/compose.yaml" -f "$work/override.yml" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT INT TERM

python3 - "$work" <<'PY'
import pathlib,secrets,sys
root=pathlib.Path(sys.argv[1])
for name in ('postgres_password','clickhouse_password'):
    path=root/name
    path.write_text(secrets.token_hex(24)+'\n')
    path.chmod(0o600)
PY
export BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password"
export PGPASSWORD
PGPASSWORD=$(cat "$BORING_CDC_POSTGRES_PASSWORD_FILE")
cat >"$work/override.yml" <<YAML
services:
  postgres:
    ports: ["127.0.0.1:${pg_port}:5432"]
YAML
docker compose -p "$project" -f "$repo/compose.yaml" -f "$work/override.yml" up -d --wait postgres >/dev/null
export PG_RUNTIME PG_CONTROL PG_ADMIN BORING_CDC_M3_DSN BORING_CDC_M3_SLOT
PG_RUNTIME='postgresql://boring_cdc:'
PG_RUNTIME+="${PGPASSWORD}@127.0.0.1:${pg_port}/boring_cdc?sslmode=disable"
PG_CONTROL=$PG_RUNTIME
PG_ADMIN=$PG_RUNTIME
BORING_CDC_M3_DSN=$PG_RUNTIME
BORING_CDC_M3_SLOT=$slot

openssl req -x509 -newkey rsa:2048 -nodes -keyout "$work/ca.key" -out "$work/ca.pem" -days 1 -subj '/CN=article2-local-ca' >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -keyout "$work/server.key" -out "$work/server.csr" -subj '/CN=127.0.0.1' >/dev/null 2>&1
cat >"$work/server.ext" <<'EOF'
subjectAltName=IP:127.0.0.1
extendedKeyUsage=serverAuth
keyUsage=digitalSignature,keyEncipherment
EOF
openssl x509 -req -in "$work/server.csr" -CA "$work/ca.pem" -CAkey "$work/ca.key" -CAcreateserial -out "$work/server.crt" -days 1 -extfile "$work/server.ext" >/dev/null 2>&1
chmod 644 "$work/server.key" "$work/server.crt"
cat >"$work/clickhouse-https.xml" <<'XML'
<clickhouse>
  <https_port>8443</https_port>
  <openSSL><server>
    <certificateFile>/etc/clickhouse-server/tls/server.crt</certificateFile>
    <privateKeyFile>/etc/clickhouse-server/tls/server.key</privateKeyFile>
    <verificationMode>none</verificationMode>
  </server></openSSL>
</clickhouse>
XML
ch_password=$(cat "$work/clickhouse_password")
printf 'CLICKHOUSE_USER=bcdc_probe\nCLICKHOUSE_PASSWORD=%s\nCLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1\n' "$ch_password" >"$work/clickhouse.env"
chmod 600 "$work/clickhouse.env"
docker run -d --name "$clickhouse_container" --platform linux/amd64 \
  -p "127.0.0.1:${ch_http_port}:8123" -p "127.0.0.1:${ch_https_port}:8443" --env-file "$work/clickhouse.env" \
  -v "$work/clickhouse-https.xml:/etc/clickhouse-server/config.d/article2-https.xml:ro" \
  -v "$work/server.crt:/etc/clickhouse-server/tls/server.crt:ro" \
  -v "$work/server.key:/etc/clickhouse-server/tls/server.key:ro" \
  docker.io/clickhouse/clickhouse-server:25.8.2.29@sha256:74c213b4d4cb4854c2497694df0c2d153c041003eadbb0457ae62c28cb8d723f >/dev/null
ch() { docker exec "$clickhouse_container" clickhouse-client --user bcdc_probe --password "$ch_password" "$@"; }
ready=false
for _ in $(seq 1 90); do
  if ch --query 'SELECT version()' >"$work/ch-version" 2>/dev/null; then ready=true; break; fi
  sleep 1
done
[[ "$ready" == true && "$(cat "$work/ch-version")" == 25.8.2.29 ]]
docker exec -i "$clickhouse_container" clickhouse-client --user bcdc_probe --password "$ch_password" --multiquery < "$repo/contracts/clickhouse/ddl.sql"
http_port=$ch_http_port
https_port=$ch_https_port
export SSL_CERT_FILE="$work/ca.pem"
export CH_RUNTIME="https://bcdc_probe:${ch_password}@127.0.0.1:${https_port}"
export BCDC_M4_CLICKHOUSE_URL="http://127.0.0.1:${http_port}"
export BCDC_M4_CLICKHOUSE_USER=bcdc_probe
printf -v BCDC_M4_CLICKHOUSE_PASSWORD '%s' "$ch_password"
export BCDC_M4_CLICKHOUSE_PASSWORD
export BCDC_ARTICLE2_SAME_STREAM=1
export BCDC_ARTICLE2_HANDOFF_OUT="$work/handoff.json"
export BCDC_ARTICLE2_HANDOFF_DB="$work/state/boring.db"
export BCDC_ARTICLE2_HANDOFF_CONFIG="$work/boring-cdc.toml"
export BCDC_ARTICLE2_ORACLE_OUT="$work/initial-oracle.json"
mkdir -p "$work/state" "$work/state/tmp" "$work/state/spool" "$work/archive/root"
chmod 700 "$work/state" "$work/state/tmp" "$work/state/spool"
python3 - "$repo/tests/fixtures/m1_config/representative.toml" "$work/boring-cdc.toml" "$slot" "$https_port" <<'PY'
from pathlib import Path
import sys
source,target,slot,port=sys.argv[1:]
text=Path(source).read_text()
for old,new in (
    ('publication = "boring_publication"','publication = "boring_cdc_m3_pub"'),
    ('slot = "boring_slot"',f'slot = "{slot}"'),
    ('source_relation = "public.orders"','source_relation = "public.m3_bootstrap_fixture"'),
    ('relation_contract = { id = "int8:not-null", total = "numeric:nullable" }','relation_contract = { id = "int8:not-null", payload = "text:nullable" }'),
    ('verify_tls = true','verify_tls = false'),
    ('unsafe_local_experiment = false','unsafe_local_experiment = true'),
    ('endpoint = "https://clickhouse.local:8443"',f'endpoint = "https://127.0.0.1:{port}"'),
):
    assert old in text,old
    text=text.replace(old,new)
Path(target).write_text(text)
PY

cd "$repo"
cargo test --locked m3_bootstrap::live_tests::exported_snapshot_uses_distinct_command_idle_sessions -- --ignored --exact
python3 - "$work/handoff.json" "$work/state/boring.db" <<'PY'
import json,sqlite3,sys
handoff=json.load(open(sys.argv[1]))
assert handoff['sqlite_path']==sys.argv[2],handoff
db=sqlite3.connect(sys.argv[2])
anchor=db.execute("SELECT state,start_seq FROM bootstrap_anchors WHERE anchor_id=?",(handoff['anchor_id'],)).fetchone()
assert anchor==('complete',0),anchor
baseline=db.execute("SELECT count(*) FROM m4_snapshot_batch_intents WHERE destination_id='snapshot-probe' AND state='verified'").fetchone()[0]
assert baseline>0,baseline
lease=db.execute("SELECT state FROM destination_generation_leases WHERE lease_id='article2-anchor-lease'").fetchone()
assert lease==('released',),lease
PY
cargo build --locked --bin boring-cdc
unset BORING_CDC_M3_DSN BORING_CDC_M3_SLOT BORING_CDC_POSTGRES_PASSWORD_FILE
cd "$work"
"$repo/target/debug/boring-cdc" run >"$work/capture.log" 2>&1 &
connector_pid=$!
for _ in $(seq 1 60); do
  if ! kill -0 "$connector_pid" 2>/dev/null; then cat "$work/capture.log" >&2; exit 1; fi
  if "$repo/target/debug/boring-cdc" status --json >"$work/status.json" 2>/dev/null; then
    if jq -e '.data.ownership.state == "held" or .ownership.state == "held"' "$work/status.json" >/dev/null; then break; fi
  fi
  sleep 1
done
kill -0 "$connector_pid"
echo "Article 2 handoff reached the shipped binary; outage sampling follows" >&2
binary="$repo/target/debug/boring-cdc"
start_ns=$(python3 -c 'import time;print(time.monotonic_ns())')
elapsed_ms() { python3 - "$start_ns" <<'PY'
import sys,time
print((time.monotonic_ns()-int(sys.argv[1]))//1_000_000)
PY
}
python3 "$repo/scripts/e2e/article2_destination_sample.py" "$work" "$pg_port" "$slot" "$binary" "$start_ns" >"$work/sampler.log" 2>&1 &
sampler_pid=$!
run_workload() {
python3 - "$pg_port" "$work" "$1" "$2" "$3" <<'PY'
import os,subprocess,sys,time
port,work,total,first,marker=sys.argv[1:]
for number in range(int(first),int(first)+int(total)):
    sql=(f"BEGIN; INSERT INTO public.m3_bootstrap_fixture(id,payload) VALUES({number},'value-{number}'); "
         f"UPDATE public.m3_bootstrap_fixture SET payload='tick-{number}' WHERE id=1; COMMIT;")
    subprocess.run(['psql','-v','ON_ERROR_STOP=1','-q','-h','127.0.0.1','-p',port,'-U','boring_cdc','-d','boring_cdc','-c',sql],
                   check=True,stdout=subprocess.DEVNULL)
    time.sleep(5)
open(os.path.join(work,marker),'w').write(total+'\n')
PY
}
run_workload 3 2 prelude-complete >"$work/prelude.log" 2>&1

in_sync=false
for _ in $(seq 1 90); do
  kill -0 "$connector_pid"
  kill -0 "$sampler_pid"
  if python3 - "$work/state/boring.db" 2>/dev/null <<'PY'
import sqlite3,sys
db=sqlite3.connect(sys.argv[1])
head=db.execute('SELECT durable_journal_seq FROM source_state').fetchone()[0]
checkpoint=db.execute("SELECT journal_seq FROM destination_checkpoints WHERE destination_id='snapshot-probe'").fetchone()[0]
lease=db.execute("SELECT run_id FROM destination_generation_leases WHERE destination_id='snapshot-probe' AND state='held'").fetchone()[0]
owner=db.execute("SELECT run_id FROM runtime_ownership WHERE state='held' ORDER BY rowid DESC LIMIT 1").fetchone()[0]
assert head>4 and checkpoint==head and lease==owner,(head,checkpoint,lease,owner)
PY
  then in_sync=true; break; fi
  sleep 1
done
if [[ "$in_sync" != true ]]; then
  mkdir -p "$packet_dir"
  python3 - "$work/state/boring.db" "$packet_dir/setup-failure.json" <<'PY'
import json,sqlite3,sys
db=sqlite3.connect(sys.argv[1])
def one(query):
    row=db.execute(query).fetchone()
    return row[0] if row else None
result={
    'phase':'initial_sync',
    'journal_head_seq':one('SELECT durable_journal_seq FROM source_state'),
    'checkpoint_seq':one("SELECT journal_seq FROM destination_checkpoints WHERE destination_id='snapshot-probe'"),
    'destination_lease_run_id':one("SELECT run_id FROM destination_generation_leases WHERE destination_id='snapshot-probe' AND state='held'"),
    'source_owner_run_id':one("SELECT run_id FROM runtime_ownership WHERE state='held' ORDER BY rowid DESC LIMIT 1"),
    'failures':[{'component':row[0],'class':row[1],'retry_class':row[2],'attempt':row[3]} for row in db.execute("SELECT component,failure_class,retry_class,attempt FROM processing_failures WHERE destination_id='snapshot-probe'")],
}
with open(sys.argv[2],'w') as stream:json.dump(result,stream,sort_keys=True)
print(json.dumps(result,sort_keys=True),file=sys.stderr)
PY
  exit 1
fi
docker stop "$clickhouse_container" >/dev/null
stopped_ms=$(elapsed_ms)
run_workload "$workload_transactions" 5 workload-complete >"$work/workload.log" 2>&1 &
workload_pid=$!
sleep "$outage_seconds"
docker start "$clickhouse_container" >/dev/null
[[ "$(docker port "$clickhouse_container" 8443/tcp)" == "127.0.0.1:${https_port}" ]]
[[ "$(docker port "$clickhouse_container" 8123/tcp)" == "127.0.0.1:${http_port}" ]]
restarted_ms=$(elapsed_ms)
wait "$workload_pid"
catch_up_ms=""
for _ in $(seq 1 "$recovery_wait_seconds"); do
  kill -0 "$connector_pid"
  if python3 - "$work/state/boring.db" 2>/dev/null <<'PY'
import sqlite3,sys
db=sqlite3.connect(sys.argv[1])
head=db.execute('SELECT durable_journal_seq FROM source_state').fetchone()[0]
checkpoint=db.execute("SELECT journal_seq FROM destination_checkpoints WHERE destination_id='snapshot-probe'").fetchone()[0]
assert head is not None and checkpoint==head,(head,checkpoint)
PY
  then catch_up_ms=$(elapsed_ms); break; fi
  sleep 1
done
sleep 5
touch "$work/stop-sampling"
wait "$sampler_pid"
echo "outage_stopped_ms=$stopped_ms restarted_ms=$restarted_ms caught_up_ms=${catch_up_ms:-unavailable}" >&2
psql -At -F '|' -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$pg_port" -U boring_cdc -d boring_cdc \
  -c 'SELECT id::text,payload FROM public.m3_bootstrap_fixture ORDER BY id' >"$work/source-oracle.tsv"
docker exec "$clickhouse_container" clickhouse-client --user bcdc_probe --password "$ch_password" \
  --query 'SELECT toString(connector_event_id) FROM boring_cdc.event_history_v1 WHERE capture_epoch=1 AND generation=1 AND journal_seq>0 FORMAT TSVRaw' >"$work/remote-event-ids.txt"
logical_table_id=$(docker exec "$clickhouse_container" clickhouse-client --user bcdc_probe --password "$ch_password" \
  --query 'SELECT DISTINCT logical_table_id FROM boring_cdc.event_history_v1 WHERE capture_epoch=1 AND generation=1')
[[ "$logical_table_id" =~ ^[0-9a-f]{64}$ ]]
python3 - "$repo/contracts/clickhouse/canonical-query.sql" "$work/candidate-query.sql" <<'PY'
from pathlib import Path
import sys
query=Path(sys.argv[1]).read_text()
old='WITH selected AS (SELECT generation,promotion_fence FROM boring_cdc.live_generation_v1 WHERE capture_epoch={capture_epoch:UInt64}),'
assert old in query
Path(sys.argv[2]).write_text(query.replace(old,'WITH selected AS (SELECT 1 AS generation,0 AS promotion_fence),',1))
PY
docker exec -i "$clickhouse_container" clickhouse-client --user bcdc_probe --password "$ch_password" \
  --param_capture_epoch=1 --param_logical_table_id="$logical_table_id" --format JSONEachRow \
  --queries-file /dev/stdin <"$work/candidate-query.sql" >"$work/candidate-rows.jsonl"
mkdir -p "$(dirname "$packet_dir")"
python3 "$repo/scripts/e2e/article2_destination_packet.py" "$work" "$repo" "$packet_dir" \
  "$stopped_ms" "$restarted_ms" "$catch_up_ms" "$outage_seconds" "$recovery_wait_seconds"
python3 "$packet_dir/validate.py" "$packet_dir/observation.json"
