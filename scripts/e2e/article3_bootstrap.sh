#!/usr/bin/env bash
set -euo pipefail
ulimit -c 0
repo=$(cd "$(dirname "$0")/../.." && pwd)
cd "$repo"
packet=${BCDC_ARTICLE3_PACKET_DIR:-$(mktemp -d /var/tmp/article3-packet.XXXXXX)}
mkdir -p "$packet"
packet=$(cd "$packet" && pwd)
work=$(mktemp -d /var/tmp/article3-bootstrap.XXXXXX)
project="article3-bootstrap-$RANDOM-$$"
clickhouse_container="$project-ch"
pg_port=$((52000 + $$ % 1000))
status_port=$((60000 + $$ % 1000))
metrics_port=$((61000 + $$ % 1000))
connector_pid=""
workload_pid=""
scenario=article3-bootstrap-cli
fixture_rows=10000
chunk_rows=25
workload_writes=6
if [[ "${BCDC_ARTICLE3_MUTATION:-0}" == 1 ]]; then
  scenario=article3-concurrent-mutation
  fixture_rows=${BCDC_ARTICLE3_MUTATION_ROWS:-200000}
  chunk_rows=500
  workload_writes=${BCDC_ARTICLE3_MUTATION_WRITES:-72}
fi
[[ "$fixture_rows" =~ ^[0-9]+$ && "$workload_writes" =~ ^[0-9]+$ ]]
stage=provision
finish() {
  local status=$?
  trap - EXIT
  cd "$repo"
  if [[ -n "$workload_pid" ]]; then kill "$workload_pid" 2>/dev/null || true; wait "$workload_pid" || true; fi
  if [[ -n "$connector_pid" ]]; then kill "$connector_pid" 2>/dev/null || true; wait "$connector_pid" || true; fi
  docker rm -f "$clickhouse_container" >/dev/null 2>&1 || true
  BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password" docker compose -p "$project" -f "$repo/compose.yaml" -f "$work/override.yml" down -v --remove-orphans >/dev/null 2>&1 || true
  python3 - "$packet" "$stage" "$status" "$work" "$scenario" <<'PY_PACKET'
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

root, stage, status, work = Path(sys.argv[1]), sys.argv[2], int(sys.argv[3]), Path(sys.argv[4])
observation = {
    "scenario": sys.argv[5],
    "stage": stage,
    "exit_code": status,
    "result": "pass" if stage == "complete" and status == 0 else "fail",
}
log = work / "connector.log"
if log.is_file():
    observation["connector_codes"] = sorted(set(re.findall(
        r"\b(?:M[0-9]|BCDC|E)_[A-Z0-9]+(?:_[A-Z0-9]+)+\b",
        log.read_text(errors="replace"),
    )))
for field, name in (("overlap", "progress.json"), ("convergence", "checkpoint.json"), ("oracle", "oracle.json"), ("status", "backfill-status.json"), ("before_restart", "before-restart.json"), ("mutation", "mutation-result.json")):
    if (root / name).is_file():
        observation[field] = json.loads((root / name).read_text())
(root / "observation.json").write_text(json.dumps(observation, indent=2, sort_keys=True) + "\n")
manifest = {
    "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
    "ci_run_id": os.getenv("GITHUB_RUN_ID"),
    "ci_run_attempt": os.getenv("GITHUB_RUN_ATTEMPT"),
    "command": ["scripts/e2e/article3_concurrent_mutation.sh" if observation["scenario"] == "article3-concurrent-mutation" else "scripts/e2e/article3_bootstrap.sh"],
    "files": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(root.iterdir()) if p.is_file() and p.name != "manifest.json"},
}
(root / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
PY_PACKET
  rm -rf "$work"
  echo "Article 3 bootstrap exited at $stage ($status). Packet: $packet"
  exit "$status"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

umask 077
for file in postgres_password clickhouse_password; do openssl rand -hex 24 >"$work/$file"; done
export BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password"
export PGPASSWORD
PGPASSWORD=$(cat "$BORING_CDC_POSTGRES_PASSWORD_FILE")
cat >"$work/override.yml" <<YAML
services:
  postgres:
    ports: ["127.0.0.1:${pg_port}:5432"]
YAML
docker compose -p "$project" -f "$repo/compose.yaml" -f "$work/override.yml" up -d --wait postgres >/dev/null
pg() { psql -X -h 127.0.0.1 -p "$pg_port" -U boring_cdc -d boring_cdc -v ON_ERROR_STOP=1 "$@"; }
[[ "$(pg -Atqc 'SHOW server_version')" == 17.6* ]]
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$work/ca.key" -out "$work/ca.pem" -days 1 -subj '/CN=article3-local-ca' >/dev/null 2>&1
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
chmod 644 "$work/clickhouse-https.xml"
ch_password=$(cat "$work/clickhouse_password")
printf 'CLICKHOUSE_USER=bcdc_probe\nCLICKHOUSE_PASSWORD=%s\nCLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1\n' "$ch_password" >"$work/clickhouse.env"
chmod 600 "$work/clickhouse.env"
clickhouse_publish=127.0.0.1::8443
if [[ "${BCDC_ARTICLE3_BASELINE_OUTAGE:-0}" == 1 ]]; then
  ch_https_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
  clickhouse_publish="127.0.0.1:$ch_https_port:8443"
fi
docker run -d --name "$clickhouse_container" --platform linux/amd64 \
  -p "$clickhouse_publish" --env-file "$work/clickhouse.env" \
  -v "$work/clickhouse-https.xml:/etc/clickhouse-server/config.d/article3-https.xml:ro" \
  -v "$work/server.crt:/etc/clickhouse-server/tls/server.crt:ro" \
  -v "$work/server.key:/etc/clickhouse-server/tls/server.key:ro" \
  docker.io/clickhouse/clickhouse-server:25.8.2.29@sha256:74c213b4d4cb4854c2497694df0c2d153c041003eadbb0457ae62c28cb8d723f >/dev/null
ch_https_port=$(docker port "$clickhouse_container" 8443/tcp)
ch_https_port=${ch_https_port##*:}
ch() { docker exec "$clickhouse_container" clickhouse-client --user bcdc_probe --password "$ch_password" "$@"; }
ready=false
for _ in $(seq 1 90); do
  if ch --query 'SELECT version()' >"$work/ch-version" 2>/dev/null; then ready=true; break; fi
  sleep 1
done
[[ "$ready" == true && "$(cat "$work/ch-version")" == 25.8.2.29 ]]
docker exec -i "$clickhouse_container" clickhouse-client --user bcdc_probe --password "$ch_password" --multiquery < "$repo/contracts/clickhouse/ddl.sql"
export SSL_CERT_FILE="$work/ca.pem"
export CH_RUNTIME="https://bcdc_probe:${ch_password}@127.0.0.1:${ch_https_port}"
export CH_MAINT=$CH_RUNTIME

stage=seed
admin_password=$(openssl rand -hex 24)
runtime_password=$(openssl rand -hex 24)
control_password=$(openssl rand -hex 24)
app_password=$(openssl rand -hex 24)
pg -v "admin_password=$admin_password" -v "runtime_password=$runtime_password" \
  -v "control_password=$control_password" -v "application_password=$app_password" \
  < scripts/setup/durable_simple_prerequisites.sql >/dev/null
pg -v fixture_rows="$fixture_rows" <<'SQL' >/dev/null
ALTER TABLE public.orders RENAME COLUMN total TO payload;
ALTER TABLE public.orders ALTER COLUMN payload TYPE text;
INSERT INTO public.orders SELECT id, 'seed-' || id FROM generate_series(1, :'fixture_rows'::integer) id;
SQL
export PG_ADMIN=postgresql:"//boring_cdc_admin:$admin_password@127.0.0.1:$pg_port/boring_cdc?sslmode=disable"
export PG_RUNTIME=postgresql:"//boring_cdc_runtime:$runtime_password@127.0.0.1:$pg_port/boring_cdc?sslmode=disable"
export PG_CONTROL=postgresql:"//boring_cdc_control_writer:$control_password@127.0.0.1:$pg_port/boring_cdc?sslmode=disable"
mkdir -p "$work/run/state/tmp" "$work/run/state/spool" "$work/run/archive/root"
sed -e 's/total = "numeric:nullable"/payload = "text:nullable"/' \
  -e 's/verify_tls = true/verify_tls = false/' \
  -e 's/unsafe_local_experiment = false/unsafe_local_experiment = true/' \
  -e "s/chunk_rows = 1000/chunk_rows = $chunk_rows/" \
  -e "s#127.0.0.1:8787#127.0.0.1:$status_port#" \
  -e "s#127.0.0.1:8788#127.0.0.1:$metrics_port#" \
  -e "s#https://clickhouse.local:8443#https://127.0.0.1:$ch_https_port#" \
  tests/fixtures/m1_config/representative.toml >"$work/run/boring-cdc.toml"

stage=build_and_init
cargo build --locked --bin boring-cdc
binary="$repo/target/debug/boring-cdc"
cd "$work/run"
confirmation=$(env -u BORING_CDC_POSTGRES_PASSWORD_FILE -u PG_ADMIN -u PG_CONTROL -u PG_RUNTIME -u CH_MAINT -u CH_RUNTIME "$binary" init --dry-run --json)
token=$(jq -er '.data.confirm_token' <<<"$confirmation")
env -u BORING_CDC_POSTGRES_PASSWORD_FILE -u PG_CONTROL -u PG_RUNTIME -u CH_RUNTIME \
  "$binary" init --confirm --confirm-token "$token" --json >"$work/init.json"
unset BORING_CDC_POSTGRES_PASSWORD_FILE PG_ADMIN CH_MAINT

stage=bootstrap
"$binary" run --bootstrap >"$work/connector.log" 2>&1 &
connector_pid=$!
deadline=$((SECONDS+60))
until [[ "$(pg -Atqc "SELECT active FROM pg_replication_slots WHERE slot_name='boring_slot'")" == t ]]; do
  kill -0 "$connector_pid"
  (( SECONDS < deadline ))
  sleep .2
done

stage=concurrent_writes
(
  for i in $(seq 1 "$workload_writes"); do
    PGPASSWORD=$app_password psql -X -h 127.0.0.1 -p "$pg_port" -U boring_cdc_app -d boring_cdc -v ON_ERROR_STOP=1 -q <<SQL
BEGIN;
UPDATE public.orders SET payload='updated-$i' WHERE id=$i;
DELETE FROM public.orders WHERE id=5000+$i;
INSERT INTO public.orders VALUES($fixture_rows+$i,'new-$i');
COMMIT;
SQL
    sleep 5
  done
) &
workload_pid=$!
python3 "$repo/scripts/e2e/article3_bootstrap_progress.py" state/boring.db "$packet/progress.json" "$connector_pid"
"$binary" status --json | jq -e '
  .. | objects | select(has("backfill")) | .backfill
  | del(.progress.run_id)
  | select(.generation > 0 and .progress.total_chunks > 0
      and .progress.completed_rows >= 0 and (.progress | has("eta_ms")))
' >"$work/backfill-status.json"
cp -f "$work/backfill-status.json" "$packet/backfill-status.json"
if [[ "${BCDC_ARTICLE3_BASELINE_OUTAGE:-0}" == 1 ]]; then
  stage=stop_clickhouse_before_baseline
  docker stop "$clickhouse_container" >/dev/null
elif [[ "$scenario" == article3-concurrent-mutation ]]; then
  PGHOST=127.0.0.1 PGPORT=$pg_port PGDATABASE=boring_cdc PGUSER=boring_cdc_app PGPASSWORD=$app_password \
    python3 "$repo/scripts/e2e/article3_mutation_driver.py" write state/boring.db "$packet/mutation-ledger.json"
fi
wait "$workload_pid"
workload_pid=""

if [[ "${BCDC_ARTICLE3_BASELINE_OUTAGE:-0}" == 1 ]]; then
  stage=wait_for_failed_baseline_dispatch
  deadline=$((SECONDS+120))
  until grep -q 'code=M5_SNAPSHOT_READBACK_FAILED' "$work/connector.log"; do
    kill -0 "$connector_pid"
    (( SECONDS < deadline ))
    sleep .2
  done
  python3 "$repo/scripts/e2e/article3_bootstrap_progress.py" state/boring.db "$packet/before-restart.json" "$connector_pid" checkpoint && exit 1
  jq -e '.complete_anchor != null and .clickhouse_checkpoints == []' "$packet/before-restart.json" >/dev/null
  stage=restore_clickhouse_and_restart
  docker start "$clickhouse_container" >/dev/null
  deadline=$((SECONDS+60))
  until ch --query 'SELECT 1' >/dev/null 2>&1; do
    (( SECONDS < deadline ))
    sleep .2
  done
  kill -TERM "$connector_pid"
  wait "$connector_pid"
  "$binary" run >>"$work/connector.log" 2>&1 &
  connector_pid=$!
fi

stage=converge
deadline=$((SECONDS+900))
while ! python3 "$repo/scripts/e2e/article3_bootstrap_progress.py" state/boring.db "$packet/checkpoint.json" "$connector_pid" checkpoint; do
  kill -0 "$connector_pid"
  (( SECONDS < deadline ))
  sleep 1
done

stage=oracle
pg -At -F '|' -c 'SELECT id,payload FROM public.orders ORDER BY id' >"$packet/source-oracle.tsv"
id=$(ch --query 'SELECT DISTINCT logical_table_id FROM boring_cdc.event_history_v1 WHERE capture_epoch=1 AND generation=1')
[[ "$id" =~ ^[0-9a-f]{64}$ ]]
python3 - "$repo/contracts/clickhouse/canonical-query.sql" >"$work/candidate.sql" <<'PY'
import sys
from pathlib import Path
query = Path(sys.argv[1]).read_text()
selector = "WITH selected AS (SELECT generation,promotion_fence FROM boring_cdc.live_generation_v1 WHERE capture_epoch={capture_epoch:UInt64}),"
assert query.count(selector) == 1
print(query.replace(selector, "WITH selected AS (SELECT 1 AS generation,0 AS promotion_fence),"))
PY
docker exec -i "$clickhouse_container" clickhouse-client --user bcdc_probe --password "$ch_password" \
  --param_capture_epoch=1 --param_logical_table_id="$id" --format JSONEachRow \
  --queries-file /dev/stdin <"$work/candidate.sql" >"$packet/candidate-rows.jsonl"
python3 "$repo/scripts/e2e/article3_bootstrap_oracle.py" "$packet/source-oracle.tsv" "$packet/candidate-rows.jsonl" "$packet/oracle.json"
if [[ "$scenario" == article3-concurrent-mutation ]]; then
  python3 "$repo/scripts/e2e/article3_mutation_driver.py" verify state/boring.db \
    "$packet/mutation-ledger.json" "$packet/source-oracle.tsv" "$packet/candidate-rows.jsonl" "$packet/mutation-result.json"
fi
stage=complete
