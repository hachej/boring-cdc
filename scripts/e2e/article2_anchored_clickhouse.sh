#!/usr/bin/env bash
set -euo pipefail
ulimit -c 0
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp

work=$(mktemp -d /var/tmp/article2-anchored-clickhouse.XXXXXX)
project="article2-anchor-$RANDOM-$$"
port=$((54000+$$%1000))
container=""
cleanup(){
  if [[ -n "$container" ]]; then docker rm -f "$container" >/dev/null 2>&1 || true; fi
  docker compose -p "$project" -f compose.yaml -f "$work/override.yml" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT INT TERM
python3 - <<'PY' >"$work/postgres_password"
import secrets
print(secrets.token_hex(24))
PY
python3 - <<'PY' >"$work/clickhouse_password"
import secrets
print(secrets.token_hex(24))
PY
chmod 600 "$work/postgres_password" "$work/clickhouse_password"
export BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password"
export PGPASSWORD
PGPASSWORD=$(cat "$BORING_CDC_POSTGRES_PASSWORD_FILE")
cat >"$work/override.yml" <<YAML
services:
  postgres:
    ports: ["127.0.0.1:${port}:5432"]
YAML
docker compose -p "$project" -f compose.yaml -f "$work/override.yml" up -d --wait postgres >/dev/null
export BORING_CDC_M3_DSN
BORING_CDC_M3_DSN=$(printf 'postgresql://%s@127.0.0.1:%s/boring_cdc?sslmode=disable' "boring_cdc:${PGPASSWORD}" "$port")

password=$(cat "$work/clickhouse_password")
printf 'CLICKHOUSE_USER=bcdc_probe\nCLICKHOUSE_PASSWORD=%s\nCLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1\n' "$password" >"$work/clickhouse.env"
chmod 600 "$work/clickhouse.env"
for attempt in 1 2; do
  container="article2-clickhouse-$RANDOM-$$-$attempt"
  docker run -d --name "$container" --platform linux/amd64 -p 127.0.0.1::8123 --env-file "$work/clickhouse.env" \
    docker.io/clickhouse/clickhouse-server:25.8.2.29@sha256:74c213b4d4cb4854c2497694df0c2d153c041003eadbb0457ae62c28cb8d723f >/dev/null
  ch(){ docker exec "$container" clickhouse-client --user bcdc_probe --password "$password" "$@"; }
  ready=false
  for probe in $(seq 1 90); do
    if ch --query 'SELECT version()' >"$work/version" 2>/dev/null; then ready=true; break; fi
    sleep 1
  done
  [[ "$ready" == true && "$(cat "$work/version")" == 25.8.2.29 ]]
  docker exec -i "$container" clickhouse-client --user bcdc_probe --password "$password" --multiquery < contracts/clickhouse/ddl.sql
  endpoint=$(docker port "$container" 8123/tcp)
  export BCDC_M4_CLICKHOUSE_URL="http://127.0.0.1:${endpoint##*:}"
  export BCDC_M4_CLICKHOUSE_USER=bcdc_probe BCDC_M4_CLICKHOUSE_PASSWORD="$password"
  export BCDC_ARTICLE2_SAME_STREAM=1 BORING_CDC_M3_SLOT="boring_cdc_article2_$attempt"
  export BCDC_ARTICLE2_ORACLE_OUT="$work/oracle-$attempt.json"
  cargo test --locked m3_bootstrap::live_tests::exported_snapshot_uses_distinct_command_idle_sessions -- --ignored --exact
  [[ "$(ch --query 'SELECT count() FROM boring_cdc.generation_selectors_v1 WHERE capture_epoch=1')" == 0 ]]
  [[ "$(ch --query "SELECT count() FROM boring_cdc.event_history_v1 WHERE capture_epoch=1 AND generation=1 AND operation='snapshot' AND journal_seq=0")" == 1 ]]
  [[ "$(ch --query 'SELECT count() FROM boring_cdc.batch_markers_v1 WHERE capture_epoch=1 AND generation=1 AND first_journal_seq=0 AND last_journal_seq=0')" == 1 ]]
  [[ "$(ch --query 'SELECT count() FROM boring_cdc.event_identity_conflicts_v1 WHERE capture_epoch=1 AND generation=1')" == 0 ]]
  logical_table_id=$(ch --query 'SELECT DISTINCT logical_table_id FROM boring_cdc.event_history_v1 WHERE capture_epoch=1 AND generation=1')
  [[ "$logical_table_id" =~ ^[0-9a-f]{64}$ ]]
  python3 - "$work/candidate-query.sql" <<'PY'
from pathlib import Path
import sys
query=Path('contracts/clickhouse/canonical-query.sql').read_text()
old='WITH selected AS (SELECT generation,promotion_fence FROM boring_cdc.live_generation_v1 WHERE capture_epoch={capture_epoch:UInt64}),'
assert query.startswith('/* Canonical current state; FINAL is intentionally absent. Run the two conflict views as a mandatory preflight. */\n'+old)
Path(sys.argv[1]).write_text(query.replace(old,'WITH selected AS (SELECT 1 AS generation,0 AS promotion_fence),',1))
PY
  docker exec -i "$container" clickhouse-client --user bcdc_probe --password "$password" \
    --param_capture_epoch=1 --param_logical_table_id="$logical_table_id" --format JSONEachRow \
    --queries-file /dev/stdin < "$work/candidate-query.sql" >"$work/current-$attempt.jsonl"
  python3 - "$work/oracle-$attempt.json" "$work/current-$attempt.jsonl" <<'PY'
import base64,json,sys
oracle=json.load(open(sys.argv[1]))
rows=[json.loads(line) for line in open(sys.argv[2]) if line.strip()]
assert len(rows)==1,len(rows)
row=rows[0]
assert json.loads(row['canonical_key'])[0]['value']==oracle['id']
cells={int(cell[0]):cell for cell in row['explicit_cells']}
def value(column):
    cell=cells[column]
    assert cell[1] in ('explicit_value',3),cell
    encoded=cell[4]
    raw=base64.urlsafe_b64decode(encoded+'='*((-len(encoded))%4))
    return int.from_bytes(raw,'big',signed=True) if int(cell[2])==20 else raw.decode()
assert (value(0),value(1))==(oracle['id'],oracle['payload'])
assert oracle['checkpoint']==4 and oracle['anchor_id']=='b'*64 and oracle['generation']==1
PY
  docker rm -f "$container" >/dev/null
  container=""
done
[[ "$(docker compose -p "$project" -f compose.yaml -f "$work/override.yml" exec -T postgres psql -At -U boring_cdc -d boring_cdc -c 'show server_version')" == 17.6* ]]
if [[ -n "${BCDC_ARTICLE2_OBSERVATION_OUT:-}" ]]; then
  [[ ! -e "$BCDC_ARTICLE2_OBSERVATION_OUT" ]]
  python3 - "$work" "$BCDC_ARTICLE2_OBSERVATION_OUT" <<'PY'
import hashlib,json,pathlib,subprocess,sys
work,out=map(pathlib.Path,sys.argv[1:])
def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
inputs=['scripts/e2e/article2_anchored_clickhouse.sh','src/m2_capture_runtime.rs','src/m2_event_envelope.rs','src/m3_bootstrap.rs','src/m4_initial_load.rs','src/m4_clickhouse_durability.rs','src/m4_clickhouse_wire.rs','contracts/clickhouse/model.json','contracts/clickhouse/ddl.sql','contracts/clickhouse/canonical-query.sql']
report={
 'schema_version':'article2-anchored-clickhouse-observation/v1',
 'git_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
 'command':'scripts/e2e/article2_anchored_clickhouse.sh',
 'status':'pass','runtime_observed':True,'evidence_tier':'component',
 'versions':{'postgres':'17.6','clickhouse':'25.8.2.29'},
 'repeats':2,
 'input_sha256':{path:digest(pathlib.Path(path)) for path in inputs},
 'oracle_sha256':[digest(work/f'oracle-{attempt}.json') for attempt in (1,2)],
 'candidate_query_sha256':[digest(work/f'current-{attempt}.jsonl') for attempt in (1,2)],
 'checks':{'complete_anchor':True,'same_stream_m3_snapshot_and_m2_tail':True,'snapshot_batch_readback':True,'child_abort_after_remote_before_checkpoint':True,'restart_adoption':True,'anchor_bound_checkpoint':True,'independent_source_oracle':True,'selector_absent':True},
 'scope':'one-relation initial-slot ClickHouse candidate; no selector promotion or existing-slot reseed',
}
out.parent.mkdir(parents=True,exist_ok=True)
out.write_text(json.dumps(report,sort_keys=True,indent=2)+'\n')
PY
fi
printf 'ARTICLE2_ANCHORED_CLICKHOUSE_OK postgres=17.6 clickhouse=25.8.2.29 repeats=2 selector=candidate_only\n'
