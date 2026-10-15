#!/usr/bin/env bash
set -euo pipefail
ulimit -c 0
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp

work=$(mktemp -d /var/tmp/m4-live-journal.XXXXXX)
container="m4-live-journal-$RANDOM-$$"
cleanup() {
  docker rm -f "$container" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT INT TERM
python3 - <<'PY' >"$work/password"
import secrets
print(secrets.token_hex(24))
PY
chmod 600 "$work/password"
password=$(cat "$work/password")
printf 'CLICKHOUSE_USER=bcdc_probe\nCLICKHOUSE_PASSWORD=%s\nCLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1\n' "$password" >"$work/clickhouse.env"
chmod 600 "$work/clickhouse.env"
docker run -d --name "$container" --platform linux/amd64 -p 127.0.0.1::8123 --env-file "$work/clickhouse.env" \
  docker.io/clickhouse/clickhouse-server:25.8.2.29@sha256:74c213b4d4cb4854c2497694df0c2d153c041003eadbb0457ae62c28cb8d723f >/dev/null
ch() { docker exec "$container" clickhouse-client --user bcdc_probe --password "$password" "$@"; }
ready=false
for attempt in $(seq 1 90); do
  if ch --query 'SELECT version()' >"$work/version" 2>/dev/null; then ready=true; break; fi
  sleep 1
done
[[ "$ready" == true && "$(cat "$work/version")" == 25.8.2.29 ]]
docker exec -i "$container" clickhouse-client --user bcdc_probe --password "$password" --multiquery < contracts/clickhouse/ddl.sql
endpoint=$(docker port "$container" 8123/tcp)
port=${endpoint##*:}
export BCDC_M4_CLICKHOUSE_URL="http://127.0.0.1:$port" BCDC_M4_CLICKHOUSE_USER=bcdc_probe BCDC_M4_CLICKHOUSE_PASSWORD=$password
export M4_CONVERGENCE_ORACLE_OUT="$work/oracle.json"
export M2_CAPTURE_OBSERVATION_OUT="$work/capture-observation.json"
if ! scripts/acceptance/durable_simple_case.sh >"$work/capture.log" 2>&1; then
  grep -Eo 'M[0-9]_[A-Z0-9_]+|E_[A-Z0-9_]+' "$work/capture.log" | tail -n 20 >&2 || true
  echo M4_LIVE_JOURNAL_CAPTURE_FAILED >&2
  exit 1
fi
grep -q '^M4_LIVE_JOURNAL_OK ' "$work/capture.log"
grep -q '^DURABLE_SIMPLE_CASE_OK ' "$work/capture.log"
[[ "$(ch --query 'SELECT count() FROM boring_cdc.live_generation_v1 WHERE capture_epoch=1')" == 0 ]]
logical_table_id=$(ch --query 'SELECT DISTINCT logical_table_id FROM boring_cdc.event_history_v1 WHERE capture_epoch=1 AND generation=6')
[[ "$logical_table_id" =~ ^[0-9a-f]{64}$ ]]
[[ "$(ch --query 'SELECT count() FROM boring_cdc.event_identity_conflicts_v1 WHERE capture_epoch=1')" == 0 ]]
physical_replays=$(ch --query 'SELECT count() FROM boring_cdc.event_history_v1 WHERE capture_epoch=1 AND generation=3')
logical_events=$(ch --query 'SELECT uniqExact(connector_event_id) FROM boring_cdc.event_history_v1 WHERE capture_epoch=1 AND generation=3')
(( physical_replays > logical_events ))
for generation in 3 6; do
  fence=$((generation == 3 ? 1 : 2))
  ch --query "INSERT INTO boring_cdc.generation_selectors_v1 VALUES (1,$fence,$generation,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc')"
  [[ "$(ch --query 'SELECT count() FROM boring_cdc.selector_conflicts_v1 WHERE capture_epoch=1')" == 0 ]]
  [[ "$(ch --query 'SELECT generation FROM boring_cdc.live_generation_v1 WHERE capture_epoch=1')" == "$generation" ]]
  docker exec -i "$container" clickhouse-client --user bcdc_probe --password "$password" \
    --param_capture_epoch=1 --param_logical_table_id="$logical_table_id" --format JSONEachRow \
    --queries-file /dev/stdin < contracts/clickhouse/canonical-query.sql >"$work/current-$generation.jsonl"
  python3 - "$work/oracle.json" "$work/current-$generation.jsonl" <<'PY'
import base64,json,sys
oracle={int(row['id']):row for row in json.load(open(sys.argv[1]))}
rows=[json.loads(line) for line in open(sys.argv[2]) if line.strip()]
actual={}
for row in rows:
    key=json.loads(row['canonical_key'])[0]['value']
    assert key not in actual,key
    cells={int(cell[0]):cell for cell in row['explicit_cells']}
    def value(column):
        cell=cells[column]
        if cell[1] in ('explicit_null',1): return None
        assert cell[1] in ('explicit_value',3),cell
        encoded=cell[4]
        raw=base64.urlsafe_b64decode(encoded+'='*((-len(encoded))%4))
        if int(cell[2])==20:
            assert len(raw)==8,cell
            return int.from_bytes(raw,'big',signed=True)
        return raw.decode()
    actual[int(key)]={'id':value(0),'total':value(1),'memo':value(2)}
assert actual==oracle,{'expected_keys':sorted(oracle),'actual_keys':sorted(actual)}
assert sorted(actual)==[101,201,202,203,302],sorted(actual)
assert actual[201]['memo']==('x'*10000)
print('M4_CANONICAL_CONVERGENCE_OK generation='+sys.argv[2].split('-')[-1].split('.')[0]+' keys='+','.join(map(str,sorted(actual))))
PY
done
ch --query "INSERT INTO boring_cdc.event_history_v1 SELECT capture_epoch,generation,logical_table_id,relation_schema_fingerprint,canonical_key,key_hash,before_key,connector_event_id,if(payload_hash=repeat('d',64),repeat('e',64),repeat('d',64)),operation,mutation_kind,lsn_u64,origin_rank,transaction_ordinal,mutation_ordinal,journal_seq,batch_id,columns FROM boring_cdc.event_history_v1 WHERE capture_epoch=1 AND generation=6 LIMIT 1"
[[ "$(ch --query 'SELECT count() FROM boring_cdc.event_identity_conflicts_v1 WHERE capture_epoch=1 AND generation=6')" == 1 ]]
if docker exec -i "$container" clickhouse-client --user bcdc_probe --password "$password" \
  --param_capture_epoch=1 --param_logical_table_id="$logical_table_id" --queries-file /dev/stdin \
  < contracts/clickhouse/canonical-query.sql >"$work/conflict.out" 2>"$work/conflict.err"; then
  echo M4_EVENT_CONFLICT_NOT_REJECTED >&2
  exit 1
fi
grep -q 'BCDC_CH_INTEGRITY_CONFLICT' "$work/conflict.err"
ch --query "INSERT INTO boring_cdc.generation_selectors_v1 VALUES (1,2,5,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd')"
[[ "$(ch --query 'SELECT count() FROM boring_cdc.selector_conflicts_v1 WHERE capture_epoch=1 AND promotion_fence=2')" == 1 ]]
[[ "$(ch --query 'SELECT count() FROM boring_cdc.live_generation_v1 WHERE capture_epoch=1')" == 0 ]]
if [[ -n "${M4_CONVERGENCE_OUT:-}" ]]; then
  [[ ! -e "$M4_CONVERGENCE_OUT" ]]
  python3 - "$work/oracle.json" "$M4_CONVERGENCE_OUT" "$physical_replays" "$logical_events" <<'PY'
import hashlib,json,pathlib,subprocess,sys
oracle,out,physical,logical=sys.argv[1:]
def digest(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()
commit=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
report={
  'schema_version':'m4-live-journal-observation/v1',
  'git_commit':commit,
  'command':'scripts/e2e/m4_live_journal_convergence.sh',
  'versions':{'postgres':'17.6','clickhouse':'25.8.2.29'},
  'input_sha256':{name:digest(path) for name,path in {
    'scenario':'scripts/e2e/m4_live_journal_convergence.sh',
    'capture_scenario':'scripts/acceptance/durable_simple_case.sh',
    'clickhouse_ddl':'contracts/clickhouse/ddl.sql',
    'canonical_query':'contracts/clickhouse/canonical-query.sql',
    'clickhouse_model':'contracts/clickhouse/model.json',
  }.items()},
  'postgres_oracle_sha256':digest(oracle),
  'selected_generations':[3,6],
  'final_keys':[101,201,202,203,302],
  'physical_replay_rows':int(physical),
  'logical_event_ids':int(logical),
  'checks':{'typed_oracle':True,'key_change':True,'delete':True,'toast_inheritance':True,'event_conflict_rejected':True,'selector_conflict_rejected':True},
  'selector':'synthetic component probe only; no production M3 anchor or promotion claim',
  'status':'pass',
}
pathlib.Path(out).parent.mkdir(parents=True,exist_ok=True)
pathlib.Path(out).write_text(json.dumps(report,sort_keys=True,indent=2)+'\n')
PY
  cp -f "$M2_CAPTURE_OBSERVATION_OUT" "$(dirname "$M4_CONVERGENCE_OUT")/capture-observation.json"
fi
printf 'M4_LIVE_JOURNAL_CONVERGENCE_OK postgres=17.6 clickhouse=25.8.2.29 replay_physical=%s replay_logical=%s selector=synthetic_component_only\n' "$physical_replays" "$logical_events"
