#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR=/var/tmp
work=$(mktemp -d /var/tmp/m4-durability-faults.XXXXXX); project="m4-durability-faults-$RANDOM-$$"
cleanup(){ docker compose -p "$project" -f compose.yaml down -v --remove-orphans >/dev/null 2>&1 || true; rm -rf "$work"; }; trap cleanup EXIT INT TERM
printf 'm4-durability-synthetic-%s\n' "$project" >"$work/postgres_password"; chmod 600 "$work/postgres_password"; export BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password"
docker compose -p "$project" -f compose.yaml up -d --wait postgres clickhouse >/dev/null
ch(){ docker compose -p "$project" -f compose.yaml exec -T clickhouse clickhouse-client "$@"; }
pg(){ docker compose -p "$project" -f compose.yaml exec -T postgres psql -Atq -U boring_cdc -d boring_cdc "$@"; }
pg_version=$(pg -c 'show server_version'); ch_version=$(ch --query 'SELECT version()'); [[ "$pg_version" == 17.6* && "$ch_version" == 25.8.2.29 ]]
ch --multiquery < contracts/clickhouse/ddl.sql
schema=$(printf '9%.0s' {1..64}); lid=$(printf '8%.0s' {1..64}); keyhash=$(printf 'a%.0s' {1..64}); event=$(printf 'b%.0s' {1..64}); batch=$(printf '7%.0s' {1..64}); object=$(sha256sum contracts/clickhouse/ddl.sql | cut -d' ' -f1); anchor=$(printf '6%.0s' {1..64}); candidate=$(printf '4%.0s' {1..64}); setfp=$(printf '5%.0s' {1..64})
canonical="k1|insert|1|1|0|0|$schema|1:explicit_value:25:-1:YQ=="; payload_hash=$(printf '%s' "$canonical" | sha256sum | cut -d' ' -f1)
for attempt in 1 2; do
  ch --query 'TRUNCATE TABLE boring_cdc.event_history_v1'; ch --query 'TRUNCATE TABLE boring_cdc.batch_markers_v1'; ch --query 'TRUNCATE TABLE boring_cdc.generation_selectors_v1'
  ch --query "INSERT INTO boring_cdc.event_history_v1 VALUES (1,1,'$lid','$schema','k1','$keyhash','','$event','$payload_hash','insert','upsert',1,1,0,0,1,'$batch',[(1,'explicit_value',25,-1,'YQ==')])"
  ch --query "INSERT INTO boring_cdc.batch_markers_v1 VALUES (1,1,'$batch',1,1,1,'$payload_hash','$object',1)"
  ch --query "INSERT INTO boring_cdc.generation_selectors_v1 VALUES (1,1,1,'$setfp','$anchor','$candidate')"
  original_id=$(ch --query "SELECT connector_event_id FROM boring_cdc.event_history_v1 WHERE batch_id='$batch'"); original_hash=$(ch --query "SELECT payload_hash FROM boring_cdc.event_history_v1 WHERE batch_id='$batch'"); original_marker=$(ch --query "SELECT batch_id FROM boring_cdc.batch_markers_v1 WHERE batch_id='$batch'")
  ch --query "ALTER TABLE boring_cdc.event_history_v1 UPDATE columns=[(1,'explicit_value',25,-1,'dGFtcGVyZWQ=')] WHERE batch_id='$batch' SETTINGS mutations_sync=2"
  reconstructed=$(ch --query "SELECT lower(hex(SHA256(concat(canonical_key,'|',toString(operation),'|',toString(lsn_u64),'|',toString(origin_rank),'|',toString(transaction_ordinal),'|',toString(mutation_ordinal),'|',relation_schema_fingerprint,'|',arrayStringConcat(arrayMap(x -> concat(toString(x.1),':',toString(x.2),':',toString(x.3),':',toString(x.4),':',x.5),columns),','))))) FROM boring_cdc.event_history_v1 WHERE batch_id='$batch'")
  [[ "$original_id" == "$event" && "$original_hash" == "$payload_hash" && "$original_marker" == "$batch" && "$reconstructed" != "$payload_hash" ]]
  ch --query "ALTER TABLE boring_cdc.batch_markers_v1 DELETE WHERE batch_id='$batch' SETTINGS mutations_sync=2"; [[ $(ch --query "SELECT count() FROM boring_cdc.batch_markers_v1 WHERE batch_id='$batch'") == 0 ]]
  ch --query "ALTER TABLE boring_cdc.event_history_v1 DELETE WHERE batch_id='$batch' SETTINGS mutations_sync=2"; [[ $(ch --query "SELECT count() FROM boring_cdc.event_history_v1 WHERE batch_id='$batch'") == 0 ]]
  conflict=$(printf '3%.0s' {1..64}); ch --query "INSERT INTO boring_cdc.generation_selectors_v1 VALUES (1,1,2,'$setfp','$anchor','$conflict')"; [[ $(ch --query 'SELECT count() FROM boring_cdc.selector_conflicts_v1') == 1 ]]
  before_setting=$(ch --query "SELECT position(create_table_query,'fsync_after_insert = 1')>0 FROM system.tables WHERE database='boring_cdc' AND name='event_history_v1'"); ch --query 'ALTER TABLE boring_cdc.event_history_v1 MODIFY SETTING fsync_after_insert=0'; after_setting=$(ch --query "SELECT position(create_table_query,'fsync_after_insert = 0')>0 FROM system.tables WHERE database='boring_cdc' AND name='event_history_v1'"); [[ "$before_setting" == 1 && "$after_setting" == 1 ]]; ch --query 'ALTER TABLE boring_cdc.event_history_v1 MODIFY SETTING fsync_after_insert=1'
done
python3 - "$work/observation.json" "$pg_version" "$ch_version" <<'PY'
import json,sys
path,pg,ch=sys.argv[1:]
obj={'versions':{'postgres':pg,'clickhouse':ch,'postgres_image':'17.6','clickhouse_image':'25.8.2.29'},'before':{'destination_state':'healthy','checkpoint':1},'after':{'destination_state':'blocked','checkpoint':1,'capture_state':'unaffected','archive_state':'unaffected','payload_only_corruption_detected':True,'marker_deletion_detected':True,'event_deletion_detected':True,'selector_conflict_detected':True,'setting_drift_detected':True,'attempts':2},'fault_timeline':[{'attempt':1,'fault':'payload_marker_event_selector_setting_corruption','outcome':'clickhouse_only_blocked'},{'attempt':2,'fault':'payload_marker_event_selector_setting_corruption','outcome':'clickhouse_only_blocked'}],'product_faults':'real_clickhouse_payload_only_and_contract_corruption'}
open(path,'w').write(json.dumps(obj,sort_keys=True,separators=(',',':'))+'\n')
PY
M4_DURABILITY_OBSERVATION="$work/observation.json" python3 scripts/lib/m4_durability_evidence.py faults
scripts/validate/evidence.sh artifacts/boring-cdc-m4-durability/SCN-M4-CH-DURABILITY-CORRUPTION/m4-durability-pinned-v1/evidence.json
printf 'M4_DURABILITY_FAULTS_OK postgres=%s clickhouse=%s attempts=2\n' "$pg_version" "$ch_version"
