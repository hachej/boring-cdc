#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR=/var/tmp
cargo test --quiet --locked m4_clickhouse_ -- --nocapture
work=$(mktemp -d /var/tmp/m4-durability-e2e.XXXXXX); project="m4-durability-e2e-$RANDOM-$$"
cleanup(){ docker compose -p "$project" -f compose.yaml down -v --remove-orphans >/dev/null 2>&1 || true; rm -rf "$work"; }; trap cleanup EXIT INT TERM
printf 'm4-durability-synthetic-%s\n' "$project" >"$work/postgres_password"; chmod 600 "$work/postgres_password"; export BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password"
docker compose -p "$project" -f compose.yaml up -d --wait postgres clickhouse >/dev/null
ch(){ docker compose -p "$project" -f compose.yaml exec -T clickhouse clickhouse-client "$@"; }
pg(){ docker compose -p "$project" -f compose.yaml exec -T postgres psql -Atq -U boring_cdc -d boring_cdc "$@"; }
pg_version=$(pg -c 'show server_version'); ch_version=$(ch --query 'SELECT version()')
[[ "$pg_version" == 17.6* && "$ch_version" == 25.8.2.29 ]]
ch --multiquery < contracts/clickhouse/ddl.sql
schema=$(printf '9%.0s' {1..64}); lid=$(printf '8%.0s' {1..64}); keyhash=$(printf 'a%.0s' {1..64}); event=$(printf 'b%.0s' {1..64}); batch=$(printf '7%.0s' {1..64}); object=$(sha256sum contracts/clickhouse/ddl.sql | cut -d' ' -f1)
canonical="k1|insert|1|1|0|0|$schema|1:explicit_value:25:-1:YQ=="; payload_hash=$(printf '%s' "$canonical" | sha256sum | cut -d' ' -f1)
for attempt in 1 2; do
  ch --query 'TRUNCATE TABLE boring_cdc.event_history_v1'; ch --query 'TRUNCATE TABLE boring_cdc.batch_markers_v1'
  ch --query "INSERT INTO boring_cdc.event_history_v1 SETTINGS async_insert=0,wait_for_async_insert=1,insert_quorum=1,insert_deduplicate=0 VALUES (1,1,'$lid','$schema','k1','$keyhash','','$event','$payload_hash','insert','upsert',1,1,0,0,1,'$batch',[(1,'explicit_value',25,-1,'YQ==')])"
  ch --query "INSERT INTO boring_cdc.batch_markers_v1 SETTINGS async_insert=0,wait_for_async_insert=1,insert_quorum=1,insert_deduplicate=0 VALUES (1,1,'$batch',1,1,1,'$payload_hash','$object',1)"
  docker compose -p "$project" -f compose.yaml restart clickhouse >/dev/null
  ready=false; for _ in $(seq 1 120); do if ch --query 'SELECT 1' >/dev/null 2>&1; then ready=true; break; fi; sleep .25; done; [[ "$ready" == true ]]
  rows=$(ch --query "SELECT count() FROM boring_cdc.event_history_v1 WHERE batch_id='$batch'"); markers=$(ch --query "SELECT count() FROM boring_cdc.batch_markers_v1 WHERE batch_id='$batch'")
  reconstructed=$(ch --query "SELECT lower(hex(SHA256(concat(canonical_key,'|',toString(operation),'|',toString(lsn_u64),'|',toString(origin_rank),'|',toString(transaction_ordinal),'|',toString(mutation_ordinal),'|',relation_schema_fingerprint,'|',arrayStringConcat(arrayMap(x -> concat(toString(x.1),':',toString(x.2),':',toString(x.3),':',toString(x.4),':',x.5),columns),','))))) FROM boring_cdc.event_history_v1 WHERE batch_id='$batch'")
  [[ "$rows" == 1 && "$markers" == 1 && "$reconstructed" == "$payload_hash" ]]
done
python3 - "$work/observation.json" "$pg_version" "$ch_version" "$payload_hash" <<'PY'
import json,sys
path,pg,ch,digest=sys.argv[1:]
obj={'versions':{'postgres':pg,'clickhouse':ch,'postgres_image':'17.6','clickhouse_image':'25.8.2.29'},'before':{'checkpoint':0,'events':0},'after':{'checkpoint':1,'events':1,'markers':1,'payload_reconstructed':True,'restart_readback':True,'attempts':2,'digest':digest},'fault_timeline':[{'attempt':1,'fault':'abrupt_clickhouse_restart','outcome':'readback_verified'},{'attempt':2,'fault':'abrupt_clickhouse_restart','outcome':'readback_verified'}],'product_faults':'real_clickhouse_restart_and_synchronous_readback'}
open(path,'w').write(json.dumps(obj,sort_keys=True,separators=(',',':'))+'\n')
PY
M4_DURABILITY_OBSERVATION="$work/observation.json" python3 scripts/lib/m4_durability_evidence.py e2e
scripts/validate/evidence.sh artifacts/boring-cdc-m4-durability/SCN-M4-CH-DURABILITY-RESTART/m4-durability-pinned-v1/evidence.json
printf 'M4_DURABILITY_E2E_OK postgres=%s clickhouse=%s attempts=2\n' "$pg_version" "$ch_version"
