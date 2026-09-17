#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR=/var/tmp
work=$(mktemp -d /var/tmp/m4-clickhouse-ddl.XXXXXX); project="m4-ddl-$RANDOM-$$"
cleanup(){ docker compose -p "$project" -f compose.yaml down -v --remove-orphans >/dev/null 2>&1 || true; rm -rf "$work"; }; trap cleanup EXIT INT TERM
printf 'm4-ddl-synthetic-%s\n' "$project" >"$work/postgres_password"; chmod 600 "$work/postgres_password"; export BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password"
docker compose -p "$project" -f compose.yaml up -d --wait postgres clickhouse >/dev/null
ch(){ docker compose -p "$project" -f compose.yaml exec -T clickhouse clickhouse-client "$@"; }
pg(){ docker compose -p "$project" -f compose.yaml exec -T postgres psql -Atq -U boring_cdc -d boring_cdc "$@"; }
pg_version=$(pg -c 'show server_version'); [[ "$pg_version" == 17.6* ]]
ch_version=$(ch --query 'SELECT version()'); [[ "$ch_version" == 25.8.2.29 ]]
ch --multiquery < contracts/clickhouse/ddl.sql
fingerprint=$(cargo run --quiet --locked --example m4_clickhouse_schema_probe); [[ "$fingerprint" =~ ^[0-9a-f]{64}$ ]]
ch --multiquery --query "CREATE USER boring_cdc_runtime IDENTIFIED WITH no_password; GRANT SELECT ON boring_cdc.* TO boring_cdc_runtime; GRANT INSERT ON boring_cdc.event_history_v1 TO boring_cdc_runtime; GRANT INSERT ON boring_cdc.batch_markers_v1 TO boring_cdc_runtime"
if ch --user boring_cdc_runtime --query 'CREATE TABLE boring_cdc.forbidden(x UInt8) ENGINE=MergeTree ORDER BY x' >/dev/null 2>&1; then echo E_RUNTIME_DDL_GRANTED >&2; exit 1; fi
if ch --user boring_cdc_runtime --query 'ALTER TABLE boring_cdc.event_history_v1 DROP PARTITION tuple(1,7)' >/dev/null 2>&1; then echo E_RUNTIME_ALTER_GRANTED >&2; exit 1; fi
lid=$(printf '8%.0s' {1..64}); schema=$(printf '9%.0s' {1..64}); keyhash=$(printf 'a%.0s' {1..64}); batch=$(printf '7%.0s' {1..64}); anchor=$(printf '6%.0s' {1..64}); setfp=$(printf '5%.0s' {1..64}); candidate=$(printf '4%.0s' {1..64})
ch --query "INSERT INTO boring_cdc.generation_selectors_v1 VALUES (1,9,7,'$setfp','$anchor','$candidate')"
# Seed two current rows, one TOAST patch, and one tombstone. Each insert is a distinct part.
for sql in \
"(1,7,'$lid','$schema','k1','$keyhash','','$(printf '1%.0s' {1..64})','$(printf 'b%.0s' {1..64})','insert','upsert',1,1,0,0,1,'$batch',[(1,'explicit_value',25,-1,'YQ==')])" \
"(1,7,'$lid','$schema','k1','$keyhash','','$(printf '2%.0s' {1..64})','$(printf 'c%.0s' {1..64})','update','upsert',2,1,0,0,2,'$batch',[(1,'unchanged_toast',25,-1,'')])" \
"(1,7,'$lid','$schema','gone','$keyhash','','$(printf '3%.0s' {1..64})','$(printf 'd%.0s' {1..64})','insert','upsert',1,1,0,0,3,'$batch',[(1,'explicit_value',25,-1,'eA==')])" \
"(1,7,'$lid','$schema','gone','$keyhash','','$(printf '4%.0s' {1..64})','$(printf 'e%.0s' {1..64})','delete','delete',2,1,0,0,4,'$batch',[])"; do
  ch --query "INSERT INTO boring_cdc.event_history_v1 VALUES $sql"
done
query_file=contracts/clickhouse/canonical-query.sql
current(){ ch --user boring_cdc_runtime --param_capture_epoch=1 --param_logical_table_id="$lid" --query "$(cat "$query_file")"; }
# The history interface is exercised directly with its exact ordering and without FINAL.
history_out=$(ch --user boring_cdc_runtime --query "SELECT connector_event_id FROM boring_cdc.event_history_v1 WHERE capture_epoch=1 AND generation=7 AND logical_table_id='$lid' ORDER BY canonical_key,lsn_u64,origin_rank,transaction_ordinal,mutation_ordinal,connector_event_id")
[[ $(wc -l <<<"$history_out") -eq 4 ]]
before=$(current); [[ "$before" == *k1* && "$before" == *YQ==* && "$before" != *gone* ]]
before_digest=$(printf '%s' "$before" | sha256sum | cut -d' ' -f1)
# Hold many physical parts, then force a real merge and query while system.merges reports it.
ch --query 'SYSTEM STOP MERGES boring_cdc.event_history_v1'
for part in $(seq 1 24); do
  ch --query "INSERT INTO boring_cdc.event_history_v1 SELECT 1,7,'$lid','$schema','k1','$keyhash','',lower(hex(SHA256(concat(toString($part),':',toString(number))))),lower(hex(SHA256(concat('p:',toString($part),':',toString(number))))),'update','upsert',toUInt64(100+$part),toUInt8(1),toUInt64(number),toUInt8(0),toUInt64(10000+$part*2000+number),'$batch',[(toUInt32(1),'explicit_value',toUInt32(25),toInt32(-1),'YQ==')] FROM numbers(2000) SETTINGS max_insert_threads=1"
done
stopped=$(current); stopped_digest=$(printf '%s' "$stopped" | sha256sum | cut -d' ' -f1); [[ "$stopped_digest" == "$before_digest" ]]
ch --query 'SYSTEM START MERGES boring_cdc.event_history_v1'
ch --query 'OPTIMIZE TABLE boring_cdc.event_history_v1 FINAL' >"$work/optimize.out" 2>"$work/optimize.err" & optimize_pid=$!
merge_observed=false; during=''
for _ in $(seq 1 200); do
  if [[ $(ch --query "SELECT count() FROM system.merges WHERE database='boring_cdc' AND table='event_history_v1'") -gt 0 ]]; then merge_observed=true; during=$(current); break; fi
  kill -0 "$optimize_pid" 2>/dev/null || break; sleep .05
done
wait "$optimize_pid"; [[ "$merge_observed" == true && -n "$during" ]]
during_digest=$(printf '%s' "$during" | sha256sum | cut -d' ' -f1); after=$(current); after_digest=$(printf '%s' "$after" | sha256sum | cut -d' ' -f1)
[[ "$before_digest" == "$during_digest" && "$during_digest" == "$after_digest" ]]
parts_after=$(ch --query "SELECT count() FROM system.parts WHERE active AND database='boring_cdc' AND table='event_history_v1'")
objects=$(ch --query "SELECT count() FROM system.tables WHERE database='boring_cdc' AND name IN ('event_history_v1','batch_markers_v1','generation_selectors_v1','selector_conflicts_v1','live_generation_v1','event_identity_conflicts_v1')")
[[ "$objects" == 6 ]]
commit=$(git rev-parse HEAD); mkdir -p artifacts/boring-cdc-m4-ddl/SCN-M4-CH-MERGE-INVARIANT
python3 - "$commit" "$pg_version" "$ch_version" "$fingerprint" "$before_digest" "$during_digest" "$after_digest" "$parts_after" > artifacts/boring-cdc-m4-ddl/SCN-M4-CH-MERGE-INVARIANT/evidence.json <<'PY'
import json,sys
commit,pg,ch,fp,before,during,after,parts=sys.argv[1:]
print(json.dumps({'schema_version':'m4-clickhouse-ddl-evidence/v1','git_commit':commit,'images':{'postgres':'17.6','clickhouse':'25.8.2.29'},'observed_versions':{'postgres':pg,'clickhouse':ch},'object_fingerprint':fp,'objects_verified':6,'ordinary_runtime':{'ddl_denied':True,'alter_denied':True,'canonical_select_allowed':True},'history_interface':{'rows':4,'ordered_without_final':True},'merge_invariant':{'system_merges_observed':True,'before_sha256':before,'stopped_sha256':before,'during_sha256':during,'after_sha256':after,'active_parts_after':int(parts)},'credentials_recorded':False,'status':'pass'},sort_keys=True,indent=2)+'\n')
PY
python3 scripts/validate/m4_clickhouse_ddl.py
printf 'M4_CLICKHOUSE_DDL_E2E_OK postgres=%s clickhouse=%s fingerprint=%s merge_observed=true\n' "$pg_version" "$ch_version" "$fingerprint"
