#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR=/var/tmp
cargo test --quiet --locked m4_mutations::tests -- --nocapture
work=$(mktemp -d /var/tmp/m4-mutations-e2e.XXXXXX); project="m4-mutations-e2e-$RANDOM-$$"
cleanup(){ docker compose -p "$project" -f compose.yaml down -v --remove-orphans >/dev/null 2>&1 || true; rm -rf "$work"; }; trap cleanup EXIT INT TERM
printf 'm4-mutations-synthetic-%s\n' "$project" >"$work/postgres_password"; chmod 600 "$work/postgres_password"; export BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password"
docker compose -p "$project" -f compose.yaml up -d --wait postgres clickhouse >/dev/null
ch(){ docker compose -p "$project" -f compose.yaml exec -T clickhouse clickhouse-client "$@"; }; pg(){ docker compose -p "$project" -f compose.yaml exec -T postgres psql -Atq -U boring_cdc -d boring_cdc "$@"; }
pg_version=$(pg -c 'show server_version'); ch_version=$(ch --query 'SELECT version()'); [[ "$pg_version" == 17.6* && "$ch_version" == 25.8.2.29 ]]
pg <<'SQL'
CREATE TABLE mutation_fixture(id bytea PRIMARY KEY,value text); INSERT INTO mutation_fixture VALUES ('\x00aa','old'); UPDATE mutation_fixture SET id='\x00bb',value='new' WHERE id='\x00aa'; DELETE FROM mutation_fixture WHERE id='\x00bb'; INSERT INTO mutation_fixture VALUES ('\x00bb','fresh');
SQL
[[ $(pg -c "SELECT encode(id,'hex')||':'||value FROM mutation_fixture") == '00bb:fresh' ]]
ch --multiquery < contracts/clickhouse/ddl.sql
schema=$(printf '9%.0s' {1..64}); lid=$(printf '8%.0s' {1..64}); kh=$(printf 'a%.0s' {1..64}); batch=$(printf '7%.0s' {1..64}); setfp=$(printf '5%.0s' {1..64}); anchor=$(printf '6%.0s' {1..64}); candidate=$(printf '4%.0s' {1..64})
row(){ local key=$1 idc=$2 hashc=$3 op=$4 kind=$5 lsn=$6 ord=$7 seq=$8 val=$9 before=${10:-}; local eid ph; eid=$(printf "$idc%.0s" {1..64}); ph=$(printf "$hashc%.0s" {1..64}); ch --query "INSERT INTO boring_cdc.event_history_v1 SETTINGS async_insert=0,wait_for_async_insert=1,insert_quorum=1,insert_deduplicate=0 VALUES (1,1,'$lid','$schema','$key','$kh','$before','$eid','$ph','$op','$kind',$lsn,1,1,$ord,$seq,'$batch',[(1,'explicit_value',25,-1,'$val')])"; }
for attempt in 1 2; do
 ch --query 'TRUNCATE TABLE boring_cdc.event_history_v1'; ch --query 'TRUNCATE TABLE boring_cdc.generation_selectors_v1'
 ch --query "INSERT INTO boring_cdc.generation_selectors_v1 VALUES (1,1,1,'$setfp','$anchor','$candidate')"
 row old b 1 insert upsert 10 0 1 b2xk; row old c 2 delete delete 20 0 2 ''; row new d 3 update upsert 20 1 3 bmV3 old; row new e 4 update upsert 30 0 4 ZnJlc2g= old; row new e 4 update upsert 30 0 4 ZnJlc2g= old
 physical=$(ch --query 'SELECT count() FROM boring_cdc.event_history_v1'); logical=$(ch --query 'SELECT uniqExact(connector_event_id) FROM boring_cdc.event_history_v1'); conflicts=$(ch --query 'SELECT count() FROM boring_cdc.event_identity_conflicts_v1'); tombstones=$(ch --query "SELECT count() FROM boring_cdc.event_history_v1 WHERE mutation_kind='delete'")
 [[ "$physical" == 5 && "$logical" == 4 && "$conflicts" == 0 && "$tombstones" == 1 ]]
 query=$(sed "s/{capture_epoch:UInt64}/1/g;s/{logical_table_id:FixedString(64)}/'$lid'/g" contracts/clickhouse/canonical-query.sql); before=$(ch --query "$query"); [[ "$before" == new* && "$before" == *ZnJlc2g=* ]]
 ch --query 'OPTIMIZE TABLE boring_cdc.event_history_v1 FINAL'; after=$(ch --query "$query"); [[ "$after" == "$before" ]]
done
python3 - "$work/observation.json" "$pg_version" "$ch_version" <<'PY'
import json,sys
path,pg,ch=sys.argv[1:]; json.dump({'versions':{'postgres':pg,'clickhouse':ch,'postgres_image':'17.6','clickhouse_image':'25.8.2.29'},'before':{'checkpoint':0,'source_rows':1},'after':{'checkpoint':4,'physical_attempts':5,'logical_events':4,'duplicate_attempts':1,'duplicate_id_conflicts':0,'tombstones':1,'canonical_rows':['new:fresh'],'before_after_merge_equal':True,'attempts':2},'fault_timeline':[{'attempt':1,'fault':'replay_before_checkpoint_and_merge','outcome':'converged'},{'attempt':2,'fault':'replay_before_checkpoint_and_merge','outcome':'converged'}],'product_faults':'real_postgresql_key_change_delete_reinsert_and_clickhouse_duplicate_replay_merge'},open(path,'w'),sort_keys=True,separators=(',',':'))
PY
M4_MUTATIONS_OBSERVATION="$work/observation.json" python3 scripts/lib/m4_mutations_evidence.py e2e
scripts/validate/evidence.sh artifacts/boring-cdc-m4-mutations/SCN-M4-MUTATION-UPDATE-DELETE-KEY-CHANGE/m4-mutations-pinned-v1/evidence.json
printf 'M4_MUTATIONS_E2E_OK postgres=%s clickhouse=%s duplicate_id_conflicts=0 attempts=2\n' "$pg_version" "$ch_version"
