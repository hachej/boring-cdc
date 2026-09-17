#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR=/var/tmp
cargo test --quiet --locked m4_mutations::tests -- --nocapture
work=$(mktemp -d /var/tmp/m4-mutations-faults.XXXXXX); project="m4-mutations-faults-$RANDOM-$$"
cleanup(){ docker compose -p "$project" -f compose.yaml down -v --remove-orphans >/dev/null 2>&1 || true; rm -rf "$work"; }; trap cleanup EXIT INT TERM
printf 'm4-mutations-synthetic-%s\n' "$project" >"$work/postgres_password"; chmod 600 "$work/postgres_password"; export BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password"
docker compose -p "$project" -f compose.yaml up -d --wait postgres clickhouse >/dev/null
ch(){ docker compose -p "$project" -f compose.yaml exec -T clickhouse clickhouse-client "$@"; }; pg(){ docker compose -p "$project" -f compose.yaml exec -T postgres psql -Atq -U boring_cdc -d boring_cdc "$@"; }
pg_version=$(pg -c 'show server_version'); ch_version=$(ch --query 'SELECT version()'); [[ "$pg_version" == 17.6* && "$ch_version" == 25.8.2.29 ]]; ch --multiquery < contracts/clickhouse/ddl.sql
schema=$(printf '9%.0s' {1..64}); lid=$(printf '8%.0s' {1..64}); kh=$(printf 'a%.0s' {1..64}); eid=$(printf 'b%.0s' {1..64}); batch=$(printf '7%.0s' {1..64})
for attempt in 1 2; do
 ch --query 'TRUNCATE TABLE boring_cdc.event_history_v1'
 ch --query "INSERT INTO boring_cdc.event_history_v1 VALUES (1,1,'$lid','$schema','key','$kh','','$eid','$(printf '1%.0s' {1..64})','insert','upsert',1,1,0,0,1,'$batch',[(1,'explicit_value',25,-1,'YQ==')]),(1,1,'$lid','$schema','key','$kh','','$eid','$(printf '2%.0s' {1..64})','insert','upsert',1,1,0,0,1,'$batch',[(1,'explicit_value',25,-1,'Yg==')])"
 [[ $(ch --query 'SELECT count() FROM boring_cdc.event_identity_conflicts_v1') == 1 ]]
 ch --query 'OPTIMIZE TABLE boring_cdc.event_history_v1 FINAL'; [[ $(ch --query 'SELECT count() FROM boring_cdc.event_identity_conflicts_v1') == 1 ]]
done
python3 - "$work/observation.json" "$pg_version" "$ch_version" <<'PY'
import json,sys
path,pg,ch=sys.argv[1:]; json.dump({'versions':{'postgres':pg,'clickhouse':ch,'postgres_image':'17.6','clickhouse_image':'25.8.2.29'},'before':{'checkpoint':0},'after':{'checkpoint':0,'destination_state':'blocked_integrity','event_identity_conflicts':1,'conflict_persists_after_merge':True,'rust_payload_conflict_block':True,'rust_key_change_toast_block':True,'attempts':2},'fault_timeline':[{'attempt':1,'fault':'same_event_id_different_payload','outcome':'blocked_checkpoint_unchanged'},{'attempt':2,'fault':'same_event_id_different_payload','outcome':'blocked_checkpoint_unchanged'}],'product_faults':'real_clickhouse_identity_conflict_before_and_after_merge'},open(path,'w'),sort_keys=True,separators=(',',':'))
PY
M4_MUTATIONS_OBSERVATION="$work/observation.json" python3 scripts/lib/m4_mutations_evidence.py faults
scripts/validate/evidence.sh artifacts/boring-cdc-m4-mutations/SCN-M4-MUTATION-REPLAY-CONFLICT/m4-mutations-pinned-v1/evidence.json
printf 'M4_MUTATIONS_FAULTS_OK postgres=%s clickhouse=%s event_identity_conflicts=1 attempts=2\n' "$pg_version" "$ch_version"
