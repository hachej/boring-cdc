#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp
cargo test --locked m3_fence::tests
work=$(mktemp -d /var/tmp/m3-fence-e2e.XXXXXX)
project="m3-fence-$RANDOM-$$"
port=$((56000+$$%800))
cleanup(){ docker compose -p "$project" -f compose.yaml -f "$work/override.yml" down -v --remove-orphans >/dev/null 2>&1||true;rm -rf "$work"; }
trap cleanup EXIT INT TERM
python3 - <<'PY' >"$work/postgres_password"
import secrets
print(secrets.token_hex(24))
PY
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
psql_cmd=(psql -X -v ON_ERROR_STOP=1 -At -h 127.0.0.1 -p "$port" -U boring_cdc -d boring_cdc)
[[ "$("${psql_cmd[@]}" -c 'show server_version')" == 17.6* ]]
for attempt in 1 2; do
  slot="m3_fence_${attempt}"
  pub="m3_fence_pub_${attempt}"
  "${psql_cmd[@]}" <<SQL >/dev/null
DROP SCHEMA IF EXISTS boring_cdc_control CASCADE;
CREATE SCHEMA boring_cdc_control;
CREATE TABLE boring_cdc_control.capture_fences(
 id text PRIMARY KEY CHECK(id='singleton'), capture_epoch bigint NOT NULL,
 generation bigint NOT NULL, table_set_fingerprint text NOT NULL, unique_nonce bigint NOT NULL);
INSERT INTO boring_cdc_control.capture_fences VALUES('singleton',0,0,repeat('0',64),0);
CREATE PUBLICATION ${pub} FOR TABLE boring_cdc_control.capture_fences WITH (publish='update');
SELECT * FROM pg_create_logical_replication_slot('${slot}','pgoutput');
SQL
  update_result=$("${psql_cmd[@]}" -c "UPDATE boring_cdc_control.capture_fences SET capture_epoch=1,generation=1,table_set_fingerprint=repeat('a',64),unique_nonce=700000000000000007 WHERE id='singleton'")
  [[ "$update_result" == "UPDATE 1" ]]
  "${psql_cmd[@]}" -F '|' -c "SELECT lsn::text,xid,encode(data,'hex') FROM pg_logical_slot_peek_binary_changes('${slot}',NULL,NULL,'proto_version','1','publication_names','${pub}')" >"$work/pgoutput-$attempt.txt"
  python3 - "$work/pgoutput-$attempt.txt" "$work/observation-$attempt.json" <<'PY'
import json,pathlib,sys
rows=[]
for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    lsn,xid,raw=line.split('|',2);data=bytes.fromhex(raw);rows.append((lsn,xid,data))
commits=[data for _,_,data in rows if data[:1]==b'C']
assert commits and any(b'700000000000000007' in data for _,_,data in rows)
commit=commits[-1]
assert len(commit)>=26
end_lsn=int.from_bytes(commit[10:18],'big')
assert end_lsn>0
pathlib.Path(sys.argv[2]).write_text(json.dumps({'affected_rows':1,'commit_end_lsn':f'{end_lsn:016X}','pgoutput_contains_nonce':True,'pgoutput_message_count':len(rows),'pgoutput_messages':[{'lsn':lsn,'data_hex':data.hex()} for lsn,_,data in rows]},sort_keys=True))
PY
  BORING_CDC_M3_FENCE_OBSERVATION="$work/observation-$attempt.json" BORING_CDC_M3_FENCE_RESULT="$work/result-$attempt.json" cargo test --locked m3_fence::tests::live_pgoutput_observation_uses_commit_message_end_lsn -- --exact
  "${psql_cmd[@]}" -c "SELECT pg_drop_replication_slot('${slot}')" >/dev/null
done
python3 - "$work/result-1.json" "$work/result-2.json" "$work/observation-1.json" "$work/observation-2.json" >"$work/result.json" <<'PY'
import json,pathlib,sys
results=[json.loads(pathlib.Path(p).read_text()) for p in sys.argv[1:3]]
observed=[json.loads(pathlib.Path(p).read_text()) for p in sys.argv[3:]]
for result in results:
 assert result['anchor_state']=='complete' and result['first_proof'] and result['post_copy_fence_seq']==6 and result['pgoutput_contains_nonce'] and result['m2_encoded_row_from_live_pgoutput'] and result['affected_rows']==1
assert all(x['pgoutput_contains_nonce'] and x['affected_rows']==1 and x['pgoutput_message_count']>=3 for x in observed)
print(json.dumps({'anchor_state':'complete','first_proof':True,'post_copy_fence_seq':6,'pgoutput_contains_nonce':True,'m2_encoded_row_from_live_pgoutput':True,'affected_rows':1,'deterministic_attempts':2,'commit_end_lsns':[x['commit_end_lsn'] for x in observed]},sort_keys=True))
PY
BORING_CDC_M3_FENCE_OBSERVATION="$work/result.json" python3 scripts/lib/m3_fence_evidence.py e2e
scripts/validate/evidence.sh artifacts/boring-cdc-m3-fence/SCN-M3-FENCE-PGOUTPUT/fence-pg17-v1/evidence.json
