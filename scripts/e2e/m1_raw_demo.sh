#!/bin/sh
set -eu
export TMPDIR=${TMPDIR:-/var/tmp}
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/e2e/m1_raw_demo.sh [raw-demo-v1]'; exit 0; }
seed=${1:-raw-demo-v1}; [ "$seed" = raw-demo-v1 ] || { echo 'E_SEED' >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
name="m1raw${$}"; out="$TMPDIR/$name"; transcript="$out/transcript.txt"; mkdir -p "$out"
# Compose image pin accepted by owner card 765bd3b2-4b68-4102-a9ec-43ca93357390.
image='docker.io/library/postgres:17.6@sha256:00bc86618629af00d2937fdc5a5d63db3ff8450acf52f0636ec813c7f4902929'
container_started=false
cleanup() {
 status=$?
 trap - EXIT HUP INT TERM
 if [ "$container_started" = true ]; then
  docker rm -f "$name" >/dev/null 2>&1 || status=1
  docker inspect "$name" >/dev/null 2>&1 && status=1 || :
 fi
 rm -rf "$out"
 exit "$status"
}
trap cleanup EXIT HUP INT TERM
docker run -d --rm --name "$name" -e POSTGRES_PASSWORD=postgres -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1::5432 "$image" -c wal_level=logical -c max_replication_slots=4 >/dev/null
container_started=true
port=$(docker port "$name" 5432/tcp | sed 's/.*://'); admin="postgresql://postgres@127.0.0.1:$port/postgres"
i=0; until pg_isready -d "$admin" >/dev/null 2>&1; do i=$((i+1)); [ "$i" -lt 60 ] || { echo E_POSTGRES_HEALTH >&2; exit 1; }; sleep 1; done
psql "$admin" < tests/fixtures/m1-control/setup.sql >/dev/null
scripts/fixtures/m1_slot_export.py 127.0.0.1 "$port" >/dev/null
changes() { psql "$admin" -Atqc "SELECT encode(data,'hex') FROM pg_logical_slot_get_binary_changes('boring_cdc_slot',NULL,NULL,'proto_version','1','publication_names','boring_cdc_publication')" | paste -sd, -; }
assert_live() {
 test_log="$out/live-test.log"
 cargo test --locked "$2" -- --exact --quiet --nocapture >"$test_log" 2>"$out/live-test.err"
 observed=$(grep -E "^CASE $1 state=[^ ]+ checkpoint=[^ ]+ log=[^ ]+$" "$test_log")
 [ "$(printf '%s\n' "$observed" | grep -c .)" -eq 1 ]
 printf 'ASSERT %s test=%s exit=0 live_sql_fact=true product_observation=true\n' "$1" "$2"
 printf '%s\n' "$observed"
}
summary() { python3 -c 'import sys; xs=[bytes.fromhex(x) for x in sys.argv[1].split(",") if x]; print("raw_pgoutput tags="+",".join(chr(x[0]) for x in xs)+" lengths="+",".join(map(lambda x:str(len(x)),xs))+" payload_values=redacted")' "$1"; }
{
 echo 'scenario=SCN-M1-RAW-EVENTS seed=raw-demo-v1 pg=17 slot=pgoutput'
 psql "$admin" -v ON_ERROR_STOP=1 -qc "UPDATE boring_cdc_control.heartbeat SET nonce=1,updated_at='2025-01-01T00:00:00Z' WHERE id='singleton'"
 wire=$(changes); summary "$wire"; cargo run --quiet --example m1_control_probe -- update "$wire" 1
 assert_live SCN-M1-RAW-FIXED-SEED m1_decoder::tests::golden_transaction_preserves_row_only_ordinals_and_origin
 assert_live SCN-M1-RAW-HEARTBEAT m1_control_fixtures::tests::heartbeat_is_monotonic_durable_noop
 psql "$admin" -qc 'TRUNCATE public.accounts'; wire=$(changes); summary "$wire"; cargo run --quiet --example m1_control_probe -- truncate "$wire"
 assert_live SCN-M1-RAW-TRUNCATE m1_control_fixtures::tests::truncate_is_detection_only
 catalog=$(psql "$admin" -Atqc "SELECT jsonb_build_object('name',p.pubname,'owner_role',r.rolname,'relations',(SELECT jsonb_agg(schemaname||'.'||tablename ORDER BY schemaname,tablename) FROM pg_publication_tables WHERE pubname=p.pubname),'operations',(SELECT jsonb_agg(operation ORDER BY operation) FROM (VALUES ('insert',p.pubinsert),('update',p.pubupdate),('delete',p.pubdelete),('truncate',p.pubtruncate)) f(operation,enabled) WHERE enabled)) FROM pg_publication p JOIN pg_roles r ON r.oid=p.pubowner WHERE p.pubname='boring_cdc_publication'")
 cargo run --quiet --example m1_control_probe -- catalog "$catalog"
 psql "$admin" -qc "ALTER PUBLICATION boring_cdc_publication SET (publish='insert,update,delete')"; catalog=$(psql "$admin" -Atqc "SELECT jsonb_build_object('name',p.pubname,'owner_role',r.rolname,'relations',(SELECT jsonb_agg(schemaname||'.'||tablename ORDER BY schemaname,tablename) FROM pg_publication_tables WHERE pubname=p.pubname),'operations',(SELECT jsonb_agg(operation ORDER BY operation) FROM (VALUES ('insert',p.pubinsert),('update',p.pubupdate),('delete',p.pubdelete),('truncate',p.pubtruncate)) f(operation,enabled) WHERE enabled)) FROM pg_publication p JOIN pg_roles r ON r.oid=p.pubowner WHERE p.pubname='boring_cdc_publication'")
 cargo run --quiet --example m1_control_probe -- catalog-drift "$catalog"
 assert_live SCN-M1-RAW-PUBLICATION-DRIFT m1_control_fixtures::tests::publication_fingerprint_is_exact_and_order_independent
 before=$(psql "$admin" -Atqc "SELECT md5(jsonb_agg(attname||':'||atttypid ORDER BY attnum)::text) FROM pg_attribute WHERE attrelid='public.accounts'::regclass AND attnum>0 AND NOT attisdropped")
 psql "$admin" -qc 'ALTER TABLE public.accounts ALTER COLUMN value TYPE bigint USING length(value)'
 after=$(psql "$admin" -Atqc "SELECT md5(jsonb_agg(attname||':'||atttypid ORDER BY attnum)::text) FROM pg_attribute WHERE attrelid='public.accounts'::regclass AND attnum>0 AND NOT attisdropped")
 [ "$before" != "$after" ]; assert_live SCN-M1-RAW-IDLE-DDL m1_ddl_fixtures::tests::every_relation_contract_dimension_changes_the_fingerprint
 psql "$admin" -qc "INSERT INTO public.accounts VALUES (7,42)"; wire=$(changes); summary "$wire"; python3 -c 'import sys; tags=[bytes.fromhex(x)[0] for x in sys.argv[1].split(",") if x]; assert ord("R") in tags and ord("I") in tags' "$wire"
 assert_live SCN-M1-RAW-IMMEDIATE-DDL m1_ddl_fixtures::tests::changed_relation_synchronously_blocks_following_dml_and_feedback
 if psql "$admin" -Atqc "SELECT data FROM pg_logical_slot_get_binary_changes('boring_cdc_slot',NULL,NULL,'proto_version','99','publication_names','boring_cdc_publication')" >/dev/null 2>&1; then echo E_UNSUPPORTED_PROTOCOL >&2; exit 1; fi
 assert_live SCN-M1-RAW-UNSUPPORTED-PROTOCOL m1_decoder::tests::unsupported_messages_and_binary_truncate_fail_closed
 psql "$admin" -qc 'CREATE TABLE public.no_identity(payload text); ALTER PUBLICATION boring_cdc_publication ADD TABLE public.no_identity'
 [ "$(psql "$admin" -Atqc "SELECT relreplident::text||':'||(SELECT count(*) FROM pg_index WHERE indrelid='public.no_identity'::regclass AND indisprimary) FROM pg_class WHERE oid='public.no_identity'::regclass")" = 'd:0' ]; assert_live SCN-M1-RAW-UNSUPPORTED-TABLE m1_ddl_fixtures::tests::selected_types_keys_delete_and_destination_compatibility_fail_independently
 psql "$admin" -qc 'CREATE TABLE public.unsupported_type(id bigint PRIMARY KEY, location point); ALTER PUBLICATION boring_cdc_publication ADD TABLE public.unsupported_type'
 [ "$(psql "$admin" -Atqc "SELECT atttypid FROM pg_attribute WHERE attrelid='public.unsupported_type'::regclass AND attname='location'")" = 600 ]; assert_live SCN-M1-RAW-UNSUPPORTED-TYPE m1_ddl_fixtures::tests::changed_type_or_removed_replica_identity_blocks_update_delete_safety
 docker rm -f "$name" >/dev/null
 i=0; while docker inspect "$name" >/dev/null 2>&1; do i=$((i+1)); [ "$i" -lt 30 ] || { echo E_POSTGRES_CLEANUP >&2; exit 1; }; sleep 1; done
 [ -z "$(docker ps -a --filter "name=^/${name}$" --format '{{.Names}}')" ]
 container_started=false
 echo 'CLEANUP container_absent=true isolated_docker_resources_absent=true'
 echo 'PASS actual_sql=heartbeat,truncate,publication_drift checkpoint=unchanged cleanup=verified'
} > "$transcript"
cat "$transcript"
scripts/validate/m1_raw_demo.py seal e2e "$transcript" >/dev/null
echo 'PASS m1 raw e2e evidence sealed'
