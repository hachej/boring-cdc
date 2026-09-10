#!/bin/sh
set -eu
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/e2e/m1_ddl_fixtures.sh [SEED]'; exit 0; }
seed=${1:-m1-ddl-v1}; [ "$seed" = m1-ddl-v1 ] || { echo 'E_SEED: expected m1-ddl-v1' >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
tmp=$(mktemp -d); name=''; guard_pid=''
cleanup() { [ -z "$guard_pid" ] || kill "$guard_pid" >/dev/null 2>&1 || true; [ -z "$name" ] || docker rm -f "$name" >/dev/null 2>&1 || true; rm -rf "$tmp"; }
trap cleanup EXIT HUP INT TERM
# M0-PROVISIONAL: boring-cdc-d-ddl (RECOMMENDED PostgreSQL majors and immutable image digests).
for major in 15 16 17; do
 case "$major" in
 15) image='postgres@sha256:fe0737ba566a2c5b2a28f34433c0a423261900ec17b9bf7ad115e1aae7e57f1b' ;;
 16) image='postgres@sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777' ;;
 17) image='postgres@sha256:c7526c0f6c3f30260a563d7bcf8ad778effac59a44f8ffa86678c35418338609' ;;
 esac
 name="boring-cdc-m1-ddl-$major-$$"
 docker run -d --rm --name "$name" -e POSTGRES_PASSWORD=postgres -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1::5432 "$image" -c wal_level=logical >/dev/null
 port=$(docker port "$name" 5432/tcp | sed 's/.*://'); dsn="postgresql://postgres@127.0.0.1:$port/postgres"
 ready=0; i=0; while [ "$i" -lt 60 ]; do if pg_isready -d "$dsn" >/dev/null 2>&1; then ready=1; break; fi; i=$((i+1)); sleep 1; done
 [ "$ready" -eq 1 ] || { echo "E_POSTGRES_HEALTH major=$major" >&2; exit 1; }
 psql "$dsn" -v ON_ERROR_STOP=1 -qc 'CREATE TABLE public.guarded(id bigint PRIMARY KEY,payload text NOT NULL); CREATE UNIQUE INDEX guarded_payload_uq ON public.guarded(payload); ALTER TABLE public.guarded REPLICA IDENTITY USING INDEX guarded_payload_uq'
 fingerprint() { psql "$dsn" -Atqc "
 SELECT md5(jsonb_build_object(
  'schema_version',1,'logical_table_id','table-guarded','database_oid',(SELECT oid FROM pg_database WHERE datname=current_database()),
  'relation_oid',c.oid,'namespace',n.nspname,'relation_name',c.relname,
  'columns',(SELECT jsonb_agg(jsonb_build_object('attnum',a.attnum,'logical_order',a.attnum,'physical_order',a.attnum,'column_name',a.attname,'dropped',a.attisdropped,'type_oid',a.atttypid,'typmod',a.atttypmod,'collation_oid',a.attcollation,'nullable',NOT a.attnotnull,'default_expression_hash',CASE WHEN d.adbin IS NULL THEN NULL ELSE md5(pg_get_expr(d.adbin,d.adrelid)) END,'generated_expression_hash',CASE WHEN a.attgenerated='' THEN NULL ELSE md5(coalesce(pg_get_expr(d.adbin,d.adrelid),'')) END,'identity_expression_hash',CASE WHEN a.attidentity='' THEN NULL ELSE md5(a.attidentity::text) END) ORDER BY a.attnum) FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=c.oid AND a.attnum>0),
  'primary_attnums',(SELECT coalesce(jsonb_agg(k ORDER BY k),'[]') FROM pg_index i CROSS JOIN LATERAL unnest(i.indkey::smallint[]) k WHERE i.indrelid=c.oid AND i.indisprimary),
  'unique_indexes',(SELECT coalesce(jsonb_object_agg(ic.relname,to_jsonb(i.indkey::smallint[]) ORDER BY ic.relname),'{}') FROM pg_index i JOIN pg_class ic ON ic.oid=i.indexrelid WHERE i.indrelid=c.oid AND i.indisunique),
  'replica_identity_mode',c.relreplident,'replica_identity_index',(SELECT ic.relname FROM pg_index i JOIN pg_class ic ON ic.oid=i.indexrelid WHERE i.indrelid=c.oid AND i.indisreplident),
  'partition_routing',CASE WHEN c.relispartition THEN 'leaf' WHEN c.relkind='p' THEN 'root' ELSE 'plain' END,
  'partition_root',(SELECT inhparent FROM pg_inherits WHERE inhrelid=c.oid LIMIT 1),'partition_key_hash',CASE WHEN c.relkind='p' THEN md5(pg_get_partkeydef(c.oid)) ELSE NULL END,
  'partition_bounds_hash',CASE WHEN c.relispartition THEN md5(pg_get_expr(c.relpartbound,c.oid)) ELSE NULL END,
  'publication_member',EXISTS(SELECT 1 FROM pg_publication_tables p WHERE p.pubname='ddl_pub' AND p.schemaname=n.nspname AND p.tablename=c.relname),
  'publication_attnums',(SELECT jsonb_agg(a.attnum ORDER BY a.attnum) FROM pg_publication_tables p CROSS JOIN LATERAL unnest(p.attnames) projected(name) JOIN pg_attribute a ON a.attrelid=c.oid AND a.attname=projected.name WHERE p.pubname='ddl_pub' AND p.schemaname=n.nspname AND p.tablename=c.relname))::text)
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname='guarded'"; }
 psql "$dsn" -v ON_ERROR_STOP=1 -qc 'CREATE PUBLICATION ddl_pub FOR TABLE public.guarded (id,payload)'
 before=$(fingerprint)
 start_guard() {
  PGAPPNAME=boring_cdc_ddl_guard psql "$dsn" -v ON_ERROR_STOP=1 -qc 'BEGIN READ ONLY; LOCK TABLE public.guarded IN ACCESS SHARE MODE; SELECT pg_sleep(90)' >"$tmp/guard-$major.out" 2>"$tmp/guard-$major.err" & guard_pid=$!
  granted=0; i=0; while [ "$i" -lt 60 ]; do if [ "$(psql "$dsn" -Atqc "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a USING(pid) WHERE a.application_name='boring_cdc_ddl_guard' AND l.relation='public.guarded'::regclass AND l.mode='AccessShareLock' AND l.granted")" = 1 ]; then granted=1; break; fi; i=$((i+1)); sleep 1; done
  [ "$granted" -eq 1 ] || { echo "E_GUARD_LOCK major=$major" >&2; exit 1; }
 }
 stop_guard() {
  psql "$dsn" -Atqc "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='boring_cdc_ddl_guard'" | grep -qx t
  wait "$guard_pid" >/dev/null 2>&1 || true; guard_pid=''
  [ "$(psql "$dsn" -Atqc "SELECT count(*) FROM pg_locks WHERE relation='public.guarded'::regclass AND mode='AccessShareLock' AND granted")" = 0 ]
 }
 conflict() {
  ddl=$1; label=$2; err="$tmp/ddl-$major-$label.err"
  set +e; PGAPPNAME=boring_cdc_ddl_waiter psql "$dsn" -v ON_ERROR_STOP=1 -qc "SET lock_timeout='300ms'; $ddl" >/dev/null 2>"$err"; rc=$?; set -e
  [ "$rc" -ne 0 ] && grep -q 'lock timeout' "$err" || { echo "E_DDL_DID_NOT_CONFLICT major=$major case=$label" >&2; exit 1; }
 }
 # One guarded lifecycle: before export -> exported snapshot -> imported copy -> durable fence.
 psql "$dsn" -qc 'CREATE TABLE public.capture_fence(generation bigint PRIMARY KEY, nonce bigint NOT NULL)'
 start_guard
 conflict 'ALTER TABLE public.guarded RENAME COLUMN payload TO payload_changed' guard-before-export
 mkfifo "$tmp/export-$major.in" "$tmp/export-$major.out"
 PGAPPNAME=boring_cdc_snapshot_exporter psql "$dsn" -qAt -v ON_ERROR_STOP=1 <"$tmp/export-$major.in" >"$tmp/export-$major.out" 2>"$tmp/export-$major.err" & exporter_pid=$!
 exec 3>"$tmp/export-$major.in"; exec 4<"$tmp/export-$major.out"
 printf 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;\nSELECT pg_export_snapshot();\n' >&3
 IFS= read -r snapshot <&4; [ -n "$snapshot" ]
 conflict 'ALTER TABLE public.guarded RENAME COLUMN payload TO payload_changed' export
 PGAPPNAME=boring_cdc_snapshot_importer psql "$dsn" -qAt -v ON_ERROR_STOP=1 -c "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '$snapshot'; SELECT count(*) FROM public.guarded; SELECT pg_sleep(2); COMMIT" >"$tmp/copy-$major.out" 2>"$tmp/copy-$major.err" & importer_pid=$!
 i=0; while [ "$i" -lt 60 ]; do [ "$(psql "$dsn" -Atqc "SELECT count(*) FROM pg_stat_activity WHERE application_name='boring_cdc_snapshot_importer'")" = 1 ] && break; i=$((i+1)); sleep 1; done; [ "$i" -lt 60 ]
 conflict 'ALTER TABLE public.guarded RENAME COLUMN payload TO payload_changed' copy
 wait "$importer_pid"; grep -qx 0 "$tmp/copy-$major.out"
 printf 'COMMIT;\n\\q\n' >&3; exec 3>&-; exec 4<&-; wait "$exporter_pid"
 psql "$dsn" -v ON_ERROR_STOP=1 -qc 'INSERT INTO public.capture_fence VALUES (7,99)'; [ "$(psql "$dsn" -Atqc 'SELECT nonce FROM public.capture_fence WHERE generation=7')" = 99 ]
 conflict 'ALTER TABLE public.guarded RENAME COLUMN payload TO payload_changed' durable-fence
 stop_guard
 # Execute the complete admitted operation matrix under one guarded generation.
 start_guard; index=0
 matrix="$tmp/matrix-$major"; python3 -c 'import json; [print(x["sql"]) for x in json.load(open("contracts/m1/ddl-fixtures.json"))["admitted_ddl_matrix"]]' >"$matrix"
 while IFS= read -r ddl; do index=$((index+1)); conflict "$ddl" "matrix-$index"; done <"$matrix"
 # A real queued waiter is observed, then guard cancellation must unblock and complete it.
 PGAPPNAME=boring_cdc_ddl_waiter psql "$dsn" -v ON_ERROR_STOP=1 -qc 'ALTER TABLE public.guarded ADD COLUMN waiter_release_probe integer' >"$tmp/waiter-$major.out" 2>"$tmp/waiter-$major.err" & waiter_pid=$!
 waiting=0; i=0; while [ "$i" -lt 60 ]; do if [ "$(psql "$dsn" -Atqc "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a USING(pid) WHERE a.application_name='boring_cdc_ddl_waiter' AND l.relation='public.guarded'::regclass AND l.mode='AccessExclusiveLock' AND NOT l.granted")" = 1 ]; then waiting=1; break; fi; i=$((i+1)); sleep 1; done
 [ "$waiting" -eq 1 ] || { echo "E_WAITER_NOT_OBSERVED major=$major" >&2; exit 1; }
 stop_guard; wait "$waiter_pid"; psql "$dsn" -qc 'ALTER TABLE public.guarded DROP COLUMN waiter_release_probe'
 psql "$dsn" -v ON_ERROR_STOP=1 -qc 'ALTER TABLE public.guarded ADD COLUMN optional text'
 after=$(fingerprint); [ "$before" != "$after" ] || { echo "E_IDLE_DDL_FINGERPRINT major=$major" >&2; exit 1; }
 printf 'PASS pg=%s matrix=6 boundaries=4 waiter=observed-released lock=AccessExclusiveLock idle-poll=full-fingerprint-changed\n' "$major"
 docker rm -f "$name" >/dev/null; name=''
done
cargo test --locked m1_ddl_fixtures::tests >/dev/null 2>&1
printf 'm1 ddl e2e pass seed=%s pg_majors=3 cleanup=trap\n' "$seed"
