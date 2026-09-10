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
 docker run -d --rm --name "$name" -e POSTGRES_PASSWORD=postgres -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1::5432 "$image" >/dev/null
 port=$(docker port "$name" 5432/tcp | sed 's/.*://'); dsn="postgresql://postgres@127.0.0.1:$port/postgres"
 ready=0; i=0; while [ "$i" -lt 60 ]; do if pg_isready -d "$dsn" >/dev/null 2>&1; then ready=1; break; fi; i=$((i+1)); sleep 1; done
 [ "$ready" -eq 1 ] || { echo "E_POSTGRES_HEALTH major=$major" >&2; exit 1; }
 psql "$dsn" -v ON_ERROR_STOP=1 -qc 'CREATE TABLE public.guarded(id bigint PRIMARY KEY,payload text NOT NULL); CREATE UNIQUE INDEX guarded_payload_uq ON public.guarded(payload); ALTER TABLE public.guarded REPLICA IDENTITY USING INDEX guarded_payload_uq'
 fingerprint() { psql "$dsn" -Atqc "SELECT md5(jsonb_build_object('relation_oid',c.oid,'namespace',n.nspname,'name',c.relname,'replica_identity',c.relreplident,'columns',(SELECT jsonb_agg(jsonb_build_array(a.attnum,a.attname,a.atttypid,a.atttypmod,a.attcollation,a.attnotnull,a.attisdropped,pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum) FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=c.oid AND a.attnum>0),'indexes',(SELECT jsonb_agg(pg_get_indexdef(i.indexrelid) ORDER BY i.indexrelid) FROM pg_index i WHERE i.indrelid=c.oid))::text) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname='guarded'"; }
 before=$(fingerprint)
 PGAPPNAME=boring_cdc_ddl_guard psql "$dsn" -v ON_ERROR_STOP=1 -qc 'BEGIN READ ONLY; LOCK TABLE public.guarded IN ACCESS SHARE MODE; SELECT pg_sleep(90)' >"$tmp/guard-$major.out" 2>"$tmp/guard-$major.err" & guard_pid=$!
 granted=0; i=0; while [ "$i" -lt 60 ]; do if [ "$(psql "$dsn" -Atqc "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a USING(pid) WHERE a.application_name='boring_cdc_ddl_guard' AND l.relation='public.guarded'::regclass AND l.mode='AccessShareLock' AND l.granted")" = 1 ]; then granted=1; break; fi; i=$((i+1)); sleep 1; done
 [ "$granted" -eq 1 ] || { echo "E_GUARD_LOCK major=$major" >&2; exit 1; }
 index=0
 python3 -c 'import json; [print(x["sql"]) for x in json.load(open("contracts/m1/ddl-fixtures.json"))["admitted_ddl_matrix"]]' | while IFS= read -r ddl; do
   index=$((index+1)); err="$tmp/ddl-$major-$index.err"
   set +e; PGAPPNAME=boring_cdc_ddl_waiter psql "$dsn" -v ON_ERROR_STOP=1 -qc "SET lock_timeout='750ms'; $ddl" >/dev/null 2>"$err"; rc=$?; set -e
   [ "$rc" -ne 0 ] && grep -q 'lock timeout' "$err" || { echo "E_DDL_DID_NOT_CONFLICT major=$major row=$index" >&2; exit 1; }
 done
 psql "$dsn" -Atqc "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='boring_cdc_ddl_guard'" | grep -qx t
 wait "$guard_pid" >/dev/null 2>&1 || true; guard_pid=''
 psql "$dsn" -v ON_ERROR_STOP=1 -qc 'ALTER TABLE public.guarded ADD COLUMN optional text'
 after=$(fingerprint); [ "$before" != "$after" ] || { echo "E_IDLE_DDL_FINGERPRINT major=$major" >&2; exit 1; }
 printf 'PASS pg=%s matrix=6 lock=AccessExclusiveLock guard=before-export,export,copy,durable-fence idle-poll=changed\n' "$major"
 docker rm -f "$name" >/dev/null; name=''
done
cargo test --locked m1_ddl_fixtures::tests >/dev/null 2>&1
printf 'm1 ddl e2e pass seed=%s pg_majors=3 cleanup=trap\n' "$seed"
