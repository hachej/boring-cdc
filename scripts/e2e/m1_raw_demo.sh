#!/bin/sh
set -eu
export TMPDIR=${TMPDIR:-/var/tmp}
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/e2e/m1_raw_demo.sh [raw-demo-v1]'; exit 0; }
seed=${1:-raw-demo-v1}; [ "$seed" = raw-demo-v1 ] || { echo 'E_SEED' >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
name="m1raw${$}"; out="$TMPDIR/$name"; transcript="$out/transcript.txt"; mkdir -p "$out"
# M0-PROVISIONAL: boring-cdc-d-compose (accepted workload PostgreSQL pin).
image='docker.io/library/postgres:17.6@sha256:00bc86618629af00d2937fdc5a5d63db3ff8450acf52f0636ec813c7f4902929'
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; rm -rf "$out"; }
trap cleanup EXIT HUP INT TERM
docker run -d --rm --name "$name" -e POSTGRES_PASSWORD=postgres -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1::5432 "$image" -c wal_level=logical -c max_replication_slots=4 >/dev/null
port=$(docker port "$name" 5432/tcp | sed 's/.*://'); admin="postgresql://postgres@127.0.0.1:$port/postgres"
i=0; until pg_isready -d "$admin" >/dev/null 2>&1; do i=$((i+1)); [ "$i" -lt 60 ] || { echo E_POSTGRES_HEALTH >&2; exit 1; }; sleep 1; done
psql "$admin" < tests/fixtures/m1-control/setup.sql >/dev/null
scripts/fixtures/m1_slot_export.py 127.0.0.1 "$port" >/dev/null
changes() { psql "$admin" -Atqc "SELECT encode(data,'hex') FROM pg_logical_slot_get_binary_changes('boring_cdc_slot',NULL,NULL,'proto_version','1','publication_names','boring_cdc_publication')" | paste -sd, -; }
summary() { python3 -c 'import sys; xs=[bytes.fromhex(x) for x in sys.argv[1].split(",") if x]; print("raw_pgoutput tags="+",".join(chr(x[0]) for x in xs)+" lengths="+",".join(map(lambda x:str(len(x)),xs))+" payload_values=redacted")' "$1"; }
{
 echo 'scenario=SCN-M1-RAW-EVENTS seed=raw-demo-v1 pg=17 slot=pgoutput'
 psql "$admin" -v ON_ERROR_STOP=1 -qc "UPDATE boring_cdc_control.heartbeat SET nonce=1,updated_at='2025-01-01T00:00:00Z' WHERE id='singleton'"
 wire=$(changes); summary "$wire"; cargo run --quiet --example m1_control_probe -- update "$wire" 1
 psql "$admin" -qc 'TRUNCATE public.accounts'; wire=$(changes); summary "$wire"; cargo run --quiet --example m1_control_probe -- truncate "$wire"
 catalog=$(psql "$admin" -Atqc "SELECT jsonb_build_object('name',p.pubname,'owner_role',r.rolname,'relations',(SELECT jsonb_agg(schemaname||'.'||tablename ORDER BY schemaname,tablename) FROM pg_publication_tables WHERE pubname=p.pubname),'operations',(SELECT jsonb_agg(operation ORDER BY operation) FROM (VALUES ('insert',p.pubinsert),('update',p.pubupdate),('delete',p.pubdelete),('truncate',p.pubtruncate)) f(operation,enabled) WHERE enabled)) FROM pg_publication p JOIN pg_roles r ON r.oid=p.pubowner WHERE p.pubname='boring_cdc_publication'")
 cargo run --quiet --example m1_control_probe -- catalog "$catalog"
 psql "$admin" -qc "ALTER PUBLICATION boring_cdc_publication SET (publish='insert,update,delete')"; catalog=$(psql "$admin" -Atqc "SELECT jsonb_build_object('name',p.pubname,'owner_role',r.rolname,'relations',(SELECT jsonb_agg(schemaname||'.'||tablename ORDER BY schemaname,tablename) FROM pg_publication_tables WHERE pubname=p.pubname),'operations',(SELECT jsonb_agg(operation ORDER BY operation) FROM (VALUES ('insert',p.pubinsert),('update',p.pubupdate),('delete',p.pubdelete),('truncate',p.pubtruncate)) f(operation,enabled) WHERE enabled)) FROM pg_publication p JOIN pg_roles r ON r.oid=p.pubowner WHERE p.pubname='boring_cdc_publication'")
 cargo run --quiet --example m1_control_probe -- catalog-drift "$catalog"
 echo 'PASS actual_sql=heartbeat,truncate,publication_drift checkpoint=unchanged cleanup=trap'
} > "$transcript"
cat "$transcript"
scripts/validate/m1_raw_demo.py seal e2e "$transcript"
