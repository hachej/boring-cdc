#!/bin/sh
set -eu
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/e2e/m1_control_fixtures.sh [SEED]'; exit 0; }
seed=${1:-m1-control-v1}; [ "$seed" = m1-control-v1 ] || { echo 'E_SEED: expected m1-control-v1' >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
# M0-PROVISIONAL: boring-cdc-d-pg-protocol (recommended supported majors and image digests).
for major in 15 16 17; do
  case "$major" in
    15) image='postgres@sha256:fe0737ba566a2c5b2a28f34433c0a423261900ec17b9bf7ad115e1aae7e57f1b' ;;
    16) image='postgres@sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777' ;;
    17) image='postgres@sha256:c7526c0f6c3f30260a563d7bcf8ad778effac59a44f8ffa86678c35418338609' ;;
  esac
  name="boring-cdc-m1-control-$major-$$"
  trap 'docker rm -f "$name" >/dev/null 2>&1 || true' EXIT HUP INT TERM
  docker run -d --rm --name "$name" -e POSTGRES_PASSWORD=postgres "$image" -c wal_level=logical -c max_replication_slots=4 >/dev/null
  ready=0; i=0
  while [ "$i" -lt 60 ]; do
    # The image briefly starts an initialization server; accept only final PID 1 postgres.
    if [ "$(docker exec "$name" cat /proc/1/comm 2>/dev/null || true)" = postgres ] && docker exec "$name" pg_isready -U postgres >/dev/null 2>&1; then ready=1; break; fi
    i=$((i+1)); sleep 1
  done
  [ "$ready" -eq 1 ] || { echo "E_POSTGRES_HEALTH major=$major" >&2; exit 1; }
  docker exec -i "$name" psql -U postgres -d postgres < tests/fixtures/m1-control/setup.sql >/dev/null
  control='postgresql://boring_cdc_control_writer:control_fixture_only@127.0.0.1/postgres'
  capture='postgresql://boring_cdc_capture_bootstrap:capture_fixture_only@127.0.0.1/postgres'
  # PostgreSQL proves the broad REPLICATION privilege; the Rust connector guard narrows name,
  # operation, ownership, intent, and administration-credential lifetime.
  docker exec "$name" psql "$capture" -Atqc "SELECT (pg_create_logical_replication_slot('boring_cdc_slot','pgoutput')).slot_name" | grep -qx boring_cdc_slot
  docker exec "$name" psql "$control" -Atqc "SELECT id FROM boring_cdc_control.heartbeat" | grep -qx singleton
  docker exec "$name" psql "$control" -v ON_ERROR_STOP=1 -qc "UPDATE boring_cdc_control.heartbeat SET nonce=1,updated_at=clock_timestamp() WHERE id='singleton'"
  [ "$(docker exec "$name" psql -U postgres -Atqc "SELECT nonce FROM boring_cdc_control.heartbeat WHERE id='singleton'")" = 1 ]
  [ "$(docker exec "$name" psql "$control" -v ON_ERROR_STOP=1 -Atqc "WITH changed AS (UPDATE boring_cdc_control.heartbeat SET nonce=2,updated_at=clock_timestamp() WHERE id='missing' RETURNING 1) SELECT count(*) FROM changed")" = 0 ]
  docker exec "$name" psql "$control" -v ON_ERROR_STOP=1 -qc "UPDATE boring_cdc_control.capture_fences SET capture_epoch=1,generation=1,table_set_fingerprint=repeat('a',64),unique_nonce=7 WHERE id='singleton'"
  docker exec "$name" psql "$control" -v ON_ERROR_STOP=1 -qc "UPDATE boring_cdc_control.capture_fences SET capture_epoch=1,generation=1,table_set_fingerprint=repeat('a',64),unique_nonce=7 WHERE id='singleton'"
  types=$(docker exec "$name" psql -U postgres -Atqc "SELECT get_byte(data,0) FROM pg_logical_slot_get_binary_changes('boring_cdc_slot',NULL,NULL,'proto_version','1','publication_names','boring_cdc_publication')")
  printf '%s\n' "$types" | grep -qx 85 # pgoutput Update
  for forbidden in \
    "INSERT INTO boring_cdc_control.heartbeat VALUES ('other',2,now())" \
    "DELETE FROM boring_cdc_control.heartbeat WHERE id='singleton'" \
    "UPDATE boring_cdc_control.heartbeat SET id='other' WHERE id='singleton'" \
    "SELECT nonce FROM boring_cdc_control.heartbeat"; do
    if docker exec "$name" psql "$control" -v ON_ERROR_STOP=1 -qc "$forbidden" >/dev/null 2>&1; then echo "E_FORBIDDEN_DML major=$major" >&2; exit 1; fi
  done
  [ "$(docker exec "$name" psql -U postgres -Atqc 'SELECT count(*) FROM boring_cdc_control.heartbeat')" = 1 ]
  [ "$(docker exec "$name" psql -U postgres -Atqc 'SELECT count(*) FROM boring_cdc_control.capture_fences')" = 1 ]
  for forbidden in "ALTER PUBLICATION boring_cdc_publication ADD TABLE pg_catalog.pg_class" "DROP PUBLICATION boring_cdc_publication"; do
    if docker exec "$name" psql "$capture" -v ON_ERROR_STOP=1 -qc "$forbidden" >/dev/null 2>&1; then echo "E_PUBLICATION_OWNER major=$major" >&2; exit 1; fi
  done
  docker exec "$name" psql -U postgres -qc 'TRUNCATE public.accounts'
  types=$(docker exec "$name" psql -U postgres -Atqc "SELECT get_byte(data,0) FROM pg_logical_slot_get_binary_changes('boring_cdc_slot',NULL,NULL,'proto_version','1','publication_names','boring_cdc_publication')")
  printf '%s\n' "$types" | grep -qx 84 # pgoutput Truncate; Rust fixture asserts fail-closed routing.
  docker exec "$name" psql -U postgres -qc 'CREATE TABLE public.forced_drift(id bigint PRIMARY KEY); ALTER PUBLICATION boring_cdc_publication ADD TABLE public.forced_drift'
  [ "$(docker exec "$name" psql -U postgres -Atqc "SELECT count(*) FROM pg_publication_tables WHERE pubname='boring_cdc_publication'")" = 4 ]
  docker rm -f "$name" >/dev/null; trap - EXIT HUP INT TERM
  printf 'PASS pg=%s grants/cardinality/slot/pgoutput-update/truncate/drift\n' "$major"
done
cargo test --locked m1_control_fixtures::tests >/dev/null 2>&1
printf 'm1 control e2e pass seed=%s cleanup=trap\n' "$seed"
