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
  docker run -d --rm --name "$name" -e POSTGRES_PASSWORD=postgres -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1::5432 "$image" -c wal_level=logical -c max_replication_slots=4 >/dev/null
  ready=0; i=0
  while [ "$i" -lt 180 ]; do
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
  port=$(docker port "$name" 5432/tcp | sed 's/.*://')
  scripts/fixtures/m1_slot_export.py 127.0.0.1 "$port" >/dev/null
  catalog() {
    docker exec "$name" psql -U postgres -Atqc "
      SELECT jsonb_build_object(
        'name', p.pubname,
        'owner_role', r.rolname,
        'relations', (SELECT jsonb_agg(schemaname || '.' || tablename ORDER BY schemaname, tablename)
                      FROM pg_publication_tables WHERE pubname=p.pubname),
        'operations', (SELECT jsonb_agg(operation ORDER BY operation)
                       FROM (VALUES ('insert',p.pubinsert),('update',p.pubupdate),
                                    ('delete',p.pubdelete),('truncate',p.pubtruncate)) AS operation_flags(operation, enabled)
                       WHERE enabled))
      FROM pg_publication p JOIN pg_roles r ON r.oid=p.pubowner
      WHERE p.pubname='boring_cdc_publication'"
  }
  changes() {
    docker exec "$name" psql -U postgres -Atqc \
      "SELECT encode(data,'hex') FROM pg_logical_slot_get_binary_changes('boring_cdc_slot',NULL,NULL,'proto_version','1','publication_names','boring_cdc_publication')" | paste -sd, -
  }

  cargo run --quiet --example m1_control_probe -- catalog "$(catalog)" >/dev/null
  docker exec "$name" psql "$control" -Atqc "SELECT id FROM boring_cdc_control.heartbeat" | grep -qx singleton
  docker exec "$name" psql "$control" -v ON_ERROR_STOP=1 -qc "UPDATE boring_cdc_control.heartbeat SET nonce=1,updated_at=clock_timestamp() WHERE id='singleton'"
  wire=$(changes)
  cargo run --quiet --example m1_control_probe -- update "$wire" 1 >/dev/null
  cargo run --quiet --example m1_control_probe -- nonce "$wire" >/dev/null

  docker exec "$name" psql "$control" -v ON_ERROR_STOP=1 -qc "UPDATE boring_cdc_control.heartbeat SET nonce=2,updated_at=clock_timestamp() WHERE id='missing'"
  cargo run --quiet --example m1_control_probe -- cardinality "$(changes)" >/dev/null
  docker exec "$name" psql "$control" -v ON_ERROR_STOP=1 -qc "BEGIN; UPDATE boring_cdc_control.heartbeat SET nonce=2,updated_at=clock_timestamp() WHERE id='singleton'; UPDATE boring_cdc_control.heartbeat SET updated_at=clock_timestamp() WHERE id='singleton'; COMMIT"
  cargo run --quiet --example m1_control_probe -- cardinality "$(changes)" 2 >/dev/null

  docker exec "$name" psql "$control" -v ON_ERROR_STOP=1 -qc "UPDATE boring_cdc_control.capture_fences SET capture_epoch=1,generation=1,table_set_fingerprint=repeat('a',64),unique_nonce=7 WHERE id='singleton'"
  cargo run --quiet --example m1_control_probe -- fence "$(changes)" 7 >/dev/null
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

  docker exec "$name" psql -U postgres -qc 'ALTER TABLE boring_cdc_control.heartbeat ADD COLUMN unexpected text; UPDATE boring_cdc_control.heartbeat SET nonce=4,updated_at=clock_timestamp()'
  cargo run --quiet --example m1_control_probe -- shape "$(changes)" >/dev/null
  docker exec "$name" psql -U postgres -qc 'ALTER TABLE boring_cdc_control.heartbeat DROP COLUMN unexpected'

  docker exec "$name" psql -U postgres -qc 'TRUNCATE public.accounts'
  cargo run --quiet --example m1_control_probe -- truncate "$(changes)" >/dev/null

  docker exec "$name" psql -U postgres -qc "ALTER PUBLICATION boring_cdc_publication SET (publish='insert,update,delete')"
  cargo run --quiet --example m1_control_probe -- catalog-drift "$(catalog)" >/dev/null
  docker exec "$name" psql -U postgres -qc "ALTER PUBLICATION boring_cdc_publication SET (publish='insert,update,delete,truncate')"
  docker exec "$name" psql -U postgres -qc 'ALTER PUBLICATION boring_cdc_publication OWNER TO postgres'
  cargo run --quiet --example m1_control_probe -- catalog-drift "$(catalog)" >/dev/null
  docker exec "$name" psql -U postgres -qc 'ALTER PUBLICATION boring_cdc_publication OWNER TO boring_cdc_admin'
  docker exec "$name" psql -U postgres -qc 'CREATE TABLE public.forced_drift(id bigint PRIMARY KEY); ALTER PUBLICATION boring_cdc_publication ADD TABLE public.forced_drift'
  cargo run --quiet --example m1_control_probe -- catalog-drift "$(catalog)" >/dev/null
  docker exec "$name" psql -U postgres -qc 'ALTER PUBLICATION boring_cdc_publication DROP TABLE public.forced_drift; DROP TABLE public.forced_drift'
  cargo run --quiet --example m1_control_probe -- catalog "$(catalog)" >/dev/null
  printf 'PASS pg=%s live-relation/tuple/nonce/cardinality/truncate/catalog-operations-owner-membership\n' "$major"
  docker rm -f "$name" >/dev/null; trap - EXIT HUP INT TERM
done
cargo test --locked m1_control_fixtures::tests >/dev/null 2>&1
printf 'm1 control e2e pass seed=%s cleanup=trap\n' "$seed"
