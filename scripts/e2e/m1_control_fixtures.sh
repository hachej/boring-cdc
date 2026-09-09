#!/bin/sh
set -eu
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/e2e/m1_control_fixtures.sh [SEED]'; exit 0; }
seed=${1:-m1-control-v1}; [ "$seed" = m1-control-v1 ] || { echo 'E_SEED: expected m1-control-v1' >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
# M0-PROVISIONAL: boring-cdc-d-pg-protocol (recommended supported majors).
majors="15 16 17"
for major in $majors; do
  name="boring-cdc-m1-control-$major-$$"
  trap 'docker rm -f "$name" >/dev/null 2>&1 || true' EXIT HUP INT TERM
  docker run -d --rm --name "$name" -e POSTGRES_PASSWORD=postgres "postgres:$major-alpine" -c wal_level=logical -c max_replication_slots=4 >/dev/null
  ready=0; i=0
  while [ "$i" -lt 60 ]; do
    if docker exec "$name" pg_isready -U postgres >/dev/null 2>&1; then ready=1; break; fi
    i=$((i+1)); sleep 1
  done
  [ "$ready" -eq 1 ] || { echo "E_POSTGRES_HEALTH major=$major" >&2; exit 1; }
  docker exec -i "$name" psql -U postgres -d postgres < tests/fixtures/m1-control/setup.sql >/dev/null
  control='postgresql://boring_cdc_control_writer:control_fixture_only@127.0.0.1/postgres'
  capture='postgresql://boring_cdc_capture_bootstrap:capture_fixture_only@127.0.0.1/postgres'
  docker exec "$name" psql "$control" -Atqc "SELECT id FROM boring_cdc_control.heartbeat" | grep -qx singleton
  docker exec "$name" psql "$control" -v ON_ERROR_STOP=1 -qc "UPDATE boring_cdc_control.heartbeat SET nonce=1,updated_at=clock_timestamp() WHERE id='singleton'"
  [ "$(docker exec "$name" psql -U postgres -Atqc "SELECT nonce FROM boring_cdc_control.heartbeat WHERE id='singleton'")" = 1 ]
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
  docker exec "$name" psql "$capture" -Atqc "SELECT (pg_create_logical_replication_slot('boring_cdc_slot','pgoutput')).slot_name" | grep -qx boring_cdc_slot
  docker exec "$name" psql -U postgres -qc 'TRUNCATE public.accounts'
  docker exec "$name" psql -U postgres -Atqc "SELECT pubinsert,pubupdate,pubdelete,pubtruncate FROM pg_publication WHERE pubname='boring_cdc_publication'" | grep -qx 't|t|t|t'
  docker rm -f "$name" >/dev/null; trap - EXIT HUP INT TERM
  printf 'PASS pg=%s publication/grants/cardinality/slot/truncate\n' "$major"
done
cargo test --locked m1_control_fixtures::tests >/dev/null
printf 'm1 control e2e pass seed=%s cleanup=trap\n' "$seed"
