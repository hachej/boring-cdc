#!/usr/bin/env bash
set -euo pipefail
ulimit -c 0
cd "$(dirname "$0")/../.."

container="m4-wire-$RANDOM-$$"
work=$(mktemp -d /var/tmp/m4-coordinator.XXXXXX)
password=$(python3 -c 'import secrets; print(secrets.token_hex(16))')
cleanup() {
  if [[ "${KEEP:-0}" == 1 ]]; then return; fi
  docker rm -f "$container" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT INT TERM

docker run -d --name "$container" --platform linux/amd64 \
  -p 127.0.0.1::8123 \
  -e CLICKHOUSE_USER=bcdc_probe \
  -e "CLICKHOUSE_PASSWORD=$password" \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  docker.io/clickhouse/clickhouse-server:25.8.2.29@sha256:74c213b4d4cb4854c2497694df0c2d153c041003eadbb0457ae62c28cb8d723f \
  >/dev/null

ready() {
  docker exec "$container" clickhouse-client --user bcdc_probe \
    --password "$password" --query 'SELECT version()' 2>/dev/null
}
for attempt in {1..60}; do
  if version=$(ready); then break; fi
  sleep 1
done
[[ "${version:-}" == 25.8.2.29 ]]
docker exec -i "$container" clickhouse-client --user bcdc_probe \
  --password "$password" --multiquery < contracts/clickhouse/ddl.sql
docker exec "$container" clickhouse-client --user bcdc_probe --password "$password" \
  --query "INSERT INTO boring_cdc.generation_selectors_v1 VALUES (1,9,7,'6666666666666666666666666666666666666666666666666666666666666666','ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff','7777777777777777777777777777777777777777777777777777777777777777')"
docker exec "$container" clickhouse-client --user bcdc_probe --password "$password" \
  --query "INSERT INTO boring_cdc.generation_selectors_v1 VALUES (1,8,6,'6666666666666666666666666666666666666666666666666666666666666666','ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff','8888888888888888888888888888888888888888888888888888888888888888')"

endpoint=$(docker port "$container" 8123/tcp)
port=${endpoint##*:}
export BCDC_M4_CLICKHOUSE_URL="http://127.0.0.1:$port"
export BCDC_M4_CLICKHOUSE_USER=bcdc_probe
export BCDC_M4_CLICKHOUSE_PASSWORD
BCDC_M4_CLICKHOUSE_PASSWORD=$password
export BCDC_M4_COORDINATOR_SQLITE="$work/coordinator.sqlite"

expect_boundary_abort() {
  local test_name=$1 code
  set +e
  BCDC_M4_ABORT_AT_BOUNDARY=1 cargo test --locked --lib \
    "m4_clickhouse_wire::tests::$test_name" -- --ignored --exact \
    >"$work/$test_name.out" 2>"$work/$test_name.err"
  code=$?
  set -e
  if [[ "$code" != 101 ]] || ! grep -q SIGABRT "$work/$test_name.err"; then
    printf 'M4_BOUNDARY_ABORT_FAILED test=%s exit=%s\n' "$test_name" "$code" >&2
    return 1
  fi
}

cargo test --locked --lib \
  m4_clickhouse_wire::tests::live_contract_inspection_matches_pinned_objects_and_selector \
  -- --ignored --exact

cargo test --locked --lib \
  m4_clickhouse_wire::tests::live_rowbinary_history_insert_and_fresh_readback \
  -- --ignored --exact
expect_boundary_abort live_coordinator_persists_intent_and_marker_before_checkpoint
docker restart "$container" >/dev/null
endpoint=$(docker port "$container" 8123/tcp)
port=${endpoint##*:}
export BCDC_M4_CLICKHOUSE_URL="http://127.0.0.1:$port"
http_ready() {
  curl -fsS --user "bcdc_probe:$password" \
    "$BCDC_M4_CLICKHOUSE_URL/?query=SELECT%201" >/dev/null 2>&1
}
for attempt in {1..120}; do
  if http_ready; then break; fi
  sleep 1
done
http_ready
cargo test --locked --lib \
  m4_clickhouse_wire::tests::live_rowbinary_history_survives_server_restart \
  -- --ignored --exact
cargo test --locked --lib \
  m4_clickhouse_wire::tests::live_coordinator_adopts_marker_after_restart_and_checkpoints \
  -- --ignored --exact

run_coordinator_replay_case() {
  local generation=$1 before_test=$2
  export BCDC_M4_COORDINATOR_GENERATION="$generation"
  export BCDC_M4_COORDINATOR_SQLITE="$work/coordinator-$generation.sqlite"
  expect_boundary_abort "$before_test"
  docker restart "$container" >/dev/null
  endpoint=$(docker port "$container" 8123/tcp)
  port=${endpoint##*:}
  export BCDC_M4_CLICKHOUSE_URL="http://127.0.0.1:$port"
  for attempt in {1..120}; do
    if http_ready; then break; fi
    sleep 1
  done
  http_ready
  cargo test --locked --lib \
    m4_clickhouse_wire::tests::live_coordinator_replays_after_restart \
    -- --ignored --exact
}
run_coordinator_replay_case 4 live_coordinator_persists_intent_before_remote_write
run_coordinator_replay_case 5 live_coordinator_persists_history_before_marker
unset BCDC_M4_COORDINATOR_GENERATION

docker exec "$container" clickhouse-client --user bcdc_probe --password "$password" \
  --query "INSERT INTO boring_cdc.generation_selectors_v1 VALUES (1,9,8,'6666666666666666666666666666666666666666666666666666666666666666','ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff','8888888888888888888888888888888888888888888888888888888888888888')"
cargo test --locked --lib \
  m4_clickhouse_wire::tests::live_contract_inspection_rejects_selector_conflict \
  -- --ignored --exact

docker exec "$container" clickhouse-client --user bcdc_probe --password "$password" \
  --query "DROP VIEW boring_cdc.live_generation_v1"
cargo test --locked --lib \
  m4_clickhouse_wire::tests::live_contract_inspection_rejects_missing_view \
  -- --ignored --exact

printf 'M4_ROWBINARY_LIVE_OK clickhouse=%s\n' "$version"
