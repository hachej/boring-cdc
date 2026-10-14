#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

container="m4-wire-$RANDOM-$$"
password=$(python3 -c 'import secrets; print(secrets.token_hex(16))')
cleanup() {
  if [[ "${KEEP:-0}" == 1 ]]; then return; fi
  docker rm -f "$container" >/dev/null 2>&1 || true
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

endpoint=$(docker port "$container" 8123/tcp)
port=${endpoint##*:}
export BCDC_M4_CLICKHOUSE_URL="http://127.0.0.1:$port"
export BCDC_M4_CLICKHOUSE_USER=bcdc_probe
export BCDC_M4_CLICKHOUSE_PASSWORD="$password"

cargo test --locked --lib \
  m4_clickhouse_wire::tests::live_rowbinary_history_insert_and_fresh_readback \
  -- --ignored --exact
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

printf 'M4_ROWBINARY_LIVE_OK clickhouse=%s\n' "$version"
