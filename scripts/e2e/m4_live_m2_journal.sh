#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp

container="m4-live-m2-$RANDOM-$$"
work=$(mktemp -d /var/tmp/m4-live-m2.XXXXXX)
password=$(python3 -c 'import secrets; print(secrets.token_hex(16))')
cleanup() {
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
ch() {
  docker exec -i "$container" clickhouse-client --user bcdc_probe --password "$password" "$@"
}
wait_ready() {
  local version
  for _ in {1..60}; do
    if version=$(ch --query 'SELECT version()' 2>/dev/null); then
      [[ "$version" == 25.8.2.29 ]]
      return
    fi
    sleep 1
  done
  return 1
}
wait_ready
ch --multiquery < contracts/clickhouse/ddl.sql
ch --query "INSERT INTO boring_cdc.generation_selectors_v1 VALUES (1,9,7,'6666666666666666666666666666666666666666666666666666666666666666','ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff','7777777777777777777777777777777777777777777777777777777777777777')"
ch --query "INSERT INTO boring_cdc.generation_selectors_v1 VALUES (1,8,6,'6666666666666666666666666666666666666666666666666666666666666666','ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff','8888888888888888888888888888888888888888888888888888888888888888')"

endpoint=$(docker port "$container" 8123/tcp)
port=${endpoint##*:}
export BCDC_M4_CLICKHOUSE_URL="http://127.0.0.1:$port"
export BCDC_M4_CLICKHOUSE_USER=bcdc_probe
export BCDC_M4_CLICKHOUSE_PASSWORD="$password"
cargo test --locked --lib \
  m4_clickhouse_wire::tests::live_contract_inspection_matches_pinned_objects_and_selector \
  -- --ignored --exact >"$work/preflight.log" 2>&1 || {
    cat "$work/preflight.log" >&2
    exit 1
  }
if ! scripts/acceptance/durable_simple_case.sh >"$work/acceptance.log" 2>&1; then
  cat "$work/acceptance.log" >&2
  exit 1
fi
grep -q '^M4_LIVE_JOURNAL_OK ' "$work/acceptance.log"
grep -q '^DURABLE_SIMPLE_CASE_OK ' "$work/acceptance.log"

docker restart "$container" >/dev/null
wait_ready
read -r events distinct_events < <(ch --query 'SELECT count(),uniqExact(connector_event_id) FROM boring_cdc.event_history_v1 WHERE capture_epoch=1 AND generation=1')
read -r markers distinct_markers < <(ch --query 'SELECT count(),uniqExact(batch_id) FROM boring_cdc.batch_markers_v1 WHERE capture_epoch=1 AND generation=1')
(( events >= 10 && events == distinct_events ))
(( markers >= 2 && markers == distinct_markers ))
printf 'M4_LIVE_M2_DESTINATION_OK clickhouse=25.8.2.29 events=%s markers=%s restart=verified\n' "$events" "$markers"
