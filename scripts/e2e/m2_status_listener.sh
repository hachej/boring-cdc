#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp

work=$(mktemp -d /var/tmp/m2-status-listener.XXXXXX)
acceptance_pid=
cleanup() {
  [[ -z "$acceptance_pid" ]] || kill "$acceptance_pid" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT INT TERM

scripts/acceptance/durable_simple_case.sh >"$work/acceptance.log" 2>&1 &
acceptance_pid=$!
observed=0
for _ in {1..150}; do
  if python3 - 2>/dev/null <<'PY'
import json
import urllib.error
import urllib.request

url = "http://127.0.0.1:8787/status"
with urllib.request.urlopen(url, timeout=1) as response:
    assert response.status == 200
    result = json.load(response)
assert result["command"] == "CMD-STATUS" and result["outcome"] == "success"
assert result["data"]["snapshot_id"] and result["data"]["schema_version"] == 1
try:
    urllib.request.urlopen(urllib.request.Request(url, method="POST"), timeout=1)
except urllib.error.HTTPError as error:
    assert error.code == 405
else:
    raise AssertionError("status listener accepted a mutation method")
metrics_url = "http://127.0.0.1:8788/metrics"
with urllib.request.urlopen(metrics_url, timeout=1) as response:
    assert response.status == 200
    assert response.headers["Content-Type"].startswith("text/plain; version=0.0.4")
    metrics = response.read().decode()
assert "boring_cdc_status_available 1\n" in metrics
assert "boring_cdc_durable_journal_seq " in metrics
assert "boring_cdc_sqlite_bytes " in metrics
assert "boring_cdc_healthy " in metrics
assert "source_wal_headroom_bytes" not in metrics
try:
    urllib.request.urlopen(urllib.request.Request(metrics_url, method="POST"), timeout=1)
except urllib.error.HTTPError as error:
    assert error.code == 405
else:
    raise AssertionError("metrics listener accepted a mutation method")
PY
  then
    observed=1
    break
  fi
  kill -0 "$acceptance_pid" >/dev/null 2>&1 || break
  sleep .2
done
if ! wait "$acceptance_pid"; then
  cat "$work/acceptance.log" >&2
  exit 1
fi
acceptance_pid=
[[ "$observed" == 1 ]]
grep -q '^DURABLE_SIMPLE_CASE_OK ' "$work/acceptance.log"
printf 'M2_STATUS_LISTENER_OK postgres=17.6 live_capture=verified status=read_only metrics=read_only\n'
