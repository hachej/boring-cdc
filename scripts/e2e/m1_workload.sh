#!/bin/sh
set -eu
export TMPDIR=${TMPDIR:-/var/tmp}
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/e2e/m1_workload.sh [workload-v1]'; exit 0; }
seed=${1:-workload-v1}; [ "$seed" = workload-v1 ] || { echo 'E_SEED' >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
project="m1workload${$}"
out="$TMPDIR/$project"; rm -rf "$out"; mkdir -p "$out"
compose="docker compose -p $project -f fixtures/m1/workload-compose.yml"
cleanup() { $compose down -v --remove-orphans >/dev/null 2>&1 || true; rm -rf "$out"; }
trap cleanup EXIT HUP INT TERM
$compose config -q
$compose up -d --wait --wait-timeout 120 postgres clickhouse >/dev/null
scripts/validate/m1_workload.py execute "$project" "$out" clean
scripts/validate/m1_workload.py evidence "$project" "$out" artifacts/boring-cdc-m1-workload/SCN-M1-WORKLOAD-CLEAN/workload-v1
printf 'm1 workload e2e PASS seed=%s ledger=pass business=pass final=pass fence=pass reader=concurrent\n' "$seed"
