#!/bin/sh
set -eu
export TMPDIR=${TMPDIR:-/var/tmp}
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/faults/m1_workload.sh [workload-v1]'; exit 0; }
seed=${1:-workload-v1}; [ "$seed" = workload-v1 ] || { echo E_SEED >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
project="m1workloadfault${$}"; out="$TMPDIR/$project"; rm -rf "$out"; mkdir -p "$out"; : > "$out/suite.stdout"
compose="docker compose -p $project -f fixtures/m1/workload-compose.yml"
cleanup() { $compose down -v --remove-orphans >/dev/null 2>&1 || true; rm -rf "$out"; }
trap cleanup EXIT HUP INT TERM
$compose up -d --wait --wait-timeout 120 postgres clickhouse >/dev/null
scripts/validate/m1_workload.py execute "$project" "$out" | tee -a "$out/suite.stdout"
for mode in business-omission ledger-omission retry-duplicate retry-conflict unavailable; do scripts/validate/m1_workload.py fault "$project" "$out" "$mode" | tee -a "$out/suite.stdout"; done
printf 'm1 workload faults PASS business-only=fail ledger-only=fail duplicate=pass conflict=fail unavailable=non-pass final=pass\n' | tee -a "$out/suite.stdout"
scripts/validate/m1_workload.py evidence "$project" "$out" artifacts/boring-cdc-m1-workload/SCN-M1-WORKLOAD-CLEAN/workload-v1
scripts/validate/m1_workload.py evidence "$project" "$out" artifacts/boring-cdc-m1-workload/SCN-M1-WORKLOAD-FAULTS/workload-v1
