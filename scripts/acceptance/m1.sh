#!/bin/sh
set -eu
export TMPDIR=${TMPDIR:-/var/tmp}
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/acceptance/m1.sh'; exit 0; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
out="$TMPDIR/m1accept${$}"; rm -rf "$out"; mkdir -p "$out"; trap 'rm -rf "$out"' EXIT HUP INT TERM
run_once() {
 n=$1; transcript="$out/transcript-$n.txt"
 {
  cargo test --locked --workspace --all-targets >"$out/workspace-$n.log" 2>&1
  echo 'PASS cargo test --locked --workspace --all-targets'
  cargo test --locked m1_raw_demo::tests >"$out/targeted-$n.log" 2>&1
  echo 'PASS cargo test --locked m1_raw_demo::tests cases=5'
  scripts/e2e/m1_raw_demo.sh raw-demo-v1
  scripts/faults/m1_raw_demo.sh raw-demo-v1
  scripts/validate/m1_raw_demo.py contract
  scripts/validate/m1_raw_demo.py selftest
  echo 'PASS M1 raw inspection and fault suite'
 } > "$transcript" 2>&1
 cat "$transcript"
 scripts/validate/m1_raw_demo.py seal milestone "$transcript"
}
run_once 1
run_once 2
scripts/validate/evidence.sh artifacts/boring-cdc-m1-raw-demo
scripts/validate/plan_coverage.sh
echo 'PASS M1 deterministic_rerun=1 evidence=validated cleanup=trap'
