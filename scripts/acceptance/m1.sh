#!/bin/sh
set -eu
export TMPDIR=${TMPDIR:-/var/tmp}
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/acceptance/m1.sh'; exit 0; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
out="$TMPDIR/m1accept${$}"; transcript="$out/transcript.txt"; rm -rf "$out"; mkdir -p "$out"; trap 'rm -rf "$out"' EXIT HUP INT TERM
{
 cargo test --locked --workspace --all-targets
 cargo test --locked m1_raw_demo::tests
 scripts/e2e/m1_raw_demo.sh raw-demo-v1
 scripts/faults/m1_raw_demo.sh raw-demo-v1
 scripts/validate/m1_raw_demo.py contract
 echo 'PASS M1 raw inspection and fault suite; rerun once for deterministic_rerun=1'
} > "$transcript" 2>&1
cat "$transcript"
scripts/validate/m1_raw_demo.py seal milestone "$transcript"
