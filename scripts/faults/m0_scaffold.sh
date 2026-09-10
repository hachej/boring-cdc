#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$ROOT"
tmp=$(mktemp "${TMPDIR:-/var/tmp}/m0-scaffold-compose.XXXXXX")
trap 'rm -f "$tmp"' EXIT HUP INT TERM
sed 's/sha256:00bc86618629af00d2937fdc5a5d63db3ff8450acf52f0636ec813c7f4902929/sha256:0000000000000000000000000000000000000000000000000000000000000000/' compose.yaml > "$tmp"
set +e
python3 scripts/lib/m0_scaffold.py probe SCN-M0-SCAFFOLD-DIGEST-MISMATCH --path "$tmp" >/dev/null
mismatch=$?
python3 scripts/lib/m0_scaffold.py probe SCN-M0-SCAFFOLD-DEPENDENCY-DELAY >/dev/null
delay=$?
set -e
[ "$mismatch" -eq 78 ] && [ "$delay" -eq 75 ]
python3 scripts/lib/m0_scaffold.py probe SCN-M0-SCAFFOLD-ZOMBIE-BOUND >/dev/null
python3 scripts/lib/m0_scaffold.py probe SCN-M0-SCAFFOLD-AGENT-READONLY >/dev/null
printf '%s\n' '{"status":"pass","executed_scenarios":4,"runtime_timing_owner":"boring-cdc-m6-failure-matrix"}'
