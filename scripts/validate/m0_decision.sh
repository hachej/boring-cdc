#!/bin/sh
set -eu
if [ "${1:-}" = "--help" ] || [ $# -eq 0 ]; then
  echo "Usage: scripts/validate/m0_decision.sh INPUT-OR-BEAD-ID [validator options]"
  echo "Deterministic decision validation; emits one JSON result and exits nonzero on findings."
  exit $([ $# -eq 0 ] && echo 2 || echo 0)
fi
input=$1
shift
case "$input" in
  boring-cdc-*)
    if ! python3 - "$input" <<'PY'
import json, sys
rows = json.load(open("contracts/m0/decisions.json"))["decisions"]
raise SystemExit(0 if any(row.get("owner_bead") == sys.argv[1] for row in rows) else 1)
PY
    then
      echo "Unknown or unmaterialized decision Bead: $input" >&2
      exit 2
    fi
    input=contracts/m0/decisions.json
    ;;
esac
exec python3 "$(dirname "$0")/../lib/core_validator.py" decision "$input" "$@"
