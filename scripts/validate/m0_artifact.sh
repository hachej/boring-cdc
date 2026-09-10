#!/bin/sh
set -eu
if [ "${1:-}" = "--help" ] || [ $# -eq 0 ]; then
  echo "Usage: scripts/validate/m0_artifact.sh INPUT [validator options]"
  echo "Deterministic artifact validation; emits one JSON result and exits nonzero on findings."
  exit $([ $# -eq 0 ] && echo 2 || echo 0)
fi
case "$1" in
  boring-cdc-m0-event-format)
    shift
    [ $# -eq 0 ] || { echo "event-format validator takes no extra options" >&2; exit 2; }
    exec python3 "$(dirname "$0")/event_format.py"
    ;;
  boring-cdc-m0-pg-contract)
    shift
    [ $# -eq 0 ] || { echo "postgres-contract validator takes no extra options" >&2; exit 2; }
    exec python3 "$(dirname "$0")/postgres_contract.py"
    ;;
esac
exec python3 "$(dirname "$0")/../lib/core_validator.py" artifact "$@"
