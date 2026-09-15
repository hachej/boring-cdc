#!/bin/sh
set -eu
if [ "${1:-}" = "--help" ] || [ $# -eq 0 ]; then
  echo "Usage: scripts/validate/m0_decisions.sh INPUT [validator options]"
  echo "Deterministic decisions validation; emits one JSON result and exits nonzero on findings."
  exit $([ $# -eq 0 ] && echo 2 || echo 0)
fi
exec python3 "$(dirname "$0")/../lib/core_validator.py" decisions "$@"
