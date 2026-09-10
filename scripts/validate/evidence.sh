#!/bin/sh
set -eu
if [ "${1:-}" = "--help" ] || [ $# -eq 0 ]; then
  echo "Usage: scripts/validate/evidence.sh INPUT [validator options]"
  echo "Deterministic evidence validation; emits one JSON result and exits nonzero on findings."
  exit $([ $# -eq 0 ] && echo 2 || echo 0)
fi
if [ -d "$1" ]; then
  root=$1
  shift
  found=0
  for manifest in "$root"/*/*/evidence.json; do
    [ -f "$manifest" ] || continue
    found=1
    python3 "$(dirname "$0")/../lib/core_validator.py" evidence "$manifest" "$@"
  done
  [ "$found" -eq 1 ] || { echo "no evidence.json manifests found under $root" >&2; exit 2; }
  case "$root" in
    */boring-cdc-m0-scaffold|boring-cdc-m0-scaffold) python3 "$(dirname "$0")/m0_scaffold_evidence.py" "$root" ;;
  esac
  exit 0
fi
exec python3 "$(dirname "$0")/../lib/core_validator.py" evidence "$@"
