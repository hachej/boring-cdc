#!/bin/sh
set -eu
export TMPDIR="${TMPDIR:-/var/tmp}"
case "${1:---write}" in
  --write|--verify|--probe) mode=$1 ;;
  *) echo "usage: $0 [--write|--verify|--probe]" >&2; exit 2 ;;
esac
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$root"
exec python3 scripts/validate/m0_completeness.py "$mode"
