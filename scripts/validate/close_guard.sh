#!/bin/sh
set -eu
if [ "${1:-}" = "--help" ] || [ $# -eq 0 ]; then
  echo "Usage: scripts/validate/close_guard.sh ROOT_BEAD [GRAPH_JSONL]"
  echo "Fail unless root and every blocking prerequisite are closed in the captured graph."
  exit $([ $# -eq 0 ] && echo 2 || echo 0)
fi
exec python3 "$(dirname "$0")/../lib/close_guard.py" "$1" "${2:-.beads/issues.jsonl}"
