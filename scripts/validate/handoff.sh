#!/bin/sh
set -eu
if [ "${1:-}" = --help ] || [ $# -eq 0 ]; then echo 'Usage: scripts/validate/handoff.sh INPUT [options]'; exit $([ $# -eq 0 ] && echo 2 || echo 0); fi
exec python3 "$(dirname "$0")/../lib/knowledge_validator.py" handoff "$@"
