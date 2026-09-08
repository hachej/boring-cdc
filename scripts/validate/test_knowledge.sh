#!/bin/sh
set -eu
[ "${1:-}" != --help ] || { echo 'Usage: scripts/validate/test_knowledge.sh [SEED]'; exit 0; }
seed=${1:-m0-knowledge-v1}; [ "$seed" = m0-knowledge-v1 ] || { echo E_SEED >&2; exit 2; }
exec python3 -m unittest -v tests.validate_knowledge
