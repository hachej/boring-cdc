#!/bin/sh
set -eu
[ "${1:-}" != --help ] || { echo 'Usage: scripts/validate/test_knowledge.sh [SEED]'; exit 0; }
seed=${1:-m0-knowledge-v1}; [ "$seed" = m0-knowledge-v1 ] || { echo E_SEED >&2; exit 2; }
python3 -m unittest -q tests.validate_knowledge >/dev/null 2>&1
printf 'm0 knowledge targeted pass seed=%s tests=11\n' "$seed"
