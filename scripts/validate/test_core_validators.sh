#!/bin/sh
set -eu
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/validate/test_core_validators.sh [SEED]'; exit 0; }
seed=${1:-m0-core-v1}; [ "$seed" = m0-core-v1 ] || { echo 'E_SEED: expected m0-core-v1' >&2; exit 2; }
exec python3 -m unittest -v tests.validate_core_validators
