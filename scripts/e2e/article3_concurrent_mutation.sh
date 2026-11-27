#!/usr/bin/env bash
set -euo pipefail
export BCDC_ARTICLE3_MUTATION=1
exec "$(dirname "$0")/article3_bootstrap.sh"
