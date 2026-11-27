#!/usr/bin/env bash
set -euo pipefail
exec "$(dirname "$0")/article3_bootstrap.sh" --kill-backfill
