#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$ROOT"
python3 scripts/lib/m0_scaffold.py execute
scripts/validate/evidence.sh artifacts/boring-cdc-m0-scaffold >/dev/null
