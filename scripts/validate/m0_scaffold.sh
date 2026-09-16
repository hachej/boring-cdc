#!/bin/sh
set -eu
exec python3 "$(dirname "$0")/../lib/m0_scaffold.py" validate
