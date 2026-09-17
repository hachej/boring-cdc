#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
python3 scripts/validate/failure_policy.py
cargo test --locked failure_policy::tests
