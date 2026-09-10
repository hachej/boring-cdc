#!/bin/sh
set -eu
python3 - <<'PY'
import pathlib
cargo=pathlib.Path('Cargo.toml').read_text()
license=pathlib.Path('LICENSE').read_text()
assert 'license = "Apache-2.0"' in cargo
assert 'Apache License' in license and 'Version 2.0' in license
print('{"status":"pass","license":"Apache-2.0"}')
PY
