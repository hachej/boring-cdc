#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
generated=$(mktemp); trap 'rm -f "$generated"' EXIT HUP INT TERM
cargo run --quiet --locked --example m1_cli_contract >"$generated"
cmp "$generated" tests/fixtures/m1_cli/registry-v1.json
cargo run --quiet --locked --example m1_cli_contract -- fixture >"$generated"
cmp "$generated" tests/fixtures/m1_cli/envelopes-v1.json
python3 - tests/fixtures/m1_cli/registry-v1.json <<'PY'
import json,sys
items=json.load(open(sys.argv[1]))
assert len(items)==28
assert len({x['id'] for x in items})==len(items)
assert len({(tuple(x['path']),x['operation_variant']) for x in items})==len(items)
assert all(x['owner_bead'].startswith('boring-cdc-') for x in items)
assert all(x['exit_codes'] and x['redaction'] for x in items)
PY
for owner in $(python3 -c 'import json; print(" ".join(sorted({x["owner_bead"] for x in json.load(open("tests/fixtures/m1_cli/registry-v1.json"))})))'); do
 br show "$owner" >/dev/null
done
printf 'm1 cli registry pass schema=1 commands=28 owners=resolvable golden=exact cleanup=complete\n'
