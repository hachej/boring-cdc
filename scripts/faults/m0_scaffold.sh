#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$ROOT"
python3 - <<'PY'
import json
p=json.load(open('fixtures/m0/scaffold/scenarios.json'))
rows={x['id']:x for x in p['scenarios']}
assert rows['SCN-M0-SCAFFOLD-DIGEST-MISMATCH']['expected_exit']==78
assert rows['SCN-M0-SCAFFOLD-DEPENDENCY-DELAY']['expected_exit']==75
assert rows['SCN-M0-SCAFFOLD-ZOMBIE-BOUND']['expected_status']=='pass'
assert rows['SCN-M0-SCAFFOLD-AGENT-READONLY']['external_effect']=='none'
compose=open('compose.yaml').read()
assert 'tcp_keepalives_idle=30' in compose and 'tcp_keepalives_interval=10' in compose and 'tcp_keepalives_count=3' in compose
assert 'restart: unless-stopped' in compose and 'condition: service_healthy' in compose
print('{"status":"pass","scenarios":4,"runtime_timing_owner":"boring-cdc-m6-failure-matrix"}')
PY
