#!/usr/bin/env bash
set -euo pipefail
python3 - <<'PY'
import json,re
c=json.load(open('contracts/m1/preflight-cases.json'))
assert c['owner_bead']=='boring-cdc-m1-preflight'
text=open('src/m1_preflight.rs').read()
for case in c['cases']:
 scenario=case['id']; reason=case['blocked_or_unknown_reason']
 assert scenario in text, scenario
 assert reason in text, reason
 assert case['state_assertion']=='before_state_sha256 == after_state_sha256'
assert '// M0-PROVISIONAL:' not in text
print(f"m1 preflight contract: PASS ({len(c['cases'])} cases)")
PY
