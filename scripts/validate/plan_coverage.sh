#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
exec python3 - "$ROOT" "$@" <<'PY'
import hashlib,json,sys
from pathlib import Path
root=Path(sys.argv[1]);sys.path.insert(0,str(root/'scripts/lib'))
from agent_context import validate
if len(sys.argv)>2 and sys.argv[2]=='--help':
 print('usage: scripts/validate/plan_coverage.sh');raise SystemExit(0)
f=validate(); prov=json.load(open(root/'contracts/coverage/plan-to-beads.provenance.json')); got=hashlib.sha256(open(root/'contracts/coverage/plan-to-beads.json','rb').read()).hexdigest()
if got!=prov.get('generated_digest'):f.append(['E_GENERATED_VIEW_DRIFT','contracts/coverage/plan-to-beads.json'])
out={'schema_version':'validation-result/v1','validator':'plan-coverage/v1','git_commit':__import__('subprocess').run(['git','rev-parse','HEAD'],cwd=root,text=True,capture_output=True).stdout.strip(),'valid':not f,'findings':[{'code':c,'pointer':p if p.startswith('/') else '/'+p,'owner_bead':'boring-cdc-m0.2','message':c} for c,p in sorted(f)]}
print(json.dumps(out,sort_keys=True,separators=(',',':')));raise SystemExit(bool(f))
PY
