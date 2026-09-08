#!/usr/bin/env python3
import hashlib,json,sys
from pathlib import Path
root,path=sys.argv[1],Path(sys.argv[2]); raw=path.read_bytes(); findings=[]; rows=[]
try:
 for n,line in enumerate(raw.splitlines()): rows.append(json.loads(line))
except Exception as e: findings.append({'code':'E_JSONL_MALFORMED','pointer':f'/lines/{n}','owner_bead':'boring-cdc-m0.1','message':str(e)})
by={r.get('id'):r for r in rows}; seen=set(); stack=[root]
if root not in by: findings.append({'code':'E_ROOT_MISSING','pointer':'/root','owner_bead':'boring-cdc-m0.1','message':f'missing root {root}'})
while stack:
 x=stack.pop()
 if x in seen: continue
 seen.add(x); row=by.get(x,{})
 if row.get('status')!='closed': findings.append({'code':'E_CLOSE_BLOCKED','pointer':f'/issues/{x}/status','owner_bead':x if x in by else 'boring-cdc-m0.1','message':'root or blocking prerequisite is not closed'})
 for d in row.get('dependencies',[]):
  if d.get('type')=='blocks': stack.append(d.get('depends_on_id'))
findings.sort(key=lambda x:(x['pointer'],x['code']))
out={'schema_version':'validation-result/v1','validator_version':'core-validators/1.0.0','owner_bead':'boring-cdc-m0.1','status':'fail' if findings else 'pass','input_sha256':hashlib.sha256(raw).hexdigest(),'findings':findings}
print(json.dumps(out,sort_keys=True,separators=(',',':'))); raise SystemExit(bool(findings))
