#!/usr/bin/env python3
import json,re,sys
from pathlib import Path
root=Path(__file__).resolve().parents[2]
src=(root/'src/m2_ownership.rs').read_text()
c=json.loads((root/'contracts/m2/ownership-cases.json').read_text())
errors=[]
ids=[x['id'] for x in c['cases']]
if len(ids)!=len(set(ids)): errors.append('duplicate scenario IDs')
for x in c['cases']:
 if not re.search(r'fn\s+'+re.escape(x['test'])+r'\s*\(',src): errors.append('missing test '+x['test'])
for symbol in ('MAX_COMMAND_BYTES','COMMAND_TIMEOUT'):
 p=src.find('pub const '+symbol)
 if p<0 or 'M0-PROVISIONAL: boring-cdc-d-security' not in src[max(0,p-100):p]: errors.append('missing provisional marker '+symbol)
for required in ('SourceLockSession','OwnershipGuard','AdminCredential','RequestWriter','expected_run_id: Option<String>','OfflineDryRun','OfflineConfirm'):
 if required not in src: errors.append('missing reusable boundary '+required)
print(json.dumps({'schema_version':'validation-result/v1','validator':'m2-ownership/v1','valid':not errors,'findings':errors},sort_keys=True,separators=(',',':')))
raise SystemExit(bool(errors))
