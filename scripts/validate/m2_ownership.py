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
for symbol,literal in (('MAX_COMMAND_BYTES','1024 * 1024'),('MAX_RESPONSE_BYTES','4 * 1024 * 1024'),('COMMAND_READ_TIMEOUT','Duration::from_secs(10)'),('COMMAND_WRITE_TIMEOUT','Duration::from_secs(30)')):
 p=src.find('pub const '+symbol)
 if p<0 or literal not in src[p:p+140]: errors.append('confirmed literal mismatch '+symbol)
if 'M0-PROVISIONAL: boring-cdc-d-security' in src: errors.append('reconciled security marker remains')
for required in ('SourceLockSession','OwnershipGuard','AdminCredential','RequestWriter','expected_run_id: Option<String>','OfflineDryRun','OfflineConfirm','libc::SO_PEERCRED','open_directory_components_nofollow'):
 if required not in src: errors.append('missing reusable boundary '+required)
if 'owner_uid' in src: errors.append('caller-supplied owner UID remains in authorization path')
for script, mode in [('scripts/e2e/m2_ownership.sh','e2e'),('scripts/faults/m2_ownership.sh','fault')]:
 text=(root/script).read_text()
 if text.count(f'm2_ownership_component.py {mode}') != 2: errors.append(f'{script} must run component probe twice')
print(json.dumps({'schema_version':'validation-result/v1','validator':'m2-ownership/v1','valid':not errors,'findings':errors},sort_keys=True,separators=(',',':')))
raise SystemExit(bool(errors))
