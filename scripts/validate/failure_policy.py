#!/usr/bin/env python3
import json,re,sys
from pathlib import Path
root=Path(__file__).resolve().parents[2]
src=(root/'src/failure_policy.rs').read_text()
contract=json.loads((root/'contracts/m2/failure-policy-cases.json').read_text())
errors=[]
if contract.get('owner_bead')!='boring-cdc-m2.1': errors.append('wrong owner')
if contract.get('fixed_seed')!='failure-policy-v1': errors.append('wrong fixed seed')
ids=[c['id'] for c in contract.get('cases',[])]
if len(ids)!=len(set(ids)): errors.append('duplicate scenario IDs')
for case in contract.get('cases',[]):
 if not re.search(r'fn\s+'+re.escape(case['test'])+r'\s*\(',src): errors.append('missing test '+case['test'])
for name,value in [('BASE_DELAY_MS',1000),('MAX_DELAY_MS',300000),('MAX_ATTEMPTS',8),('JITTER_BASIS_POINTS',2000)]:
 match=re.search(rf'pub const {name}: [^=]+ = ([0-9_]+)',src)
 if not match or int(match.group(1).replace('_',''))!=value: errors.append('literal mismatch '+name)
 if match and 'M0-PROVISIONAL: boring-cdc-m2.1' not in src[max(0,match.start()-100):match.start()]: errors.append('missing provisional marker '+name)
for required in ['pub fn transition(','pub fn build_fingerprint(','pub fn load_failure(','pub enum PreparedFailureOperation','pub trait DomainRecoveryHook','fn complete_policy_vector_inventory_uses_bounded_harness_schedules(']:
 if required not in src: errors.append('missing production surface '+required)
if 'String>' in re.search(r'pub context: ([^\n]+)',src).group(1): errors.append('fingerprint context accepts arbitrary strings')
if 'format!(\"capture|' in src: errors.append('boundary canonicalization uses ambiguous delimiter join')
fence='current_failure_id=?2 AND capture_epoch=?3 AND generation=?4'
if src.count(fence)<2: errors.append('missing destination epoch/generation CAS for rearm and clear')
if 'rearm-token=' not in src: errors.append('missing durable rearm-token consumption')
print(json.dumps({'schema_version':'validation-result/v1','validator':'failure-policy/v1','valid':not errors,'findings':errors},sort_keys=True,separators=(',',':')))
raise SystemExit(bool(errors))
