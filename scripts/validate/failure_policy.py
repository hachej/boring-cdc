#!/usr/bin/env python3
import json,re,sys
from pathlib import Path
root=Path(__file__).resolve().parents[2]
src=(root/'src/failure_policy.rs').read_text()
contract=json.loads((root/'contracts/m2/failure-policy-cases.json').read_text())
errors=[]
if contract.get('owner_bead')!='boring-cdc-m2.1': errors.append('wrong owner')
if contract.get('fixed_seed')!='0x424344435f52455452595f563031': errors.append('wrong fixed seed')
ids=[c['id'] for c in contract.get('cases',[])]
if len(ids)!=len(set(ids)): errors.append('duplicate scenario IDs')
expected_assertions={
 'SCN-M2-FAILURE-ENUM':'all eight confirmed failure classes and all eight public stable error codes serialize and round-trip through their exact closed representations; only the four transient/rate classes auto-retry',
 'SCN-M2-FAILURE-SCHEDULE':'approved zero-padded ChaCha20 seed expansion and one next_u64 draw per ascending attempt match exact multi-attempt full-jitter golden vectors, with exponential cap and monotonic clock rollback handling',
 'SCN-M2-FAILURE-JITTER-GOLDEN':'the 14 decoded seed bytes occupy the leading bytes of the 32-byte ChaCha20 seed with 18 trailing zero bytes, then exactly one next_u64 draw is consumed for each ascending attempt',
 'SCN-M2-FAILURE-REARM':'integrity, ownership_lost, configuration, and unsupported classes require their typed stronger proof',
 'SCN-M2-FAILURE-HOOKS':'synthetic typed domain hook accepts expected close only for matching run_id and connection generation',
}
for case_id,assertion in expected_assertions.items():
 case=next((item for item in contract.get('cases',[]) if item.get('id')==case_id),None)
 if not case or case.get('assertion')!=assertion: errors.append('stale assertion '+case_id)
for case in contract.get('cases',[]):
 if not re.search(r'fn\s+'+re.escape(case['test'])+r'\s*\(',src): errors.append('missing test '+case['test'])
expected_codes=[
 'BCDC_SHARED_TRANSPORT_UNAVAILABLE','BCDC_SHARED_DEADLINE_EXCEEDED',
 'BCDC_SHARED_INVALID_RECORD','BCDC_SHARED_CHECKSUM_MISMATCH',
 'BCDC_SHARED_HISTORY_UNAVAILABLE','BCDC_SHARED_UNSUPPORTED_CONFIGURATION',
 'BCDC_SHARED_RESOURCE_LIMIT','BCDC_SHARED_OPERATOR_PAUSE',
]
if contract.get('confirmed_literals',{}).get('stable_error_codes')!=expected_codes: errors.append('wrong stable error code contract')
for code in expected_codes:
 if f'#[serde(rename = "{code}")]' not in src: errors.append('missing public serde stable code '+code)
seed_expansion=contract.get('confirmed_literals',{}).get('jitter_seed_expansion',{})
if seed_expansion != {
 'decoded_ascii':'BCDC_RETRY_V01',
 'decoded_hex':'424344435f52455452595f563031',
 'expanded_32_bytes_hex':'424344435f52455452595f563031'+'00'*18,
 'rule':'decoded bytes copied at offset 0; remaining trailing bytes zero-filled',
}: errors.append('wrong ChaCha20 seed expansion contract')
expected_vectors=[
 (1,2469525849052024087,250,132),(2,14682752550997020102,500,273),
 (3,6383775579002771793,1000,844),(4,18292979100849785532,2000,204),
 (5,12610900134770089858,4000,3694),(6,12413990053327801956,8000,7521),
 (7,13472926905698362994,16000,8489),(8,6063425432550872070,30000,11847),
 (9,15583386212084045179,30000,12137),(10,5276732411777251899,30000,29910),
]
actual_vectors=[(v.get('attempt'),v.get('sample_u64'),v.get('nominal_delay_ms'),v.get('delay_ms')) for v in contract.get('confirmed_literals',{}).get('jitter_golden_vectors',[])]
if actual_vectors!=expected_vectors: errors.append('wrong ChaCha20 jitter golden vectors')
if contract.get('confirmed_literals',{}).get('jitter_draw_order')!='one ChaCha20Rng::next_u64 draw per attempt in ascending attempt order before inclusive modulo': errors.append('wrong ChaCha20 draw order')
for seam in ('ChaCha20Rng::from_seed(approved_test_seed())','let expanded = approved_test_seed();','let mut randomness = ApprovedTestRandomness::seeded();','let sample = randomness.next_u64();'):
 if seam not in src: errors.append('golden vectors bypass approved randomness seam: '+seam)
for attempt,sample,nominal,delay in expected_vectors:
 for literal in (f'({attempt}, {sample:_}', f'{nominal:_}, {delay:_})'):
  if literal not in src: errors.append('missing Rust jitter golden literal '+literal)
for name,value in [('BASE_DELAY_MS',250),('MAX_DELAY_MS',30000),('MAX_ATTEMPTS',10)]:
 match=re.search(rf'pub const {name}: [^=]+ = ([0-9_]+)',src)
 if not match or int(match.group(1).replace('_',''))!=value: errors.append('literal mismatch '+name)
if 'M0-PROVISIONAL: boring-cdc-m2.1' in src: errors.append('reconciled failure-policy marker remains')
for literal in ('0x424344435f52455452595f563031','sample % nominal.saturating_add(1)','transient_io','transient_source','transient_destination','rate_limited','ownership_lost','BCDC_SHARED_TRANSPORT_UNAVAILABLE','ChaCha20Rng','ApprovedTestRandomness'):
 if literal not in src: errors.append('missing confirmed failure-policy literal '+literal)
for required in ['pub fn transition(','pub fn build_fingerprint(','pub fn load_failure(','pub enum PreparedFailureOperation','pub trait DomainRecoveryHook','fn complete_policy_vector_inventory_uses_bounded_harness_schedules(']:
 if required not in src: errors.append('missing production surface '+required)
if 'String>' in re.search(r'pub context: ([^\n]+)',src).group(1): errors.append('fingerprint context accepts arbitrary strings')
if 'format!(\"capture|' in src: errors.append('boundary canonicalization uses ambiguous delimiter join')
fence='current_failure_id=?2 AND capture_epoch=?3 AND generation=?4'
if src.count(fence)<2: errors.append('missing destination epoch/generation CAS for rearm and clear')
if 'rearm-token=' not in src: errors.append('missing durable rearm-token consumption')
print(json.dumps({'schema_version':'validation-result/v1','validator':'failure-policy/v1','valid':not errors,'findings':errors},sort_keys=True,separators=(',',':')))
raise SystemExit(bool(errors))
