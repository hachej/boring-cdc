#!/bin/sh
set -eu
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CASE_ID=${1:-all}
exec python3 - "$ROOT" "$CASE_ID" <<'PY'
import hashlib,json,re,subprocess,sys
from pathlib import Path
root=Path(sys.argv[1]); selected=sys.argv[2]
owner='boring-cdc-d-failure-policy'; did='DEC-SHARED-FAILURE-RETRY-POLICY'
contract_rel='contracts/m0/failure-policy.json'; fixture_rel='fixtures/m0/decisions/boring-cdc-d-failure-policy.json'
executors=['boring-cdc-m2.1','boring-cdc-m2-ownership','boring-cdc-m2-capture-runtime','boring-cdc-m2-jsonl','boring-cdc-m4-durability','boring-cdc-m5-loops']
states=['running','retry_wait','capture_safe_stopped','destination_degraded','requires_operator','requires_reseed','fatal']
classes=['transient_io','transient_source','transient_destination','rate_limited','integrity','unsupported','ownership_lost','configuration']; auto=classes[:4]
seed='424344435f52455452595f563031'; M=0xffffffff
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def rol(v,n):return ((v<<n)&M)|(v>>(32-n))
def qr(x,a,b,c,d):
 x[a]=(x[a]+x[b])&M;x[d]=rol(x[d]^x[a],16);x[c]=(x[c]+x[d])&M;x[b]=rol(x[b]^x[c],12);x[a]=(x[a]+x[b])&M;x[d]=rol(x[d]^x[a],8);x[c]=(x[c]+x[d])&M;x[b]=rol(x[b]^x[c],7)
def draw(a):
 key=hashlib.sha256(bytes.fromhex(seed)).digest(); z=b'expand 32-byte k'; w=[int.from_bytes(z[i:i+4],'little') for i in range(0,16,4)]+[int.from_bytes(key[i:i+4],'little') for i in range(0,32,4)]+[a,0,0,0];x=w[:]
 for _ in range(10):
  qr(x,0,4,8,12);qr(x,1,5,9,13);qr(x,2,6,10,14);qr(x,3,7,11,15);qr(x,0,5,10,15);qr(x,1,6,11,12);qr(x,2,7,8,13);qr(x,3,4,9,14)
 return int.from_bytes(b''.join(((x[i]+w[i])&M).to_bytes(4,'little') for i in range(16))[:8],'little')
def delay(a):return min(30000,250*2**(a-1))
def jitter(a):return draw(a)*(delay(a)+1)>>64
def fail(code='FAILURE_POLICY_FIXTURE_INVALID'):
 print(json.dumps({'code':code,'outcome':'fail','phase':'validate_spec'},sort_keys=True,separators=(',',':')));raise SystemExit(1)
try:
 c=json.loads((root/contract_rel).read_text()); f=json.loads((root/fixture_rel).read_text()); decisions=json.loads((root/'contracts/m0/decisions.json').read_text()); artifacts=json.loads((root/'contracts/m0/artifacts.json').read_text()); registry=json.loads((root/'contracts/agent/stable-ids.json').read_text()); coverage=json.loads((root/'contracts/coverage/plan-to-beads.json').read_text()); graph=[json.loads(x) for x in (root/'.beads/issues.jsonl').read_text().splitlines()]
 if c['schema_version']!='failure-policy-contract/v1' or c['policy_version']!=1 or c['decision_id']!=did or c['owner_bead']!=owner:fail()
 if c['states']!=states or c['failure_classes']!=classes or c['exhaustive_match_required'] is not True:fail()
 if c['schedule']!={'attempt_semantics':'attempt is the persisted count of failed executions for this incident; failures producing attempts 1 through 9 schedule retry; failure producing attempt 10 transitions to requires_operator','attempt_type':'u16','base_ms':250,'multiplier':2,'cap_ms':30000,'maximum_attempts':10,'raw_delay_formula':'min(30000, 250 * 2^(attempt-1)) milliseconds','jitter_distribution':'full_jitter_inclusive_[0,raw_delay_ms]','production_randomness':'OS_CSPRNG','test_randomness':{'algorithm':'ChaCha20-IETF 20 rounds','seed_hex':'0x'+seed,'key_derivation':'SHA-256(seed bytes)','nonce_hex':'0x000000000000000000000000','block_counter':'attempt u32','sample':'first 8 block bytes as little-endian u64','range_mapping':'floor(sample * (raw_delay_ms + 1) / 2^64)'}}:fail()
 expected_targets={'transient_io':'retry_wait','transient_source':'retry_wait','transient_destination':'retry_wait','rate_limited':'retry_wait','integrity':'requires_reseed','unsupported':'fatal','ownership_lost':'capture_safe_stopped','configuration':'component_safe_state'}
 if {x['class']:x['initial_failure_target'] for x in c['class_transitions']}!=expected_targets or [x['class'] for x in c['class_transitions'] if x['auto_retry']]!=auto:fail()
 fields={x[0]:(x[1],x[2]) for x in c['persisted_record']['fields']}
 required_fields={'incident_id','policy_version','state','class','component','first_seen_utc','attempt','next_retry_utc','monotonic_remaining_ms','last_code','fingerprint','relevant_config_fingerprint','failed_boundary','consumed_rearm_nonce'}
 if set(fields)!=required_fields or fields['attempt']!=('u16',False) or fields['next_retry_utc']!=('RFC3339 UTC timestamp',True) or c['persisted_record']['optionals']!='every optional field is present as explicit null':fail()
 if [x['kind'] for x in c['failed_boundary']['variants']]!=['capture','destination','ownership'] or c['failed_boundary']['cross_epoch_retry'] is not False:fail()
 fp=c['fingerprint']; allow={'relation_id','destination_id','operation','SQLSTATE','errno','HTTP status','generation'}
 if fp['version']!=1 or fp['hash']!='SHA-256' or fp['canonicalization']!='RFC 8785 JCS UTF-8' or set(fp['context_allowlist'])!=allow or not {'payloads','DSNs','tokens','source identifiers','absolute paths','messages','credentials'}.issubset(fp['excluded']):fail()
 if c['rearm']['result_enum']!=['rearmed','stale','conflict','forbidden'] or set(c['rearm']['recovery_predicates'])!={'transient_exhausted','configuration','integrity','ownership_lost','unsupported'}:fail()
 if c['component_safe_states']!={'capture':'capture_safe_stopped','clickhouse':'destination_degraded','archive':'destination_degraded'}:fail()
 if c['expected_close']['token_fields']!=['run_id','capture_generation','advisory_session_id'] or 'no reconnect' not in c['expected_close']['persistence_failure'] or c['expected_close']['reconnect'].split(';')[0]!='never in-process':fail()
 if c['observability']['metric']!={'name':'boring_cdc_failures_total','labels':['component','class','code'],'increment':'once per persisted failure occurrence'} or c['observability']['code_pattern']!='BCDC_<DOMAIN>_<CAUSE>':fail()
 if c['later_executors']!=executors or set(c['executor_ownership'])!=set(executors):fail()
 if f['schema_version']!='m0-decision-fixture/v1' or f['fixture_id']!=did or f['owner_bead']!=owner or f['contract_sha256']!=sha(root/contract_rel) or f['fixed_seed']!={'ascii':'BCDC_RETRY_V01','hex':'0x'+seed} or f['later_executors']!=executors:fail()
 vectors=f['golden_vectors']; ids=[v['case_id'] for v in vectors]
 required_ids={'class-'+x for x in classes}|{f'schedule-attempt-{x}' for x in (1,7,8,9,10)}|{'restart-preserves-attempt','clock-forward-does-not-shorten','clock-backward-extends','timer-at-boundary','same-fingerprint-suppressed','different-fingerprint-new-incident','unrelated-config-forbidden','relevant-config-rearmed','rearm-stale-nonce','rearm-boundary-conflict','integrity-without-reseed-forbidden','integrity-new-epoch-rearmed','stale-completion','cross-epoch-retry-forbidden','capture-hook-safe-stop','destination-hook-degraded','expected-close-delayed-eof','expected-close-unmatched-eof','expected-close-persistence-failure','expected-close-advisory-loss-race','redaction-excludes-secret-context'}
 if len(ids)!=len(set(ids)) or set(ids)!=required_ids or f['vector_inventory']!={'classes':8,'total':34,'required_kinds':['clock','completion','expected_close','failure','fingerprint','hook','rearm','redaction','restart','schedule','timer']}:fail()
 by={v['case_id']:v for v in vectors}
 for a in (1,7,8,9,10):
  if by[f'schedule-attempt-{a}']['expected']!={'raw_delay_ms':delay(a),'jitter_ms':jitter(a),'inclusive_range':[0,delay(a)]}:fail()
 for x in classes:
  e=by['class-'+x]['expected']
  if e.get('checkpoint_advance') is not False or e.get('feedback_advance') is not False:fail()
 if by['class-transient_io']['expected']['state']!='retry_wait' or by['class-integrity']['expected']['state']!='requires_reseed' or by['class-unsupported']['expected']['state']!='fatal':fail()
 if by['restart-preserves-attempt']['expected']['attempt']!=6 or by['clock-forward-does-not-shorten']['expected']['effective_remaining_ms']!=19000 or by['clock-backward-extends']['expected']['effective_remaining_ms']!=25000:fail()
 if by['expected-close-delayed-eof']['expected']!={'state':'capture_safe_stopped','class':None,'reconnect':False} or by['expected-close-advisory-loss-race']['expected']['class']!='ownership_lost':fail()
 if selected!='all' and selected not in by:fail('FAILURE_POLICY_CASE_UNKNOWN')
 graph_ids=[x['id'] for x in graph]
 if len(graph_ids)!=len(set(graph_ids)) or set(executors)-set(graph_ids):fail()
 stable=next(x for x in registry['entries'] if x['id']==did); covered=next(x for x in coverage['assignments'] if x['id']==did)
 if stable['owner_bead']!=owner or covered!={'evidence_status':'pending','id':did,'owner_bead':owner,'source':'docs/PLAN.md','source_digest':stable['source_digest']}:fail()
 if subprocess.run([str(root/'scripts/validate/plan_coverage.sh')],cwd=root,capture_output=True).returncode:fail()
 proposed='Version 1. States running,retry_wait,capture_safe_stopped,destination_degraded,requires_operator,requires_reseed,fatal. Classes transient_io,transient_source,transient_destination,rate_limited,integrity,unsupported,ownership_lost,configuration; only first four auto-retry. Base 250 ms, multiplier 2, cap 30 s, maximum 10 attempts then requires_operator. Full jitter [0,delay]; OS CSPRNG production; ChaCha20 seed 0x424344435f52455452595f563031 tests. Persist UUIDv7 incident,class,component,first_seen UTC,attempt u16,next_retry UTC,monotonic_remaining_ms,last_code,fingerprint,failed_boundary; optionals explicit null. Clock changes never shorten delay. Fingerprint v1 SHA-256 of RFC8785 JCS typed allowlisted context: relation_id,destination_id,operation,SQLSTATE,errno,HTTP status,generation; exclude secrets/paths/messages. Re-arm requires fresh nonce and matching fingerprint/boundary; results rearmed,stale,conflict,forbidden. Expected-close binds run_id/capture generation; mismatch ownership_lost. Codes BCDC_<DOMAIN>_<CAUSE>; metric boring_cdc_failures_total{component,class,code}.'
 d=next(x for x in decisions['decisions'] if x['id']==did)
 approval={'approved_at':'2026-09-10T14:03:30.949Z','approved_by':'Julien Hurault (repository owner), intention 765bd3b2-4b68-4102-a9ec-43ca93357390','value_digest':hashlib.sha256(proposed.encode()).hexdigest()}
 if d!={'approval':approval,'executor_beads':executors,'fixture_sha256':sha(root/fixture_rel),'fixture_spec':fixture_rel,'id':did,'owner_bead':owner,'proposed_value':proposed,'status':'approved'}:fail()
 probe_rel='artifacts/m0/decisions/boring-cdc-d-failure-policy/fixture-run.jsonl'; probe=[json.loads(x) for x in (root/probe_rel).read_text().splitlines()]
 if probe!=[f['expected_probe']]:fail()
 needed={'ART-M0-FAILURE-POLICY-CONTRACT':contract_rel,'ART-M0-FAILURE-POLICY-FIXTURE':fixture_rel,'ART-M0-FAILURE-POLICY-PROBE':probe_rel,'ART-M0-FAILURE-POLICY-VALIDATION':'artifacts/m0/decisions/boring-cdc-d-failure-policy/evidence.json'}
 owned={x['id']:x for x in artifacts['artifacts'] if x.get('owner_bead')==owner}
 if set(owned)!=set(needed):fail()
 for ident,path in needed.items():
  if owned[ident]!={'id':ident,'owner_bead':owner,'path':path,'sha256':sha(root/path),'status':'complete'}:fail()
 ev=json.loads((root/needed['ART-M0-FAILURE-POLICY-VALIDATION']).read_text())
 if ev.get('schema_version')!='validation-result/v1' or ev.get('status')!='pass' or ev.get('findings')!=[] or ev.get('input_sha256')!=sha(root/'contracts/m0/decisions.json') or not re.fullmatch('[0-9a-f]{40}',ev.get('git_commit','')):fail()
 guarded=[contract_rel,fixture_rel,'scripts/validate/failure_policy.sh','contracts/m0/decisions.json']
 if subprocess.run(['git','cat-file','-e',ev['git_commit']+'^{commit}'],cwd=root,capture_output=True).returncode or subprocess.run(['git','merge-base','--is-ancestor',ev['git_commit'],'HEAD'],cwd=root,capture_output=True).returncode or subprocess.run(['git','diff','--quiet',ev['git_commit']+'..HEAD','--',*guarded],cwd=root).returncode:fail()
except (OSError,KeyError,ValueError,TypeError,StopIteration,json.JSONDecodeError):fail()
if selected=='all':print(json.dumps({'code':'FAILURE_POLICY_FIXTURE_VALID','class_count':8,'outcome':'pass','phase':'validate_spec','vector_count':34},sort_keys=True,separators=(',',':')))
else:print(json.dumps({'case_id':selected,'code':'FAILURE_POLICY_CASE_VALID','outcome':'pass','phase':'validate_spec'},sort_keys=True,separators=(',',':')))
PY
