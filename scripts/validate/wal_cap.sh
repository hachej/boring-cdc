#!/bin/sh
set -eu
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CASE_ID=${1:-all}
exec python3 - "$ROOT" "$CASE_ID" <<'PY'
import hashlib,json,re,subprocess,sys
from pathlib import Path
root=Path(sys.argv[1]); selected=sys.argv[2]
owner='boring-cdc-d-wal-cap'; decision_id='DEC-WAL-CAP'
fixture_rel='fixtures/m0/decisions/boring-cdc-d-wal-cap.json'
executors=['boring-cdc-m2-pressure','boring-cdc-m6-safety-sm','boring-cdc-m6-metrics']
proposed='max_slot_wal_keep_size=64 GiB (68,719,476,736 bytes); raw headroom=max(0,min(cap-retained_slot_bytes,source_free_bytes-16 GiB)); WAL rate=nearest-rank p95 (rank 15 after ascending sort) over exactly 15 complete 1-minute byte/s buckets, stale after 3 minutes; fresh zero rate gives infinite horizon only with positive headroom; raw horizon=floor(raw_headroom/rate), safety horizon=max(0,raw_horizon-30-second monitor delay-120-second reaction reserve); warning <=60 minutes, action_required <=30 minutes, critical <=10 minutes on safety horizon with equality more severe; missing/unsupported/stale inputs produce unknown, block new bootstrap/backfill, and preserve healthy capture; local-only --unsafe-unbounded-slot-wal expires at 24 hours, requires confirmation UNBOUNDED_WAL_LOCAL_ONLY and actor/time/config-digest audit, and is forbidden remotely; invalidated slots require explicit reseed.'
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def fail():
 print('{"code":"WAL_CAP_FIXTURE_INVALID","outcome":"fail","phase":"validate_spec"}'); raise SystemExit(1)
def expected_eval(i):
 buckets=i['buckets']
 if i['cap']!=68719476736 or i['retained'] is None or i['free'] is None or i['retained']<0 or i['free']<0 or buckets is None or len(buckets)!=15 or any(not isinstance(x,int) or isinstance(x,bool) or x<0 for x in buckets) or i['age']>180: return ('unknown',None,None,None)
 # Nearest-rank p95 over 15 complete one-minute buckets: ceil(.95*15)=15.
 rate=sorted(buckets)[14]
 raw_headroom=max(0,min(i['cap']-i['retained'],i['free']-17179869184))
 if raw_headroom==0: raw_horizon=safety_horizon=0
 elif rate==0: raw_horizon=safety_horizon='infinite'
 else:
  raw_horizon=raw_headroom//rate
  safety_horizon=max(0,raw_horizon-30-120)
 if safety_horizon=='infinite' or safety_horizon>3600: state='normal'
 elif safety_horizon<=600: state='critical'
 elif safety_horizon<=1800: state='action_required'
 else: state='warning'
 return state,raw_headroom,raw_horizon,safety_horizon
def expected_operation(i):
 op=i.get('operation')
 if i.get('headroom_state')=='unknown':
  if op in ('new_bootstrap','new_backfill'): return ('blocked_unknown_headroom',2,'none',['WAL_HEADROOM_UNKNOWN'])
  if op=='healthy_capture': return ('capture_continues',0,'unchanged_durable_boundary',['WAL_HEADROOM_UNKNOWN'])
 if 'flag' in i:
  audit=i.get('audit',{})
  audit_ok=set(audit)=={'actor','activated_at','configuration_sha256'} and bool(audit.get('actor')) and bool(audit.get('activated_at')) and bool(re.fullmatch(r'[0-9a-f]{64}',audit.get('configuration_sha256','')))
  if i.get('origin')=='remote': return ('blocked_remote_override',2,'none',['WAL_HEADROOM_UNKNOWN'])
  if i.get('origin')!='local_process': return ('blocked_nonlocal_override',2,'none',['WAL_HEADROOM_UNKNOWN'])
  if i.get('flag')!='--unsafe-unbounded-slot-wal' or i.get('confirmation')!='UNBOUNDED_WAL_LOCAL_ONLY' or not audit_ok: return ('blocked_confirmation',2,'none',['WAL_HEADROOM_UNKNOWN'])
  if i.get('age_seconds',86400)>=86400: return ('blocked_override_expired',2,'none',['WAL_HEADROOM_UNKNOWN'])
  return ('unsafe_override_active',0,'none',['WAL_CAP_DISABLED_UNSAFE'])
 if i.get('slot_status')=='wal_removed' and i.get('automatic_action')=='none': return ('require_reseed',2,'none',['WAL_HEADROOM_CRITICAL'])
 if i.get('headroom_state')=='action_required' and i.get('predicted_catch_up') is True: return ('action_required',0,'unchanged_durable_boundary',['WAL_HEADROOM_ACTION_REQUIRED'])
 fail()
try:
 spec=json.loads((root/fixture_rel).read_text()); decisions=json.loads((root/'contracts/m0/decisions.json').read_text())
 artifacts=json.loads((root/'contracts/m0/artifacts.json').read_text()); registry=json.loads((root/'contracts/agent/stable-ids.json').read_text())
 coverage=json.loads((root/'contracts/coverage/plan-to-beads.json').read_text()); graph=[json.loads(x) for x in (root/'.beads/issues.jsonl').read_text().splitlines()]
 required=('inputs','preconditions','supported_matrix','deterministic_phase','expected','expected_failure','result_contract','redaction_assertions','later_executors','vectors','approved_boundary')
 if any(not spec.get(x) for x in required): fail()
 if spec.get('schema_version')!='m0-decision-fixture/v1' or spec.get('fixture_id')!=decision_id or spec.get('decision_id')!=decision_id or spec.get('owner_bead')!=owner or spec.get('later_executors')!=executors: fail()
 approval={'approved_at':'2026-09-10T14:03:30.949Z','approved_by':'Julien Hurault (repository owner)','intention_id':'765bd3b2-4b68-4102-a9ec-43ca93357390','selection':'Accept recommended defaults'}
 if spec.get('approval')!=approval or spec.get('fixed_seed')!='0x424344435f57414c4341505f563031': fail()
 b=spec['approved_boundary']
 if b.get('max_slot_wal_keep_size')!={'bytes':68719476736,'display':'64 GiB','required':'finite_exact'} or b.get('source_free_space_reserve_bytes')!=17179869184: fail()
 if b.get('headroom')!={'raw_bytes':'max(0, min(68719476736 - retained_slot_bytes, source_free_bytes - 17179869184))','reaction_adjusted_bytes':'max(0, raw_bytes - wal_rate_bytes_per_second * (30 + 120))','unit':'byte'}: fail()
 if b.get('wal_rate')!={'bucket_count':15,'bucket_seconds':60,'window_seconds':900,'statistic':'nearest_rank_p95','rank_formula':'ceil(0.95 * 15) = 15 after ascending sort','unit':'byte_per_second','fresh_through_age_seconds':180,'stale_after_age_seconds':180,'zero_rate_horizon':'infinite_when_fresh_and_raw_headroom_positive','zero_headroom_horizon_seconds':0}: fail()
 if b.get('monitor_interval_seconds')!=30 or b.get('reaction_reserve_seconds')!=120 or b.get('horizon')!={'raw_formula':'floor(raw_headroom_bytes / wal_rate_bytes_per_second)','safety_formula':'max(0, raw_horizon_seconds - monitor_interval_seconds - reaction_reserve_seconds)','threshold_input':'safety_horizon_seconds','rounding':'floor','unit':'second'}: fail()
 if b.get('thresholds')!=[{'horizon_seconds_lte':600,'state':'critical'},{'horizon_seconds_lte':1800,'state':'action_required'},{'horizon_seconds_lte':3600,'state':'warning'},{'horizon_seconds_gt':3600,'state':'normal'}] or b.get('threshold_precedence')!=['unknown','critical','action_required','warning','normal']: fail()
 if b.get('unknown_predicates')!=['source_free_bytes_missing_or_negative','configured_cap_missing_or_not_68719476736','retained_slot_bytes_missing_or_negative','wal_rate_buckets_missing_or_count_not_15','wal_rate_bucket_negative','wal_rate_age_seconds_greater_than_180']: fail()
 if b.get('unknown_behavior')!={'automatic_cap_increase':False,'automatic_detach_destination':False,'automatic_drop_slot':False,'fabricate_feedback':False,'healthy_capture':'continues','new_backfill':'blocked','new_bootstrap':'blocked'}: fail()
 override={'allowed_origin':'local_process','audit_fields':['actor','activated_at','configuration_sha256'],'configuration_digest':'SHA-256','confirmation':'UNBOUNDED_WAL_LOCAL_ONLY','expires_when_age_seconds_gte':86400,'flag':'--unsafe-unbounded-slot-wal','forbidden_origin':'remote','scope':'local_experiment_bootstrap_and_backfill_admission_only','status_code':'WAL_CAP_DISABLED_UNSAFE','valid_for_seconds':86400}
 if b.get('unsafe_override')!=override: fail()
 if b.get('stable_state_codes')!={'action_required':'WAL_HEADROOM_ACTION_REQUIRED','critical':'WAL_HEADROOM_CRITICAL','unknown':'WAL_HEADROOM_UNKNOWN','unsafe_override':'WAL_CAP_DISABLED_UNSAFE','warning':'WAL_HEADROOM_WARNING'}: fail()
 if b.get('metric_names')!={'headroom_bytes':'wal_headroom_bytes','horizon_seconds':'wal_headroom_horizon_seconds','rate_bytes_per_second':'wal_rate_bytes_per_second','state':'wal_headroom_state'}: fail()
 if b.get('invalidation')!={'automatic_reseed':False,'forecast_is_conditional':True,'forecast_may_override_source_safety':False,'slot_invalidated_outcome':'require_reseed'}: fail()
 vectors=spec['vectors']; matrix=spec['supported_matrix']; cases={x['case_id']:x for x in matrix}
 numeric={'normal_above_warning','warning_equality_60m','warning_above_action','action_equality_30m','action_above_critical','critical_equality_10m','critical_floor_fraction','cap_headroom_clamped_zero','free_space_is_minimum','free_headroom_clamped_zero','fresh_zero_rate_infinite','zero_headroom_zero_rate','missing_free_unknown','missing_retained_unknown','missing_rate_unknown','stale_rate_unknown','unsupported_unbounded_cap_unknown','different_finite_cap_unknown','negative_retained_unknown','negative_free_unknown'}
 operational={'unknown_blocks_new_work','unknown_blocks_new_backfill','unknown_healthy_capture_continues','local_override_valid_before_expiry','local_override_expired_at_24h','remote_override_forbidden','override_confirmation_mismatch','unsupported_origin_override_forbidden','slot_invalidation_requires_reseed','conditional_forecast_cannot_override_action'}
 if set(vectors)!=numeric|operational or set(cases)!=set(vectors) or len(matrix)!=len(vectors): fail()
 for cid,v in vectors.items():
  if v.get('case_id')!=cid or cases[cid].get('expected_outcome')!=v.get('expected_outcome') or v.get('executor_bead') not in executors or not v.get('preconditions') or not v.get('fault_hook') or not v.get('expected'): fail()
  e=v['expected']
  if e.get('state')!=v['expected_outcome'] or e.get('checkpoint')!='unchanged' or e.get('feedback') is None or not e.get('stable_log_codes') or e.get('external_effects')!=['no runtime effects; specification validation only']: fail()
 for cid in numeric:
  i=vectors[cid]['inputs']; mapped={'cap':i.get('configured_cap_bytes'),'retained':i.get('retained_slot_bytes'),'free':i.get('source_free_bytes'),'buckets':i.get('wal_rate_buckets_bytes_per_second'),'age':i.get('wal_rate_age_seconds')}
  state,headroom,raw_horizon,safety_horizon=expected_eval(mapped)
  e=vectors[cid]['expected']
  if vectors[cid]['expected_outcome']!=state or (e.get('raw_headroom_bytes'),e.get('raw_horizon_seconds'),e.get('safety_horizon_seconds'))!=(headroom,raw_horizon,safety_horizon): fail()
 # Derive every operational result from its fixture inputs rather than approving expected literals.
 for cid in operational:
  e=vectors[cid]['expected']; derived=expected_operation(vectors[cid]['inputs'])
  if (e['state'],e['exit_code'],e['feedback'],e['stable_log_codes'])!=derived: fail()
 if vectors['local_override_valid_before_expiry']['inputs']['age_seconds']!=86399 or vectors['local_override_expired_at_24h']['inputs']['age_seconds']!=86400: fail()
 if spec['script']['path']!='scripts/validate/wal_cap.sh' or sha(root/spec['script']['path'])!=spec['script']['sha256']: fail()
 graph_ids={x['id'] for x in graph}
 if set(executors)-graph_ids: fail()
 stable=next(x for x in registry['entries'] if x['id']==decision_id); covered=next(x for x in coverage['assignments'] if x['id']==decision_id)
 if stable['owner_bead']!=owner or covered!={'evidence_status':'pending','id':decision_id,'owner_bead':owner,'source':'docs/PLAN.md','source_digest':stable['source_digest']}: fail()
 if subprocess.run([str(root/'scripts/validate/plan_coverage.sh')],cwd=root,capture_output=True).returncode: fail()
 probe_rel='artifacts/m0/decisions/boring-cdc-d-wal-cap/fixture-run.jsonl'; probe=[json.loads(x) for x in (root/probe_rel).read_text().splitlines()]
 expected_probe=[{'code':'WAL_CAP_FIXTURE_VALID','outcome':'pass','phase':'validate_spec','vector_count':30}]
 if probe!=expected_probe or spec['execution_probe']!={'expected_lines':expected_probe,'path':probe_rel,'sha256':sha(root/probe_rel)}: fail()
 decision=next(x for x in decisions['decisions'] if x['id']==decision_id)
 decision_approval={'approved_at':'2026-09-10T14:03:30.949Z','approved_by':'Julien Hurault (repository owner), intention 765bd3b2-4b68-4102-a9ec-43ca93357390','value_digest':hashlib.sha256(proposed.encode()).hexdigest()}
 if decision!={'approval':decision_approval,'executor_beads':executors,'fixture_sha256':sha(root/fixture_rel),'fixture_spec':fixture_rel,'id':decision_id,'owner_bead':owner,'proposed_value':proposed,'status':'approved'}: fail()
 needed={'ART-M0-WAL-CAP-FIXTURE':fixture_rel,'ART-M0-WAL-CAP-PROBE':probe_rel,'ART-M0-WAL-CAP-VALIDATION':'artifacts/m0/decisions/boring-cdc-d-wal-cap/evidence.json'}
 owned={x['id']:x for x in artifacts['artifacts'] if x.get('owner_bead')==owner}
 if set(owned)!=set(needed): fail()
 for ident,path in needed.items():
  if owned[ident]!={'id':ident,'owner_bead':owner,'path':path,'sha256':sha(root/path),'status':'complete'}: fail()
 evidence=json.loads((root/needed['ART-M0-WAL-CAP-VALIDATION']).read_text())
 if evidence.get('schema_version')!='validation-result/v1' or evidence.get('validator_version')!='core-validators/1.0.0' or evidence.get('owner_bead')!='boring-cdc-m0.1' or evidence.get('status')!='pass' or evidence.get('findings')!=[] or evidence.get('input_sha256')!=sha(root/'contracts/m0/decisions.json') or not re.fullmatch(r'[0-9a-f]{40}',evidence.get('git_commit','')): fail()
 anchor=evidence['git_commit']; guarded=[fixture_rel,'scripts/validate/wal_cap.sh','contracts/m0/decisions.json']
 if subprocess.run(['git','cat-file','-e',anchor+'^{commit}'],cwd=root,capture_output=True).returncode or subprocess.run(['git','merge-base','--is-ancestor',anchor,'HEAD'],cwd=root,capture_output=True).returncode or subprocess.run(['git','diff','--quiet',anchor+'..HEAD','--',*guarded],cwd=root).returncode: fail()
except (OSError,KeyError,ValueError,TypeError,StopIteration,json.JSONDecodeError): fail()
if selected=='all': print(json.dumps({'code':'WAL_CAP_FIXTURE_VALID','outcome':'pass','phase':'validate_spec','vector_count':len(vectors)},sort_keys=True,separators=(',',':')))
elif selected in vectors: print(json.dumps({'case_id':selected,'code':'WAL_CAP_CASE_VALID','expected_outcome':vectors[selected]['expected_outcome'],'outcome':'pass','phase':'validate_spec'},sort_keys=True,separators=(',',':')))
else: fail()
PY
