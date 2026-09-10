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
proposed='max_slot_wal_keep_size=64 GiB (68,719,476,736 bytes); headroom=max(0,min(cap-retained_slot_bytes,source_free_bytes-16 GiB)); WAL rate=p95 bytes/s over rolling 15 minutes from 1-minute buckets, stale after 3 minutes; fresh zero rate gives infinite horizon only with positive headroom; monitor every 30 seconds with 2-minute reaction reserve; horizon=floor(headroom/rate); warning <=60 minutes, action_required <=30 minutes, critical <=10 minutes with equality more severe; missing/unsupported/stale inputs produce unknown, block new bootstrap/backfill, and preserve healthy capture; local-only --unsafe-unbounded-slot-wal expires at 24 hours, requires confirmation UNBOUNDED_WAL_LOCAL_ONLY and actor/time/config-digest audit, and is forbidden remotely; invalidated slots require explicit reseed.'
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def fail():
 print('{"code":"WAL_CAP_FIXTURE_INVALID","outcome":"fail","phase":"validate_spec"}'); raise SystemExit(1)
def expected_eval(i):
 if i['cap']!=68719476736 or i['retained'] is None or i['free'] is None or i['rate'] is None or i['age']>180: return ('unknown',None)
 head=max(0,min(i['cap']-i['retained'],i['free']-17179869184))
 if head==0: horizon=0
 elif i['rate']==0: horizon='infinite'
 elif i['rate']<0: return ('unknown',None)
 else: horizon=head//i['rate']
 if horizon=='infinite' or horizon>3600: state='normal'
 elif horizon<=600: state='critical'
 elif horizon<=1800: state='action_required'
 else: state='warning'
 return state,horizon
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
 if b.get('headroom')!={'raw_bytes':'min(68719476736 - retained_slot_bytes, source_free_bytes - 17179869184)','clamp':'max(0, raw_bytes)','unit':'byte'}: fail()
 if b.get('wal_rate')!={'bucket_seconds':60,'window_seconds':900,'statistic':'p95','unit':'byte_per_second','fresh_through_age_seconds':180,'stale_after_age_seconds':180,'zero_rate_horizon':'infinite_when_fresh_and_headroom_positive','zero_headroom_horizon_seconds':0}: fail()
 if b.get('monitor_interval_seconds')!=30 or b.get('reaction_reserve_seconds')!=120 or b.get('horizon')!={'formula':'floor(headroom_bytes / wal_rate_bytes_per_second)','rounding':'floor','unit':'second'}: fail()
 if b.get('thresholds')!=[{'horizon_seconds_lte':600,'state':'critical'},{'horizon_seconds_lte':1800,'state':'action_required'},{'horizon_seconds_lte':3600,'state':'warning'},{'horizon_seconds_gt':3600,'state':'normal'}] or b.get('threshold_precedence')!=['unknown','critical','action_required','warning','normal']: fail()
 if b.get('unknown_predicates')!=['source_free_bytes_missing','configured_cap_missing_or_not_68719476736','retained_slot_bytes_missing','wal_rate_missing','wal_rate_age_seconds_greater_than_180']: fail()
 if b.get('unknown_behavior')!={'automatic_cap_increase':False,'automatic_detach_destination':False,'automatic_drop_slot':False,'fabricate_feedback':False,'healthy_capture':'continues','new_backfill':'blocked','new_bootstrap':'blocked'}: fail()
 override={'allowed_origin':'local_process','audit_fields':['actor','activated_at','configuration_sha256'],'configuration_digest':'SHA-256','confirmation':'UNBOUNDED_WAL_LOCAL_ONLY','expires_when_age_seconds_gte':86400,'flag':'--unsafe-unbounded-slot-wal','forbidden_origin':'remote','scope':'local_experiment_bootstrap_and_backfill_admission_only','status_code':'WAL_CAP_DISABLED_UNSAFE','valid_for_seconds':86400}
 if b.get('unsafe_override')!=override: fail()
 if b.get('stable_state_codes')!={'action_required':'WAL_HEADROOM_ACTION_REQUIRED','critical':'WAL_HEADROOM_CRITICAL','unknown':'WAL_HEADROOM_UNKNOWN','unsafe_override':'WAL_CAP_DISABLED_UNSAFE','warning':'WAL_HEADROOM_WARNING'}: fail()
 if b.get('metric_names')!={'headroom_bytes':'wal_headroom_bytes','horizon_seconds':'wal_headroom_horizon_seconds','rate_bytes_per_second':'wal_rate_bytes_per_second','state':'wal_headroom_state'}: fail()
 if b.get('invalidation')!={'automatic_reseed':False,'forecast_is_conditional':True,'forecast_may_override_source_safety':False,'slot_invalidated_outcome':'require_reseed'}: fail()
 vectors=spec['vectors']; matrix=spec['supported_matrix']; cases={x['case_id']:x for x in matrix}
 numeric={'normal_above_warning','warning_equality_60m','warning_above_action','action_equality_30m','action_above_critical','critical_equality_10m','critical_floor_fraction','cap_headroom_clamped_zero','free_space_is_minimum','free_headroom_clamped_zero','fresh_zero_rate_infinite','zero_headroom_zero_rate','missing_free_unknown','missing_retained_unknown','missing_rate_unknown','stale_rate_unknown','unsupported_unbounded_cap_unknown','different_finite_cap_unknown'}
 operational={'unknown_blocks_new_work','unknown_healthy_capture_continues','local_override_valid_before_expiry','local_override_expired_at_24h','remote_override_forbidden','override_confirmation_mismatch','slot_invalidation_requires_reseed','conditional_forecast_cannot_override_action'}
 if set(vectors)!=numeric|operational or set(cases)!=set(vectors) or len(matrix)!=len(vectors): fail()
 for cid,v in vectors.items():
  if v.get('case_id')!=cid or cases[cid].get('expected_outcome')!=v.get('expected_outcome') or v.get('executor_bead') not in executors or not v.get('preconditions') or not v.get('fault_hook') or not v.get('expected'): fail()
  e=v['expected']
  if e.get('state')!=v['expected_outcome'] or e.get('checkpoint')!='unchanged' or e.get('feedback') is None or not e.get('stable_log_codes') or e.get('external_effects')!=['no runtime effects; specification validation only']: fail()
 for cid in numeric:
  i=vectors[cid]['inputs']; mapped={'cap':i.get('configured_cap_bytes'),'retained':i.get('retained_slot_bytes'),'free':i.get('source_free_bytes'),'rate':i.get('wal_rate_bytes_per_second'),'age':i.get('wal_rate_age_seconds')}
  state,horizon=expected_eval(mapped)
  if vectors[cid]['expected_outcome']!=state or vectors[cid]['expected'].get('headroom_horizon_seconds')!=horizon: fail()
 # Exact safety/override boundary cases.
 checks={
  'unknown_blocks_new_work':('blocked_unknown_headroom',2,'none',['WAL_HEADROOM_UNKNOWN']),
  'unknown_healthy_capture_continues':('capture_continues',0,'unchanged_durable_boundary',['WAL_HEADROOM_UNKNOWN']),
  'local_override_valid_before_expiry':('unsafe_override_active',0,'none',['WAL_CAP_DISABLED_UNSAFE']),
  'local_override_expired_at_24h':('blocked_override_expired',2,'none',['WAL_HEADROOM_UNKNOWN']),
  'remote_override_forbidden':('blocked_remote_override',2,'none',['WAL_HEADROOM_UNKNOWN']),
  'override_confirmation_mismatch':('blocked_confirmation',2,'none',['WAL_HEADROOM_UNKNOWN']),
  'slot_invalidation_requires_reseed':('require_reseed',2,'none',['WAL_HEADROOM_CRITICAL']),
  'conditional_forecast_cannot_override_action':('action_required',0,'unchanged_durable_boundary',['WAL_HEADROOM_ACTION_REQUIRED'])}
 for cid,(state,exitc,feedback,codes) in checks.items():
  e=vectors[cid]['expected']
  if (e['state'],e['exit_code'],e['feedback'],e['stable_log_codes'])!=(state,exitc,feedback,codes): fail()
 for cid in ('local_override_valid_before_expiry','local_override_expired_at_24h','remote_override_forbidden','override_confirmation_mismatch'):
  audit=vectors[cid]['inputs']['audit']
  if set(audit)!=set(override['audit_fields']) or not audit['actor'] or not audit['activated_at'] or not re.fullmatch(r'[0-9a-f]{64}',audit['configuration_sha256']): fail()
 if vectors['local_override_valid_before_expiry']['inputs']['age_seconds']!=86399 or vectors['local_override_expired_at_24h']['inputs']['age_seconds']!=86400 or vectors['remote_override_forbidden']['inputs']['origin']!='remote': fail()
 if spec['script']['path']!='scripts/validate/wal_cap.sh' or sha(root/spec['script']['path'])!=spec['script']['sha256']: fail()
 graph_ids={x['id'] for x in graph}
 if set(executors)-graph_ids: fail()
 stable=next(x for x in registry['entries'] if x['id']==decision_id); covered=next(x for x in coverage['assignments'] if x['id']==decision_id)
 if stable['owner_bead']!=owner or covered!={'evidence_status':'pending','id':decision_id,'owner_bead':owner,'source':'docs/PLAN.md','source_digest':stable['source_digest']}: fail()
 if subprocess.run([str(root/'scripts/validate/plan_coverage.sh')],cwd=root,capture_output=True).returncode: fail()
 probe_rel='artifacts/m0/decisions/boring-cdc-d-wal-cap/fixture-run.jsonl'; probe=[json.loads(x) for x in (root/probe_rel).read_text().splitlines()]
 expected_probe=[{'code':'WAL_CAP_FIXTURE_VALID','outcome':'pass','phase':'validate_spec','vector_count':26}]
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
if selected=='all': print('{"code":"WAL_CAP_FIXTURE_VALID","outcome":"pass","phase":"validate_spec"}')
elif selected in vectors: print(json.dumps({'case_id':selected,'code':'WAL_CAP_CASE_VALID','expected_outcome':vectors[selected]['expected_outcome'],'outcome':'pass','phase':'validate_spec'},sort_keys=True,separators=(',',':')))
else: fail()
PY
