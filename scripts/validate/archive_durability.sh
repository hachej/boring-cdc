#!/bin/sh
set -eu
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CASE_ID=${1:-all}
exec python3 - "$ROOT" "$CASE_ID" <<'PY'
import hashlib,json,sys
from pathlib import Path
r=Path(sys.argv[1]); selected=sys.argv[2]
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def fail():print('{"code":"ARCHIVE_DURABILITY_FIXTURE_INVALID","outcome":"fail","phase":"validate_spec"}');raise SystemExit(1)
def outcome(cid,i,a):
 if cid in ('finite_budget_pass','byte_budget_boundary','event_budget_boundary','time_budget_boundary'):
  return 'pass' if i['bytes']<=a['max_bytes_per_pass'] and i['events']<=a['max_events_per_pass'] and i['elapsed_ms']<=a['max_milliseconds_per_pass'] else 'pass_bounded_resume'
 if cid=='oversized_unit_resume': return 'pass_bounded_resume' if i['unit_bytes']>a['max_bytes_per_pass'] and i['pass_bytes']==a['max_bytes_per_pass'] and i['resume_offset']==i['pass_bytes'] and i['hash_state_version']==1 else 'invalid'
 if cid=='identity_change_restart': return 'pass_restart_round' if i['new_generation']!=i['generation'] and i['new_selector_sha256']!=i['selector_sha256'] and i['partial_cursor']['byte_offset']>0 else 'invalid'
 if cid=='freshness_expired': return 'archive_partial_unknown' if i['last_complete_age_seconds']>i['freshness_seconds'] else 'pass'
 if cid=='inadequate_service_three_misses': return 'archive_blocked_inadequate_service' if i['missed_cadences']>=a['inadequate_service_after_consecutive_missed_cadences'] and i['missed_cadences']*i['cadence_seconds']>=i['freshness_seconds'] else 'pass'
 if cid=='transient_read': return 'retry_wait' if i.get('failure_class')=='transient_io' and 1 <= i.get('attempt_after_failure',0) < 10 else 'invalid'
 if cid=='rate_limited_reader': return 'retry_wait' if i.get('failure_class')=='rate_limited' and 1 <= i.get('attempt_after_failure',0) < 10 else 'invalid'
 if cid=='invalid_configuration': return 'archive_blocked_configuration' if i.get('failure_class')=='configuration' else 'invalid'
 if cid=='manifest_mismatch': return 'archive_blocked_integrity' if i.get('failure_class')=='integrity' and i.get('expected_sha256')!=i.get('actual_sha256') else 'invalid'
 if cid=='continuity_gap': return 'archive_blocked_continuity' if i.get('failure_class')=='integrity' and i.get('next_start')!=i.get('verified_end',-1)+1 else 'invalid'
 if cid=='unsupported_filesystem': return 'archive_blocked_unsupported' if i.get('failure_class')=='unsupported' and i.get('filesystem') in {'nfs','smb_cifs','fuse','tmpfs','overlayfs','remote_or_network_filesystem'} else 'invalid'
 return 'invalid'
try:
 f=json.loads((r/'fixtures/m0/decisions/boring-cdc-d-archive-durability.json').read_text()); a=f['confirmed_boundary']['audit']; m=f['confirmed_boundary']['failure_mapping']; ds=json.loads((r/'contracts/m0/decisions.json').read_text()); ar=json.loads((r/'contracts/m0/artifacts.json').read_text()); c=json.loads((r/'contracts/archive/archive-model.json').read_text()); st=json.loads((r/'contracts/storage/storage-model.json').read_text()); policy=json.loads((r/'contracts/m0/failure-policy.json').read_text())
 approval=f['approval']; boundary_sha=hashlib.sha256(json.dumps(f['confirmed_boundary'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
 if approval.get('approved_at')!='2026-09-16T06:05:55.752Z' or approval.get('approved_by')!='Julien Hurault (repository owner)' or approval.get('intention_id')!='59a63169-9b07-4a00-bbfb-cbed02a7e4ac' or approval.get('selection')!='Accept recommended defaults' or approval.get('confirmed_boundary_sha256')!=boundary_sha or approval.get('authority_note')!='host-directed exact d-archive-durability literals under answered owner card 59a63169':fail()
 if (a['max_bytes_per_pass'],a['max_events_per_pass'],a['max_milliseconds_per_pass'],a['cadence_seconds'],a['freshness_seconds'],a['inadequate_service_after_consecutive_missed_cadences'])!=(67108864,100000,5000,300,900,3):fail()
 if a['freshness_seconds']!=a['cadence_seconds']*a['inadequate_service_after_consecutive_missed_cadences'] or a['checkpoint_moves'] or a['separate_ranges']!=['journal_verified_range','self_consistent_range']:fail()
 expected_keys={'transient_read_or_stat','rate_limited_reader','invalid_configuration','unsupported_filesystem','hash_or_manifest_mismatch','continuity_gap','freshness_expired','inadequate_service'}
 if set(m)!=expected_keys:fail()
 outputs={x['class']:x['output'] for x in policy['domain_hooks']['exhaustive_class_component_outputs'] if x['component']=='archive'}
 expected_classes={'transient_io','rate_limited','configuration','unsupported','integrity'}
 if set(x['class'] for x in m.values())!=expected_classes or any(x['failure_policy_output']!=outputs[x['class']] or not x['code'].startswith('BCDC_ARCHIVE_') for x in m.values()):fail()
 if c['audit']['budgets']!={'max_bytes_per_pass':67108864,'max_events_per_pass':100000,'max_milliseconds_per_pass':5000,'cadence_seconds':300,'freshness_seconds':900}:fail()
 marker='// M0-'+'PROVISIONAL: boring-cdc-d-archive-durability'
 if marker in json.dumps(c) or marker in json.dumps(st):fail()
 if c['provisional_markers']!=[] or marker in st['provisional_markers']:fail()
 if c['consumes']['durability']['source_sha256']!=sha(r/'fixtures/m0/decisions/boring-cdc-d-archive-durability.json') or c['consumes']['failure_policy']['source_sha256']!=sha(r/'contracts/m0/failure-policy.json'):fail()
 if f['script']['sha256']!=sha(r/f['script']['path']):fail()
 vs=f['vectors']; matrix={x['case_id']:x['expected_outcome'] for x in f['supported_matrix']}
 if len(vs)!=14 or set(vs)!=set(matrix):fail()
 for k,v in vs.items():
  if v['expected_outcome']!=matrix[k] or v['expected']['checkpoint']!='unchanged' or v['expected']['feedback']!='unaffected' or outcome(k,v['inputs'],a)!=v['expected_outcome']:fail()
  if 'failure_class' in v['inputs'] and v['expected']['failure_policy_output']!=outputs[v['inputs']['failure_class']]:fail()
 row=next(x for x in ds['decisions'] if x['id']=='DEC-ARCHIVE-DURABILITY-CONTINUITY')
 if row['status']!='approved' or row['fixture_sha256']!=sha(r/'fixtures/m0/decisions/boring-cdc-d-archive-durability.json') or row.get('provisional_markers'):fail()
 needed={'ART-M0-ARCHIVE-DURABILITY-FIXTURE':'fixtures/m0/decisions/boring-cdc-d-archive-durability.json','ART-M0-ARCHIVE-DURABILITY-PROBE':'artifacts/m0/decisions/boring-cdc-d-archive-durability/fixture-run.jsonl','ART-M0-ARCHIVE-DURABILITY-VALIDATION':'artifacts/m0/decisions/boring-cdc-d-archive-durability/evidence.json'}
 owned={x['id']:x for x in ar['artifacts'] if x.get('owner_bead')=='boring-cdc-d-archive-durability'}
 if set(owned)!=set(needed) or any(owned[k]['path']!=p or owned[k]['sha256']!=sha(r/p) for k,p in needed.items()):fail()
except Exception:fail()
if selected=='all':print(json.dumps({'code':'ARCHIVE_DURABILITY_FIXTURE_VALID','outcome':'pass','phase':'validate_spec','vector_count':len(vs)},sort_keys=True,separators=(',',':')))
elif selected in vs:print(json.dumps({'case_id':selected,'code':'ARCHIVE_DURABILITY_CASE_VALID','expected_outcome':vs[selected]['expected_outcome'],'outcome':'pass','phase':'validate_spec'},sort_keys=True,separators=(',',':')))
else:fail()
PY
