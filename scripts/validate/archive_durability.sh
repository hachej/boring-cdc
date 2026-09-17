#!/bin/sh
set -eu
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CASE_ID=${1:-all}
exec python3 - "$ROOT" "$CASE_ID" <<'PY'
import hashlib,json,re,subprocess,sys
from pathlib import Path
r=Path(sys.argv[1]); selected=sys.argv[2]
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def fail():print('{"code":"ARCHIVE_DURABILITY_FIXTURE_INVALID","outcome":"fail","phase":"validate_spec"}');raise SystemExit(1)
try:
 f=json.loads((r/'fixtures/m0/decisions/boring-cdc-d-archive-durability.json').read_text()); a=f['confirmed_boundary']['audit']; m=f['confirmed_boundary']['failure_mapping']; ds=json.loads((r/'contracts/m0/decisions.json').read_text()); ar=json.loads((r/'contracts/m0/artifacts.json').read_text()); c=json.loads((r/'contracts/archive/archive-model.json').read_text()); st=json.loads((r/'contracts/storage/storage-model.json').read_text())
 if f['approval']!={'approved_at':'2026-09-16T06:05:55.752Z','approved_by':'Julien Hurault (repository owner)','intention_id':'59a63169-9b07-4a00-bbfb-cbed02a7e4ac','selection':'Accept recommended defaults'}:fail()
 if (a['max_bytes_per_pass'],a['max_events_per_pass'],a['max_milliseconds_per_pass'],a['cadence_seconds'],a['freshness_seconds'],a['inadequate_service_after_consecutive_missed_cadences'])!=(67108864,100000,5000,300,900,3):fail()
 if a['freshness_seconds']!=a['cadence_seconds']*a['inadequate_service_after_consecutive_missed_cadences'] or a['checkpoint_moves'] or a['separate_ranges']!=['journal_verified_range','self_consistent_range']:fail()
 if set(m)!= {'transient_read_or_stat','rate_limited_reader','invalid_configuration','unsupported_filesystem','hash_or_manifest_mismatch','continuity_gap','freshness_expired','inadequate_service'}:fail()
 allowed={'transient_io','rate_limited','configuration','unsupported','integrity'}
 if any(x['class'] not in allowed or x['hook']!='destination_degraded' or not x['code'].startswith('BCDC_ARCHIVE_') for x in m.values()):fail()
 if c['audit']['budgets']!={'max_bytes_per_pass':67108864,'max_events_per_pass':100000,'max_milliseconds_per_pass':5000,'cadence_seconds':300,'freshness_seconds':900}:fail()
 marker='// M0-'+'PROVISIONAL: boring-cdc-d-archive-durability'
 if marker in json.dumps(c) or marker in json.dumps(st):fail()
 if c['provisional_markers']!=['// M0-'+'PROVISIONAL: boring-cdc-d-compose'] or marker in st['provisional_markers']:fail()
 if f['script']['sha256']!=sha(r/f['script']['path']):fail()
 vs=f['vectors']; matrix={x['case_id']:x['expected_outcome'] for x in f['supported_matrix']}
 if len(vs)!=12 or set(vs)!=set(matrix) or any(v['expected_outcome']!=matrix[k] or v['expected']['checkpoint']!='unchanged' or v['expected']['feedback']!='unaffected' for k,v in vs.items()):fail()
 row=next(x for x in ds['decisions'] if x['id']=='DEC-ARCHIVE-DURABILITY-CONTINUITY')
 if row['status']!='approved' or row['fixture_sha256']!=sha(r/'fixtures/m0/decisions/boring-cdc-d-archive-durability.json') or row.get('provisional_markers'):fail()
 needed={'ART-M0-ARCHIVE-DURABILITY-FIXTURE':'fixtures/m0/decisions/boring-cdc-d-archive-durability.json','ART-M0-ARCHIVE-DURABILITY-PROBE':'artifacts/m0/decisions/boring-cdc-d-archive-durability/fixture-run.jsonl','ART-M0-ARCHIVE-DURABILITY-VALIDATION':'artifacts/m0/decisions/boring-cdc-d-archive-durability/evidence.json'}
 owned={x['id']:x for x in ar['artifacts'] if x.get('owner_bead')=='boring-cdc-d-archive-durability'}
 if set(owned)!=set(needed) or any(owned[k]['path']!=p or owned[k]['sha256']!=sha(r/p) for k,p in needed.items()):fail()
except Exception:fail()
if selected=='all':print(json.dumps({'code':'ARCHIVE_DURABILITY_FIXTURE_VALID','outcome':'pass','phase':'validate_spec','vector_count':12},sort_keys=True,separators=(',',':')))
elif selected in vs:print(json.dumps({'case_id':selected,'code':'ARCHIVE_DURABILITY_CASE_VALID','expected_outcome':vs[selected]['expected_outcome'],'outcome':'pass','phase':'validate_spec'},sort_keys=True,separators=(',',':')))
else:fail()
PY
