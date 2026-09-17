#!/usr/bin/env python3
import json,re,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
E=ROOT/'artifacts/boring-cdc-m4-ddl/SCN-M4-CH-MERGE-INVARIANT/evidence.json'
def validate():
 errors=[]
 if not E.is_file(): return ['E_EVIDENCE_MISSING']
 x=json.loads(E.read_text())
 if x.get('schema_version')!='m4-clickhouse-ddl-evidence/v1' or x.get('status')!='pass': errors.append('E_EVIDENCE_SCHEMA')
 if x.get('images')!={'postgres':'17.6','clickhouse':'25.8.2.29'}: errors.append('E_IMAGE_PIN')
 if x.get('observed_versions',{}).get('clickhouse')!='25.8.2.29' or not x.get('observed_versions',{}).get('postgres','').startswith('17.6'): errors.append('E_REAL_VERSION')
 if not re.fullmatch(r'[0-9a-f]{64}',x.get('object_fingerprint','')): errors.append('E_OBJECT_FINGERPRINT')
 if x.get('objects_verified')!=6: errors.append('E_OBJECTS')
 ordinary=x.get('ordinary_runtime',{})
 if ordinary!={'ddl_denied':True,'alter_denied':True,'canonical_select_allowed':True}: errors.append('E_PRIVILEGES')
 merge=x.get('merge_invariant',{}); digests=[merge.get(k) for k in ('before_sha256','stopped_sha256','during_sha256','after_sha256')]
 if not merge.get('system_merges_observed') or len(set(digests))!=1 or not re.fullmatch(r'[0-9a-f]{64}',digests[0] or ''): errors.append('E_MERGE_INVARIANT')
 if x.get('credentials_recorded') is not False or any(t in E.read_text().lower() for t in ('password=','postgresql://','clickhouse://')): errors.append('E_SECRET')
 return errors
if __name__=='__main__':
 errors=validate(); print(json.dumps({'validator':'m4-clickhouse-ddl','status':'fail' if errors else 'pass','findings':errors},sort_keys=True)); raise SystemExit(bool(errors))
