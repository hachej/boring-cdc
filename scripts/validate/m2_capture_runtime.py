#!/usr/bin/env python3
import json,pathlib,sys
p=pathlib.Path('contracts/m2/capture-runtime-cases.json'); d=json.loads(p.read_text()); cases=d['cases']; ids=[x['id'] for x in cases]
assert d['owner_bead']=='boring-cdc-m2-capture-runtime' and len(ids)==len(set(ids)) and 'TRANS-CAPTURE-SAFE-STOPPED' in ids
assert all(len(x['assertion'])>=40 and ('test' in x or 'consumed_test' in x) for x in cases)
s=pathlib.Path('src/m2_capture_runtime.rs').read_text(); assert 'send_standby_status_update' in s and 'packet.write_lsn' in s and 'packet.flush_lsn' in s and 'packet.apply_lsn' in s
assert s.count('ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol')==2
print(json.dumps({'schema_version':'m2-capture-runtime-validation/v1','cases':len(cases),'status':'pass'},sort_keys=True,separators=(',',':')))
