#!/usr/bin/env python3
import json,pathlib,re,sys
root=pathlib.Path(__file__).resolve().parents[2]
cases=json.loads((root/'contracts/m2/fault-status-cases.json').read_text())
index=json.loads((root/'contracts/runbooks/index.json').read_text())
expected=set(cases['conditions']); rows=index['runbooks']; found={r['condition_id'][5:].lower().replace('-','_') for r in rows}
assert cases['owner_bead']=='boring-cdc-m2-fault-status' and len(expected)==14 and found==expected
assert len({r['id'] for r in rows})==14 and len({r['condition_id'] for r in rows})==14
assert all(r['condition_owner']=='boring-cdc-m2-fault-status' and r['procedure_owner']=='boring-cdc-m6-runbooks' and r['procedure'] is None for r in rows)
src=(root/'src/m2_fault_status.rs').read_text(); assert 'BORING_CDC_M2_FAULT_HOOK' in src and 'SQLITE_OPEN_READ_ONLY' in src
for forbidden in ('raw_driver_error','snapshot_token','canonical_key','postgresql://'):
    assert forbidden not in src
print(json.dumps({'schema_version':'validation-result/v1','status':'pass','owner_bead':'boring-cdc-m2-fault-status','validator_version':'m2-fault-status/v1','findings':[]}))
