#!/usr/bin/env python3
import json,pathlib,sys
p=pathlib.Path(__file__).resolve().parents[2]/'contracts/m3/bootstrap-cases.json'
d=json.loads(p.read_text()); cases=d['cases']; ids=[x['scenario_id'] for x in cases]
assert d['owner_bead']=='boring-cdc-m3-bootstrap' and len(cases)==7 and len(ids)==len(set(ids))
source=(pathlib.Path(__file__).resolve().parents[2]/'src/m3_bootstrap.rs').read_text()
for case in cases: assert case['unit_test'] in source,case
print(json.dumps({'code':'M3_BOOTSTRAP_CASES_OK','cases':len(cases),'status':'pass'},sort_keys=True,separators=(',',':')))
