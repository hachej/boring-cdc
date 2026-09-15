#!/usr/bin/env python3
import json,pathlib,sys
root=pathlib.Path(__file__).resolve().parents[2]
cases=json.loads((root/'contracts/m2/heartbeat-cases.json').read_text())
source=(root/'src/m2_heartbeat.rs').read_text()
errors=[]
if cases.get('owner_bead')!='boring-cdc-m2-heartbeat' or cases.get('evidence_tier')!='component': errors.append('contract owner/tier')
ids=[]
for case in cases.get('cases',[]):
 ids.append(case['id'])
 if f"fn {case['test']}" not in source: errors.append('missing test '+case['test'])
 # Milestone registries may retain a later aggregate owner; this leaf binds and executes the
 # embedded Bead contract without rewriting the read-only generated coverage projection.
if len(ids)!=len(set(ids)): errors.append('duplicate case id')
for literal in ["UPDATE boring_cdc_control.heartbeat SET nonce = $1, updated_at = clock_timestamp() WHERE id = 'singleton'","SELECT id FROM boring_cdc_control.heartbeat WHERE id = 'singleton'","HEARTBEAT_WRITE_UNAVAILABLE"]:
 if literal not in source: errors.append('missing literal '+literal)
if errors:
 print('\n'.join(errors),file=sys.stderr);sys.exit(1)
print(f'M2_HEARTBEAT_VALIDATION_OK cases={len(ids)}')
