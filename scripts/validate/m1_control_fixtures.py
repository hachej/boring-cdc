#!/usr/bin/env python3
import json,re
from pathlib import Path
p=Path('contracts/m1/control-fixtures.json'); data=json.loads(p.read_text())
assert data['schema_version']=='m1-control-fixtures/v1'
assert data['owner_bead']=='boring-cdc-m1-control-fixtures'
majors=data['supported_postgresql_majors']; assert majors and len({x['major'] for x in majors})==len(majors)
for x in majors: assert x['provenance']=='// M0-PROVISIONAL: boring-cdc-d-pg-protocol'
scenarios=data['scenarios']; ids=[x['id'] for x in scenarios]
assert len(ids)==len(set(ids)) and all(re.fullmatch(r'SCN-[A-Z0-9-]+',x) for x in ids)
coverage={x['id']:x['owner_bead'] for x in json.loads(Path('contracts/coverage/plan-to-beads.json').read_text())['assignments']}
owned={x['id'] for x in json.loads(Path('contracts/coverage/plan-to-beads.json').read_text())['assignments'] if x['owner_bead']=='boring-cdc-m1-control-fixtures'}
assert set(data['owned_plan_ids'])==owned,(set(data['owned_plan_ids'])^owned)
required={
 'SCN-M1-CONTROL-FIXED-ROW-ABUSE','SCN-M1-CONTROL-HEARTBEAT-OUTAGE',
 'SCN-M1-CONTROL-IDLE-HEARTBEAT','SCN-M1-CONTROL-INTERNAL-NOOP',
 'SCN-M1-CONTROL-TRUNCATE-DETECTION','SCN-EXTERNAL-LIVE-PUBLICATION-MUTATION',
 'SCN-TIMELINE-SOURCE-PUBLICATION-SLOT-MISMATCH','SCN-M1-CONTROL-ADMIN-LIFETIME',
 'SCN-CRASH-DURING-TABLE-ADD-RE-SEED'}
assert required <= set(ids)
rust=Path('src/m1_control_fixtures.rs').read_text()
for case in scenarios: assert case['test'].split('::')[-1] in rust,case['test']
for token in ('TBD','TODO','FIXME','<unresolved>'): assert token not in p.read_text()
print(f"PASS m1 control fixture scenarios={len(scenarios)} pg_majors={len(majors)} unresolved=0")
