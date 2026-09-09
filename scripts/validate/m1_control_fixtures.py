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
artifact=Path('artifacts/boring-cdc-m1-control-fixtures/SCN-M1-CONTROL-COMPONENT/m1-control-v1')
if artifact.exists():
    import hashlib,subprocess
    subprocess.run(['scripts/validate/evidence.sh','artifacts/boring-cdc-m1-control-fixtures'],check=True,stdout=subprocess.DEVNULL)
    digest=lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    inventory={line.split('  ',1)[1]:line.split('  ',1)[0] for line in (artifact/'sha256.txt').read_text().splitlines()}
    expected={str(path.relative_to(artifact)):digest(path) for path in artifact.rglob('*') if path.is_file() and path.name not in {'manifest.json','sha256.txt'}}
    assert inventory==expected, 'SHA-256 inventory mismatch'
    for stem in ('e2e','faults'):
        assert (artifact/f'stdout/{stem}-1.txt').read_bytes()==(artifact/f'stdout/{stem}-2.txt').read_bytes()
        assert (artifact/f'stderr/{stem}-1.txt').read_bytes()==(artifact/f'stderr/{stem}-2.txt').read_bytes()
    required_log={'schema_version','case_event_seq','bead_id','scenario_id','correlation_id','run_id','capture_epoch','component','phase','outcome','config_fingerprint','evidence_digest'}
    rows=[json.loads(line) for line in (artifact/'logs/boring-cdc.jsonl').read_text().splitlines()]
    assert [row['case_event_seq'] for row in rows]==list(range(1,len(rows)+1))
    assert all(required_log<=row.keys() and row['bead_id']==data['owner_bead'] for row in rows)
    corpus='\n'.join(path.read_text(errors='replace') for path in artifact.rglob('*') if path.is_file())
    for forbidden in ('postgresql://','capture_fixture_only','control_fixture_only','application_fixture_only','/home/'):
        assert forbidden not in corpus, f'redaction failure: {forbidden}'
    print(f"PASS artifact inventory={len(inventory)} logs={len(rows)} deterministic_rerun=1 redaction=1")
