#!/usr/bin/env python3
import json, pathlib, re, sys
root=pathlib.Path(__file__).resolve().parents[2]
contract=json.loads((root/'contracts/m1/decoder-cases.json').read_text())
source=(root/'src/m1_decoder.rs').read_text()
fixture=json.loads((root/'tests/fixtures/m1_decoder/pgoutput-v1.json').read_text())
errors=[]
if contract.get('schema_version')!='boring-cdc/m1-decoder-cases/v1': errors.append('schema_version')
if contract.get('owner_bead')!='boring-cdc-m1-decoder': errors.append('owner_bead')
if contract.get('supported_postgres_majors')!=[15,16,17]: errors.append('postgres_matrix')
vectors=fixture.get('vectors',[])
if [v.get('postgres_major') for v in vectors] != [15,16,17]: errors.append('golden_postgres_matrix')
if any(len(v.get('frames_hex',[])) != 8 or any(not re.fullmatch(r'[0-9a-f]+', f) or len(f)%2 for f in v.get('frames_hex',[])) for v in vectors): errors.append('golden_wire_frames')
if fixture.get('provenance',{}).get('kind') != 'frozen_protocol_v1_contract_vector': errors.append('golden_provenance')
if [v.get('source_release_tag') for v in vectors] != ['REL_15_STABLE','REL_16_STABLE','REL_17_STABLE']: errors.append('golden_release_tags')
if any(v.get('expected',{}).get('row_ordinals') != [0,1,2] or v.get('expected',{}).get('xid') != 99 or v.get('expected',{}).get('origin') != 'upstream' or not v.get('expected',{}).get('keepalive_reply_requested') for v in vectors): errors.append('golden_expectations')
coverage=json.loads((root/'contracts/coverage/plan-to-beads.json').read_text())
owned=[item['id'] for item in coverage['assignments'] if item.get('owner_bead') == 'boring-cdc-m1-decoder']
mapped=contract.get('canonical_assignments',[])
if [item.get('id') for item in mapped] != owned: errors.append('canonical_assignment_inventory')
case_id_set={case.get('id') for case in contract.get('cases',[])}
if any(not item.get('executed_by') or not set(item.get('executed_by',[])) <= case_id_set for item in mapped): errors.append('canonical_assignment_execution_map')
ids=[c.get('id') for c in contract.get('cases',[])]
if len(ids)!=10 or len(ids)!=len(set(ids)) or any(not re.fullmatch(r'SCN-M1-DECODER-[A-Z-]+', x or '') for x in ids): errors.append('case_inventory')
for token in ["b'B'","b'R'","b'O'","b'I'","b'U'","b'D'","b'C'","b'T'","b'k'","b'w'","b'r'"]:
    if token not in source: errors.append('missing_tag:'+token)
for marker in [
'M0-PROVISIONAL: boring-cdc-d-pg-protocol (RECOMMENDED PostgreSQL majors).',
'M0-PROVISIONAL: boring-cdc-d-pg-protocol (RECOMMENDED protocol zero sentinel).',
'M0-PROVISIONAL: boring-cdc-d-pg-protocol (RECOMMENDED PostgreSQL-to-Unix epoch offset).']:
    if marker not in source: errors.append('missing_provisional_marker:'+marker)
if re.search(r'\b(TBD|TODO|FIXME)\b', source+json.dumps(contract)): errors.append('unresolved_contract_value')
result={'schema_version':'boring-cdc/validation-result/v1','validator':'m1_decoder','status':'pass' if not errors else 'fail','case_count':len(ids),'errors':errors}
print(json.dumps(result,sort_keys=True))
sys.exit(bool(errors))
