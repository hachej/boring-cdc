#!/usr/bin/env python3
import json, pathlib, re, sys
root=pathlib.Path(__file__).resolve().parents[2]
contract=json.loads((root/'contracts/m1/decoder-cases.json').read_text())
source=(root/'src/m1_decoder.rs').read_text()
errors=[]
if contract.get('schema_version')!='boring-cdc/m1-decoder-cases/v1': errors.append('schema_version')
if contract.get('owner_bead')!='boring-cdc-m1-decoder': errors.append('owner_bead')
if contract.get('supported_postgres_majors')!=[15,16,17]: errors.append('postgres_matrix')
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
