#!/usr/bin/env python3
import json,pathlib,re,sys
root=pathlib.Path(__file__).resolve().parents[2]
c=json.loads((root/'contracts/m1/ddl-fixtures.json').read_text()); src=(root/'src/m1_ddl_fixtures.rs').read_text(); errors=[]
if c.get('schema_version')!='boring-cdc/m1-ddl-fixtures/v1': errors.append('schema_version')
if c.get('owner_bead')!='boring-cdc-m1-ddl-fixtures': errors.append('owner')
if [x.get('major') for x in c.get('postgres',[])] != [15,16,17]: errors.append('postgres_matrix')
if len(c.get('catalog_fingerprint_fields',[]))!=28: errors.append('fingerprint_fields')
if c.get('canonical_lock_order')!=['database_oid','relation_oid','logical_table_id'] or c.get('lock_mode')!='ACCESS SHARE': errors.append('lock_contract')
m=c.get('admitted_ddl_matrix',[])
if len(m)!=6 or any(x.get('required_lock')!='AccessExclusiveLock' for x in m): errors.append('ddl_matrix')
ids=[x.get('id') for x in c.get('cases',[])]
markers=re.findall(r'// SCENARIO: (SCN-[A-Z0-9-]+)',src)
if set(ids)!=set(markers) or len(ids)!=len(set(ids)): errors.append('scenario_inventory')
cov=json.loads((root/'contracts/coverage/plan-to-beads.json').read_text())
owned=[x['id'] for x in cov['assignments'] if x.get('owner_bead')=='boring-cdc-m1-ddl-fixtures']
ca=c.get('canonical_assignments',[])
if [x.get('id') for x in ca]!=owned or any(not x.get('executed_by') or not set(x['executed_by'])<=set(ids) for x in ca): errors.append('coverage_map')
for marker in ['// M0-PROVISIONAL: boring-cdc-d-ddl (RECOMMENDED catalog poll interval).','// M0-PROVISIONAL: boring-cdc-d-ddl (RECOMMENDED DDL waiter source-impact bound).','// M0-PROVISIONAL: boring-cdc-d-ddl (RECOMMENDED supported PostgreSQL majors).']:
 if marker not in src: errors.append('provisional_marker')
if re.search(r'\b(TBD|TODO|FIXME)\b',src+json.dumps(c)):errors.append('unresolved')
print(json.dumps({'schema_version':'boring-cdc/validation-result/v1','validator':'m1_ddl_fixtures','status':'pass' if not errors else 'fail','scenarios':len(ids),'matrix_rows':len(m),'errors':errors},sort_keys=True))
sys.exit(bool(errors))
