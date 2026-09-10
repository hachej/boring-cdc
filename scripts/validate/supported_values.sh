#!/bin/sh
set -eu
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CASE_ID=${1:-all}
exec python3 - "$ROOT" "$CASE_ID" <<'PY'
import hashlib,json,re,subprocess,sys
from pathlib import Path
root=Path(sys.argv[1]); selected_case=sys.argv[2]
owner='boring-cdc-d-values'; decision_id='DEC-SUPPORTED-VALUES'
fixture_rel='fixtures/m0/decisions/boring-cdc-d-values.json'
executors=['boring-cdc-m1-decoder','boring-cdc-m0-event-format']
proposed='Envelope v1 uses type tags and distinct null, absent, and unchanged states for the approved bool, int2, int4, int8, numeric, float4, float8, date, timestamp, timestamptz, uuid, text, varchar, bpchar, bytea, and one-dimensional array OIDs; canonical bodies, destination mappings, and limits are fixed by owner intention 765bd3b2-4b68-4102-a9ec-43ca93357390.'
SCALARS=[('bool',16,'boolean','Bool','BOOLEAN'),('int2',21,'signed_integer','Int16','INT32'),('int4',23,'signed_integer','Int32','INT32'),('int8',20,'signed_integer','Int64','INT64'),('numeric',1700,'arbitrary_precision_numeric','String(tagged canonical body)','BYTE_ARRAY(canonical tagged body)'),('float4',700,'ieee754','Float32','FLOAT'),('float8',701,'ieee754','Float64','DOUBLE'),('date',1082,'date','String(tagged canonical body)','BYTE_ARRAY(canonical tagged body)'),('timestamp',1114,'timestamp_without_time_zone','String(tagged canonical body)','BYTE_ARRAY(canonical tagged body)'),('timestamptz',1184,'timestamp_with_time_zone','String(tagged canonical body)','BYTE_ARRAY(canonical tagged body)'),('uuid',2950,'uuid','UUID','FIXED_LEN_BYTE_ARRAY(16, UUID)'),('text',25,'utf8_text','String','BYTE_ARRAY(UTF8)'),('varchar',1043,'utf8_text','String','BYTE_ARRAY(UTF8)'),('bpchar',1042,'utf8_text','String','BYTE_ARRAY(UTF8)'),('bytea',17,'bytes','String(base64url unpadded)','BYTE_ARRAY')]
ARRAY_OIDS=[1000,1005,1007,1016,1231,1021,1022,1182,1115,1185,2951,1009,1015,1014,1001]
EXPECTED_MATRIX=[{'array_oid':a,'canonical_family':f,'clickhouse':ch,'jsonl':'envelope_v1 tagged value','name':n,'oid':o,'parquet':pq+' plus value-state tag metadata'} for (n,o,f,ch,pq),a in zip(SCALARS,ARRAY_OIDS)]
def env(state,typ=None,value=None):
 d={'state':state}
 if typ is not None:d['type']=typ
 if value is not None:d['value']=value
 return d
EXPECTED_CANONICAL=[
 ('bool-false',env('value','bool','false')),('bool-true',env('value','bool','true')),('int2-min',env('value','int2','-32768')),('int4-zero',env('value','int4','0')),('int8-max',env('value','int8','9223372036854775807')),('numeric-trailing-zero-normalized',env('value','numeric','+12345e+2')),('numeric-negative-scale',env('value','numeric','+1e-3')),('numeric-negative-zero',env('value','numeric','-0e+0')),('float4-nan',env('value','float4','NaN')),('float8-positive-infinity',env('value','float8','+Infinity')),('float8-negative-infinity',env('value','float8','-Infinity')),('float8-negative-zero',env('value','float8','-0')),('float8-round-trip',env('value','float8','1.2345678901234567')),('date',env('value','date','2024-02-29')),('date-positive-infinity',env('value','date','+infinity')),('timestamp-naive',env('value','timestamp','2024-02-29T12:34:56.123456')),('timestamp-negative-infinity',env('value','timestamp','-infinity')),('timestamptz-utc',env('value','timestamptz','2024-02-29T12:34:56.123456Z')),('uuid',env('value','uuid','01234567-89ab-cdef-0123-456789abcdef')),('text-no-normalization',env('value','text','é')),('bpchar-preserve-space',env('value','bpchar','x  ')),('bytea',env('value','bytea','-_8A')),('null',env('null','text')),('absent',env('absent')),('unchanged',env('unchanged','text')),('array-lower-bound',env('value','int4[]',{'element_type':'int4','length':3,'lower_bound':-2,'values':[env('value','int4','1'),env('null','int4'),env('unchanged','int4')]}))]
EXPECTED_FAILURES=[{'case_id':'unsupported-oid','input':{'oid':114},'expected_code':'SUPPORTED_VALUES_UNSUPPORTED_TYPE'},{'case_id':'numeric-precision-over','input':{'precision':1001},'expected_code':'SUPPORTED_VALUES_LIMIT_EXCEEDED'},{'case_id':'numeric-scale-low','input':{'scale':-16384},'expected_code':'SUPPORTED_VALUES_LIMIT_EXCEEDED'},{'case_id':'numeric-scale-high','input':{'scale':16384},'expected_code':'SUPPORTED_VALUES_LIMIT_EXCEEDED'},{'case_id':'scalar-over','input':{'scalar_bytes':1048577},'expected_code':'SUPPORTED_VALUES_LIMIT_EXCEEDED'},{'case_id':'row-over','input':{'row_bytes':4194305},'expected_code':'SUPPORTED_VALUES_LIMIT_EXCEEDED'},{'case_id':'event-over','input':{'event_bytes':8388609},'expected_code':'SUPPORTED_VALUES_LIMIT_EXCEEDED'},{'case_id':'array-dimensions-over','input':{'array_dimensions':2},'expected_code':'SUPPORTED_VALUES_LIMIT_EXCEEDED'},{'case_id':'array-elements-over','input':{'array_elements':10001},'expected_code':'SUPPORTED_VALUES_LIMIT_EXCEEDED'}]
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def canonical_bytes(value): return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
def fail():
 print('{"code":"SUPPORTED_VALUES_FIXTURE_INVALID","outcome":"fail","phase":"validate_spec"}'); raise SystemExit(1)
try:
 spec=json.loads((root/fixture_rel).read_text()); decisions=json.loads((root/'contracts/m0/decisions.json').read_text()); artifacts=json.loads((root/'contracts/m0/artifacts.json').read_text()); registry=json.loads((root/'contracts/agent/stable-ids.json').read_text()); coverage=json.loads((root/'contracts/coverage/plan-to-beads.json').read_text()); graph=[json.loads(x) for x in (root/'.beads/issues.jsonl').read_text().splitlines()]
 required=('inputs','preconditions','type_matrix','canonical_contract','destination_contract','limits','golden_vectors','failure_vectors','deterministic_phase','expected','expected_failure','result_contract','redaction_assertions','later_executors')
 if any(not spec.get(x) for x in required): fail()
 if spec['schema_version']!='m0-decision-fixture/v1' or spec['fixture_id']!=decision_id or spec['decision_id']!=decision_id or spec['owner_bead']!=owner or spec['later_executors']!=executors: fail()
 if spec['approval']!={'approved_at':'2026-09-10T14:03:30.949Z','approved_by':'Julien Hurault (repository owner)','intention_id':'765bd3b2-4b68-4102-a9ec-43ca93357390','selection':'Accept recommended defaults'}: fail()
 if spec['fixed_seed']!={'ascii':'BCDC_VALUES_V01','hex':'0x424344435f56414c5545535f563031'} or spec['type_matrix']!=EXPECTED_MATRIX: fail()
 if spec['limits']!={'array_dimensions_max':1,'array_elements_max':10000,'event_bytes_max':8388608,'numeric_precision_max':1000,'numeric_scale_max':16383,'numeric_scale_min':-16383,'row_bytes_max':4194304,'scalar_bytes_max':1048576,'units':'bytes'}: fail()
 envelope=spec['canonical_contract']['envelope']
 if envelope!={'version':1,'states':['value','null','absent','unchanged'],'value_requires_type_tag':True,'states_are_distinct':True}: fail()
 required_literals=['minimal base-10 decimal','<sign><unscaled-digits>e<signed-scale>','shortest round-trip','exactly six fractional digits','base64url without padding','without Unicode normalization','preserve lower bound']
 contract_text=json.dumps(spec['canonical_contract'],sort_keys=True)
 if any(x not in contract_text for x in required_literals): fail()
 if spec['destination_contract']!={'clickhouse':{'fallback':'String containing tagged canonical body whenever the listed native type cannot represent the value exactly','state_representation':'distinct value-state tag alongside native value, or inside tagged String'},'jsonl':{'mapping':'envelope_v1 tagged value for every supported type and state'},'parquet':{'mapping':'listed nullable typed column plus required value-state tag metadata','fallback':'BYTE_ARRAY containing canonical tagged body when typed representation is not exact'}}: fail()
 expected_vectors=[]
 for case_id,value in EXPECTED_CANONICAL:
  body=canonical_bytes(value); expected_vectors.append({'canonical':value,'canonical_utf8_hex':body.hex(),'case_id':case_id,'sha256':hashlib.sha256(body).hexdigest()})
 if spec['golden_vectors']!=expected_vectors or spec['failure_vectors']!=EXPECTED_FAILURES: fail()
 case_ids={x['case_id'] for x in expected_vectors}|{x['case_id'] for x in EXPECTED_FAILURES}
 if selected_case!='all' and selected_case not in case_ids: fail()
 if spec['key_projection']!={'canonical_body':'identical to the value canonical body for the same scalar type','forbidden_states':['null','absent','unchanged'],'scope':'encoding only; effective replica identity selection remains owned by DEC-SUPPORTED-KEYS'}: fail()
 if spec['script']['path']!='scripts/validate/supported_values.sh' or sha(root/spec['script']['path'])!=spec['script']['sha256']: fail()
 graph_ids={x['id'] for x in graph}
 if set(executors)-graph_ids or len(graph_ids)!=len(graph): fail()
 stable=next(x for x in registry['entries'] if x['id']==decision_id); covered=next(x for x in coverage['assignments'] if x['id']==decision_id)
 if stable['owner_bead']!=owner or covered!={'evidence_status':'pending','id':decision_id,'owner_bead':owner,'source':'docs/PLAN.md','source_digest':stable['source_digest']}: fail()
 if subprocess.run([str(root/'scripts/validate/plan_coverage.sh')],cwd=root,capture_output=True).returncode: fail()
 probe_rel='artifacts/m0/decisions/boring-cdc-d-values/fixture-run.jsonl'; probe=[json.loads(x) for x in (root/probe_rel).read_text().splitlines()]
 expected_probe=[{'code':'SUPPORTED_VALUES_FIXTURE_VALID','golden_vectors':len(expected_vectors),'outcome':'pass','phase':'validate_spec','type_oids':30}]
 if probe!=expected_probe or spec['execution_probe']!={'expected_lines':expected_probe,'path':probe_rel,'sha256':sha(root/probe_rel)}: fail()
 decision=next(x for x in decisions['decisions'] if x['id']==decision_id)
 approval={'approved_at':'2026-09-10T14:03:30.949Z','approved_by':'Julien Hurault (repository owner), intention 765bd3b2-4b68-4102-a9ec-43ca93357390','value_digest':hashlib.sha256(proposed.encode()).hexdigest()}
 if decision!={'approval':approval,'executor_beads':executors,'fixture_sha256':sha(root/fixture_rel),'fixture_spec':fixture_rel,'id':decision_id,'owner_bead':owner,'proposed_value':proposed,'status':'approved'}: fail()
 needed={'ART-M0-SUPPORTED-VALUES-FIXTURE':fixture_rel,'ART-M0-SUPPORTED-VALUES-PROBE':probe_rel,'ART-M0-SUPPORTED-VALUES-VALIDATION':'artifacts/m0/decisions/boring-cdc-d-values/evidence.json'}
 owned={x['id']:x for x in artifacts['artifacts'] if x.get('owner_bead')==owner}
 if set(owned)!=set(needed): fail()
 for ident,path in needed.items():
  if owned[ident]!={'id':ident,'owner_bead':owner,'path':path,'sha256':sha(root/path),'status':'complete'}: fail()
 evidence=json.loads((root/needed['ART-M0-SUPPORTED-VALUES-VALIDATION']).read_text())
 if evidence.get('schema_version')!='validation-result/v1' or evidence.get('validator_version')!='core-validators/1.0.0' or evidence.get('owner_bead')!='boring-cdc-m0.1' or evidence.get('status')!='pass' or evidence.get('findings')!=[] or evidence.get('input_sha256')!=sha(root/'contracts/m0/decisions.json') or not re.fullmatch(r'[0-9a-f]{40}',evidence.get('git_commit','')): fail()
 evidence_sha=evidence['git_commit']; guarded=[fixture_rel,'scripts/validate/supported_values.sh','contracts/m0/decisions.json']
 if subprocess.run(['git','cat-file','-e',evidence_sha+'^{commit}'],cwd=root,capture_output=True).returncode or subprocess.run(['git','merge-base','--is-ancestor',evidence_sha,'HEAD'],cwd=root,capture_output=True).returncode or subprocess.run(['git','diff','--quiet',evidence_sha+'..HEAD','--',*guarded],cwd=root).returncode: fail()
except (OSError,KeyError,ValueError,TypeError,StopIteration,json.JSONDecodeError): fail()
if selected_case=='all': print('{"code":"SUPPORTED_VALUES_FIXTURE_VALID","golden_vectors":26,"outcome":"pass","phase":"validate_spec","type_oids":30}')
else:
 failure=next((x for x in EXPECTED_FAILURES if x['case_id']==selected_case),None)
 print(json.dumps({'case_id':selected_case,'code':failure['expected_code'] if failure else 'SUPPORTED_VALUES_CASE_VALID','expected_outcome':'block_before_feedback' if failure else 'pass','outcome':'pass','phase':'validate_spec'},sort_keys=True,separators=(',',':')))
PY
