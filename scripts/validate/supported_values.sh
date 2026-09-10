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
EXPECTED_MATRIX=[{'array_oid':a,'canonical_family':f,'clickhouse':ch,'clickhouse_array':'String(tagged canonical array body)','jsonl':'envelope_v1 tagged value','name':n,'oid':o,'parquet':pq+' plus value-state tag metadata','parquet_array':'BYTE_ARRAY(canonical tagged array body) plus value-state tag metadata'} for (n,o,f,ch,pq),a in zip(SCALARS,ARRAY_OIDS)]
def env(state,typ=None,value=None):
 d={'state':state}
 if typ is not None:d['type']=typ
 if value is not None:d['value']=value
 return d
EXPECTED_CANONICAL=[
 ('bool-false',env('value','bool','false')),('bool-true',env('value','bool','true')),('int2-min',env('value','int2','-32768')),('int4-zero',env('value','int4','0')),('int8-max',env('value','int8','9223372036854775807')),('numeric-trailing-zero-normalized',env('value','numeric','+12345e+2')),('numeric-negative-scale',env('value','numeric','+1e-3')),('numeric-negative-zero',env('value','numeric','-0e+0')),('float4-nan',env('value','float4','NaN')),('float4-shortest-point-one',env('value','float4','0.1')),('float4-min-subnormal',env('value','float4','1e-45')),('float4-max-finite',env('value','float4','34028235e31')),('float4-halfway-ties-even',env('value','float4','1')),('float8-positive-infinity',env('value','float8','+Infinity')),('float8-negative-infinity',env('value','float8','-Infinity')),('float8-negative-zero',env('value','float8','-0')),('float8-shortest-point-one',env('value','float8','0.1')),('float8-fixed-exponent-shorter',env('value','float8','1e-4')),('float8-negative-fixed-exponent-shorter',env('value','float8','-1e-4')),('float8-fixed-exponent-tie-small',env('value','float8','0.01')),('float8-fixed-exponent-tie-large',env('value','float8','100')),('float8-min-subnormal',env('value','float8','5e-324')),('float8-halfway-ties-even',env('value','float8','1')),('float8-shortest-exponent',env('value','float8','1e-9')),('float8-round-trip',env('value','float8','1.2345678901234567')),('date',env('value','date','2024-02-29')),('date-positive-infinity',env('value','date','+infinity')),('timestamp-naive',env('value','timestamp','2024-02-29T12:34:56.123456')),('timestamp-negative-infinity',env('value','timestamp','-infinity')),('timestamptz-utc',env('value','timestamptz','2024-02-29T12:34:56.123456Z')),('uuid',env('value','uuid','01234567-89ab-cdef-0123-456789abcdef')),('text-no-normalization',env('value','text','é')),('bpchar-preserve-space',env('value','bpchar','x  ')),('bytea',env('value','bytea','-_8A')),('null',env('null','text')),('absent',env('absent')),('unchanged',env('unchanged','text')),('array-lower-bound',env('value','int4[]',{'element_type':'int4','length':3,'lower_bound':-2,'values':[env('value','int4','1'),env('null','int4'),env('value','int4','3')]}))]
EXPECTED_SOURCES={'bool-false':(16,'f'),'bool-true':(16,'t'),'int2-min':(21,'-32768'),'int4-zero':(23,'0'),'int8-max':(20,'9223372036854775807'),'numeric-trailing-zero-normalized':(1700,'123.4500'),'numeric-negative-scale':(1700,'1e3'),'numeric-negative-zero':(1700,'-0.000'),'float4-nan':(700,'NaN'),'float4-shortest-point-one':(700,'0.1'),'float4-min-subnormal':(700,'1.401298464324817e-45'),'float4-max-finite':(700,'3.4028234663852886e38'),'float4-halfway-ties-even':(700,'1.0000000596046448'),'float8-positive-infinity':(701,'Infinity'),'float8-negative-infinity':(701,'-Infinity'),'float8-negative-zero':(701,'-0'),'float8-shortest-point-one':(701,'0.1'),'float8-fixed-exponent-shorter':(701,'0.0001'),'float8-negative-fixed-exponent-shorter':(701,'-0.0001'),'float8-fixed-exponent-tie-small':(701,'0.01'),'float8-fixed-exponent-tie-large':(701,'100'),'float8-min-subnormal':(701,'4.9406564584124654e-324'),'float8-halfway-ties-even':(701,'1.00000000000000011102230246251565404236316680908203125'),'float8-shortest-exponent':(701,'1e-9'),'float8-round-trip':(701,'1.2345678901234567'),'date':(1082,'2024-02-29'),'date-positive-infinity':(1082,'infinity'),'timestamp-naive':(1114,'2024-02-29 12:34:56.123456'),'timestamp-negative-infinity':(1114,'-infinity'),'timestamptz-utc':(1184,'2024-02-29 12:34:56.123456+00'),'uuid':(2950,'01234567-89AB-CDEF-0123-456789ABCDEF'),'text-no-normalization':(25,'é'),'bpchar-preserve-space':(1042,'x  '),'bytea':(17,'\\xfbff00'),'array-lower-bound':(1007,'[-2:0]={1,NULL,3}')}
EXPECTED_FAILURES=[{'case_id':'unsupported-oid','input':{'oid':114},'expected_code':'SUPPORTED_VALUES_UNSUPPORTED_TYPE'},{'case_id':'numeric-precision-over','input':{'precision':1001},'expected_code':'SUPPORTED_VALUES_LIMIT_EXCEEDED'},{'case_id':'numeric-scale-low','input':{'scale':-16384},'expected_code':'SUPPORTED_VALUES_LIMIT_EXCEEDED'},{'case_id':'numeric-scale-high','input':{'scale':16384},'expected_code':'SUPPORTED_VALUES_LIMIT_EXCEEDED'},{'case_id':'scalar-over','input':{'scalar_bytes':1048577},'expected_code':'SUPPORTED_VALUES_LIMIT_EXCEEDED'},{'case_id':'row-over','input':{'row_bytes':4194305},'expected_code':'SUPPORTED_VALUES_LIMIT_EXCEEDED'},{'case_id':'event-over','input':{'event_bytes':8388609},'expected_code':'SUPPORTED_VALUES_LIMIT_EXCEEDED'},{'case_id':'array-dimensions-over','input':{'array_dimensions':2},'expected_code':'SUPPORTED_VALUES_LIMIT_EXCEEDED'},{'case_id':'array-elements-over','input':{'array_elements':10001},'expected_code':'SUPPORTED_VALUES_LIMIT_EXCEEDED'}]
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def canonical_bytes(value): return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
def numeric_body(text):
 m=re.fullmatch(r'([+-]?)([0-9]*)(?:\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?',text)
 if not m or not (m.group(2) or m.group(3)): raise ValueError('invalid numeric fixture input')
 sign='-' if m.group(1)=='-' else '+'; whole=m.group(2) or ''; frac=m.group(3) or ''; exponent=int(m.group(4) or 0)
 digits=(whole+frac).lstrip('0') or '0'; scale=len(frac)-exponent
 if digits!='0':
  while digits.endswith('0'): digits=digits[:-1]; scale-=1
 else: scale=0
 return f'{sign}{digits}e{scale:+d}'
def source_record(case_id):
 if case_id in EXPECTED_SOURCES:
  oid,text=EXPECTED_SOURCES[case_id]; return {'format':'pgoutput_text','oid':oid,'text_utf8_hex':text.encode().hex()}
 if case_id=='null': return {'format':'pgoutput_tuple_state','oid':25,'token':'n'}
 if case_id=='absent': return {'attribute_number':3,'format':'connector_state','reason':'tuple_side_not_present','relation_oid':42000,'source_protocol':'pgoutput','token':'absent','type_oid':25}
 if case_id=='unchanged': return {'format':'pgoutput_tuple_state','oid':25,'token':'u'}
 raise ValueError('unknown golden source')
def shortest_float(text,width):
 import struct
 value=float(text); pack=lambda x: struct.pack('>f' if width==32 else '>d',x)
 target=pack(value); rounded=struct.unpack('>f',target)[0] if width==32 else value
 def equivalent_spellings(decimal):
  sign=''
  if decimal.startswith('-'): sign,decimal='-',decimal[1:]
  mantissa,marker,exponent=decimal.partition('e')
  decimal_exponent=int(exponent) if marker else 0
  whole,dot,fraction=mantissa.partition('.')
  digits=(whole+fraction).lstrip('0') or '0'
  decimal_exponent-=len(fraction)
  while len(digits)>1 and digits.endswith('0'):
   digits=digits[:-1]; decimal_exponent+=1
  point=len(digits)+decimal_exponent
  if point<=0: fixed='0.'+'0'*(-point)+digits
  elif point>=len(digits): fixed=digits+'0'*(point-len(digits))
  else: fixed=digits[:point]+'.'+digits[point:]
  yield sign+fixed
  for split in range(1,len(digits)+1):
   mantissa=digits[:split]+(('.'+digits[split:]) if split<len(digits) else '')
   yield sign+mantissa+'e'+str(decimal_exponent+len(digits)-split)
 candidates=set()
 for precision in range(1,10 if width==32 else 18):
  rounded_decimal=format(rounded,f'.{precision}g').lower()
  for candidate in equivalent_spellings(rounded_decimal):
   try: matches=pack(float(candidate))==target
   except OverflowError: matches=False
   if matches: candidates.add(candidate)
 if not candidates: raise ValueError('no shortest round-trip float')
 # Global byte minimum; lexical UTF-8 order breaks equal-length ties and therefore
 # selects fixed notation over exponent notation for ties such as 0.01/1e-2.
 return min(candidates,key=lambda candidate:(len(candidate.encode()),candidate.encode()))
def encode_source(case_id,source):
 if source!=source_record(case_id): raise ValueError('source drift')
 if source['format']!='pgoutput_text':
  typ='text' if source.get('oid')==25 else None
  if source['format']=='connector_state':
   if source.get('source_protocol')!='pgoutput' or source.get('type_oid')!=25 or source.get('reason')!='tuple_side_not_present': raise ValueError('untyped absent state')
   typ=None
  return env({'n':'null','u':'unchanged','absent':'absent'}[source['token']],typ)
 oid=source['oid']; text=bytes.fromhex(source['text_utf8_hex']).decode(); name=next((n for n,o,*_ in SCALARS if o==oid),None)
 if oid==1007:
  m=re.fullmatch(r'\[(-?[0-9]+):(-?[0-9]+)\]=\{(.*)\}',text)
  if not m: raise ValueError('invalid array fixture input')
  lo,hi=int(m.group(1)),int(m.group(2)); parts=m.group(3).split(',')
  if hi-lo+1!=len(parts): raise ValueError('array bound mismatch')
  vals=[env('null','int4') if x=='NULL' else env('value','int4',str(int(x))) for x in parts]
  return env('value','int4[]',{'element_type':'int4','length':len(vals),'lower_bound':lo,'values':vals})
 if name=='bool': body={'t':'true','f':'false'}[text]
 elif name in ('int2','int4','int8'): body=str(int(text))
 elif name=='numeric': body=numeric_body(text)
 elif name in ('float4','float8'):
  body={'+Infinity':'+Infinity','Infinity':'+Infinity','-Infinity':'-Infinity','NaN':'NaN','-0':'-0'}.get(text)
  if body is None: body=shortest_float(text,32 if name=='float4' else 64)
 elif name in ('date','timestamp'):
  body=('+infinity' if text=='infinity' else text.replace(' ','T'))
 elif name=='timestamptz': body=text.replace(' ','T').removesuffix('+00')+'Z'
 elif name=='uuid': body=text.lower()
 elif name=='bytea':
  import base64
  body=base64.urlsafe_b64encode(bytes.fromhex(text.removeprefix('\\x'))).decode().rstrip('=')
 else: body=text
 return env('value',name,body)
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
 required_literals=['minimal base-10 decimal','<sign><unscaled-digits>e<signed-scale>','globally shortest JSON number spelling','lexicographically smallest UTF-8 bytes','exactly six fractional digits','base64url without padding','without Unicode normalization','preserve lower bound']
 contract_text=json.dumps(spec['canonical_contract'],sort_keys=True)
 if any(x not in contract_text for x in required_literals): fail()
 serialization={'encoding':'UTF-8','hash':'SHA-256','profile':'RFC 8785 JCS over the envelope object; all object keys are ASCII and all numeric bodies are strings','scope':'golden corpus transport and hash only; event framing and event/payload hash preimages remain owned by boring-cdc-m0-event-format'}
 if spec['canonical_fixture_serialization']!=serialization: fail()
 destinations={'clickhouse':{'array_layout':'value_state Enum8 plus value_type LowCardinality(String) plus String containing the RFC-8785 tagged canonical array body','exact_native_scalars':['bool:Bool','int2:Int16','int4:Int32','int8:Int64','float4:Float32','float8:Float64','uuid:UUID','text:String','varchar:String','bpchar:String','bytea:String(base64url unpadded)'],'fallback_scalars':['numeric:String(tagged canonical body)','date:String(tagged canonical body)','timestamp:String(tagged canonical body)','timestamptz:String(tagged canonical body)'],'state_layout':"value_state Enum8('value'=1,'null'=2,'absent'=3,'unchanged'=4) plus value_type LowCardinality(String); native value is nullable and read only when state=value"},'jsonl':{'mapping':'RFC-8785 envelope object with exactly state and, when applicable, type and value; value body rules are canonical_contract'},'parquet':{'array_layout':'required INT32 value_state enum plus required UTF8 value_type plus nullable BYTE_ARRAY value containing RFC-8785 tagged canonical array body','scalar_layout':'required INT32 value_state enum (value=1,null=2,absent=3,unchanged=4), required UTF8 value_type when known, and nullable listed typed value column','fallback':'BYTE_ARRAY containing canonical tagged body when typed representation is not exact'}}
 if spec['destination_contract']!=destinations: fail()
 expected_vectors=[]
 for case_id,value in EXPECTED_CANONICAL:
  source=source_record(case_id); encoded=encode_source(case_id,source)
  if encoded!=value: fail()
  body=canonical_bytes(encoded); expected_vectors.append({'canonical':encoded,'canonical_utf8_hex':body.hex(),'case_id':case_id,'sha256':hashlib.sha256(body).hexdigest(),'source':source})
 if spec['golden_vectors']!=expected_vectors or spec['failure_vectors']!=EXPECTED_FAILURES: fail()
 if spec['expected']['metrics']!={'golden_vector_count':{'unit':'vectors','value':len(expected_vectors)},'supported_oid_count':{'unit':'oids','value':30}}: fail()
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
if selected_case=='all': print('{"code":"SUPPORTED_VALUES_FIXTURE_VALID","golden_vectors":38,"outcome":"pass","phase":"validate_spec","type_oids":30}')
else:
 failure=next((x for x in EXPECTED_FAILURES if x['case_id']==selected_case),None)
 print(json.dumps({'case_id':selected_case,'code':failure['expected_code'] if failure else 'SUPPORTED_VALUES_CASE_VALID','expected_outcome':'block_before_feedback' if failure else 'pass','outcome':'fail' if failure else 'pass','phase':'execute_fixture'},sort_keys=True,separators=(',',':')))
 if failure: raise SystemExit(2)
PY
