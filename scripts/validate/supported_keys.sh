#!/bin/sh
set -eu
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CASE_ID=${1:-all}
exec python3 - "$ROOT" "$CASE_ID" <<'PY'
import hashlib,json,re,subprocess,sys
from pathlib import Path
root=Path(sys.argv[1]); selected=sys.argv[2]
owner='boring-cdc-d-keys'; decision='DEC-SUPPORTED-KEYS'
fixture_rel='fixtures/m0/decisions/boring-cdc-d-keys.json'
executors=['boring-cdc-m1-ordering','boring-cdc-m3-planner']
proposed='Canonical source keys use 1..8 non-null effective-replica-identity components in index order from int2, int4, int8, numeric(precision <=100, scale -18..18), uuid, date, timestamp, timestamptz, C-collated text/varchar/bpchar, or bytea <=1 KiB; encoding v1 and PostgreSQL ascending B-tree order are fixed by owner intention 765bd3b2-4b68-4102-a9ec-43ca93357390.'
TYPES=[
 {'name':'int2','oid':21,'constraints':{},'payload':'sign-bit-flipped big-endian 16-bit integer'},
 {'name':'int4','oid':23,'constraints':{},'payload':'sign-bit-flipped big-endian 32-bit integer'},
 {'name':'int8','oid':20,'constraints':{},'payload':'sign-bit-flipped big-endian 64-bit integer'},
 {'name':'numeric','oid':1700,'constraints':{'precision_max':100,'scale_max':18,'scale_min':-18},'payload':'canonical sign, exponent, and digits'},
 {'name':'uuid','oid':2950,'constraints':{},'payload':'network-order 16 bytes'},
 {'name':'date','oid':1082,'constraints':{},'payload':'signed big-endian days'},
 {'name':'timestamp','oid':1114,'constraints':{},'payload':'signed big-endian microseconds'},
 {'name':'timestamptz','oid':1184,'constraints':{},'payload':'signed big-endian microseconds'},
 {'name':'text','oid':25,'constraints':{'collation':'C'},'payload':'original UTF-8 bytes'},
 {'name':'varchar','oid':1043,'constraints':{'collation':'C'},'payload':'original UTF-8 bytes'},
 {'name':'bpchar','oid':1042,'constraints':{'collation':'C'},'payload':'original UTF-8 bytes'},
 {'name':'bytea','oid':17,'constraints':{'bytes_max':1024},'payload':'raw bytes'}]
CODES=['KEY_NO_UNIQUE_NOT_NULL_INDEX','KEY_NULLABLE_COMPONENT','KEY_UNSUPPORTED_TYPE','KEY_UNSUPPORTED_COLLATION','KEY_ARITY_EXCEEDED','KEY_VALUE_TOO_LARGE','KEY_ENCODING_VERSION_UNSUPPORTED']
FAILURES=[
 ('no-unique-not-null-index',{'index':None},'KEY_NO_UNIQUE_NOT_NULL_INDEX'),
 ('nullable-index-component',{'nullable':True},'KEY_NULLABLE_COMPONENT'),
 ('runtime-null-component',{'component_state':'null'},'KEY_NULLABLE_COMPONENT'),
 ('update-old-key-absent',{'operation':'update','old_key_state':'absent'},'KEY_NULLABLE_COMPONENT'),
 ('delete-key-partial',{'operation':'delete','old_key_state':'partial'},'KEY_NULLABLE_COMPONENT'),
 ('update-key-unchanged-toast',{'operation':'update','new_key_state':'unchanged_toast'},'KEY_NULLABLE_COMPONENT'),
 ('float-key',{'type':'float8'},'KEY_UNSUPPORTED_TYPE'),
 ('bool-key',{'type':'bool'},'KEY_UNSUPPORTED_TYPE'),
 ('array-key',{'type':'int4[]'},'KEY_UNSUPPORTED_TYPE'),
 ('domain-key',{'type_kind':'domain'},'KEY_UNSUPPORTED_TYPE'),
 ('enum-key',{'type_kind':'enum'},'KEY_UNSUPPORTED_TYPE'),
 ('composite-key',{'type_kind':'composite'},'KEY_UNSUPPORTED_TYPE'),
 ('udt-key',{'type_kind':'user_defined'},'KEY_UNSUPPORTED_TYPE'),
 ('icu-text-key',{'type':'text','collation_provider':'icu'},'KEY_UNSUPPORTED_COLLATION'),
 ('non-c-text-key',{'type':'varchar','collation':'en_US'},'KEY_UNSUPPORTED_COLLATION'),
 ('arity-nine',{'arity':9},'KEY_ARITY_EXCEEDED'),
 ('bytea-over-limit',{'bytes':1025,'type':'bytea'},'KEY_VALUE_TOO_LARGE'),
 ('unknown-encoding-version',{'encoding_version':2},'KEY_ENCODING_VERSION_UNSUPPORTED')]
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def fail():
 print('{"code":"SUPPORTED_KEYS_FIXTURE_INVALID","outcome":"fail","phase":"validate_spec"}'); raise SystemExit(1)
try:
 spec=json.loads((root/fixture_rel).read_text()); decisions=json.loads((root/'contracts/m0/decisions.json').read_text()); artifacts=json.loads((root/'contracts/m0/artifacts.json').read_text()); coverage=json.loads((root/'contracts/coverage/plan-to-beads.json').read_text()); registry=json.loads((root/'contracts/agent/stable-ids.json').read_text()); graph=[json.loads(x) for x in (root/'.beads/issues.jsonl').read_text().splitlines()]
 required=('inputs','preconditions','supported_types','rejected_types','canonical_encoding','ordering','identity_contract','keyset_contract','mutable_key_contract','failure_vectors','deterministic_phase','expected','expected_failure','result_contract','redaction_assertions','later_executors')
 if any(not spec.get(x) for x in required): fail()
 if spec['schema_version']!='m0-decision-fixture/v1' or spec['fixture_id']!=decision or spec['decision_id']!=decision or spec['owner_bead']!=owner or spec['later_executors']!=executors: fail()
 if spec['approval']!={'approved_at':'2026-09-10T14:03:30.949Z','approved_by':'Julien Hurault (repository owner)','intention_id':'765bd3b2-4b68-4102-a9ec-43ca93357390','selection':'Accept recommended defaults'}: fail()
 if spec['fixed_seed']!={'ascii':'BCDC_KEYS_V01','hex':'0x424344435f4b4559535f563031'}: fail()
 if spec['supported_types']!=TYPES or spec['stable_rejection_codes']!=CODES: fail()
 if [(x['case_id'],x['input'],x['expected_code']) for x in spec['failure_vectors']]!=FAILURES: fail()
 enc=spec['canonical_encoding']
 if enc!={'arity':{'maximum':8,'minimum':1,'order':'effective replica identity index order'},'component_frame':['length-delimited PostgreSQL OID/type','length-delimited canonical payload'],'tuple_frame':['encoding version','arity','components in index order'],'version':1}: fail()
 if spec['ordering']!={'collation':'PostgreSQL C for text, varchar, and bpchar','direction':'ascending','nulls':'forbidden','rule':'lexicographic PostgreSQL B-tree order over typed components; compare component values using their admitted PostgreSQL ascending operator class, then the next component'}: fail()
 if spec['rejected_types']!=['float4','float8','bool','arrays','domains','enums','composites','user-defined types']: fail()
 identity=spec['identity_contract']
 if identity['canonical_identity']!='one effective replica identity shared by snapshot keysets, WAL update/delete keys, checksums, and destination grouping' or identity['forbidden_component_states']!=['null','absent','partial','unchanged_toast']: fail()
 if spec['keyset_contract']!={'pagination':'half-open keyset predicate in canonical ascending tuple order','prohibited':['OFFSET','ctid'],'resume_rule':'last emitted complete canonical key is the exclusive lower bound'}: fail()
 if spec['mutable_key_contract']!={'complete_old_key_required':True,'complete_new_key_required':True,'new_tuple_required':True,'order':['old-key tombstone','new-key upsert'],'unchanged_toast_any_column':'block before feedback'}: fail()
 if spec['expected']['metrics']!={'failure_vector_count':{'unit':'vectors','value':18},'supported_type_count':{'unit':'types','value':12}}: fail()
 ids=[x['id'] for x in graph]
 if len(ids)!=len(set(ids)) or set(executors)-set(ids): fail()
 stable=next(x for x in registry['entries'] if x['id']==decision); covered=next(x for x in coverage['assignments'] if x['id']==decision)
 if stable['owner_bead']!=owner or covered!={'evidence_status':'pending','id':decision,'owner_bead':owner,'source':'docs/PLAN.md','source_digest':stable['source_digest']}: fail()
 if subprocess.run([str(root/'scripts/validate/plan_coverage.sh')],cwd=root,capture_output=True).returncode: fail()
 probe_rel='artifacts/m0/decisions/boring-cdc-d-keys/fixture-run.jsonl'; probe=[json.loads(x) for x in (root/probe_rel).read_text().splitlines()]
 expected_probe=[{'code':'SUPPORTED_KEYS_FIXTURE_VALID','failure_vectors':18,'outcome':'pass','phase':'validate_spec','supported_types':12}]
 if probe!=expected_probe or spec['execution_probe']!={'expected_lines':expected_probe,'path':probe_rel,'sha256':sha(root/probe_rel)}: fail()
 if spec['script']['path']!='scripts/validate/supported_keys.sh' or sha(root/spec['script']['path'])!=spec['script']['sha256']: fail()
 decision_row=next(x for x in decisions['decisions'] if x['id']==decision)
 approval={'approved_at':'2026-09-10T14:03:30.949Z','approved_by':'Julien Hurault (repository owner), intention 765bd3b2-4b68-4102-a9ec-43ca93357390','value_digest':hashlib.sha256(proposed.encode()).hexdigest()}
 if decision_row!={'approval':approval,'executor_beads':executors,'fixture_sha256':sha(root/fixture_rel),'fixture_spec':fixture_rel,'id':decision,'owner_bead':owner,'proposed_value':proposed,'status':'approved'}: fail()
 needed={'ART-M0-SUPPORTED-KEYS-FIXTURE':fixture_rel,'ART-M0-SUPPORTED-KEYS-PROBE':probe_rel,'ART-M0-SUPPORTED-KEYS-VALIDATION':'artifacts/m0/decisions/boring-cdc-d-keys/evidence.json'}
 owned={x['id']:x for x in artifacts['artifacts'] if x.get('owner_bead')==owner}
 if set(owned)!=set(needed): fail()
 for ident,path in needed.items():
  if owned[ident]!={'id':ident,'owner_bead':owner,'path':path,'sha256':sha(root/path),'status':'complete'}: fail()
 evidence=json.loads((root/needed['ART-M0-SUPPORTED-KEYS-VALIDATION']).read_text())
 if evidence.get('schema_version')!='validation-result/v1' or evidence.get('validator_version')!='core-validators/1.0.0' or evidence.get('owner_bead')!='boring-cdc-m0.1' or evidence.get('status')!='pass' or evidence.get('findings')!=[] or evidence.get('input_sha256')!=sha(root/'contracts/m0/decisions.json') or not re.fullmatch(r'[0-9a-f]{40}',evidence.get('git_commit','')): fail()
 evidence_sha=evidence['git_commit']; guarded=[fixture_rel,'scripts/validate/supported_keys.sh','contracts/m0/decisions.json']
 if subprocess.run(['git','cat-file','-e',evidence_sha+'^{commit}'],cwd=root,capture_output=True).returncode or subprocess.run(['git','merge-base','--is-ancestor',evidence_sha,'HEAD'],cwd=root,capture_output=True).returncode or subprocess.run(['git','diff','--quiet',evidence_sha+'..HEAD','--',*guarded],cwd=root).returncode: fail()
except (OSError,KeyError,ValueError,TypeError,StopIteration,json.JSONDecodeError): fail()
if selected=='all': print('{"code":"SUPPORTED_KEYS_FIXTURE_VALID","failure_vectors":18,"outcome":"pass","phase":"validate_spec","supported_types":12}')
else:
 row=next((x for x in FAILURES if x[0]==selected),None)
 if row is None: fail()
 print(json.dumps({'case_id':selected,'code':row[2],'expected_outcome':'block_before_feedback','outcome':'fail','phase':'execute_fixture'},sort_keys=True,separators=(',',':'))); raise SystemExit(2)
PY
