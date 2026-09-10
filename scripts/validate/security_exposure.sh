#!/bin/sh
set -eu
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CASE_ID=${1:-all}
exec python3 - "$ROOT" "$CASE_ID" <<'PY'
import hashlib,json,re,subprocess,sys
from pathlib import Path
root=Path(sys.argv[1]); selected_case=sys.argv[2]; owner='boring-cdc-d-security'; decision_id='DEC-SECURITY-EXPOSURE'
fixture_rel='fixtures/m0/decisions/boring-cdc-d-security.json'
executors=['boring-cdc-m1-preflight','boring-cdc-m1-cli-contract','boring-cdc-m2-ownership','boring-cdc-m2-fault-status','boring-cdc-m5.1','boring-cdc-m6-metrics','boring-cdc-m6-failure-matrix']
proposed='Status and metrics are read-only and loopback-bound by default; non-loopback exposure requires authentication and verified TLS (minimum TLS 1.2, TLS 1.3 preferred); PostgreSQL and ClickHouse TLS certificates are verified; the mutating endpoint is Unix-domain-only under a 0700 directory with a 0600 socket, Linux peer credentials, 1 MiB requests, 4 MiB responses, 10 second reads, and 30 second writes; confirmations expire after 5 minutes and bind a 128-bit CSPRNG base64url-unpadded nonce to RFC 8785 JCS canonical payloads with SHA-256; state, spool, and archive directories are 0700 and secret-bearing files are 0600 or stricter; administration credentials exist only around the sole maintenance-owner request and readback; recursive redaction is bounded to depth 8 and 64 KiB and covers driver authentication errors, DSN/URL strings, TLS handshake errors, nested cause chains, SQLSTATE detail, and filesystem paths.'
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
CATEGORY_PAYLOADS={
 'driver_authentication_errors':{'kind':'driver_authentication_error','driver':'postgresql','message':'password authentication failed for user fixture_user: CREDENTIAL_MARKER','credential':{'username':'fixture_user','password':'CREDENTIAL_MARKER'}},
 'dsn_and_url_strings':{'kind':'dsn_url_error','dsn':'postgresql://fixture_user:CREDENTIAL_MARKER@db.invalid/app?sslpassword=CREDENTIAL_MARKER','url':'https://fixture_user:CREDENTIAL_MARKER@sink.invalid/ingest?token=CREDENTIAL_MARKER'},
 'tls_handshake_errors':{'kind':'tls_handshake_error','message':'TLS handshake failed: client token CREDENTIAL_MARKER','server_name':'db.internal.invalid','certificate_subject':'CN=CREDENTIAL_MARKER'},
 'nested_cause_chains':{'kind':'nested_cause_error','message':'outer connector failure CREDENTIAL_MARKER','secret':'CREDENTIAL_MARKER'},
 'sqlstate_detail':{'kind':'sqlstate_error','sqlstate':'28P01','detail':'password CREDENTIAL_MARKER rejected for fixture_user','hint':'check postgresql://fixture_user:CREDENTIAL_MARKER@db.invalid/app'},
 'filesystem_paths':{'kind':'filesystem_error','path':'/var/lib/boring-cdc/CREDENTIAL_MARKER/state.sqlite','message':'permission denied opening /var/lib/boring-cdc/CREDENTIAL_MARKER/state.sqlite'},
}
CATEGORY_SIGNATURES={
 'driver_authentication_error':('driver_authentication_errors',{'driver','credential'}),
 'dsn_url_error':('dsn_and_url_strings',{'dsn','url'}),
 'tls_handshake_error':('tls_handshake_errors',{'server_name','certificate_subject'}),
 'nested_cause_error':('nested_cause_chains',{'message','secret'}),
 'sqlstate_error':('sqlstate_detail',{'sqlstate','detail'}),
 'filesystem_error':('filesystem_paths',{'path','message'}),
}
SENSITIVE_KEYS={'password','dsn','url','certificate_subject','detail','hint','path','secret'}
EXPECTED_RECIPE={'encoding':'utf-8','fill_byte':' ','kind':'category_specific_structured_error','serialization':'canonical_compact_json_with_trailing_space_padding','templates':'validator_owned_CATEGORY_PAYLOADS','cause_link':'cause'}
def canonical_bytes(value): return json.dumps(value,sort_keys=True,separators=(',',':')).encode('utf-8')
def structured_input(corpus, depth, byte_length, recipe):
 root_node=json.loads(json.dumps(CATEGORY_PAYLOADS[corpus]))
 node=root_node
 for level in range(2,depth+1):
  node['cause']={'kind':'cause','level':level,'message':f'cause level {level}: CREDENTIAL_MARKER','secret':'CREDENTIAL_MARKER'}
  node=node['cause']
 base=canonical_bytes(root_node); fill=recipe['fill_byte'].encode(recipe['encoding'])
 if fill!=b' ' or len(base)>byte_length: raise ValueError('structured redaction input exceeds vector size')
 raw=base + fill*(byte_length-len(base))
 if len(raw)!=byte_length: raise ValueError('structured redaction input has wrong byte length')
 return raw
def sanitize_value(value, key=None):
 if key in SENSITIVE_KEYS: return '<redacted>',1
 if isinstance(value,dict):
  result={}; count=0
  for child_key,child in value.items():
   if child_key=='cause': continue
   result[child_key],added=sanitize_value(child,child_key); count+=added
  return result,count
 if isinstance(value,list):
  result=[]; count=0
  for child in value:
   clean,added=sanitize_value(child); result.append(clean); count+=added
  return result,count
 if isinstance(value,str) and ('CREDENTIAL_MARKER' in value or 'postgresql://' in value or 'https://' in value or '/var/lib/' in value): return '<redacted>',1
 return value,0
def reference_redact(corpus, depth, byte_length, recipe):
 raw=structured_input(corpus,depth,byte_length,recipe)
 collected=raw[:65536]
 parsed=json.loads(collected.decode(recipe['encoding']))
 try: observed_corpus,required=CATEGORY_SIGNATURES[parsed['kind']]
 except (KeyError,TypeError): raise ValueError('unrecognized structured redaction category')
 if not required.issubset(parsed): raise ValueError('incomplete structured redaction category')
 sanitized=[]; redacted_fields=0; node=parsed; actual_depth=0
 while node is not None:
  if not isinstance(node,dict): raise ValueError('cause chain node is not structured')
  actual_depth+=1
  if actual_depth<=8:
   clean,added=sanitize_value(node); sanitized.append(clean); redacted_fields+=added
  node=node.get('cause')
 depth_processed=min(actual_depth,8); depth_truncated=actual_depth>8
 bytes_truncated=len(raw)>65536; sanitized_bytes=canonical_bytes(sanitized)
 if b'CREDENTIAL_MARKER' in sanitized_bytes or observed_corpus!=corpus or actual_depth!=depth or redacted_fields<depth_processed: raise ValueError('structured redaction traversal failed')
 return {'bytes_truncated':bytes_truncated,'case_id':f'{observed_corpus}-depth-{actual_depth}-bytes-{len(raw)}','code':'SECURITY_REDACTION_LIMITED' if depth_truncated or bytes_truncated else 'SECURITY_REDACTED','consumed_bytes':min(len(raw),65536),'contains_input_markers':False,'depth_processed':depth_processed,'depth_truncated':depth_truncated,'input_kind':parsed['kind'],'redacted_corpus':observed_corpus,'redacted_fields':redacted_fields,'sanitized_shape_sha256':hashlib.sha256(sanitized_bytes).hexdigest(),'sanitized_value':'<redacted>'}
def fail():
 print('{"code":"SECURITY_EXPOSURE_FIXTURE_INVALID","outcome":"fail","phase":"validate_spec"}'); raise SystemExit(1)
try:
 spec=json.loads((root/fixture_rel).read_text()); decisions=json.loads((root/'contracts/m0/decisions.json').read_text())
 artifacts=json.loads((root/'contracts/m0/artifacts.json').read_text()); registry=json.loads((root/'contracts/agent/stable-ids.json').read_text())
 coverage=json.loads((root/'contracts/coverage/plan-to-beads.json').read_text()); graph=[json.loads(x) for x in (root/'.beads/issues.jsonl').read_text().splitlines()]
 required=('inputs','preconditions','supported_matrix','deterministic_phase','expected','expected_failure','result_contract','redaction_assertions','later_executors')
 if any(not spec.get(x) for x in required): fail()
 if spec['schema_version']!='m0-decision-fixture/v1' or spec['fixture_id']!=decision_id or spec['decision_id']!=decision_id or spec['owner_bead']!=owner or spec['later_executors']!=executors: fail()
 boundary=spec['approved_boundary']
 if boundary['listener']!={'default_bind':'loopback','mutation_routes':0,'non_loopback_authentication':'required','non_loopback_tls':'required','read_only':True}: fail()
 if boundary['tls']!={'clickhouse_certificate_verification':True,'minimum_version':'TLS 1.2','postgresql_certificate_verification':True,'preferred_version':'TLS 1.3'}: fail()
 if boundary['command_endpoint']!={'directory_mode':'0700','kind':'unix_domain_only','peer_authentication':'linux_peer_credentials_connector_account','request_bytes_max':1048576,'response_bytes_max':4194304,'socket_mode':'0600','read_timeout_seconds':10,'write_timeout_seconds':30}: fail()
 if boundary['confirmation']!={'canonicalization':'RFC 8785 JCS','digest':'SHA-256','expiry_seconds':300,'nonce_encoding':'base64url_unpadded','nonce_entropy_bits':128,'nonce_source':'CSPRNG','request_id_preimage':'nonce || canonical_request_payload'}: fail()
 if boundary['redaction']!={'corpus':['driver_authentication_errors','dsn_and_url_strings','tls_handshake_errors','nested_cause_chains','sqlstate_detail','filesystem_paths'],'depth_max':8,'input_bytes_max':65536,'replacement':'stable_redacted_reason_codes'}: fail()
 if boundary['filesystem']!={'directory_mode':'0700','directories':['state','spool','archive'],'file_mode':'0600_or_stricter','secret_bearing_files':['sqlite','spool','intent','manifest','configuration']}: fail()
 if boundary['administration_credential']!={'loaded_by':'sole_lock_owning_maintenance_runtime','loaded_for':'exact_owner_only_request_and_readback','normal_run_loads':False,'released_after_readback':True}: fail()
 cases={x['case_id']:x for x in spec['supported_matrix']}
 required_cases={'loopback_default','non_loopback_without_auth','non_loopback_without_tls','verified_tls','normal_progress_confirmation','self_issued_nonce','changed_relevant_revision','unrelated_observation_progress','pin_loss','resource_failure','status_tcp_mutation','unix_peer_mismatch','filesystem_mode_too_open','redacted_nested_driver_error','redacted_explanation'}
 if set(cases)!=required_cases or len(cases)!=len(spec['supported_matrix']): fail()
 expected_outcomes={'loopback_default':'pass','non_loopback_without_auth':'reject','non_loopback_without_tls':'reject','verified_tls':'pass','normal_progress_confirmation':'pass','self_issued_nonce':'reject','changed_relevant_revision':'reject_new_dry_run_required','unrelated_observation_progress':'pass','pin_loss':'reject','resource_failure':'reject','status_tcp_mutation':'reject','unix_peer_mismatch':'reject','filesystem_mode_too_open':'reject','redacted_nested_driver_error':'pass_redacted','redacted_explanation':'pass_redacted'}
 if any(cases[k].get('expected_outcome')!=v for k,v in expected_outcomes.items()): fail()
 vectors=spec['vectors']
 if set(vectors)!=required_cases or any(vectors[k].get('case_id')!=k or vectors[k].get('expected_outcome')!=expected_outcomes[k] or not vectors[k].get('inputs') or not vectors[k].get('before') or not vectors[k].get('action') or not vectors[k].get('expected') for k in required_cases): fail()
 for name in ('normal_progress_confirmation','self_issued_nonce','changed_relevant_revision','unrelated_observation_progress','pin_loss','resource_failure'):
  v=vectors[name]
  if not v.get('action_control_revisions') or 'state_revision' not in v['before'] or 'snapshot_id' not in v['before'] or 'intent_record' not in v['expected']: fail()
 for name in ('redacted_nested_driver_error','redacted_explanation'):
  if not vectors[name].get('redaction_assertions'): fail()
 rv=vectors['redacted_nested_driver_error']
 if rv['inputs'].get('depth_boundary')!=[8,9] or rv['inputs'].get('byte_boundary')!=[65536,65537] or set(rv['inputs'].get('corpus',[]))!=set(boundary['redaction']['corpus']): fail()
 recipe=rv['inputs']['generator']
 if recipe!=EXPECTED_RECIPE: fail()
 generated=[]
 for corpus in boundary['redaction']['corpus']:
  for depth in rv['inputs']['depth_boundary']:
   for size in rv['inputs']['byte_boundary']: generated.append(reference_redact(corpus,depth,size,recipe))
 if generated!=rv['expected']['output']['sanitized_vectors'] or any(x['contains_input_markers'] or x['consumed_bytes']>65536 or x['depth_processed']>8 for x in generated): fail()
 registration=spec['operation_registration_contract']
 if registration!={'registry_owner':'boring-cdc-m1-cli-contract','planning_inventory_combined_command':'traceability_only_not_cli_registry','required_entries':[{'operation_key':'journal_inspect','runtime_executor':'boring-cdc-m2-reconcile','mutation_class':'read_only','result_kind':'bounded_event_inspection'},{'operation_key':'journal_inspect_explain','runtime_executor':'boring-cdc-m5.1','mutation_class':'read_only','result_kind':'evidence_qualified_redacted_explanation'}],'separate_operation_ids_required':True,'combined_registration_forbidden':True}: fail()
 if selected_case!='all' and selected_case not in vectors: fail()
 if spec['confirmation_revision_contract']!={'authorization_dependencies':'per_command_action_relevant_control_revisions','observation_provenance':['snapshot_id','state_revision'],'resource_measurements':'atomically_rechecked_current_safety_predicates','unrelated_observation_change':'does_not_invalidate','relevant_revision_or_bound_fingerprint_change':'new_dry_run_required','pin_or_resource_predicate_loss':'reject_before_intent_acceptance'}: fail()
 if spec['explanation_contract']!={'command_variant':'journal inspect --event-id ID --explain --json','mutation_class':'read_only','registration_owner':'boring-cdc-m1-cli-contract','runtime_executor':'boring-cdc-m5.1','separate_from_base_inspect':True,'output_policy':'identifiers_hashes_and_redacted_reason_codes_no_payloads_or_credentials'}: fail()
 if spec['script']['path']!='scripts/validate/security_exposure.sh' or sha(root/spec['script']['path'])!=spec['script']['sha256']: fail()
 graph_ids={x['id'] for x in graph}
 if set(executors)-graph_ids: fail()
 stable=next(x for x in registry['entries'] if x['id']==decision_id); covered=next(x for x in coverage['assignments'] if x['id']==decision_id)
 if stable['owner_bead']!=owner or covered!={'evidence_status':'pending','id':decision_id,'owner_bead':owner,'source':'docs/PLAN.md','source_digest':stable['source_digest']}: fail()
 if subprocess.run([str(root/'scripts/validate/plan_coverage.sh')],cwd=root,capture_output=True).returncode: fail()
 probe_rel='artifacts/m0/decisions/boring-cdc-d-security/fixture-run.jsonl'; probe=[json.loads(x) for x in (root/probe_rel).read_text().splitlines()]
 expected_probe=[{'code':'SECURITY_EXPOSURE_FIXTURE_VALID','outcome':'pass','phase':'validate_spec'}]
 if probe!=expected_probe or spec['execution_probe']!={'expected_lines':expected_probe,'path':probe_rel,'sha256':sha(root/probe_rel)}: fail()
 decision=next(x for x in decisions['decisions'] if x['id']==decision_id)
 approval={'approved_at':'2026-09-10T14:03:30.910Z','approved_by':'Julien Hurault (repository owner), intentions d3e8abc3-d2f4-4bc0-8aec-d6ffd7bf2e36 and 5a994cfd-e4e2-46a7-b512-5dae280acae0','value_digest':hashlib.sha256(proposed.encode()).hexdigest()}
 if decision!={'approval':approval,'executor_beads':executors,'fixture_sha256':sha(root/fixture_rel),'fixture_spec':fixture_rel,'id':decision_id,'owner_bead':owner,'proposed_value':proposed,'status':'approved'}: fail()
 needed={'ART-M0-SECURITY-EXPOSURE-FIXTURE':fixture_rel,'ART-M0-SECURITY-EXPOSURE-PROBE':probe_rel,'ART-M0-SECURITY-EXPOSURE-VALIDATION':'artifacts/m0/decisions/boring-cdc-d-security/evidence.json'}
 owned={x['id']:x for x in artifacts['artifacts'] if x.get('owner_bead')==owner}
 if set(owned)!=set(needed): fail()
 for ident,path in needed.items():
  if owned[ident]!={'id':ident,'owner_bead':owner,'path':path,'sha256':sha(root/path),'status':'complete'}: fail()
 evidence=json.loads((root/needed['ART-M0-SECURITY-EXPOSURE-VALIDATION']).read_text())
 if evidence.get('schema_version')!='validation-result/v1' or evidence.get('validator_version')!='core-validators/1.0.0' or evidence.get('owner_bead')!='boring-cdc-m0.1' or evidence.get('status')!='pass' or evidence.get('findings')!=[] or evidence.get('input_sha256')!=sha(root/'contracts/m0/decisions.json') or not re.fullmatch(r'[0-9a-f]{40}',evidence.get('git_commit','')): fail()
 evidence_sha=evidence['git_commit']
 guarded=[fixture_rel,'scripts/validate/security_exposure.sh','contracts/m0/decisions.json']
 if subprocess.run(['git','cat-file','-e',evidence_sha+'^{commit}'],cwd=root,capture_output=True).returncode or subprocess.run(['git','merge-base','--is-ancestor',evidence_sha,'HEAD'],cwd=root,capture_output=True).returncode or subprocess.run(['git','diff','--quiet',evidence_sha+'..HEAD','--',*guarded],cwd=root).returncode: fail()
except (OSError,KeyError,ValueError,TypeError,StopIteration,json.JSONDecodeError): fail()
if selected_case=='all': print('{"code":"SECURITY_EXPOSURE_FIXTURE_VALID","outcome":"pass","phase":"validate_spec"}')
else: print(json.dumps({'case_id':selected_case,'code':'SECURITY_EXPOSURE_CASE_VALID','expected_outcome':vectors[selected_case]['expected_outcome'],'outcome':'pass','phase':'validate_spec'},sort_keys=True,separators=(',',':')))
PY
