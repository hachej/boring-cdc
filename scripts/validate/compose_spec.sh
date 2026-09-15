#!/bin/sh
set -eu
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CASE_ID=${1:-all}
exec python3 - "$ROOT" "$CASE_ID" <<'PY'
import hashlib,json,re,subprocess,sys
from pathlib import Path
root=Path(sys.argv[1]); selected=sys.argv[2]
owner='boring-cdc-d-compose'; decision_id='DEC-COMPOSE-IMAGES-RUNTIME-RECOVERY'
contract_rel='contracts/m0/compose.json'; fixture_rel='fixtures/m0/decisions/boring-cdc-d-compose.json'
contract_sha='8a78b0a63d36a039eabae79768a35d66707f46d8c4c635508059d1a065cf8017'; result_schema_rel='contracts/m0/compose-execution-result.schema.json'; result_schema_sha='1bf2a5ce493239bbd173d3b23c36d1c7aa15703e044a5c45a64687f0f8ae1291'
executors=['boring-cdc-m0-scaffold','boring-cdc-m6-failure-matrix','boring-cdc-m7-artifacts']
proposed='linux/amd64 only; PostgreSQL 17.6, ClickHouse 25.8.2.29, Rust 1.89.0 Bookworm builder, and Debian bookworm-20250811-slim runtime are pinned by accepted OCI index and amd64 digests; Docker Engine 28.3.3, Compose 2.39.2, BuildKit 0.24.0, and Dockerfile frontend 1.12.0; cargo build --locked --release; project boring-cdc with postgres, clickhouse, connector and pgdata, chdata, cdcdata; connector restart unless-stopped with unlimited supervisor attempts while the approved persisted FailurePolicy remains retry authority; PostgreSQL keepalive 30/10/3 and client check 10 seconds, measured reap <=70 seconds, takeover at 90 seconds; health 5/3/12 with 30-second start period and 120-second readiness; patch/minor changes rerun clean-pull with rollback-compatible state, major PostgreSQL/ClickHouse changes require migration plan and reseed.'
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def canonical_digest(obj): return hashlib.sha256(json.dumps(obj,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def fail():
 print('{"code":"COMPOSE_FIXTURE_INVALID","outcome":"fail","phase":"validate_spec"}'); raise SystemExit(1)
def ensure(cond):
 if not cond: fail()
try:
 contract=json.loads((root/contract_rel).read_text()); spec=json.loads((root/fixture_rel).read_text())
 ensure(sha(root/contract_rel)==contract_sha and sha(root/result_schema_rel)==result_schema_sha)
 decisions=json.loads((root/'contracts/m0/decisions.json').read_text()); artifacts=json.loads((root/'contracts/m0/artifacts.json').read_text())
 registry=json.loads((root/'contracts/agent/stable-ids.json').read_text()); coverage=json.loads((root/'contracts/coverage/plan-to-beads.json').read_text())
 graph=[json.loads(x) for x in (root/'.beads/issues.jsonl').read_text().splitlines() if x.strip()]
 approval={'approved_at':'2026-09-10T14:03:30.949Z','approved_by':'Julien Hurault (repository owner)','intention_id':'765bd3b2-4b68-4102-a9ec-43ca93357390','selection':'Accept recommended defaults'}
 ensure(contract.get('schema_version')=='compose-reproducibility-contract/v1' and contract.get('decision_id')==decision_id and contract.get('owner_bead')==owner and contract.get('approval')==approval)
 ensure(spec.get('schema_version')=='m0-decision-fixture/v1' and spec.get('fixture_id')==decision_id and spec.get('decision_id')==decision_id and spec.get('owner_bead')==owner and spec.get('approval')==approval)
 required=('inputs','preconditions','supported_matrix','deterministic_phase','expected','expected_failure','result_contract','redaction_assertions','later_executors','vectors','approved_boundary')
 ensure(all(spec.get(x) for x in required) and spec['approved_boundary']==contract and spec['later_executors']==executors)
 ensure(contract.get('architecture_allowlist')==['linux/amd64'])
 expected_images={
  'postgres':('docker.io/library/postgres','17.6','00bc86618629af00d2937fdc5a5d63db3ff8450acf52f0636ec813c7f4902929','b86568d3e0fe1dfaeff52714f9da36f206a30e4c49131b82bf96982d78627409'),
  'clickhouse':('docker.io/clickhouse/clickhouse-server','25.8.2.29','74c213b4d4cb4854c2497694df0c2d153c041003eadbb0457ae62c28cb8d723f','78b6f0863688458b229b597f6a1bbf891855a01cf59c3f3dbe66428571c518c9'),
  'builder':('docker.io/library/rust','1.89.0-bookworm','948f9b08a66e7fe01b03a98ef1c7568292e07ec2e4fe90d88c07bb14563c84ff','c9ac3fa8945b61dede1e4500d25028aa8fd8a8fe46365fcf9c0422f8d999b9b0'),
  'runtime':('docker.io/library/debian','bookworm-20250811-slim','b1a741487078b369e78119849663d7f1a5341ef2768798f7b7406c4240f86aef','cea2634840f5a87503d8210e4df97b9f23a2acd67ff860a76c133d963032f866')}
 ensure(set(contract.get('images',{}))==set(expected_images))
 for key,(repo,tag,index,platform) in expected_images.items():
  ensure(contract['images'][key]=={'architecture':'linux/amd64','index_digest':'sha256:'+index,'platform_digest':'sha256:'+platform,'repository':repo,'tag':tag})
 tool=contract.get('toolchain',{})
 ensure({k:tool.get(k) for k in ('docker_engine','docker_compose_plugin','buildkit','dockerfile_frontend','rust')}=={'docker_engine':'28.3.3','docker_compose_plugin':'2.39.2','buildkit':'0.24.0','dockerfile_frontend':'1.12.0','rust':'1.89.0'})
 recipe=contract['build_recipe']; ensure(contract.get('build_recipe_sha256')==canonical_digest(recipe))
 ensure(recipe.get('architecture')=='linux/amd64' and recipe.get('cargo_command')==['cargo','build','--locked','--release'])
 ensure(recipe.get('builder_image')=='docker.io/library/rust:1.89.0-bookworm@sha256:'+expected_images['builder'][3] and recipe.get('runtime_image')=='docker.io/library/debian:bookworm-20250811-slim@sha256:'+expected_images['runtime'][3])
 ensure(recipe.get('connector_output_digest')=={'m0_requirement':False,'recorded_by':'boring-cdc-m7-artifacts'} and recipe.get('dockerfile_syntax_directive')=='# syntax=docker/dockerfile:1.12.0')
 ensure(recipe.get('exact_build_command')==['docker','build','--platform','linux/amd64','--pull','--no-cache','--file','Dockerfile','--tag','boring-cdc/connector:local','.'] and recipe.get('build_output_boundary').startswith('The local connector image ID'))
 comp=contract['compose']; ensure(comp.get('canonical_path')=='compose.yaml' and comp.get('project_name')=='boring-cdc' and comp.get('services')==['postgres','clickhouse','connector'] and comp.get('named_volumes')==['pgdata','chdata','cdcdata'])
 restart=comp['restart']; failure_path=root/restart['retry_authority']
 ensure(restart=={'connector':'unless-stopped','process_restart_must_not_reset_persisted_schedule':True,'retry_authority':'contracts/m0/failure-policy.json','retry_authority_sha256':'d0e3e956bc79ddcde4fc77bb8804100a7d56cda6778059028d3b940f98fce831','supervisor_attempts':'unlimited'})
 ensure(sha(failure_path)==restart['retry_authority_sha256'])
 failure=json.loads(failure_path.read_text()); ensure(failure['restart_and_clock']['restart'].startswith('reload persisted attempt') and failure['schedule']['base_ms']==250 and failure['schedule']['cap_ms']==30000 and failure['schedule']['maximum_attempts']==10)
 ownership=contract['ownership']; ensure(ownership['postgres_settings']=={'client_connection_check_interval_seconds':10,'tcp_keepalives_count':3,'tcp_keepalives_idle_seconds':30,'tcp_keepalives_interval_seconds':10})
 ensure(ownership['measured_zombie_reap_bound_seconds_lte']==70 and ownership['takeover_wait_seconds']==90 and 70 < 90 and ownership['equality_policy'].startswith('a measured reap of exactly 70 seconds passes'))
 health=contract['health']; ensure({k:health[k] for k in ('interval_seconds','timeout_seconds','retries','start_period_seconds','readiness_timeout_seconds')}=={'interval_seconds':5,'timeout_seconds':3,'retries':12,'start_period_seconds':30,'readiness_timeout_seconds':120})
 ensure(health['checks']=={'clickhouse':['clickhouse-client','--query','SELECT 1'],'connector':['boring-cdc','check'],'postgres':['pg_isready']} and health['failure_exit_codes']=={'configuration':78,'temporary_dependency':75})
 clean=contract['clean_pull_execution']; ensure(clean['environment']=={'allowlist':{'DOCKER_CONFIG':'<isolated-empty-directory>','HOME':'<isolated-empty-directory>','LC_ALL':'C','PATH':'/usr/bin:/bin','SOURCE_DATE_EPOCH':'<git-commit-author-timestamp>','TZ':'UTC'},'clear_ambient_environment':True,'forbidden':['COMPOSE_FILE','COMPOSE_PROFILES','DOCKER_CONTEXT','DOCKER_DEFAULT_PLATFORM','DOCKER_HOST','HTTP_PROXY','HTTPS_PROXY','NO_PROXY','RUSTFLAGS','CARGO_HOME','RUSTUP_HOME']} and len(clean['commands'])==10 and clean['commands'][-1]==['docker','compose','--project-name','boring-cdc','--file','compose.yaml','up','--detach','--wait','--wait-timeout','120'] and clean['cleanup_command']==['docker','compose','--project-name','boring-cdc','--file','compose.yaml','down','--volumes','--remove-orphans'])
 ensure(contract['result_schema']=={'path':result_schema_rel,'schema_id':'https://boring-cdc.dev/contracts/m0/compose-execution-result.schema.json'})
 ensure(contract['faults']=={'hook_policy':'later executors inject only at named fixture phases; M0 records no runtime result','seed':'0x424344435f434f4d504f53455f563031','vectors':['partition','restart','zombie_connection','stale_completion','dependency_delay','digest_mismatch']})
 ensure(contract['upgrade_policy']['rolling_or_ha_claims'] is False and 're-seed' in contract['upgrade_policy']['postgres_clickhouse_major'] and spec['fixed_seed']==contract['faults']['seed'])
 required_fields={'tag','index_digest','platform_digest','architecture','tool_versions','configuration_sha256','cargo_lock_sha256','dockerfile_sha256','build_recipe_sha256','start_times','readiness_times','health_outputs','exit_codes'}
 ensure(set(contract['clean_pull_evidence']['required_fields'])==required_fields and contract['clean_pull_evidence']['result_schema']=='compose-execution-result/v1')
 vectors=spec['vectors']; expected_cases={'clean_pull','partition','restart','zombie_connection','stale_completion','dependency_delay','digest_mismatch'}
 ensure(set(vectors)==expected_cases and {x['case_id'] for x in spec['supported_matrix']}==expected_cases and len(spec['supported_matrix'])==7)
 expected_map={'clean_pull':('boring-cdc-m0-scaffold','ready',0,'BCDC_COMPOSE_READY'),'partition':('boring-cdc-m6-failure-matrix','retry_wait',75,'BCDC_COMPOSE_PARTITION_RETRY_WAIT'),'restart':('boring-cdc-m6-failure-matrix','retry_wait',75,'BCDC_COMPOSE_RESTART_PRESERVED_RETRY'),'zombie_connection':('boring-cdc-m6-failure-matrix','takeover_reconciles',0,'BCDC_COMPOSE_ZOMBIE_REAP_CONFIRMED'),'stale_completion':('boring-cdc-m6-failure-matrix','stale_completion_rejected',75,'BCDC_COMPOSE_STALE_COMPLETION_REJECTED'),'dependency_delay':('boring-cdc-m6-failure-matrix','temporary_dependency',75,'BCDC_COMPOSE_DEPENDENCY_TIMEOUT'),'digest_mismatch':('boring-cdc-m0-scaffold','configuration_error',78,'BCDC_COMPOSE_DIGEST_MISMATCH')}
 for cid,v in vectors.items():
  executor,state,exit_code,code=expected_map[cid]; e=v['expected']
  ensure(v['case_id']==cid and v['executor_bead']==executor and v['expected_outcome']==state and v.get('preconditions') and v.get('fault_hook'))
  ensure(e['state']==state and e['exit_code']==exit_code and e['checkpoint']=='unchanged' and e['feedback']=='unchanged' and e['stable_log_codes']==[code] and e.get('external_effects') is not None and e.get('metrics') is not None)
 ensure(vectors['dependency_delay']['inputs']=={'deadline_seconds':120,'failure_class':'transient_source','ready_at_seconds':121} and vectors['dependency_delay']['inputs']['failure_class'] in failure['failure_classes'])
 ensure(vectors['digest_mismatch']['inputs']['observed_platform_digest']=='sha256:'+'0'*64)
 ensure(vectors['zombie_connection']['inputs']=={'reap_bound_seconds':70,'takeover_wait_seconds':90})
 ensure(spec['result_contract']['schema_path']==result_schema_rel and spec['result_contract']['positive_fixture']=='fixtures/m0/compose-execution-result/valid.json' and spec['result_contract']['hostile_fixtures']==['fixtures/m0/compose-execution-result/reject-unknown-field.json'])
 valid=subprocess.run(['python3',str(root/'scripts/lib/core_validator.py'),'schema',str(root/spec['result_contract']['positive_fixture']),'--schema',str(root/result_schema_rel)],cwd=root,capture_output=True)
 hostile=subprocess.run(['python3',str(root/'scripts/lib/core_validator.py'),'schema',str(root/spec['result_contract']['hostile_fixtures'][0]),'--schema',str(root/result_schema_rel)],cwd=root,capture_output=True)
 ensure(valid.returncode==0 and hostile.returncode!=0 and b'E_SCHEMA' in hostile.stdout)
 ensure(spec['script']['path']=='scripts/validate/compose_spec.sh' and sha(root/spec['script']['path'])==spec['script']['sha256'])
 graph_ids={x['id'] for x in graph}; ensure(not(set(executors)-graph_ids))
 stable=next(x for x in registry['entries'] if x['id']==decision_id); covered=next(x for x in coverage['assignments'] if x['id']==decision_id)
 ensure(stable['owner_bead']==owner and covered=={'evidence_status':'pending','id':decision_id,'owner_bead':owner,'source':'docs/PLAN.md','source_digest':stable['source_digest']})
 ensure(subprocess.run([str(root/'scripts/validate/plan_coverage.sh')],cwd=root,capture_output=True).returncode==0)
 probe_rel='artifacts/m0/decisions/boring-cdc-d-compose/fixture-run.jsonl'; expected_probe=[{'code':'COMPOSE_FIXTURE_VALID','outcome':'pass','phase':'validate_spec','vector_count':7}]
 probe=[json.loads(x) for x in (root/probe_rel).read_text().splitlines() if x.strip()]
 ensure(probe==expected_probe and spec['execution_probe']=={'expected_lines':expected_probe,'path':probe_rel,'sha256':sha(root/probe_rel)})
 decision=next(x for x in decisions['decisions'] if x['id']==decision_id)
 decision_approval={'approved_at':'2026-09-10T14:03:30.949Z','approved_by':'Julien Hurault (repository owner), intention 765bd3b2-4b68-4102-a9ec-43ca93357390','value_digest':hashlib.sha256(proposed.encode()).hexdigest()}
 ensure(decision=={'approval':decision_approval,'executor_beads':executors,'fixture_sha256':sha(root/fixture_rel),'fixture_spec':fixture_rel,'id':decision_id,'owner_bead':owner,'proposed_value':proposed,'status':'approved'})
 needed={'ART-M0-COMPOSE-CONTRACT':contract_rel,'ART-M0-COMPOSE-FIXTURE':fixture_rel,'ART-M0-COMPOSE-PROBE':probe_rel,'ART-M0-COMPOSE-RESULT-SCHEMA':result_schema_rel,'ART-M0-COMPOSE-RESULT-VALID':'fixtures/m0/compose-execution-result/valid.json','ART-M0-COMPOSE-RESULT-REJECT-UNKNOWN':'fixtures/m0/compose-execution-result/reject-unknown-field.json','ART-M0-COMPOSE-VALIDATION':'artifacts/m0/decisions/boring-cdc-d-compose/evidence.json'}
 owned={x['id']:x for x in artifacts['artifacts'] if x.get('owner_bead')==owner}; ensure(set(owned)==set(needed))
 for ident,path in needed.items(): ensure(owned[ident]=={'id':ident,'owner_bead':owner,'path':path,'sha256':sha(root/path),'status':'complete'})
 evidence=json.loads((root/needed['ART-M0-COMPOSE-VALIDATION']).read_text())
 ensure(evidence.get('schema_version')=='validation-result/v1' and evidence.get('validator_version')=='core-validators/1.0.0' and evidence.get('owner_bead')=='boring-cdc-m0.1' and evidence.get('status')=='pass' and evidence.get('findings')==[] and evidence.get('input_sha256')==sha(root/'contracts/m0/decisions.json') and re.fullmatch(r'[0-9a-f]{40}',evidence.get('git_commit','')))
 anchor=evidence['git_commit']; guarded=[contract_rel,result_schema_rel,fixture_rel,'fixtures/m0/compose-execution-result/valid.json','fixtures/m0/compose-execution-result/reject-unknown-field.json','scripts/validate/compose_spec.sh','contracts/m0/decisions.json']
 ensure(subprocess.run(['git','cat-file','-e',anchor+'^{commit}'],cwd=root,capture_output=True).returncode==0 and subprocess.run(['git','merge-base','--is-ancestor',anchor,'HEAD'],cwd=root,capture_output=True).returncode==0 and subprocess.run(['git','diff','--quiet',anchor+'..HEAD','--',*guarded],cwd=root).returncode==0)
except (OSError,KeyError,ValueError,TypeError,StopIteration,json.JSONDecodeError): fail()
if selected=='all': print(json.dumps({'code':'COMPOSE_FIXTURE_VALID','outcome':'pass','phase':'validate_spec','vector_count':len(vectors)},sort_keys=True,separators=(',',':')))
elif selected in vectors: print(json.dumps({'case_id':selected,'code':'COMPOSE_CASE_VALID','expected_outcome':vectors[selected]['expected_outcome'],'outcome':'pass','phase':'validate_spec'},sort_keys=True,separators=(',',':')))
else: fail()
PY
