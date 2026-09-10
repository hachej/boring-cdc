#!/bin/sh
set -eu
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
CASE_ID=${1:-all}
exec python3 - "$ROOT" "$CASE_ID" <<'PY'
import hashlib,json,re,subprocess,sys
from pathlib import Path
root=Path(sys.argv[1]); selected=sys.argv[2]
owner='boring-cdc-d-sqlite'; decision_id='DEC-SQLITE-PHYSICAL-JOURNAL-DURABILITY'
fixture_rel='fixtures/m0/decisions/boring-cdc-d-sqlite.json'
executors=['boring-cdc-m1-preflight','boring-cdc-m2-schema','boring-cdc-m2-journal','boring-cdc-m2-pressure','boring-cdc-m2-reconcile']
proposed='SQLite 3.45.3; one physical database with logically append-only journal_events; WAL; synchronous=FULL on every admitted durability connection; journal_size_limit=256 MiB; page_size=4096; temp_store=FILE; foreign_keys=ON; secure_delete=FAST; pre-schema auto_vacuum=INCREMENTAL; wal_autocheckpoint=0; busy_timeout=5000 ms; mmap_size=0; at most 16 readers; local ext4/XFS non-network block devices only; PASSIVE checkpoint at 1,000 WAL pages or 30 seconds, RESTART only when pins permit, TRUNCATE maintenance-only; incremental vacuum at most 1,000 pages per cycle; attestation valid 24 hours and invalid on mount/device change; FULL sync of database/WAL and required directory lifecycle; quick_check at startup and daily, integrity_check maintenance-only; SHA-256 exported evidence/backups; no automatic full VACUUM.'
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def fail():
 print('{"code":"SQLITE_DURABILITY_FIXTURE_INVALID","outcome":"fail","phase":"validate_spec"}'); raise SystemExit(1)
try:
 spec=json.loads((root/fixture_rel).read_text()); decisions=json.loads((root/'contracts/m0/decisions.json').read_text())
 artifacts=json.loads((root/'contracts/m0/artifacts.json').read_text()); registry=json.loads((root/'contracts/agent/stable-ids.json').read_text())
 coverage=json.loads((root/'contracts/coverage/plan-to-beads.json').read_text()); graph=[json.loads(x) for x in (root/'.beads/issues.jsonl').read_text().splitlines()]
 required=('inputs','preconditions','supported_matrix','deterministic_phase','expected','expected_failure','result_contract','redaction_assertions','later_executors','vectors')
 if any(not spec.get(x) for x in required): fail()
 if spec.get('schema_version')!='m0-decision-fixture/v1' or spec.get('fixture_id')!=decision_id or spec.get('decision_id')!=decision_id or spec.get('owner_bead')!=owner or spec.get('later_executors')!=executors: fail()
 if spec.get('approval')!={'approved_at':'2026-09-10T14:03:30.949Z','approved_by':'Julien Hurault (repository owner)','intention_id':'765bd3b2-4b68-4102-a9ec-43ca93357390','selection':'Accept recommended defaults'}: fail()
 expected_boundary={'sqlite_version':'3.45.3','database_count':1,'payload_journal':'journal_events_logically_append_only','pragmas':{'journal_mode':'WAL','synchronous':'FULL','journal_size_limit_bytes':268435456,'page_size_bytes':4096,'temp_store':'FILE','foreign_keys':'ON','secure_delete':'FAST','auto_vacuum':'INCREMENTAL','wal_autocheckpoint_pages':0,'busy_timeout_ms':5000,'mmap_size_bytes':0},'max_readers':16,'filesystems':{'allow':['ext4','xfs'],'device':'local_non_network_block_device','reject':['nfs','smb_cifs','fuse','tmpfs','overlayfs','remote_volume']},'checkpoint':{'passive_wal_pages':1000,'passive_interval_seconds':30,'restart':'only_when_no_pin_blocks_recycling','truncate':'maintenance_only'},'incremental_vacuum_pages_per_cycle_max':1000,'automatic_full_vacuum':'prohibited','attestation':{'validity_seconds':86400,'invalidate_on':['mount_change','device_change']},'integrity':{'quick_check':['startup','daily'],'integrity_check':'maintenance_only','exported_evidence_backup_digest':'SHA-256'},'permissions':{'directory_mode':'0700','database_wal_and_related_file_mode':'0600_or_stricter'},'crash_claim':{'guarantees':['process_crash','container_crash','abrupt_host_restart_on_honest_tested_storage_stack'],'does_not_guarantee':['media_or_controller_failure','filesystem_corruption','hardware_lying_about_flushes']}}
 if spec.get('approved_boundary')!=expected_boundary: fail()
 if spec.get('connection_admission')!={'actual_writer_must_read_back':{'journal_mode':'WAL','synchronous':'FULL'},'observer_readback_cannot_certify_writer':True,'persistent_settings':['journal_mode','auto_vacuum','page_size'],'connection_local_settings':['synchronous','temp_store','foreign_keys','secure_delete','busy_timeout','mmap_size'],'reopened_and_maintenance_writers_rechecked':True}: fail()
 if spec.get('directory_sync')!={'create':['sync_database_file','sync_wal_file_when_present','sync_parent_directory'],'replace_or_rename':['sync_replacement_file','sync_source_parent_before_rename','atomic_rename','sync_destination_parent_after_rename'],'delete':['sync_parent_directory_after_delete'],'publication':['sync_file_and_parent_before_rename','atomic_rename','sync_parent_after_rename']}: fail()
 cases={x['case_id']:x for x in spec['supported_matrix']}; vectors=spec['vectors']
 required_cases={'ext4_admitted','xfs_admitted','unsupported_filesystem','writer_off_observer_full','reopened_writer_full','stale_attestation','mount_or_device_change','reader_limit','passive_page_threshold','passive_time_threshold','wal_busy_reader_pin','restart_pin_permitted','truncate_runtime_forbidden','incremental_vacuum_bound','checksum_mismatch','abrupt_host_ext4','abrupt_host_xfs','directory_publication_sync'}
 if set(cases)!=required_cases or len(cases)!=len(spec['supported_matrix']) or set(vectors)!=required_cases: fail()
 outcomes={k:v['expected_outcome'] for k,v in cases.items()}
 expected_outcomes={'ext4_admitted':'pass','xfs_admitted':'pass','unsupported_filesystem':'blocked_unsupported_storage','writer_off_observer_full':'blocked_writer_pragma_mismatch','reopened_writer_full':'pass','stale_attestation':'blocked_stale_attestation','mount_or_device_change':'blocked_attestation_invalidated','reader_limit':'blocked_reader_limit','passive_page_threshold':'pass_checkpoint_requested','passive_time_threshold':'pass_checkpoint_requested','wal_busy_reader_pin':'busy_no_recycling','restart_pin_permitted':'pass_restart_checkpoint','truncate_runtime_forbidden':'blocked_maintenance_only','incremental_vacuum_bound':'pass_bounded_work','checksum_mismatch':'blocked_integrity_failure','abrupt_host_ext4':'pass_recovery_required','abrupt_host_xfs':'pass_recovery_required','directory_publication_sync':'pass'}
 if outcomes!=expected_outcomes: fail()
 for case_id,outcome in expected_outcomes.items():
  v=vectors[case_id]
  if v.get('case_id')!=case_id or v.get('expected_outcome')!=outcome or not v.get('inputs') or not v.get('preconditions') or not v.get('fault_hook') or not v.get('expected') or not v.get('executor_bead'): fail()
  if v['executor_bead'] not in executors or v['expected'].get('feedback') is None or v['expected'].get('checkpoint') is None or not v['expected'].get('stable_log_codes'): fail()
 if vectors['writer_off_observer_full']['inputs']!={'actual_writer_synchronous':'OFF','observer_synchronous':'FULL'} or vectors['writer_off_observer_full']['expected']['feedback']!='none': fail()
 if vectors['stale_attestation']['inputs'].get('age_seconds')!=86401 or vectors['reader_limit']['inputs']!={'active_readers':17,'max_readers':16}: fail()
 if vectors['passive_page_threshold']['inputs'].get('wal_pages')!=1000 or vectors['passive_time_threshold']['inputs'].get('elapsed_seconds')!=30: fail()
 if vectors['incremental_vacuum_bound']['inputs'].get('requested_pages')!=1001 or vectors['incremental_vacuum_bound']['expected'].get('pages_processed')!=1000: fail()
 if vectors['wal_busy_reader_pin']['expected'].get('checkpoint')!='unchanged_busy' or vectors['restart_pin_permitted']['preconditions']!=['all logical range pins permit WAL recycling']: fail()
 if vectors['abrupt_host_ext4']['expected'].get('claim')!='recovery only after successful FULL and required directory syncs on honest ext4' or vectors['abrupt_host_xfs']['expected'].get('claim')!='recovery only after successful FULL and required directory syncs on honest xfs': fail()
 if spec['script']['path']!='scripts/validate/sqlite_durability.sh' or sha(root/spec['script']['path'])!=spec['script']['sha256']: fail()
 graph_ids={x['id'] for x in graph}
 if set(executors)-graph_ids: fail()
 stable=next(x for x in registry['entries'] if x['id']==decision_id); covered=next(x for x in coverage['assignments'] if x['id']==decision_id)
 if stable['owner_bead']!=owner or covered!={'evidence_status':'pending','id':decision_id,'owner_bead':owner,'source':'docs/PLAN.md','source_digest':stable['source_digest']}: fail()
 if subprocess.run([str(root/'scripts/validate/plan_coverage.sh')],cwd=root,capture_output=True).returncode: fail()
 probe_rel='artifacts/m0/decisions/boring-cdc-d-sqlite/fixture-run.jsonl'; probe=[json.loads(x) for x in (root/probe_rel).read_text().splitlines()]
 expected_probe=[{'code':'SQLITE_DURABILITY_FIXTURE_VALID','outcome':'pass','phase':'validate_spec','vector_count':18}]
 if probe!=expected_probe or spec['execution_probe']!={'expected_lines':expected_probe,'path':probe_rel,'sha256':sha(root/probe_rel)}: fail()
 decision=next(x for x in decisions['decisions'] if x['id']==decision_id)
 approval={'approved_at':'2026-09-10T14:03:30.949Z','approved_by':'Julien Hurault (repository owner), intention 765bd3b2-4b68-4102-a9ec-43ca93357390','value_digest':hashlib.sha256(proposed.encode()).hexdigest()}
 if decision!={'approval':approval,'executor_beads':executors,'fixture_sha256':sha(root/fixture_rel),'fixture_spec':fixture_rel,'id':decision_id,'owner_bead':owner,'proposed_value':proposed,'status':'approved'}: fail()
 needed={'ART-M0-SQLITE-DURABILITY-FIXTURE':fixture_rel,'ART-M0-SQLITE-DURABILITY-PROBE':probe_rel,'ART-M0-SQLITE-DURABILITY-VALIDATION':'artifacts/m0/decisions/boring-cdc-d-sqlite/evidence.json'}
 owned={x['id']:x for x in artifacts['artifacts'] if x.get('owner_bead')==owner}
 if set(owned)!=set(needed): fail()
 for ident,path in needed.items():
  if owned[ident]!={'id':ident,'owner_bead':owner,'path':path,'sha256':sha(root/path),'status':'complete'}: fail()
 evidence=json.loads((root/needed['ART-M0-SQLITE-DURABILITY-VALIDATION']).read_text())
 if evidence.get('schema_version')!='validation-result/v1' or evidence.get('validator_version')!='core-validators/1.0.0' or evidence.get('owner_bead')!='boring-cdc-m0.1' or evidence.get('status')!='pass' or evidence.get('findings')!=[] or evidence.get('input_sha256')!=sha(root/'contracts/m0/decisions.json') or not re.fullmatch(r'[0-9a-f]{40}',evidence.get('git_commit','')): fail()
 anchor=evidence['git_commit']; guarded=[fixture_rel,'scripts/validate/sqlite_durability.sh','contracts/m0/decisions.json']
 if subprocess.run(['git','cat-file','-e',anchor+'^{commit}'],cwd=root,capture_output=True).returncode or subprocess.run(['git','merge-base','--is-ancestor',anchor,'HEAD'],cwd=root,capture_output=True).returncode or subprocess.run(['git','diff','--quiet',anchor+'..HEAD','--',*guarded],cwd=root).returncode: fail()
except (OSError,KeyError,ValueError,TypeError,StopIteration,json.JSONDecodeError): fail()
if selected=='all': print('{"code":"SQLITE_DURABILITY_FIXTURE_VALID","outcome":"pass","phase":"validate_spec"}')
elif selected in vectors: print(json.dumps({'case_id':selected,'code':'SQLITE_DURABILITY_CASE_VALID','expected_outcome':vectors[selected]['expected_outcome'],'outcome':'pass','phase':'validate_spec'},sort_keys=True,separators=(',',':')))
else: fail()
PY
