#!/usr/bin/env python3
import hashlib, importlib.util, json, sqlite3, subprocess, tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]; OWNER='boring-cdc-m0-storage-model'
C=ROOT/'contracts/storage/storage-model.json'; S=ROOT/'contracts/storage/storage-model.schema.json'; Q=ROOT/'contracts/storage/sqlite-schema.sql'; F=ROOT/'fixtures/m0/storage/scenarios.json'; FS=ROOT/'contracts/storage/storage-fixtures.schema.json'; R=ROOT/'contracts/storage/storage-result.schema.json'; E=ROOT/'artifacts/boring-cdc-m0-storage-model/spec/evidence.json'; V=Path(__file__)
core_spec=importlib.util.spec_from_file_location('core_validator',ROOT/'scripts/lib/core_validator.py'); core=importlib.util.module_from_spec(core_spec); core_spec.loader.exec_module(core)
def load(p):
 def pairs(xs):
  d={}
  for k,v in xs:
   if k in d: raise ValueError(f'duplicate key {k}')
   d[k]=v
  return d
 return json.loads(p.read_text(),object_pairs_hook=pairs)
def add(fs,code,path,msg): fs.append({'code':code,'path':path,'message':msg})
def validate():
 fs=[]
 try: c,s,f,fschema,r=map(load,(C,S,F,FS,R))
 except Exception as e: return [{'code':'E_JSON','path':'inputs','message':str(e)}],{}
 sf=[]; core.validate_schema_instance(c,s,sf,base=S.parent,root=s)
 for x in sf:add(fs,'E_SCHEMA',x['pointer'],x['message'])
 sf=[]; core.validate_schema_instance(f,fschema,sf,base=FS.parent,root=fschema)
 for x in sf:add(fs,'E_FIXTURE_SCHEMA',x['pointer'],x['message'])
 markers={f'// M0-PROVISIONAL: {x}' for x in ('boring-cdc-d-sqlite','boring-cdc-d-admission','boring-cdc-d-security','boring-cdc-d-archive-durability','boring-cdc-d-values','boring-cdc-d-wal-cap')}
 if set(c['provisional_markers'])!=markers:add(fs,'E_PROVISIONAL','provisional_markers','exact provisional dependency inventory changed')
 p=c['sqlite']; expected=('3.45.3',4096,5000,0,16,5000,4096,10000)
 got=(p['version'],p['page_size_bytes'],p['connection_pragmas']['busy_timeout_ms'],p['connection_pragmas']['wal_autocheckpoint_pages'],p['connections']['max_readers'],p['connections']['max_reader_age_ms'],p['connections']['max_reader_pages'],p['actual_connection_attestation']['freshness_ms'])
 if got!=expected:add(fs,'E_SQLITE_LITERALS','sqlite','recommended SQLite literals changed')
 if p['connection_pragmas']['synchronous']!='FULL' or p['persistent_pragmas']['journal_mode']!='WAL' or p['persistent_pragmas']['auto_vacuum']!='INCREMENTAL':add(fs,'E_PRAGMA','sqlite','durability PRAGMAs weakened')
 if p['maintenance']!={'owner':'single run-owned maintenance scheduler','wal_autocheckpoint_pages':0,'checkpoint_mode':'RESTART','checkpoint_cadence_ms':1000,'checkpoint_max_wal_pages_per_attempt':4096,'checkpoint_busy_timeout_ms':50,'incremental_vacuum_max_pages':1024,'incremental_vacuum_cadence_ms':1000,'automatic_full_vacuum':'forbidden','offline_full_vacuum':'stopped and backed-up store only'}:add(fs,'E_MAINTENANCE','sqlite/maintenance','checkpoint/vacuum ownership or bound changed')
 allow=[x['type'] for x in c['filesystem']['allowlist']]
 if allow!=['ext4','xfs'] or c['filesystem']['modes']!={'roots':'0700','database_and_sidecars':'0600','spool_intents_manifests':'0600','command_socket':'0600'}:add(fs,'E_FILESYSTEM','filesystem','allowlist or strict modes changed')
 a=c['admission']; lim=a['limits']; mem=a['runtime_memory']
 calc=mem['fixed_runtime_overhead_budget']+mem['max_copyboth_receive_buffer_bytes']+mem['max_decoder_queue_bytes']+mem['max_in_memory_event_staging_bytes']+mem['max_sqlite_writer_staging_bytes']+lim['backfill_worker_count']*lim['max_backfill_worker_memory_bytes']+sum(mem['destination_workers'].values())+mem['telemetry_and_command_headroom_bytes']
 if calc!=mem['required_runtime_memory_bytes']:add(fs,'E_MEMORY_EQUATION','admission/runtime_memory',f'computed {calc}')
 for name,x in a['profiles'].items():
  vals=[x[k] for k in ('warning_free_bytes','action_free_bytes','critical_free_bytes','hard_free_bytes','reserved_free_bytes')]
  if not (vals[0]>vals[1]>vals[2]>vals[3]>=vals[4]):add(fs,'E_THRESHOLDS',f'admission/profiles/{name}','free-space order invalid')
 if lim['max_wire_frame_bytes']!=8388608 or lim['max_event_bytes']!=8388608 or lim['max_transaction_bytes']!=1073741824 or lim['max_transaction_events']!=100000:add(fs,'E_ADMISSION_LITERALS','admission/limits','wire/event/transaction limits changed')
 w=c['writer_service']
 if (w['queue_capacity'],w['blocking_worker_capacity'],w['capture_max_hold_ms'],w['noncapture_max_wait_ms'])!=(1024,4,50,250):add(fs,'E_FAIRNESS','writer_service','fair writer bounds changed')
 oc=c['ownership_commands']; ep=oc['command_endpoint']
 if (oc['probe_interval_ms'],oc['ownership_deadline_ms'],oc['takeover_wait_ms'])!=(5000,15000,90000) or (ep['request_max_bytes'],ep['response_max_bytes'],ep['read_timeout_ms'],ep['write_timeout_ms'])!=(1048576,4194304,10000,30000):add(fs,'E_OWNERSHIP','ownership_commands','ownership/endpoint bounds changed')
 cases=f['cases']; ids=[x['fixture_id'] for x in cases]
 if ids!=c['fixture_ids'] or len(ids)!=len(set(ids)):add(fs,'E_FIXTURE_INVENTORY','fixtures','ordered fixture inventory mismatch/duplicate')
 if {x['executor_id'] for x in cases}!=set(c['executors']):add(fs,'E_EXECUTORS','fixtures','executor coverage mismatch')
 required={'fixture_id','executor_id','seed','matrix','hook','inputs','expected'}; expected={'state','exit_code','feedback','checkpoint','external_effect','status_code','log_code','redacted'}
 for i,x in enumerate(cases):
  if set(x)!=required or set(x['expected'])!=expected or x['seed']!=f['fixed_seed'] or not x['hook'] or not x['inputs']['fault_once'] or x['expected']['redacted'] is not True:add(fs,'E_FIXTURE_SHAPE',f'cases/{i}','fixture is not closed and executable')
 # Actual connection-level attestation against a disposable database; never mutate the real store.
 try:
  with tempfile.TemporaryDirectory() as td:
   db=Path(td)/'journal.sqlite'; con=sqlite3.connect(db)
   con.execute('PRAGMA page_size=4096'); con.execute('PRAGMA auto_vacuum=INCREMENTAL'); con.execute('PRAGMA journal_mode=WAL'); con.execute('PRAGMA synchronous=FULL'); con.execute('PRAGMA foreign_keys=ON'); con.execute('PRAGMA trusted_schema=OFF'); con.execute('PRAGMA wal_autocheckpoint=0'); con.execute('PRAGMA busy_timeout=5000')
   con.executescript(Q.read_text())
   observed=(con.execute('PRAGMA journal_mode').fetchone()[0],con.execute('PRAGMA synchronous').fetchone()[0],con.execute('PRAGMA auto_vacuum').fetchone()[0],con.execute('PRAGMA foreign_keys').fetchone()[0],con.execute('PRAGMA trusted_schema').fetchone()[0],con.execute('PRAGMA wal_autocheckpoint').fetchone()[0])
   if observed!=('wal',2,2,1,0,0):add(fs,'E_ACTUAL_ATTESTATION','sqlite-schema.sql',repr(observed))
   tables={x[0] for x in con.execute("SELECT name FROM sqlite_schema WHERE type='table'")}; required_tables={'journal_events','source_transactions','source_state','runtime_ownership','writer_attestations','operator_command_requests','relation_schemas','destinations','destination_checkpoints','backfill_runs','backfill_generations','backfill_chunks','bootstrap_intents','bootstrap_imports','durable_capture_fences','bootstrap_anchors','reseed_intents','destination_generation_leases','destination_promotion_intents','clickhouse_batch_intents','archive_generations','archive_segment_intents','archive_segments','archive_generation_markers','processing_failures','destination_audits','logical_range_pins','condition_hysteresis','alerts','schema_migrations'}
   if not required_tables<=tables:add(fs,'E_SQL_SCHEMA','sqlite-schema.sql',','.join(sorted(required_tables-tables)))
   con.close()
 except Exception as e:add(fs,'E_SQL_EXEC','sqlite-schema.sql',str(e))
 text='\n'.join(p.read_text(errors='replace') for p in (C,Q,F,FS,R))
 for secret in ('postgres://','password=','BEGIN PRIVATE KEY','AKIA'):
  if secret in text:add(fs,'E_SECRET','inputs',f'forbidden token {secret}')
 inputs={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in (C,S,Q,F,FS,R)}
 return fs,inputs
def main():
 fs,inputs=validate(); prior=load(E) if E.exists() else {}; parent=prior.get('source_parent_git_commit') or subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(); material=''.join(k+'\0'+v+'\n' for k,v in sorted(inputs.items())).encode()
 ev={'schema_version':'m0-storage-contract-evidence/v1','owner_bead':OWNER,'status':'pass' if not fs else 'fail','validator':'scripts/validate/storage_contract.py','validator_sha256':hashlib.sha256(V.read_bytes()).hexdigest(),'source_parent_git_commit':parent,'input_tree_sha256':hashlib.sha256(material).hexdigest(),'inputs':inputs,'fixture_count':len(load(F)['cases']),'actual_connection_attestation':True,'runtime_observed':False,'product_faults':'fault_not_applicable','findings':fs}
 E.parent.mkdir(parents=True,exist_ok=True); E.write_text(json.dumps(ev,indent=2,sort_keys=True)+'\n'); print(json.dumps(ev,sort_keys=True,separators=(',',':'))); return 0 if not fs else 1
if __name__=='__main__': raise SystemExit(main())
