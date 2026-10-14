#!/usr/bin/env python3
import hashlib, importlib.util, json, re, subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]; OWNER='boring-cdc-m0-ch-model'
C=ROOT/'contracts/clickhouse/model.json'; S=ROOT/'contracts/clickhouse/model.schema.json'; F=ROOT/'fixtures/m0/clickhouse/scenarios.json'; FS=ROOT/'contracts/clickhouse/fixtures.schema.json'; RS=ROOT/'contracts/clickhouse/result.schema.json'
DDL=ROOT/'contracts/clickhouse/ddl.sql'; Q=ROOT/'contracts/clickhouse/canonical-query.sql'; R=ROOT/'contracts/clickhouse/retire-generation.sql'; D=ROOT/'docs/CLICKHOUSE_MODEL.md'; V=Path(__file__); E=ROOT/'artifacts/boring-cdc-m0-ch-model/spec/evidence.json'; M=ROOT/'contracts/m0/manifest.json'; A=ROOT/'contracts/m0/artifacts.json'
spec=importlib.util.spec_from_file_location('core',ROOT/'scripts/lib/core_validator.py'); core=importlib.util.module_from_spec(spec); spec.loader.exec_module(core)
def load(p):
 def pairs(xs):
  d={}
  for k,v in xs:
   if k in d: raise ValueError('duplicate key '+k)
   d[k]=v
  return d
 return json.loads(p.read_text(),object_pairs_hook=pairs)
def digest(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def add(o,c,p,m): o.append({'code':c,'path':p,'message':m})
SELECTOR_FIELDS=('capture_epoch','promotion_fence','generation','table_set_fingerprint','compatible_anchor_id','candidate_digest')
def selector_fingerprint(rows,epoch):
 selected=[row for row in rows if row['capture_epoch']==epoch]
 if not selected:return None
 by_fence={}
 for row in selected:
  by_fence.setdefault(row['promotion_fence'],set()).add(tuple(row[field] for field in SELECTOR_FIELDS))
 if any(len(candidates)!=1 for candidates in by_fence.values()):raise ValueError('selector conflict')
 greatest=next(iter(by_fence[max(by_fence)]))
 h=hashlib.sha256()
 for value in ('boring-cdc/clickhouse-selector/v1',*greatest):
  encoded=str(value).encode('ascii');h.update(len(encoded).to_bytes(8,'big'));h.update(encoded)
 return h.hexdigest()
def resolve_pointer(document,pointer):
 current=document
 for token in pointer.removeprefix('/').split('/'):
  token=token.replace('~1','/').replace('~0','~')
  current=current[int(token)] if isinstance(current,list) else current[token]
 return current
def simulate(events):
 byid={}; bykey={}
 for e in events:
  old=byid.get(e['id'])
  if old and old!=e['hash']: raise ValueError('event conflict')
  if old: continue
  byid[e['id']]=e['hash']; bykey.setdefault(e['key'],[]).append(e)
 rows=[]
 for key,es in bykey.items():
  es.sort(key=lambda e:tuple(e['version'])+(e['id'],)); latest=es[-1]
  if latest['op']=='delete' or latest['mutation_kind']=='delete': continue
  cells={}
  for e in es:
   for cid,state,type_oid,typmod,value in e['cells']:
    if state in ('explicit_value','explicit_null'): cells[cid]=(state,type_oid,typmod,value)
    elif state=='absent_for_schema': cells[cid]=('explicit_null',type_oid,typmod,'')
    elif state=='unchanged_toast' and cid not in cells: raise ValueError('missing predecessor')
  rows.append({'canonical_key':key,'columns':[{'column_id':k,'state':v[0],'type_oid':v[1],'typmod':v[2],'value_base64':v[3]} for k,v in sorted(cells.items())]})
 return sorted(rows,key=lambda x:x['canonical_key'])
def validate_marker_oracles(case,index,out):
 setup=case.get('execution',{}).get('setup',{}); rows=setup.get('history_events',[]); markers=setup.get('batch_markers',[])
 batches={}
 for row in rows:
  key=(row.get('capture_epoch'),row.get('generation'),row.get('batch_id'))
  batches.setdefault(key,[]).append(row)
 valid=[]
 for j,marker in enumerate(markers):
  path=f'cases/{index}/execution/setup/batch_markers/{j}'
  key=(marker.get('capture_epoch'),marker.get('generation'),marker.get('batch_id')); represented=batches.get(key,[])
  identity_hashes={}
  for row in represented: identity_hashes.setdefault(row.get('id'),set()).add(row.get('hash'))
  if not represented or any(len(hashes)!=1 for hashes in identity_hashes.values()) or marker.get('first_journal_seq')!=min(row.get('journal_seq') for row in represented) or marker.get('last_journal_seq')!=max(row.get('journal_seq') for row in represented) or marker.get('event_count')!=len(identity_hashes):
   add(out,'E_BATCH_MARKER_BOUNDARY',path,'marker range/count must equal its represented history batch and logical event identities')
  else: valid.append(marker)
 checkpoint=case.get('execution',{}).get('oracle',{}).get('checkpoint','')
 match=re.fullmatch(r'advance_to_(\d+)',checkpoint)
 adoption=case.get('action',{}).get('fault_hook') in {'replay_identical','after_marker_before_checkpoint'}
 if adoption and not match:
  add(out,'E_CHECKPOINT_ORACLE',f'cases/{index}/execution/oracle/checkpoint','valid-marker adoption must advance to its finalized range')
 if match:
  previous=case.get('pre_state',{}).get('checkpoint_journal_seq'); endpoint=previous
  for marker in sorted(valid,key=lambda item:item['first_journal_seq']):
   if marker['first_journal_seq']==endpoint+1: endpoint=marker['last_journal_seq']
  target=case.get('pre_state',{}).get('target_journal_seq')
  if previous is None or endpoint==previous or endpoint!=target or int(match.group(1))!=endpoint:
   add(out,'E_CHECKPOINT_ORACLE',f'cases/{index}/execution/oracle/checkpoint','advanced checkpoint must equal the target at the end of the contiguous valid finalized marker range')
def validate():
 out=[]
 try: c,s,f,fs,rs=map(load,(C,S,F,FS,RS))
 except Exception as e:return [{'code':'E_JSON','path':'inputs','message':str(e)}],{}
 for inst,sch,code in ((c,s,'E_SCHEMA'),(f,fs,'E_FIXTURE_SCHEMA')):
  failures=[]; core.validate_schema_instance(inst,sch,failures,base=S.parent,root=sch)
  for x in failures:add(out,code,x['pointer'],x['message'])
 paths=[C,S,F,FS,RS,DDL,Q,R,D,V]
 text='\n'.join(p.read_text(errors='replace') for p in paths)
 if c['objects']['materialized_views']!=[] or not c['objects']['materialized_view_policy'].startswith('none:'):add(out,'E_MATERIALIZED_VIEW','objects','materialized-view policy changed')
 if c['pins']['client_protocol']!='HTTP RowBinary' or c['pins']['rust_client_crate']!='clickhouse 0.13.3':add(out,'E_CLIENT_TRANSPORT','pins','pinned crate requires HTTP RowBinary transport')
 if c['insert_acceptance']['settings'].get('wait_end_of_query')!=1 or 'insert.end()' not in ' '.join(c['insert_acceptance']['ordered_steps']):add(out,'E_INSERT_FINALIZATION','insert_acceptance','HTTP insert must await complete response and end() before readback')
 if c['lookup_behavior']['external_dictionaries']!='forbidden for correctness and TOAST reconstruction' or c['lookup_behavior']['postgres_joins']!='forbidden':add(out,'E_LOOKUP','lookup_behavior','dictionary/source join would weaken reconstruction')
 provisional=[]
 if c.get('provisional_markers')!=provisional:add(out,'E_PROVISIONAL','provisional_markers','owner-card provisional inventory changed')
 if c.get('authority',{}).get('owner_cards')!=['59a63169'] or not c.get('authority',{}).get('status','').startswith('engineering artifact with accepted d-values, canonical-float, d-keys, and d-compose literals'):add(out,'E_AUTHORITY','authority','owner card 59a63169 must remain the literal decision authority')
 for marker in provisional:
  if marker not in text:add(out,'E_PROVISIONAL','inputs','missing '+marker)
 for token in ('postgres'+ '://','password'+'=','BEGIN PRIVATE'+' KEY','AK'+'IA'):
  if token in text:add(out,'E_SECRET','inputs','forbidden secret token')
 for name,item in c['consumes'].items():
  if name=='confirmation':continue
  p=item.get('path') or item.get('confirmed_projection_source'); h=item.get('sha256') or item.get('source_sha256')
  if not p or not h or digest(ROOT/p)!=h:add(out,'E_CONSUMED_DIGEST','consumes/'+name,'consumed input digest mismatch')
 expected_index='docker.io/clickhouse/clickhouse-server:25.8.2.29@sha256:74c213b4d4cb4854c2497694df0c2d153c041003eadbb0457ae62c28cb8d723f'
 expected_platform='sha256:78b6f0863688458b229b597f6a1bbf891855a01cf59c3f3dbe66428571c518c9'
 if (c['pins']['server'],c['pins']['platform'],c['pins']['image'],c['pins']['image_platform_digest'])!=('25.8.2.29','linux/amd64',expected_index,expected_platform):add(out,'E_PIN','pins','canonical Compose index/platform image identity changed')
 fp=c['consumes']['failure_policy']
 if (fp['version'],fp['base_delay_ms'],fp['cap_delay_ms'],fp['maximum_attempts'])!=(1,250,30000,10):add(out,'E_FAILURE_POLICY','consumes/failure_policy','confirmed retry literals changed')
 ddl=DDL.read_text(); query=Q.read_text(); retire=R.read_text()
 settings=c['insert_acceptance']['settings']
 for required in ('fsync_after_insert=1','fsync_part_directory=1'):
  if ddl.count(required)<3:add(out,'E_DDL_DURABILITY','files/ddl','all durable tables must pin '+required)
 if settings!={'async_insert':0,'wait_for_async_insert':1,'wait_end_of_query':1,'insert_quorum':1,'fsync_after_insert':1,'fsync_part_directory':1,'insert_deduplicate':0}:add(out,'E_DURABILITY_SETTINGS','insert_acceptance/settings','pinned settings changed')
 for obj in c['objects']['tables']+c['objects']['views']:
  if obj not in ddl:add(out,'E_DDL_OBJECT','files/ddl','missing '+obj)
 for forbidden in ('ReplacingMergeTree','CollapsingMergeTree','VersionedCollapsingMergeTree',' TTL '):
  if forbidden in ddl:add(out,'E_FORBIDDEN_ENGINE','files/ddl','unsafe history narrowing')
 order=['persist immutable batch intent','insert event_history','Rust insert','read back exact event count','insert batch_markers','read back one identical marker','commit destination checkpoint']
 pos=[next((i for i,x in enumerate(c['insert_acceptance']['ordered_steps']) if t in x),-1) for t in order]
 if -1 in pos or pos!=sorted(pos):add(out,'E_ACCEPT_ORDER','insert_acceptance/ordered_steps','acceptance ordering incomplete')
 for term in ('argMax','payload_variants',"latest_mutation.2!='delete'",'ARRAY JOIN','promotion_fence',"raw_value.1='absent_for_schema'", "if(raw_value.1='absent_for_schema','explicit_null'"):
  if term not in query:add(out,'E_QUERY','files/canonical_query','missing '+term)
 if ddl.count('capture_epoch UInt64')!=3 or 'capture_epoch FixedString' in ddl or '{capture_epoch:UInt64}' not in query or '{capture_epoch:UInt64}' not in retire or 'capture_epoch:FixedString' in retire:add(out,'E_CAPTURE_EPOCH','files','capture_epoch must map directly from event ABI u64 to ClickHouse UInt64 in DDL, queries, and retirement')
 if 'FINAL' in query and 'FINAL is intentionally absent' not in query:add(out,'E_FINAL','files/canonical_query','FINAL must not provide correctness')
 if 'DROP PARTITION' not in retire or re.search(r'ALTER TABLE\s+boring_cdc\.generation_selectors_v1',retire,re.I):add(out,'E_RETIRE','files/retirement','retirement scope unsafe')
 source=c['event_projection']['source_order']
 if source!=['lsn_u64','origin_rank','transaction_ordinal','mutation_ordinal','connector_event_id']:add(out,'E_SOURCE_ORDER','event_projection/source_order','full source order changed')
 required_hash={'capture_epoch','lsn_u64','origin_rank','transaction_ordinal','mutation_ordinal','connector_event_id','relation_schema_fingerprint','canonical_key','key_hash','operation','before_key','mutation_kind','columns.column_id','columns.state','columns.type_oid','columns.typmod','columns.value_base64'}
 if set(c['event_projection']['stored_payload_hash_inputs'])!=required_hash:add(out,'E_PAYLOAD_FIELDS','event_projection/stored_payload_hash_inputs','stored event cannot reproduce payload hash')
 if c['event_projection']['cross_epoch_comparison']!='forbidden' or c['objects']['native_replacement_version']!='forbidden':add(out,'E_VERSION_NARROW','event_projection','epoch/native narrowing enabled')
 audit=c['audit']; b=audit['budgets']
 if audit['checkpoint_moves'] or any(b[k]<=0 for k in b) or 'actual' not in audit['payload_verification'] or 'stored payload_hash' not in audit['payload_only_corruption']:add(out,'E_AUDIT','audit','finite actual-payload audit weakened')
 inspection=c['live_inspection']; metadata=inspection['object_metadata']; observed_settings=inspection['settings']
 expected_queries={
  'batch_markers_v1':('MergeTree','7f95b63c458fd873cdfd178cc0c3eb93589cb599b18b67ea4b4cc16b284000a6'),
  'event_history_v1':('MergeTree','e73c8f3d25cc9b492602bcc0c3f5bea8c5c31fe6e018f78450e50ee28b0e7857'),
  'event_identity_conflicts_v1':('View','e16a288d2fc597d4258b4d7525c4e7d2f67afd1a63fb6f9b49b6af743fe286aa'),
  'generation_selectors_v1':('MergeTree','51a51f357ca22db39280a8d0c6ca397aa78f63d145df85f85e9b6277792d86ea'),
  'live_generation_v1':('View','efe75aec390cdb952546d59403a09bec91ee246c4e14b46bf0ed9f45b4fd5caa'),
  'selector_conflicts_v1':('View','ddbc48206308d80c83e1e9ac8cbd34ea1a215ca436ca3e5d055550ddb124b81a'),
 }
 if inspection['server_version']!=c['pins']['server'] or {name:(row['engine'],row['create_query_sha256']) for name,row in metadata['expected'].items()}!=expected_queries or metadata['columns_row_count']!=43 or metadata['columns_digest_domain']!='boring-cdc/clickhouse-system-columns/v1' or metadata['columns_digest_sha256']!='938f3680ed9451d6d24ca72c886cd62c9607dc66540bf7c860873754f55d52ed' or metadata['columns_field_order']!=['table','name','type','position','is_in_partition_key','is_in_sorting_key'] or 'system.tables' not in metadata['query'] or 'system.columns' not in metadata['columns_query'] or '64-bit big-endian' not in metadata['columns_framing'] or 'only after fresh' not in inspection['object_fingerprint_rule']:add(out,'E_LIVE_OBJECT_INSPECTION','live_inspection/object_metadata','pinned live metadata projection changed')
 expected_query_options={'async_insert':'0','insert_deduplicate':'0','insert_quorum':'1','wait_for_async_insert':'1'}
 tables=('batch_markers_v1','event_history_v1','generation_selectors_v1')
 expected_table_options={table:{'fsync_after_insert':'1','fsync_part_directory':'1'} for table in tables}
 parts=['boring-cdc/clickhouse-settings/v1','wait_end_of_query','1']
 for name,value in sorted(expected_query_options.items()):parts.extend((name,value))
 for table in tables:
  parts.append(table)
  for name,value in sorted(expected_table_options[table].items()):parts.extend((name,value))
 settings_hash=hashlib.sha256()
 for part in parts:
  encoded=part.encode('ascii');settings_hash.update(len(encoded).to_bytes(8,'big'));settings_hash.update(encoded)
 if observed_settings['query_options']!=expected_query_options or observed_settings['transport_option']!={'wait_end_of_query':'1'} or observed_settings['table_options']!=expected_table_options or observed_settings['fingerprint_domain']!=parts[0] or observed_settings['expected_fingerprint_sha256']!=settings_hash.hexdigest() or 'system.settings' not in observed_settings['query_probe'] or 'system.settings does not expose' not in observed_settings['transport_proof'] or 'create_table_query' not in observed_settings['table_proof']:add(out,'E_LIVE_SETTINGS_INSPECTION','live_inspection/settings','effective query, transport, or table setting proof changed')
 selector=audit['selector_fingerprint']
 if audit['identity']!=['capture_epoch','generation','promotion_fence','selector_fingerprint','target_clickhouse_checkpoint','object_fingerprint','event_contract_sha256'] or selector['domain']!='boring-cdc/clickhouse-selector/v1' or selector['algorithm']!='SHA-256 lowercase hexadecimal' or selector['field_order']!=list(SELECTOR_FIELDS) or 'greatest-promotion_fence' not in selector['row_scope'] or 'selector_conflicts_v1' not in selector['conflict'] or '64-bit big-endian' not in selector['field_framing'] or 'without trimming' not in selector['string_encoding'] or 'without leading zeroes' not in selector['integer_encoding'] or 'no live selector' not in selector['empty']:add(out,'E_SELECTOR_FINGERPRINT','audit/selector_fingerprint','approved greatest-row selector identity changed')
 golden={'capture_epoch':1,'promotion_fence':9,'generation':7,'table_set_fingerprint':'6'*64,'compatible_anchor_id':'f'*64,'candidate_digest':'7'*64}
 expected='2d3882a0f2ad8eb3c95fc1d98de156a1093a4200a32275c46d926cecd1f8c86e'
 lower={**golden,'promotion_fence':8,'candidate_digest':'8'*64}
 higher={**golden,'promotion_fence':10,'candidate_digest':'9'*64}
 try:
  if selector_fingerprint([],1) is not None or selector_fingerprint([golden,golden],1)!=expected or selector_fingerprint([golden,lower],1)!=expected or selector_fingerprint([golden,higher],1)==expected:add(out,'E_SELECTOR_VECTOR','audit/selector_fingerprint','empty, identical, lower, or higher selector behavior changed')
  try:selector_fingerprint([golden,{**golden,'candidate_digest':'0'*64}],1)
  except ValueError:pass
  else:add(out,'E_SELECTOR_VECTOR','audit/selector_fingerprint','same-fence conflict was accepted')
 except Exception as e:add(out,'E_SELECTOR_VECTOR','audit/selector_fingerprint',str(e))
 quota=c['history_retention']['quota']
 if (quota['warning_history_bytes'],quota['hard_history_bytes'],quota['emergency_free_bytes'])!=(68719476736,85899345920,10737418240):add(out,'E_QUOTA','history_retention/quota','confirmed quota changed')
 if c['history_retention']['retirement_grace_seconds']!=86400 or c['history_retention']['ttl']!='forbidden':add(out,'E_RETENTION','history_retention','retention safety changed')
 ids=[x['fixture_id'] for x in f['cases']]
 for j,e in enumerate(f['golden_vectors']['events']):
  v=e.get('version',[])
  if len(v)!=5 or not isinstance(v[0],int) or v[1] not in (0,1) or not isinstance(v[2],int) or not isinstance(v[3],int) or v[4]!=e['id']:add(out,'E_GOLDEN_VERSION',f'golden_vectors/events/{j}/version','version must be exact event ABI tuple')
 if ids!=c['fixture_ids'] or len(ids)!=len(set(ids)):add(out,'E_FIXTURE_INVENTORY','fixtures/cases','fixture inventory mismatch')
 if {x['executor_id'] for x in f['cases']}!=set(c['executors']):add(out,'E_EXECUTORS','fixtures/cases','executor inventory mismatch')
 required={'DUPLICATE-CONFLICT','SOURCE-ORDER-SNAPSHOT-LATE','TOAST-PREDECESSOR','TOMBSTONE','BATCH-CRASH-AFTER-INSERT','PAYLOAD-ONLY-CORRUPTION','AUDIT-UNIT-RESUME','SAME-FENCE-CONFLICT','EXTERNAL-FENCE-AHEAD','INCOMPLETE-ANCHOR','QUOTA-HARD-BLOCK','RETIRE-ELIGIBLE','DETERMINISTIC-POISON','RESUME-CHANGED-BOUNDARY'}
 suffix={x.removeprefix('SCN-M0-CH-') for x in ids}
 if not required<=suffix:add(out,'E_FIXTURE_BRANCHES','fixtures/cases','required branches missing')
 for i,x in enumerate(f['cases']):
  execution=x.get('execution',{}); oracle=execution.get('oracle',{}); fault=execution.get('fault',{})
  if not execution.get('setup') or fault.get('operation')!=x['action']['fault_hook'] or oracle.get('destination_state')!=x['expected']['destination_state'] or oracle.get('checkpoint')!=x['expected']['checkpoint']:add(out,'E_EXECUTABLE_FIXTURE',f'cases/{i}','fixture lacks exact setup/fault/oracle binding')
  try:
   pointer=fault['arguments']['setup_pointer']; resolve_pointer(x,pointer)
   if pointer.endswith('/none'): raise ValueError('no-op pointer')
  except (KeyError,IndexError,TypeError,ValueError) as e:add(out,'E_EXECUTABLE_FIXTURE',f'cases/{i}/execution/fault/arguments','fault setup_pointer is not executable: '+str(e))
  if x['action']['fault_hook']=='mutate_payload_keep_ids_hash_marker' and fault.get('preserve')!=['connector_event_id','payload_hash','batch_marker']:add(out,'E_PAYLOAD_CORRUPTION_FIXTURE',f'cases/{i}','payload-only corruption does not preserve required identity')
  setup=execution.get('setup',{}); hook=x['action']['fault_hook']
  if hook=='after_insert_before_marker' and fault.get('arguments',{}).get('value')!={'client':'clickhouse 0.13.3','protocol':'HTTP RowBinary','history_insert_finalization':'insert.end().await succeeded with complete response and no server error','crash_boundary':'after acknowledged history insert, before marker insert'}:add(out,'E_ACK_CRASH_FIXTURE',f'cases/{i}/execution/fault/arguments/value','crash fixture must identify the acknowledged HTTP insert boundary')
  required_setup={'history_events','selector_rows','batch_markers','selected_table_ids','candidate_table_ids','audit','quota','retirement','retry','sqlite'}
  required_history={'capture_epoch','generation','logical_table_id','journal_seq','batch_id','before_key','cells','hash','id','key','key_hash','mutation_kind','op','relation_schema_fingerprint','version'}
  for j,event in enumerate(setup.get('history_events',[])):
   if set(event)!=required_history:add(out,'E_HISTORY_ROW',f'cases/{i}/execution/setup/history_events/{j}','history row is not directly insertable into DDL projection')
   if event.get('capture_epoch')!=x['pre_state']['capture_epoch'] or event.get('generation')!=x['pre_state']['generation']:add(out,'E_HISTORY_ROW',f'cases/{i}/execution/setup/history_events/{j}','history row epoch/generation does not match case')
  required_marker={'capture_epoch','generation','batch_id','first_journal_seq','last_journal_seq','event_count','ordered_event_digest','object_fingerprint','finalized_at_unix_ms'}
  history_batches={e.get('batch_id') for e in setup.get('history_events',[])}
  for j,marker in enumerate(setup.get('batch_markers',[])):
   if set(marker)!=required_marker:add(out,'E_BATCH_MARKER',f'cases/{i}/execution/setup/batch_markers/{j}','batch marker is not directly insertable into DDL')
   if marker.get('capture_epoch')!=x['pre_state']['capture_epoch'] or marker.get('generation')!=x['pre_state']['generation'] or marker.get('batch_id') not in history_batches:add(out,'E_BATCH_MARKER',f'cases/{i}/execution/setup/batch_markers/{j}','marker does not identify the scenario history batch')
  validate_marker_oracles(x,i,out)
  if set(setup)!=required_setup:add(out,'E_FIXTURE_SETUP',f'cases/{i}','scenario setup is not complete and exact')
  if hook=='higher_fence' and not any(r['promotion_fence']==10 and r['generation']==8 for r in setup.get('selector_rows',[])):add(out,'E_PROMOTION_FIXTURE',f'cases/{i}','higher selector missing')
  if hook=='same_fence_different_candidate' and len({(r['generation'],r['candidate_digest']) for r in setup.get('selector_rows',[]) if r['promotion_fence']==9})<2:add(out,'E_PROMOTION_FIXTURE',f'cases/{i}','same-fence conflict missing')
  if hook=='candidate_missing_table' and set(setup.get('candidate_table_ids',[]))>=set(setup.get('selected_table_ids',[])):add(out,'E_ANCHOR_FIXTURE',f'cases/{i}','incomplete candidate not modeled')
  if hook=='history_bytes_at_hard' and setup.get('quota',{}).get('history_bytes')!=85899345920:add(out,'E_QUOTA_FIXTURE',f'cases/{i}','hard quota literal missing')
  if hook=='retire_nonlive_after_grace_no_pins' and not (setup.get('retirement',{}).get('target_generation')==6 and setup['retirement']['elapsed_seconds']>86400 and setup['retirement']['pins']==[]):add(out,'E_RETIRE_FIXTURE',f'cases/{i}','eligible retirement proof missing')
  if not x['action']['fault_once'] or x['expected']['capture_and_archive']!='continue' or not x['expected']['redacted']:add(out,'E_FIXTURE_EXPECTED',f'cases/{i}','determinism/independence/redaction invalid')
  if x['expected']['destination_state'].startswith('blocked') and x['expected']['checkpoint']!='unchanged':add(out,'E_CHECKPOINT_SKIP',f'cases/{i}','blocked case advances checkpoint')
 try:
  got=simulate(f['golden_vectors']['events'])
  if got!=f['golden_vectors']['expected_current_rows']:add(out,'E_GOLDEN_ROWS','golden_vectors','expected rows do not follow dedup/order/TOAST/delete semantics')
 except Exception as e:add(out,'E_GOLDEN_ROWS','golden_vectors',str(e))
 try:
  manifest=load(M); artifacts=load(A); row=[x for x in manifest['artifacts'] if x['owner_bead']==OWNER]
  if len(row)!=1 or row[0]['path']!=str(C.relative_to(ROOT)) or row[0]['sha256']!=digest(C) or row[0]['fixture_ids']!=ids or row[0]['executor_ids']!=c['executors']:add(out,'E_MANIFEST','contracts/m0/manifest.json','manifest binding mismatch')
  expected={str(p.relative_to(ROOT)) for p in paths+[E]}
  rows=[x for x in artifacts['artifacts'] if x['owner_bead']==OWNER]
  if {x['path'] for x in rows}!=expected:add(out,'E_ARTIFACT_INVENTORY','contracts/m0/artifacts.json','artifact inventory mismatch')
  for x in rows:
   p=ROOT/x['path']
   if not p.is_file() or (p!=E and x['sha256']!=digest(p)):add(out,'E_ARTIFACT_HASH',x['path'],'artifact hash mismatch')
 except Exception as e:add(out,'E_REGISTRY','contracts/m0',str(e))
 return out,{str(p.relative_to(ROOT)):digest(p) for p in paths[:-1]}
def main():
 findings,inputs=validate(); previous=load(E) if E.exists() else {}; current=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(); validator_sha=digest(V); material=''.join(k+'\0'+v+'\n' for k,v in sorted(inputs.items())).encode()
 parent=previous.get('source_parent_git_commit') or current
 if previous.get('inputs')!=inputs or previous.get('validator_sha256')!=validator_sha: parent=current
 evidence={'schema_version':'m0-clickhouse-contract-evidence/v1','owner_bead':OWNER,'status':'pass' if not findings else 'fail','validator':str(V.relative_to(ROOT)),'validator_sha256':validator_sha,'source_parent_git_commit':parent,'input_tree_sha256':hashlib.sha256(material).hexdigest(),'inputs':inputs,'fixture_count':len(load(F)['cases']),'runtime_observed':False,'product_faults':'fault_not_applicable','findings':findings}
 E.parent.mkdir(parents=True,exist_ok=True); E.write_text(json.dumps(evidence,indent=2,sort_keys=True)+'\n'); print(json.dumps(evidence,sort_keys=True,separators=(',',':'))); return 0 if not findings else 1
if __name__=='__main__':raise SystemExit(main())
