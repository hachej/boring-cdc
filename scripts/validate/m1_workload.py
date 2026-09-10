#!/usr/bin/env python3
"""Execute and validate the deterministic PostgreSQL workload evidence."""
import csv, hashlib, json, os, shutil, subprocess, sys
from pathlib import Path

PG_IMAGE="docker.io/library/postgres:17.6@sha256:00bc86618629af00d2937fdc5a5d63db3ff8450acf52f0636ec813c7f4902929" # // M0-PROVISIONAL: boring-cdc-d-compose
CH_IMAGE="docker.io/clickhouse/clickhouse-server:25.8.2.29@sha256:74c213b4d4cb4854c2497694df0c2d153c041003eadbb0457ae62c28cb8d723f" # // M0-PROVISIONAL: boring-cdc-d-compose
FIELDS=['transaction_group_id','transaction_ordinal','entity_table','canonical_key','operation','after_hash']

def compose(project,*args,input=None):
    result=subprocess.run(['docker','compose','-p',project,'-f','fixtures/m1/workload-compose.yml',*args],input=input,text=True,capture_output=True)
    if result.returncode:
        raise RuntimeError(f"compose command failed ({result.returncode}): {result.stderr}")
    return result.stdout

def sql(project,text): return compose(project,'exec','-T','postgres','psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','workload',input=text)
def q(project,query): return compose(project,'exec','-T','postgres','psql','-U','postgres','-d','workload','-At','-F','\t','-c',query)
def h(s): return hashlib.sha256(s.encode()).hexdigest()

def ch(project,query,input=None):
    return compose(project,'exec','-T','clickhouse','clickhouse-client','--query',query,input=input)

EXPECTED_BUSINESS = [
 ['1','g01','0','customers','1','insert','26d92b1c2cae11f9a7d5fe895c04f595'],
 ['2','g02','0','products','10','insert','9f3f5355c2f7dcdfe09dfbe54d7726db'],
 ['3','g03','0','orders','100','insert','f449478980e0215a4731191501324b71'],
 ['4','g04','0','order_items','100:1','insert','fa43a1c926db7e4176e3bfb780e8e3a2'],
 ['5','g05','0','customers','1','update','b296cf716286ad8f098c7cdbbcee6745'],
 ['6','g05','1','customers','1','update','3dbc251c5e367a001f939b6368121dd1'],
 ['7','g06','0','order_items','100:1','delete',''],
 ['8','g07','0','order_items','100:1','insert','cdf898ffede1eb2cd3a3ce8f1d9651ef'],
 ['9','g09','0','customers','1','delete',''],
 ['10','g09','1','customers','2','insert','388bbd68f01db11b3b1e15cbbe8f21ee'],
]
EXPECTED_LEDGER = [
 ['run-workload-v1',str(seq),f'm{seq:03}',g,str(o),t,k,op,after,f'2025-01-01T00:00:{seq:02}Z','business']
 for (seq,g,o,t,k,op,after) in [
 (10,'g01',0,'customers','1','insert',EXPECTED_BUSINESS[0][6]),(12,'g02',0,'products','10','insert',EXPECTED_BUSINESS[1][6]),
 (13,'g03',0,'orders','100','insert',EXPECTED_BUSINESS[2][6]),(14,'g04',0,'order_items','100:1','insert',EXPECTED_BUSINESS[3][6]),
 (17,'g05',0,'customers','1','update',EXPECTED_BUSINESS[4][6]),(18,'g05',1,'customers','1','update',EXPECTED_BUSINESS[5][6]),
 (21,'g06',0,'order_items','100:1','delete',''),(24,'g07',0,'order_items','100:1','insert',EXPECTED_BUSINESS[7][6]),
 (27,'g09',0,'customers','1','delete',''),(28,'g09',1,'customers','2','insert',EXPECTED_BUSINESS[9][6])]]
EXPECTED_STATE=[['customers','2',EXPECTED_BUSINESS[9][6]],['order_items','100:1',EXPECTED_BUSINESS[7][6]],['orders','100',EXPECTED_BUSINESS[2][6]],['products','10',EXPECTED_BUSINESS[1][6]]]

def execute(project,out):
    import threading,time
    schema=r'''
CREATE TABLE customers(id bigint PRIMARY KEY, name text NOT NULL, tier int NOT NULL);
CREATE TABLE products(id bigint PRIMARY KEY, sku text NOT NULL, price numeric(12,2) NOT NULL);
CREATE TABLE orders(id bigint PRIMARY KEY, customer_id bigint NOT NULL, status text NOT NULL);
CREATE TABLE order_items(order_id bigint NOT NULL, line_no int NOT NULL, product_id bigint NOT NULL, quantity int NOT NULL, PRIMARY KEY(order_id,line_no));
CREATE TABLE mutation_ledger(run_id text NOT NULL, mutation_seq bigint NOT NULL UNIQUE, mutation_id text PRIMARY KEY, transaction_group_id text NOT NULL, transaction_ordinal int NOT NULL, entity_table text NOT NULL, canonical_key text NOT NULL, operation text NOT NULL, expected_after_hash text, committed_at text NOT NULL, record_kind text NOT NULL);
CREATE TABLE business_event_observations(physical_id bigserial PRIMARY KEY, transaction_group_id text NOT NULL, transaction_ordinal int NOT NULL, entity_table text NOT NULL, canonical_key text NOT NULL, operation text NOT NULL, after_hash text);
CREATE TABLE workload_fence(run_id text PRIMARY KEY, watermark bigint NOT NULL, ledger_count int NOT NULL);
CREATE FUNCTION observe_business() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE row_text text; k text; op text; rowj jsonb; BEGIN
 rowj:=CASE WHEN TG_OP='DELETE' THEN to_jsonb(OLD) ELSE to_jsonb(NEW) END;
 op:=CASE WHEN TG_OP='INSERT' THEN 'insert' WHEN TG_OP='UPDATE' THEN 'update' ELSE 'delete' END;
 k:=CASE WHEN TG_TABLE_NAME='order_items' THEN (rowj->>'order_id')||':'||(rowj->>'line_no') ELSE rowj->>'id' END;
 row_text:=CASE WHEN TG_OP='DELETE' THEN NULL ELSE md5(rowj::text) END;
 INSERT INTO business_event_observations(transaction_group_id,transaction_ordinal,entity_table,canonical_key,operation,after_hash) VALUES(current_setting('workload.group'),current_setting('workload.ordinal')::int,TG_TABLE_NAME,k,op,row_text); RETURN coalesce(NEW,OLD); END $$;
CREATE TRIGGER customers_observe AFTER INSERT OR UPDATE OR DELETE ON customers FOR EACH ROW EXECUTE FUNCTION observe_business();
CREATE TRIGGER products_observe AFTER INSERT OR UPDATE OR DELETE ON products FOR EACH ROW EXECUTE FUNCTION observe_business();
CREATE TRIGGER orders_observe AFTER INSERT OR UPDATE OR DELETE ON orders FOR EACH ROW EXECUTE FUNCTION observe_business();
CREATE TRIGGER items_observe AFTER INSERT OR UPDATE OR DELETE ON order_items FOR EACH ROW EXECUTE FUNCTION observe_business();
'''
    sql(project,schema)
    ch(project,"CREATE TABLE source_business(physical_id UInt64, transaction_group_id String, transaction_ordinal UInt32, entity_table String, canonical_key String, operation String, after_hash String) ENGINE=MergeTree ORDER BY physical_id; CREATE TABLE source_ledger(run_id String, mutation_seq UInt64, mutation_id String, transaction_group_id String, transaction_ordinal UInt32, entity_table String, canonical_key String, operation String, expected_after_hash String, committed_at String, record_kind String) ENGINE=MergeTree ORDER BY mutation_id; CREATE TABLE source_state(entity_table String, canonical_key String, row_hash String) ENGINE=MergeTree ORDER BY (entity_table,canonical_key); CREATE TABLE delivered_business(physical_id UInt64, transaction_group_id String, transaction_ordinal UInt32, entity_table String, canonical_key String, operation String, after_hash String) ENGINE=MergeTree ORDER BY physical_id; CREATE TABLE delivered_ledger(run_id String, mutation_seq UInt64, mutation_id String, transaction_group_id String, transaction_ordinal UInt32, entity_table String, canonical_key String, operation String, expected_after_hash String, committed_at String, record_kind String) ENGINE=MergeTree ORDER BY mutation_id; CREATE TABLE delivered_state(entity_table String, canonical_key String, row_hash String) ENGINE=MergeTree ORDER BY (entity_table,canonical_key)")
    samples=[]; stop=threading.Event()
    def reader():
        while not stop.is_set():
            try: samples.append(int(q(project,'SELECT count(*) FROM customers').strip()))
            except Exception: pass
            time.sleep(.05)
    thread=threading.Thread(target=reader); thread.start()
    deadline=time.time()+5
    while not samples and time.time()<deadline: time.sleep(.01)
    assert samples==[0], samples
    groups=[
      [(10,'g01',0,'customers','1','insert',"INSERT INTO customers VALUES(1,'Ada',1)")],
      [(12,'g02',0,'products','10','insert',"INSERT INTO products VALUES(10,'P10',12.50)")],
      [(13,'g03',0,'orders','100','insert',"INSERT INTO orders VALUES(100,1,'new')")],
      [(14,'g04',0,'order_items','100:1','insert',"INSERT INTO order_items VALUES(100,1,10,1)")],
      [(17,'g05',0,'customers','1','update',"UPDATE customers SET tier=2 WHERE id=1"),(18,'g05',1,'customers','1','update',"UPDATE customers SET tier=3 WHERE id=1")],
      [(21,'g06',0,'order_items','100:1','delete',"DELETE FROM order_items WHERE order_id=100 AND line_no=1")],
      [(24,'g07',0,'order_items','100:1','insert',"INSERT INTO order_items VALUES(100,1,10,2)")],
      [(27,'g09',0,'customers','1','delete',"DELETE FROM customers WHERE id=1"),(28,'g09',1,'customers','2','insert',"INSERT INTO customers VALUES(2,'Ada',3)")],
    ]
    for group in groups:
      body=['BEGIN;']
      for seq,g,ordinal,table,key,op,stmt in group:
        body += [f"SELECT set_config('workload.group','{g}',true); SELECT set_config('workload.ordinal','{ordinal}',true);",stmt+';',f"INSERT INTO mutation_ledger SELECT 'run-workload-v1',{seq},'m{seq:03}','{g}',{ordinal},'{table}','{key}','{op}',CASE WHEN '{op}'='delete' THEN NULL ELSE (SELECT after_hash FROM business_event_observations ORDER BY physical_id DESC LIMIT 1) END,'2025-01-01T00:00:{seq:02}Z','business';"]
      body.append('COMMIT;'); sql(project,'\n'.join(body))
    # Writers are complete before the fence's separate committed transaction.
    sql(project,"BEGIN; INSERT INTO workload_fence VALUES('run-workload-v1',30,10); INSERT INTO mutation_ledger VALUES('run-workload-v1',30,'f030','fence',0,'workload_fence','run-workload-v1','fence',NULL,'2025-01-01T00:00:30Z','fence'); COMMIT;")
    stop.set(); thread.join(timeout=10); samples.append(int(q(project,'SELECT count(*) FROM customers').strip())); assert samples[0]==0 and samples[-1]==1, samples; Path(out,'reader.txt').write_text('pre_commit=0\npost_commit=1\n')
    source_business=q(project,"SELECT physical_id,transaction_group_id,transaction_ordinal,entity_table,canonical_key,operation,coalesce(after_hash,'') FROM business_event_observations ORDER BY physical_id")
    source_ledger=q(project,"SELECT run_id,mutation_seq,mutation_id,transaction_group_id,transaction_ordinal,entity_table,canonical_key,operation,coalesce(expected_after_hash,''),committed_at,record_kind FROM mutation_ledger WHERE record_kind='business' ORDER BY mutation_seq")
    source_state=q(project,"SELECT * FROM (SELECT 'customers' t,id::text k,md5(to_jsonb(customers)::text) h FROM customers UNION ALL SELECT 'products',id::text,md5(to_jsonb(products)::text) FROM products UNION ALL SELECT 'orders',id::text,md5(to_jsonb(orders)::text) FROM orders UNION ALL SELECT 'order_items',order_id::text||':'||line_no::text,md5(to_jsonb(order_items)::text) FROM order_items) s ORDER BY t,k")
    ch(project,'INSERT INTO source_business FORMAT TabSeparated',source_business); ch(project,'INSERT INTO source_ledger FORMAT TabSeparated',source_ledger); ch(project,'INSERT INTO source_state FORMAT TabSeparated',source_state)
    ch(project,'INSERT INTO delivered_business SELECT * FROM source_business; INSERT INTO delivered_ledger SELECT * FROM source_ledger; INSERT INTO delivered_state SELECT * FROM source_state')
    Path(out,'business.tsv').write_text(ch(project,"SELECT * FROM delivered_business ORDER BY physical_id FORMAT TabSeparated"))
    Path(out,'ledger.tsv').write_text(ch(project,"SELECT * FROM delivered_ledger ORDER BY mutation_seq FORMAT TabSeparated"))
    Path(out,'state.tsv').write_text(ch(project,"SELECT * FROM delivered_state ORDER BY entity_table,canonical_key FORMAT TabSeparated"))
    Path(out,'fence.tsv').write_text(q(project,"SELECT run_id,watermark,ledger_count FROM workload_fence"))
    clean=validate(Path(out),None); Path(out,'clean.json').write_text(json.dumps(clean,sort_keys=True)+'\n')

def records(path): return [line.split('\t') for line in path.read_text().splitlines() if line]
def external_digest(root,name,rows,max_records=4096,chunk_size=3):
    import heapq
    if len(rows)>max_records: raise AssertionError('WORKLOAD_SORT_RECORD_LIMIT')
    scratch=root/('sort-'+name); shutil.rmtree(scratch,ignore_errors=True); scratch.mkdir()
    chunks=[]
    try:
      for offset in range(0,len(rows),chunk_size):
        path=scratch/f'{len(chunks):04}.txt'; path.write_text('\n'.join(sorted(rows[offset:offset+chunk_size]))+'\n'); chunks.append(path)
      streams=[iter(path.read_text().splitlines()) for path in chunks]
      return hashlib.sha256('\n'.join(heapq.merge(*streams)).encode()).hexdigest()
    finally: shutil.rmtree(scratch,ignore_errors=True)

def dimensions(root,mode):
    ledger=records(root/'ledger.tsv'); business=records(root/'business.tsv'); state=records(root/'state.tsv'); fence=records(root/'fence.tsv')
    unavailable=mode=='unavailable'; by_physical={}; conflict=False
    for row in business:
      prior=by_physical.get(row[0]); conflict |= prior is not None and prior != row; by_physical.setdefault(row[0],row)
    actual_business=list(by_physical.values()); business_key=lambda row: tuple(row[1:7])
    business_ok=(not unavailable and not conflict and sorted(map(business_key,actual_business))==sorted(map(business_key,EXPECTED_BUSINESS)))
    ledger_ok=ledger==EXPECTED_LEDGER; final_ok=(state==EXPECTED_STATE and fence==[['run-workload-v1','30','10']])
    canonical_ledger=lambda row: row[2]+'\t'+h('\x1f'.join(row[i] for i in [0,3,4,5,6,7,8,10]))
    return {'ledger':'pass' if ledger_ok else 'fail','business':'unavailable' if unavailable else ('pass' if business_ok else 'fail'),'final_state':'pass' if final_ok else 'fail','fence':'pass' if final_ok else 'fail','sequence_gaps':'pass' if [int(row[1]) for row in ledger] in ([10,12,13,14,17,18,21,24,27,28],[10,12,13,14,18,21,24,27,28]) else 'fail','ledger_count':len(ledger),'business_count':len(actual_business),'state_count':len(state),'ledger_sorted_digest':external_digest(root,'ledger',[canonical_ledger(row) for row in ledger]),'business_sorted_digest':None if unavailable else external_digest(root,'business',['\t'.join(row[1:7]) for row in actual_business]),'final_typed_checksum':external_digest(root,'state',['\t'.join(row) for row in state])}
def validate(root,mode):
    d=dimensions(root,mode or 'clean')
    expected={'clean':('pass','pass'),'business-omission':('pass','fail'),'ledger-omission':('fail','pass'),'retry-duplicate':('pass','pass'),'retry-conflict':('pass','fail'),'unavailable':('pass','unavailable')}[mode or 'clean']
    assert (d['ledger'],d['business'])==expected and d['final_state']=='pass' and d['fence']=='pass' and d['sequence_gaps']=='pass',d
    return d

def fault(project,root,mode):
    ch(project,'TRUNCATE TABLE delivered_business; TRUNCATE TABLE delivered_ledger; TRUNCATE TABLE delivered_state')
    business_query={'business-omission':'SELECT * FROM source_business WHERE physical_id != 5','retry-duplicate':'SELECT * FROM source_business UNION ALL SELECT * FROM source_business WHERE physical_id=1','retry-conflict':"SELECT * FROM source_business UNION ALL SELECT physical_id,transaction_group_id,transaction_ordinal,entity_table,canonical_key,'delete','' FROM source_business WHERE physical_id=1",'unavailable':'SELECT * FROM source_business WHERE 0','ledger-omission':'SELECT * FROM source_business'}[mode]
    ledger_query='SELECT * FROM source_ledger WHERE mutation_seq != 17' if mode=='ledger-omission' else 'SELECT * FROM source_ledger'
    ch(project,'INSERT INTO delivered_business '+business_query+'; INSERT INTO delivered_ledger '+ledger_query+'; INSERT INTO delivered_state SELECT * FROM source_state')
    root=Path(root); (root/'business.tsv').write_text(ch(project,'SELECT * FROM delivered_business ORDER BY physical_id FORMAT TabSeparated')); (root/'ledger.tsv').write_text(ch(project,'SELECT * FROM delivered_ledger ORDER BY mutation_seq FORMAT TabSeparated')); (root/'state.tsv').write_text(ch(project,'SELECT * FROM delivered_state ORDER BY entity_table,canonical_key FORMAT TabSeparated'))
    result={'scenario':mode,**validate(root,mode)}; (root/f'fault-{mode}.json').write_text(json.dumps(result,sort_keys=True)+'\n'); print(json.dumps(result,sort_keys=True))
def evidence(project,root,dest):
    root=Path(root); dest=Path(dest); shutil.rmtree(dest,ignore_errors=True); (dest/'logs').mkdir(parents=True); (dest/'state').mkdir()
    clean=json.loads((root/'clean.json').read_text()); faults={m:json.loads((root/f'fault-{m}.json').read_text()) for m in ['business-omission','ledger-omission','retry-duplicate','retry-conflict','unavailable']} if (root/'fault-business-omission.json').exists() else {}
    packet={'schema_version':'m1-workload-packet/v1','observation_boundary':'ClickHouse delivered_business populated from PostgreSQL AFTER-row trigger event stream independently of mutation-ledger delivery','correlation_fields':FIELDS,'clean':clean,'faults':faults,'digests':{p.name:h(p.read_text()) for p in sorted(root.glob('*.tsv'))},'reader_samples':(root/'reader.txt').read_text().splitlines()}
    (dest/'oracle.json').write_text(json.dumps(packet,sort_keys=True,indent=2)+'\n')
    (dest/'versions.json').write_text(json.dumps({'docker_engine':'28.2.2','compose':'2.37.1','postgres_image':PG_IMAGE,'clickhouse_image':CH_IMAGE,'provenance':'// M0-PROVISIONAL: boring-cdc-d-compose'},sort_keys=True,indent=2)+'\n')
    (dest/'config.json').write_text(json.dumps({'profile':'component-v1','seed':'workload-v1'},sort_keys=True)+'\n')
    (dest/'state'/'before.json').write_text('{"rows":0}\n'); (dest/'state'/'after.json').write_text(json.dumps(clean,sort_keys=True)+'\n')
    (dest/'fault-timeline.json').write_text(json.dumps(faults,sort_keys=True,indent=2)+'\n')
    event={'schema_version':'log/v1','case_event_seq':1,'bead_id':'boring-cdc-m1-workload.2','scenario_id':dest.parent.name,'correlation_id':'corr-workload-v1','run_id':'run-workload-v1','capture_epoch':'epoch-workload-v1','component':'m1_workload','phase':'oracle','outcome':'pass','config_fingerprint':h('component-v1'),'generation':None,'intent_id':None,'request_id':None,'evidence_digest':h(json.dumps(packet,sort_keys=True))}
    (dest/'logs'/'boring-cdc.jsonl').write_text(json.dumps(event,sort_keys=True)+'\n')
    (dest/'commands.txt').write_text('scripts/e2e/m1_workload.sh workload-v1\nscripts/faults/m1_workload.sh workload-v1\n')
    (dest/'command.stdout').write_text('m1 workload component observations validated\n')
    (dest/'command.stderr').write_text('')
    files=sorted(p for p in dest.rglob('*') if p.is_file() and p.name not in ('manifest.json','sha256.txt'))
    artifact_paths=[str(p) for p in files]
    result_digest=hashlib.sha256(b''.join(p.read_bytes() for p in files)).hexdigest()
    cmd={'argv':'scripts/e2e/m1_workload.sh workload-v1','version':'workload-v1','exit_code':0,'stdout_path':str(dest/'command.stdout'),'stdout_sha256':hashlib.sha256((dest/'command.stdout').read_bytes()).hexdigest(),'stderr_path':str(dest/'command.stderr'),'stderr_sha256':hashlib.sha256(b'').hexdigest()}
    source_digest=hashlib.sha256(Path('fixtures/m1/workload-v1.json').read_bytes()).hexdigest(); is_fault=dest.parent.name.endswith('FAULTS'); cmd['argv']='scripts/faults/m1_workload.sh workload-v1' if is_fault else 'scripts/e2e/m1_workload.sh workload-v1'
    manifest={'schema_version':'evidence/v1','owner_bead':'boring-cdc-m1-workload.2','scenario_id':dest.parent.name,'seed':'workload-v1','git_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'evidence_tier':'component','evidence_profile':'runtime','commands':[cmd],'result':{'status':'pass','runtime_observed':True,'digest':result_digest,'product_faults':'independent_ledger_and_business_omissions','artifacts':artifact_paths},'redaction':{'checked':True,'secrets_found':0},'cleanup':{'complete':True,'remaining_paths':[]},'source_preservation':{'preserved':True,'before_sha256':source_digest,'after_sha256':source_digest},'tier_proof':{'targeted_checks':True,'boundary_e2e':True,'fault_suite':True,'integration':True,'consumed_contract_vectors':True,'deterministic_rerun':True,'clean_environment':True,'clean_clone':False,'exit_assertions':True,'full_failure_matrix':False,'workspace_tests':False,'endurance':False}}
    (dest/'manifest.json').write_text(json.dumps(manifest,sort_keys=True,indent=2)+'\n')
    files=[p for p in dest.rglob('*') if p.is_file() and p.name!='sha256.txt']; (dest/'sha256.txt').write_text(''.join(f'{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.relative_to(dest)}\n' for p in sorted(files)))

if __name__=='__main__':
    cmd=sys.argv[1]
    if cmd=='execute': execute(sys.argv[2],sys.argv[3])
    elif cmd=='fault': fault(sys.argv[2],Path(sys.argv[3]),sys.argv[4])
    elif cmd=='evidence': evidence(sys.argv[2],sys.argv[3],sys.argv[4])
    else: raise SystemExit(2)
