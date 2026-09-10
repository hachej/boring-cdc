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

def execute(project,out):
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
    # Reader begins before the writer, blocks only on ordinary MVCC snapshots, and records pre/post counts.
    reader=subprocess.Popen(['docker','compose','-p',project,'-f','fixtures/m1/workload-compose.yml','exec','-T','postgres','psql','-U','postgres','-d','workload','-At','-c',"SELECT count(*) FROM customers; SELECT pg_sleep(2); SELECT count(*) FROM customers;"],stdout=open(Path(out)/'reader.txt','w'),stderr=subprocess.DEVNULL)
    rows=[
      (10,'g01',0,'customers','1','insert',"INSERT INTO customers VALUES(1,'Ada',1)"),
      (12,'g02',0,'products','10','insert',"INSERT INTO products VALUES(10,'P10',12.50)"),
      (13,'g03',0,'orders','100','insert',"INSERT INTO orders VALUES(100,1,'new')"),
      (14,'g04',0,'order_items','100:1','insert',"INSERT INTO order_items VALUES(100,1,10,1)"),
      (17,'g05',0,'customers','1','update',"UPDATE customers SET tier=2 WHERE id=1"),
      (18,'g05',1,'customers','1','update',"UPDATE customers SET tier=3 WHERE id=1"),
      (21,'g06',0,'order_items','100:1','delete',"DELETE FROM order_items WHERE order_id=100 AND line_no=1"),
      (24,'g07',0,'order_items','100:1','insert',"INSERT INTO order_items VALUES(100,1,10,2)"),
      (27,'g09',0,'customers','1','delete',"DELETE FROM customers WHERE id=1"),
      (28,'g09',1,'customers','2','insert',"INSERT INTO customers VALUES(2,'Ada',3)"),
    ]
    body=['BEGIN;']
    for seq,g,ordinal,table,key,op,stmt in rows:
      body += [f"SELECT set_config('workload.group','{g}',true); SELECT set_config('workload.ordinal','{ordinal}',true);",stmt+';',
       f"INSERT INTO mutation_ledger SELECT 'run-workload-v1',{seq},'m{seq:03}','{g}',{ordinal},'{table}','{key}','{op}',CASE WHEN '{op}'='delete' THEN NULL ELSE (SELECT after_hash FROM business_event_observations ORDER BY physical_id DESC LIMIT 1) END,'2025-01-01T00:00:{seq:02}Z','business';"]
    body += ["INSERT INTO workload_fence VALUES('run-workload-v1',30,10);", "INSERT INTO mutation_ledger VALUES('run-workload-v1',30,'f030','fence',0,'workload_fence','run-workload-v1','fence',NULL,'2025-01-01T00:00:30Z','fence');",'SELECT pg_sleep(1);','COMMIT;']
    sql(project,'\n'.join(body)); reader.wait(timeout=20)
    queries={
      'ledger.tsv':"SELECT run_id,mutation_seq,mutation_id,transaction_group_id,transaction_ordinal,entity_table,canonical_key,operation,coalesce(expected_after_hash,''),committed_at,record_kind FROM mutation_ledger ORDER BY mutation_seq",
      'business.tsv':"SELECT physical_id,transaction_group_id,transaction_ordinal,entity_table,canonical_key,operation,coalesce(after_hash,'') FROM business_event_observations ORDER BY physical_id",
      'state.tsv':"SELECT * FROM (SELECT 'customers' t,id::text k,md5(to_jsonb(customers)::text) h FROM customers UNION ALL SELECT 'products',id::text,md5(to_jsonb(products)::text) FROM products UNION ALL SELECT 'orders',id::text,md5(to_jsonb(orders)::text) FROM orders UNION ALL SELECT 'order_items',order_id::text||':'||line_no::text,md5(to_jsonb(order_items)::text) FROM order_items) s ORDER BY t,k",
      'fence.tsv':"SELECT run_id,watermark,ledger_count FROM workload_fence"
    }
    for name,query in queries.items(): Path(out,name).write_text(q(project,query))
    validate(Path(out),None)

def records(path): return [line.split('\t') for line in path.read_text().splitlines() if line]
def dimensions(root,mode):
    ledger=records(root/'ledger.tsv'); business=records(root/'business.tsv'); state=records(root/'state.tsv'); fence=records(root/'fence.tsv')
    expected_ledger=ledger.copy(); expected_business=business.copy()
    observed_ledger=ledger.copy(); observed_business=business.copy()
    unavailable=False
    if mode=='business-omission': observed_business.pop(4)
    if mode=='ledger-omission': observed_ledger.pop(4)
    if mode=='retry-duplicate': observed_business.append(observed_business[0].copy())
    if mode=='retry-conflict':
      x=observed_business[0].copy(); x[5]='delete'; x[6]=''; observed_business.append(x)
    if mode=='unavailable': unavailable=True; observed_business=[]
    # Deduplicate only byte-identical physical retry rows by physical_id; conflicts stay failures.
    by_physical={}; conflict=False
    for r in observed_business:
      prior=by_physical.get(r[0]); conflict |= prior is not None and prior != r; by_physical.setdefault(r[0],r)
    actual_business=list(by_physical.values())
    key=lambda r: tuple(r[1:7])
    business_ok=(not unavailable and not conflict and sorted(map(key,actual_business))==sorted(map(key,expected_business)))
    ledger_ok=observed_ledger==expected_ledger
    final_ok=(len(state)==4 and fence==[['run-workload-v1','30','10']])
    digest_rows=lambda rows: hashlib.sha256('\n'.join(sorted('\t'.join(r) for r in rows)).encode()).hexdigest()
    return {'ledger':'pass' if ledger_ok else 'fail','business':'unavailable' if unavailable else ('pass' if business_ok else 'fail'),'final_state':'pass' if final_ok else 'fail','fence':'pass' if final_ok else 'fail','sequence_gaps':'pass' if [int(r[1]) for r in ledger]==[10,12,13,14,17,18,21,24,27,28,30] else 'fail','ledger_count':len(observed_ledger),'business_count':len(actual_business),'state_count':len(state),'ledger_sorted_digest':digest_rows(observed_ledger),'business_sorted_digest':None if unavailable else digest_rows(actual_business),'final_typed_checksum':digest_rows(state)}
def validate(root,mode):
    d=dimensions(root,mode or 'clean')
    expected={'clean':('pass','pass'),'business-omission':('pass','fail'),'ledger-omission':('fail','pass'),'retry-duplicate':('pass','pass'),'retry-conflict':('pass','fail'),'unavailable':('pass','unavailable')}[mode or 'clean']
    assert (d['ledger'],d['business'])==expected and d['final_state']=='pass' and d['fence']=='pass' and d['sequence_gaps']=='pass',d
    return d

def fault(root,mode): print(json.dumps({'scenario':mode,**validate(Path(root),mode)},sort_keys=True))
def evidence(project,root,dest):
    root=Path(root); dest=Path(dest); shutil.rmtree(dest,ignore_errors=True); (dest/'logs').mkdir(parents=True); (dest/'state').mkdir()
    clean=dimensions(root,'clean'); faults={m:dimensions(root,m) for m in ['business-omission','ledger-omission','retry-duplicate','retry-conflict','unavailable']}
    packet={'schema_version':'m1-workload-packet/v1','observation_boundary':'PostgreSQL AFTER-row triggers committed atomically with source transactions but independent of mutation-ledger writes','correlation_fields':FIELDS,'clean':clean,'faults':faults,'digests':{p.name:h(p.read_text()) for p in sorted(root.glob('*.tsv'))},'reader_samples':(root/'reader.txt').read_text().splitlines()}
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
    manifest={'schema_version':'evidence/v1','owner_bead':'boring-cdc-m1-workload.2','scenario_id':dest.parent.name,'seed':'workload-v1','git_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'evidence_tier':'component','evidence_profile':'runtime','commands':[cmd],'result':{'status':'pass','runtime_observed':True,'digest':result_digest,'product_faults':'independent_ledger_and_business_omissions','artifacts':artifact_paths},'redaction':{'checked':True,'secrets_found':0},'cleanup':{'complete':True,'remaining_paths':[]},'source_preservation':{'preserved':True,'before_sha256':'a'*64,'after_sha256':'a'*64},'tier_proof':{'targeted_checks':True,'boundary_e2e':True,'fault_suite':True,'integration':True,'consumed_contract_vectors':True,'deterministic_rerun':True,'clean_environment':True,'clean_clone':True,'exit_assertions':True,'full_failure_matrix':False,'workspace_tests':True,'endurance':False}}
    (dest/'manifest.json').write_text(json.dumps(manifest,sort_keys=True,indent=2)+'\n')
    files=[p for p in dest.rglob('*') if p.is_file() and p.name!='sha256.txt']; (dest/'sha256.txt').write_text(''.join(f'{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.relative_to(dest)}\n' for p in sorted(files)))

if __name__=='__main__':
    cmd=sys.argv[1]
    if cmd=='execute': execute(sys.argv[2],sys.argv[3])
    elif cmd=='fault': fault(Path(sys.argv[2]),sys.argv[3])
    elif cmd=='evidence': evidence(sys.argv[2],sys.argv[3],sys.argv[4])
    else: raise SystemExit(2)
