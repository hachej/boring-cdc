#!/usr/bin/env python3
"""Fail-closed validator for DEC-POSTGRESQL-VERSIONS-TRANSPORT-OPTIONS."""
import hashlib,json,re,sys,subprocess,struct
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
C=ROOT/'contracts/m0/postgres-protocol.json'; F=ROOT/'fixtures/m0/decisions/boring-cdc-d-pg-protocol.json'
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def load(p):
 return json.loads(p.read_text())
def validate():
 findings=[]
 def req(ok,code,msg):
  if not ok: findings.append({'code':code,'message':msg})
 try: c,f=load(C),load(F)
 except Exception as e: return [{'code':'E_JSON','message':str(e)}]
 req(c.get('decision_id')=='DEC-POSTGRESQL-VERSIONS-TRANSPORT-OPTIONS','E_ID','decision identity changed')
 req(c.get('supported_postgresql')==[{'major':17,'exact_fixture_version':'17.6','platform':'linux/amd64','image_manifest':'sha256:00bc86618629af00d2937fdc5a5d63db3ff8450acf52f0636ec813c7f4902929','pg_stat_replication_slots':{'spill_counters':['spill_txns','spill_count','spill_bytes'],'stream_counters':['stream_txns','stream_count','stream_bytes']}}],'E_MATRIX','server matrix changed')
 t=c.get('transport',{}); req((t.get('crate'),t.get('version'),t.get('source'),t.get('crate_sha256'),t.get('default_features'),t.get('features'))==('pg_walstream','0.8.1','registry+https://github.com/rust-lang/crates.io-index','4cb204bf29c07ccaedb26f3c6c87fd02fc7cb99021bf7b40f339ddb3aae7fcc2',False,['rustls-tls']),'E_TRANSPORT','transport pin changed')
 opts=c.get('start_replication',{}).get('options'); req(opts=={'proto_version':'1','publication_names':'caller-owned quoted publication literal','binary':'false','messages':'true','streaming':'false','two_phase':'false','origin':'any'},'E_OPTIONS','START_REPLICATION options changed')
 w=c.get('wire',{}); req(w.get('xlog_data',{}).get('fixed_prefix_bytes')==25 and w.get('primary_keepalive',{}).get('payload_bytes')==18 and w.get('standby_status',{}).get('payload_bytes')==34,'E_WIRE','wire layouts changed')
 req(w.get('admission_owner')=='boring-cdc-d-admission' and not any(k.endswith('_bytes') for k in w if k not in {'xlog_data','primary_keepalive','standby_status'}),'E_ADMISSION_OWNER','protocol duplicated an admission literal')
 req(c.get('decoding_memory',{}).get('logical_decoding_work_mem_bytes')==67108864 and c['decoding_memory'].get('streamed_transactions') is False,'E_DECODING_MEMORY','decoding memory/stream policy changed')
 policy=c.get('failure_policy',{}); req(sha(ROOT/policy.get('contract','missing'))==policy.get('sha256'),'E_FAILURE_DIGEST','shared FailurePolicy digest changed')
 mapping=policy.get('mapping',[]); req(len(mapping)==11 and len({x.get('cause') for x in mapping})==11 and all(x.get('checkpoint')=='unchanged' and x.get('feedback')=='unchanged' for x in mapping) and {x.get('class') for x in mapping if x.get('class')} <= {'transient_io','transient_source','transient_destination','rate_limited','integrity','unsupported','ownership_lost','configuration'},'E_FAILURE_MAPPING','failure projection is incomplete')
 req(f.get('contract_sha256')==sha(C) and len(f.get('cases',[]))==len(mapping),'E_FIXTURE_BINDING','fixture not bound to contract/mapping')
 by={x['input']['cause']:x['expected'] for x in f.get('cases',[])}
 req(all(by.get(x['cause'],{}).get('class')==x.get('class') and by.get(x['cause'],{}).get('code')==x['code'] for x in mapping),'E_FIXTURE_CASES','fixture outcomes differ from mapping')
 def u(v): return struct.pack('>Q',v)
 def i(v): return struct.pack('>q',v)
 vectors={v['id']:v for v in f.get('wire_vectors',[])}
 encoded={'xlog-data':(b'w'+u(1)+u(2)+i(3)+b'BI').hex(),'primary-keepalive':(b'k'+u(4)+i(5)+b'\x01').hex(),'standby-status':(b'r'+u(6)*3+i(7)+b'\x00').hex()}
 req(set(vectors)==set(encoded) and all(vectors[k].get('expected_payload_hex')==v for k,v in encoded.items()),'E_WIRE_VECTORS','wire golden vectors differ')
 lock=(ROOT/'Cargo.lock').read_text(); req('name = "pg_walstream"\nversion = "0.8.1"' in lock and 'checksum = "4cb204bf29c07ccaedb26f3c6c87fd02fc7cb99021bf7b40f339ddb3aae7fcc2"' in lock,'E_LOCK','Cargo.lock transport pin absent')
 cargo=(ROOT/'Cargo.toml').read_text(); req('pg_walstream = { version = "=0.8.1", default-features = false, features = ["rustls-tls"] }' in cargo,'E_CARGO','Cargo transport differs')
 article=(ROOT/'src/article1_capture.rs').read_text(); req('("messages", MESSAGES)' in article and 'pub const MESSAGES: &str = "true";' in article,'E_ARTICLE_OPTIONS','Article 1 option set differs')
 req(article.count('ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol')==4,'E_ARTICLE_BOUNDARY','only publication/slot/table literals may remain provisional')
 pg=load(ROOT/'contracts/postgres/capture-backfill.json')['protocol']['transport']; req(pg.get('crate')=='pg_walstream' and pg.get('version')=='0.8.1' and pg.get('crate_sha256')==t.get('crate_sha256'),'E_PG_CONSUMER','PostgreSQL contract transport differs')
 return findings
if __name__=='__main__':
 fs=validate(); result={'schema_version':'postgres-protocol-validation/v1','status':'pass' if not fs else 'fail','findings':fs}
 if '--write-evidence' in sys.argv:
  if fs: raise SystemExit('refusing to write failing evidence')
  out=ROOT/'artifacts/m0/decisions/boring-cdc-d-pg-protocol'; out.mkdir(parents=True,exist_ok=True)
  fixture=load(F); lines=[json.dumps({'fixture_id':x['fixture_id'],'status':'pass','class':x['expected']['class'],'code':x['expected']['code']},sort_keys=True,separators=(',',':')) for x in fixture['cases']]
  (out/'fixture-run.jsonl').write_text('\n'.join(lines)+'\n')
  result.update({'owner_bead':'boring-cdc-d-pg-protocol','git_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),'inputs':{str(p.relative_to(ROOT)):sha(p) for p in [C,F,ROOT/'contracts/m0/failure-policy.json',ROOT/'Cargo.lock',ROOT/'Cargo.toml',ROOT/'src/article1_capture.rs',ROOT/'contracts/postgres/capture-backfill.json']},'fixture_count':len(fixture['cases']),'runtime_observed':False,'redaction':'pass: no DSN, credentials, source identifiers, or payloads'})
  (out/'evidence.json').write_text(json.dumps(result,sort_keys=True,separators=(',',':'))+'\n')
 print(json.dumps(result,sort_keys=True,separators=(',',':'))); raise SystemExit(bool(fs))
