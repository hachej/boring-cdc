#!/usr/bin/env python3
"""Read-only canonical context/impact projections for repository agents."""
from __future__ import annotations
import argparse, hashlib, json, os, re, subprocess, sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
REG=ROOT/'contracts/agent/stable-ids.json'; GRAPH=ROOT/'.beads/issues.jsonl'; LIMIT=16*1024
CONTRACTS=sorted((ROOT/'contracts').rglob('*.schema.json'))+[REG,ROOT/'contracts/coverage/plan-to-beads.json']
def digest_bytes(b): return hashlib.sha256(b).hexdigest()
def digest(path): return digest_bytes(path.read_bytes())
def canonical(x): return json.dumps(x,sort_keys=True,separators=(',',':'))
def run(*argv): return subprocess.run(argv,cwd=ROOT,text=True,capture_output=True,check=False)
def rows():
 out=[]
 for n,line in enumerate(GRAPH.read_text().splitlines(),1):
  try: out.append(json.loads(line))
  except Exception as e: raise SystemExit(f'E_GRAPH_JSON:{n}:{e}')
 return out
def git_status():
 p=run('git','status','--porcelain=v1','-z');
 if p.returncode: raise SystemExit('E_GIT_STATUS:'+p.stderr.strip())
 return sorted(x for x in p.stdout.split('\0') if x)
def closure(selected,by):
 seen=set(); stack=[selected]
 while stack:
  cur=stack.pop()
  for d in by.get(cur,{}).get('dependencies',[]):
   dep=d.get('depends_on_id')
   if dep and dep not in seen: seen.add(dep);stack.append(dep)
 return sorted(seen)
def world(selected=None, observed=None):
 rs=rows(); by={r['id']:r for r in rs}; deps=closure(selected,by) if selected else []
 status=git_status(); commit=run('git','rev-parse','HEAD').stdout.strip()
 cds={str(p.relative_to(ROOT)):digest(p) for p in CONTRACTS if p.exists()}
 return {'schema_version':'world-state/v1','git_commit':commit,'working_tree_digest':digest_bytes('\n'.join(status).encode()),'dirty_paths':status,'beads_snapshot_digest':digest(GRAPH),'selected_bead':selected,'dependency_closure_digest':digest_bytes(canonical(deps).encode()),'contract_digests':cds,'generated_view_versions':['stable-registry/v1','plan-to-beads/v1'],'evidence_index_status':'pending_unavailable','observed_at':observed or os.environ.get('BORING_AGENT_NOW','1970-01-01T00:00:00Z')}
def registry(): return json.loads(REG.read_text())
def source_conflicts(reg):
 result=[]
 for path,want in reg['source_files'].items():
  p=ROOT/path; got=digest(p) if p.is_file() else 'missing'
  if got!=want: result.append({'source':path,'expected':want,'actual':got,'owner_bead':'boring-cdc-m0.2'})
 return result
def duplicate_ids(entries):
 seen=set();return sorted({e.get('id') for e in entries if isinstance(e.get('id'),str) and (e['id'] in seen or seen.add(e['id']))})
def validate():
 reg=registry(); graph=rows(); by={r.get('id'):r for r in graph}; findings=[]
 if reg.get('schema_version')!='stable-registry/v1': findings.append(['E_SCHEMA_VERSION','/schema_version'])
 allowed={'REQ','INV','DEC','CMD','COND','TRANS','SCN','REL','RISK','RUNBOOK','CLAIM','FINDING'}
 for ident in duplicate_ids(reg.get('entries',[])):findings.append(['E_ID_DUPLICATE',ident])
 for i,e in enumerate(reg.get('entries',[])):
  ident=e.get('id'); ns=e.get('namespace'); own=e.get('owner_bead')
  if not isinstance(ident,str) or not re.fullmatch(r'(REQ|INV|DEC|CMD|COND|TRANS|SCN|REL|RISK|RUNBOOK|CLAIM|FINDING)-[A-Z0-9-]+',ident):findings.append(['E_ID_INVALID',f'/entries/{i}/id'])
  elif ns not in allowed or not ident.startswith(ns+'-'):findings.append(['E_NAMESPACE_INVALID',ident])
  if own not in by:findings.append(['E_OWNER_DANGLING',str(own)])
  if e.get('evidence_status')!='pending':findings.append(['E_EVIDENCE_NOT_PENDING',str(ident)])
 findings += [['E_SOURCE_CONFLICT',x['source']] for x in source_conflicts(reg)]
 cov=json.loads((ROOT/'contracts/coverage/plan-to-beads.json').read_text())
 if {(e['id'],e['owner_bead'],e['source_digest']) for e in reg['entries']} != {(e.get('id'),e.get('owner_bead'),e.get('source_digest')) for e in cov.get('assignments',[])}:findings.append(['E_COVERAGE_DRIFT','contracts/coverage/plan-to-beads.json'])
 return sorted(findings)
def effective(bead, ws, reg, by):
 task=by[bead]; assigned=[e for e in reg['entries'] if e['owner_bead']==bead]
 policy={'version':'agent-common-policy/v1','owner':'boring-cdc-m0.2','source':'AGENTS.md','digest':digest(ROOT/'AGENTS.md')}
 core={'schema_version':'effective-contract/v1','owner_bead':bead,'graph_snapshot_digest':ws['beads_snapshot_digest'],'task':task,'common_policy':policy,'assignments':assigned,'source_digests':reg['source_files']}
 core['materialization_digest']=digest_bytes(canonical(core).encode());return core
def cmd_doctor(a):
 f=validate(); out={'schema_version':'agent-doctor/v1','ok':not f,'read_only':True,'world_state':world(None,a.observed_at),'findings':[{'code':x[0],'target':x[1],'owner_bead':'boring-cdc-m0.2'} for x in f],'claim_index':'pending_unavailable','claim_reuse':False}; print(canonical(out));return bool(f)
def cmd_next(a):
 rs=rows(); by={r['id']:r for r in rs}; candidates=[]
 for r in rs:
  if r.get('status')!='open' or r.get('assignee'):continue
  blocking=[d['depends_on_id'] for d in r.get('dependencies',[]) if d.get('type')=='blocks' and by.get(d.get('depends_on_id'),{}).get('status')!='closed']
  if blocking:continue
  labels=r.get('labels',[]); score=100-int(r.get('priority',4))*10+(20 if 'epic:boring-cdc-m0' in labels else 0)
  candidates.append({'id':r['id'],'title':r['title'],'priority':r.get('priority'),'score':score,'ranking_reason':'unassigned; all blocking dependencies closed; higher priority and active epic label rank first'})
 candidates.sort(key=lambda x:(-x['score'],x['id']))
 print(canonical({'schema_version':'agent-next/v1','read_only':True,'ranking_formula':'100 - priority*10 + active_epic_label*20; tie=id','candidates':candidates,'world_state':world(None,a.observed_at)}));return False
def cmd_context(a):
 reg=registry(); rs=rows();by={r['id']:r for r in rs}
 if a.bead not in by: raise SystemExit('E_BEAD_UNKNOWN:'+a.bead)
 task_text=canonical(by[a.bead])
 if re.search(r'(?i)(?:postgres(?:ql)?|https?)://[^\s/:]+:[^\s/@]+@|-----BEGIN (?:RSA |OPENSSH )?PRIVATE KEY-----',task_text): raise SystemExit('E_SECRET_DETECTED')
 ws=world(a.bead,a.observed_at); eff=effective(a.bead,ws,reg,by); owned=eff['assignments']
 summary=f"{a.bead}: {by[a.bead].get('title','')}\nStatus: {by[a.bead].get('status')}\nOwned IDs: {len(owned)}\nDependencies: {', '.join(closure(a.bead,by)) or 'none'}\nEvidence index: pending/unavailable; reuse disabled."
 if len(summary.encode())>LIMIT: raise SystemExit('E_SUMMARY_LIMIT')
 all_ids=[e['id'] for e in owned]; included=all_ids if a.expand=='all' else all_ids[:64]; omitted=[x for x in all_ids if x not in included]
 attachments=[{'name':'selected_bead','complete':True,'bytes':len(canonical(by[a.bead]).encode()),'content':by[a.bead]},{'name':'manifest','complete':True,'bytes':len(canonical(ws).encode()),'content':ws}]
 if a.expand:
  ids=all_ids if a.expand=='all' else [a.expand]
  attachments.append({'name':'expansion','complete':True,'bytes':len(canonical(ids).encode()),'content':[e for e in owned if e['id'] in ids]})
 pack={'schema_version':'context-pack/v1','profile':a.profile,'world_state':ws,'summary':summary,'summary_bytes':len(summary.encode()),'total_bytes':0,'token_estimate':0,'included_ids':included,'omitted_ids':omitted,'attachments':attachments,'expansion_plan':[{'id':x,'command':f'scripts/agent/context {a.bead} --expand {x}'} for x in omitted],'source_digests':reg['source_files'],'effective_contract':eff}
 pack['total_bytes']=len(canonical(pack).encode());pack['token_estimate']=(pack['total_bytes']+3)//4
 print(canonical(pack));return False
def cmd_impact(a):
 reg=registry();rs=rows();by={r['id']:r for r in rs}; target=a.target
 affected=[e for e in reg['entries'] if e['id']==target or e['source']==target]
 current=digest(ROOT/target) if (ROOT/target).is_file() else (affected[0]['source_digest'] if affected else 'unavailable')
 changed_sources={x['source'] for x in source_conflicts(reg)}
 stale=sorted({e['owner_bead'] for e in affected if e['source'] in changed_sources and by.get(e['owner_bead'],{}).get('status') in ('open','in_progress')})
 historical=sorted({e['owner_bead'] for e in affected if e['source'] in changed_sources and by.get(e['owner_bead'],{}).get('status')=='closed'})
 conflicts=source_conflicts(reg)
 out={'schema_version':'impact/v1','target':target,'source_digest':current,'affected_ids':sorted(e['id'] for e in affected),'stale_open_or_in_progress':stale,'historical_closed':historical,'conflicts':conflicts,'remediation':'canonical source owner updates registry; regenerate affected open/in-progress assignments; never rewrite historical evidence'}
 print(canonical(out));return False
def main(tool=None):
 tool=tool or Path(sys.argv[0]).name;p=argparse.ArgumentParser(prog=tool);p.add_argument('--observed-at')
 if tool=='context':p.add_argument('bead');p.add_argument('--profile',choices=['orient','implement','review','handoff'],default='implement');p.add_argument('--expand')
 elif tool=='impact':p.add_argument('target')
 a=p.parse_args(); return {'doctor':cmd_doctor,'next':cmd_next,'context':cmd_context,'impact':cmd_impact}[tool](a)
if __name__=='__main__':sys.exit(main())
