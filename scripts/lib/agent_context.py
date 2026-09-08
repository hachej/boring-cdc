#!/usr/bin/env python3
"""Read-only, fail-closed canonical context and impact projections."""
from __future__ import annotations
import argparse, hashlib, json, os, re, subprocess, sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]; REG=ROOT/'contracts/agent/stable-ids.json'; GRAPH=ROOT/'.beads/issues.jsonl'; LIMIT=16384
CONTRACTS=sorted((ROOT/'contracts').rglob('*.schema.json'))+[REG,ROOT/'contracts/coverage/plan-to-beads.json']
def digest_bytes(b):return hashlib.sha256(b).hexdigest()
def digest(p):return digest_bytes(p.read_bytes())
def canonical(x):return json.dumps(x,sort_keys=True,separators=(',',':'))
def run(*a):return subprocess.run(a,cwd=ROOT,text=True,capture_output=True,check=False)
def safe_path(value):
 p=Path(value)
 if p.is_absolute() or '..' in p.parts:raise SystemExit('E_PATH_TRAVERSAL')
 target=ROOT/p
 try:target.resolve(strict=False).relative_to(ROOT.resolve())
 except ValueError:raise SystemExit('E_PATH_ESCAPE')
 cur=ROOT
 for part in p.parts:
  cur=cur/part
  if cur.is_symlink():raise SystemExit('E_PATH_SYMLINK')
 return target
def rows():
 out=[]
 for n,line in enumerate(GRAPH.read_text().splitlines(),1):
  try:out.append(json.loads(line))
  except Exception as e:raise SystemExit(f'E_GRAPH_JSON:{n}:{e}')
 return out
def dirty_state():
 changed=run('git','diff','HEAD','--name-only','-z');untracked=run('git','ls-files','--others','--exclude-standard','-z')
 if changed.returncode or untracked.returncode:raise SystemExit('E_GIT_STATUS')
 paths=sorted(set(filter(None,(changed.stdout+untracked.stdout).split('\0'))));chunks=[]
 for name in paths:
  p=safe_path(name);chunks.append(name.encode()+b'\0')
  if p.is_file():chunks.append(p.read_bytes())
  else:chunks.append(b'<deleted-or-nonfile>')
  chunks.append(b'\0')
 return paths,digest_bytes(b''.join(chunks))
def closure(selected,by):
 seen=set();stack=[selected]
 while stack:
  for d in by.get(stack.pop(),{}).get('dependencies',[]):
   dep=d.get('depends_on_id')
   if isinstance(dep,str) and dep not in seen:seen.add(dep);stack.append(dep)
 return sorted(seen)
def world(selected=None,observed=None):
 rs=rows();by={r['id']:r for r in rs};deps=closure(selected,by) if selected else [];paths,tree=dirty_state();commit=run('git','rev-parse','HEAD').stdout.strip()
 cds={str(p.relative_to(ROOT)):digest(p) for p in CONTRACTS if p.exists()}
 return {'schema_version':'world-state/v1','git_commit':commit,'working_tree_digest':tree,'dirty_paths':paths,'beads_snapshot_digest':digest(GRAPH),'selected_bead':selected,'dependency_closure_digest':digest_bytes(canonical(deps).encode()),'contract_digests':cds,'generated_view_versions':['stable-registry/v1','plan-to-beads/v1'],'evidence_index_status':'pending_unavailable','observed_at':observed or os.environ.get('BORING_AGENT_NOW','1970-01-01T00:00:00Z')}
def registry():return json.loads(REG.read_text())
def source_conflicts(reg):
 out=[]
 for name,want in reg.get('source_files',{}).items():
  p=safe_path(name);got=digest(p) if p.is_file() else 'missing'
  if got!=want:out.append({'source':name,'expected':want,'actual':got,'owner_bead':'boring-cdc-m0.2'})
 return out
def duplicate_ids(es):
 seen=set();out=set()
 for e in es:
  x=e.get('id')
  if isinstance(x,str):
   if x in seen:out.add(x)
   seen.add(x)
 return sorted(out)
def validate():
 reg=registry();graph=rows();by={r.get('id'):r for r in graph};f=[]
 if reg.get('schema_version')!='stable-registry/v1':f.append(['E_SCHEMA_VERSION','/schema_version'])
 expected={'REQ':144,'INV':20,'DEC':25,'CMD':26,'COND':6,'TRANS':6,'SCN':120,'REL':33,'RISK':38,'RUNBOOK':0,'CLAIM':0,'FINDING':0}
 counts={k:sum(e.get('namespace')==k for e in reg.get('entries',[])) for k in expected}
 for k,want in expected.items():
  if counts[k]!=want:f.append(['E_COVERAGE_INCOMPLETE',f'/{k}:{counts[k]}!={want}'])
 allowed=set(expected)
 for x in duplicate_ids(reg.get('entries',[])):f.append(['E_ID_DUPLICATE',x])
 for i,e in enumerate(reg.get('entries',[])):
  ident=e.get('id');ns=e.get('namespace');own=e.get('owner_bead')
  if not isinstance(ident,str) or not re.fullmatch(r'(REQ|INV|DEC|CMD|COND|TRANS|SCN|REL|RISK|RUNBOOK|CLAIM|FINDING)-[A-Z0-9-]+',ident):f.append(['E_ID_INVALID',f'/entries/{i}/id'])
  elif ns not in allowed or not ident.startswith(ns+'-'):f.append(['E_NAMESPACE_INVALID',ident])
  if own not in by:f.append(['E_OWNER_DANGLING',str(own)])
  if e.get('evidence_status')!='pending':f.append(['E_EVIDENCE_NOT_PENDING',str(ident)])
  source=e.get('source');excerpt=e.get('source_excerpt');sp=safe_path(source) if isinstance(source,str) else None
  if not isinstance(excerpt,str) or not sp or not sp.is_file() or excerpt not in sp.read_text() or digest_bytes(excerpt.encode())!=e.get('source_digest'):f.append(['E_SOURCE_FRAGMENT',str(ident)])
 f += [['E_SOURCE_CONFLICT',x['source']] for x in source_conflicts(reg)]
 covp=ROOT/'contracts/coverage/plan-to-beads.json';cov=json.loads(covp.read_text())
 if {(e['id'],e['owner_bead'],e['source_digest']) for e in reg['entries']}!={(e.get('id'),e.get('owner_bead'),e.get('source_digest')) for e in cov.get('assignments',[])}:f.append(['E_COVERAGE_DRIFT',str(covp.relative_to(ROOT))])
 return sorted(f)
def require_authority():
 f=validate()
 if f:raise SystemExit('E_AUTHORITY_CONFLICT:'+','.join(x[0] for x in f))
def effective(bead,ws,reg,by):
 assigned=[e for e in reg['entries'] if e['owner_bead']==bead];policy={'version':'agent-common-policy/v1','owner':'boring-cdc-m0.2','source':'AGENTS.md','digest':digest(ROOT/'AGENTS.md')}
 core={'schema_version':'effective-contract/v1','owner_bead':bead,'graph_snapshot_digest':ws['beads_snapshot_digest'],'task':by[bead],'common_policy':policy,'assignments':assigned,'source_digests':reg['source_files']};core['materialization_digest']=digest_bytes(canonical(core).encode());return core
def cmd_doctor(a):
 f=validate();print(canonical({'schema_version':'agent-doctor/v1','ok':not f,'read_only':True,'world_state':world(None,a.observed_at),'findings':[{'code':c,'target':p,'owner_bead':'boring-cdc-m0.2'} for c,p in f],'claim_index':'pending_unavailable','claim_reuse':False}));return bool(f)
def cmd_next(a):
 require_authority();rs=rows();by={r['id']:r for r in rs};cs=[]
 for r in rs:
  if r.get('status')!='open' or r.get('assignee'):continue
  blocked=[d['depends_on_id'] for d in r.get('dependencies',[]) if d.get('type')=='blocks' and by.get(d.get('depends_on_id'),{}).get('status')!='closed']
  if blocked:continue
  score=100-int(r.get('priority',4))*10+(20 if 'epic:boring-cdc-m0' in r.get('labels',[]) else 0);cs.append({'id':r['id'],'title':r['title'],'priority':r.get('priority'),'score':score,'ranking_reason':'unassigned; all blocking dependencies closed; priority then active epic label; lexical tie-break'})
 cs.sort(key=lambda x:(-x['score'],x['id']));print(canonical({'schema_version':'agent-next/v1','read_only':True,'ranking_formula':'100 - priority*10 + active_epic_label*20; tie=id','candidates':cs,'world_state':world(None,a.observed_at)}));return False
def profile_attachments(profile,bead,by,reg):
 direct=sorted({d['depends_on_id'] for d in by[bead].get('dependencies',[]) if d.get('depends_on_id') in by});reverse=sorted(r['id'] for r in by.values() if any(d.get('depends_on_id')==bead for d in r.get('dependencies',[])))
 base=[{'name':'dependency_outputs','complete':True,'content':[by[x] for x in direct]},{'name':'consumed_canonical_rows','complete':True,'content':[e for e in reg['entries'] if e['owner_bead'] in direct]}]
 if profile=='orient':return [{'name':'charter','complete':True,'content':(ROOT/'README.md').read_text()},{'name':'agent_rules','complete':True,'content':(ROOT/'AGENTS.md').read_text()},{'name':'orientation','complete':True,'content':{'health':'authority validation passed','current_milestone':'M0','blockers':direct,'status':by[bead].get('status'),'ready_candidates':[r['id'] for r in by.values() if r.get('status')=='open' and not r.get('assignee') and all(by.get(d.get('depends_on_id'),{}).get('status')=='closed' for d in r.get('dependencies',[]) if d.get('type')=='blocks')] }}]
 if profile=='implement':return base
 if profile=='review':return base+[{'name':'review_diff','complete':True,'content':run('git','show','--format=','--binary','HEAD').stdout},{'name':'impacted_consumers','complete':True,'content':reverse}]
 return base+[{'name':'handoff_state','complete':True,'content':{'changed_paths':dirty_state()[0],'checks':{'authority':'pass','claim_index':'pending_unavailable'},'evidence':{'status':'pending','reuse':False},'decisions':[],'risks':['revalidate world-state before mutation'],'unsafe_repeats':['do not claim, commit, push, or mutate from this reader'],'next_safe_command':f'scripts/agent/context {bead} --profile implement','consumers':reverse}}]
def validate_pack(pack):
 required={'schema_version','profile','world_state','summary','summary_bytes','total_bytes','token_estimate','included_ids','omitted_ids','attachments','expansion_plan','source_digests','effective_contract'}
 if set(pack)!=required or pack['schema_version']!='context-pack/v1':raise SystemExit('E_CONTEXT_SCHEMA')
 if pack['summary_bytes']!=len(pack['summary'].encode()) or pack['summary_bytes']>LIMIT:raise SystemExit('E_CONTEXT_BUDGET')
 if pack['total_bytes']!=len(canonical(pack).encode()) or pack['token_estimate']!=(pack['total_bytes']+3)//4:raise SystemExit('E_CONTEXT_ACCOUNTING')
 if any(not a.get('complete') or a.get('bytes')!=len(canonical(a.get('content')).encode()) for a in pack['attachments']):raise SystemExit('E_CONTEXT_ATTACHMENT')
 if set(pack['included_ids'])&set(pack['omitted_ids']):raise SystemExit('E_CONTEXT_IDS')
 return True
def cmd_context(a):
 require_authority();reg=registry();rs=rows();by={r['id']:r for r in rs}
 if a.bead not in by:raise SystemExit('E_BEAD_UNKNOWN:'+a.bead)
 if re.search(r'(?i)(?:postgres(?:ql)?|https?)://[^\s/:]+:[^\s/@]+@|-----BEGIN (?:RSA |OPENSSH )?PRIVATE KEY-----',canonical(by[a.bead])):raise SystemExit('E_SECRET_DETECTED')
 ws=world(a.bead,a.observed_at);eff=effective(a.bead,ws,reg,by);relevant=[e for e in reg['entries'] if e['owner_bead'] in ({a.bead}|set(closure(a.bead,by)))]
 summary=f"{a.bead}: {by[a.bead].get('title','')}\nProfile: {a.profile}\nStatus: {by[a.bead].get('status')}\nOwned IDs: {len(eff['assignments'])}; relevant IDs: {len(relevant)}\nDependencies: {', '.join(closure(a.bead,by)) or 'none'}\nEvidence index: pending/unavailable; reuse disabled."
 if len(summary.encode())>LIMIT:raise SystemExit('E_SUMMARY_LIMIT')
 all_ids=[e['id'] for e in relevant];included=all_ids if a.expand=='all' else all_ids[:64];omitted=[x for x in all_ids if x not in included]
 attachments=[{'name':'selected_bead','complete':True,'content':by[a.bead]},{'name':'manifest','complete':True,'content':ws}]+profile_attachments(a.profile,a.bead,by,reg)
 for x in attachments:x['bytes']=len(canonical(x['content']).encode())
 if a.expand:
  ids=all_ids if a.expand=='all' else [a.expand];content=[e for e in reg['entries'] if e['id'] in ids]
  if a.expand!='all' and not content:raise SystemExit('E_EXPANSION_UNKNOWN:'+a.expand)
  attachments.append({'name':'expansion','complete':True,'bytes':len(canonical(content).encode()),'content':content})
 pack={'schema_version':'context-pack/v1','profile':a.profile,'world_state':ws,'summary':summary,'summary_bytes':len(summary.encode()),'total_bytes':0,'token_estimate':0,'included_ids':included,'omitted_ids':omitted,'attachments':attachments,'expansion_plan':[{'id':x,'command':f'scripts/agent/context {a.bead} --profile {a.profile} --expand {x}'} for x in omitted],'source_digests':reg['source_files'],'effective_contract':eff}
 for _ in range(8):
  size=len(canonical(pack).encode());tokens=(size+3)//4
  if (size,tokens)==(pack['total_bytes'],pack['token_estimate']):break
  pack['total_bytes']=size;pack['token_estimate']=tokens
 validate_pack(pack);print(canonical(pack));return False
def changed_rows(reg,affected):
 changed=set();cache={}
 for e in affected:
  if e['source'] not in cache:
   p=safe_path(e['source']);cache[e['source']]=p.read_text() if p.is_file() else ''
  excerpt=e.get('source_excerpt','')
  if excerpt not in cache[e['source']] or digest_bytes(excerpt.encode())!=e.get('source_digest'):changed.add(e['id'])
 return changed
def cmd_impact(a):
 reg=registry();rs=rows();by={r['id']:r for r in rs};target=a.target
 if re.fullmatch(r'(REQ|INV|DEC|CMD|COND|TRANS|SCN|REL|RISK|RUNBOOK|CLAIM|FINDING)-[A-Z0-9-]+',target):affected=[e for e in reg['entries'] if e['id']==target];current=affected[0]['source_digest'] if affected else 'unavailable'
 else:
  p=safe_path(target);affected=[e for e in reg['entries'] if e['source']==target];current=digest(p) if p.is_file() else 'missing'
 changed=changed_rows(reg,affected);stale=sorted({e['owner_bead'] for e in affected if e['id'] in changed and by.get(e['owner_bead'],{}).get('status') in ('open','in_progress')});historical=sorted({e['owner_bead'] for e in affected if e['id'] in changed and by.get(e['owner_bead'],{}).get('status')=='closed'});conflicts=source_conflicts(reg)
 print(canonical({'schema_version':'impact/v1','target':target,'source_digest':current,'affected_ids':sorted(changed or [e['id'] for e in affected]),'stale_open_or_in_progress':stale,'historical_closed':historical,'conflicts':conflicts,'remediation':'canonical source owner updates registry; regenerate exactly the changed open/in-progress assignments; preserve historical evidence'}));return bool(conflicts and not changed)
def main(tool=None):
 tool=tool or Path(sys.argv[0]).name;p=argparse.ArgumentParser(prog=tool);p.add_argument('--observed-at')
 if tool=='context':p.add_argument('bead');p.add_argument('--profile',choices=['orient','implement','review','handoff'],default='implement');p.add_argument('--expand')
 elif tool=='impact':p.add_argument('target')
 a=p.parse_args();return {'doctor':cmd_doctor,'next':cmd_next,'context':cmd_context,'impact':cmd_impact}[tool](a)
if __name__=='__main__':sys.exit(main())
