#!/usr/bin/env python3
"""Immutable claim, finding, and handoff contract validators."""
from __future__ import annotations
import argparse, hashlib, json, re, subprocess, sys
from datetime import datetime, timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]; VERSION="knowledge-validators/1.0.0"; OWNER="boring-cdc-m0.3"
SHA=re.compile(r"^[0-9a-f]{64}$"); GIT=re.compile(r"^[0-9a-f]{40}$"); BEAD=re.compile(r"^boring-cdc-[A-Za-z0-9.-]+$"); SID=re.compile(r"^(CLAIM|FINDING|SCN|REQ|INV|DEC|CMD|COND|TRANS|REL|RISK|RUNBOOK|ART)-[A-Z0-9-]+$")
SECRET=re.compile(r'(?i)(password\s*[=:]|api[_-]?key\s*[=:]|secret\s*[=:]|token\s*[=:]|[a-z][a-z0-9+.-]*://|-----BEGIN .*PRIVATE KEY-----|(?:^|[\s"])/(?:[A-Za-z0-9._-]+/)+|raw[_ -]?payload|driver[_ -]?error)')
BINDINGS=("git_commit","graph_digest","effective_contract_digest","capture_epoch_digest","code_digest","binary_digest","fixture_digest","image_digest","config_digest","profile_digest","seed_digest","environment_digest","command_digest","result_digest","artifact_digest","redaction_digest")
class DuplicateKey(ValueError):pass
def unique(pairs):
 out={}
 for k,v in pairs:
  if k in out:raise DuplicateKey(k)
  out[k]=v
 return out
def dg(x:bytes):return hashlib.sha256(x).hexdigest()
def add(fs,c,p,m,owner=OWNER):fs.append({"code":c,"pointer":p,"owner_bead":owner,"message":m})
def load(path,fs,jsonl=False):
 try:raw=Path(path).read_bytes()
 except OSError:add(fs,"E_INPUT_MISSING","/","input cannot be read");return None,b""
 try:
  if jsonl:return [json.loads(x,object_pairs_hook=unique) for x in raw.splitlines() if x.strip()],raw
  return json.loads(raw,object_pairs_hook=unique),raw
 except DuplicateKey as e:add(fs,"E_DUPLICATE_KEY","/",f"duplicate key: {e}")
 except (json.JSONDecodeError,UnicodeDecodeError) as e:add(fs,"E_JSON_MALFORMED","/",f"malformed JSON: {e}")
 return None,raw
def req(o,names,fs,p=""):
 if not isinstance(o,dict):add(fs,"E_TYPE",p or "/","expected object");return False
 for n in names:
  if n not in o:add(fs,"E_REQUIRED",f"{p}/{n}","required field is absent")
 return True
def closed(o,names,fs,p=""):
 if isinstance(o,dict):
  for n in sorted(set(o)-set(names)):add(fs,"E_UNKNOWN_FIELD",f"{p}/{n}","unknown field")
def ids(values,fs,p,prefix=None,nonempty=True):
 if not isinstance(values,list) or (nonempty and not values):add(fs,"E_ID_LIST",p,"expected nonempty ID array");return
 seen=set()
 for i,v in enumerate(values):
  if not isinstance(v,str) or not SID.fullmatch(v) or (prefix and not v.startswith(prefix)):add(fs,"E_ID_INVALID",f"{p}/{i}","invalid stable ID")
  elif v in seen:add(fs,"E_DUPLICATE_ID",f"{p}/{i}",f"duplicate ID: {v}")
  else:seen.add(v)
def digest_fields(o,names,fs,p):
 for n in names:
  v=o.get(n) if isinstance(o,dict) else None
  pattern=GIT if n=="git_commit" else SHA
  if not isinstance(v,str) or not pattern.fullmatch(v):add(fs,"E_DIGEST",f"{p}/{n}",f"invalid {n}")
def secret_check(o,fs):
 if SECRET.search(json.dumps(o,sort_keys=True)):add(fs,"E_SECRET","/","secret, DSN, raw absolute path, or private key is forbidden")
def parse_time(v):
 try:return datetime.fromisoformat(v.replace("Z","+00:00"))
 except (ValueError,AttributeError):return None

def validate_claims(doc,index,baseline,owners,actual,compat,observed,selected,fs):
 fields=["schema_version","claims"];req(doc,fields,fs);closed(doc,fields,fs)
 if not isinstance(doc,dict) or doc.get("schema_version")!="claims/v1":add(fs,"E_SCHEMA_VERSION","/schema_version","expected claims/v1");return
 rows=doc.get("claims");
 if not isinstance(rows,list):add(fs,"E_TYPE","/claims","expected array");return
 if index is None:add(fs,"E_CLAIM_INDEX_REQUIRED","/","absent claim index cannot imply verified evidence");idx={}
 else:
  req(index,["schema_version","entries"],fs,"/index");closed(index,["schema_version","entries"],fs,"/index")
  if index.get("schema_version")!="claim-index/v1":add(fs,"E_SCHEMA_VERSION","/index/schema_version","expected claim-index/v1")
  entries=index.get("entries",[]);idx={e.get("claim_id"):e for e in entries if isinstance(e,dict) and isinstance(e.get("claim_id"),str)}
  if len(idx)!=len(entries):add(fs,"E_INDEX_DUPLICATE","/index/entries","index claim IDs must be unique scalar strings")
 if baseline is None:add(fs,"E_INDEX_BASELINE_REQUIRED","/index","trusted immutable index baseline is required")
 elif index != baseline:add(fs,"E_INDEX_REWRITTEN","/index","claim index differs from trusted baseline")
 claim_owners=owners.get("claim_owners",{}) if isinstance(owners,dict) else {}
 predicate_owners=owners.get("predicate_owners",{}) if isinstance(owners,dict) else {}
 if owners is None:add(fs,"E_OWNER_REGISTRY_REQUIRED","/owners","canonical owner registry is required")
 predicates={}
 if compat is not None:
  req(compat,["schema_version","predicates"],fs,"/compatibility")
  if compat.get("schema_version")!="claim-compatibility/v1":add(fs,"E_SCHEMA_VERSION","/compatibility/schema_version","expected claim-compatibility/v1")
  predicates={x.get("predicate_id"):x for x in compat.get("predicates",[]) if isinstance(x,dict) and isinstance(x.get("predicate_id"),str)}
 seen=set();superseded=set()
 for r in rows:
  if isinstance(r,dict):superseded.update(x for x in r.get("supersedes",[]) if isinstance(x,str))
 for i,r in enumerate(rows):
  p=f"/claims/{i}";names=["claim_id","owner_bead","scenario_ids","bindings","applicability","supersedes","rerun"]
  if not req(r,names,fs,p):continue
  closed(r,names,fs,p);cid=r.get("claim_id")
  if not isinstance(cid,str) or not cid.startswith("CLAIM-") or not SID.fullmatch(cid):add(fs,"E_ID_INVALID",p+"/claim_id","invalid CLAIM ID")
  elif cid in seen:add(fs,"E_DUPLICATE_ID",p+"/claim_id","duplicate claim ID")
  else:seen.add(cid)
  if not isinstance(r.get("owner_bead"),str) or not BEAD.fullmatch(r["owner_bead"]):add(fs,"E_OWNER_INVALID",p+"/owner_bead","invalid owner Bead")
  ids(r.get("scenario_ids"),fs,p+"/scenario_ids","SCN-");b=r.get("bindings");req(b,BINDINGS,fs,p+"/bindings");closed(b,BINDINGS,fs,p+"/bindings");digest_fields(b,BINDINGS,fs,p+"/bindings")
  ids(r.get("supersedes"),fs,p+"/supersedes","CLAIM-",False);rr=r.get("rerun");req(rr,["owner_bead","command"],fs,p+"/rerun");closed(rr,["owner_bead","command"],fs,p+"/rerun")
  if isinstance(rr,dict) and rr.get("owner_bead")!=r.get("owner_bead"):add(fs,"E_RERUN_OWNER",p+"/rerun/owner_bead","rerun owner must be claim owner")
  for old in r.get("supersedes",[]) if isinstance(r.get("supersedes"),list) else []:
   prior=next((x for x in rows if isinstance(x,dict) and x.get("claim_id")==old),None)
   if not prior:add(fs,"E_SUPERSESSION_MISSING",p+"/supersedes","superseded claim must remain in immutable history")
   elif prior.get("owner_bead")!=r.get("owner_bead"):add(fs,"E_SUPERSESSION_OWNER",p+"/supersedes","supersession must retain canonical owner")
  if selected==cid and cid in superseded:add(fs,"E_CLAIM_SUPERSEDED",p+"/claim_id","selected superseded evidence cannot prove current semantics",r.get("owner_bead",OWNER))
  entry=idx.get(cid)
  if not entry:add(fs,"E_CLAIM_UNINDEXED",p+"/claim_id","claim absent from immutable index",r.get("owner_bead",OWNER))
  else:
   expected=dg(json.dumps(r,sort_keys=True,separators=(",",":")).encode())
   canonical=claim_owners.get(cid)
   if not canonical or canonical!=r.get("owner_bead") or entry.get("owner_bead")!=canonical:add(fs,"E_CLAIM_OWNER_FORGED",p+"/owner_bead","claim owner differs from canonical owner registry",canonical or OWNER)
   if entry.get("claim_sha256")!=expected:add(fs,"E_CLAIM_HASH",p+"/claim_id","claim content differs from immutable index",entry.get("owner_bead",OWNER))
   if entry.get("status") not in ("current","superseded"):add(fs,"E_INDEX_STATUS",p+"/claim_id","index status must be current or superseded")
  app=r.get("applicability");req(app,["mode","freshness"],fs,p+"/applicability")
  if isinstance(app,dict):
   mode=app.get("mode");fresh=app.get("freshness")
   if fresh=="fresh_until":
    until=parse_time(app.get("fresh_until"));now=parse_time(observed)
    if not until or not now:add(fs,"E_TIME_INVALID",p+"/applicability/fresh_until","deterministic timestamps required")
    elif now>until:add(fs,"E_CLAIM_STALE",p+"/applicability/fresh_until","freshness expired",r.get("owner_bead",OWNER))
   elif fresh!="timeless":add(fs,"E_FRESHNESS",p+"/applicability/freshness","expected timeless or fresh_until")
   if mode=="exact":
    if actual is None:add(fs,"E_ACTUAL_INPUT_REQUIRED",p+"/bindings","exact applicability requires declared actual inputs")
    else:
     for n in BINDINGS:
      if b.get(n)!=actual.get(n):add(fs,"E_CLAIM_INPUT_MISMATCH",p+f"/bindings/{n}",f"invalidating input {n}; rerun owner {r.get('owner_bead')}",r.get("owner_bead",OWNER))
   elif mode=="compatible":
    pred=predicates.get(app.get("predicate_id"))
    if not pred or predicate_owners.get(app.get("predicate_id"))!=pred.get("owner_bead"):add(fs,"E_COMPATIBILITY_UNOWNED",p+"/applicability/predicate_id","compatible range lacks canonical predicate owner")
    elif pred.get("owner_bead")!=r.get("owner_bead"):add(fs,"E_COMPATIBILITY_OWNER",p+"/applicability/predicate_id","predicate owner differs from claim owner",pred.get("owner_bead",OWNER))
    elif actual is None:add(fs,"E_ACTUAL_INPUT_REQUIRED",p+"/bindings","compatibility requires actual inputs")
    else:
     allowed=pred.get("allowed_values",{})
     for n in BINDINGS:
      if actual.get(n)!=b.get(n) and actual.get(n) not in allowed.get(n,[]):add(fs,"E_CLAIM_INPUT_MISMATCH",p+f"/bindings/{n}",f"invalidating input {n}; rerun owner {r.get('owner_bead')}",r.get("owner_bead",OWNER))
   else:add(fs,"E_APPLICABILITY_MODE",p+"/applicability/mode","expected exact or compatible")
 if actual is not None and actual.get("git_ancestry") is False:add(fs,"E_HISTORY_REWRITTEN","/actual/git_ancestry","claim Git history is not ancestral")
 secret_check(doc,fs)

def validate_findings(rows,baseline,fs):
 if baseline is None:add(fs,"E_FINDING_BASELINE_REQUIRED","/","trusted append-only baseline is required")
 elif not isinstance(rows,list) or rows[:len(baseline)]!=baseline:add(fs,"E_FINDING_REWRITE","/","candidate findings must preserve the trusted baseline as an exact prefix")
 seen=set();superseded=set()
 for r in rows if isinstance(rows,list) else []:
  if isinstance(r,dict):superseded.update(x for x in r.get("supersedes",[]) if isinstance(x,str))
 for i,r in enumerate(rows if isinstance(rows,list) else []):
  p=f"/lines/{i}";names=["schema_version","finding_id","kind","observation","hypothesis","affected_ids","environment_digest","input_digest","failed_approach","owner_bead","status","resolution","invalidation_trigger","supersedes"]
  if not req(r,names,fs,p):continue
  closed(r,names,fs,p)
  if r.get("schema_version")!="finding/v1":add(fs,"E_SCHEMA_VERSION",p+"/schema_version","expected finding/v1")
  fid=r.get("finding_id")
  if not isinstance(fid,str) or not fid.startswith("FINDING-") or not SID.fullmatch(fid):add(fs,"E_ID_INVALID",p+"/finding_id","invalid finding ID")
  elif fid in seen:add(fs,"E_FINDING_REWRITE",p+"/finding_id","append-only finding IDs cannot be rewritten")
  else:seen.add(fid)
  if r.get("kind") not in ("verified_observation","hypothesis"):add(fs,"E_FINDING_KIND",p+"/kind","kind must distinguish observation from hypothesis")
  if r.get("kind")=="hypothesis" and r.get("observation"):add(fs,"E_HYPOTHESIS_PROMOTION",p+"/observation","hypothesis cannot be recorded as verified observation")
  if r.get("kind")=="verified_observation" and r.get("hypothesis"):add(fs,"E_OBSERVATION_MIXED",p+"/hypothesis","verified observation cannot contain a hypothesis")
  ids(r.get("affected_ids"),fs,p+"/affected_ids",nonempty=False);ids(r.get("supersedes"),fs,p+"/supersedes","FINDING-",False)
  digest_fields(r,["environment_digest","input_digest"],fs,p)
  if r.get("status") not in ("open","resolved","invalidated"):add(fs,"E_STATUS",p+"/status","invalid finding status")
  if r.get("status") in ("resolved","invalidated") and not r.get("resolution"):add(fs,"E_RESOLUTION_REQUIRED",p+"/resolution","terminal finding requires resolution")
 for target in superseded:
  if target not in seen:add(fs,"E_SUPERSESSION_MISSING","/","superseded finding must remain in append-only log")
 secret_check(rows,fs)

def validate_handoff(o,fs):
 names=["schema_version","bead_id","world_state_digest","base_sha","head_sha","dirty","changed_paths","touched_ids","checks","facts","observations","hypotheses","intents","risks","unsafe_repeats","next_safe_command","log_references","redaction"]
 req(o,names,fs);closed(o,names,fs)
 if not isinstance(o,dict):return
 if o.get("schema_version")!="handoff/v1":add(fs,"E_SCHEMA_VERSION","/schema_version","expected handoff/v1")
 digest_fields(o,["world_state_digest"],fs,"")
 for n in ("base_sha","head_sha"):
  if not isinstance(o.get(n),str) or not GIT.fullmatch(o[n]):add(fs,"E_GIT_SHA","/"+n,"expected full Git SHA")
 dirty=o.get("dirty");req(dirty,["is_dirty","paths"],fs,"/dirty");closed(dirty,["is_dirty","paths"],fs,"/dirty")
 if isinstance(dirty,dict) and bool(dirty.get("paths"))!=bool(dirty.get("is_dirty")):add(fs,"E_DIRTY_STATE","/dirty","dirty flag and paths disagree")
 checks=o.get("checks");req(checks,["completed","failed","stale"],fs,"/checks");closed(checks,["completed","failed","stale"],fs,"/checks")
 if isinstance(checks,dict):
  expected={"completed":"pass","failed":"fail","stale":"stale"}
  for bucket,want in expected.items():
   values=checks.get(bucket)
   if not isinstance(values,list):add(fs,"E_TYPE",f"/checks/{bucket}","expected array");continue
   for i,item in enumerate(values):
    cp=f"/checks/{bucket}/{i}";req(item,["command","result","digest"],fs,cp);closed(item,["command","result","digest"],fs,cp)
    if isinstance(item,dict):
     if item.get("result")!=want:add(fs,"E_CHECK_BUCKET",cp+"/result",f"{bucket} check must have result {want}")
     digest_fields(item,["digest"],fs,cp)
 for n in ("facts","observations","hypotheses","risks","unsafe_repeats","changed_paths"):
  if not isinstance(o.get(n),list):add(fs,"E_TYPE","/"+n,"expected array")
 ids(o.get("touched_ids"),fs,"/touched_ids",nonempty=False)
 if not isinstance(o.get("next_safe_command"),str) or not o["next_safe_command"].strip():add(fs,"E_NEXT_COMMAND","/next_safe_command","exact next safe command required")
 if o.get("hypotheses") and any(x in o.get("facts",[]) for x in o["hypotheses"]):add(fs,"E_HYPOTHESIS_PROMOTION","/facts","hypothesis cannot also be a fact")
 intents=o.get("intents");req(intents,["active","ambiguous"],fs,"/intents");closed(intents,["active","ambiguous"],fs,"/intents")
 if isinstance(intents,dict) and intents.get("ambiguous") and not o.get("unsafe_repeats"):add(fs,"E_UNSAFE_REPEAT_REQUIRED","/unsafe_repeats","ambiguous effects require unsafe-repeat commands")
 for i,r in enumerate(o.get("log_references",[]) if isinstance(o.get("log_references"),list) else []):
  req(r,["path","sha256"],fs,f"/log_references/{i}");digest_fields(r,["sha256"],fs,f"/log_references/{i}")
 red=o.get("redaction");req(red,["checked","secrets_found"],fs,"/redaction")
 if isinstance(red,dict) and (red.get("checked") is not True or red.get("secrets_found")!=0):add(fs,"E_REDACTION","/redaction","redaction must pass with zero secrets")
 secret_check(o,fs)

def main():
 p=argparse.ArgumentParser();p.add_argument("kind",choices=["claims","findings","handoff"]);p.add_argument("input");p.add_argument("--index");p.add_argument("--baseline-index");p.add_argument("--owners");p.add_argument("--claim-id");p.add_argument("--baseline");p.add_argument("--actual");p.add_argument("--compatibility");p.add_argument("--observed-at",default="2026-01-01T00:00:00Z");a=p.parse_args();fs=[]
 doc,raw=load(a.input,fs,a.kind=="findings")
 if doc is not None:
  if a.kind=="claims":
   idx,ir=load(a.index,fs) if a.index else (None,b"");base,br=load(a.baseline_index,fs) if a.baseline_index else (None,b"");owners,orr=load(a.owners,fs) if a.owners else (None,b"");act,ar=load(a.actual,fs) if a.actual else (None,b"");comp,cr=load(a.compatibility,fs) if a.compatibility else (None,b"");raw+=ir+br+orr+ar+cr;validate_claims(doc,idx,base,owners,act,comp,a.observed_at,a.claim_id,fs)
  elif a.kind=="findings":
   baseline,br=load(a.baseline,fs,True) if a.baseline else (None,b"");raw+=br;validate_findings(doc,baseline,fs)
  else:validate_handoff(doc,fs)
 fs.sort(key=lambda x:(x["pointer"],x["code"],x["message"]));out={"schema_version":"validation-result/v1","validator_version":VERSION,"owner_bead":OWNER,"status":"fail" if fs else "pass","input_sha256":dg(raw),"git_commit":subprocess.run(["git","rev-parse","HEAD"],cwd=ROOT,text=True,capture_output=True).stdout.strip(),"findings":fs};print(json.dumps(out,sort_keys=True,separators=(",",":")));return bool(fs)
if __name__=="__main__":raise SystemExit(main())
