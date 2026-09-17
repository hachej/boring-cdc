#!/usr/bin/env python3
import json,re,sys
from pathlib import Path
R=Path(__file__).resolve().parents[2];errors=[];src=(R/'src/m2_jsonl.rs').read_text();cases=json.loads((R/'contracts/m2/jsonl-cases.json').read_text())
for c in cases['cases']:
 if not re.search(r'fn\s+'+re.escape(c['test'])+r'\s*\(',src):errors.append('missing test '+c['id'])
for token in ['AfterIntent','AfterWrite','AfterFileSync','AfterDirectorySync','AfterRename','AfterParentSync','BeforeMarker','AfterMarkerWrite','AfterMarkerSync','BeforeCheckpoint','ArchiveFailureAdapter','read_complete_range','SEGMENT_READY']:
 if token not in src:errors.append('missing '+token)
sel={'e2e':['SCN-M2-JSONL-COMPONENT'],'fault':['SCN-M2-JSONL-FAULTS'],'all':['SCN-M2-JSONL-COMPONENT','SCN-M2-JSONL-FAULTS']}.get(sys.argv[1] if len(sys.argv)>1 else 'all',[])
for s in sel:
 p=R/'artifacts/boring-cdc-m2-jsonl'/s/'jsonl-component-v1';m=json.loads((p/'evidence.json').read_text());events=[json.loads(x) for x in (p/'logs/boring-cdc.jsonl').read_text().splitlines()]
 if m['result']['status']!='pass' or not m['tier_proof']['deterministic_rerun']:errors.append(s+' incomplete')
 if [e['case_event_seq'] for e in events]!=list(range(1,len(events)+1)):errors.append(s+' log sequence')
 if any(x in (p/'logs/boring-cdc.jsonl').read_text().lower() for x in ['postgres://','password=','/home/','canonical_key']):errors.append(s+' redaction')
print(json.dumps({'schema_version':'validation-result/v1','validator':'m2-jsonl/v1','valid':not errors,'findings':errors},sort_keys=True,separators=(',',':')));raise SystemExit(bool(errors))
