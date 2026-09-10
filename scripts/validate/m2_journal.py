#!/usr/bin/env python3
import json,re,sys
from pathlib import Path
root=Path(__file__).resolve().parents[2]; src=(root/'src/m2_journal.rs').read_text(); contract=json.loads((root/'contracts/m2/journal-cases.json').read_text()); errors=[]
ids=[c['id'] for c in contract['cases']]
if len(ids)!=len(set(ids)): errors.append('duplicate scenario IDs')
for case in contract['cases']:
 if not re.search(r'fn\s+'+re.escape(case['test'])+r'\s*\(',src): errors.append('missing test '+case['test'])
for symbol in ('JournalStore','DurableCommit','CommitFault','read_complete_range','CapturePriorityScheduler','BusyBoundExceededAfterCommit'):
 if symbol not in src: errors.append('missing journal boundary '+symbol)
if 'WRITER_BUSY_TIMEOUT: Duration = Duration::from_secs(5)' not in (root/'src/m2_schema.rs').read_text(): errors.append('confirmed sqlite writer timeout changed')
for scenario in ('SCN-M2-JOURNAL-COMPONENT','SCN-M2-JOURNAL-CRASH-BOUNDARY'):
 packet=root/'artifacts/boring-cdc-m2-journal'/scenario/'journal-component-v1'
 if packet.exists():
  events=[json.loads(x) for x in (packet/'logs/boring-cdc.jsonl').read_text().splitlines()]
  for i,event in enumerate(events,1):
   for field in ('schema_version','case_event_seq','bead_id','scenario_id','correlation_id','run_id','capture_epoch','component','phase','outcome','config_fingerprint'):
    if field not in event or event[field] in ('',None): errors.append(f'{scenario} event {i} missing {field}')
   forbidden=json.dumps(event).lower()
   for token in ('password=','postgres://','postgresql://','canonical_key','dsn'):
    if token in forbidden: errors.append(f'{scenario} event {i} contains forbidden {token}')
for script,mode in [('scripts/e2e/m2_journal.sh','e2e'),('scripts/faults/m2_journal.sh','fault')]:
 text=(root/script).read_text()
 if text.count(f'm2_journal_component.py {mode}')!=2: errors.append(f'{script} must run component probe twice')
print(json.dumps({'schema_version':'validation-result/v1','validator':'m2-journal/v1','valid':not errors,'findings':errors},sort_keys=True,separators=(',',':')))
raise SystemExit(bool(errors))
