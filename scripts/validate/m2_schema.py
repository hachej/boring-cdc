#!/usr/bin/env python3
import json,re,sys
from pathlib import Path
root=Path(__file__).resolve().parents[2]
src=(root/'src/m2_schema.rs').read_text()
contract=json.loads((root/'contracts/m2/schema-cases.json').read_text())
tables={'journal_events','source_transactions','source_state','runtime_ownership','operator_command_requests','relation_schemas','destinations','destination_checkpoints','backfill_runs','backfill_generations','backfill_chunks','bootstrap_intents','bootstrap_imports','durable_capture_fences','bootstrap_anchors','reseed_intents','destination_generation_leases','destination_promotion_intents','clickhouse_batch_intents','archive_generations','archive_segment_intents','archive_segments','archive_generation_markers','processing_failures','destination_audits','condition_hysteresis','alerts','schema_migrations'}
found=set(re.findall(r'CREATE TABLE(?: IF NOT EXISTS)? ([a-z_]+)',src))
found.discard('forbidden')
errors=[]
auxiliary={'audit_coverage_subranges'}
if not tables.issubset(found) or found-tables!=auxiliary: errors.append(f'table inventory mismatch missing={sorted(tables-found)} extra={sorted(found-tables)}')
ids=[c['id'] for c in contract['cases']]
if len(ids)!=len(set(ids)): errors.append('duplicate scenario IDs')
for case in contract['cases']:
 if not re.search(r'fn\s+'+re.escape(case['test'])+r'\s*\(',src): errors.append(f"missing test {case['test']}")
for version in (1,2,3):
 migration=re.search(rf'(?:pub )?const MIGRATION_{version}: &str = r#"(.*?)"#;',src,re.S).group(1)
 declared=re.search(rf'const MIGRATION_{version}_CHECKSUM: &str\s*=\s*"sha256:([0-9a-f]{{64}})";',src).group(1)
 import hashlib
 if hashlib.sha256(migration.encode()).hexdigest()!=declared: errors.append(f'migration {version} checksum mismatch')
for literal in ('WRITER_BUSY_TIMEOUT','READER_MAX_AGE','READER_MAX_ROWS'):
 pos=src.find('pub const '+literal)
 if pos<0 or 'M0-PROVISIONAL: boring-cdc-m2-schema' not in src[max(0,pos-100):pos]: errors.append(f'missing provisional marker for {literal}')
print(json.dumps({'schema_version':'validation-result/v1','validator':'m2-schema/v1','valid':not errors,'findings':errors},sort_keys=True,separators=(',',':')))
raise SystemExit(bool(errors))
