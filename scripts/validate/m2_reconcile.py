#!/usr/bin/env python3
import json,re
from pathlib import Path
root=Path(__file__).resolve().parents[2]
src=(root/'src/m2_reconcile.rs').read_text()
required=['compatible_requests_durable_position_and_persists_before_ready','fresh_journal_with_preexisting_slot_is_ambiguous_without_provenance','identity_mismatch_blocks','server_ahead_requires_reseed','invalid_slot_and_missing_wal_require_reseed','creation_floor_null_or_equal_never_becomes_durable_progress','creation_floor_compound_safety_precedes_floor_resume','ambiguous_bootstrap_is_transitioned_and_persisted','external_ahead_blocks','archive_hook_blocks_without_adopting','startup_blocks_structurally_valid_payload_checksum_corruption','reports_are_bounded_and_read_only']
findings=[f'missing test {name}' for name in required if not re.search(r'fn\s+'+name+r'\s*\(',src)]
for symbol in ['ArchiveReconciler','SLOT_INVALID_WAL_REMOVED','RESUME_WAL_STATUS_UNAVAILABLE','journal_report','recover_report','startup_reconciliations','drop(reader)']:
 if symbol not in src: findings.append(f'missing boundary {symbol}')
for script in ['scripts/e2e/m2_reconcile.sh','scripts/faults/m2_reconcile.sh']:
 if 'TMPDIR=/var/tmp' not in (root/script).read_text(): findings.append(f'{script} does not pin TMPDIR')
print(json.dumps({'schema_version':'validation-result/v1','validator':'m2-reconcile/v1','valid':not findings,'findings':findings},sort_keys=True,separators=(',',':')))
raise SystemExit(bool(findings))
