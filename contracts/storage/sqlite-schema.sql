-- storage-contract/v2: materialized SQLite schema after M2 migrations 1..17.
-- Runtime migration checksums and upgrade path remain in src/m2_schema.rs.
-- This file describes a fresh resulting schema; do not apply it as an upgrade.

CREATE TABLE alerts(alert_id TEXT PRIMARY KEY, condition_id TEXT NOT NULL, failure_id TEXT REFERENCES processing_failures(failure_id), state TEXT NOT NULL CHECK(state IN ('active','acknowledged','cleared')), opened_at TEXT NOT NULL, cleared_at TEXT, evidence_digest TEXT);

CREATE TABLE archive_generation_markers(marker_id TEXT PRIMARY KEY, generation_id TEXT NOT NULL REFERENCES archive_generations(generation_id), promotion_fence INTEGER NOT NULL CHECK(promotion_fence>0), intent_id TEXT NOT NULL REFERENCES destination_promotion_intents(intent_id), marker_digest TEXT NOT NULL, UNIQUE(generation_id,promotion_fence), UNIQUE(promotion_fence,marker_digest));

CREATE TABLE archive_generations(generation_id TEXT PRIMARY KEY, destination_id TEXT NOT NULL REFERENCES destinations(destination_id), capture_epoch TEXT NOT NULL, generation INTEGER NOT NULL, anchor_id TEXT REFERENCES bootstrap_anchors(anchor_id), state TEXT NOT NULL CHECK(state IN ('candidate','live','retirement_eligible','retired','invalidated')), UNIQUE(destination_id,capture_epoch,generation));

CREATE TABLE archive_segment_intents(intent_id TEXT PRIMARY KEY, generation_id TEXT NOT NULL REFERENCES archive_generations(generation_id), first_seq INTEGER NOT NULL, last_seq INTEGER NOT NULL CHECK(last_seq>=first_seq), selection_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('selected','writing','published','failed')), UNIQUE(generation_id,first_seq,last_seq));

CREATE TABLE archive_segments(segment_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL UNIQUE REFERENCES archive_segment_intents(intent_id), manifest_digest TEXT NOT NULL, ready_marker_digest TEXT NOT NULL, first_seq INTEGER NOT NULL, last_seq INTEGER NOT NULL CHECK(last_seq>=first_seq));

CREATE TABLE audit_coverage_subranges(
 audit_id TEXT NOT NULL REFERENCES destination_audits(audit_id) ON DELETE CASCADE,
 coverage_kind TEXT NOT NULL CHECK(coverage_kind IN ('journal_verified','self_consistent')),
 start_seq INTEGER NOT NULL CHECK(start_seq>=0), end_seq INTEGER NOT NULL CHECK(end_seq>=start_seq),
 fresh_until TEXT NOT NULL, evidence_digest TEXT NOT NULL,
 PRIMARY KEY(audit_id,coverage_kind,start_seq,end_seq));

CREATE TABLE audit_incarnation_counter(
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 last_incarnation INTEGER NOT NULL CHECK(last_incarnation>=0));

CREATE TABLE backfill_chunks(chunk_id TEXT PRIMARY KEY, generation_id TEXT NOT NULL REFERENCES backfill_generations(generation_id), range_start BLOB NOT NULL, range_end BLOB NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending','complete','invalidated')), completed_seq INTEGER, checksum TEXT, CHECK(state!='complete' OR (completed_seq IS NOT NULL AND checksum IS NOT NULL)));

CREATE TABLE backfill_generations(generation_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES backfill_runs(run_id), generation INTEGER NOT NULL CHECK(generation>0), intended_fence_nonce TEXT UNIQUE, state TEXT NOT NULL CHECK(state IN ('prepared','copying','fencing','complete','invalidated')), UNIQUE(run_id,generation));

CREATE TABLE backfill_runs(run_id TEXT PRIMARY KEY, destination_id TEXT NOT NULL REFERENCES destinations(destination_id), capture_epoch TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('prepared','running','paused','complete','invalidated')), revision INTEGER NOT NULL DEFAULT 0);

CREATE TABLE bootstrap_anchors(anchor_id TEXT PRIMARY KEY, capture_epoch TEXT NOT NULL, generation INTEGER NOT NULL, lower_stitch_lsn TEXT CHECK(lower_stitch_lsn IS NULL OR (length(lower_stitch_lsn)=16 AND lower_stitch_lsn NOT GLOB '*[^0-9A-F]*')), start_seq INTEGER NOT NULL CHECK(start_seq>=0), snapshot_boundary_lsn TEXT NOT NULL CHECK(length(snapshot_boundary_lsn)=16 AND snapshot_boundary_lsn NOT GLOB '*[^0-9A-F]*'), snapshot_complete_seq INTEGER, post_copy_fence_nonce TEXT, post_copy_fence_lsn TEXT CHECK(post_copy_fence_lsn IS NULL OR (length(post_copy_fence_lsn)=16 AND post_copy_fence_lsn NOT GLOB '*[^0-9A-F]*')), post_copy_fence_seq INTEGER, table_set_fingerprint TEXT NOT NULL, snapshot_schema_fingerprints TEXT NOT NULL CHECK(json_valid(snapshot_schema_fingerprints) AND json_type(snapshot_schema_fingerprints)='array' AND json_array_length(snapshot_schema_fingerprints)>0), state TEXT NOT NULL CHECK(state IN ('building','complete','expired','invalidated')), expires_at TEXT NOT NULL, generation_id TEXT, bootstrap_intent_id TEXT, CHECK(state!='complete' OR (snapshot_complete_seq IS NOT NULL AND post_copy_fence_nonce IS NOT NULL AND post_copy_fence_lsn IS NOT NULL AND post_copy_fence_seq IS NOT NULL)), UNIQUE(capture_epoch,generation));

CREATE TABLE bootstrap_imports(import_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL REFERENCES bootstrap_intents(intent_id), destination_id TEXT NOT NULL REFERENCES destinations(destination_id), state TEXT NOT NULL CHECK(state IN ('prepared','importing','acknowledged','failed','invalidated')), revision INTEGER NOT NULL DEFAULT 0, UNIQUE(intent_id,destination_id));

CREATE TABLE bootstrap_intents(
 intent_id TEXT PRIMARY KEY, capture_epoch TEXT NOT NULL, source_system_id TEXT NOT NULL, database_id TEXT NOT NULL, slot_name TEXT NOT NULL,
 creation_floor_lsn TEXT CHECK(creation_floor_lsn IS NULL OR (length(creation_floor_lsn)=16 AND creation_floor_lsn NOT GLOB '*[^0-9A-F]*')),
 state TEXT NOT NULL CHECK(state IN ('prepared','remote_slot_unknown','slot_created','copying','complete','invalidated','aborted')),
 revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0), created_at TEXT NOT NULL,
 UNIQUE(intent_id,capture_epoch,source_system_id,database_id,slot_name,creation_floor_lsn));

CREATE TABLE capture_configuration_receipts(
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 capture_epoch TEXT NOT NULL, runtime_fingerprint TEXT NOT NULL,
 non_limit_fingerprint TEXT NOT NULL, limit_fingerprint TEXT NOT NULL,
 revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0));

CREATE TABLE capture_health_observations(
 run_id TEXT PRIMARY KEY REFERENCES startup_reconciliations(run_id),
 capture_epoch TEXT NOT NULL,
 last_heartbeat_seq INTEGER NOT NULL CHECK(last_heartbeat_seq>0),
 last_heartbeat_end_lsn TEXT NOT NULL CHECK(length(last_heartbeat_end_lsn)=16 AND last_heartbeat_end_lsn NOT GLOB '*[^0-9A-F]*'),
 observed_at_unix_seconds INTEGER NOT NULL CHECK(observed_at_unix_seconds>=0));

CREATE TABLE clickhouse_batch_intents(intent_id TEXT PRIMARY KEY, destination_id TEXT NOT NULL REFERENCES destinations(destination_id), capture_epoch TEXT NOT NULL, generation INTEGER NOT NULL, first_seq INTEGER NOT NULL, last_seq INTEGER NOT NULL CHECK(last_seq>=first_seq), payload_checksum TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('prepared','dispatched','verified','failed')), UNIQUE(destination_id,capture_epoch,generation,first_seq,last_seq));

CREATE TABLE condition_hysteresis(condition_id TEXT PRIMARY KEY, entered_at TEXT, last_observed_at TEXT NOT NULL, cleared_at TEXT);

CREATE TABLE destination_audits(audit_id TEXT PRIMARY KEY, destination_id TEXT NOT NULL REFERENCES destinations(destination_id), configuration_fingerprint TEXT NOT NULL, capture_epoch TEXT NOT NULL, generation INTEGER NOT NULL, round_target_seq INTEGER NOT NULL, round_identity_digest TEXT NOT NULL, journal_cursor_seq INTEGER NOT NULL, journal_verified_start_seq INTEGER, journal_verified_end_seq INTEGER, self_cursor_seq INTEGER, self_consistent_start_seq INTEGER, self_consistent_end_seq INTEGER, budget_bytes_used INTEGER NOT NULL DEFAULT 0, budget_events_used INTEGER NOT NULL DEFAULT 0, budget_ms_used INTEGER NOT NULL DEFAULT 0, freshness_window_started_at TEXT NOT NULL, freshness_expires_at TEXT NOT NULL, contract_digest TEXT NOT NULL, evidence_digest TEXT, first_mismatch TEXT, revision INTEGER NOT NULL DEFAULT 0, retained_history_start_seq INTEGER CHECK(retained_history_start_seq IS NULL OR retained_history_start_seq>=0), unverifiable_before_seq INTEGER CHECK(unverifiable_before_seq IS NULL OR unverifiable_before_seq>=0), incarnation INTEGER NOT NULL DEFAULT 0
 CHECK(incarnation>=0),
 CHECK(round_target_seq>=0 AND journal_cursor_seq>=0 AND journal_cursor_seq<=round_target_seq AND self_cursor_seq>=0 AND self_cursor_seq<=round_target_seq),
 CHECK(budget_bytes_used>=0 AND budget_events_used>=0 AND budget_ms_used>=0),
 CHECK((journal_verified_start_seq IS NULL)=(journal_verified_end_seq IS NULL)), CHECK(journal_verified_end_seq IS NULL OR (journal_verified_end_seq>=journal_verified_start_seq AND journal_verified_end_seq<=round_target_seq)),
 CHECK((self_consistent_start_seq IS NULL)=(self_consistent_end_seq IS NULL)), CHECK(self_consistent_end_seq IS NULL OR (self_consistent_end_seq>=self_consistent_start_seq AND self_consistent_end_seq<=round_target_seq)),
 CHECK(freshness_expires_at>freshness_window_started_at));

CREATE TABLE destination_checkpoints(
 destination_id TEXT PRIMARY KEY REFERENCES destinations(destination_id), capture_epoch TEXT NOT NULL, anchor_id TEXT,
 configuration_fingerprint TEXT NOT NULL, generation INTEGER NOT NULL CHECK(generation>0), complete_transaction_id TEXT NOT NULL REFERENCES source_transactions(transaction_id),
 journal_seq INTEGER NOT NULL, current_failure_id TEXT, revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0),
 FOREIGN KEY(complete_transaction_id,capture_epoch,journal_seq) REFERENCES source_transactions(transaction_id,capture_epoch,last_seq), FOREIGN KEY(current_failure_id,destination_id) REFERENCES processing_failures(failure_id,destination_id));

CREATE TABLE destination_current_audits(
 destination_id TEXT PRIMARY KEY REFERENCES destinations(destination_id) ON DELETE CASCADE,
 audit_id TEXT NOT NULL UNIQUE REFERENCES destination_audits(audit_id) ON DELETE CASCADE,
 incarnation INTEGER NOT NULL CHECK(incarnation>0));

CREATE TABLE destination_generation_leases(lease_id TEXT PRIMARY KEY, destination_id TEXT NOT NULL REFERENCES destinations(destination_id), capture_epoch TEXT NOT NULL, anchor_id TEXT REFERENCES bootstrap_anchors(anchor_id), generation INTEGER NOT NULL, configuration_fingerprint TEXT NOT NULL, run_id TEXT NOT NULL, expires_mono_ms INTEGER NOT NULL, state TEXT NOT NULL CHECK(state IN ('held','fenced','expired','released')), revision INTEGER NOT NULL DEFAULT 0, UNIQUE(destination_id,capture_epoch,generation));

CREATE TABLE destination_promotion_intents(intent_id TEXT PRIMARY KEY, destination_id TEXT NOT NULL REFERENCES destinations(destination_id), capture_epoch TEXT NOT NULL, old_generation INTEGER, candidate_generation INTEGER NOT NULL, anchor_id TEXT NOT NULL REFERENCES bootstrap_anchors(anchor_id), configuration_fingerprint TEXT NOT NULL, promotion_fence INTEGER NOT NULL CHECK(promotion_fence>0), expected_selector_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('prepared','old_leases_fenced','candidate_verified','switch_pending','switched','verified','retirement_eligible','retired','promotion_recovery_required')), revision INTEGER NOT NULL DEFAULT 0, UNIQUE(destination_id,promotion_fence));

CREATE TABLE destinations(
 destination_id TEXT PRIMARY KEY, kind TEXT NOT NULL CHECK(kind IN ('clickhouse','archive')), configuration_fingerprint TEXT NOT NULL,
 capture_epoch TEXT NOT NULL, generation INTEGER NOT NULL CHECK(generation>0), highest_external_fence INTEGER NOT NULL DEFAULT 0 CHECK(highest_external_fence>=0),
 adopted_external_fence_at TEXT, adopted_external_fence_evidence TEXT, current_failure_id TEXT, revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0),
 FOREIGN KEY(current_failure_id,destination_id) REFERENCES processing_failures(failure_id,destination_id));

CREATE TABLE durable_capture_fences(fence_id TEXT PRIMARY KEY, capture_epoch TEXT NOT NULL, generation INTEGER NOT NULL, nonce TEXT NOT NULL, transaction_id TEXT NOT NULL REFERENCES source_transactions(transaction_id), post_copy_fence_lsn TEXT NOT NULL CHECK(length(post_copy_fence_lsn)=16 AND post_copy_fence_lsn NOT GLOB '*[^0-9A-F]*'), post_copy_fence_seq INTEGER NOT NULL, first_proof INTEGER NOT NULL CHECK(first_proof IN (0,1)), FOREIGN KEY(transaction_id,capture_epoch,post_copy_fence_seq) REFERENCES source_transactions(transaction_id,capture_epoch,last_seq), UNIQUE(capture_epoch,generation,nonce,transaction_id));

CREATE TABLE journal_events(
 journal_seq INTEGER PRIMARY KEY CHECK(journal_seq>0), event_id TEXT NOT NULL UNIQUE,
 transaction_id TEXT NOT NULL REFERENCES source_transactions(transaction_id), transaction_ordinal INTEGER NOT NULL CHECK(transaction_ordinal>=0),
 capture_epoch TEXT NOT NULL, relation_schema_fingerprint TEXT REFERENCES relation_schemas(schema_fingerprint),
 control_kind TEXT CHECK(control_kind IN ('heartbeat','capture_fence') OR control_kind IS NULL), payload BLOB NOT NULL, payload_hash TEXT NOT NULL, source_row_ordinal INTEGER NOT NULL DEFAULT 0
 CHECK(source_row_ordinal>=0 AND source_row_ordinal<=4294967295), source_mutation_ordinal INTEGER NOT NULL DEFAULT 0
 CHECK(source_mutation_ordinal IN (0,1)), canonical_payload BLOB, canonical_payload_hash TEXT
 CHECK(canonical_payload_hash IS NULL OR
       (length(canonical_payload_hash)=64 AND canonical_payload_hash NOT GLOB '*[^0-9a-f]*')),
 UNIQUE(transaction_id,transaction_ordinal));

CREATE TABLE journal_gc_audits(
 audit_id INTEGER PRIMARY KEY AUTOINCREMENT, capture_epoch TEXT NOT NULL,
 first_seq INTEGER NOT NULL, last_seq INTEGER NOT NULL CHECK(last_seq>=first_seq),
 transaction_count INTEGER NOT NULL CHECK(transaction_count>0), reason TEXT NOT NULL, created_at_unix_ms INTEGER NOT NULL);

CREATE TABLE journal_retention_clock(
 transaction_id TEXT PRIMARY KEY REFERENCES source_transactions(transaction_id) ON DELETE CASCADE,
 committed_at_unix_ms INTEGER NOT NULL);

CREATE TABLE logical_range_pins(
 pin_id TEXT PRIMARY KEY, owner_kind TEXT NOT NULL, owner_id TEXT NOT NULL,
 capture_epoch TEXT NOT NULL, start_seq INTEGER NOT NULL CHECK(start_seq>0),
 end_seq INTEGER CHECK(end_seq IS NULL OR end_seq>=start_seq),
 expires_at_unix_ms INTEGER, state TEXT NOT NULL CHECK(state IN ('active','release_pending','released')),
 revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0));

CREATE TABLE operator_command_requests(
 request_id TEXT PRIMARY KEY, dry_run_nonce TEXT NOT NULL UNIQUE, canonical_payload BLOB NOT NULL, payload_digest TEXT NOT NULL,
 run_id TEXT NOT NULL, peer_identity TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('accepted','reconciling','completed','failed','aborted_by_restart')),
 result BLOB, observation_revision INTEGER NOT NULL, control_revision INTEGER NOT NULL, expires_at TEXT NOT NULL, request_revision INTEGER NOT NULL DEFAULT 0 CHECK(request_revision>=0), plan_digest TEXT, immutable_intent_id TEXT, external_effect_evidence_digest TEXT, postcondition_evidence_digest TEXT,
 CHECK((state IN ('completed','failed'))=(result IS NOT NULL)), UNIQUE(request_id,payload_digest));

CREATE TABLE orphan_diagnostics(
 diagnostic_id TEXT PRIMARY KEY, state TEXT NOT NULL CHECK(state IN ('active','resolved')),
 created_at_unix_ms INTEGER NOT NULL, resolved_at_unix_ms INTEGER);

CREATE TABLE processing_failures(
 failure_id TEXT PRIMARY KEY, destination_id TEXT REFERENCES destinations(destination_id), component TEXT NOT NULL, failure_class TEXT NOT NULL,
 fingerprint TEXT NOT NULL, failed_boundary_start_seq INTEGER, failed_boundary_end_seq INTEGER, retry_class TEXT NOT NULL CHECK(retry_class IN ('transient','deterministic','exhausted','integrity_mismatch')),
 attempt INTEGER NOT NULL DEFAULT 0 CHECK(attempt>=0), next_retry_at TEXT, armed INTEGER NOT NULL DEFAULT 0 CHECK(armed IN (0,1)), first_failed_at TEXT NOT NULL, last_failed_at TEXT NOT NULL, failed_capture_epoch TEXT, failed_xid TEXT, failed_final_lsn TEXT
 CHECK(failed_final_lsn IS NULL OR (length(failed_final_lsn)=16 AND failed_final_lsn NOT GLOB '*[^0-9A-F]*')), observed_transaction_bytes INTEGER
 CHECK(observed_transaction_bytes IS NULL OR observed_transaction_bytes>=0), observed_transaction_events INTEGER
 CHECK(observed_transaction_events IS NULL OR observed_transaction_events>=0), limit_bytes INTEGER
 CHECK(limit_bytes IS NULL OR limit_bytes>=0), limit_events INTEGER
 CHECK(limit_events IS NULL OR limit_events>=0),
 CHECK((failed_boundary_start_seq IS NULL)=(failed_boundary_end_seq IS NULL)), CHECK(failed_boundary_end_seq IS NULL OR failed_boundary_end_seq>=failed_boundary_start_seq),
 CHECK((retry_class='transient' AND next_retry_at IS NOT NULL) OR (retry_class!='transient' AND next_retry_at IS NULL)),
 UNIQUE(component,fingerprint,failed_boundary_start_seq,failed_boundary_end_seq), UNIQUE(failure_id,destination_id));

CREATE TABLE relation_schemas(
 schema_fingerprint TEXT PRIMARY KEY, capture_epoch TEXT NOT NULL, relation_id TEXT NOT NULL,
 canonical_schema BLOB NOT NULL, schema_checksum TEXT NOT NULL, created_seq INTEGER NOT NULL CHECK(created_seq>0),
 UNIQUE(capture_epoch,relation_id,schema_fingerprint));

CREATE TABLE reseed_intents(intent_id TEXT PRIMARY KEY, destination_id TEXT REFERENCES destinations(destination_id), capture_epoch TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('prepared','reconciling','ready','complete','blocked','aborted')), revision INTEGER NOT NULL DEFAULT 0, evidence_digest TEXT);

CREATE TABLE runtime_ownership(
 run_id TEXT PRIMARY KEY, backend_pid INTEGER NOT NULL, connection_nonce TEXT NOT NULL UNIQUE, ownership_deadline_mono_ms INTEGER NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('held','expected_close','lost','released')), connection_generation INTEGER NOT NULL CHECK(connection_generation>0),
 expected_close_token TEXT UNIQUE, loss_evidence TEXT, takeover_evidence TEXT, revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0));

CREATE TABLE schema_migrations(
 version INTEGER PRIMARY KEY CHECK(version>0), name TEXT NOT NULL UNIQUE,
 checksum TEXT NOT NULL CHECK(length(checksum)>0), applied_at TEXT NOT NULL);

CREATE TABLE source_state(
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), capture_epoch TEXT NOT NULL, source_system_id TEXT NOT NULL,
 timeline_id TEXT NOT NULL, database_id TEXT NOT NULL, slot_name TEXT NOT NULL, plugin TEXT NOT NULL, publication_fingerprint TEXT NOT NULL, protocol_fingerprint TEXT NOT NULL,
 durable_transaction_end_lsn TEXT CHECK(durable_transaction_end_lsn IS NULL OR (length(durable_transaction_end_lsn)=16 AND durable_transaction_end_lsn NOT GLOB '*[^0-9A-F]*')),
 durable_transaction_id TEXT REFERENCES source_transactions(transaction_id), durable_journal_seq INTEGER,
 slot_creation_floor_lsn TEXT CHECK(slot_creation_floor_lsn IS NULL OR (length(slot_creation_floor_lsn)=16 AND slot_creation_floor_lsn NOT GLOB '*[^0-9A-F]*')),
 slot_creation_intent_id TEXT, last_feedback_lsn TEXT CHECK(last_feedback_lsn IS NULL OR (length(last_feedback_lsn)=16 AND last_feedback_lsn NOT GLOB '*[^0-9A-F]*')),
 last_feedback_repeated_creation_floor INTEGER NOT NULL DEFAULT 0 CHECK(last_feedback_repeated_creation_floor IN (0,1)),
 observed_confirmed_flush_lsn TEXT, observed_restart_lsn TEXT, control_revision INTEGER NOT NULL DEFAULT 0 CHECK(control_revision>=0),
 CHECK((durable_transaction_end_lsn IS NULL)=(durable_transaction_id IS NULL)), CHECK((durable_transaction_end_lsn IS NULL)=(durable_journal_seq IS NULL)),
 CHECK((slot_creation_floor_lsn IS NULL)=(slot_creation_intent_id IS NULL)),
 CHECK(last_feedback_repeated_creation_floor=0 OR (slot_creation_floor_lsn IS NOT NULL AND last_feedback_lsn=slot_creation_floor_lsn)),
 FOREIGN KEY(durable_transaction_id,capture_epoch,durable_journal_seq,durable_transaction_end_lsn) REFERENCES source_transactions(transaction_id,capture_epoch,last_seq,end_lsn),
 FOREIGN KEY(slot_creation_intent_id,capture_epoch,source_system_id,database_id,slot_name,slot_creation_floor_lsn) REFERENCES bootstrap_intents(intent_id,capture_epoch,source_system_id,database_id,slot_name,creation_floor_lsn));

CREATE TABLE source_transactions(
 transaction_id TEXT PRIMARY KEY, capture_epoch TEXT NOT NULL, source_system_id TEXT NOT NULL,
 database_id TEXT NOT NULL, slot_name TEXT NOT NULL, xid TEXT NOT NULL, end_lsn TEXT NOT NULL CHECK(length(end_lsn)=16 AND end_lsn NOT GLOB '*[^0-9A-F]*'),
 first_seq INTEGER NOT NULL CHECK(first_seq>0), last_seq INTEGER NOT NULL CHECK(last_seq>=first_seq), event_count INTEGER NOT NULL CHECK(event_count>=0),
 payload_checksum TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('committed','gc_removed')),
 UNIQUE(capture_epoch,source_system_id,database_id,slot_name,end_lsn), UNIQUE(capture_epoch,last_seq), UNIQUE(transaction_id,capture_epoch,last_seq), UNIQUE(transaction_id,capture_epoch,last_seq,end_lsn));

CREATE TABLE startup_reconciliations(
 reconciliation_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL UNIQUE,
 capture_epoch TEXT NOT NULL, outcome TEXT NOT NULL CHECK(outcome IN ('ready','duplicate_replay_expected','creation_floor_only','bootstrap_ambiguous_requires_restart','requires_reseed','blocked')),
 reason_code TEXT NOT NULL, requested_lsn TEXT, effective_restart_lsn TEXT,
 durable_transaction_end_lsn TEXT, creation_floor_lsn TEXT, created_at TEXT NOT NULL,
 CHECK(requested_lsn IS NULL OR (length(requested_lsn)=16 AND requested_lsn NOT GLOB '*[^0-9A-F]*')),
 CHECK(effective_restart_lsn IS NULL OR (length(effective_restart_lsn)=16 AND effective_restart_lsn NOT GLOB '*[^0-9A-F]*')));

CREATE TABLE terminal_metadata_retention(
 category TEXT NOT NULL CHECK(category IN ('destination_audit','invalid_generation','retired_generation')),
 object_id TEXT NOT NULL, terminal_at_unix_ms INTEGER NOT NULL,
 PRIMARY KEY(category,object_id));

CREATE UNIQUE INDEX journal_source_position
 ON journal_events(transaction_id,source_row_ordinal,source_mutation_ordinal);

CREATE INDEX logical_range_pins_active_start ON logical_range_pins(state,capture_epoch,start_seq);

CREATE INDEX logical_range_pins_owner_state
ON logical_range_pins(owner_kind,owner_id,pin_id) WHERE state!='released';

CREATE UNIQUE INDEX one_first_capture_fence_proof ON durable_capture_fences(capture_epoch,generation,nonce) WHERE first_proof=1;

CREATE INDEX terminal_metadata_retention_age
ON terminal_metadata_retention(category,terminal_at_unix_ms,object_id);

CREATE TRIGGER anchor_v2_insert BEFORE INSERT ON bootstrap_anchors WHEN NEW.state='complete' AND (
 NEW.generation_id IS NULL OR NEW.bootstrap_intent_id IS NULL OR
 NOT EXISTS(SELECT 1 FROM backfill_generations g JOIN backfill_runs r ON r.run_id=g.run_id WHERE g.generation_id=NEW.generation_id AND r.capture_epoch=NEW.capture_epoch AND g.generation=NEW.generation AND g.state='complete') OR
 NOT EXISTS(SELECT 1 FROM backfill_chunks c WHERE c.generation_id=NEW.generation_id AND c.state='complete') OR
 EXISTS(SELECT 1 FROM backfill_chunks c WHERE c.generation_id=NEW.generation_id AND c.state!='complete') OR
 NEW.snapshot_complete_seq!=(SELECT max(c.completed_seq) FROM backfill_chunks c WHERE c.generation_id=NEW.generation_id) OR
 NOT EXISTS(SELECT 1 FROM bootstrap_intents b WHERE b.intent_id=NEW.bootstrap_intent_id AND b.capture_epoch=NEW.capture_epoch AND (NEW.lower_stitch_lsn IS NOT NULL OR b.creation_floor_lsn=NEW.snapshot_boundary_lsn)) OR
 NOT EXISTS(SELECT 1 FROM bootstrap_imports i WHERE i.intent_id=NEW.bootstrap_intent_id AND i.state='acknowledged') OR
 EXISTS(SELECT 1 FROM bootstrap_imports i WHERE i.intent_id=NEW.bootstrap_intent_id AND i.state!='acknowledged') OR
 (NEW.start_seq!=0 AND NOT EXISTS(SELECT 1 FROM source_transactions t WHERE t.capture_epoch=NEW.capture_epoch AND t.last_seq=NEW.start_seq))
) BEGIN SELECT RAISE(ABORT,'complete anchor lacks exact baseline proofs'); END;

CREATE TRIGGER anchor_v2_update BEFORE UPDATE OF state ON bootstrap_anchors WHEN NEW.state='complete' AND (
 NEW.generation_id IS NULL OR NEW.bootstrap_intent_id IS NULL OR
 NOT EXISTS(SELECT 1 FROM backfill_generations g JOIN backfill_runs r ON r.run_id=g.run_id WHERE g.generation_id=NEW.generation_id AND r.capture_epoch=NEW.capture_epoch AND g.generation=NEW.generation AND g.state='complete') OR
 NOT EXISTS(SELECT 1 FROM backfill_chunks c WHERE c.generation_id=NEW.generation_id AND c.state='complete') OR
 EXISTS(SELECT 1 FROM backfill_chunks c WHERE c.generation_id=NEW.generation_id AND c.state!='complete') OR
 NEW.snapshot_complete_seq!=(SELECT max(c.completed_seq) FROM backfill_chunks c WHERE c.generation_id=NEW.generation_id) OR
 NOT EXISTS(SELECT 1 FROM bootstrap_intents b WHERE b.intent_id=NEW.bootstrap_intent_id AND b.capture_epoch=NEW.capture_epoch AND (NEW.lower_stitch_lsn IS NOT NULL OR b.creation_floor_lsn=NEW.snapshot_boundary_lsn)) OR
 NOT EXISTS(SELECT 1 FROM bootstrap_imports i WHERE i.intent_id=NEW.bootstrap_intent_id AND i.state='acknowledged') OR
 EXISTS(SELECT 1 FROM bootstrap_imports i WHERE i.intent_id=NEW.bootstrap_intent_id AND i.state!='acknowledged') OR
 (NEW.start_seq!=0 AND NOT EXISTS(SELECT 1 FROM source_transactions t WHERE t.capture_epoch=NEW.capture_epoch AND t.last_seq=NEW.start_seq))
) BEGIN SELECT RAISE(ABORT,'complete anchor lacks exact baseline proofs'); END;

CREATE TRIGGER archive_generation_transition BEFORE UPDATE OF state ON archive_generations WHEN NOT ((OLD.state='candidate' AND NEW.state IN ('live','invalidated')) OR (OLD.state='live' AND NEW.state IN ('retirement_eligible','invalidated')) OR (OLD.state='retirement_eligible' AND NEW.state='retired') OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid archive-generation transition'); END;

CREATE TRIGGER archive_segment_transition BEFORE UPDATE OF state ON archive_segment_intents WHEN NOT ((OLD.state='selected' AND NEW.state IN ('writing','failed')) OR (OLD.state='writing' AND NEW.state IN ('published','failed')) OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid archive-segment transition'); END;

CREATE TRIGGER archive_selection_immutable BEFORE UPDATE OF generation_id,first_seq,last_seq,selection_digest ON archive_segment_intents BEGIN SELECT RAISE(ABORT,'archive selection is immutable'); END;

CREATE TRIGGER audit_identity_change_requires_empty_coverage BEFORE UPDATE ON destination_audits
WHEN (NEW.destination_id IS NOT OLD.destination_id
   OR NEW.capture_epoch IS NOT OLD.capture_epoch
   OR NEW.generation IS NOT OLD.generation
   OR NEW.round_target_seq IS NOT OLD.round_target_seq
   OR NEW.round_identity_digest IS NOT OLD.round_identity_digest
   OR NEW.configuration_fingerprint IS NOT OLD.configuration_fingerprint
   OR NEW.contract_digest IS NOT OLD.contract_digest
   OR NEW.freshness_window_started_at IS NOT OLD.freshness_window_started_at
   OR NEW.freshness_expires_at IS NOT OLD.freshness_expires_at)
 AND (OLD.journal_verified_start_seq IS NOT NULL
   OR OLD.self_consistent_start_seq IS NOT NULL
   OR EXISTS(SELECT 1 FROM audit_coverage_subranges r WHERE r.audit_id=OLD.audit_id))
BEGIN SELECT RAISE(ABORT,'audit coverage must be invalidated before round identity changes'); END;

CREATE TRIGGER audit_inline_insert_bounds BEFORE INSERT ON destination_audits
WHEN (NEW.journal_verified_start_seq IS NOT NULL AND (
       NEW.journal_verified_start_seq<coalesce(NEW.retained_history_start_seq,0)
    OR NEW.journal_verified_start_seq<coalesce(NEW.unverifiable_before_seq,0)
    OR NEW.journal_verified_end_seq>NEW.round_target_seq))
 OR (NEW.self_consistent_start_seq IS NOT NULL AND (
       NEW.self_consistent_start_seq<coalesce(NEW.retained_history_start_seq,0)
    OR NEW.self_consistent_start_seq<coalesce(NEW.unverifiable_before_seq,0)
    OR NEW.self_consistent_end_seq>NEW.round_target_seq))
BEGIN SELECT RAISE(ABORT,'inline audit coverage outside frozen retention target'); END;

CREATE TRIGGER audit_inline_update_bounds BEFORE UPDATE ON destination_audits
WHEN (NEW.journal_verified_start_seq IS NOT NULL AND (
       NEW.journal_verified_start_seq<coalesce(NEW.retained_history_start_seq,0)
    OR NEW.journal_verified_start_seq<coalesce(NEW.unverifiable_before_seq,0)
    OR NEW.journal_verified_end_seq>NEW.round_target_seq))
 OR (NEW.self_consistent_start_seq IS NOT NULL AND (
       NEW.self_consistent_start_seq<coalesce(NEW.retained_history_start_seq,0)
    OR NEW.self_consistent_start_seq<coalesce(NEW.unverifiable_before_seq,0)
    OR NEW.self_consistent_end_seq>NEW.round_target_seq))
BEGIN SELECT RAISE(ABORT,'inline audit coverage outside frozen retention target'); END;

CREATE TRIGGER audit_parent_update_preserves_subrange_bounds BEFORE UPDATE ON destination_audits
WHEN EXISTS(
 SELECT 1 FROM audit_coverage_subranges r WHERE r.audit_id=OLD.audit_id AND (
      r.start_seq<coalesce(NEW.retained_history_start_seq,0)
   OR r.start_seq<coalesce(NEW.unverifiable_before_seq,0)
   OR r.end_seq>NEW.round_target_seq
   OR r.fresh_until<=NEW.freshness_window_started_at
   OR r.fresh_until>NEW.freshness_expires_at))
BEGIN SELECT RAISE(ABORT,'audit update would retain out-of-bounds coverage'); END;

CREATE TRIGGER audit_revision BEFORE UPDATE ON destination_audits WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale audit revision'); END;

CREATE TRIGGER audit_subrange_insert BEFORE INSERT ON audit_coverage_subranges WHEN NOT EXISTS(SELECT 1 FROM destination_audits a WHERE a.audit_id=NEW.audit_id AND NEW.start_seq>=coalesce(a.retained_history_start_seq,0) AND NEW.start_seq>=coalesce(a.unverifiable_before_seq,0) AND NEW.end_seq<=a.round_target_seq AND NEW.fresh_until>a.freshness_window_started_at AND NEW.fresh_until<=a.freshness_expires_at) BEGIN SELECT RAISE(ABORT,'audit subrange outside frozen retention/freshness target'); END;

CREATE TRIGGER audit_subrange_update BEFORE UPDATE ON audit_coverage_subranges WHEN NOT EXISTS(SELECT 1 FROM destination_audits a WHERE a.audit_id=NEW.audit_id AND NEW.start_seq>=coalesce(a.retained_history_start_seq,0) AND NEW.start_seq>=coalesce(a.unverifiable_before_seq,0) AND NEW.end_seq<=a.round_target_seq AND NEW.fresh_until>a.freshness_window_started_at AND NEW.fresh_until<=a.freshness_expires_at) BEGIN SELECT RAISE(ABORT,'audit subrange outside frozen retention/freshness target'); END;

CREATE TRIGGER backfill_chunk_transition BEFORE UPDATE OF state ON backfill_chunks WHEN NOT (OLD.state='pending' AND NEW.state IN ('complete','invalidated') OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid backfill-chunk transition'); END;

CREATE TRIGGER backfill_generation_transition BEFORE UPDATE OF state ON backfill_generations WHEN NOT ((OLD.state='prepared' AND NEW.state IN ('copying','invalidated')) OR (OLD.state='copying' AND NEW.state IN ('fencing','invalidated')) OR (OLD.state='fencing' AND NEW.state IN ('complete','invalidated')) OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid backfill-generation transition'); END;

CREATE TRIGGER backfill_run_revision BEFORE UPDATE ON backfill_runs WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale backfill-run revision'); END;

CREATE TRIGGER backfill_run_transition BEFORE UPDATE OF state ON backfill_runs WHEN NOT ((OLD.state='prepared' AND NEW.state IN ('running','invalidated')) OR (OLD.state='running' AND NEW.state IN ('paused','complete','invalidated')) OR (OLD.state='paused' AND NEW.state IN ('running','invalidated')) OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid backfill-run transition'); END;

CREATE TRIGGER batch_transition BEFORE UPDATE OF state ON clickhouse_batch_intents WHEN NOT ((OLD.state='prepared' AND NEW.state IN ('dispatched','failed')) OR (OLD.state='dispatched' AND NEW.state IN ('verified','failed')) OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid batch transition'); END;

CREATE TRIGGER bootstrap_import_revision BEFORE UPDATE ON bootstrap_imports WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale import revision'); END;

CREATE TRIGGER bootstrap_import_transition BEFORE UPDATE OF state ON bootstrap_imports WHEN NOT ((OLD.state='prepared' AND NEW.state IN ('importing','failed','invalidated')) OR (OLD.state='importing' AND NEW.state IN ('acknowledged','failed','invalidated')) OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid import transition'); END;

CREATE TRIGGER bootstrap_intent_revision BEFORE UPDATE ON bootstrap_intents WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale bootstrap revision'); END;

CREATE TRIGGER bootstrap_intent_transition BEFORE UPDATE OF state ON bootstrap_intents WHEN NOT ((OLD.state='prepared' AND NEW.state IN ('remote_slot_unknown','slot_created','aborted')) OR (OLD.state='remote_slot_unknown' AND NEW.state IN ('slot_created','aborted')) OR (OLD.state='slot_created' AND NEW.state IN ('copying','invalidated','aborted')) OR (OLD.state='copying' AND NEW.state IN ('complete','invalidated','aborted')) OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid bootstrap transition'); END;

CREATE TRIGGER checkpoint_blocks_armed_failure BEFORE UPDATE ON destination_checkpoints
WHEN (NEW.journal_seq!=OLD.journal_seq OR NEW.complete_transaction_id!=OLD.complete_transaction_id)
 AND EXISTS(
  SELECT 1 FROM destinations d JOIN processing_failures f
   ON f.failure_id=d.current_failure_id AND f.destination_id=d.destination_id
  WHERE d.destination_id=OLD.destination_id AND f.armed=1)
BEGIN SELECT RAISE(ABORT,'armed destination failure blocks checkpoint advancement'); END;

CREATE TRIGGER checkpoint_revision BEFORE UPDATE ON destination_checkpoints WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale checkpoint revision'); END;

CREATE TRIGGER command_causality_insert BEFORE INSERT ON operator_command_requests
WHEN (NEW.immutable_intent_id IS NOT NULL AND NEW.plan_digest IS NULL)
 OR (NEW.external_effect_evidence_digest IS NOT NULL AND NEW.immutable_intent_id IS NULL)
 OR (NEW.postcondition_evidence_digest IS NOT NULL AND NEW.immutable_intent_id IS NULL)
BEGIN SELECT RAISE(ABORT,'incomplete operator action causal link'); END;

CREATE TRIGGER command_causality_links_immutable BEFORE UPDATE ON operator_command_requests
WHEN (OLD.plan_digest IS NOT NULL AND NEW.plan_digest IS NOT OLD.plan_digest)
 OR (OLD.immutable_intent_id IS NOT NULL AND NEW.immutable_intent_id IS NOT OLD.immutable_intent_id)
 OR (OLD.external_effect_evidence_digest IS NOT NULL AND NEW.external_effect_evidence_digest IS NOT OLD.external_effect_evidence_digest)
 OR (OLD.postcondition_evidence_digest IS NOT NULL AND NEW.postcondition_evidence_digest IS NOT OLD.postcondition_evidence_digest)
BEGIN SELECT RAISE(ABORT,'operator action causal link is immutable'); END;

CREATE TRIGGER command_causality_update BEFORE UPDATE ON operator_command_requests
WHEN (NEW.immutable_intent_id IS NOT NULL AND NEW.plan_digest IS NULL)
 OR (NEW.external_effect_evidence_digest IS NOT NULL AND NEW.immutable_intent_id IS NULL)
 OR (NEW.postcondition_evidence_digest IS NOT NULL AND NEW.immutable_intent_id IS NULL)
BEGIN SELECT RAISE(ABORT,'incomplete operator action causal link'); END;

CREATE TRIGGER command_identity_immutable BEFORE UPDATE ON operator_command_requests
WHEN NEW.request_id IS NOT OLD.request_id
 OR NEW.dry_run_nonce IS NOT OLD.dry_run_nonce
 OR NEW.canonical_payload IS NOT OLD.canonical_payload
 OR NEW.payload_digest IS NOT OLD.payload_digest
 OR NEW.run_id IS NOT OLD.run_id
 OR NEW.peer_identity IS NOT OLD.peer_identity
 OR NEW.observation_revision IS NOT OLD.observation_revision
 OR NEW.control_revision IS NOT OLD.control_revision
 OR NEW.expires_at IS NOT OLD.expires_at
BEGIN SELECT RAISE(ABORT,'operator command request identity is immutable'); END;

CREATE TRIGGER command_revision BEFORE UPDATE ON operator_command_requests WHEN NEW.request_revision!=OLD.request_revision+1 BEGIN SELECT RAISE(ABORT,'stale command revision'); END;

CREATE TRIGGER command_transition BEFORE UPDATE OF state ON operator_command_requests WHEN NOT ((OLD.state='accepted' AND NEW.state IN ('reconciling','completed','failed','aborted_by_restart')) OR (OLD.state='reconciling' AND NEW.state IN ('completed','failed','aborted_by_restart')) OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid command transition'); END;

CREATE TRIGGER complete_anchor_blocks_chunk_change BEFORE UPDATE ON backfill_chunks WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.generation_id=OLD.generation_id) BEGIN SELECT RAISE(ABORT,'anchor chunk proof is immutable'); END;

CREATE TRIGGER complete_anchor_blocks_chunk_delete BEFORE DELETE ON backfill_chunks WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.generation_id=OLD.generation_id) BEGIN SELECT RAISE(ABORT,'anchor chunk proof is immutable'); END;

CREATE TRIGGER complete_anchor_blocks_chunk_insert BEFORE INSERT ON backfill_chunks WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.generation_id=NEW.generation_id) BEGIN SELECT RAISE(ABORT,'anchor chunk set is immutable'); END;

CREATE TRIGGER complete_anchor_blocks_fence_delete BEFORE DELETE ON durable_capture_fences WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.capture_epoch=OLD.capture_epoch AND a.generation=OLD.generation AND a.post_copy_fence_nonce=OLD.nonce) BEGIN SELECT RAISE(ABORT,'anchor fence proof is immutable'); END;

CREATE TRIGGER complete_anchor_blocks_fence_update BEFORE UPDATE ON durable_capture_fences WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.capture_epoch=OLD.capture_epoch AND a.generation=OLD.generation AND a.post_copy_fence_nonce=OLD.nonce) BEGIN SELECT RAISE(ABORT,'anchor fence proof is immutable'); END;

CREATE TRIGGER complete_anchor_blocks_generation_change BEFORE UPDATE ON backfill_generations WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.generation_id=OLD.generation_id) BEGIN SELECT RAISE(ABORT,'anchor generation proof is immutable'); END;

CREATE TRIGGER complete_anchor_blocks_generation_delete BEFORE DELETE ON backfill_generations WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.generation_id=OLD.generation_id) BEGIN SELECT RAISE(ABORT,'anchor generation proof is immutable'); END;

CREATE TRIGGER complete_anchor_blocks_import_change BEFORE UPDATE ON bootstrap_imports WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.bootstrap_intent_id=OLD.intent_id) BEGIN SELECT RAISE(ABORT,'anchor import proof is immutable'); END;

CREATE TRIGGER complete_anchor_blocks_import_delete BEFORE DELETE ON bootstrap_imports WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.bootstrap_intent_id=OLD.intent_id) BEGIN SELECT RAISE(ABORT,'anchor import proof is immutable'); END;

CREATE TRIGGER complete_anchor_blocks_import_insert BEFORE INSERT ON bootstrap_imports WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.bootstrap_intent_id=NEW.intent_id) BEGIN SELECT RAISE(ABORT,'anchor importer set is immutable'); END;

CREATE TRIGGER complete_anchor_blocks_intent_update BEFORE UPDATE ON bootstrap_intents WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.bootstrap_intent_id=OLD.intent_id) BEGIN SELECT RAISE(ABORT,'anchor bootstrap proof is immutable'); END;

CREATE TRIGGER complete_anchor_blocks_run_update BEFORE UPDATE ON backfill_runs WHEN EXISTS(SELECT 1 FROM backfill_generations g JOIN bootstrap_anchors a ON a.generation_id=g.generation_id WHERE a.state='complete' AND g.run_id=OLD.run_id) BEGIN SELECT RAISE(ABORT,'anchor run proof is immutable'); END;

CREATE TRIGGER complete_anchor_blocks_schema_delete BEFORE DELETE ON relation_schemas WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a,json_each(a.snapshot_schema_fingerprints) j WHERE a.state='complete' AND a.capture_epoch=OLD.capture_epoch AND j.value=OLD.schema_fingerprint) BEGIN SELECT RAISE(ABORT,'anchor schema proof is immutable'); END;

CREATE TRIGGER complete_anchor_blocks_source_delete BEFORE DELETE ON source_transactions WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.capture_epoch=OLD.capture_epoch AND a.start_seq=OLD.last_seq) BEGIN SELECT RAISE(ABORT,'anchor lower proof is immutable'); END;

CREATE TRIGGER complete_anchor_immutable BEFORE UPDATE ON bootstrap_anchors WHEN OLD.state='complete' BEGIN SELECT RAISE(ABORT,'complete anchor is immutable'); END;

CREATE TRIGGER complete_anchor_requires_fence BEFORE INSERT ON bootstrap_anchors WHEN NEW.state='complete' AND (NOT EXISTS(SELECT 1 FROM durable_capture_fences f WHERE f.capture_epoch=NEW.capture_epoch AND f.generation=NEW.generation AND f.nonce=NEW.post_copy_fence_nonce AND f.post_copy_fence_lsn=NEW.post_copy_fence_lsn AND f.post_copy_fence_seq=NEW.post_copy_fence_seq AND f.first_proof=1) OR NOT EXISTS(SELECT 1 FROM backfill_generations g JOIN backfill_runs r ON r.run_id=g.run_id WHERE r.capture_epoch=NEW.capture_epoch AND g.generation=NEW.generation AND g.state='complete') OR NOT EXISTS(SELECT 1 FROM backfill_chunks c JOIN backfill_generations g ON g.generation_id=c.generation_id JOIN backfill_runs r ON r.run_id=g.run_id WHERE r.capture_epoch=NEW.capture_epoch AND g.generation=NEW.generation AND c.state='complete') OR EXISTS(SELECT 1 FROM backfill_chunks c JOIN backfill_generations g ON g.generation_id=c.generation_id JOIN backfill_runs r ON r.run_id=g.run_id WHERE r.capture_epoch=NEW.capture_epoch AND g.generation=NEW.generation AND c.state!='complete') OR NOT EXISTS(SELECT 1 FROM bootstrap_imports i JOIN bootstrap_intents b ON b.intent_id=i.intent_id WHERE b.capture_epoch=NEW.capture_epoch AND i.state='acknowledged') OR EXISTS(SELECT 1 FROM json_each(NEW.snapshot_schema_fingerprints) j LEFT JOIN relation_schemas rs ON rs.schema_fingerprint=j.value AND rs.capture_epoch=NEW.capture_epoch WHERE rs.schema_fingerprint IS NULL) OR (NEW.lower_stitch_lsn IS NOT NULL AND NOT EXISTS(SELECT 1 FROM source_transactions t WHERE t.capture_epoch=NEW.capture_epoch AND t.end_lsn=NEW.lower_stitch_lsn AND t.last_seq=NEW.start_seq))) BEGIN SELECT RAISE(ABORT,'complete anchor lacks matching durable fence'); END;

CREATE TRIGGER complete_anchor_update_requires_fence BEFORE UPDATE OF state ON bootstrap_anchors WHEN NEW.state='complete' AND (NOT EXISTS(SELECT 1 FROM durable_capture_fences f WHERE f.capture_epoch=NEW.capture_epoch AND f.generation=NEW.generation AND f.nonce=NEW.post_copy_fence_nonce AND f.post_copy_fence_lsn=NEW.post_copy_fence_lsn AND f.post_copy_fence_seq=NEW.post_copy_fence_seq AND f.first_proof=1) OR NOT EXISTS(SELECT 1 FROM backfill_generations g JOIN backfill_runs r ON r.run_id=g.run_id WHERE r.capture_epoch=NEW.capture_epoch AND g.generation=NEW.generation AND g.state='complete') OR NOT EXISTS(SELECT 1 FROM backfill_chunks c JOIN backfill_generations g ON g.generation_id=c.generation_id JOIN backfill_runs r ON r.run_id=g.run_id WHERE r.capture_epoch=NEW.capture_epoch AND g.generation=NEW.generation AND c.state='complete') OR EXISTS(SELECT 1 FROM backfill_chunks c JOIN backfill_generations g ON g.generation_id=c.generation_id JOIN backfill_runs r ON r.run_id=g.run_id WHERE r.capture_epoch=NEW.capture_epoch AND g.generation=NEW.generation AND c.state!='complete') OR NOT EXISTS(SELECT 1 FROM bootstrap_imports i JOIN bootstrap_intents b ON b.intent_id=i.intent_id WHERE b.capture_epoch=NEW.capture_epoch AND i.state='acknowledged') OR EXISTS(SELECT 1 FROM json_each(NEW.snapshot_schema_fingerprints) j LEFT JOIN relation_schemas rs ON rs.schema_fingerprint=j.value AND rs.capture_epoch=NEW.capture_epoch WHERE rs.schema_fingerprint IS NULL) OR (NEW.lower_stitch_lsn IS NOT NULL AND NOT EXISTS(SELECT 1 FROM source_transactions t WHERE t.capture_epoch=NEW.capture_epoch AND t.end_lsn=NEW.lower_stitch_lsn AND t.last_seq=NEW.start_seq))) BEGIN SELECT RAISE(ABORT,'complete anchor lacks matching durable fence'); END;

CREATE TRIGGER current_audit_checkpoint_change AFTER UPDATE OF configuration_fingerprint,capture_epoch,generation,journal_seq,complete_transaction_id ON destination_checkpoints
WHEN NEW.configuration_fingerprint!=OLD.configuration_fingerprint
 OR NEW.capture_epoch!=OLD.capture_epoch OR NEW.generation!=OLD.generation
 OR NEW.journal_seq!=OLD.journal_seq OR NEW.complete_transaction_id!=OLD.complete_transaction_id
BEGIN DELETE FROM destination_current_audits WHERE destination_id=NEW.destination_id; END;

CREATE TRIGGER current_audit_checkpoint_delete AFTER DELETE ON destination_checkpoints
BEGIN DELETE FROM destination_current_audits WHERE destination_id=OLD.destination_id; END;

CREATE TRIGGER current_audit_checkpoint_insert AFTER INSERT ON destination_checkpoints
BEGIN DELETE FROM destination_current_audits WHERE destination_id=NEW.destination_id; END;

CREATE TRIGGER current_audit_destination_change AFTER UPDATE OF kind,configuration_fingerprint,capture_epoch,generation ON destinations
WHEN NEW.kind!=OLD.kind OR NEW.configuration_fingerprint!=OLD.configuration_fingerprint
 OR NEW.capture_epoch!=OLD.capture_epoch OR NEW.generation!=OLD.generation
BEGIN DELETE FROM destination_current_audits WHERE destination_id=NEW.destination_id; END;

CREATE TRIGGER current_audit_insert BEFORE INSERT ON destination_current_audits
WHEN NOT EXISTS(
 SELECT 1 FROM destination_audits a JOIN destinations d
 ON d.destination_id=a.destination_id
 WHERE a.audit_id=NEW.audit_id AND a.incarnation=NEW.incarnation
 AND a.destination_id=NEW.destination_id
 AND a.configuration_fingerprint=d.configuration_fingerprint
 AND a.capture_epoch=d.capture_epoch AND a.generation=d.generation)
BEGIN SELECT RAISE(ABORT,'current audit identity mismatch'); END;

CREATE TRIGGER current_audit_update BEFORE UPDATE ON destination_current_audits
WHEN NOT EXISTS(
 SELECT 1 FROM destination_audits a JOIN destinations d
 ON d.destination_id=a.destination_id
 WHERE a.audit_id=NEW.audit_id AND a.incarnation=NEW.incarnation
 AND a.destination_id=NEW.destination_id
 AND a.configuration_fingerprint=d.configuration_fingerprint
 AND a.capture_epoch=d.capture_epoch AND a.generation=d.generation)
BEGIN SELECT RAISE(ABORT,'current audit identity mismatch'); END;

CREATE TRIGGER destination_fence_monotonic BEFORE UPDATE OF highest_external_fence ON destinations WHEN NEW.highest_external_fence < OLD.highest_external_fence BEGIN SELECT RAISE(ABORT,'external fence regression'); END;

CREATE TRIGGER destination_revision BEFORE UPDATE ON destinations WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale destination revision'); END;

CREATE TRIGGER journal_events_immutable BEFORE UPDATE ON journal_events
 BEGIN SELECT RAISE(ABORT,'journal event is immutable'); END;

CREATE TRIGGER journal_retention_clock_insert AFTER INSERT ON source_transactions
 WHEN NEW.state='committed' BEGIN
 INSERT INTO journal_retention_clock(transaction_id,committed_at_unix_ms)
 VALUES(NEW.transaction_id,CAST(unixepoch('subsec')*1000 AS INTEGER));
 END;

CREATE TRIGGER lease_revision BEFORE UPDATE ON destination_generation_leases WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale lease revision'); END;

CREATE TRIGGER lease_transition BEFORE UPDATE OF state ON destination_generation_leases WHEN NOT ((OLD.state='held' AND NEW.state IN ('fenced','expired','released')) OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid lease transition'); END;

CREATE TRIGGER logical_pin_revision BEFORE UPDATE ON logical_range_pins
 WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale logical pin revision'); END;

CREATE TRIGGER logical_pin_transition BEFORE UPDATE OF state ON logical_range_pins
 WHEN NOT ((OLD.state='active' AND NEW.state='release_pending') OR (OLD.state='release_pending' AND NEW.state='released') OR NEW.state=OLD.state)
 BEGIN SELECT RAISE(ABORT,'invalid logical pin transition'); END;

CREATE TRIGGER ownership_revision BEFORE UPDATE ON runtime_ownership WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale ownership revision'); END;

CREATE TRIGGER ownership_transition BEFORE UPDATE OF state ON runtime_ownership WHEN NOT ((OLD.state='held' AND NEW.state IN ('expected_close','lost','released')) OR (OLD.state='expected_close' AND NEW.state IN ('lost','released')) OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid ownership transition'); END;

CREATE TRIGGER promotion_fence_above_high_water BEFORE INSERT ON destination_promotion_intents WHEN NEW.promotion_fence <= (SELECT highest_external_fence FROM destinations WHERE destination_id=NEW.destination_id) BEGIN SELECT RAISE(ABORT,'promotion fence is not above high-water'); END;

CREATE TRIGGER promotion_fence_allocated AFTER INSERT ON destination_promotion_intents BEGIN UPDATE destinations SET highest_external_fence=NEW.promotion_fence,revision=revision+1 WHERE destination_id=NEW.destination_id; END;

CREATE TRIGGER promotion_revision BEFORE UPDATE ON destination_promotion_intents WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale promotion revision'); END;

CREATE TRIGGER promotion_transition BEFORE UPDATE OF state ON destination_promotion_intents WHEN NOT ((OLD.state='prepared' AND NEW.state IN ('old_leases_fenced','promotion_recovery_required')) OR (OLD.state='old_leases_fenced' AND NEW.state IN ('candidate_verified','promotion_recovery_required')) OR (OLD.state='candidate_verified' AND NEW.state IN ('switch_pending','promotion_recovery_required')) OR (OLD.state='switch_pending' AND NEW.state IN ('switched','promotion_recovery_required')) OR (OLD.state='switched' AND NEW.state IN ('verified','promotion_recovery_required')) OR (OLD.state='verified' AND NEW.state='retirement_eligible') OR (OLD.state='retirement_eligible' AND NEW.state='retired') OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid promotion transition'); END;

CREATE TRIGGER referenced_bootstrap_intent_stays_nonterminal BEFORE UPDATE OF state ON bootstrap_intents WHEN NEW.state IN ('complete','invalidated','aborted') AND EXISTS(SELECT 1 FROM source_state s WHERE s.slot_creation_intent_id=OLD.intent_id) BEGIN SELECT RAISE(ABORT,'referenced creation-floor intent cannot become terminal'); END;

CREATE TRIGGER relation_schemas_immutable BEFORE UPDATE ON relation_schemas BEGIN SELECT RAISE(ABORT,'relation schema is immutable'); END;

CREATE TRIGGER reseed_revision BEFORE UPDATE ON reseed_intents WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale reseed revision'); END;

CREATE TRIGGER reseed_transition BEFORE UPDATE OF state ON reseed_intents WHEN NOT ((OLD.state='prepared' AND NEW.state IN ('reconciling','blocked','aborted')) OR (OLD.state='reconciling' AND NEW.state IN ('ready','blocked','aborted')) OR (OLD.state='ready' AND NEW.state IN ('complete','blocked','aborted')) OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid reseed transition'); END;

CREATE TRIGGER source_floor_requires_nonterminal_intent BEFORE INSERT ON source_state WHEN NEW.slot_creation_intent_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM bootstrap_intents b WHERE b.intent_id=NEW.slot_creation_intent_id AND b.state NOT IN ('complete','invalidated','aborted')) BEGIN SELECT RAISE(ABORT,'creation floor intent is terminal'); END;

CREATE TRIGGER source_floor_update_requires_nonterminal_intent BEFORE UPDATE OF slot_creation_intent_id,slot_creation_floor_lsn ON source_state WHEN NEW.slot_creation_intent_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM bootstrap_intents b WHERE b.intent_id=NEW.slot_creation_intent_id AND b.state NOT IN ('complete','invalidated','aborted')) BEGIN SELECT RAISE(ABORT,'creation floor intent is terminal'); END;

CREATE TRIGGER source_progress_monotonic BEFORE UPDATE OF durable_transaction_end_lsn ON source_state WHEN OLD.durable_transaction_end_lsn IS NOT NULL AND (NEW.durable_transaction_end_lsn IS NULL OR NEW.durable_transaction_end_lsn < OLD.durable_transaction_end_lsn) BEGIN SELECT RAISE(ABORT,'durable source progress regression'); END;

CREATE TRIGGER source_state_revision BEFORE UPDATE ON source_state WHEN NEW.control_revision!=OLD.control_revision+1 BEGIN SELECT RAISE(ABORT,'stale source-state revision'); END;

CREATE TRIGGER source_transactions_immutable BEFORE UPDATE OF capture_epoch,source_system_id,database_id,slot_name,end_lsn,first_seq,last_seq,event_count,payload_checksum ON source_transactions BEGIN SELECT RAISE(ABORT,'source transaction identity and checksum are immutable'); END;
