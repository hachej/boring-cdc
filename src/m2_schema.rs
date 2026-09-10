//! Versioned SQLite state schema and durability-bearing connection admission.

use rusqlite::{Connection, OpenFlags};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub const SCHEMA_VERSION: i64 = 2;
// M0-PROVISIONAL: boring-cdc-m2-schema
pub const WRITER_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
// M0-PROVISIONAL: boring-cdc-m2-schema
pub const READER_MAX_AGE: Duration = Duration::from_secs(30);
// M0-PROVISIONAL: boring-cdc-m2-schema
pub const READER_MAX_ROWS: usize = 1_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterAttestation {
    pub run_id: String,
    pub connection_generation: u64,
    pub journal_mode: String,
    pub synchronous: i64,
    pub auto_vacuum: i64,
    pub observed_at_unix_ms: i64,
    pub fresh_until_unix_ms: i64,
}

pub struct WriterConnection {
    connection: Connection,
    attestation: WriterAttestation,
}

impl WriterConnection {
    pub fn connection(&self) -> &Connection {
        &self.connection
    }
    pub fn connection_mut(&mut self) -> &mut Connection {
        &mut self.connection
    }
    pub fn attestation(&self) -> &WriterAttestation {
        &self.attestation
    }
}

pub struct ReaderHandle {
    connection: Connection,
    opened_at: Instant,
    deadline: Instant,
    max_rows: usize,
}

impl ReaderHandle {
    pub fn query_bounded<T, F>(&self, sql: &str, mut map: F) -> rusqlite::Result<Vec<T>>
    where
        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        if self.expired() {
            return Err(rusqlite::Error::InvalidQuery);
        }
        let mut statement = self.connection.prepare(sql)?;
        if statement.column_count() == 0 {
            return Err(rusqlite::Error::InvalidQuery);
        }
        let mut rows = statement.query([])?;
        let mut values = Vec::new();
        while let Some(row) = rows.next()? {
            if self.expired() || values.len() == self.max_rows {
                return Err(rusqlite::Error::InvalidQuery);
            }
            values.push(map(row)?);
        }
        Ok(values)
    }
    pub fn expired(&self) -> bool {
        Instant::now() >= self.deadline
    }
    pub fn age(&self) -> Duration {
        self.opened_at.elapsed()
    }
    pub fn max_rows(&self) -> usize {
        self.max_rows
    }
}

pub fn open_writer(
    path: &Path,
    run_id: impl Into<String>,
    connection_generation: u64,
    now_unix_ms: i64,
) -> rusqlite::Result<WriterConnection> {
    reject_non_file(path)?;
    let is_new = !path.exists();
    let connection = Connection::open(path)?;
    connection.busy_timeout(WRITER_BUSY_TIMEOUT)?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    let user_table_count: i64 = connection.query_row(
        "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    if is_new || user_table_count == 0 {
        connection.pragma_update(None, "auto_vacuum", "INCREMENTAL")?;
    }
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "wal_autocheckpoint", 0)?;
    let journal_mode: String = connection.pragma_query_value(None, "journal_mode", |r| r.get(0))?;
    let synchronous: i64 = connection.pragma_query_value(None, "synchronous", |r| r.get(0))?;
    let auto_vacuum: i64 = connection.pragma_query_value(None, "auto_vacuum", |r| r.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") || synchronous != 2 || auto_vacuum != 2 {
        return Err(rusqlite::Error::InvalidQuery);
    }
    apply_migrations(&connection)?;
    Ok(WriterConnection {
        connection,
        attestation: WriterAttestation {
            run_id: run_id.into(),
            connection_generation,
            journal_mode,
            synchronous,
            auto_vacuum,
            observed_at_unix_ms: now_unix_ms,
            fresh_until_unix_ms: now_unix_ms.saturating_add(READER_MAX_AGE.as_millis() as i64),
        },
    })
}

pub fn open_reader(path: &Path) -> rusqlite::Result<ReaderHandle> {
    open_reader_with_limits(path, READER_MAX_AGE, READER_MAX_ROWS)
}

pub fn open_reader_with_limits(
    path: &Path,
    max_age: Duration,
    max_rows: usize,
) -> rusqlite::Result<ReaderHandle> {
    reject_non_file(path)?;
    if max_age.is_zero() || max_rows == 0 {
        return Err(rusqlite::Error::InvalidQuery);
    }
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    connection.pragma_update(None, "query_only", "ON")?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    let opened_at = Instant::now();
    let deadline = opened_at + max_age;
    connection.progress_handler(1_000, Some(move || Instant::now() >= deadline));
    Ok(ReaderHandle {
        connection,
        opened_at,
        deadline,
        max_rows,
    })
}

fn reject_non_file(path: &Path) -> rusqlite::Result<()> {
    if path == Path::new(":memory:") || path.as_os_str().is_empty() {
        return Err(rusqlite::Error::InvalidPath(PathBuf::from(path)));
    }
    Ok(())
}

pub fn apply_migrations(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute_batch("BEGIN IMMEDIATE")?;
    let result = (|| {
        connection.execute_batch(MIGRATION_1)?;
        connection.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, name, checksum, applied_at) VALUES (1, 'initial', ?1, strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            [MIGRATION_1_CHECKSUM],
        )?;
        let checksum: String = connection.query_row(
            "SELECT checksum FROM schema_migrations WHERE version=1",
            [],
            |r| r.get(0),
        )?;
        if checksum != MIGRATION_1_CHECKSUM {
            return Err(rusqlite::Error::InvalidQuery);
        }
        let has_v2: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=2)",
            [],
            |r| r.get(0),
        )?;
        if !has_v2 {
            connection.execute_batch(MIGRATION_2)?;
            connection.execute(
                "INSERT INTO schema_migrations(version,name,checksum,applied_at) VALUES(2,'anchor-audit-cas-hardening',?1,strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
                [MIGRATION_2_CHECKSUM],
            )?;
        }
        let checksum: String = connection.query_row(
            "SELECT checksum FROM schema_migrations WHERE version=2",
            [],
            |r| r.get(0),
        )?;
        if checksum != MIGRATION_2_CHECKSUM {
            return Err(rusqlite::Error::InvalidQuery);
        }
        Ok(())
    })();
    match result {
        Ok(()) => connection.execute_batch("COMMIT"),
        Err(error) => {
            let _ = connection.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

const MIGRATION_1_CHECKSUM: &str =
    "sha256:0928c0dbd4dc4e4ca158be5a35518981f7909c9a8f0b85e6139675ee4a72b18f";

// LSN columns are canonical zero-padded uppercase 16-hex strings, so SQLite bytewise
// ordering equals PostgreSQL unsigned LSN ordering without signed arithmetic.
pub const MIGRATION_1: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations(
 version INTEGER PRIMARY KEY CHECK(version>0), name TEXT NOT NULL UNIQUE,
 checksum TEXT NOT NULL CHECK(length(checksum)>0), applied_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS source_transactions(
 transaction_id TEXT PRIMARY KEY, capture_epoch TEXT NOT NULL, source_system_id TEXT NOT NULL,
 database_id TEXT NOT NULL, slot_name TEXT NOT NULL, xid TEXT NOT NULL, end_lsn TEXT NOT NULL CHECK(length(end_lsn)=16 AND end_lsn NOT GLOB '*[^0-9A-F]*'),
 first_seq INTEGER NOT NULL CHECK(first_seq>0), last_seq INTEGER NOT NULL CHECK(last_seq>=first_seq), event_count INTEGER NOT NULL CHECK(event_count>=0),
 payload_checksum TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('committed','gc_removed')),
 UNIQUE(capture_epoch,source_system_id,database_id,slot_name,end_lsn), UNIQUE(capture_epoch,last_seq), UNIQUE(transaction_id,capture_epoch,last_seq), UNIQUE(transaction_id,capture_epoch,last_seq,end_lsn));
CREATE TABLE IF NOT EXISTS relation_schemas(
 schema_fingerprint TEXT PRIMARY KEY, capture_epoch TEXT NOT NULL, relation_id TEXT NOT NULL,
 canonical_schema BLOB NOT NULL, schema_checksum TEXT NOT NULL, created_seq INTEGER NOT NULL CHECK(created_seq>0),
 UNIQUE(capture_epoch,relation_id,schema_fingerprint));
CREATE TABLE IF NOT EXISTS journal_events(
 journal_seq INTEGER PRIMARY KEY CHECK(journal_seq>0), event_id TEXT NOT NULL UNIQUE,
 transaction_id TEXT NOT NULL REFERENCES source_transactions(transaction_id), transaction_ordinal INTEGER NOT NULL CHECK(transaction_ordinal>=0),
 capture_epoch TEXT NOT NULL, relation_schema_fingerprint TEXT REFERENCES relation_schemas(schema_fingerprint),
 control_kind TEXT CHECK(control_kind IN ('heartbeat','capture_fence') OR control_kind IS NULL), payload BLOB NOT NULL, payload_hash TEXT NOT NULL,
 UNIQUE(transaction_id,transaction_ordinal));
CREATE TABLE IF NOT EXISTS bootstrap_intents(
 intent_id TEXT PRIMARY KEY, capture_epoch TEXT NOT NULL, source_system_id TEXT NOT NULL, database_id TEXT NOT NULL, slot_name TEXT NOT NULL,
 creation_floor_lsn TEXT CHECK(creation_floor_lsn IS NULL OR (length(creation_floor_lsn)=16 AND creation_floor_lsn NOT GLOB '*[^0-9A-F]*')),
 state TEXT NOT NULL CHECK(state IN ('prepared','remote_slot_unknown','slot_created','copying','complete','invalidated','aborted')),
 revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0), created_at TEXT NOT NULL,
 UNIQUE(intent_id,capture_epoch,source_system_id,database_id,slot_name,creation_floor_lsn));
CREATE TABLE IF NOT EXISTS source_state(
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
CREATE TABLE IF NOT EXISTS runtime_ownership(
 run_id TEXT PRIMARY KEY, backend_pid INTEGER NOT NULL, connection_nonce TEXT NOT NULL UNIQUE, ownership_deadline_mono_ms INTEGER NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('held','expected_close','lost','released')), connection_generation INTEGER NOT NULL CHECK(connection_generation>0),
 expected_close_token TEXT UNIQUE, loss_evidence TEXT, takeover_evidence TEXT, revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0));
CREATE TABLE IF NOT EXISTS operator_command_requests(
 request_id TEXT PRIMARY KEY, dry_run_nonce TEXT NOT NULL UNIQUE, canonical_payload BLOB NOT NULL, payload_digest TEXT NOT NULL,
 run_id TEXT NOT NULL, peer_identity TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('accepted','reconciling','completed','failed','aborted_by_restart')),
 result BLOB, observation_revision INTEGER NOT NULL, control_revision INTEGER NOT NULL, expires_at TEXT NOT NULL,
 CHECK((state IN ('completed','failed'))=(result IS NOT NULL)), UNIQUE(request_id,payload_digest));
CREATE TABLE IF NOT EXISTS destinations(
 destination_id TEXT PRIMARY KEY, kind TEXT NOT NULL CHECK(kind IN ('clickhouse','archive')), configuration_fingerprint TEXT NOT NULL,
 capture_epoch TEXT NOT NULL, generation INTEGER NOT NULL CHECK(generation>0), highest_external_fence INTEGER NOT NULL DEFAULT 0 CHECK(highest_external_fence>=0),
 adopted_external_fence_at TEXT, adopted_external_fence_evidence TEXT, current_failure_id TEXT,
 FOREIGN KEY(current_failure_id,destination_id) REFERENCES processing_failures(failure_id,destination_id));
CREATE TABLE IF NOT EXISTS processing_failures(
 failure_id TEXT PRIMARY KEY, destination_id TEXT REFERENCES destinations(destination_id), component TEXT NOT NULL, failure_class TEXT NOT NULL,
 fingerprint TEXT NOT NULL, failed_boundary_start_seq INTEGER, failed_boundary_end_seq INTEGER, retry_class TEXT NOT NULL CHECK(retry_class IN ('transient','deterministic','exhausted','integrity_mismatch')),
 attempt INTEGER NOT NULL DEFAULT 0 CHECK(attempt>=0), next_retry_at TEXT, armed INTEGER NOT NULL DEFAULT 0 CHECK(armed IN (0,1)), first_failed_at TEXT NOT NULL, last_failed_at TEXT NOT NULL,
 CHECK((failed_boundary_start_seq IS NULL)=(failed_boundary_end_seq IS NULL)), CHECK(failed_boundary_end_seq IS NULL OR failed_boundary_end_seq>=failed_boundary_start_seq),
 CHECK((retry_class='transient' AND next_retry_at IS NOT NULL) OR (retry_class!='transient' AND next_retry_at IS NULL)),
 UNIQUE(component,fingerprint,failed_boundary_start_seq,failed_boundary_end_seq), UNIQUE(failure_id,destination_id));
CREATE TABLE IF NOT EXISTS destination_checkpoints(
 destination_id TEXT PRIMARY KEY REFERENCES destinations(destination_id), capture_epoch TEXT NOT NULL, anchor_id TEXT,
 configuration_fingerprint TEXT NOT NULL, generation INTEGER NOT NULL CHECK(generation>0), complete_transaction_id TEXT NOT NULL REFERENCES source_transactions(transaction_id),
 journal_seq INTEGER NOT NULL, current_failure_id TEXT, revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0),
 FOREIGN KEY(complete_transaction_id,capture_epoch,journal_seq) REFERENCES source_transactions(transaction_id,capture_epoch,last_seq), FOREIGN KEY(current_failure_id,destination_id) REFERENCES processing_failures(failure_id,destination_id));
CREATE TABLE IF NOT EXISTS backfill_runs(run_id TEXT PRIMARY KEY, destination_id TEXT NOT NULL REFERENCES destinations(destination_id), capture_epoch TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('prepared','running','paused','complete','invalidated')), revision INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS backfill_generations(generation_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES backfill_runs(run_id), generation INTEGER NOT NULL CHECK(generation>0), intended_fence_nonce TEXT UNIQUE, state TEXT NOT NULL CHECK(state IN ('prepared','copying','fencing','complete','invalidated')), UNIQUE(run_id,generation));
CREATE TABLE IF NOT EXISTS backfill_chunks(chunk_id TEXT PRIMARY KEY, generation_id TEXT NOT NULL REFERENCES backfill_generations(generation_id), range_start BLOB NOT NULL, range_end BLOB NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending','complete','invalidated')), completed_seq INTEGER, checksum TEXT, CHECK(state!='complete' OR (completed_seq IS NOT NULL AND checksum IS NOT NULL)));
CREATE TABLE IF NOT EXISTS bootstrap_imports(import_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL REFERENCES bootstrap_intents(intent_id), destination_id TEXT NOT NULL REFERENCES destinations(destination_id), state TEXT NOT NULL CHECK(state IN ('prepared','importing','acknowledged','failed','invalidated')), revision INTEGER NOT NULL DEFAULT 0, UNIQUE(intent_id,destination_id));
CREATE TABLE IF NOT EXISTS durable_capture_fences(fence_id TEXT PRIMARY KEY, capture_epoch TEXT NOT NULL, generation INTEGER NOT NULL, nonce TEXT NOT NULL, transaction_id TEXT NOT NULL REFERENCES source_transactions(transaction_id), post_copy_fence_lsn TEXT NOT NULL CHECK(length(post_copy_fence_lsn)=16 AND post_copy_fence_lsn NOT GLOB '*[^0-9A-F]*'), post_copy_fence_seq INTEGER NOT NULL, first_proof INTEGER NOT NULL CHECK(first_proof IN (0,1)), FOREIGN KEY(transaction_id,capture_epoch,post_copy_fence_seq) REFERENCES source_transactions(transaction_id,capture_epoch,last_seq), UNIQUE(capture_epoch,generation,nonce,transaction_id));
CREATE UNIQUE INDEX IF NOT EXISTS one_first_capture_fence_proof ON durable_capture_fences(capture_epoch,generation,nonce) WHERE first_proof=1;
CREATE TABLE IF NOT EXISTS bootstrap_anchors(anchor_id TEXT PRIMARY KEY, capture_epoch TEXT NOT NULL, generation INTEGER NOT NULL, lower_stitch_lsn TEXT CHECK(lower_stitch_lsn IS NULL OR (length(lower_stitch_lsn)=16 AND lower_stitch_lsn NOT GLOB '*[^0-9A-F]*')), start_seq INTEGER NOT NULL CHECK(start_seq>=0), snapshot_boundary_lsn TEXT NOT NULL CHECK(length(snapshot_boundary_lsn)=16 AND snapshot_boundary_lsn NOT GLOB '*[^0-9A-F]*'), snapshot_complete_seq INTEGER, post_copy_fence_nonce TEXT, post_copy_fence_lsn TEXT CHECK(post_copy_fence_lsn IS NULL OR (length(post_copy_fence_lsn)=16 AND post_copy_fence_lsn NOT GLOB '*[^0-9A-F]*')), post_copy_fence_seq INTEGER, table_set_fingerprint TEXT NOT NULL, snapshot_schema_fingerprints TEXT NOT NULL CHECK(json_valid(snapshot_schema_fingerprints) AND json_type(snapshot_schema_fingerprints)='array' AND json_array_length(snapshot_schema_fingerprints)>0), state TEXT NOT NULL CHECK(state IN ('building','complete','expired','invalidated')), expires_at TEXT NOT NULL, CHECK(state!='complete' OR (snapshot_complete_seq IS NOT NULL AND post_copy_fence_nonce IS NOT NULL AND post_copy_fence_lsn IS NOT NULL AND post_copy_fence_seq IS NOT NULL)), UNIQUE(capture_epoch,generation));
CREATE TABLE IF NOT EXISTS reseed_intents(intent_id TEXT PRIMARY KEY, destination_id TEXT REFERENCES destinations(destination_id), capture_epoch TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('prepared','reconciling','ready','complete','blocked','aborted')), revision INTEGER NOT NULL DEFAULT 0, evidence_digest TEXT);
CREATE TABLE IF NOT EXISTS destination_generation_leases(lease_id TEXT PRIMARY KEY, destination_id TEXT NOT NULL REFERENCES destinations(destination_id), capture_epoch TEXT NOT NULL, anchor_id TEXT REFERENCES bootstrap_anchors(anchor_id), generation INTEGER NOT NULL, configuration_fingerprint TEXT NOT NULL, run_id TEXT NOT NULL, expires_mono_ms INTEGER NOT NULL, state TEXT NOT NULL CHECK(state IN ('held','fenced','expired','released')), revision INTEGER NOT NULL DEFAULT 0, UNIQUE(destination_id,capture_epoch,generation));
CREATE TABLE IF NOT EXISTS destination_promotion_intents(intent_id TEXT PRIMARY KEY, destination_id TEXT NOT NULL REFERENCES destinations(destination_id), capture_epoch TEXT NOT NULL, old_generation INTEGER, candidate_generation INTEGER NOT NULL, anchor_id TEXT NOT NULL REFERENCES bootstrap_anchors(anchor_id), configuration_fingerprint TEXT NOT NULL, promotion_fence INTEGER NOT NULL CHECK(promotion_fence>0), expected_selector_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('prepared','old_leases_fenced','candidate_verified','switch_pending','switched','verified','retirement_eligible','retired','promotion_recovery_required')), revision INTEGER NOT NULL DEFAULT 0, UNIQUE(destination_id,promotion_fence));
CREATE TABLE IF NOT EXISTS clickhouse_batch_intents(intent_id TEXT PRIMARY KEY, destination_id TEXT NOT NULL REFERENCES destinations(destination_id), capture_epoch TEXT NOT NULL, generation INTEGER NOT NULL, first_seq INTEGER NOT NULL, last_seq INTEGER NOT NULL CHECK(last_seq>=first_seq), payload_checksum TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('prepared','dispatched','verified','failed')), UNIQUE(destination_id,capture_epoch,generation,first_seq,last_seq));
CREATE TABLE IF NOT EXISTS archive_generations(generation_id TEXT PRIMARY KEY, destination_id TEXT NOT NULL REFERENCES destinations(destination_id), capture_epoch TEXT NOT NULL, generation INTEGER NOT NULL, anchor_id TEXT REFERENCES bootstrap_anchors(anchor_id), state TEXT NOT NULL CHECK(state IN ('candidate','live','retirement_eligible','retired','invalidated')), UNIQUE(destination_id,capture_epoch,generation));
CREATE TABLE IF NOT EXISTS archive_segment_intents(intent_id TEXT PRIMARY KEY, generation_id TEXT NOT NULL REFERENCES archive_generations(generation_id), first_seq INTEGER NOT NULL, last_seq INTEGER NOT NULL CHECK(last_seq>=first_seq), selection_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('selected','writing','published','failed')), UNIQUE(generation_id,first_seq,last_seq));
CREATE TABLE IF NOT EXISTS archive_segments(segment_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL UNIQUE REFERENCES archive_segment_intents(intent_id), manifest_digest TEXT NOT NULL, ready_marker_digest TEXT NOT NULL, first_seq INTEGER NOT NULL, last_seq INTEGER NOT NULL CHECK(last_seq>=first_seq));
CREATE TABLE IF NOT EXISTS archive_generation_markers(marker_id TEXT PRIMARY KEY, generation_id TEXT NOT NULL REFERENCES archive_generations(generation_id), promotion_fence INTEGER NOT NULL CHECK(promotion_fence>0), intent_id TEXT NOT NULL REFERENCES destination_promotion_intents(intent_id), marker_digest TEXT NOT NULL, UNIQUE(generation_id,promotion_fence), UNIQUE(promotion_fence,marker_digest));
CREATE TABLE IF NOT EXISTS destination_audits(audit_id TEXT PRIMARY KEY, destination_id TEXT NOT NULL REFERENCES destinations(destination_id), configuration_fingerprint TEXT NOT NULL, capture_epoch TEXT NOT NULL, generation INTEGER NOT NULL, round_target_seq INTEGER NOT NULL, round_identity_digest TEXT NOT NULL, journal_cursor_seq INTEGER NOT NULL, journal_verified_start_seq INTEGER, journal_verified_end_seq INTEGER, self_cursor_seq INTEGER, self_consistent_start_seq INTEGER, self_consistent_end_seq INTEGER, budget_bytes_used INTEGER NOT NULL DEFAULT 0, budget_events_used INTEGER NOT NULL DEFAULT 0, budget_ms_used INTEGER NOT NULL DEFAULT 0, freshness_window_started_at TEXT NOT NULL, freshness_expires_at TEXT NOT NULL, contract_digest TEXT NOT NULL, evidence_digest TEXT, first_mismatch TEXT, revision INTEGER NOT NULL DEFAULT 0,
 CHECK(round_target_seq>=0 AND journal_cursor_seq>=0 AND journal_cursor_seq<=round_target_seq AND self_cursor_seq>=0 AND self_cursor_seq<=round_target_seq),
 CHECK(budget_bytes_used>=0 AND budget_events_used>=0 AND budget_ms_used>=0),
 CHECK((journal_verified_start_seq IS NULL)=(journal_verified_end_seq IS NULL)), CHECK(journal_verified_end_seq IS NULL OR (journal_verified_end_seq>=journal_verified_start_seq AND journal_verified_end_seq<=round_target_seq)),
 CHECK((self_consistent_start_seq IS NULL)=(self_consistent_end_seq IS NULL)), CHECK(self_consistent_end_seq IS NULL OR (self_consistent_end_seq>=self_consistent_start_seq AND self_consistent_end_seq<=round_target_seq)),
 CHECK(freshness_expires_at>freshness_window_started_at));
CREATE TABLE IF NOT EXISTS condition_hysteresis(condition_id TEXT PRIMARY KEY, entered_at TEXT, last_observed_at TEXT NOT NULL, cleared_at TEXT);
CREATE TABLE IF NOT EXISTS alerts(alert_id TEXT PRIMARY KEY, condition_id TEXT NOT NULL, failure_id TEXT REFERENCES processing_failures(failure_id), state TEXT NOT NULL CHECK(state IN ('active','acknowledged','cleared')), opened_at TEXT NOT NULL, cleared_at TEXT, evidence_digest TEXT);
CREATE TRIGGER IF NOT EXISTS relation_schemas_immutable BEFORE UPDATE ON relation_schemas BEGIN SELECT RAISE(ABORT,'relation schema is immutable'); END;
CREATE TRIGGER IF NOT EXISTS journal_events_immutable BEFORE UPDATE ON journal_events BEGIN SELECT RAISE(ABORT,'journal event is immutable'); END;
CREATE TRIGGER IF NOT EXISTS source_transactions_immutable BEFORE UPDATE OF capture_epoch,source_system_id,database_id,slot_name,end_lsn,first_seq,last_seq,event_count,payload_checksum ON source_transactions BEGIN SELECT RAISE(ABORT,'source transaction identity and checksum are immutable'); END;
CREATE TRIGGER IF NOT EXISTS archive_selection_immutable BEFORE UPDATE OF generation_id,first_seq,last_seq,selection_digest ON archive_segment_intents BEGIN SELECT RAISE(ABORT,'archive selection is immutable'); END;
CREATE TRIGGER IF NOT EXISTS destination_fence_monotonic BEFORE UPDATE OF highest_external_fence ON destinations WHEN NEW.highest_external_fence < OLD.highest_external_fence BEGIN SELECT RAISE(ABORT,'external fence regression'); END;
CREATE TRIGGER IF NOT EXISTS source_progress_monotonic BEFORE UPDATE OF durable_transaction_end_lsn ON source_state WHEN OLD.durable_transaction_end_lsn IS NOT NULL AND (NEW.durable_transaction_end_lsn IS NULL OR NEW.durable_transaction_end_lsn < OLD.durable_transaction_end_lsn) BEGIN SELECT RAISE(ABORT,'durable source progress regression'); END;
CREATE TRIGGER IF NOT EXISTS complete_anchor_requires_fence BEFORE INSERT ON bootstrap_anchors WHEN NEW.state='complete' AND (NOT EXISTS(SELECT 1 FROM durable_capture_fences f WHERE f.capture_epoch=NEW.capture_epoch AND f.generation=NEW.generation AND f.nonce=NEW.post_copy_fence_nonce AND f.post_copy_fence_lsn=NEW.post_copy_fence_lsn AND f.post_copy_fence_seq=NEW.post_copy_fence_seq AND f.first_proof=1) OR NOT EXISTS(SELECT 1 FROM backfill_generations g JOIN backfill_runs r ON r.run_id=g.run_id WHERE r.capture_epoch=NEW.capture_epoch AND g.generation=NEW.generation AND g.state='complete') OR NOT EXISTS(SELECT 1 FROM backfill_chunks c JOIN backfill_generations g ON g.generation_id=c.generation_id JOIN backfill_runs r ON r.run_id=g.run_id WHERE r.capture_epoch=NEW.capture_epoch AND g.generation=NEW.generation AND c.state='complete') OR EXISTS(SELECT 1 FROM backfill_chunks c JOIN backfill_generations g ON g.generation_id=c.generation_id JOIN backfill_runs r ON r.run_id=g.run_id WHERE r.capture_epoch=NEW.capture_epoch AND g.generation=NEW.generation AND c.state!='complete') OR NOT EXISTS(SELECT 1 FROM bootstrap_imports i JOIN bootstrap_intents b ON b.intent_id=i.intent_id WHERE b.capture_epoch=NEW.capture_epoch AND i.state='acknowledged') OR EXISTS(SELECT 1 FROM json_each(NEW.snapshot_schema_fingerprints) j LEFT JOIN relation_schemas rs ON rs.schema_fingerprint=j.value AND rs.capture_epoch=NEW.capture_epoch WHERE rs.schema_fingerprint IS NULL) OR (NEW.lower_stitch_lsn IS NOT NULL AND NOT EXISTS(SELECT 1 FROM source_transactions t WHERE t.capture_epoch=NEW.capture_epoch AND t.end_lsn=NEW.lower_stitch_lsn AND t.last_seq=NEW.start_seq))) BEGIN SELECT RAISE(ABORT,'complete anchor lacks matching durable fence'); END;
CREATE TRIGGER IF NOT EXISTS complete_anchor_update_requires_fence BEFORE UPDATE OF state ON bootstrap_anchors WHEN NEW.state='complete' AND (NOT EXISTS(SELECT 1 FROM durable_capture_fences f WHERE f.capture_epoch=NEW.capture_epoch AND f.generation=NEW.generation AND f.nonce=NEW.post_copy_fence_nonce AND f.post_copy_fence_lsn=NEW.post_copy_fence_lsn AND f.post_copy_fence_seq=NEW.post_copy_fence_seq AND f.first_proof=1) OR NOT EXISTS(SELECT 1 FROM backfill_generations g JOIN backfill_runs r ON r.run_id=g.run_id WHERE r.capture_epoch=NEW.capture_epoch AND g.generation=NEW.generation AND g.state='complete') OR NOT EXISTS(SELECT 1 FROM backfill_chunks c JOIN backfill_generations g ON g.generation_id=c.generation_id JOIN backfill_runs r ON r.run_id=g.run_id WHERE r.capture_epoch=NEW.capture_epoch AND g.generation=NEW.generation AND c.state='complete') OR EXISTS(SELECT 1 FROM backfill_chunks c JOIN backfill_generations g ON g.generation_id=c.generation_id JOIN backfill_runs r ON r.run_id=g.run_id WHERE r.capture_epoch=NEW.capture_epoch AND g.generation=NEW.generation AND c.state!='complete') OR NOT EXISTS(SELECT 1 FROM bootstrap_imports i JOIN bootstrap_intents b ON b.intent_id=i.intent_id WHERE b.capture_epoch=NEW.capture_epoch AND i.state='acknowledged') OR EXISTS(SELECT 1 FROM json_each(NEW.snapshot_schema_fingerprints) j LEFT JOIN relation_schemas rs ON rs.schema_fingerprint=j.value AND rs.capture_epoch=NEW.capture_epoch WHERE rs.schema_fingerprint IS NULL) OR (NEW.lower_stitch_lsn IS NOT NULL AND NOT EXISTS(SELECT 1 FROM source_transactions t WHERE t.capture_epoch=NEW.capture_epoch AND t.end_lsn=NEW.lower_stitch_lsn AND t.last_seq=NEW.start_seq))) BEGIN SELECT RAISE(ABORT,'complete anchor lacks matching durable fence'); END;
CREATE TRIGGER IF NOT EXISTS complete_anchor_immutable BEFORE UPDATE ON bootstrap_anchors WHEN OLD.state='complete' BEGIN SELECT RAISE(ABORT,'complete anchor is immutable'); END;
CREATE TRIGGER IF NOT EXISTS source_floor_requires_nonterminal_intent BEFORE INSERT ON source_state WHEN NEW.slot_creation_intent_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM bootstrap_intents b WHERE b.intent_id=NEW.slot_creation_intent_id AND b.state NOT IN ('complete','invalidated','aborted')) BEGIN SELECT RAISE(ABORT,'creation floor intent is terminal'); END;
CREATE TRIGGER IF NOT EXISTS source_floor_update_requires_nonterminal_intent BEFORE UPDATE OF slot_creation_intent_id,slot_creation_floor_lsn ON source_state WHEN NEW.slot_creation_intent_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM bootstrap_intents b WHERE b.intent_id=NEW.slot_creation_intent_id AND b.state NOT IN ('complete','invalidated','aborted')) BEGIN SELECT RAISE(ABORT,'creation floor intent is terminal'); END;
CREATE TRIGGER IF NOT EXISTS referenced_bootstrap_intent_stays_nonterminal BEFORE UPDATE OF state ON bootstrap_intents WHEN NEW.state IN ('complete','invalidated','aborted') AND EXISTS(SELECT 1 FROM source_state s WHERE s.slot_creation_intent_id=OLD.intent_id) BEGIN SELECT RAISE(ABORT,'referenced creation-floor intent cannot become terminal'); END;
CREATE TRIGGER IF NOT EXISTS promotion_fence_above_high_water BEFORE INSERT ON destination_promotion_intents WHEN NEW.promotion_fence <= (SELECT highest_external_fence FROM destinations WHERE destination_id=NEW.destination_id) BEGIN SELECT RAISE(ABORT,'promotion fence is not above high-water'); END;
CREATE TRIGGER IF NOT EXISTS promotion_fence_allocated AFTER INSERT ON destination_promotion_intents BEGIN UPDATE destinations SET highest_external_fence=NEW.promotion_fence WHERE destination_id=NEW.destination_id; END;
"#;

const MIGRATION_2_CHECKSUM: &str =
    "sha256:eb434cd6a33109e6ecafebc92c510763f9bd964944079b895c018e737e36ee3b";
const MIGRATION_2: &str = r#"
DROP TRIGGER complete_anchor_immutable;
DROP TRIGGER promotion_fence_allocated;
ALTER TABLE bootstrap_anchors ADD COLUMN generation_id TEXT;
ALTER TABLE bootstrap_anchors ADD COLUMN bootstrap_intent_id TEXT;
UPDATE bootstrap_anchors SET state='invalidated' WHERE state='complete';
ALTER TABLE destinations ADD COLUMN revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0);
ALTER TABLE operator_command_requests ADD COLUMN request_revision INTEGER NOT NULL DEFAULT 0 CHECK(request_revision>=0);
ALTER TABLE destination_audits ADD COLUMN retained_history_start_seq INTEGER CHECK(retained_history_start_seq IS NULL OR retained_history_start_seq>=0);
ALTER TABLE destination_audits ADD COLUMN unverifiable_before_seq INTEGER CHECK(unverifiable_before_seq IS NULL OR unverifiable_before_seq>=0);
CREATE TABLE audit_coverage_subranges(
 audit_id TEXT NOT NULL REFERENCES destination_audits(audit_id) ON DELETE CASCADE,
 coverage_kind TEXT NOT NULL CHECK(coverage_kind IN ('journal_verified','self_consistent')),
 start_seq INTEGER NOT NULL CHECK(start_seq>=0), end_seq INTEGER NOT NULL CHECK(end_seq>=start_seq),
 fresh_until TEXT NOT NULL, evidence_digest TEXT NOT NULL,
 PRIMARY KEY(audit_id,coverage_kind,start_seq,end_seq));
CREATE TRIGGER complete_anchor_immutable BEFORE UPDATE ON bootstrap_anchors WHEN OLD.state='complete' BEGIN SELECT RAISE(ABORT,'complete anchor is immutable'); END;
CREATE TRIGGER promotion_fence_allocated AFTER INSERT ON destination_promotion_intents BEGIN UPDATE destinations SET highest_external_fence=NEW.promotion_fence,revision=revision+1 WHERE destination_id=NEW.destination_id; END;
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
CREATE TRIGGER complete_anchor_blocks_generation_change BEFORE UPDATE ON backfill_generations WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.generation_id=OLD.generation_id) BEGIN SELECT RAISE(ABORT,'anchor generation proof is immutable'); END;
CREATE TRIGGER complete_anchor_blocks_chunk_change BEFORE UPDATE ON backfill_chunks WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.generation_id=OLD.generation_id) BEGIN SELECT RAISE(ABORT,'anchor chunk proof is immutable'); END;
CREATE TRIGGER complete_anchor_blocks_chunk_delete BEFORE DELETE ON backfill_chunks WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.generation_id=OLD.generation_id) BEGIN SELECT RAISE(ABORT,'anchor chunk proof is immutable'); END;
CREATE TRIGGER complete_anchor_blocks_import_change BEFORE UPDATE ON bootstrap_imports WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.bootstrap_intent_id=OLD.intent_id) BEGIN SELECT RAISE(ABORT,'anchor import proof is immutable'); END;
CREATE TRIGGER complete_anchor_blocks_import_delete BEFORE DELETE ON bootstrap_imports WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.bootstrap_intent_id=OLD.intent_id) BEGIN SELECT RAISE(ABORT,'anchor import proof is immutable'); END;
CREATE TRIGGER complete_anchor_blocks_fence_delete BEFORE DELETE ON durable_capture_fences WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.capture_epoch=OLD.capture_epoch AND a.generation=OLD.generation AND a.post_copy_fence_nonce=OLD.nonce) BEGIN SELECT RAISE(ABORT,'anchor fence proof is immutable'); END;
CREATE TRIGGER complete_anchor_blocks_schema_delete BEFORE DELETE ON relation_schemas WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a,json_each(a.snapshot_schema_fingerprints) j WHERE a.state='complete' AND a.capture_epoch=OLD.capture_epoch AND j.value=OLD.schema_fingerprint) BEGIN SELECT RAISE(ABORT,'anchor schema proof is immutable'); END;
CREATE TRIGGER bootstrap_intent_transition BEFORE UPDATE OF state ON bootstrap_intents WHEN NOT ((OLD.state='prepared' AND NEW.state IN ('remote_slot_unknown','slot_created','aborted')) OR (OLD.state='remote_slot_unknown' AND NEW.state IN ('slot_created','aborted')) OR (OLD.state='slot_created' AND NEW.state IN ('copying','invalidated','aborted')) OR (OLD.state='copying' AND NEW.state IN ('complete','invalidated','aborted')) OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid bootstrap transition'); END;
CREATE TRIGGER bootstrap_intent_revision BEFORE UPDATE ON bootstrap_intents WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale bootstrap revision'); END;
CREATE TRIGGER bootstrap_import_transition BEFORE UPDATE OF state ON bootstrap_imports WHEN NOT ((OLD.state='prepared' AND NEW.state IN ('importing','failed','invalidated')) OR (OLD.state='importing' AND NEW.state IN ('acknowledged','failed','invalidated')) OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid import transition'); END;
CREATE TRIGGER bootstrap_import_revision BEFORE UPDATE ON bootstrap_imports WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale import revision'); END;
CREATE TRIGGER lease_transition BEFORE UPDATE OF state ON destination_generation_leases WHEN NOT ((OLD.state='held' AND NEW.state IN ('fenced','expired','released')) OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid lease transition'); END;
CREATE TRIGGER lease_revision BEFORE UPDATE ON destination_generation_leases WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale lease revision'); END;
CREATE TRIGGER promotion_transition BEFORE UPDATE OF state ON destination_promotion_intents WHEN NOT ((OLD.state='prepared' AND NEW.state IN ('old_leases_fenced','promotion_recovery_required')) OR (OLD.state='old_leases_fenced' AND NEW.state IN ('candidate_verified','promotion_recovery_required')) OR (OLD.state='candidate_verified' AND NEW.state IN ('switch_pending','promotion_recovery_required')) OR (OLD.state='switch_pending' AND NEW.state IN ('switched','promotion_recovery_required')) OR (OLD.state='switched' AND NEW.state IN ('verified','promotion_recovery_required')) OR (OLD.state='verified' AND NEW.state='retirement_eligible') OR (OLD.state='retirement_eligible' AND NEW.state='retired') OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid promotion transition'); END;
CREATE TRIGGER promotion_revision BEFORE UPDATE ON destination_promotion_intents WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale promotion revision'); END;
CREATE TRIGGER destination_revision BEFORE UPDATE ON destinations WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale destination revision'); END;
CREATE TRIGGER ownership_transition BEFORE UPDATE OF state ON runtime_ownership WHEN NOT ((OLD.state='held' AND NEW.state IN ('expected_close','lost','released')) OR (OLD.state='expected_close' AND NEW.state IN ('lost','released')) OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid ownership transition'); END;
CREATE TRIGGER ownership_revision BEFORE UPDATE ON runtime_ownership WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale ownership revision'); END;
CREATE TRIGGER command_transition BEFORE UPDATE OF state ON operator_command_requests WHEN NOT ((OLD.state='accepted' AND NEW.state IN ('reconciling','completed','failed','aborted_by_restart')) OR (OLD.state='reconciling' AND NEW.state IN ('completed','failed','aborted_by_restart')) OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid command transition'); END;
CREATE TRIGGER backfill_run_transition BEFORE UPDATE OF state ON backfill_runs WHEN NOT ((OLD.state='prepared' AND NEW.state IN ('running','invalidated')) OR (OLD.state='running' AND NEW.state IN ('paused','complete','invalidated')) OR (OLD.state='paused' AND NEW.state IN ('running','invalidated')) OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid backfill-run transition'); END;
CREATE TRIGGER backfill_run_revision BEFORE UPDATE ON backfill_runs WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale backfill-run revision'); END;
CREATE TRIGGER backfill_generation_transition BEFORE UPDATE OF state ON backfill_generations WHEN NOT ((OLD.state='prepared' AND NEW.state IN ('copying','invalidated')) OR (OLD.state='copying' AND NEW.state IN ('fencing','invalidated')) OR (OLD.state='fencing' AND NEW.state IN ('complete','invalidated')) OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid backfill-generation transition'); END;
CREATE TRIGGER backfill_chunk_transition BEFORE UPDATE OF state ON backfill_chunks WHEN NOT (OLD.state='pending' AND NEW.state IN ('complete','invalidated') OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid backfill-chunk transition'); END;
CREATE TRIGGER reseed_transition BEFORE UPDATE OF state ON reseed_intents WHEN NOT ((OLD.state='prepared' AND NEW.state IN ('reconciling','blocked','aborted')) OR (OLD.state='reconciling' AND NEW.state IN ('ready','blocked','aborted')) OR (OLD.state='ready' AND NEW.state IN ('complete','blocked','aborted')) OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid reseed transition'); END;
CREATE TRIGGER reseed_revision BEFORE UPDATE ON reseed_intents WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale reseed revision'); END;
CREATE TRIGGER batch_transition BEFORE UPDATE OF state ON clickhouse_batch_intents WHEN NOT ((OLD.state='prepared' AND NEW.state IN ('dispatched','failed')) OR (OLD.state='dispatched' AND NEW.state IN ('verified','failed')) OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid batch transition'); END;
CREATE TRIGGER archive_generation_transition BEFORE UPDATE OF state ON archive_generations WHEN NOT ((OLD.state='candidate' AND NEW.state IN ('live','invalidated')) OR (OLD.state='live' AND NEW.state IN ('retirement_eligible','invalidated')) OR (OLD.state='retirement_eligible' AND NEW.state='retired') OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid archive-generation transition'); END;
CREATE TRIGGER archive_segment_transition BEFORE UPDATE OF state ON archive_segment_intents WHEN NOT ((OLD.state='selected' AND NEW.state IN ('writing','failed')) OR (OLD.state='writing' AND NEW.state IN ('published','failed')) OR NEW.state=OLD.state) BEGIN SELECT RAISE(ABORT,'invalid archive-segment transition'); END;
CREATE TRIGGER audit_revision BEFORE UPDATE ON destination_audits WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale audit revision'); END;
CREATE TRIGGER complete_anchor_blocks_generation_delete BEFORE DELETE ON backfill_generations WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.generation_id=OLD.generation_id) BEGIN SELECT RAISE(ABORT,'anchor generation proof is immutable'); END;
CREATE TRIGGER complete_anchor_blocks_chunk_insert BEFORE INSERT ON backfill_chunks WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.generation_id=NEW.generation_id) BEGIN SELECT RAISE(ABORT,'anchor chunk set is immutable'); END;
CREATE TRIGGER complete_anchor_blocks_import_insert BEFORE INSERT ON bootstrap_imports WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.bootstrap_intent_id=NEW.intent_id) BEGIN SELECT RAISE(ABORT,'anchor importer set is immutable'); END;
CREATE TRIGGER complete_anchor_blocks_fence_update BEFORE UPDATE ON durable_capture_fences WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.capture_epoch=OLD.capture_epoch AND a.generation=OLD.generation AND a.post_copy_fence_nonce=OLD.nonce) BEGIN SELECT RAISE(ABORT,'anchor fence proof is immutable'); END;
CREATE TRIGGER complete_anchor_blocks_run_update BEFORE UPDATE ON backfill_runs WHEN EXISTS(SELECT 1 FROM backfill_generations g JOIN bootstrap_anchors a ON a.generation_id=g.generation_id WHERE a.state='complete' AND g.run_id=OLD.run_id) BEGIN SELECT RAISE(ABORT,'anchor run proof is immutable'); END;
CREATE TRIGGER complete_anchor_blocks_intent_update BEFORE UPDATE ON bootstrap_intents WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.bootstrap_intent_id=OLD.intent_id) BEGIN SELECT RAISE(ABORT,'anchor bootstrap proof is immutable'); END;
CREATE TRIGGER complete_anchor_blocks_source_delete BEFORE DELETE ON source_transactions WHEN EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.state='complete' AND a.capture_epoch=OLD.capture_epoch AND a.start_seq=OLD.last_seq) BEGIN SELECT RAISE(ABORT,'anchor lower proof is immutable'); END;
CREATE TRIGGER checkpoint_revision BEFORE UPDATE ON destination_checkpoints WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale checkpoint revision'); END;
CREATE TRIGGER source_state_revision BEFORE UPDATE ON source_state WHEN NEW.control_revision!=OLD.control_revision+1 BEGIN SELECT RAISE(ABORT,'stale source-state revision'); END;
CREATE TRIGGER command_revision BEFORE UPDATE ON operator_command_requests WHEN NEW.request_revision!=OLD.request_revision+1 BEGIN SELECT RAISE(ABORT,'stale command revision'); END;
CREATE TRIGGER audit_subrange_insert BEFORE INSERT ON audit_coverage_subranges WHEN NOT EXISTS(SELECT 1 FROM destination_audits a WHERE a.audit_id=NEW.audit_id AND NEW.start_seq>=coalesce(a.retained_history_start_seq,0) AND NEW.start_seq>=coalesce(a.unverifiable_before_seq,0) AND NEW.end_seq<=a.round_target_seq AND NEW.fresh_until>a.freshness_window_started_at AND NEW.fresh_until<=a.freshness_expires_at) BEGIN SELECT RAISE(ABORT,'audit subrange outside frozen retention/freshness target'); END;
CREATE TRIGGER audit_subrange_update BEFORE UPDATE ON audit_coverage_subranges WHEN NOT EXISTS(SELECT 1 FROM destination_audits a WHERE a.audit_id=NEW.audit_id AND NEW.start_seq>=coalesce(a.retained_history_start_seq,0) AND NEW.start_seq>=coalesce(a.unverifiable_before_seq,0) AND NEW.end_seq<=a.round_target_seq AND NEW.fresh_until>a.freshness_window_started_at AND NEW.fresh_until<=a.freshness_expires_at) BEGIN SELECT RAISE(ABORT,'audit subrange outside frozen retention/freshness target'); END;
"#;

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(1);
    fn path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "boring-cdc-m2-{name}-{}-{}.sqlite",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }
    fn writer(name: &str) -> (PathBuf, WriterConnection) {
        let p = path(name);
        let w = open_writer(&p, "run-1", 1, 1000).unwrap();
        (p, w)
    }
    fn tx(c: &Connection) {
        c.execute("INSERT INTO source_transactions VALUES('tx1','epoch','sys','db','slot','7','0000000000000010',1,1,0,'sum','committed')",[]).unwrap();
    }

    #[test]
    fn clean_install_has_all_tables_and_required_pragmas() {
        let (p, w) = writer("clean");
        assert_eq!(
            (
                w.attestation().journal_mode.as_str(),
                w.attestation().synchronous,
                w.attestation().auto_vacuum
            ),
            ("wal", 2, 2)
        );
        let names: Vec<String> = w
            .connection()
            .prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        for n in [
            "journal_events",
            "source_transactions",
            "source_state",
            "runtime_ownership",
            "operator_command_requests",
            "relation_schemas",
            "destinations",
            "destination_checkpoints",
            "backfill_runs",
            "backfill_generations",
            "backfill_chunks",
            "bootstrap_intents",
            "bootstrap_imports",
            "durable_capture_fences",
            "bootstrap_anchors",
            "reseed_intents",
            "destination_generation_leases",
            "destination_promotion_intents",
            "clickhouse_batch_intents",
            "archive_generations",
            "archive_segment_intents",
            "archive_segments",
            "archive_generation_markers",
            "processing_failures",
            "destination_audits",
            "condition_hysteresis",
            "alerts",
            "schema_migrations",
        ] {
            assert!(names.contains(&n.to_string()), "missing {n}");
        }
        assert_eq!(
            w.connection()
                .query_row("PRAGMA foreign_key_check", [], |_| Ok(()))
                .optional()
                .unwrap(),
            None
        );
        drop(w);
        let _ = fs::remove_file(p);
    }

    #[test]
    fn creation_floor_is_separate_bound_and_marker_survives_reopen() {
        let (p, w) = writer("floor");
        tx(w.connection());
        w.connection().execute("INSERT INTO bootstrap_intents VALUES('boot','epoch','sys','db','slot','0000000000000008','slot_created',0,'now')",[]).unwrap();
        w.connection().execute("INSERT INTO source_state(singleton,capture_epoch,source_system_id,timeline_id,database_id,slot_name,plugin,publication_fingerprint,protocol_fingerprint,slot_creation_floor_lsn,slot_creation_intent_id,last_feedback_lsn,last_feedback_repeated_creation_floor) VALUES(1,'epoch','sys','tl','db','slot','pgoutput','pub','proto','0000000000000008','boot','0000000000000008',1)",[]).unwrap();
        assert_eq!(
            w.connection()
                .query_row(
                    "SELECT durable_transaction_end_lsn IS NULL FROM source_state",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        assert!(
            w.connection()
                .execute(
                    "UPDATE source_state SET slot_name='other' WHERE singleton=1",
                    []
                )
                .is_err()
        );
        assert!(w.connection().execute("UPDATE source_state SET slot_creation_floor_lsn='0000000000000009' WHERE singleton=1",[]).is_err());
        assert!(
            w.connection()
                .execute(
                    "UPDATE bootstrap_intents SET state='invalidated' WHERE intent_id='boot'",
                    []
                )
                .is_err()
        );
        assert!(w.connection().execute("UPDATE source_state SET durable_transaction_id='tx1',durable_journal_seq=1,durable_transaction_end_lsn='FFFFFFFFFFFFFFFF' WHERE singleton=1",[]).is_err());
        drop(w);
        let w = open_writer(&p, "run-2", 2, 2000).unwrap();
        assert_eq!(
            w.connection()
                .query_row(
                    "SELECT last_feedback_repeated_creation_floor FROM source_state",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        let _ = fs::remove_file(p);
    }

    #[test]
    fn constraints_reject_bad_lsn_duplicate_identity_and_incomplete_checkpoint() {
        let (_p, w) = writer("constraints");
        tx(w.connection());
        assert!(w.connection().execute("INSERT INTO source_transactions VALUES('tx2','epoch','sys','db','slot','8','0000000000000010',2,2,0,'sum2','committed')",[]).is_err());
        assert!(w.connection().execute("INSERT INTO source_transactions VALUES('tx3','epoch','sys','db','slot','8','0/10',2,2,0,'sum2','committed')",[]).is_err());
        w.connection()
            .execute(
                "INSERT INTO relation_schemas VALUES('fp','epoch','r',X'01','sum',1)",
                [],
            )
            .unwrap();
        w.connection()
            .execute(
                "INSERT INTO journal_events VALUES(1,'pos','tx1',0,'epoch','fp',NULL,X'01','hash')",
                [],
            )
            .unwrap();
        assert!(w.connection().execute("INSERT INTO journal_events VALUES(2,'pos','tx1',1,'epoch','fp',NULL,X'02','different')",[]).is_err());
        w.connection().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('d','archive','cfg','epoch',1)",[]).unwrap();
        assert!(w.connection().execute("INSERT INTO destination_checkpoints VALUES('d','epoch',NULL,'cfg',1,'tx1',2,NULL,0)",[]).is_err());
    }

    #[test]
    fn immutable_rows_fences_and_anchor_proof_fail_closed() {
        let (_p, w) = writer("immutability");
        tx(w.connection());
        w.connection()
            .execute(
                "INSERT INTO relation_schemas VALUES('fp','epoch','r',X'01','sum',1)",
                [],
            )
            .unwrap();
        assert!(w.connection().execute("UPDATE relation_schemas SET canonical_schema=X'02' WHERE schema_fingerprint='fp'",[]).is_err());
        w.connection().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation,highest_external_fence) VALUES('d','archive','cfg','epoch',1,9)",[]).unwrap();
        assert!(
            w.connection()
                .execute(
                    "UPDATE destinations SET highest_external_fence=8 WHERE destination_id='d'",
                    []
                )
                .is_err()
        );
        w.connection()
            .execute(
                "INSERT INTO backfill_runs VALUES('br','d','epoch','complete',0)",
                [],
            )
            .unwrap();
        w.connection()
            .execute(
                "INSERT INTO backfill_generations VALUES('bg','br',1,'nonce','complete')",
                [],
            )
            .unwrap();
        w.connection()
            .execute(
                "INSERT INTO backfill_chunks VALUES('chunk','bg',X'00',X'01','complete',1,'sum')",
                [],
            )
            .unwrap();
        w.connection().execute("INSERT INTO bootstrap_intents VALUES('boot','epoch','sys','db','slot','0000000000000000','complete',0,'now')",[]).unwrap();
        w.connection()
            .execute(
                "INSERT INTO bootstrap_imports VALUES('import','boot','d','acknowledged',0)",
                [],
            )
            .unwrap();
        assert!(w.connection().execute("INSERT INTO bootstrap_anchors(anchor_id,capture_epoch,generation,lower_stitch_lsn,start_seq,snapshot_boundary_lsn,snapshot_complete_seq,post_copy_fence_nonce,post_copy_fence_lsn,post_copy_fence_seq,table_set_fingerprint,snapshot_schema_fingerprints,state,expires_at,generation_id,bootstrap_intent_id) VALUES('a','epoch',1,NULL,0,'0000000000000000',1,'nonce','0000000000000010',1,'tables','[\"fp\"]','complete','later','bg','boot')",[]).is_err());
        w.connection().execute("INSERT INTO durable_capture_fences VALUES('f','epoch',1,'nonce','tx1','0000000000000010',1,1)",[]).unwrap();
        assert!(w.connection().execute("INSERT INTO bootstrap_anchors(anchor_id,capture_epoch,generation,start_seq,snapshot_boundary_lsn,snapshot_complete_seq,post_copy_fence_nonce,post_copy_fence_lsn,post_copy_fence_seq,table_set_fingerprint,snapshot_schema_fingerprints,state,expires_at,generation_id,bootstrap_intent_id) VALUES('bad','epoch',1,777,'0000000000000000',1,'nonce','0000000000000010',1,'tables','[\"fp\"]','complete','later','bg','boot')",[]).is_err());
        w.connection().execute("INSERT INTO bootstrap_anchors(anchor_id,capture_epoch,generation,lower_stitch_lsn,start_seq,snapshot_boundary_lsn,snapshot_complete_seq,post_copy_fence_nonce,post_copy_fence_lsn,post_copy_fence_seq,table_set_fingerprint,snapshot_schema_fingerprints,state,expires_at,generation_id,bootstrap_intent_id) VALUES('a','epoch',1,NULL,0,'0000000000000000',1,'nonce','0000000000000010',1,'tables','[\"fp\"]','complete','later','bg','boot')",[]).unwrap();
        assert!(w.connection().execute("UPDATE bootstrap_anchors SET post_copy_fence_nonce='other' WHERE anchor_id='a'",[]).is_err());
        assert!(
            w.connection()
                .execute(
                    "UPDATE backfill_chunks SET state='invalidated' WHERE chunk_id='chunk'",
                    []
                )
                .is_err()
        );
        assert!(w.connection().execute("UPDATE bootstrap_imports SET state='failed',revision=revision+1 WHERE import_id='import'",[]).is_err());
        assert!(
            w.connection()
                .execute("DELETE FROM durable_capture_fences WHERE fence_id='f'", [])
                .is_err()
        );
        assert!(
            w.connection()
                .execute(
                    "DELETE FROM relation_schemas WHERE schema_fingerprint='fp'",
                    []
                )
                .is_err()
        );
        assert!(w.connection().execute("UPDATE durable_capture_fences SET post_copy_fence_lsn='0000000000000011' WHERE fence_id='f'",[]).is_err());
        assert!(
            w.connection()
                .execute(
                    "UPDATE backfill_runs SET revision=revision+1 WHERE run_id='br'",
                    []
                )
                .is_err()
        );
        assert!(
            w.connection()
                .execute(
                    "UPDATE bootstrap_intents SET revision=revision+1 WHERE intent_id='boot'",
                    []
                )
                .is_err()
        );
        assert!(w.connection().execute("INSERT INTO backfill_chunks VALUES('late','bg',X'02',X'03','pending',NULL,NULL)",[]).is_err());
    }

    #[test]
    fn command_nonce_abort_and_external_fence_survive_restart() {
        let (p, w) = writer("restart");
        w.connection().execute("INSERT INTO operator_command_requests(request_id,dry_run_nonce,canonical_payload,payload_digest,run_id,peer_identity,state,result,observation_revision,control_revision,expires_at) VALUES('req','nonce',X'01','digest','run','peer','accepted',NULL,1,1,'later')",[]).unwrap();
        w.connection().execute("UPDATE operator_command_requests SET state='aborted_by_restart',request_revision=request_revision+1 WHERE state IN ('accepted','reconciling') AND run_id!='new-run'",[]).unwrap();
        w.connection().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation,highest_external_fence,adopted_external_fence_at,adopted_external_fence_evidence) VALUES('d','clickhouse','cfg','epoch',1,9,'now','proof')",[]).unwrap();
        drop(w);
        let w = open_writer(&p, "new-run", 2, 2000).unwrap();
        assert_eq!(
            w.connection()
                .query_row("SELECT state FROM operator_command_requests", [], |r| {
                    r.get::<_, String>(0)
                })
                .unwrap(),
            "aborted_by_restart"
        );
        assert_eq!(
            w.connection()
                .query_row("SELECT highest_external_fence FROM destinations", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            9
        );
        let _ = fs::remove_file(p);
    }

    #[test]
    fn failed_migration_rolls_back_atomically_and_upgrade_is_idempotent() {
        let bad = path("migration-crash");
        let c = Connection::open(&bad).unwrap();
        c.pragma_update(None, "auto_vacuum", "INCREMENTAL").unwrap();
        c.pragma_update(None, "journal_mode", "WAL").unwrap();
        c.pragma_update(None, "synchronous", "FULL").unwrap();
        c.execute_batch("CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY,name TEXT UNIQUE NOT NULL,checksum TEXT NOT NULL,applied_at TEXT NOT NULL); INSERT INTO schema_migrations VALUES(1,'initial','wrong','now');").unwrap();
        assert!(apply_migrations(&c).is_err());
        assert_eq!(
            c.query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name='journal_events'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
        drop(c);
        let _ = fs::remove_file(bad);

        let upgrade = path("v1-upgrade");
        let c = Connection::open(&upgrade).unwrap();
        c.pragma_update(None, "auto_vacuum", "INCREMENTAL").unwrap();
        c.pragma_update(None, "journal_mode", "WAL").unwrap();
        c.pragma_update(None, "synchronous", "FULL").unwrap();
        c.execute_batch(MIGRATION_1).unwrap();
        c.execute(
            "INSERT INTO schema_migrations VALUES(1,'initial',?1,'before-upgrade')",
            [MIGRATION_1_CHECKSUM],
        )
        .unwrap();
        c.execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation,highest_external_fence) VALUES('preserved','archive','cfg','epoch',1,11)",[]).unwrap();
        c.execute_batch(
            "BEGIN IMMEDIATE; ALTER TABLE bootstrap_anchors ADD COLUMN crash_probe TEXT; ROLLBACK;",
        )
        .unwrap();
        drop(c);
        let c = Connection::open(&upgrade).unwrap();
        assert_eq!(c.query_row("SELECT count(*) FROM pragma_table_info('bootstrap_anchors') WHERE name='crash_probe'",[],|r|r.get::<_,i64>(0)).unwrap(),0);
        apply_migrations(&c).unwrap();
        apply_migrations(&c).unwrap();
        assert_eq!(
            c.query_row("SELECT count(*) FROM schema_migrations", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            c.query_row(
                "SELECT highest_external_fence FROM destinations WHERE destination_id='preserved'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            11
        );
        assert_eq!(
            c.query_row(
                "SELECT revision FROM destinations WHERE destination_id='preserved'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
        drop(c);
        let _ = fs::remove_file(upgrade);
    }

    #[test]
    fn reopened_writer_reapplies_full_and_attests_generation() {
        let empty = path("precreated-empty");
        fs::File::create(&empty).unwrap();
        let empty_writer = open_writer(&empty, "run-empty", 1, 1_000).unwrap();
        assert_eq!(empty_writer.attestation().auto_vacuum, 2);
        drop(empty_writer);
        let _ = fs::remove_file(empty);
        let (p, w) = writer("reopen");
        drop(w);
        let c = Connection::open(&p).unwrap();
        c.pragma_update(None, "synchronous", "OFF").unwrap();
        drop(c);
        let w = open_writer(&p, "run-2", 7, 5_000).unwrap();
        assert_eq!(
            (
                w.attestation().run_id.as_str(),
                w.attestation().connection_generation,
                w.attestation().synchronous
            ),
            ("run-2", 7, 2)
        );
        let _ = fs::remove_file(p);
    }

    #[test]
    fn failure_and_audit_state_survive_without_checkpoint_advance() {
        let (p, w) = writer("audit");
        tx(w.connection());
        w.connection().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('d','archive','cfg','epoch',1)",[]).unwrap();
        w.connection().execute("INSERT INTO destination_checkpoints VALUES('d','epoch',NULL,'cfg',1,'tx1',1,NULL,0)",[]).unwrap();
        w.connection().execute("INSERT INTO processing_failures VALUES('fail','d','archive','io','fingerprint',1,1,'transient',2,'later',1,'first','last')",[]).unwrap();
        w.connection()
            .execute(
                "UPDATE destinations SET current_failure_id='fail',revision=revision+1 WHERE destination_id='d'",
                [],
            )
            .unwrap();
        w.connection().execute("INSERT INTO destination_audits(audit_id,destination_id,configuration_fingerprint,capture_epoch,generation,round_target_seq,round_identity_digest,journal_cursor_seq,self_cursor_seq,budget_bytes_used,budget_events_used,budget_ms_used,freshness_window_started_at,freshness_expires_at,contract_digest,evidence_digest,first_mismatch,revision,retained_history_start_seq,unverifiable_before_seq) VALUES('audit','d','cfg','epoch',1,1,'identity',0,0,10,2,3,'2026-01-01','2027-01-01','contract','evidence','mismatch',0,0,NULL)",[]).unwrap();
        w.connection().execute("INSERT INTO audit_coverage_subranges VALUES('audit','journal_verified',0,1,'2027-01-01','range-proof')",[]).unwrap();
        assert!(w.connection().execute("INSERT INTO audit_coverage_subranges VALUES('audit','self_consistent',2,1,'2027-01-01','bad')",[]).is_err());
        assert!(w.connection().execute("INSERT INTO audit_coverage_subranges VALUES('audit','self_consistent',0,2,'2027-01-01','beyond-target')",[]).is_err());
        assert!(w.connection().execute("INSERT INTO audit_coverage_subranges VALUES('audit','self_consistent',0,1,'2028-01-01','expired')",[]).is_err());
        drop(w);
        let w = open_writer(&p, "run-2", 2, 2_000).unwrap();
        assert_eq!(
            w.connection()
                .query_row("SELECT journal_seq FROM destination_checkpoints", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            w.connection()
                .query_row("SELECT attempt FROM processing_failures", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            w.connection()
                .query_row(
                    "SELECT round_identity_digest FROM destination_audits",
                    [],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
            "identity"
        );
        let _ = fs::remove_file(p);
    }

    #[test]
    fn revision_cas_and_promotion_fence_uniqueness() {
        let (_p, w) = writer("cas");
        w.connection().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('d','clickhouse','cfg','epoch',1)",[]).unwrap();
        assert_eq!(w.connection().execute("UPDATE destinations SET generation=2,revision=revision+1 WHERE destination_id='d' AND revision=0",[]).unwrap(),1);
        assert_eq!(w.connection().execute("UPDATE destinations SET generation=3,revision=revision+1 WHERE destination_id='d' AND revision=0",[]).unwrap(),0);
        w.connection().execute("INSERT INTO bootstrap_anchors(anchor_id,capture_epoch,generation,start_seq,snapshot_boundary_lsn,table_set_fingerprint,snapshot_schema_fingerprints,state,expires_at) VALUES('a','epoch',1,0,'0000000000000000','tables','[\"fp\"]','building','later')",[]).unwrap();
        w.connection().execute("INSERT INTO destination_promotion_intents VALUES('p1','d','epoch',1,2,'a','cfg',5,'selector','prepared',0)",[]).unwrap();
        assert!(w.connection().execute("UPDATE destination_promotion_intents SET state='switched',revision=revision+1 WHERE intent_id='p1'",[]).is_err());
        assert!(w.connection().execute("UPDATE destination_promotion_intents SET state='old_leases_fenced' WHERE intent_id='p1'",[]).is_err());
        w.connection().execute("UPDATE destination_promotion_intents SET state='old_leases_fenced',revision=revision+1 WHERE intent_id='p1'",[]).unwrap();
        assert!(w.connection().execute("INSERT INTO destination_promotion_intents VALUES('p2','d','epoch',1,3,'a','cfg',5,'other','prepared',0)",[]).is_err());
        assert_eq!(
            w.connection()
                .query_row(
                    "SELECT highest_external_fence FROM destinations WHERE destination_id='d'",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            5
        );
        assert!(w.connection().execute("INSERT INTO destination_promotion_intents VALUES('p3','d','epoch',1,3,'a','cfg',4,'older','prepared',0)",[]).is_err());
    }

    #[test]
    fn reader_is_read_only_and_bounded() {
        let (p, w) = writer("reader");
        drop(w);
        let r = open_reader(&p).unwrap();
        assert_eq!(r.max_rows(), READER_MAX_ROWS);
        assert!(!r.expired());
        assert!(
            r.query_bounded("CREATE TABLE forbidden(x)", |_| Ok(()))
                .is_err()
        );
        let short = open_reader_with_limits(&p, Duration::from_millis(1), 1).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        assert!(
            short
                .query_bounded("SELECT 1", |row| row.get::<_, i64>(0))
                .is_err()
        );
        let interrupted = open_reader_with_limits(&p, Duration::from_millis(1), 1).unwrap();
        assert!(interrupted.query_bounded("WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT sum(x) FROM n", |row| row.get::<_, i64>(0)).is_err());
        let limited = open_reader_with_limits(&p, Duration::from_secs(1), 1).unwrap();
        assert!(
            limited
                .query_bounded("SELECT 1 UNION ALL SELECT 2", |row| row.get::<_, i64>(0))
                .is_err()
        );
        let _ = fs::remove_file(p);
    }

    use rusqlite::OptionalExtension;
}
