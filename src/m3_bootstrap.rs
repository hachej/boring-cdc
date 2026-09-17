//! Durable initial-bootstrap coordinator.
//!
//! This module owns only the M3 intent/export/import lifecycle. It deliberately reuses the M2
//! writer admission, ownership, capture feedback interface, and source schema. PostgreSQL session
//! orchestration is exposed behind small capabilities so the permanent-slot creation response can
//! be committed before CopyBoth starts on a different connection.

use crate::m2_capture_runtime::{FeedbackGate, FeedbackPermit};
use crate::m2_schema::WriterConnection;
use rusqlite::{OptionalExtension, params};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fmt;

pub const OWNER_BEAD: &str = "boring-cdc-m3-bootstrap";
const MAX_ID_BYTES: usize = 256;
const MAX_TOKEN_BYTES: usize = 1_024;

const INSTALL_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS m3_bootstrap_runtime(
 intent_id TEXT PRIMARY KEY REFERENCES bootstrap_intents(intent_id),
 generation INTEGER NOT NULL CHECK(generation>0),
 table_set_fingerprint TEXT NOT NULL,
 configuration_fingerprint TEXT NOT NULL,
 snapshot_token TEXT,
 consistent_lsn TEXT CHECK(consistent_lsn IS NULL OR (length(consistent_lsn)=16 AND consistent_lsn NOT GLOB '*[^0-9A-F]*')),
 start_seq INTEGER CHECK(start_seq IS NULL OR start_seq>=0),
 exporter_liveness TEXT NOT NULL CHECK(exporter_liveness IN ('not_started','command_idle','released','lost')),
 guard_liveness TEXT NOT NULL CHECK(guard_liveness IN ('not_started','held','released','lost')),
 snapshot_promotable INTEGER NOT NULL CHECK(snapshot_promotable IN(0,1)),
 feedback_gate_open INTEGER NOT NULL CHECK(feedback_gate_open IN(0,1)),
 state TEXT NOT NULL CHECK(state IN ('prepared','snapshot_exported','imports_pending','imports_complete','exporter_release_permitted','exporter_released','snapshot_unusable','bootstrap_ambiguous_requires_restart','existing_slot_generation_required','full_reseed_required')),
 exporter_backend_pid INTEGER,
 guard_backend_pid INTEGER,
 capture_backend_pid INTEGER,
 revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0),
 CHECK((snapshot_token IS NULL)=(consistent_lsn IS NULL)),
 CHECK((consistent_lsn IS NULL)=(start_seq IS NULL)),
 CHECK(snapshot_promotable=0 OR snapshot_token IS NOT NULL)
) STRICT;
CREATE TABLE IF NOT EXISTS m3_bootstrap_importers(
 intent_id TEXT NOT NULL REFERENCES m3_bootstrap_runtime(intent_id) ON DELETE CASCADE,
 importer_id TEXT NOT NULL,
 assigned_ranges_digest TEXT NOT NULL,
 snapshot_schema_fingerprint TEXT,
 backend_pid INTEGER,
 state TEXT NOT NULL CHECK(state IN ('assigned','snapshot_set_first','contract_bound','acknowledged','reads_complete','failed','invalidated')),
 revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0),
 PRIMARY KEY(intent_id,importer_id)
) STRICT;
CREATE TRIGGER IF NOT EXISTS m3_bootstrap_runtime_revision BEFORE UPDATE ON m3_bootstrap_runtime
 WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale m3 bootstrap revision'); END;
CREATE TRIGGER IF NOT EXISTS m3_bootstrap_importer_revision BEFORE UPDATE ON m3_bootstrap_importers
 WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale m3 importer revision'); END;
"#;

#[derive(Debug)]
pub enum BootstrapError {
    Invalid(&'static str),
    Conflict(&'static str),
    Sqlite(rusqlite::Error),
}
impl fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(code) | Self::Conflict(code) => f.write_str(code),
            Self::Sqlite(_) => f.write_str("M3_BOOTSTRAP_STORE_FAILED"),
        }
    }
}
impl std::error::Error for BootstrapError {}
impl From<rusqlite::Error> for BootstrapError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Sqlite(value)
    }
}

#[derive(Clone, Debug)]
pub struct PrepareIntent {
    pub intent_id: String,
    pub capture_epoch: String,
    pub source_system_id: String,
    pub database_id: String,
    pub slot_name: String,
    pub generation: u64,
    pub table_set_fingerprint: String,
    pub configuration_fingerprint: String,
    pub importers: Vec<ImporterAssignment>,
    pub created_at: String,
}
#[derive(Clone, Debug)]
pub struct ImporterAssignment {
    pub importer_id: String,
    /// Digest of the immutable assigned half-open ranges. Canonical key values are never stored.
    pub assigned_ranges_digest: String,
}
#[derive(Clone, Debug)]
pub struct ExportResponse {
    pub consistent_lsn: u64,
    pub snapshot_token: String,
    pub start_seq: u64,
    pub exporter_backend_pid: i32,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconcileDecision {
    RetryPreparedCreation,
    BootstrapAmbiguousRequiresRestart,
    ResumeImports,
    ExistingSlotGenerationRequired,
    FullReseedRequired,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RedactedStatus {
    pub intent_id: String,
    pub state: String,
    pub exporter_liveness: String,
    pub guard_liveness: String,
    pub creation_floor_lsn: Option<String>,
    pub start_seq: Option<u64>,
    pub acknowledged_importers: u64,
    pub expected_importers: u64,
    pub snapshot_promotable: bool,
    pub feedback_gate_open: bool,
    pub revision: u64,
}

pub struct BootstrapStore {
    writer: WriterConnection,
}
impl BootstrapStore {
    pub fn open(writer: WriterConnection) -> Result<Self, BootstrapError> {
        writer.connection().execute_batch(INSTALL_SQL)?;
        Ok(Self { writer })
    }

    pub fn prepare(&mut self, input: &PrepareIntent) -> Result<(), BootstrapError> {
        validate_prepare(input)?;
        let tx = self.writer.connection_mut().transaction()?;
        tx.execute(
            "INSERT INTO bootstrap_intents(intent_id,capture_epoch,source_system_id,database_id,slot_name,creation_floor_lsn,state,revision,created_at) VALUES(?1,?2,?3,?4,?5,NULL,'prepared',0,?6)",
            params![input.intent_id,input.capture_epoch,input.source_system_id,input.database_id,input.slot_name,input.created_at],
        )?;
        tx.execute(
            "INSERT INTO m3_bootstrap_runtime(intent_id,generation,table_set_fingerprint,configuration_fingerprint,exporter_liveness,guard_liveness,snapshot_promotable,feedback_gate_open,state,guard_backend_pid) VALUES(?1,?2,?3,?4,'not_started','not_started',0,0,'prepared',NULL)",
            params![input.intent_id,input.generation,input.table_set_fingerprint,input.configuration_fingerprint],
        )?;
        for importer in &input.importers {
            tx.execute("INSERT INTO m3_bootstrap_importers(intent_id,importer_id,assigned_ranges_digest,state) VALUES(?1,?2,?3,'assigned')", params![input.intent_id,importer.importer_id,importer.assigned_ranges_digest])?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Persists guard acquisition after the prepared intent, and before slot creation.
    pub fn record_guard_acquired(
        &mut self,
        intent_id: &str,
        guard_backend_pid: i32,
    ) -> Result<(), BootstrapError> {
        if guard_backend_pid <= 0 {
            return Err(BootstrapError::Invalid("M3_GUARD_PID_INVALID"));
        }
        let changed = self.writer.connection().execute(
            "UPDATE m3_bootstrap_runtime SET guard_liveness='held',guard_backend_pid=?2,revision=revision+1 WHERE intent_id=?1 AND state='prepared' AND guard_liveness='not_started' AND guard_backend_pid IS NULL",
            params![intent_id, guard_backend_pid],
        )?;
        if changed != 1 {
            return Err(BootstrapError::Conflict("M3_GUARD_ACQUIRE_STALE"));
        }
        Ok(())
    }

    /// Commits the only copy of the exported token, the server floor, and the complete journal
    /// boundary before the separate CopyBoth connection is allowed to start.
    pub fn persist_export_response(
        &mut self,
        intent_id: &str,
        response: &ExportResponse,
    ) -> Result<(), BootstrapError> {
        if response.consistent_lsn == 0
            || response.snapshot_token.is_empty()
            || response.snapshot_token.len() > MAX_TOKEN_BYTES
            || !response.snapshot_token.is_ascii()
            || response.exporter_backend_pid <= 0
        {
            return Err(BootstrapError::Invalid("M3_EXPORT_RESPONSE_INVALID"));
        }
        let lsn = format!("{:016X}", response.consistent_lsn);
        let tx = self.writer.connection_mut().transaction()?;
        let changed = tx.execute(
            "UPDATE m3_bootstrap_runtime SET snapshot_token=?2,consistent_lsn=?3,start_seq=?4,exporter_liveness='command_idle',exporter_backend_pid=?5,snapshot_promotable=1,state='imports_pending',revision=revision+1 WHERE intent_id=?1 AND state='prepared' AND guard_liveness='held' AND exporter_liveness='not_started'",
            params![intent_id,response.snapshot_token,lsn,response.start_seq,response.exporter_backend_pid],
        )?;
        if changed != 1 {
            return Err(BootstrapError::Conflict("M3_EXPORT_RESPONSE_STALE"));
        }
        tx.execute("UPDATE bootstrap_intents SET creation_floor_lsn=?2,state='slot_created',revision=revision+1 WHERE intent_id=?1 AND state='prepared'", params![intent_id,lsn])?;
        let identity: (String,String,String,String) = tx.query_row("SELECT capture_epoch,source_system_id,database_id,slot_name FROM bootstrap_intents WHERE intent_id=?1",[intent_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
        let source_changed = tx.execute(
            "UPDATE source_state SET slot_creation_floor_lsn=?1,slot_creation_intent_id=?2,control_revision=control_revision+1 WHERE singleton=1 AND capture_epoch=?3 AND source_system_id=?4 AND database_id=?5 AND slot_name=?6 AND slot_creation_floor_lsn IS NULL AND durable_transaction_end_lsn IS NULL",
            params![lsn,intent_id,identity.0,identity.1,identity.2,identity.3],
        )?;
        if source_changed != 1 {
            return Err(BootstrapError::Conflict(
                "M3_CREATION_FLOOR_SOURCE_MISMATCH",
            ));
        }
        tx.commit()?;
        Ok(())
    }

    /// Records the separate CopyBoth backend only after the export response transaction commits.
    pub fn record_capture_started(
        &mut self,
        intent_id: &str,
        capture_backend_pid: i32,
    ) -> Result<(), BootstrapError> {
        if capture_backend_pid <= 0 {
            return Err(BootstrapError::Invalid("M3_CAPTURE_PID_INVALID"));
        }
        let changed = self.writer.connection().execute(
            "UPDATE m3_bootstrap_runtime SET capture_backend_pid=?2,revision=revision+1 WHERE intent_id=?1 AND state='imports_pending' AND capture_backend_pid IS NULL AND exporter_backend_pid!=?2",
            params![intent_id, capture_backend_pid],
        )?;
        if changed != 1 {
            return Err(BootstrapError::Conflict("M3_CAPTURE_START_STALE"));
        }
        Ok(())
    }

    pub fn snapshot_set_first(
        &mut self,
        intent_id: &str,
        importer_id: &str,
        backend_pid: i32,
    ) -> Result<(), BootstrapError> {
        if backend_pid <= 0 {
            return Err(BootstrapError::Invalid("M3_IMPORTER_PID_INVALID"));
        }
        self.importer_transition(
            intent_id,
            importer_id,
            "assigned",
            "snapshot_set_first",
            Some(backend_pid),
            None,
        )
    }
    pub fn bind_importer_contract(
        &mut self,
        intent_id: &str,
        importer_id: &str,
        schema_fingerprint: &str,
    ) -> Result<(), BootstrapError> {
        if !valid_id(schema_fingerprint) {
            return Err(BootstrapError::Invalid("M3_SCHEMA_FINGERPRINT_INVALID"));
        }
        self.importer_transition(
            intent_id,
            importer_id,
            "snapshot_set_first",
            "contract_bound",
            None,
            Some(schema_fingerprint),
        )
    }
    pub fn acknowledge_import(
        &mut self,
        intent_id: &str,
        importer_id: &str,
    ) -> Result<(), BootstrapError> {
        let tx = self.writer.connection_mut().transaction()?;
        let changed = tx.execute(
            "UPDATE m3_bootstrap_importers SET state='acknowledged',revision=revision+1 WHERE intent_id=?1 AND importer_id=?2 AND state='contract_bound'",
            params![intent_id, importer_id],
        )?;
        if changed != 1 {
            return Err(BootstrapError::Conflict("M3_IMPORTER_COMPLETION_STALE"));
        }
        let pending: i64 = tx.query_row("SELECT count(*) FROM m3_bootstrap_importers WHERE intent_id=?1 AND state!='acknowledged'",[intent_id],|r|r.get(0))?;
        if pending == 0 {
            let changed=tx.execute("UPDATE m3_bootstrap_runtime SET state='exporter_release_permitted',feedback_gate_open=1,revision=revision+1 WHERE intent_id=?1 AND state='imports_pending' AND exporter_liveness='command_idle' AND guard_liveness='held'",[intent_id])?;
            if changed != 1 {
                return Err(BootstrapError::Conflict("M3_IMPORT_ACK_GATE_STALE"));
            }
        }
        tx.commit()?;
        Ok(())
    }
    pub fn release_exporter(&mut self, intent_id: &str) -> Result<(), BootstrapError> {
        let changed=self.writer.connection().execute("UPDATE m3_bootstrap_runtime SET state='exporter_released',exporter_liveness='released',revision=revision+1 WHERE intent_id=?1 AND state='exporter_release_permitted' AND NOT EXISTS(SELECT 1 FROM m3_bootstrap_importers WHERE intent_id=?1 AND state!='acknowledged')",[intent_id])?;
        if changed != 1 {
            return Err(BootstrapError::Conflict(
                "M3_EXPORTER_RELEASE_NOT_PERMITTED",
            ));
        }
        Ok(())
    }

    /// Exporter/importer/guard loss is one SQLite transaction with feedback-gate release and
    /// permanent snapshot ineligibility. Durable WAL can then progress independently.
    pub fn invalidate_generation(
        &mut self,
        intent_id: &str,
        lost_session: &str,
    ) -> Result<(), BootstrapError> {
        if !matches!(lost_session, "exporter" | "importer" | "guard") {
            return Err(BootstrapError::Invalid("M3_LOST_SESSION_INVALID"));
        }
        let tx = self.writer.connection_mut().transaction()?;
        let liveness = if lost_session == "exporter" {
            "exporter_liveness='lost',"
        } else {
            ""
        };
        let guard = if lost_session == "guard" {
            "guard_liveness='lost',"
        } else {
            ""
        };
        let sql = format!(
            "UPDATE m3_bootstrap_runtime SET {liveness}{guard} state='snapshot_unusable',snapshot_promotable=0,feedback_gate_open=1,revision=revision+1 WHERE intent_id=?1 AND state IN ('snapshot_exported','imports_pending','exporter_release_permitted','exporter_released')"
        );
        if tx.execute(&sql, [intent_id])? != 1 {
            return Err(BootstrapError::Conflict("M3_INVALIDATION_STALE"));
        }
        tx.execute("UPDATE m3_bootstrap_importers SET state='invalidated',revision=revision+1 WHERE intent_id=?1 AND state NOT IN ('reads_complete','invalidated')",[intent_id])?;
        tx.commit()?;
        Ok(())
    }

    pub fn reconcile(
        &mut self,
        intent_id: &str,
        remote_slot_exists: bool,
        wal_continuous: bool,
        provenance_matches: bool,
    ) -> Result<ReconcileDecision, BootstrapError> {
        let (state, token): (String, Option<String>) = self.writer.connection().query_row(
            "SELECT state,snapshot_token FROM m3_bootstrap_runtime WHERE intent_id=?1",
            [intent_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if state == "prepared" && !remote_slot_exists {
            return Ok(ReconcileDecision::RetryPreparedCreation);
        }
        if state == "prepared" && remote_slot_exists && token.is_none() {
            self.writer.connection().execute("UPDATE m3_bootstrap_runtime SET state='bootstrap_ambiguous_requires_restart',exporter_liveness='lost',snapshot_promotable=0,feedback_gate_open=1,revision=revision+1 WHERE intent_id=?1 AND state='prepared'",[intent_id])?;
            return Ok(ReconcileDecision::BootstrapAmbiguousRequiresRestart);
        }
        if state == "bootstrap_ambiguous_requires_restart" || state == "snapshot_unusable" {
            if remote_slot_exists && wal_continuous && provenance_matches {
                self.writer.connection().execute("UPDATE m3_bootstrap_runtime SET state='existing_slot_generation_required',feedback_gate_open=1,snapshot_promotable=0,revision=revision+1 WHERE intent_id=?1 AND state IN ('bootstrap_ambiguous_requires_restart','snapshot_unusable')",[intent_id])?;
                return Ok(ReconcileDecision::ExistingSlotGenerationRequired);
            }
            self.writer.connection().execute("UPDATE m3_bootstrap_runtime SET state='full_reseed_required',feedback_gate_open=1,snapshot_promotable=0,revision=revision+1 WHERE intent_id=?1 AND state IN ('bootstrap_ambiguous_requires_restart','snapshot_unusable')",[intent_id])?;
            return Ok(ReconcileDecision::FullReseedRequired);
        }
        Ok(ReconcileDecision::ResumeImports)
    }

    pub fn status(&self, intent_id: &str) -> Result<RedactedStatus, BootstrapError> {
        let mut status=self.writer.connection().query_row("SELECT intent_id,state,exporter_liveness,guard_liveness,consistent_lsn,start_seq,snapshot_promotable,feedback_gate_open,revision FROM m3_bootstrap_runtime WHERE intent_id=?1",[intent_id],|r|Ok(RedactedStatus{intent_id:r.get(0)?,state:r.get(1)?,exporter_liveness:r.get(2)?,guard_liveness:r.get(3)?,creation_floor_lsn:r.get(4)?,start_seq:r.get::<_,Option<i64>>(5)?.map(|v|v as u64),acknowledged_importers:0,expected_importers:0,snapshot_promotable:r.get::<_,i64>(6)?==1,feedback_gate_open:r.get::<_,i64>(7)?==1,revision:r.get::<_,i64>(8)? as u64}))?;
        (status.expected_importers,status.acknowledged_importers)=self.writer.connection().query_row("SELECT count(*),sum(state='acknowledged') FROM m3_bootstrap_importers WHERE intent_id=?1",[intent_id],|r|Ok((r.get::<_,i64>(0)? as u64,r.get::<_,i64>(1)? as u64)))?;
        Ok(status)
    }

    fn importer_transition(
        &mut self,
        intent_id: &str,
        importer_id: &str,
        from: &str,
        to: &str,
        pid: Option<i32>,
        schema: Option<&str>,
    ) -> Result<(), BootstrapError> {
        let changed=self.writer.connection().execute("UPDATE m3_bootstrap_importers SET state=?3,backend_pid=coalesce(?4,backend_pid),snapshot_schema_fingerprint=coalesce(?5,snapshot_schema_fingerprint),revision=revision+1 WHERE intent_id=?1 AND importer_id=?2 AND state=?6",params![intent_id,importer_id,to,pid,schema,from])?;
        if changed != 1 {
            return Err(BootstrapError::Conflict("M3_IMPORTER_COMPLETION_STALE"));
        }
        Ok(())
    }
}

pub struct StoreFeedbackGate<'a> {
    store: &'a BootstrapStore,
    intent_id: &'a str,
}
impl<'a> StoreFeedbackGate<'a> {
    pub fn new(store: &'a BootstrapStore, intent_id: &'a str) -> Self {
        Self { store, intent_id }
    }
}
impl FeedbackGate for StoreFeedbackGate<'_> {
    fn permit(&mut self, durable_end_lsn: Option<u64>) -> FeedbackPermit {
        let row = self
            .store
            .writer
            .connection()
            .query_row(
                "SELECT feedback_gate_open,state FROM m3_bootstrap_runtime WHERE intent_id=?1",
                [self.intent_id],
                |r| Ok((r.get::<_, i64>(0)? == 1, r.get::<_, String>(1)?)),
            )
            .optional();
        match row {
            Ok(Some((true, _))) => FeedbackPermit::AllowSafeBoundary {
                lsn: durable_end_lsn.unwrap_or(0),
            },
            Ok(Some((false, state)))
                if matches!(
                    state.as_str(),
                    "imports_pending" | "exporter_release_permitted" | "exporter_released"
                ) =>
            {
                FeedbackPermit::Hold
            }
            _ => FeedbackPermit::Stale,
        }
    }
}

fn valid_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_ID_BYTES && value.is_ascii()
}
fn validate_prepare(input: &PrepareIntent) -> Result<(), BootstrapError> {
    if input.generation == 0
        || input.importers.is_empty()
        || input.importers.len() > 16
        || ![
            &input.intent_id,
            &input.capture_epoch,
            &input.source_system_id,
            &input.database_id,
            &input.slot_name,
            &input.table_set_fingerprint,
            &input.configuration_fingerprint,
            &input.created_at,
        ]
        .into_iter()
        .all(|v| valid_id(v))
        || input
            .importers
            .iter()
            .any(|i| !valid_id(&i.importer_id) || !is_sha256(&i.assigned_ranges_digest))
    {
        return Err(BootstrapError::Invalid("M3_BOOTSTRAP_PREPARE_INVALID"));
    }
    let mut ids = input
        .importers
        .iter()
        .map(|v| v.importer_id.as_str())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    if ids.len() != input.importers.len() {
        return Err(BootstrapError::Invalid("M3_IMPORTER_DUPLICATE"));
    }
    Ok(())
}
fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && (!b.is_ascii_alphabetic() || b.is_ascii_lowercase()))
}
pub fn digest_assignment(canonical_ranges: &[u8]) -> String {
    format!("{:x}", Sha256::digest(canonical_ranges))
}

/// Command-idle replication exporter retaining the one-use creation snapshot.
pub struct ExportedSlotSession {
    connection: pg_walstream::PgReplicationConnection,
    pub consistent_lsn: u64,
    snapshot_token: String,
    pub backend_pid: i32,
}
impl ExportedSlotSession {
    pub fn create(dsn: &str, slot: &str) -> Result<Self, BootstrapError> {
        use pg_walstream::{ReplicationSlotOptions, SlotType};
        if !valid_identifier(slot) {
            return Err(BootstrapError::Invalid("M3_SLOT_NAME_INVALID"));
        }
        let replication_dsn = append_replication(dsn);
        let mut connection = pg_walstream::PgReplicationConnection::connect(&replication_dsn)
            .map_err(|_| BootstrapError::Conflict("M3_EXPORTER_CONNECT_FAILED"))?;
        let backend_pid = query_pid(&mut connection)?;
        let result = connection
            .create_replication_slot_with_options(
                slot,
                SlotType::Logical,
                Some("pgoutput"),
                &ReplicationSlotOptions {
                    snapshot: Some("export".into()),
                    ..Default::default()
                },
            )
            .map_err(|_| BootstrapError::Conflict("M3_SLOT_EXPORT_FAILED"))?;
        let lsn = result
            .get_value(0, 1)
            .and_then(|v| parse_pg_lsn(&v))
            .ok_or(BootstrapError::Conflict("M3_CONSISTENT_POINT_MISSING"))?;
        let snapshot_token = result
            .get_value(0, 2)
            .filter(|v| !v.is_empty() && v.len() <= MAX_TOKEN_BYTES && v.is_ascii())
            .ok_or(BootstrapError::Conflict("M3_SNAPSHOT_TOKEN_MISSING"))?;
        Ok(Self {
            connection,
            consistent_lsn: lsn,
            snapshot_token,
            backend_pid,
        })
    }
    pub fn snapshot_token(&self) -> &str {
        &self.snapshot_token
    }
    pub fn command_idle_alive(&self) -> bool {
        self.connection.is_alive()
    }
}

pub struct GuardSession {
    connection: pg_walstream::PgReplicationConnection,
    pub backend_pid: i32,
}
impl GuardSession {
    pub fn acquire(
        dsn: &str,
        relations: &[String],
        statement_timeout_ms: u64,
        idle_timeout_ms: u64,
    ) -> Result<Self, BootstrapError> {
        if relations.is_empty() || statement_timeout_ms == 0 || idle_timeout_ms == 0 {
            return Err(BootstrapError::Invalid("M3_GUARD_INPUT_INVALID"));
        }
        let mut connection = pg_walstream::PgReplicationConnection::connect(dsn)
            .map_err(|_| BootstrapError::Conflict("M3_GUARD_CONNECT_FAILED"))?;
        let backend_pid = query_pid(&mut connection)?;
        connection.exec(&format!("SET statement_timeout='{statement_timeout_ms}ms'; SET idle_in_transaction_session_timeout='{idle_timeout_ms}ms'; BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")).map_err(|_|BootstrapError::Conflict("M3_GUARD_BEGIN_FAILED"))?;
        let mut locked = relations
            .iter()
            .map(|r| quote_relation(r))
            .collect::<Result<Vec<_>, _>>()?;
        locked.sort_unstable();
        locked.dedup();
        if locked.len() != relations.len() {
            return Err(BootstrapError::Invalid("M3_GUARD_RELATION_DUPLICATE"));
        }
        connection
            .exec(&format!(
                "LOCK TABLE {} IN ACCESS SHARE MODE",
                locked.join(",")
            ))
            .map_err(|_| BootstrapError::Conflict("M3_GUARD_LOCK_FAILED"))?;
        Ok(Self {
            connection,
            backend_pid,
        })
    }
    /// Verifies every guarded relation while the ACCESS SHARE locks are already held.
    pub fn verify_relations(&mut self, relations: &[String]) -> Result<(), BootstrapError> {
        for relation in relations {
            let quoted = quote_relation(relation)?;
            self.connection
                .exec(&format!("SELECT * FROM {quoted} LIMIT 0"))
                .map_err(|_| BootstrapError::Conflict("M3_GUARD_CONTRACT_FAILED"))?;
        }
        Ok(())
    }

    pub fn keepalive(&mut self) -> Result<(), BootstrapError> {
        self.connection
            .exec("SELECT 1")
            .map(|_| ())
            .map_err(|_| BootstrapError::Conflict("M3_GUARD_KEEPALIVE_FAILED"))
    }
}

pub struct ImporterSession {
    connection: pg_walstream::PgReplicationConnection,
    pub backend_pid: i32,
}
impl ImporterSession {
    /// `SET TRANSACTION SNAPSHOT` is sent in the same simple-query batch as BEGIN and before any
    /// catalog/data statement; callers cannot obtain a session in a pre-import queryable state.
    pub fn import(
        dsn: &str,
        snapshot_token: &str,
        statement_timeout_ms: u64,
        idle_timeout_ms: u64,
    ) -> Result<Self, BootstrapError> {
        if snapshot_token.is_empty()
            || snapshot_token.len() > MAX_TOKEN_BYTES
            || !snapshot_token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(BootstrapError::Invalid("M3_SNAPSHOT_TOKEN_INVALID"));
        }
        let mut connection = pg_walstream::PgReplicationConnection::connect(dsn)
            .map_err(|_| BootstrapError::Conflict("M3_IMPORTER_CONNECT_FAILED"))?;
        connection.exec(&format!("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '{snapshot_token}'; SET LOCAL statement_timeout='{statement_timeout_ms}ms'; SET LOCAL idle_in_transaction_session_timeout='{idle_timeout_ms}ms'")).map_err(|_|BootstrapError::Conflict("M3_SNAPSHOT_IMPORT_FAILED"))?;
        let backend_pid = query_pid(&mut connection)?;
        Ok(Self {
            connection,
            backend_pid,
        })
    }
    pub fn verify_relations(&mut self, relations: &[String]) -> Result<(), BootstrapError> {
        for relation in relations {
            let quoted = quote_relation(relation)?;
            self.connection
                .exec(&format!("SELECT * FROM {quoted} LIMIT 0"))
                .map_err(|_| BootstrapError::Conflict("M3_IMPORTED_CONTRACT_FAILED"))?;
        }
        Ok(())
    }
}

pub struct CaptureSession {
    connection: pg_walstream::PgReplicationConnection,
    pub backend_pid: i32,
}
impl CaptureSession {
    pub fn start(
        dsn: &str,
        slot: &str,
        consistent_lsn: u64,
        publication: &str,
    ) -> Result<Self, BootstrapError> {
        if !valid_identifier(slot) || !valid_identifier(publication) || consistent_lsn == 0 {
            return Err(BootstrapError::Invalid("M3_CAPTURE_INPUT_INVALID"));
        }
        let mut connection =
            pg_walstream::PgReplicationConnection::connect(&append_replication(dsn))
                .map_err(|_| BootstrapError::Conflict("M3_CAPTURE_CONNECT_FAILED"))?;
        let backend_pid = query_pid(&mut connection)?;
        connection
            .start_replication(
                slot,
                consistent_lsn,
                &[
                    ("proto_version", "1"),
                    ("publication_names", publication),
                    ("origin", "any"),
                    ("streaming", "false"),
                    ("two_phase", "false"),
                    ("binary", "false"),
                ],
            )
            .map_err(|_| BootstrapError::Conflict("M3_CAPTURE_START_FAILED"))?;
        Ok(Self {
            connection,
            backend_pid,
        })
    }
    pub fn alive(&self) -> bool {
        self.connection.is_alive()
    }
}
/// Live intent-bound session bundle. Construction is the only production path that can create the
/// permanent slot, so the type itself preserves intent -> guard -> export persistence -> CopyBoth.
pub struct BootstrapRuntime {
    store: BootstrapStore,
    intent_id: String,
    guard: GuardSession,
    _capture: CaptureSession,
    _importers: Vec<ImporterSession>,
}
impl BootstrapRuntime {
    pub fn start(
        writer: WriterConnection,
        intent: PrepareIntent,
        dsn: &str,
        relations: &[String],
        publication: &str,
        statement_timeout_ms: u64,
        idle_timeout_ms: u64,
        start_seq: u64,
    ) -> Result<Self, BootstrapError> {
        let mut store = BootstrapStore::open(writer)?;
        store.prepare(&intent)?;
        let mut guard =
            GuardSession::acquire(dsn, relations, statement_timeout_ms, idle_timeout_ms)?;
        guard.verify_relations(relations)?;
        store.record_guard_acquired(&intent.intent_id, guard.backend_pid)?;
        let exporter = ExportedSlotSession::create(dsn, &intent.slot_name)?;
        store.persist_export_response(
            &intent.intent_id,
            &ExportResponse {
                consistent_lsn: exporter.consistent_lsn,
                snapshot_token: exporter.snapshot_token().to_owned(),
                start_seq,
                exporter_backend_pid: exporter.backend_pid,
            },
        )?;
        let capture =
            CaptureSession::start(dsn, &intent.slot_name, exporter.consistent_lsn, publication)?;
        store.record_capture_started(&intent.intent_id, capture.backend_pid)?;
        let mut importers = Vec::with_capacity(intent.importers.len());
        for assignment in &intent.importers {
            let mut importer = ImporterSession::import(
                dsn,
                exporter.snapshot_token(),
                statement_timeout_ms,
                idle_timeout_ms,
            )?;
            store.snapshot_set_first(
                &intent.intent_id,
                &assignment.importer_id,
                importer.backend_pid,
            )?;
            importer.verify_relations(relations)?;
            store.bind_importer_contract(
                &intent.intent_id,
                &assignment.importer_id,
                &intent.table_set_fingerprint,
            )?;
            store.acknowledge_import(&intent.intent_id, &assignment.importer_id)?;
            importers.push(importer);
        }
        store.release_exporter(&intent.intent_id)?;
        drop(exporter);
        Ok(Self {
            store,
            intent_id: intent.intent_id,
            guard,
            _capture: capture,
            _importers: importers,
        })
    }

    pub fn keepalive(&mut self) -> Result<(), BootstrapError> {
        self.guard.keepalive()
    }

    pub fn invalidate(mut self, lost_session: &str) -> Result<(), BootstrapError> {
        self.store
            .invalidate_generation(&self.intent_id, lost_session)
    }
}

fn append_replication(dsn: &str) -> String {
    if dsn.contains('?') {
        format!("{dsn}&replication=database")
    } else {
        format!("{dsn}?replication=database")
    }
}
fn query_pid(
    connection: &mut pg_walstream::PgReplicationConnection,
) -> Result<i32, BootstrapError> {
    connection
        .exec("SELECT pg_backend_pid()::text")
        .ok()
        .and_then(|r| r.get_value(0, 0))
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .ok_or(BootstrapError::Conflict("M3_BACKEND_PID_MISSING"))
}
fn parse_pg_lsn(value: &str) -> Option<u64> {
    let (hi, lo) = value.split_once('/')?;
    Some((u64::from_str_radix(hi, 16).ok()? << 32) | u64::from_str_radix(lo, 16).ok()?)
}
fn valid_identifier(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 63
        && v.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        && v.as_bytes()[0].is_ascii_alphabetic()
}
fn quote_relation(v: &str) -> Result<String, BootstrapError> {
    let mut parts = v.split('.');
    let schema = parts
        .next()
        .filter(|v| valid_identifier(v))
        .ok_or(BootstrapError::Invalid("M3_RELATION_INVALID"))?;
    let table = parts
        .next()
        .filter(|v| valid_identifier(v))
        .ok_or(BootstrapError::Invalid("M3_RELATION_INVALID"))?;
    if parts.next().is_some() {
        return Err(BootstrapError::Invalid("M3_RELATION_INVALID"));
    }
    Ok(format!("\"{schema}\".\"{table}\""))
}

#[cfg(test)]
mod live_tests {
    use super::*;
    #[test]
    #[ignore = "requires pinned PostgreSQL 17.6 Compose"]
    fn exported_snapshot_uses_distinct_command_idle_sessions() {
        use crate::m2_schema::open_writer;
        let dsn = std::env::var("BORING_CDC_M3_DSN").expect("BORING_CDC_M3_DSN");
        let slot = std::env::var("BORING_CDC_M3_SLOT")
            .unwrap_or_else(|_| "boring_cdc_m3_bootstrap_test".into());
        let mut admin = pg_walstream::PgReplicationConnection::connect(&dsn).unwrap();
        let _=admin.exec(&format!("SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name='{slot}')"));
        admin.exec("DROP PUBLICATION IF EXISTS boring_cdc_m3_pub; DROP TABLE IF EXISTS public.m3_bootstrap_fixture; CREATE TABLE public.m3_bootstrap_fixture(id bigint PRIMARY KEY,payload text); INSERT INTO public.m3_bootstrap_fixture VALUES(1,'before'); CREATE PUBLICATION boring_cdc_m3_pub FOR TABLE public.m3_bootstrap_fixture").unwrap();
        let path =
            std::env::temp_dir().join(format!("m3-live-{}-{}.sqlite", std::process::id(), slot));
        let writer = open_writer(&path, "m3-live", 1, 0).unwrap();
        writer.connection().execute("INSERT INTO source_state(singleton,capture_epoch,source_system_id,timeline_id,database_id,slot_name,plugin,publication_fingerprint,protocol_fingerprint) VALUES(1,'epoch','system','1','database',?1,'pgoutput','publication','protocol')",[&slot]).unwrap();
        let relations = vec!["public.m3_bootstrap_fixture".to_string()];
        let intent = PrepareIntent {
            intent_id: "live-intent".into(),
            capture_epoch: "epoch".into(),
            source_system_id: "system".into(),
            database_id: "database".into(),
            slot_name: slot.clone(),
            generation: 1,
            table_set_fingerprint: "live-schema".into(),
            configuration_fingerprint: "live-config".into(),
            importers: vec![ImporterAssignment {
                importer_id: "worker-0".into(),
                assigned_ranges_digest: digest_assignment(b"all"),
            }],
            created_at: "unix-ms:0".into(),
        };
        let runtime = BootstrapRuntime::start(
            writer,
            intent,
            &dsn,
            &relations,
            "boring_cdc_m3_pub",
            10_000,
            30_000,
            0,
        )
        .unwrap();
        let (guard, exporter, capture, importer): (i32, i32, i32, i32) = runtime
            .store
            .writer
            .connection()
            .query_row(
                "SELECT r.guard_backend_pid,r.exporter_backend_pid,r.capture_backend_pid,i.backend_pid FROM m3_bootstrap_runtime r JOIN m3_bootstrap_importers i USING(intent_id) WHERE r.intent_id='live-intent'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        let mut pids = vec![guard, exporter, capture, importer];
        pids.sort_unstable();
        pids.dedup();
        assert_eq!(pids.len(), 4);
        assert_eq!(
            runtime.store.status("live-intent").unwrap().state,
            "exporter_released"
        );
        runtime.invalidate("importer").unwrap();
        let mut cleanup = pg_walstream::PgReplicationConnection::connect(&dsn).unwrap();
        cleanup
            .exec(&format!("SELECT pg_drop_replication_slot('{slot}')"))
            .unwrap();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m2_schema::open_writer;
    use tempfile_path::TempPath;
    mod tempfile_path {
        use std::path::{Path, PathBuf};
        pub struct TempPath(PathBuf);
        impl TempPath {
            pub fn new() -> Self {
                let p = std::env::temp_dir().join(format!(
                    "m3-bootstrap-{}-{}.db",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                ));
                Self(p)
            }
            pub fn as_path(&self) -> &Path {
                &self.0
            }
        }
        impl Drop for TempPath {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
                let _ = std::fs::remove_file(format!("{}-wal", self.0.display()));
                let _ = std::fs::remove_file(format!("{}-shm", self.0.display()));
            }
        }
    }
    fn store() -> (TempPath, BootstrapStore) {
        let p = TempPath::new();
        let w = open_writer(p.as_path(), "test", 1, 0).unwrap();
        w.connection().execute("INSERT INTO source_state(singleton,capture_epoch,source_system_id,timeline_id,database_id,slot_name,plugin,publication_fingerprint,protocol_fingerprint) VALUES(1,'epoch','system','1','database','slot','pgoutput','publication','protocol')",[]).unwrap();
        (p, BootstrapStore::open(w).unwrap())
    }
    fn intent() -> PrepareIntent {
        PrepareIntent {
            intent_id: "intent".into(),
            capture_epoch: "epoch".into(),
            source_system_id: "system".into(),
            database_id: "database".into(),
            slot_name: "slot".into(),
            generation: 1,
            table_set_fingerprint: "tables".into(),
            configuration_fingerprint: "config".into(),
            importers: vec![ImporterAssignment {
                importer_id: "worker-0".into(),
                assigned_ranges_digest: digest_assignment(b"all"),
            }],
            created_at: "unix-ms:0".into(),
        }
    }
    fn exported(s: &mut BootstrapStore) {
        s.prepare(&intent()).unwrap();
        s.record_guard_acquired("intent", 11).unwrap();
        s.persist_export_response(
            "intent",
            &ExportResponse {
                consistent_lsn: 16,
                snapshot_token: String::from(concat!("00000003-", "00000001-1")),
                start_seq: 0,
                exporter_backend_pid: 12,
            },
        )
        .unwrap();
        let capture_pid: Option<i32> = s
            .writer
            .connection()
            .query_row(
                "SELECT capture_backend_pid FROM m3_bootstrap_runtime WHERE intent_id='intent'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            capture_pid, None,
            "CopyBoth cannot predate token durability"
        );
        s.record_capture_started("intent", 13).unwrap();
    }
    #[test]
    fn intent_is_durable_before_export_and_floor_is_separate() {
        let (_p, mut s) = store();
        s.prepare(&intent()).unwrap();
        let st = s.status("intent").unwrap();
        assert_eq!(st.state, "prepared");
        assert_eq!(st.creation_floor_lsn, None);
        exported_after_prepare(&mut s);
        let st = s.status("intent").unwrap();
        assert_eq!(st.creation_floor_lsn.as_deref(), Some("0000000000000010"));
        assert_eq!(st.start_seq, Some(0));
    }
    fn exported_after_prepare(s: &mut BootstrapStore) {
        s.record_guard_acquired("intent", 11).unwrap();
        s.persist_export_response(
            "intent",
            &ExportResponse {
                consistent_lsn: 16,
                snapshot_token: String::from(concat!("00000003-", "00000001-1")),
                start_seq: 0,
                exporter_backend_pid: 12,
            },
        )
        .unwrap();
        s.record_capture_started("intent", 13).unwrap();
    }

    #[test]
    fn importer_acknowledgements_gate_exporter_release() {
        let (_p, mut s) = store();
        exported(&mut s);
        assert!(s.release_exporter("intent").is_err());
        s.snapshot_set_first("intent", "worker-0", 14).unwrap();
        s.bind_importer_contract("intent", "worker-0", "schema")
            .unwrap();
        s.acknowledge_import("intent", "worker-0").unwrap();
        let permitted = s.status("intent").unwrap();
        assert_eq!(permitted.state, "exporter_release_permitted");
        assert!(permitted.feedback_gate_open);
        s.release_exporter("intent").unwrap();
        assert_eq!(s.status("intent").unwrap().state, "exporter_released");
    }
    #[test]
    fn invalidation_atomically_releases_feedback_gate() {
        let (_p, mut s) = store();
        exported(&mut s);
        s.invalidate_generation("intent", "exporter").unwrap();
        let st = s.status("intent").unwrap();
        assert_eq!(st.state, "snapshot_unusable");
        assert!(!st.snapshot_promotable && st.feedback_gate_open);
        let mut gate = StoreFeedbackGate::new(&s, "intent");
        assert_eq!(
            gate.permit(Some(32)),
            FeedbackPermit::AllowSafeBoundary { lsn: 32 }
        );
    }
    #[test]
    fn pending_imports_hold_feedback() {
        let (_p, mut s) = store();
        exported(&mut s);
        let mut gate = StoreFeedbackGate::new(&s, "intent");
        assert_eq!(gate.permit(Some(32)), FeedbackPermit::Hold);
    }
    #[test]
    fn lost_response_never_recreates_or_drops_slot() {
        let (_p, mut s) = store();
        s.prepare(&intent()).unwrap();
        assert_eq!(
            s.reconcile("intent", true, true, true).unwrap(),
            ReconcileDecision::BootstrapAmbiguousRequiresRestart
        );
        assert_eq!(
            s.reconcile("intent", true, true, true).unwrap(),
            ReconcileDecision::ExistingSlotGenerationRequired
        );
    }
    #[test]
    fn ambiguous_continuity_requires_full_reseed() {
        let (_p, mut s) = store();
        s.prepare(&intent()).unwrap();
        s.reconcile("intent", true, true, true).unwrap();
        assert_eq!(
            s.reconcile("intent", true, false, true).unwrap(),
            ReconcileDecision::FullReseedRequired
        );
    }
    #[test]
    fn redacted_status_never_contains_snapshot_token() {
        let (_p, mut s) = store();
        exported(&mut s);
        let json = serde_json::to_string(&s.status("intent").unwrap()).unwrap();
        assert!(!json.contains("00000003"));
    }
}
