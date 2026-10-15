//! Durable initial-bootstrap coordinator.
//!
//! This module owns only the M3 intent/export/import lifecycle. It deliberately reuses the M2
//! writer admission, ownership, capture feedback interface, and source schema. PostgreSQL session
//! orchestration is exposed behind small capabilities so the permanent-slot creation response can
//! be committed before CopyBoth starts on a different connection.

use crate::m2_schema::WriterConnection;
use rusqlite::params;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fmt;

pub const OWNER_BEAD: &str = "boring-cdc-m3-bootstrap";
const MAX_ID_BYTES: usize = 256;
const MAX_TOKEN_BYTES: usize = 1_024;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

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
CREATE TABLE IF NOT EXISTS m3_snapshot_schema_proofs(
 intent_id TEXT NOT NULL,
 importer_id TEXT NOT NULL,
 capture_epoch TEXT NOT NULL,
 relation_id TEXT NOT NULL,
 namespace TEXT NOT NULL,
 relation_name TEXT NOT NULL,
 schema_fingerprint TEXT NOT NULL,
 canonical_schema BLOB NOT NULL,
 schema_checksum TEXT NOT NULL,
 PRIMARY KEY(intent_id,importer_id),
 FOREIGN KEY(intent_id,importer_id) REFERENCES m3_bootstrap_importers(intent_id,importer_id)
) STRICT;
CREATE TRIGGER IF NOT EXISTS m3_snapshot_schema_proofs_immutable_update BEFORE UPDATE ON m3_snapshot_schema_proofs
 BEGIN SELECT RAISE(ABORT,'imported schema proof is immutable'); END;
CREATE TRIGGER IF NOT EXISTS m3_snapshot_schema_proofs_immutable_delete BEFORE DELETE ON m3_snapshot_schema_proofs
 BEGIN SELECT RAISE(ABORT,'imported schema proof is immutable'); END;
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
pub(crate) struct SnapshotSchemaProof {
    pub capture_epoch: String,
    pub relation_id: String,
    pub namespace: String,
    pub relation_name: String,
    pub schema_fingerprint: String,
    pub canonical_schema: Vec<u8>,
    pub schema_checksum: String,
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
        crate::m3_fence::install(writer.connection())
            .map_err(|_| BootstrapError::Conflict("M3_FEEDBACK_GATE_INSTALL_FAILED"))?;
        Ok(Self { writer })
    }

    pub fn into_writer(self) -> WriterConnection {
        self.writer
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
        crate::m3_fence::register_feedback_gate(&tx, &input.intent_id, input.generation)
            .map_err(|_| BootstrapError::Conflict("M3_FEEDBACK_GATE_PREPARE_FAILED"))?;
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
    pub(crate) fn bind_imported_schema(
        &mut self,
        intent_id: &str,
        importer_id: &str,
        proof: &SnapshotSchemaProof,
    ) -> Result<(), BootstrapError> {
        let tx = self.writer.connection_mut().transaction()?;
        let changed = tx.execute("UPDATE m3_bootstrap_importers SET state='contract_bound',snapshot_schema_fingerprint=?3,revision=revision+1 WHERE intent_id=?1 AND importer_id=?2 AND state='snapshot_set_first' AND NOT EXISTS(SELECT 1 FROM m3_snapshot_schema_proofs WHERE intent_id=?1 AND importer_id=?2)",params![intent_id,importer_id,proof.schema_fingerprint])?;
        if changed != 1 {
            return Err(BootstrapError::Conflict("M3_IMPORTED_SCHEMA_STALE"));
        }
        tx.execute("INSERT INTO m3_snapshot_schema_proofs(intent_id,importer_id,capture_epoch,relation_id,namespace,relation_name,schema_fingerprint,canonical_schema,schema_checksum) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",params![intent_id,importer_id,proof.capture_epoch,proof.relation_id,proof.namespace,proof.relation_name,proof.schema_fingerprint,proof.canonical_schema,proof.schema_checksum])?;
        tx.commit()?;
        Ok(())
    }
    pub fn acknowledge_import(
        &mut self,
        intent_id: &str,
        importer_id: &str,
    ) -> Result<(), BootstrapError> {
        let generation: i64 = self.writer.connection().query_row(
            "SELECT generation FROM m3_bootstrap_runtime WHERE intent_id=?1",
            [intent_id],
            |r| r.get(0),
        )?;
        crate::m3_fence::acknowledge_and_open(
            self.writer.connection_mut(),
            intent_id,
            importer_id,
            generation as u64,
        )
        .map_err(|_| BootstrapError::Conflict("M3_IMPORT_ACK_GATE_STALE"))
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
        let generation: i64 = self.writer.connection().query_row(
            "SELECT generation FROM m3_bootstrap_runtime WHERE intent_id=?1",
            [intent_id],
            |r| r.get(0),
        )?;
        crate::m3_fence::invalidate_and_release(
            self.writer.connection_mut(),
            intent_id,
            generation as u64,
            lost_session,
        )
        .map_err(|_| BootstrapError::Conflict("M3_INVALIDATION_STALE"))
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
            crate::m3_fence::reconcile_and_release(
                self.writer.connection_mut(),
                intent_id,
                "bootstrap_ambiguous_requires_restart",
            )
            .map_err(|_| BootstrapError::Conflict("M3_RECONCILE_GATE_STALE"))?;
            return Ok(ReconcileDecision::BootstrapAmbiguousRequiresRestart);
        }
        if state == "bootstrap_ambiguous_requires_restart" || state == "snapshot_unusable" {
            if remote_slot_exists && wal_continuous && provenance_matches {
                crate::m3_fence::reconcile_and_release(
                    self.writer.connection_mut(),
                    intent_id,
                    "existing_slot_generation_required",
                )
                .map_err(|_| BootstrapError::Conflict("M3_RECONCILE_GATE_STALE"))?;
                return Ok(ReconcileDecision::ExistingSlotGenerationRequired);
            }
            crate::m3_fence::reconcile_and_release(
                self.writer.connection_mut(),
                intent_id,
                "full_reseed_required",
            )
            .map_err(|_| BootstrapError::Conflict("M3_RECONCILE_GATE_STALE"))?;
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
    guarded_relations: Vec<String>,
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
            guarded_relations: locked,
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

    /// `pg_locks.waitstart` is the server clock for this actual blocked DDL request. Checking
    /// the held relation OIDs avoids treating unrelated database waiters as source impact.
    pub fn conflicting_waiter_exceeded(&mut self, bound_ms: u64) -> Result<bool, BootstrapError> {
        if bound_ms == 0 || self.guarded_relations.is_empty() {
            return Err(BootstrapError::Invalid("M3_DDL_WAITER_BOUND_INVALID"));
        }
        let relations = self
            .guarded_relations
            .iter()
            .map(|relation| format!("to_regclass('{relation}')::oid"))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE NOT granted AND mode='AccessExclusiveLock' AND relation=ANY(ARRAY[{relations}]) AND waitstart IS NOT NULL AND extract(epoch FROM (clock_timestamp()-waitstart))*1000 >= {bound_ms})::text"
        );
        let value = self
            .connection
            .exec(&sql)
            .map_err(|_| BootstrapError::Conflict("M3_DDL_WAITER_PROBE_FAILED"))?
            .get_value(0, 0)
            .ok_or(BootstrapError::Conflict("M3_DDL_WAITER_PROBE_FAILED"))?;
        match value.as_str() {
            "t" | "true" => Ok(true),
            "f" | "false" => Ok(false),
            _ => Err(BootstrapError::Conflict("M3_DDL_WAITER_PROBE_FAILED")),
        }
    }

    fn release_and_verify(&mut self) -> Result<(), BootstrapError> {
        self.connection
            .exec("ROLLBACK")
            .map_err(|_| BootstrapError::Conflict("M3_GUARD_RELEASE_UNVERIFIED"))?;
        let relations = self
            .guarded_relations
            .iter()
            .map(|relation| format!("to_regclass('{relation}')::oid"))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND granted AND mode='AccessShareLock' AND relation=ANY(ARRAY[{relations}]))::text"
        );
        let value = self
            .connection
            .exec(&sql)
            .map_err(|_| BootstrapError::Conflict("M3_GUARD_RELEASE_UNVERIFIED"))?
            .get_value(0, 0)
            .ok_or(BootstrapError::Conflict("M3_GUARD_RELEASE_UNVERIFIED"))?;
        if matches!(value.as_str(), "f" | "false") {
            Ok(())
        } else {
            Err(BootstrapError::Conflict("M3_GUARD_RELEASE_UNVERIFIED"))
        }
    }
}

pub struct ImporterSession {
    connection: pg_walstream::PgReplicationConnection,
    pub backend_pid: i32,
}

/// Capability proving that bootstrap imported the exported snapshot, verified the guarded schema,
/// and durably acknowledged that exact importer. There is deliberately no raw-connection
/// constructor: only [`BootstrapRuntime`] can mint and transfer this capability.
pub struct ImportedSnapshotSession {
    connection: pg_walstream::PgReplicationConnection,
    importer_id: String,
    assigned_ranges_digest: String,
    table_set_fingerprint: String,
    guarded_relations: Vec<String>,
}
impl ImportedSnapshotSession {
    pub(crate) fn into_planner_connection(
        self,
        importer_id: &str,
        assigned_ranges_digest: &str,
        schema_fingerprint: &str,
        relation: &str,
    ) -> Result<pg_walstream::PgReplicationConnection, BootstrapError> {
        if self.importer_id != importer_id
            || self.assigned_ranges_digest != assigned_ranges_digest
            || self.table_set_fingerprint != schema_fingerprint
            || !self.guarded_relations.iter().any(|value| value == relation)
        {
            return Err(BootstrapError::Conflict(
                "M3_IMPORTER_CAPABILITY_BINDING_MISMATCH",
            ));
        }
        Ok(self.connection)
    }
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
    pub(crate) fn canonical_schema(
        &mut self,
        publication: &str,
        source: &crate::m1_source_identity::SourceIdentity,
        expected: &crate::m1_decoder::RelationContract,
        capture_epoch: &str,
    ) -> Result<SnapshotSchemaProof, BootstrapError> {
        let catalog = crate::m2_catalog_reader::read_simple_relation_in_snapshot(
            &mut self.connection,
            publication,
            source,
            expected,
        )
        .map_err(|_| BootstrapError::Conflict("M3_IMPORTED_CATALOG_INVALID"))?;
        let canonical_schema = catalog
            .encode()
            .map_err(|_| BootstrapError::Conflict("M3_IMPORTED_SCHEMA_INVALID"))?;
        let version = crate::m1_source_identity::RelationSchemaVersion::derive(
            catalog.logical_table,
            catalog.relation_id,
            &canonical_schema,
        )
        .map_err(|_| BootstrapError::Conflict("M3_IMPORTED_SCHEMA_INVALID"))?;
        Ok(SnapshotSchemaProof {
            capture_epoch: capture_epoch.to_owned(),
            relation_id: catalog.relation_id.to_string(),
            namespace: expected.relation.namespace.clone(),
            relation_name: expected.relation.name.clone(),
            schema_fingerprint: hex(&version.fingerprint().bytes()),
            schema_checksum: format!("{:x}", Sha256::digest(&canonical_schema)),
            canonical_schema,
        })
    }

    pub(crate) fn into_acknowledged(
        self,
        importer_id: String,
        assigned_ranges_digest: String,
        table_set_fingerprint: String,
        guarded_relations: Vec<String>,
    ) -> ImportedSnapshotSession {
        ImportedSnapshotSession {
            connection: self.connection,
            importer_id,
            assigned_ranges_digest,
            table_set_fingerprint,
            guarded_relations,
        }
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
#[derive(Clone, Copy, Debug)]
pub struct SessionBounds {
    pub statement_timeout_ms: u64,
    pub idle_timeout_ms: u64,
}

pub struct BootstrapRuntime {
    store: BootstrapStore,
    intent_id: String,
    guard: GuardSession,
    _capture: CaptureSession,
    importers: Vec<(String, ImportedSnapshotSession)>,
}
/// PostgreSQL snapshot sessions remain owned while the sole SQLite writer is in the planner.
pub struct BootstrapSessions {
    intent_id: String,
    writer_attestation: crate::m2_schema::WriterAttestation,
    guard: GuardSession,
    capture: CaptureSession,
    importers: Vec<(String, ImportedSnapshotSession)>,
}
impl BootstrapSessions {
    pub fn keepalive(&mut self) -> Result<(), BootstrapError> {
        self.guard.keepalive()
    }

    pub fn capture_alive(&self) -> bool {
        self.capture.alive()
    }

    /// Consume the original CopyBoth generation with M2 durability while the guard stays live.
    pub async fn capture_until<J, S, G>(
        &mut self,
        config: &crate::article1_capture::CaptureConfig,
        cancellation: &pg_walstream::CancellationToken,
        runtime: &mut crate::m2_capture_runtime::CaptureRuntime<J, S, G>,
        stop_after_commits: u64,
        ddl_waiter_bound_ms: u64,
        mut ownership_probe: impl FnMut(
            &mut crate::m2_capture_runtime::CaptureRuntime<J, S, G>,
        ) -> Result<bool, crate::m2_journal::JournalError>,
    ) -> Result<(), crate::article1_capture::CaptureFailure>
    where
        J: crate::m2_capture_runtime::DurableJournal,
        S: crate::m2_capture_runtime::SpoolFactory,
        G: crate::m2_capture_runtime::FeedbackGate,
    {
        let guard = &mut self.guard;
        let capture_pid = self.capture.backend_pid;
        crate::m2_capture_runtime::capture_existing_copyboth_until(
            config,
            &mut self.capture.connection,
            capture_pid,
            cancellation,
            runtime,
            stop_after_commits,
            |runtime| {
                if guard.keepalive().is_err()
                    || guard
                        .conflicting_waiter_exceeded(ddl_waiter_bound_ms)
                        .unwrap_or(true)
                {
                    return Ok(false);
                }
                ownership_probe(runtime)
            },
        )
        .await
    }

    pub fn conflicting_ddl_waiter_exceeded(
        &mut self,
        bound_ms: u64,
    ) -> Result<bool, BootstrapError> {
        self.guard.conflicting_waiter_exceeded(bound_ms)
    }

    pub fn take_importer(
        &mut self,
        importer_id: &str,
    ) -> Result<ImportedSnapshotSession, BootstrapError> {
        take_importer(&mut self.importers, importer_id)
    }

    pub fn resume(self, writer: WriterConnection) -> Result<BootstrapRuntime, BootstrapError> {
        if writer.attestation() != &self.writer_attestation {
            return Err(BootstrapError::Conflict(
                "M3_WRITER_HANDOFF_IDENTITY_MISMATCH",
            ));
        }
        Ok(BootstrapRuntime {
            store: BootstrapStore { writer },
            intent_id: self.intent_id,
            guard: self.guard,
            _capture: self.capture,
            importers: self.importers,
        })
    }
}
fn take_importer(
    importers: &mut Vec<(String, ImportedSnapshotSession)>,
    importer_id: &str,
) -> Result<ImportedSnapshotSession, BootstrapError> {
    let position = importers
        .iter()
        .position(|(id, _)| id == importer_id)
        .ok_or(BootstrapError::Conflict(
            "M3_IMPORTER_CAPABILITY_UNAVAILABLE",
        ))?;
    Ok(importers.swap_remove(position).1)
}
impl BootstrapRuntime {
    /// Move, rather than reopen, the only SQLite writer into the planner. A caller must return
    /// that writer with `BootstrapSessions::resume` before using the canonical invalidation path.
    pub fn into_planner_handoff(self) -> (WriterConnection, BootstrapSessions) {
        let Self {
            store,
            intent_id,
            guard,
            _capture,
            importers,
        } = self;
        let writer = store.into_writer();
        let writer_attestation = writer.attestation().clone();
        (
            writer,
            BootstrapSessions {
                intent_id,
                writer_attestation,
                guard,
                capture: _capture,
                importers,
            },
        )
    }
    pub fn start(
        writer: WriterConnection,
        intent: PrepareIntent,
        dsn: &str,
        relations: &[String],
        publication: &str,
        bounds: SessionBounds,
        start_seq: u64,
    ) -> Result<Self, BootstrapError> {
        if relations.len() != 1 {
            return Err(BootstrapError::Invalid(
                "M3_SINGLE_RELATION_PROFILE_REQUIRED",
            ));
        }
        let mut store = BootstrapStore::open(writer)?;
        store.prepare(&intent)?;
        let mut guard = GuardSession::acquire(
            dsn,
            relations,
            bounds.statement_timeout_ms,
            bounds.idle_timeout_ms,
        )?;
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
        let config = crate::article1_capture::CaptureConfig::production(
            dsn,
            publication,
            &intent.slot_name,
            relations.iter().cloned(),
        )
        .map_err(|_| BootstrapError::Invalid("M3_CAPTURE_CONFIG_INVALID"))?;
        let contracts =
            crate::article1_capture::preflight_active_capture(&config, capture.backend_pid)
                .map_err(|_| BootstrapError::Conflict("M3_CAPTURE_PREFLIGHT_FAILED"))?;
        let (system_id,timeline_id,database_id,plugin,publication_fp,protocol_fp): (String,String,String,String,String,String) = store.writer.connection().query_row(
            "SELECT source_system_id,timeline_id,database_id,plugin,publication_fingerprint,protocol_fingerprint FROM source_state WHERE singleton=1 AND capture_epoch=?1 AND slot_name=?2",
            params![intent.capture_epoch,intent.slot_name],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?)),
        )?;
        if protocol_fp != "pgoutput-v1" {
            return Err(BootstrapError::Conflict("M3_SOURCE_IDENTITY_INVALID"));
        }
        let source = crate::m1_source_identity::SourceIdentity {
            system_identifier: system_id
                .parse()
                .map_err(|_| BootstrapError::Conflict("M3_SOURCE_IDENTITY_INVALID"))?,
            timeline: timeline_id
                .parse()
                .map_err(|_| BootstrapError::Conflict("M3_SOURCE_IDENTITY_INVALID"))?,
            database_identity: database_id
                .parse()
                .map_err(|_| BootstrapError::Conflict("M3_SOURCE_IDENTITY_INVALID"))?,
            slot_name: intent.slot_name.clone(),
            plugin,
            publication_fingerprint: crate::m1_source_identity::Fingerprint::from_lower_hex(
                &publication_fp,
            )
            .ok_or(BootstrapError::Conflict("M3_SOURCE_IDENTITY_INVALID"))?,
            protocol_fingerprint: crate::m1_source_identity::supported_protocol_fingerprint(),
        };
        source
            .validate()
            .map_err(|_| BootstrapError::Conflict("M3_SOURCE_IDENTITY_INVALID"))?;
        if system_id != intent.source_system_id || database_id != intent.database_id {
            return Err(BootstrapError::Conflict("M3_SOURCE_IDENTITY_MISMATCH"));
        }
        let relation = &relations[0];
        let expected = contracts
            .values()
            .find(|contract| {
                format!("{}.{}", contract.relation.namespace, contract.relation.name) == *relation
            })
            .ok_or(BootstrapError::Conflict("M3_RELATION_CONTRACT_MISSING"))?;
        let mut importers = Vec::with_capacity(intent.importers.len());
        for assignment in &intent.importers {
            let mut importer = ImporterSession::import(
                dsn,
                exporter.snapshot_token(),
                bounds.statement_timeout_ms,
                bounds.idle_timeout_ms,
            )?;
            store.snapshot_set_first(
                &intent.intent_id,
                &assignment.importer_id,
                importer.backend_pid,
            )?;
            importer.verify_relations(relations)?;
            let proof =
                importer.canonical_schema(publication, &source, expected, &intent.capture_epoch)?;
            store.bind_imported_schema(&intent.intent_id, &assignment.importer_id, &proof)?;
            store.acknowledge_import(&intent.intent_id, &assignment.importer_id)?;
            importers.push((
                assignment.importer_id.clone(),
                importer.into_acknowledged(
                    assignment.importer_id.clone(),
                    assignment.assigned_ranges_digest.clone(),
                    proof.schema_fingerprint,
                    relations.to_vec(),
                ),
            ));
        }
        store.release_exporter(&intent.intent_id)?;
        drop(exporter);
        Ok(Self {
            store,
            intent_id: intent.intent_id,
            guard,
            _capture: capture,
            importers,
        })
    }

    /// Transfers one acknowledged imported session to its assigned planner worker. The runtime
    /// never exposes a connection before the acknowledgement transaction has committed, and a
    /// capability can be taken only once.
    pub fn take_importer(
        &mut self,
        importer_id: &str,
    ) -> Result<ImportedSnapshotSession, BootstrapError> {
        take_importer(&mut self.importers, importer_id)
    }

    pub fn keepalive(&mut self) -> Result<(), BootstrapError> {
        self.guard.keepalive()
    }

    pub fn conflicting_ddl_waiter_exceeded(
        &mut self,
        bound_ms: u64,
    ) -> Result<bool, BootstrapError> {
        self.guard.conflicting_waiter_exceeded(bound_ms)
    }

    pub fn invalidate_for_ddl_waiter(mut self) -> Result<(), BootstrapError> {
        self.store.invalidate_generation(&self.intent_id, "guard")?;
        self.guard.release_and_verify()
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
pub(crate) fn query_pid(
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
    fn live_identity(
        dsn: &str,
        publication: &str,
        relation: &str,
    ) -> (String, String, String, String, String) {
        let mut replication =
            pg_walstream::PgReplicationConnection::connect(&append_replication(dsn)).unwrap();
        let identified = replication.exec("IDENTIFY_SYSTEM").unwrap();
        let mut sql = pg_walstream::PgReplicationConnection::connect(dsn).unwrap();
        let database = sql
            .exec("SELECT oid::text FROM pg_database WHERE datname=current_database()")
            .unwrap();
        let owner = sql.exec("SELECT current_user").unwrap();
        let fingerprint = crate::m1_control_fixtures::PublicationSpec::new(
            publication,
            &owner.get_value(0, 0).unwrap(),
            [relation.to_owned()],
        )
        .fingerprint();
        (
            identified.get_value(0, 0).unwrap(),
            identified.get_value(0, 1).unwrap(),
            database.get_value(0, 0).unwrap(),
            fingerprint,
            "pgoutput-v1".into(),
        )
    }
    #[test]
    #[ignore = "requires pinned PostgreSQL 17.6 Compose"]
    fn bounded_ddl_waiter_releases_only_guarded_relation_lock() {
        use crate::m2_schema::open_writer;
        let dsn = std::env::var("BORING_CDC_M3_DSN").expect("BORING_CDC_M3_DSN");
        let table = format!("m3_guard_waiter_{}", std::process::id());
        let publication = format!("m3_guard_waiter_pub_{}", std::process::id());
        let slot = format!("m3_guard_waiter_slot_{}", std::process::id());
        let relation = format!("public.{table}");
        let mut admin = pg_walstream::PgReplicationConnection::connect(&dsn).unwrap();
        admin
            .exec(&format!("CREATE TABLE {relation}(id bigint PRIMARY KEY)"))
            .unwrap();
        admin.exec("CREATE SCHEMA IF NOT EXISTS boring_cdc_control; CREATE TABLE boring_cdc_control.heartbeat(id smallint PRIMARY KEY CHECK(id=1),nonce bigint NOT NULL,updated_at timestamptz NOT NULL); CREATE TABLE boring_cdc_control.capture_fences(id smallint PRIMARY KEY CHECK(id=1),capture_epoch bigint NOT NULL,generation bigint NOT NULL,table_set_fingerprint bytea NOT NULL,unique_nonce bytea NOT NULL)").unwrap();
        admin.exec(&format!("CREATE PUBLICATION {publication} FOR TABLE {relation},boring_cdc_control.heartbeat,boring_cdc_control.capture_fences")).unwrap();
        let path =
            std::env::temp_dir().join(format!("m3-guard-waiter-{}.sqlite", std::process::id()));
        let writer = open_writer(&path, "guard-live", 1, 0).unwrap();
        let (system_id, timeline_id, database_id, publication_fp, protocol_fp) =
            live_identity(&dsn, &publication, &relation);
        writer.connection().execute("INSERT INTO source_state(singleton,capture_epoch,source_system_id,timeline_id,database_id,slot_name,plugin,publication_fingerprint,protocol_fingerprint) VALUES(1,'epoch',?1,?2,?3,?4,'pgoutput',?5,?6)",rusqlite::params![system_id,timeline_id,database_id,slot,publication_fp,protocol_fp]).unwrap();
        let intent = PrepareIntent {
            intent_id: "guard-live-intent".into(),
            capture_epoch: "epoch".into(),
            source_system_id: system_id.clone(),
            database_id: database_id.clone(),
            slot_name: slot.clone(),
            generation: 1,
            table_set_fingerprint: "guard-live-schema".into(),
            configuration_fingerprint: "guard-live-config".into(),
            importers: vec![ImporterAssignment {
                importer_id: "worker-0".into(),
                assigned_ranges_digest: digest_assignment(b"all"),
            }],
            created_at: "unix-ms:0".into(),
        };
        let mut runtime = BootstrapRuntime::start(
            writer,
            intent,
            &dsn,
            std::slice::from_ref(&relation),
            &publication,
            SessionBounds {
                statement_timeout_ms: 10_000,
                idle_timeout_ms: 30_000,
            },
            0,
        )
        .unwrap();
        assert!(!runtime.conflicting_ddl_waiter_exceeded(100).unwrap());
        let worker_dsn = dsn.clone();
        let worker_relation = relation.clone();
        let worker = std::thread::spawn(move || {
            let mut ddl = pg_walstream::PgReplicationConnection::connect(&worker_dsn).unwrap();
            ddl.exec(&format!(
                "ALTER TABLE {worker_relation} ADD COLUMN changed bigint"
            ))
            .unwrap();
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if runtime.conflicting_ddl_waiter_exceeded(100).unwrap() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "DDL waiter was not detected"
            );
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        runtime.invalidate_for_ddl_waiter().unwrap();
        worker.join().unwrap();
        let connection = rusqlite::Connection::open(&path).unwrap();
        let state: (String, String, i64, i64) = connection.query_row(
            "SELECT state,guard_liveness,snapshot_promotable,feedback_gate_open FROM m3_bootstrap_runtime WHERE intent_id='guard-live-intent'",
            [], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).unwrap();
        assert_eq!(state, ("snapshot_unusable".into(), "lost".into(), 0, 1));
        drop(connection);
        admin
            .exec(&format!("SELECT pg_drop_replication_slot('{slot}')"))
            .unwrap();
        admin
            .exec(&format!("DROP PUBLICATION {publication}"))
            .unwrap();
        admin.exec(&format!("DROP TABLE {relation}")).unwrap();
        admin
            .exec("DROP SCHEMA boring_cdc_control CASCADE")
            .unwrap();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }
    #[test]
    #[ignore = "requires pinned PostgreSQL 17.6 Compose"]
    fn exported_snapshot_uses_distinct_command_idle_sessions() {
        use crate::m2_schema::open_writer;
        let dsn = std::env::var("BORING_CDC_M3_DSN").expect("BORING_CDC_M3_DSN");
        let slot = std::env::var("BORING_CDC_M3_SLOT")
            .unwrap_or_else(|_| "boring_cdc_m3_bootstrap_test".into());
        let mut admin = pg_walstream::PgReplicationConnection::connect(&dsn).unwrap();
        let _=admin.exec(&format!("SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name='{slot}')"));
        admin.exec("DROP PUBLICATION IF EXISTS boring_cdc_m3_pub; DROP TABLE IF EXISTS public.m3_bootstrap_fixture; DROP SCHEMA IF EXISTS boring_cdc_control CASCADE; CREATE TABLE public.m3_bootstrap_fixture(id bigint PRIMARY KEY,payload text); INSERT INTO public.m3_bootstrap_fixture VALUES(1,'before'); CREATE SCHEMA boring_cdc_control; CREATE TABLE boring_cdc_control.heartbeat(id smallint PRIMARY KEY CHECK(id=1),nonce bigint NOT NULL,updated_at timestamptz NOT NULL); INSERT INTO boring_cdc_control.heartbeat VALUES(1,0,'-infinity'); CREATE TABLE boring_cdc_control.capture_fences(id smallint PRIMARY KEY CHECK(id=1),capture_epoch bigint NOT NULL,generation bigint NOT NULL,table_set_fingerprint bytea NOT NULL CHECK(octet_length(table_set_fingerprint)=32),unique_nonce bytea NOT NULL CHECK(octet_length(unique_nonce)=16)); INSERT INTO boring_cdc_control.capture_fences VALUES(1,0,0,decode(repeat('00',32),'hex'),decode(repeat('00',16),'hex')); CREATE PUBLICATION boring_cdc_m3_pub FOR TABLE public.m3_bootstrap_fixture,boring_cdc_control.heartbeat,boring_cdc_control.capture_fences").unwrap();
        let path =
            std::env::temp_dir().join(format!("m3-live-{}-{}.sqlite", std::process::id(), slot));
        let writer = open_writer(&path, "m3-live", 1, 0).unwrap();
        let (system_id, timeline_id, database_id, publication_fp, protocol_fp) =
            live_identity(&dsn, "boring_cdc_m3_pub", "public.m3_bootstrap_fixture");
        writer.connection().execute("INSERT INTO source_state(singleton,capture_epoch,source_system_id,timeline_id,database_id,slot_name,plugin,publication_fingerprint,protocol_fingerprint) VALUES(1,'1',?1,?2,?3,?4,'pgoutput',?5,?6)",rusqlite::params![system_id,timeline_id,database_id,slot,publication_fp,protocol_fp]).unwrap();
        writer.connection().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('snapshot-probe','archive','live-config','1',1)",[]).unwrap();
        let relations = vec!["public.m3_bootstrap_fixture".to_string()];
        let assignment_digest = digest_assignment(&[0; 8]);
        let intent = PrepareIntent {
            intent_id: "live-intent".into(),
            capture_epoch: "1".into(),
            source_system_id: system_id.clone(),
            database_id: database_id.clone(),
            slot_name: slot.clone(),
            generation: 1,
            table_set_fingerprint: "aa".repeat(32),
            configuration_fingerprint: "live-config".into(),
            importers: vec![ImporterAssignment {
                importer_id: "worker-0".into(),
                assigned_ranges_digest: assignment_digest.clone(),
            }],
            created_at: "unix-ms:0".into(),
        };
        let runtime = BootstrapRuntime::start(
            writer,
            intent,
            &dsn,
            &relations,
            "boring_cdc_m3_pub",
            SessionBounds {
                statement_timeout_ms: 10_000,
                idle_timeout_ms: 30_000,
            },
            0,
        )
        .unwrap();
        let (guard, exporter, capture_pid, importer): (i32, i32, i32, i32) = runtime
            .store
            .writer
            .connection()
            .query_row(
                "SELECT r.guard_backend_pid,r.exporter_backend_pid,r.capture_backend_pid,i.backend_pid FROM m3_bootstrap_runtime r JOIN m3_bootstrap_importers i USING(intent_id) WHERE r.intent_id='live-intent'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        let mut pids = vec![guard, exporter, capture_pid, importer];
        pids.sort_unstable();
        pids.dedup();
        assert_eq!(pids.len(), 4);
        assert_eq!(
            runtime.store.status("live-intent").unwrap().state,
            "exporter_released"
        );
        let snapshot_schema_fingerprint: String = runtime.store.writer.connection().query_row(
            "SELECT snapshot_schema_fingerprint FROM m3_bootstrap_importers WHERE intent_id='live-intent' AND importer_id='worker-0'",
            [], |row| row.get(0),
        ).unwrap();
        assert_eq!(snapshot_schema_fingerprint.len(), 64);
        let imported_proof: SnapshotSchemaProof = runtime.store.writer.connection().query_row(
            "SELECT capture_epoch,relation_id,namespace,relation_name,schema_fingerprint,canonical_schema,schema_checksum FROM m3_snapshot_schema_proofs WHERE intent_id='live-intent' AND importer_id='worker-0'",
            [], |row| Ok(SnapshotSchemaProof {
                capture_epoch: row.get(0)?, relation_id: row.get(1)?, namespace: row.get(2)?,
                relation_name: row.get(3)?, schema_fingerprint: row.get(4)?,
                canonical_schema: row.get(5)?, schema_checksum: row.get(6)?,
            }),
        ).unwrap();
        let canonical_source = crate::m1_source_identity::SourceIdentity {
            system_identifier: system_id.parse().unwrap(),
            timeline: timeline_id.parse().unwrap(),
            database_identity: database_id.parse().unwrap(),
            slot_name: slot.clone(),
            plugin: "pgoutput".into(),
            publication_fingerprint: crate::m1_source_identity::Fingerprint::from_lower_hex(
                &publication_fp,
            )
            .unwrap(),
            protocol_fingerprint: crate::m1_source_identity::supported_protocol_fingerprint(),
        };
        crate::m3_fence::verify_imported_schema_proof(
            &canonical_source,
            "1",
            &imported_proof,
            &snapshot_schema_fingerprint,
        )
        .unwrap();
        let mut altered_proof = imported_proof;
        altered_proof.schema_checksum = "wrong".into();
        assert!(matches!(
            crate::m3_fence::verify_imported_schema_proof(
                &canonical_source,
                "1",
                &altered_proof,
                &snapshot_schema_fingerprint,
            ),
            Err(crate::m3_fence::FenceError::Conflict(
                "M3_IMPORTED_SCHEMA_PROOF_MISMATCH"
            ))
        ));
        let (writer, mut sessions) = runtime.into_planner_handoff();
        assert!(sessions.capture_alive());
        sessions.keepalive().unwrap();
        let limits = crate::m3_planner::PlannerLimits {
            chunk_rows: 4,
            chunk_bytes: 16 * 1024,
            chunk_duration: std::time::Duration::from_secs(1),
            writer_hold: std::time::Duration::from_secs(1),
            concurrency: 1,
            source_impact_bytes: 128 * 1024,
            max_rows_per_second: 1000,
        };
        let (failure, writer) = match crate::m3_planner::PlannerStore::open_recoverable(
            writer,
            crate::m3_planner::PlannerLimits {
                chunk_rows: 0,
                ..limits
            },
        ) {
            Ok(_) => panic!("invalid planner bound must return the bootstrap writer"),
            Err(result) => result,
        };
        assert!(matches!(
            failure,
            crate::m3_planner::PlannerError::Invalid("M3_ZERO_BOUND")
        ));
        assert!(sessions.capture_alive());
        let mut planner = match crate::m3_planner::PlannerStore::open_recoverable(*writer, limits) {
            Ok(planner) => planner,
            Err((error, _)) => panic!("planner handoff failed: {error}"),
        };
        assert_eq!(
            planner
                .persist_plan(&crate::m3_planner::PlanInput {
                    run_id: "snapshot-run".into(),
                    generation_id: "snapshot-generation".into(),
                    destination_id: "snapshot-probe".into(),
                    capture_epoch: "1".into(),
                    generation: 1,
                    bootstrap_intent_id: "live-intent".into(),
                    importer_id: "worker-0".into(),
                    snapshot_schema_fingerprint: snapshot_schema_fingerprint.clone(),
                    key_schema: vec![crate::m3_planner::KeyPartType::I64],
                    boundaries: vec![],
                    estimated_rows: 1,
                    start_seq: 0,
                    started_mono_ms: 100,
                })
                .unwrap(),
            1
        );
        let imported = sessions.take_importer("worker-0").unwrap();
        let mut source = crate::m3_planner::PostgresRangeSource::from_imported(
            imported,
            "worker-0",
            &assignment_digest,
            &snapshot_schema_fingerprint,
            "public.m3_bootstrap_fixture",
            &["id"],
        )
        .unwrap();
        let claim = planner
            .claim_next(
                "snapshot-generation",
                "worker-0",
                101,
                std::time::Duration::from_secs(5),
            )
            .unwrap()
            .unwrap();
        assert_eq!(planner.execute_claim(&claim, &mut source, 102).unwrap(), 1);
        planner.finish_copy("snapshot-generation").unwrap();
        assert!(sessions.capture_alive());
        sessions.keepalive().unwrap();
        drop(source);
        let writer = planner.into_writer();
        let (copied_rows, generation_state): (i64, String) = writer.connection().query_row(
            "SELECT c.row_count,g.state FROM m3_chunk_commits c JOIN backfill_chunks b USING(chunk_id) JOIN backfill_generations g USING(generation_id) WHERE g.generation_id='snapshot-generation'",
            [],
            |row| Ok((row.get(0)?,row.get(1)?)),
        ).unwrap();
        assert_eq!((copied_rows, generation_state.as_str()), (1, "fencing"));
        let (snapshot_seq, key, payload, payload_hash): (i64, Vec<u8>, Vec<u8>, String) = writer
            .connection()
            .query_row(
                "SELECT snapshot_seq,canonical_key,payload,payload_hash FROM m3_snapshot_events WHERE generation_id='snapshot-generation'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(snapshot_seq, 1);
        assert_eq!(
            key,
            crate::m3_planner::CanonicalKey::encode(
                &[crate::m3_planner::KeyPartType::I64],
                &[crate::m3_planner::KeyPart::I64(1)],
            )
            .unwrap()
            .as_bytes(),
        );
        let copied: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(copied["id"], 1);
        assert_eq!(copied["payload"], "before");
        assert_eq!(payload_hash, format!("{:x}", Sha256::digest(&payload)));
        let mut fence = crate::m3_fence::FenceStore::open(writer).unwrap();
        let prepared = fence
            .prepare_after_copy(&crate::m3_fence::FenceIntentInput {
                intent_id: "snapshot-fence".into(),
                generation_id: "snapshot-generation".into(),
                bootstrap_intent_id: "live-intent".into(),
                capture_epoch: 1,
                generation: 1,
                nonce: [0x17; 16],
                table_set_fingerprint: [0xaa; 32],
                anchor_id: "snapshot-anchor".into(),
                expires_at: "future".into(),
            })
            .unwrap();
        assert_eq!(fence.load_dispatch("snapshot-fence").unwrap(), prepared);
        let mut control = pg_walstream::PgReplicationConnection::connect(&dsn).unwrap();
        fence
            .dispatch_source("snapshot-fence", &mut control)
            .unwrap();
        let result = control
            .exec("SELECT capture_epoch::text,generation::text,encode(table_set_fingerprint,'hex'),encode(unique_nonce,'hex') FROM boring_cdc_control.capture_fences WHERE id=1")
            .unwrap();
        assert_eq!(result.ntuples(), 1);
        assert_eq!(result.get_value(0, 0).as_deref(), Some("1"));
        assert_eq!(result.get_value(0, 1).as_deref(), Some("1"));
        assert_eq!(
            result.get_value(0, 2).as_deref(),
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
        assert_eq!(
            result.get_value(0, 3).as_deref(),
            Some("17171717171717171717171717171717")
        );
        control
            .exec("DELETE FROM boring_cdc_control.capture_fences WHERE id=1")
            .unwrap();
        assert!(matches!(
            fence.dispatch_source("snapshot-fence", &mut control),
            Err(crate::m3_fence::FenceError::Conflict(
                "M3_FENCE_CONTROL_CARDINALITY"
            ))
        ));
        assert!(sessions.capture_alive());
        sessions.keepalive().unwrap();
        let writer = fence.into_writer();
        let dispatch_state: String = writer
            .connection()
            .query_row(
                "SELECT state FROM m3_fence_intents WHERE intent_id='snapshot-fence'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(dispatch_state, "dispatched");
        use crate::m2_capture_runtime::{CaptureRuntime, RuntimeError, RuntimeSpool};
        use crate::m2_journal::{CommitLimits, JournalStore, SourceIdentity};
        use crate::m2_spool::{
            DiskAdmission, FilesystemAdmissionController, FilesystemLimit, MemoryBudget,
            MemoryLimits, PosixAllocation, SpoolLimits, StatvfsSpace, TxnBuffer,
        };
        use std::os::unix::fs::MetadataExt;
        let spool_path = path.with_extension("spool");
        std::fs::create_dir_all(&spool_path).unwrap();
        let spool_cleanup = spool_path.clone();
        let disk = FilesystemAdmissionController::new(DiskAdmission::default());
        disk.configure(
            std::fs::metadata(&spool_path).unwrap().dev(),
            FilesystemLimit {
                total_budget: 32 * 1024 * 1024,
                emergency_reserve: 1024 * 1024,
            },
        )
        .unwrap();
        let spools = move |xid: u32| -> Result<Box<dyn RuntimeSpool>, RuntimeError> {
            let memory = MemoryBudget::new(MemoryLimits {
                process_limit: 24 * 1024 * 1024,
                runtime_fixed: 1,
                receive: 8 * 1024 * 1024,
                decoder: 4 * 1024 * 1024,
                staging: 8 * 1024 * 1024,
            })
            .map_err(|_| RuntimeError::Spool("M3_TEST_MEMORY_ADMISSION".into()))?;
            TxnBuffer::new(
                spool_path.clone(),
                "1".into(),
                xid.to_string(),
                "m3-live-capture".into(),
                SpoolLimits {
                    max_frame_bytes: 8 * 1024 * 1024,
                    max_event_bytes: 4 * 1024 * 1024,
                    max_transaction_bytes: 8 * 1024 * 1024,
                    max_transaction_events: 16,
                    memory_prefix_bytes: 4 * 1024 * 1024,
                },
                memory,
                disk.clone(),
                Box::new(StatvfsSpace),
                Box::new(PosixAllocation),
            )
            .map(|spool| Box::new(spool) as Box<dyn RuntimeSpool>)
            .map_err(|_| RuntimeError::Spool("M3_TEST_SPOOL_ADMISSION".into()))
        };
        let gate_reader = rusqlite::Connection::open_with_flags(
            &path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let journal = JournalStore::new(
            writer,
            SourceIdentity {
                capture_epoch: "1".into(),
                source_system_id: system_id.clone(),
                timeline_id: timeline_id.clone(),
                database_id: database_id.clone(),
                slot_name: slot.clone(),
                publication_fingerprint: publication_fp.clone(),
                protocol_fingerprint: protocol_fp.clone(),
            },
            CommitLimits {
                max_events: 16,
                max_copied_bytes: 1024 * 1024,
                max_writer_hold: std::time::Duration::from_secs(2),
            },
        )
        .unwrap();
        let mut capture = CaptureRuntime::new(
            journal,
            spools,
            crate::m3_fence::PersistedFeedbackGate::new(&gate_reader, "live-intent", 1),
            std::collections::BTreeMap::new(),
            None,
        );
        let config = crate::article1_capture::CaptureConfig::production(
            dsn.clone(),
            "boring_cdc_m3_pub",
            slot.clone(),
            ["public.m3_bootstrap_fixture".to_string()],
        )
        .unwrap();
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        executor
            .block_on(sessions.capture_until(
                &config,
                &pg_walstream::CancellationToken::new(),
                &mut capture,
                1,
                5_000,
                |_| Ok(true),
            ))
            .unwrap();
        assert_eq!(capture.committed_transactions(), 1);
        let slot_owner = admin
            .exec(&format!(
                "SELECT active_pid::text FROM pg_replication_slots WHERE slot_name='{slot}'"
            ))
            .unwrap();
        assert_eq!(
            slot_owner.get_value(0, 0).unwrap().parse::<i32>().unwrap(),
            capture_pid
        );
        let durable_end_lsn = capture.durable_end_lsn().unwrap();
        let writer = capture.into_journal().unwrap().into_writer();
        let (transaction_id, end_lsn, journal_seq, control_kind, payload, source_lsn):
            (String, String, i64, String, Vec<u8>, String) = writer
            .connection()
            .query_row(
                "SELECT t.transaction_id,t.end_lsn,e.journal_seq,e.control_kind,e.payload,s.durable_transaction_end_lsn FROM source_transactions t JOIN journal_events e ON e.transaction_id=t.transaction_id AND e.journal_seq=t.last_seq CROSS JOIN source_state s WHERE e.control_kind='capture_fence'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
            )
            .unwrap();
        assert_eq!(u64::from_str_radix(&end_lsn, 16).unwrap(), durable_end_lsn);
        assert_eq!(source_lsn, end_lsn);
        assert_eq!(journal_seq, 1);
        assert_eq!(control_kind, "capture_fence");
        let captured: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(captured["kind"], "update");
        assert_eq!(
            captured["new"][4]["bytes"],
            serde_json::json!(b"\\x17171717171717171717171717171717".as_slice())
        );
        let mut fence = crate::m3_fence::FenceStore::open(writer).unwrap();
        let proof = fence
            .observe_durable("snapshot-fence", &transaction_id)
            .unwrap();
        assert_eq!(proof.post_copy_fence_lsn, end_lsn);
        assert_eq!(proof.post_copy_fence_seq, journal_seq as u64);
        assert!(proof.first_proof);
        let repeated = fence
            .observe_durable("snapshot-fence", &transaction_id)
            .unwrap();
        assert!(!repeated.first_proof);
        let writer = fence.into_writer();
        let anchor: (String,String,String) = writer.connection().query_row(
            "SELECT a.state,a.snapshot_schema_fingerprints,r.schema_fingerprint FROM bootstrap_anchors a JOIN relation_schemas r ON r.capture_epoch=a.capture_epoch WHERE a.anchor_id='snapshot-anchor'",
            [], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
        ).unwrap();
        assert_eq!(anchor.0, "complete");
        assert_eq!(anchor.1, format!("[\"{snapshot_schema_fingerprint}\"]"));
        assert_eq!(anchor.2, snapshot_schema_fingerprint);
        assert!(writer.connection().execute(
            "UPDATE m3_snapshot_schema_proofs SET schema_checksum='wrong' WHERE intent_id='live-intent'",
            [],
        ).is_err());
        drop(sessions);
        drop(writer);
        let mut cleanup = pg_walstream::PgReplicationConnection::connect(&dsn).unwrap();
        cleanup
            .exec(&format!("SELECT pg_drop_replication_slot('{slot}')"))
            .unwrap();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
        let _ = std::fs::remove_dir_all(spool_cleanup);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m2_capture_runtime::{FeedbackGate, FeedbackPermit};
    use crate::m2_schema::open_writer;
    use crate::m3_fence::PersistedFeedbackGate;
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
    #[test]
    fn multi_relation_bootstrap_is_rejected_before_source_effects() {
        let (_path, store) = store();
        let result = BootstrapRuntime::start(
            store.into_writer(),
            intent(),
            "invalid dsn",
            &["public.first".into(), "public.second".into()],
            "publication",
            SessionBounds {
                statement_timeout_ms: 1_000,
                idle_timeout_ms: 2_000,
            },
            0,
        );
        assert!(matches!(
            result,
            Err(BootstrapError::Invalid(
                "M3_SINGLE_RELATION_PROFILE_REQUIRED"
            ))
        ));
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
        let mut gate = PersistedFeedbackGate::new(s.writer.connection(), "intent", 1);
        assert_eq!(
            gate.permit(Some(32)),
            FeedbackPermit::AllowSafeBoundary { lsn: 32 }
        );
    }
    #[test]
    fn pending_imports_hold_feedback() {
        let (_p, mut s) = store();
        exported(&mut s);
        let mut gate = PersistedFeedbackGate::new(s.writer.connection(), "intent", 1);
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
        {
            let mut gate = PersistedFeedbackGate::new(s.writer.connection(), "intent", 1);
            assert_eq!(
                gate.permit(Some(32)),
                FeedbackPermit::AllowSafeBoundary { lsn: 32 }
            );
        }
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
