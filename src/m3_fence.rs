//! Transactional capture-fence, importer feedback gate, and immutable anchor proof.
//!
//! The only accepted post-copy proof is a published control-row update that M2 has already
//! committed as a `capture_fence` journal event.  The source transaction end LSN and its journal
//! sequence are read together from that durable transaction; clocks, sampled LSNs, and `wal_end`
//! are deliberately absent from this API.

use crate::m2_capture_runtime::{FeedbackGate, FeedbackPermit};
use crate::m2_schema::WriterConnection;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;

pub const OWNER_BEAD: &str = "boring-cdc-m3-fence";
const INSTALL_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS m3_feedback_gates(
 intent_id TEXT PRIMARY KEY REFERENCES m3_bootstrap_runtime(intent_id) ON DELETE CASCADE,
 generation INTEGER NOT NULL CHECK(generation>0),
 state TEXT NOT NULL CHECK(state IN ('hold','open','invalidated')),
 opened_durable_lsn TEXT,
 revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0));
CREATE TABLE IF NOT EXISTS m3_fence_intents(
 intent_id TEXT PRIMARY KEY, generation_id TEXT NOT NULL UNIQUE REFERENCES backfill_generations(generation_id),
 bootstrap_intent_id TEXT NOT NULL REFERENCES m3_bootstrap_runtime(intent_id), capture_epoch TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK(generation>0), nonce TEXT NOT NULL, table_set_fingerprint TEXT NOT NULL,
 anchor_id TEXT NOT NULL UNIQUE, expires_at TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('intended','dispatched','complete','invalidated')),
 revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0), UNIQUE(capture_epoch,generation,nonce));
CREATE TABLE IF NOT EXISTS m3_fence_observations(
 observation_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL REFERENCES m3_fence_intents(intent_id),
 transaction_id TEXT NOT NULL REFERENCES source_transactions(transaction_id), end_lsn TEXT NOT NULL,
 journal_seq INTEGER NOT NULL, first_proof INTEGER NOT NULL CHECK(first_proof IN(0,1)),
 UNIQUE(intent_id,transaction_id));
CREATE UNIQUE INDEX IF NOT EXISTS one_m3_fence_first_observation ON m3_fence_observations(intent_id) WHERE first_proof=1;
CREATE TRIGGER IF NOT EXISTS m3_feedback_gate_revision BEFORE UPDATE ON m3_feedback_gates
 WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale m3 feedback gate revision'); END;
CREATE TRIGGER IF NOT EXISTS m3_fence_intent_revision BEFORE UPDATE ON m3_fence_intents
 WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale m3 fence intent revision'); END;
"#;

#[derive(Debug)]
pub enum FenceError {
    Invalid(&'static str),
    Conflict(&'static str),
    Sqlite(rusqlite::Error),
}
impl fmt::Display for FenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(v) | Self::Conflict(v) => f.write_str(v),
            Self::Sqlite(_) => f.write_str("M3_FENCE_STORE_FAILED"),
        }
    }
}
impl std::error::Error for FenceError {}
impl From<rusqlite::Error> for FenceError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Sqlite(value)
    }
}

#[derive(Clone, Debug)]
pub struct FenceIntentInput {
    pub intent_id: String,
    pub generation_id: String,
    pub bootstrap_intent_id: String,
    pub capture_epoch: i64,
    pub generation: i64,
    pub nonce: [u8; 16],
    pub table_set_fingerprint: [u8; 32],
    pub anchor_id: String,
    pub expires_at: String,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FenceDispatch {
    pub sql: String,
    pub capture_epoch: i64,
    pub generation: i64,
    pub table_set_fingerprint: [u8; 32],
    /// Kept out of logs and status; the dispatcher binds it as a query parameter.
    pub nonce: [u8; 16],
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AnchorProof {
    pub anchor_id: String,
    pub transaction_id: String,
    pub post_copy_fence_lsn: String,
    pub post_copy_fence_seq: u64,
    pub first_proof: bool,
}
#[derive(Deserialize)]
struct CapturedFenceRow {
    kind: String,
    new: Option<Vec<CapturedTuple>>,
}
#[derive(Deserialize)]
#[serde(tag = "state", content = "bytes", rename_all = "snake_case")]
enum CapturedTuple {
    Null,
    UnchangedToast,
    Text(Vec<u8>),
}

pub fn install(connection: &Connection) -> Result<(), FenceError> {
    connection.execute_batch(INSTALL_SQL)?;
    Ok(())
}

/// Fence-owned persisted gate registration, invoked by bootstrap in its prepare transaction.
pub(crate) fn register_feedback_gate(
    tx: &rusqlite::Transaction<'_>,
    intent_id: &str,
    generation: u64,
) -> Result<(), FenceError> {
    tx.execute(
        "INSERT INTO m3_feedback_gates(intent_id,generation,state) VALUES(?1,?2,'hold')",
        params![intent_id, generation],
    )?;
    Ok(())
}

/// Acknowledges one importer and opens the reusable gate only when every importer is durable.
/// Retry of the same acknowledgement is idempotent; a stale generation is rejected.
pub(crate) fn acknowledge_and_open(
    connection: &mut Connection,
    intent_id: &str,
    importer_id: &str,
    generation: u64,
) -> Result<(), FenceError> {
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let gate: Option<(i64, String)> = tx
        .query_row(
            "SELECT generation,state FROM m3_feedback_gates WHERE intent_id=?1",
            [intent_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let Some((persisted_generation, state)) = gate else {
        return Err(FenceError::Conflict("M3_FEEDBACK_GATE_MISSING"));
    };
    if persisted_generation != generation as i64 || state == "invalidated" {
        return Err(FenceError::Conflict("M3_FEEDBACK_GATE_STALE_GENERATION"));
    }
    let importer: Option<String> = tx
        .query_row(
            "SELECT state FROM m3_bootstrap_importers WHERE intent_id=?1 AND importer_id=?2",
            params![intent_id, importer_id],
            |r| r.get(0),
        )
        .optional()?;
    match importer.as_deref() {
        Some("contract_bound") => {
            tx.execute("UPDATE m3_bootstrap_importers SET state='acknowledged',revision=revision+1 WHERE intent_id=?1 AND importer_id=?2 AND state='contract_bound'", params![intent_id, importer_id])?;
        }
        Some("acknowledged") => {}
        _ => return Err(FenceError::Conflict("M3_IMPORTER_COMPLETION_STALE")),
    }
    let pending: i64 = tx.query_row(
        "SELECT count(*) FROM m3_bootstrap_importers WHERE intent_id=?1 AND state!='acknowledged'",
        [intent_id],
        |r| r.get(0),
    )?;
    if pending == 0 && state == "hold" {
        tx.execute("UPDATE m3_feedback_gates SET state='open',revision=revision+1 WHERE intent_id=?1 AND generation=?2 AND state='hold'", params![intent_id, generation])?;
        let changed=tx.execute("UPDATE m3_bootstrap_runtime SET state='exporter_release_permitted',feedback_gate_open=1,revision=revision+1 WHERE intent_id=?1 AND generation=?2 AND state='imports_pending' AND exporter_liveness='command_idle' AND guard_liveness='held'",params![intent_id,generation])?;
        if changed != 1 {
            return Err(FenceError::Conflict("M3_IMPORT_ACK_GATE_STALE"));
        }
    }
    tx.commit()?;
    Ok(())
}

/// Invalidates snapshot eligibility, importer states, and releases WAL feedback atomically.
pub(crate) fn reconcile_and_release(
    connection: &mut Connection,
    intent_id: &str,
    target: &str,
) -> Result<(), FenceError> {
    let assignment = match target {
        "bootstrap_ambiguous_requires_restart" => {
            "state='bootstrap_ambiguous_requires_restart',exporter_liveness='lost',snapshot_promotable=0,feedback_gate_open=1"
        }
        "existing_slot_generation_required" => {
            "state='existing_slot_generation_required',snapshot_promotable=0,feedback_gate_open=1"
        }
        "full_reseed_required" => {
            "state='full_reseed_required',snapshot_promotable=0,feedback_gate_open=1"
        }
        _ => return Err(FenceError::Invalid("M3_RECONCILE_TARGET_INVALID")),
    };
    let source_states = match target {
        "bootstrap_ambiguous_requires_restart" => "state='prepared'",
        _ => "state IN ('bootstrap_ambiguous_requires_restart','snapshot_unusable')",
    };
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let changed=tx.execute(&format!("UPDATE m3_bootstrap_runtime SET {assignment},revision=revision+1 WHERE intent_id=?1 AND {source_states}"),[intent_id])?;
    if changed != 1 {
        return Err(FenceError::Conflict("M3_RECONCILE_GATE_STALE"));
    }
    let gate_state: String = tx.query_row(
        "SELECT state FROM m3_feedback_gates WHERE intent_id=?1",
        [intent_id],
        |r| r.get(0),
    )?;
    if gate_state != "invalidated" {
        let gate=tx.execute("UPDATE m3_feedback_gates SET state='invalidated',revision=revision+1 WHERE intent_id=?1 AND state IN ('hold','open')",[intent_id])?;
        if gate != 1 {
            return Err(FenceError::Conflict("M3_RECONCILE_GATE_STALE"));
        }
    }
    tx.commit()?;
    Ok(())
}

pub(crate) fn invalidate_and_release(
    connection: &mut Connection,
    intent_id: &str,
    generation: u64,
    lost_session: &str,
) -> Result<(), FenceError> {
    if !matches!(lost_session, "exporter" | "importer" | "guard") {
        return Err(FenceError::Invalid("M3_LOST_SESSION_INVALID"));
    }
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let planner_installed: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='m3_planner_runs')",
        [],
        |r| r.get(0),
    )?;
    if planner_installed {
        tx.execute("UPDATE backfill_chunks SET state='invalidated' WHERE generation_id IN (SELECT generation_id FROM m3_planner_runs WHERE bootstrap_intent_id=?1) AND state='pending'", [intent_id])?;
        tx.execute("DELETE FROM m3_chunk_claims WHERE generation_id IN (SELECT generation_id FROM m3_planner_runs WHERE bootstrap_intent_id=?1)", [intent_id])?;
        tx.execute("UPDATE backfill_generations SET state='invalidated' WHERE generation_id IN (SELECT generation_id FROM m3_planner_runs WHERE bootstrap_intent_id=?1) AND state IN ('copying','fencing')", [intent_id])?;
    }
    tx.execute("UPDATE m3_fence_intents SET state='invalidated',revision=revision+1 WHERE bootstrap_intent_id=?1 AND state IN ('intended','dispatched')", [intent_id])?;
    let (liveness, guard) = (
        if lost_session == "exporter" {
            "exporter_liveness='lost',"
        } else {
            ""
        },
        if lost_session == "guard" {
            "guard_liveness='lost',"
        } else {
            ""
        },
    );
    let changed=tx.execute(&format!("UPDATE m3_bootstrap_runtime SET {liveness}{guard} state='snapshot_unusable',snapshot_promotable=0,feedback_gate_open=1,revision=revision+1 WHERE intent_id=?1 AND generation=?2 AND state IN ('snapshot_exported','imports_pending','exporter_release_permitted','exporter_released')"),params![intent_id,generation])?;
    if changed != 1 {
        return Err(FenceError::Conflict("M3_INVALIDATION_STALE"));
    }
    tx.execute("UPDATE m3_bootstrap_importers SET state='invalidated',revision=revision+1 WHERE intent_id=?1 AND state NOT IN ('reads_complete','invalidated')",[intent_id])?;
    let gate=tx.execute("UPDATE m3_feedback_gates SET state='invalidated',revision=revision+1 WHERE intent_id=?1 AND generation=?2 AND state IN ('hold','open')",params![intent_id,generation])?;
    if gate != 1 {
        return Err(FenceError::Conflict("M3_FEEDBACK_GATE_STALE_GENERATION"));
    }
    tx.commit()?;
    Ok(())
}

/// Sole production `FeedbackPermit` provider for a snapshot/importer generation.
pub struct PersistedFeedbackGate<'a> {
    connection: &'a Connection,
    intent_id: &'a str,
    generation: u64,
}
impl<'a> PersistedFeedbackGate<'a> {
    pub fn new(connection: &'a Connection, intent_id: &'a str, generation: u64) -> Self {
        Self {
            connection,
            intent_id,
            generation,
        }
    }
}
impl FeedbackGate for PersistedFeedbackGate<'_> {
    fn permit(&mut self, durable_end_lsn: Option<u64>) -> FeedbackPermit {
        let row = self
            .connection
            .query_row(
                "SELECT state FROM m3_feedback_gates WHERE intent_id=?1 AND generation=?2",
                params![self.intent_id, self.generation],
                |r| r.get::<_, String>(0),
            )
            .optional();
        match row {
            Ok(Some(state)) if state == "open" || state == "invalidated" => {
                FeedbackPermit::AllowSafeBoundary {
                    lsn: durable_end_lsn.unwrap_or(0),
                }
            }
            Ok(Some(state)) if state == "hold" => FeedbackPermit::Hold,
            _ => FeedbackPermit::Stale,
        }
    }
}

pub struct FenceStore {
    writer: WriterConnection,
}
impl FenceStore {
    pub fn open(writer: WriterConnection) -> Result<Self, FenceError> {
        install(writer.connection())?;
        Ok(Self { writer })
    }

    /// Persists the unique nonce before returning the one-row source UPDATE dispatch.
    pub fn prepare_after_copy(
        &mut self,
        input: &FenceIntentInput,
    ) -> Result<FenceDispatch, FenceError> {
        validate_input(input)?;
        let tx = self
            .writer
            .connection_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let proof:Option<(String,i64,String,String,i64,i64,String)>=tx.query_row(
            "SELECT g.state,x.generation,b.capture_epoch,x.table_set_fingerprint,(SELECT count(*) FROM backfill_chunks c WHERE c.generation_id=g.generation_id AND c.state!='complete'),(SELECT count(*) FROM m3_bootstrap_importers i WHERE i.intent_id=x.intent_id AND i.state!='acknowledged'),f.state FROM backfill_generations g JOIN backfill_runs b ON b.run_id=g.run_id JOIN m3_bootstrap_runtime x ON x.intent_id=?2 JOIN m3_planner_runs p ON p.generation_id=g.generation_id AND p.bootstrap_intent_id=x.intent_id JOIN m3_feedback_gates f ON f.intent_id=x.intent_id AND f.generation=x.generation WHERE g.generation_id=?1",
            params![input.generation_id,input.bootstrap_intent_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?))).optional()?;
        let Some((state, generation, epoch, tables, incomplete, pending_imports, gate)) = proof
        else {
            return Err(FenceError::Conflict("M3_FENCE_PREREQUISITE_MISSING"));
        };
        if state != "fencing"
            || generation != input.generation
            || epoch != input.capture_epoch.to_string()
            || tables != hex(&input.table_set_fingerprint)
            || incomplete != 0
            || pending_imports != 0
            || gate != "open"
        {
            return Err(FenceError::Conflict("M3_FENCE_PREREQUISITE_MISMATCH"));
        }
        let nonce = hex(&input.nonce);
        let table_set_fingerprint = hex(&input.table_set_fingerprint);
        let changed=tx.execute("UPDATE backfill_generations SET intended_fence_nonce=?2 WHERE generation_id=?1 AND intended_fence_nonce IS NULL AND state='fencing'",params![input.generation_id,nonce])?;
        if changed != 1 {
            return Err(FenceError::Conflict("M3_FENCE_NONCE_ALREADY_INTENDED"));
        }
        tx.execute("INSERT INTO m3_fence_intents(intent_id,generation_id,bootstrap_intent_id,capture_epoch,generation,nonce,table_set_fingerprint,anchor_id,expires_at,state) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,'intended')",params![input.intent_id,input.generation_id,input.bootstrap_intent_id,input.capture_epoch,input.generation,nonce,table_set_fingerprint,input.anchor_id,input.expires_at])?;
        tx.commit()?;
        Ok(dispatch(
            input.capture_epoch,
            input.generation,
            input.table_set_fingerprint,
            input.nonce,
        ))
    }

    /// Reloads the immutable dispatch after a crash before or after the source UPDATE.
    pub fn load_dispatch(&self, intent_id: &str) -> Result<FenceDispatch, FenceError> {
        let row:Option<(String,i64,String,String)>=self.writer.connection().query_row(
            "SELECT capture_epoch,generation,table_set_fingerprint,nonce FROM m3_fence_intents WHERE intent_id=?1 AND state IN ('intended','dispatched')",
            [intent_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)),
        ).optional()?;
        let Some((epoch, generation, tables, nonce)) = row else {
            return Err(FenceError::Conflict("M3_FENCE_DISPATCH_STALE"));
        };
        let epoch = epoch
            .parse::<i64>()
            .map_err(|_| FenceError::Conflict("M3_FENCE_STORED_IDENTITY_INVALID"))?;
        Ok(dispatch(
            epoch,
            generation,
            decode_fixed(&tables)?,
            decode_fixed(&nonce)?,
        ))
    }

    /// Records that the runtime credential's fixed-row UPDATE affected exactly one row.
    pub fn mark_dispatched(
        &mut self,
        intent_id: &str,
        affected_rows: u64,
    ) -> Result<(), FenceError> {
        if affected_rows != 1 {
            return Err(FenceError::Conflict("M3_FENCE_CONTROL_CARDINALITY"));
        }
        let state: String = self.writer.connection().query_row(
            "SELECT state FROM m3_fence_intents WHERE intent_id=?1",
            [intent_id],
            |r| r.get(0),
        )?;
        if state == "dispatched" {
            return Ok(());
        }
        let changed=self.writer.connection().execute("UPDATE m3_fence_intents SET state='dispatched',revision=revision+1 WHERE intent_id=?1 AND state='intended'",[intent_id])?;
        if changed != 1 {
            return Err(FenceError::Conflict("M3_FENCE_DISPATCH_STALE"));
        }
        Ok(())
    }

    /// Atomically pairs the exact durable transaction LSN/sequence with the first proof and anchor.
    /// Later matching observations are retained with `first_proof=false` and never rewrite it.
    pub fn observe_durable(
        &mut self,
        intent_id: &str,
        transaction_id: &str,
    ) -> Result<AnchorProof, FenceError> {
        let tx = self
            .writer
            .connection_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        type Intent = (
            String,
            String,
            String,
            i64,
            String,
            String,
            String,
            String,
            String,
        );
        let intent:Intent=tx.query_row("SELECT generation_id,bootstrap_intent_id,capture_epoch,generation,nonce,table_set_fingerprint,anchor_id,expires_at,state FROM m3_fence_intents WHERE intent_id=?1",[intent_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?)))?;
        if !matches!(intent.8.as_str(), "intended" | "dispatched" | "complete") {
            return Err(FenceError::Conflict("M3_FENCE_OBSERVATION_STALE"));
        }
        let durable:Option<(String,i64,Vec<u8>)>=tx.query_row("SELECT t.end_lsn,t.last_seq,e.payload FROM source_transactions t JOIN journal_events e ON e.transaction_id=t.transaction_id AND e.journal_seq=t.last_seq WHERE t.transaction_id=?1 AND t.capture_epoch=?2 AND t.state='committed' AND t.event_count=1 AND t.first_seq=t.last_seq AND e.control_kind='capture_fence'",params![transaction_id,intent.2],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        let Some((lsn, seq, payload)) = durable else {
            return Err(FenceError::Conflict("M3_FENCE_DURABLE_PAIR_MISSING"));
        };
        let decoded: CapturedFenceRow = serde_json::from_slice(&payload)
            .map_err(|_| FenceError::Conflict("M3_FENCE_PAYLOAD_INVALID"))?;
        let values = decoded
            .new
            .ok_or(FenceError::Conflict("M3_FENCE_PAYLOAD_INVALID"))?;
        if decoded.kind != "update" || values.len() != 5 {
            return Err(FenceError::Conflict("M3_FENCE_PAYLOAD_INVALID"));
        }
        let text = values
            .into_iter()
            .map(|value| match value {
                CapturedTuple::Text(bytes) => String::from_utf8(bytes)
                    .map_err(|_| FenceError::Conflict("M3_FENCE_PAYLOAD_INVALID")),
                CapturedTuple::Null | CapturedTuple::UnchangedToast => {
                    Err(FenceError::Conflict("M3_FENCE_PAYLOAD_INVALID"))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let observed_epoch = text[1]
            .parse::<u64>()
            .map_err(|_| FenceError::Conflict("M3_FENCE_PAYLOAD_INVALID"))?;
        let observed_generation = text[2]
            .parse::<u64>()
            .map_err(|_| FenceError::Conflict("M3_FENCE_PAYLOAD_INVALID"))?;
        let observed_tables = text[3]
            .strip_prefix("\\x")
            .ok_or(FenceError::Conflict("M3_FENCE_PAYLOAD_INVALID"))?;
        let observed_nonce = text[4]
            .strip_prefix("\\x")
            .ok_or(FenceError::Conflict("M3_FENCE_PAYLOAD_INVALID"))?;
        if text[0] != "1"
            || intent.2.parse::<u64>().ok() != Some(observed_epoch)
            || observed_generation != intent.3 as u64
            || observed_nonce != intent.4
            || observed_tables != intent.5
        {
            return Err(FenceError::Conflict("M3_FENCE_IDENTITY_MISMATCH"));
        }
        let existing:Option<(String,String,i64)>=tx.query_row("SELECT transaction_id,end_lsn,journal_seq FROM m3_fence_observations WHERE intent_id=?1 AND first_proof=1",[intent_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        let first = existing.is_none();
        let observation_id = format!("{}:{}", intent_id, transaction_id);
        tx.execute(
            "INSERT OR IGNORE INTO m3_fence_observations VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                observation_id,
                intent_id,
                transaction_id,
                lsn,
                seq,
                if first { 1 } else { 0 }
            ],
        )?;
        if first {
            tx.execute("INSERT INTO durable_capture_fences(fence_id,capture_epoch,generation,nonce,transaction_id,post_copy_fence_lsn,post_copy_fence_seq,first_proof) VALUES(?1,?2,?3,?4,?5,?6,?7,1)",params![format!("fence:{}",intent_id),intent.2,intent.3,intent.4,transaction_id,lsn,seq])?;
            let snapshot:(String,i64,String,String,i64)=tx.query_row("SELECT x.consistent_lsn,p.next_snapshot_seq,x.table_set_fingerprint,json_group_array(DISTINCT i.snapshot_schema_fingerprint),x.start_seq FROM m3_bootstrap_runtime x JOIN m3_planner_runs p ON p.bootstrap_intent_id=x.intent_id AND p.generation_id=?2 JOIN m3_bootstrap_importers i ON i.intent_id=x.intent_id WHERE x.intent_id=?1 GROUP BY x.consistent_lsn,p.next_snapshot_seq,x.table_set_fingerprint",params![intent.1,intent.0],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?;
            tx.execute("UPDATE backfill_generations SET state='complete' WHERE generation_id=?1 AND state='fencing'",[&intent.0])?;
            tx.execute("UPDATE backfill_runs SET state='complete',revision=revision+1 WHERE run_id=(SELECT run_id FROM backfill_generations WHERE generation_id=?1) AND state='running'",[&intent.0])?;
            tx.execute("INSERT INTO bootstrap_anchors(anchor_id,capture_epoch,generation,lower_stitch_lsn,start_seq,snapshot_boundary_lsn,snapshot_complete_seq,post_copy_fence_nonce,post_copy_fence_lsn,post_copy_fence_seq,table_set_fingerprint,snapshot_schema_fingerprints,state,expires_at,generation_id,bootstrap_intent_id) VALUES(?1,?2,?3,NULL,?14,?4,?5,?6,?7,?8,?9,?10,'complete',?11,?12,?13)",params![intent.6,intent.2,intent.3,snapshot.0,snapshot.1,intent.4,lsn,seq,snapshot.2,snapshot.3,intent.7,intent.0,intent.1,snapshot.4])?;
            tx.execute("UPDATE m3_fence_intents SET state='complete',revision=revision+1 WHERE intent_id=?1 AND state IN ('intended','dispatched')",[intent_id])?;
        } else if existing
            .as_ref()
            .is_some_and(|v| v.0 == transaction_id && v.1 == lsn && v.2 == seq)
        {
            // Idempotent retry of the first transaction: no second audit row or proof mutation.
        }
        tx.commit()?;
        let chosen = existing.unwrap_or((transaction_id.to_owned(), lsn.clone(), seq));
        Ok(AnchorProof {
            anchor_id: intent.6,
            transaction_id: chosen.0,
            post_copy_fence_lsn: chosen.1,
            post_copy_fence_seq: chosen.2 as u64,
            first_proof: first,
        })
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|v| format!("{v:02x}")).collect()
}
fn decode_fixed<const N: usize>(value: &str) -> Result<[u8; N], FenceError> {
    if value.len() != N * 2 {
        return Err(FenceError::Conflict("M3_FENCE_STORED_IDENTITY_INVALID"));
    }
    let mut out = [0u8; N];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let digit = |v: u8| match v {
            b'0'..=b'9' => Ok(v - b'0'),
            b'a'..=b'f' => Ok(v - b'a' + 10),
            _ => Err(FenceError::Conflict("M3_FENCE_STORED_IDENTITY_INVALID")),
        };
        out[index] = (digit(pair[0])? << 4) | digit(pair[1])?;
    }
    Ok(out)
}
fn dispatch(
    capture_epoch: i64,
    generation: i64,
    table_set_fingerprint: [u8; 32],
    nonce: [u8; 16],
) -> FenceDispatch {
    FenceDispatch{sql:"UPDATE boring_cdc_control.capture_fences SET capture_epoch=$1,generation=$2,table_set_fingerprint=$3,unique_nonce=$4 WHERE id=1".into(),capture_epoch,generation,table_set_fingerprint,nonce}
}
pub fn fence_payload(
    capture_epoch: i64,
    generation: i64,
    table_set_fingerprint: [u8; 32],
    nonce: [u8; 16],
) -> Vec<u8> {
    let text = |value: &str| serde_json::json!({"state":"text","bytes":value.as_bytes()});
    serde_json::to_vec(&serde_json::json!({
        "kind":"update","relation_id":42,"ordinal":0,"old_kind":"key",
        "old":[text("1")],
        "new":[text("1"),text(&capture_epoch.to_string()),text(&generation.to_string()),text(&format!("\\x{}",hex(&table_set_fingerprint))),text(&format!("\\x{}",hex(&nonce)))],
        "origin_lsn":null,"origin_name":null
    })).expect("typed M2 encoded fence row")
}
fn valid(v: &str) -> bool {
    !v.is_empty() && v.len() <= 256 && v.is_ascii()
}
fn validate_input(v: &FenceIntentInput) -> Result<(), FenceError> {
    if v.generation <= 0
        || v.nonce.iter().all(|v| *v == 0)
        || v.capture_epoch <= 0
        || v.table_set_fingerprint.iter().all(|v| *v == 0)
        || ![
            &v.intent_id,
            &v.generation_id,
            &v.bootstrap_intent_id,
            &v.anchor_id,
            &v.expires_at,
        ]
        .into_iter()
        .all(|x| valid(x))
    {
        return Err(FenceError::Invalid("M3_FENCE_INTENT_INVALID"));
    }
    Ok(())
}
pub fn nonce_hash(nonce: [u8; 16]) -> String {
    format!("{:x}", Sha256::digest(nonce))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m1_decoder::{
        ControlContract, CopyBothEvent, Decoder, PgoutputEvent, RelationContract, WireLimits,
    };
    use crate::m2_capture_runtime::{FeedbackGate, encode_row};
    use crate::m2_schema::open_writer;
    use crate::m3_bootstrap::{
        BootstrapStore, ExportResponse, ImporterAssignment, PrepareIntent, digest_assignment,
    };
    use crate::m3_planner::{
        BoundedRangeSource, KeyPartType, KeyRange, PlanInput, PlannerLimits, PlannerStore,
        ReadBudget, SnapshotRow,
    };
    use std::time::{SystemTime, UNIX_EPOCH};
    fn path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "m3-fence-{name}-{}-{}.db",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }
    fn writer(name: &str) -> WriterConnection {
        open_writer(&path(name), "test", 1, 1).unwrap()
    }
    fn setup(name: &str) -> FenceStore {
        let w = writer(name);
        w.connection().execute_batch("CREATE TABLE m3_bootstrap_runtime(intent_id TEXT PRIMARY KEY,generation INTEGER,start_seq INTEGER,consistent_lsn TEXT,table_set_fingerprint TEXT,state TEXT,exporter_liveness TEXT,guard_liveness TEXT,snapshot_promotable INTEGER,feedback_gate_open INTEGER,revision INTEGER); CREATE TABLE m3_bootstrap_importers(intent_id TEXT,importer_id TEXT,state TEXT,revision INTEGER,snapshot_schema_fingerprint TEXT,PRIMARY KEY(intent_id,importer_id)); CREATE TABLE m3_planner_runs(generation_id TEXT,bootstrap_intent_id TEXT,next_snapshot_seq INTEGER); CREATE TABLE m3_chunk_claims(generation_id TEXT);").unwrap();
        install(w.connection()).unwrap();
        w.connection().execute_batch("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('d','archive','cfg','1',1); INSERT INTO bootstrap_intents VALUES('boot','1','sys','db','slot','0000000000000001','slot_created',0,'now'); INSERT INTO m3_bootstrap_runtime VALUES('boot',1,0,'0000000000000001','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','exporter_released','released','held',1,1,0); INSERT INTO m3_bootstrap_importers VALUES('boot','worker','acknowledged',0,'schema'); INSERT INTO m3_feedback_gates VALUES('boot',1,'open',NULL,0); INSERT INTO bootstrap_imports VALUES('import','boot','d','acknowledged',0); INSERT INTO backfill_runs VALUES('run','d','1','running',0); INSERT INTO backfill_generations VALUES('gen','run',1,NULL,'fencing'); INSERT INTO backfill_chunks VALUES('chunk','gen',X'',X'','complete',5,'sum'); INSERT INTO m3_planner_runs VALUES('gen','boot',5); INSERT INTO relation_schemas VALUES('schema','1','rel',X'00','sum',1);").unwrap();
        FenceStore::open(w).unwrap()
    }
    fn nonce() -> [u8; 16] {
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    }
    fn input() -> FenceIntentInput {
        FenceIntentInput {
            intent_id: "intent".into(),
            generation_id: "gen".into(),
            bootstrap_intent_id: "boot".into(),
            capture_epoch: 1,
            generation: 1,
            nonce: nonce(),
            table_set_fingerprint: [0xaa; 32],
            anchor_id: "anchor".into(),
            expires_at: "later".into(),
        }
    }
    fn journal_payload(s: &mut FenceStore, txid: &str, lsn: &str, payload: Vec<u8>) {
        let hash = crate::m2_journal::sha256(&payload);
        s.writer.connection().execute("INSERT INTO source_transactions VALUES(?1,'1','sys','db','slot','7',?2,6,6,1,'sum','committed')",params![txid,lsn]).unwrap();
        s.writer
            .connection()
            .execute(
                "INSERT INTO journal_events VALUES(6,?1,?2,0,'1',NULL,'capture_fence',?3,?4)",
                params![format!("event-{txid}"), txid, payload, hash],
            )
            .unwrap();
    }
    fn journal(s: &mut FenceStore, txid: &str, lsn: &str, nonce: [u8; 16]) {
        journal_payload(s, txid, lsn, fence_payload(1, 1, [0xaa; 32], nonce));
    }
    fn decode_hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let digit = |v: u8| match v {
                    b'0'..=b'9' => v - b'0',
                    b'a'..=b'f' => v - b'a' + 10,
                    b'A'..=b'F' => v - b'A' + 10,
                    _ => panic!("invalid fixture hex"),
                };
                (digit(pair[0]) << 4) | digit(pair[1])
            })
            .collect()
    }
    fn parse_pg_lsn(value: &str) -> u64 {
        let (hi, lo) = value.split_once('/').unwrap();
        (u64::from_str_radix(hi, 16).unwrap() << 32) | u64::from_str_radix(lo, 16).unwrap()
    }
    #[test]
    fn exact_durable_pair_completes_anchor_and_duplicate_is_audit_only() {
        let mut s = setup("pair");
        s.prepare_after_copy(&input()).unwrap();
        s.mark_dispatched("intent", 1).unwrap();
        s.writer
            .connection()
            .execute(
                "UPDATE m3_bootstrap_runtime SET start_seq=5 WHERE intent_id='boot'",
                [],
            )
            .unwrap();
        s.writer.connection().execute("INSERT INTO source_transactions VALUES('lower','1','sys','db','slot','6','000000000000000F',5,5,0,'lower-sum','committed')",[]).unwrap();
        s.writer
            .connection()
            .execute(
                "INSERT INTO m3_planner_runs VALUES('other-generation','boot',999)",
                [],
            )
            .unwrap();
        journal(&mut s, "tx", "0000000000000010", nonce());
        let p = s.observe_durable("intent", "tx").unwrap();
        assert!(p.first_proof);
        let again = s.observe_durable("intent", "tx").unwrap();
        assert!(!again.first_proof);
        assert_eq!(again.post_copy_fence_lsn, "0000000000000010");
        assert_eq!(
            s.writer
                .connection()
                .query_row(
                    "SELECT start_seq FROM bootstrap_anchors WHERE anchor_id='anchor'",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            5
        );
        assert_eq!(
            s.writer
                .connection()
                .query_row(
                    "SELECT count(*) FROM bootstrap_anchors WHERE state='complete'",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
    }
    #[test]
    fn acknowledged_bootstrap_planner_and_fence_compose_one_canonical_anchor() {
        struct EmptyImportedSource;
        impl BoundedRangeSource for EmptyImportedSource {
            fn read_range(
                &mut self,
                _: &KeyRange,
                _: &[KeyPartType],
                _: ReadBudget,
            ) -> Result<Vec<SnapshotRow>, crate::m3_planner::PlannerError> {
                Ok(Vec::new())
            }
        }

        let database = path("composed-anchor");
        let writer = open_writer(&database, "bootstrap", 1, 1).unwrap();
        writer.connection().execute_batch(
            "INSERT INTO source_state(singleton,capture_epoch,source_system_id,timeline_id,database_id,slot_name,plugin,publication_fingerprint,protocol_fingerprint) VALUES(1,'1','sys','1','db','slot','pgoutput','publication','protocol');
             INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('destination','archive','cfg','1',1);
             INSERT INTO relation_schemas(schema_fingerprint,capture_epoch,relation_id,canonical_schema,schema_checksum,created_seq) VALUES('schema-fp','1','rel',X'00','sum',1);",
        ).unwrap();
        let assignment = digest_assignment(&[0, 0, 0, 0, 0, 0, 0, 0]);
        let mut bootstrap = BootstrapStore::open(writer).unwrap();
        bootstrap
            .prepare(&PrepareIntent {
                intent_id: "boot".into(),
                capture_epoch: "1".into(),
                source_system_id: "sys".into(),
                database_id: "db".into(),
                slot_name: "slot".into(),
                generation: 1,
                table_set_fingerprint: hex(&[0xaa; 32]),
                configuration_fingerprint: "cfg".into(),
                importers: vec![ImporterAssignment {
                    importer_id: "worker".into(),
                    assigned_ranges_digest: assignment,
                }],
                created_at: "now".into(),
            })
            .unwrap();
        bootstrap.record_guard_acquired("boot", 10).unwrap();
        bootstrap
            .persist_export_response(
                "boot",
                &ExportResponse {
                    consistent_lsn: 1,
                    snapshot_token: ["snapshot", "token"].join("-"),
                    start_seq: 0,
                    exporter_backend_pid: 11,
                },
            )
            .unwrap();
        bootstrap.snapshot_set_first("boot", "worker", 12).unwrap();
        bootstrap
            .bind_importer_contract("boot", "worker", "schema-fp")
            .unwrap();
        bootstrap.acknowledge_import("boot", "worker").unwrap();
        bootstrap.release_exporter("boot").unwrap();
        drop(bootstrap);

        let limits = PlannerLimits {
            chunk_rows: 1,
            chunk_bytes: 1,
            chunk_duration: std::time::Duration::from_secs(1),
            writer_hold: std::time::Duration::from_secs(1),
            concurrency: 1,
            source_impact_bytes: 1024,
            max_rows_per_second: 100,
        };
        let mut mismatched_writer = open_writer(&database, "planner-mismatch", 2, 2).unwrap();
        mismatched_writer.connection_mut().execute(
            "UPDATE destinations SET configuration_fingerprint='other',revision=revision+1 WHERE destination_id='destination'",
            [],
        ).unwrap();
        let mut mismatched_planner = PlannerStore::open(mismatched_writer, limits).unwrap();
        let plan_input = || PlanInput {
            run_id: "run".into(),
            generation_id: "gen".into(),
            destination_id: "destination".into(),
            capture_epoch: "1".into(),
            generation: 1,
            bootstrap_intent_id: "boot".into(),
            importer_id: "worker".into(),
            snapshot_schema_fingerprint: "schema-fp".into(),
            key_schema: vec![KeyPartType::I64],
            boundaries: vec![],
            estimated_rows: 0,
            start_seq: 0,
            started_mono_ms: 1,
        };
        assert!(matches!(
            mismatched_planner.persist_plan(&plan_input()),
            Err(crate::m3_planner::PlannerError::Conflict(
                "M3_CANONICAL_IMPORT_PROOF_MISSING"
            ))
        ));
        drop(mismatched_planner);
        let mut matching_writer = open_writer(&database, "planner", 3, 3).unwrap();
        matching_writer.connection_mut().execute(
            "UPDATE destinations SET configuration_fingerprint='cfg',revision=revision+1 WHERE destination_id='destination'",
            [],
        ).unwrap();
        let mut planner = PlannerStore::open(matching_writer, limits).unwrap();
        planner.persist_plan(&plan_input()).unwrap();
        let claim = planner
            .claim_next("gen", "worker", 2, std::time::Duration::from_secs(1))
            .unwrap()
            .unwrap();
        planner
            .execute_claim(&claim, &mut EmptyImportedSource, 3)
            .unwrap();
        planner.finish_copy("gen").unwrap();
        drop(planner);

        let mut fence = FenceStore::open(open_writer(&database, "fence", 4, 4).unwrap()).unwrap();
        assert_eq!(
            fence
                .writer
                .connection()
                .query_row(
                    "SELECT intent_id||':'||destination_id||':'||state FROM bootstrap_imports",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .unwrap(),
            "boot:destination:acknowledged"
        );
        fence.prepare_after_copy(&input()).unwrap();
        fence.mark_dispatched("intent", 1).unwrap();
        journal(&mut fence, "tx", "0000000000000010", nonce());
        let proof = fence.observe_durable("intent", "tx").unwrap();
        assert!(proof.first_proof);
        assert_eq!(fence.writer.connection().query_row(
            "SELECT bootstrap_intent_id||':'||generation_id||':'||state FROM bootstrap_anchors WHERE anchor_id='anchor'", [],
            |row| row.get::<_, String>(0),
        ).unwrap(), "boot:gen:complete");
    }

    #[test]
    fn delayed_copy_cannot_dispatch_or_complete_anchor() {
        let mut s = setup("delayed");
        s.writer
            .connection()
            .execute(
                "INSERT INTO backfill_chunks VALUES('delayed','gen',X'01',X'02','pending',NULL,NULL)",
                [],
            )
            .unwrap();
        assert!(matches!(
            s.prepare_after_copy(&input()),
            Err(FenceError::Conflict("M3_FENCE_PREREQUISITE_MISMATCH"))
        ));
        assert_eq!(
            s.writer
                .connection()
                .query_row("SELECT count(*) FROM m3_fence_intents", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            s.writer
                .connection()
                .query_row("SELECT count(*) FROM bootstrap_anchors", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
    #[test]
    fn restart_reconciles_persisted_intent_without_inventing_a_proof() {
        let mut s = setup("restart");
        let expected = s.prepare_after_copy(&input()).unwrap();
        let database_path: String = s
            .writer
            .connection()
            .query_row(
                "SELECT file FROM pragma_database_list WHERE name='main'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        drop(s);
        let mut s = FenceStore::open(
            open_writer(std::path::Path::new(&database_path), "restart", 2, 2).unwrap(),
        )
        .unwrap();
        assert_eq!(s.load_dispatch("intent").unwrap(), expected);
        assert!(matches!(
            s.observe_durable("intent", "missing"),
            Err(FenceError::Conflict("M3_FENCE_DURABLE_PAIR_MISSING"))
        ));
        journal(&mut s, "tx", "0000000000000010", nonce());
        let proof = s.observe_durable("intent", "tx").unwrap();
        assert!(proof.first_proof);
        assert_eq!(
            s.writer
                .connection()
                .query_row(
                    "SELECT state FROM m3_fence_intents WHERE intent_id='intent'",
                    [],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
            "complete"
        );
    }
    #[test]
    fn sampled_or_mismatched_observation_cannot_complete_anchor() {
        let mut s = setup("mismatch");
        s.prepare_after_copy(&input()).unwrap();
        s.mark_dispatched("intent", 1).unwrap();
        journal(&mut s, "tx", "0000000000000010", [0xbb; 16]);
        assert!(matches!(
            s.observe_durable("intent", "tx"),
            Err(FenceError::Conflict("M3_FENCE_IDENTITY_MISMATCH"))
        ));
        assert_eq!(
            s.writer
                .connection()
                .query_row("SELECT count(*) FROM bootstrap_anchors", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
    #[test]
    fn repeated_matching_transaction_is_audit_only_and_first_pair_is_immutable() {
        let mut s = setup("audit");
        s.prepare_after_copy(&input()).unwrap();
        s.mark_dispatched("intent", 1).unwrap();
        journal(&mut s, "tx", "0000000000000010", nonce());
        let first = s.observe_durable("intent", "tx").unwrap();
        let payload = fence_payload(1, 1, [0xaa; 32], nonce());
        let hash = crate::m2_journal::sha256(&payload);
        s.writer.connection().execute("INSERT INTO source_transactions VALUES('tx2','1','sys','db','slot','8','0000000000000020',7,7,1,'sum2','committed')", []).unwrap();
        s.writer.connection().execute("INSERT INTO journal_events VALUES(7,'event-tx2','tx2',0,'1',NULL,'capture_fence',?1,?2)", params![payload,hash]).unwrap();
        let duplicate = s.observe_durable("intent", "tx2").unwrap();
        assert!(!duplicate.first_proof);
        assert_eq!(duplicate.transaction_id, first.transaction_id);
        assert_eq!(duplicate.post_copy_fence_lsn, first.post_copy_fence_lsn);
        assert_eq!(
            s.writer
                .connection()
                .query_row(
                    "SELECT count(*) FROM m3_fence_observations WHERE first_proof=0",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
    }
    #[test]
    fn fixed_row_dispatch_requires_exactly_one_row() {
        let mut s = setup("cardinality");
        let dispatch = s.prepare_after_copy(&input()).unwrap();
        assert_eq!(dispatch.capture_epoch, 1);
        assert_eq!(dispatch.generation, 1);
        assert_eq!(dispatch.table_set_fingerprint, [0xaa; 32]);
        assert_eq!(dispatch.nonce, nonce());
        assert!(dispatch.sql.ends_with("WHERE id=1"));
        assert!(s.mark_dispatched("intent", 0).is_err());
        assert!(s.mark_dispatched("intent", 2).is_err());
        s.mark_dispatched("intent", 1).unwrap();
    }
    #[test]
    fn invalidation_marks_planner_generation_and_fence_intent_ineligible_atomically() {
        let mut s = setup("invalidate");
        s.prepare_after_copy(&input()).unwrap();
        s.mark_dispatched("intent", 1).unwrap();
        invalidate_and_release(s.writer.connection_mut(), "boot", 1, "guard").unwrap();
        let states: (String,String,String,String)=s.writer.connection().query_row(
            "SELECT x.state,f.state,g.state,i.state FROM m3_bootstrap_runtime x JOIN m3_feedback_gates f USING(intent_id) JOIN m3_planner_runs p ON p.bootstrap_intent_id=x.intent_id JOIN backfill_generations g USING(generation_id) JOIN m3_fence_intents i USING(generation_id) WHERE x.intent_id='boot'",
            [],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)),
        ).unwrap();
        assert_eq!(
            states,
            (
                "snapshot_unusable".into(),
                "invalidated".into(),
                "invalidated".into(),
                "invalidated".into()
            )
        );
        let mut gate = PersistedFeedbackGate::new(s.writer.connection(), "boot", 1);
        assert_eq!(
            gate.permit(Some(11)),
            FeedbackPermit::AllowSafeBoundary { lsn: 11 }
        );
    }
    #[test]
    fn live_pgoutput_observation_uses_commit_message_end_lsn() {
        let Some(path) = std::env::var_os("BORING_CDC_M3_FENCE_OBSERVATION") else {
            return;
        };
        let observed: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(observed["pgoutput_contains_nonce"], true);
        assert_eq!(observed["affected_rows"], 1);
        let lsn = observed["commit_end_lsn"].as_str().unwrap();
        assert_eq!(lsn.len(), 16);
        let mut decoder = Decoder::new(WireLimits::default());
        let mut captured_payload = None;
        let mut decoded_end_lsn = None;
        for message in observed["pgoutput_messages"].as_array().unwrap() {
            let position = parse_pg_lsn(message["lsn"].as_str().unwrap());
            let data = decode_hex(message["data_hex"].as_str().unwrap());
            let mut frame = vec![b'w'];
            frame.extend_from_slice(&position.to_be_bytes());
            frame.extend_from_slice(&position.to_be_bytes());
            frame.extend_from_slice(&0_i64.to_be_bytes());
            frame.extend_from_slice(&data);
            match decoder.decode_copy_data(&frame).unwrap() {
                CopyBothEvent::XLogData {
                    event: PgoutputEvent::RelationNeedsValidation(relation),
                    ..
                } => decoder
                    .admit_relation(RelationContract {
                        relation,
                        key_columns: vec![0],
                        control: Some(ControlContract {
                            immutable_key: vec![b"1".to_vec()],
                            mutable_columns: vec![1, 2, 3, 4],
                        }),
                    })
                    .unwrap(),
                CopyBothEvent::XLogData {
                    event: PgoutputEvent::Row(row),
                    ..
                } => captured_payload = Some(encode_row(&row, None).unwrap()),
                CopyBothEvent::XLogData {
                    event: PgoutputEvent::Commit { end_lsn, .. },
                    ..
                } => decoded_end_lsn = Some(format!("{end_lsn:016X}")),
                _ => {}
            }
        }
        assert_eq!(decoded_end_lsn.as_deref(), Some(lsn));
        let mut s = setup("live");
        s.prepare_after_copy(&input()).unwrap();
        s.mark_dispatched("intent", observed["affected_rows"].as_u64().unwrap())
            .unwrap();
        journal_payload(&mut s, "live-tx", lsn, captured_payload.unwrap());
        let proof = s.observe_durable("intent", "live-tx").unwrap();
        assert_eq!(proof.post_copy_fence_lsn, lsn);
        if let Some(out) = std::env::var_os("BORING_CDC_M3_FENCE_RESULT") {
            std::fs::write(out, serde_json::to_vec(&serde_json::json!({"anchor_state":"complete","first_proof":proof.first_proof,"post_copy_fence_lsn":proof.post_copy_fence_lsn,"post_copy_fence_seq":proof.post_copy_fence_seq,"pgoutput_contains_nonce":true,"m2_encoded_row_from_live_pgoutput":true,"affected_rows":1})).unwrap()).unwrap();
        }
    }
    #[test]
    fn feedback_gate_holds_opens_and_stale_generation_fails_closed() {
        let mut w = writer("gate");
        w.connection().execute_batch("CREATE TABLE m3_bootstrap_runtime(intent_id TEXT PRIMARY KEY,generation INTEGER,state TEXT,exporter_liveness TEXT,guard_liveness TEXT,snapshot_promotable INTEGER,feedback_gate_open INTEGER,revision INTEGER); CREATE TABLE m3_bootstrap_importers(intent_id TEXT,importer_id TEXT,state TEXT,revision INTEGER,PRIMARY KEY(intent_id,importer_id)); INSERT INTO m3_bootstrap_runtime VALUES('boot',1,'imports_pending','command_idle','held',1,0,0); INSERT INTO m3_bootstrap_importers VALUES('boot','a','contract_bound',0); INSERT INTO m3_bootstrap_importers VALUES('boot','b','contract_bound',0);").unwrap();
        install(w.connection()).unwrap();
        {
            let tx = w.connection_mut().transaction().unwrap();
            register_feedback_gate(&tx, "boot", 1).unwrap();
            tx.commit().unwrap();
        }
        {
            let mut g = PersistedFeedbackGate::new(w.connection(), "boot", 1);
            assert_eq!(g.permit(Some(9)), FeedbackPermit::Hold);
        }
        acknowledge_and_open(w.connection_mut(), "boot", "a", 1).unwrap();
        acknowledge_and_open(w.connection_mut(), "boot", "a", 1).unwrap();
        {
            let mut g = PersistedFeedbackGate::new(w.connection(), "boot", 1);
            assert_eq!(g.permit(Some(9)), FeedbackPermit::Hold);
        }
        acknowledge_and_open(w.connection_mut(), "boot", "b", 1).unwrap();
        {
            let mut g = PersistedFeedbackGate::new(w.connection(), "boot", 1);
            assert_eq!(
                g.permit(Some(9)),
                FeedbackPermit::AllowSafeBoundary { lsn: 9 }
            );
        }
        assert!(acknowledge_and_open(w.connection_mut(), "boot", "b", 2).is_err());
    }
}
