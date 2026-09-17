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
    pub capture_epoch: String,
    pub generation: u64,
    pub nonce: String,
    pub table_set_fingerprint: String,
    pub anchor_id: String,
    pub expires_at: String,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FenceDispatch {
    pub sql: String,
    pub capture_epoch: String,
    pub generation: u64,
    pub table_set_fingerprint: String,
    /// Kept out of logs and status; the dispatcher binds it as a query parameter.
    pub nonce: String,
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
#[serde(deny_unknown_fields)]
struct FencePayload {
    capture_epoch: String,
    generation: u64,
    table_set_fingerprint: String,
    unique_nonce: String,
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
            || generation != input.generation as i64
            || epoch != input.capture_epoch
            || tables != input.table_set_fingerprint
            || incomplete != 0
            || pending_imports != 0
            || gate != "open"
        {
            return Err(FenceError::Conflict("M3_FENCE_PREREQUISITE_MISMATCH"));
        }
        let changed=tx.execute("UPDATE backfill_generations SET intended_fence_nonce=?2 WHERE generation_id=?1 AND intended_fence_nonce IS NULL AND state='fencing'",params![input.generation_id,input.nonce])?;
        if changed != 1 {
            return Err(FenceError::Conflict("M3_FENCE_NONCE_ALREADY_INTENDED"));
        }
        tx.execute("INSERT INTO m3_fence_intents(intent_id,generation_id,bootstrap_intent_id,capture_epoch,generation,nonce,table_set_fingerprint,anchor_id,expires_at,state) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,'intended')",params![input.intent_id,input.generation_id,input.bootstrap_intent_id,input.capture_epoch,input.generation,input.nonce,input.table_set_fingerprint,input.anchor_id,input.expires_at])?;
        tx.commit()?;
        Ok(FenceDispatch { sql:"UPDATE boring_cdc_control.capture_fences SET capture_epoch=$1,generation=$2,table_set_fingerprint=$3,unique_nonce=$4 WHERE id='singleton'".into(), capture_epoch:input.capture_epoch.clone(), generation:input.generation, table_set_fingerprint:input.table_set_fingerprint.clone(), nonce:input.nonce.clone() })
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
        if !matches!(intent.8.as_str(), "dispatched" | "complete") {
            return Err(FenceError::Conflict("M3_FENCE_OBSERVATION_STALE"));
        }
        let durable:Option<(String,i64,Vec<u8>)>=tx.query_row("SELECT t.end_lsn,t.last_seq,e.payload FROM source_transactions t JOIN journal_events e ON e.transaction_id=t.transaction_id AND e.journal_seq=t.last_seq WHERE t.transaction_id=?1 AND t.capture_epoch=?2 AND t.state='committed' AND e.control_kind='capture_fence'",params![transaction_id,intent.2],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        let Some((lsn, seq, payload)) = durable else {
            return Err(FenceError::Conflict("M3_FENCE_DURABLE_PAIR_MISSING"));
        };
        let decoded: FencePayload = serde_json::from_slice(&payload)
            .map_err(|_| FenceError::Conflict("M3_FENCE_PAYLOAD_INVALID"))?;
        if decoded.capture_epoch != intent.2
            || decoded.generation != intent.3 as u64
            || decoded.unique_nonce != intent.4
            || decoded.table_set_fingerprint != intent.5
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
            let snapshot:(String,i64,String,String)=tx.query_row("SELECT x.consistent_lsn,p.next_snapshot_seq,x.table_set_fingerprint,json_group_array(DISTINCT i.snapshot_schema_fingerprint) FROM m3_bootstrap_runtime x JOIN m3_planner_runs p ON p.bootstrap_intent_id=x.intent_id JOIN m3_bootstrap_importers i ON i.intent_id=x.intent_id WHERE x.intent_id=?1 GROUP BY x.consistent_lsn,p.next_snapshot_seq,x.table_set_fingerprint",[&intent.1],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
            tx.execute("UPDATE backfill_generations SET state='complete' WHERE generation_id=?1 AND state='fencing'",[&intent.0])?;
            tx.execute("UPDATE backfill_runs SET state='complete',revision=revision+1 WHERE run_id=(SELECT run_id FROM backfill_generations WHERE generation_id=?1) AND state='running'",[&intent.0])?;
            tx.execute("INSERT INTO bootstrap_anchors(anchor_id,capture_epoch,generation,lower_stitch_lsn,start_seq,snapshot_boundary_lsn,snapshot_complete_seq,post_copy_fence_nonce,post_copy_fence_lsn,post_copy_fence_seq,table_set_fingerprint,snapshot_schema_fingerprints,state,expires_at,generation_id,bootstrap_intent_id) VALUES(?1,?2,?3,NULL,0,?4,?5,?6,?7,?8,?9,?10,'complete',?11,?12,?13)",params![intent.6,intent.2,intent.3,snapshot.0,snapshot.1,intent.4,lsn,seq,snapshot.2,snapshot.3,intent.7,intent.0,intent.1])?;
            tx.execute("UPDATE m3_fence_intents SET state='complete',revision=revision+1 WHERE intent_id=?1 AND state='dispatched'",[intent_id])?;
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

pub fn fence_payload(
    capture_epoch: &str,
    generation: u64,
    table_set_fingerprint: &str,
    nonce: &str,
) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"capture_epoch":capture_epoch,"generation":generation,"table_set_fingerprint":table_set_fingerprint,"unique_nonce":nonce})).expect("typed fence payload")
}
fn valid(v: &str) -> bool {
    !v.is_empty() && v.len() <= 256 && v.is_ascii()
}
fn validate_input(v: &FenceIntentInput) -> Result<(), FenceError> {
    if v.generation == 0
        || ![
            &v.intent_id,
            &v.generation_id,
            &v.bootstrap_intent_id,
            &v.capture_epoch,
            &v.nonce,
            &v.table_set_fingerprint,
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
pub fn nonce_hash(nonce: &str) -> String {
    format!("{:x}", Sha256::digest(nonce.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m2_capture_runtime::FeedbackGate;
    use crate::m2_schema::open_writer;
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
        w.connection().execute_batch("CREATE TABLE m3_bootstrap_runtime(intent_id TEXT PRIMARY KEY,generation INTEGER,start_seq INTEGER,consistent_lsn TEXT,table_set_fingerprint TEXT,state TEXT,exporter_liveness TEXT,guard_liveness TEXT,snapshot_promotable INTEGER,feedback_gate_open INTEGER,revision INTEGER); CREATE TABLE m3_bootstrap_importers(intent_id TEXT,importer_id TEXT,state TEXT,revision INTEGER,snapshot_schema_fingerprint TEXT,PRIMARY KEY(intent_id,importer_id)); CREATE TABLE m3_planner_runs(generation_id TEXT,bootstrap_intent_id TEXT,next_snapshot_seq INTEGER);").unwrap();
        install(w.connection()).unwrap();
        w.connection().execute_batch("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('d','archive','cfg','epoch',1); INSERT INTO bootstrap_intents VALUES('boot','epoch','sys','db','slot','0000000000000001','slot_created',0,'now'); INSERT INTO m3_bootstrap_runtime VALUES('boot',1,0,'0000000000000001','tables','exporter_released','released','held',1,1,0); INSERT INTO m3_bootstrap_importers VALUES('boot','worker','acknowledged',0,'schema'); INSERT INTO m3_feedback_gates VALUES('boot',1,'open',NULL,0); INSERT INTO bootstrap_imports VALUES('import','boot','d','acknowledged',0); INSERT INTO backfill_runs VALUES('run','d','epoch','running',0); INSERT INTO backfill_generations VALUES('gen','run',1,NULL,'fencing'); INSERT INTO backfill_chunks VALUES('chunk','gen',X'',X'','complete',5,'sum'); INSERT INTO m3_planner_runs VALUES('gen','boot',5); INSERT INTO relation_schemas VALUES('schema','epoch','rel',X'00','sum',1);").unwrap();
        FenceStore::open(w).unwrap()
    }
    fn input() -> FenceIntentInput {
        FenceIntentInput {
            intent_id: "intent".into(),
            generation_id: "gen".into(),
            bootstrap_intent_id: "boot".into(),
            capture_epoch: "epoch".into(),
            generation: 1,
            nonce: "nonce-1".into(),
            table_set_fingerprint: "tables".into(),
            anchor_id: "anchor".into(),
            expires_at: "later".into(),
        }
    }
    fn journal(s: &mut FenceStore, txid: &str, lsn: &str, nonce: &str) {
        let p = fence_payload("epoch", 1, "tables", nonce);
        let h = crate::m2_journal::sha256(&p);
        s.writer.connection().execute("INSERT INTO source_transactions VALUES(?1,'epoch','sys','db','slot','7',?2,6,6,1,'sum','committed')",params![txid,lsn]).unwrap();
        s.writer
            .connection()
            .execute(
                "INSERT INTO journal_events VALUES(6,?1,?2,0,'epoch',NULL,'capture_fence',?3,?4)",
                params![format!("event-{txid}"), txid, p, h],
            )
            .unwrap();
    }
    #[test]
    fn exact_durable_pair_completes_anchor_and_duplicate_is_audit_only() {
        let mut s = setup("pair");
        s.prepare_after_copy(&input()).unwrap();
        s.mark_dispatched("intent", 1).unwrap();
        journal(&mut s, "tx", "0000000000000010", "nonce-1");
        let p = s.observe_durable("intent", "tx").unwrap();
        assert!(p.first_proof);
        let again = s.observe_durable("intent", "tx").unwrap();
        assert!(!again.first_proof);
        assert_eq!(again.post_copy_fence_lsn, "0000000000000010");
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
        s.prepare_after_copy(&input()).unwrap();
        s.mark_dispatched("intent", 1).unwrap();
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
        assert!(matches!(
            s.observe_durable("intent", "missing"),
            Err(FenceError::Conflict("M3_FENCE_DURABLE_PAIR_MISSING"))
        ));
        assert_eq!(
            s.writer
                .connection()
                .query_row(
                    "SELECT state FROM m3_fence_intents WHERE intent_id='intent'",
                    [],
                    |r| r.get::<_, String>(0),
                )
                .unwrap(),
            "dispatched"
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
    fn sampled_or_mismatched_observation_cannot_complete_anchor() {
        let mut s = setup("mismatch");
        s.prepare_after_copy(&input()).unwrap();
        s.mark_dispatched("intent", 1).unwrap();
        journal(&mut s, "tx", "0000000000000010", "other");
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
        journal(&mut s, "tx", "0000000000000010", "nonce-1");
        let first = s.observe_durable("intent", "tx").unwrap();
        let payload = fence_payload("epoch", 1, "tables", "nonce-1");
        let hash = crate::m2_journal::sha256(&payload);
        s.writer.connection().execute("INSERT INTO source_transactions VALUES('tx2','epoch','sys','db','slot','8','0000000000000020',7,7,1,'sum2','committed')", []).unwrap();
        s.writer.connection().execute("INSERT INTO journal_events VALUES(7,'event-tx2','tx2',0,'epoch',NULL,'capture_fence',?1,?2)", params![payload,hash]).unwrap();
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
        s.prepare_after_copy(&input()).unwrap();
        assert!(s.mark_dispatched("intent", 0).is_err());
        assert!(s.mark_dispatched("intent", 2).is_err());
        s.mark_dispatched("intent", 1).unwrap();
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
        let mut s = setup("live");
        s.prepare_after_copy(&input()).unwrap();
        s.mark_dispatched("intent", observed["affected_rows"].as_u64().unwrap())
            .unwrap();
        journal(&mut s, "live-tx", lsn, "nonce-1");
        let proof = s.observe_durable("intent", "live-tx").unwrap();
        assert_eq!(proof.post_copy_fence_lsn, lsn);
        if let Some(out) = std::env::var_os("BORING_CDC_M3_FENCE_RESULT") {
            std::fs::write(out, serde_json::to_vec(&serde_json::json!({"anchor_state":"complete","first_proof":proof.first_proof,"post_copy_fence_lsn":proof.post_copy_fence_lsn,"post_copy_fence_seq":proof.post_copy_fence_seq,"pgoutput_contains_nonce":true,"affected_rows":1})).unwrap()).unwrap();
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
