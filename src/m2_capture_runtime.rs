//! Durable CopyBoth capture orchestration.
//!
//! This is the single integration boundary between the M1 decoder, the capped M2 spool,
//! SQLite publication and PostgreSQL feedback.  A connection generation is consumed once:
//! every unexpected transport or ownership loss returns to the external supervisor and is
//! never repaired in process.

use crate::article1_capture::{CaptureConfig, CaptureFailure, setup_runtime};
use crate::m1_decoder::{
    CopyBothEvent, Decoder, PgoutputEvent, RelationContract, RowChange, TupleValue, WireLimits,
};
use crate::m2_journal::{
    DurableCommit, JournalError, JournalEvent, JournalStore, SourceCommit, transaction_checksum,
};
use crate::m2_spool::{SpoolError, TxnBuffer};
use pg_walstream::CancellationToken;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;

pub const OWNER_BEAD: &str = "boring-cdc-m2-capture-runtime";
// ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol
pub const PUBLICATION: &str = crate::article1_capture::PUBLICATION;
// ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol
pub const SLOT: &str = crate::article1_capture::SLOT;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FeedbackPacket {
    pub write_lsn: u64,
    pub flush_lsn: u64,
    pub apply_lsn: u64,
    pub reply_requested: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeedbackPermit {
    /// No snapshot generation is attached; ordinary durable transaction progress is safe.
    AllowSafeBoundary { lsn: u64 },
    /// A downstream importer gate is closed. M2 does not persist or mutate that gate.
    Hold,
    /// The supplied generation/permit is stale and must not produce feedback.
    Stale,
}

pub trait FeedbackGate {
    fn permit(&mut self, durable_end_lsn: Option<u64>) -> FeedbackPermit;
}

pub struct NoSnapshotGate;
impl FeedbackGate for NoSnapshotGate {
    fn permit(&mut self, durable_end_lsn: Option<u64>) -> FeedbackPermit {
        FeedbackPermit::AllowSafeBoundary {
            lsn: durable_end_lsn.unwrap_or(0),
        }
    }
}

pub trait RuntimeSpool {
    fn push(&mut self, event: &[u8]) -> Result<(), SpoolError>;
    fn drain(&mut self) -> Result<Vec<Vec<u8>>, SpoolError>;
    fn finish(self: Box<Self>) -> Result<(), SpoolError>;
}
impl RuntimeSpool for TxnBuffer {
    fn push(&mut self, event: &[u8]) -> Result<(), SpoolError> {
        let mut receive = self.admit_receive(event.len())?;
        receive.extend_from_slice(event)?;
        self.push_received(receive)
    }
    fn drain(&mut self) -> Result<Vec<Vec<u8>>, SpoolError> {
        self.commit_iter()?
            .map(|entry| entry.map(|bytes| bytes.as_ref().to_vec()))
            .collect()
    }
    fn finish(self: Box<Self>) -> Result<(), SpoolError> {
        (*self).finish()
    }
}

pub trait SpoolFactory {
    fn begin(&mut self, xid: u32) -> Result<Box<dyn RuntimeSpool>, RuntimeError>;
}
impl<F> SpoolFactory for F
where
    F: FnMut(u32) -> Result<Box<dyn RuntimeSpool>, RuntimeError>,
{
    fn begin(&mut self, xid: u32) -> Result<Box<dyn RuntimeSpool>, RuntimeError> {
        self(xid)
    }
}

pub trait DurableJournal {
    fn commit(&mut self, commit: &SourceCommit) -> Result<DurableCommit, JournalError>;
}
impl DurableJournal for JournalStore {
    fn commit(&mut self, commit: &SourceCommit) -> Result<DurableCommit, JournalError> {
        self.commit_atomic(commit, crate::m2_journal::CommitFault::None)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeState {
    Starting,
    Capturing,
    CaptureSafeStopped,
    ExpectedClose,
    OwnershipLost,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeError {
    Decode(&'static str),
    Spool(String),
    Journal(String),
    Protocol(&'static str),
    Feedback,
    UnexpectedCopyBothLoss,
    OwnershipLost,
    RetryNotDue { next_retry_at_ms: u64 },
    ReconciliationRequired,
}
impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for RuntimeError {}

struct ActiveTransaction {
    xid: u32,
    origin: Option<(u64, String)>,
    spool: Box<dyn RuntimeSpool>,
}

pub struct CaptureRuntime<J, S, G> {
    decoder: Decoder,
    contracts: BTreeMap<u32, RelationContract>,
    journal: J,
    spools: S,
    gate: G,
    active: Option<ActiveTransaction>,
    durable_end_lsn: Option<u64>,
    state: RuntimeState,
    feedback: Vec<FeedbackPacket>,
    committed_transactions: u64,
}

impl<J: DurableJournal, S: SpoolFactory, G: FeedbackGate> CaptureRuntime<J, S, G> {
    pub fn new(
        journal: J,
        spools: S,
        gate: G,
        contracts: BTreeMap<u32, RelationContract>,
        durable_end_lsn: Option<u64>,
    ) -> Self {
        Self {
            decoder: Decoder::new(WireLimits::default()),
            contracts,
            journal,
            spools,
            gate,
            active: None,
            durable_end_lsn,
            state: RuntimeState::Starting,
            feedback: Vec::new(),
            committed_transactions: 0,
        }
    }
    pub fn state(&self) -> RuntimeState {
        self.state
    }
    pub fn durable_end_lsn(&self) -> Option<u64> {
        self.durable_end_lsn
    }
    pub fn committed_transactions(&self) -> u64 {
        self.committed_transactions
    }
    pub fn take_feedback(&mut self) -> Vec<FeedbackPacket> {
        std::mem::take(&mut self.feedback)
    }

    pub fn receive(&mut self, frame: &[u8]) -> Result<(), RuntimeError> {
        if matches!(
            self.state,
            RuntimeState::CaptureSafeStopped
                | RuntimeState::OwnershipLost
                | RuntimeState::ExpectedClose
        ) {
            return Err(RuntimeError::Protocol("runtime_not_receiving"));
        }
        self.state = RuntimeState::Capturing;
        let decoded = self.decoder.decode_copy_data(frame).map_err(|failure| {
            self.state = RuntimeState::CaptureSafeStopped;
            RuntimeError::Decode(failure.fingerprint)
        })?;
        let result = self.route(decoded);
        if result.is_err() && self.state == RuntimeState::Capturing {
            self.state = RuntimeState::CaptureSafeStopped;
        }
        result
    }

    fn route(&mut self, decoded: CopyBothEvent) -> Result<(), RuntimeError> {
        match decoded {
            CopyBothEvent::Keepalive {
                reply_requested, ..
            } => {
                if reply_requested {
                    self.queue_feedback(true);
                }
                Ok(())
            }
            CopyBothEvent::XLogData { event, .. } => self.route_pgoutput(event),
        }
    }

    fn route_pgoutput(&mut self, event: PgoutputEvent) -> Result<(), RuntimeError> {
        match event {
            PgoutputEvent::Begin { xid, .. } => {
                if self.active.is_some() {
                    return self.safe_stop(RuntimeError::Protocol("nested_transaction"));
                }
                let spool = self.spools.begin(xid)?;
                self.active = Some(ActiveTransaction {
                    xid,
                    origin: None,
                    spool,
                });
                Ok(())
            }
            PgoutputEvent::RelationNeedsValidation(relation) => {
                let contract = self
                    .contracts
                    .get(&relation.id)
                    .filter(|c| c.relation == relation)
                    .cloned()
                    .ok_or(RuntimeError::Protocol("relation_contract_mismatch"))?;
                self.decoder
                    .admit_relation(contract)
                    .map_err(|failure| RuntimeError::Decode(failure.fingerprint))
            }
            PgoutputEvent::RelationMetadata(_) => Ok(()),
            PgoutputEvent::Origin { origin_lsn, name } => {
                let active = self
                    .active
                    .as_mut()
                    .ok_or(RuntimeError::Protocol("origin_without_transaction"))?;
                active.origin = Some((origin_lsn, name));
                Ok(())
            }
            PgoutputEvent::Row(row) => {
                let control_kind = self
                    .contracts
                    .get(&row.relation_id)
                    .and_then(|contract| contract.control.as_ref())
                    .map(|_| {
                        if self.contracts[&row.relation_id].relation.name == "heartbeat" {
                            "heartbeat"
                        } else {
                            "capture_fence"
                        }
                    });
                let payload =
                    encode_row(&row, self.active.as_ref().and_then(|a| a.origin.as_ref()))?;
                let staged = serde_json::to_vec(&StagedEvent {
                    relation_id: row.relation_id,
                    control_kind,
                    payload,
                })
                .map_err(|_| RuntimeError::Protocol("event_encoding"))?;
                self.active
                    .as_mut()
                    .ok_or(RuntimeError::Protocol("row_without_transaction"))?
                    .spool
                    .push(&staged)
                    .map_err(|e| {
                        self.state = RuntimeState::CaptureSafeStopped;
                        RuntimeError::Spool(e.to_string())
                    })
            }
            PgoutputEvent::Commit {
                commit_lsn,
                end_lsn,
                row_count,
                ..
            } => self.publish(commit_lsn, end_lsn, row_count),
        }
    }

    fn publish(
        &mut self,
        commit_lsn: u64,
        end_lsn: u64,
        row_count: u64,
    ) -> Result<(), RuntimeError> {
        let mut active = self
            .active
            .take()
            .ok_or(RuntimeError::Protocol("commit_without_spool"))?;
        let payloads = active
            .spool
            .drain()
            .map_err(|e| RuntimeError::Spool(e.to_string()))?;
        if payloads.len() as u64 != row_count {
            return self.safe_stop(RuntimeError::Protocol("row_count_mismatch"));
        }
        let events = payloads
            .into_iter()
            .enumerate()
            .map(|(ordinal, staged)| {
                let staged: StagedEventOwned = serde_json::from_slice(&staged)
                    .map_err(|_| RuntimeError::Protocol("staged_event_corrupt"))?;
                let payload_hash = format!("{:x}", Sha256::digest(&staged.payload));
                Ok(JournalEvent {
                    event_id: format!("wal:{end_lsn:016X}:{ordinal}"),
                    transaction_ordinal: ordinal as u32,
                    relation_schema_fingerprint: None,
                    control_kind: staged.control_kind,
                    payload: staged.payload,
                    payload_hash,
                })
            })
            .collect::<Result<Vec<_>, RuntimeError>>()?;
        if events.is_empty() {
            return self.safe_stop(RuntimeError::Protocol("empty_transaction"));
        }
        let commit = SourceCommit {
            transaction_id: format!("wal:{end_lsn:016X}"),
            xid: active.xid.to_string(),
            end_lsn: format!("{end_lsn:016X}"),
            payload_checksum: transaction_checksum(&events),
            schemas: Vec::new(),
            events,
        };
        let durable = self.journal.commit(&commit).map_err(|error| {
            self.state = RuntimeState::CaptureSafeStopped;
            RuntimeError::Journal(error.to_string())
        })?;
        let durable_lsn = parse_lsn(durable.feedback_eligible_end_lsn())?;
        if durable_lsn != end_lsn || commit_lsn > end_lsn {
            return self.safe_stop(RuntimeError::Protocol("durable_boundary_mismatch"));
        }
        active
            .spool
            .finish()
            .map_err(|e| RuntimeError::Spool(e.to_string()))?;
        self.committed_transactions = self.committed_transactions.saturating_add(1);
        self.durable_end_lsn = Some(
            self.durable_end_lsn
                .map_or(durable_lsn, |old| old.max(durable_lsn)),
        );
        self.queue_feedback(false);
        Ok(())
    }

    fn queue_feedback(&mut self, requested: bool) {
        let durable = self.durable_end_lsn;
        if let FeedbackPermit::AllowSafeBoundary { lsn: permitted } = self.gate.permit(durable) {
            let safe = durable.map_or(permitted, |value| value.min(permitted));
            self.feedback.push(FeedbackPacket {
                write_lsn: safe,
                flush_lsn: safe,
                apply_lsn: safe,
                reply_requested: requested,
            });
        }
    }
    fn safe_stop<T>(&mut self, error: RuntimeError) -> Result<T, RuntimeError> {
        self.state = RuntimeState::CaptureSafeStopped;
        Err(error)
    }
    pub fn graceful_shutdown(&mut self) -> Result<(), RuntimeError> {
        if self.active.is_some() {
            return self.safe_stop(RuntimeError::ReconciliationRequired);
        }
        self.state = RuntimeState::ExpectedClose;
        Ok(())
    }
    pub fn unexpected_eof(&mut self) -> RuntimeError {
        self.state = RuntimeState::OwnershipLost;
        RuntimeError::UnexpectedCopyBothLoss
    }
    pub fn ownership_lost(&mut self) -> RuntimeError {
        self.state = RuntimeState::OwnershipLost;
        RuntimeError::OwnershipLost
    }
}

#[derive(Serialize)]
struct StagedEvent<'a> {
    relation_id: u32,
    control_kind: Option<&'a str>,
    payload: Vec<u8>,
}
#[derive(Deserialize)]
struct StagedEventOwned {
    control_kind: Option<String>,
    payload: Vec<u8>,
}

#[derive(Serialize)]
struct EncodedRow<'a> {
    kind: &'static str,
    relation_id: u32,
    ordinal: u64,
    old_kind: Option<&'static str>,
    old: Option<Vec<EncodedTuple>>,
    new: Option<Vec<EncodedTuple>>,
    origin_lsn: Option<u64>,
    origin_name: Option<&'a str>,
}
#[derive(Serialize)]
#[serde(tag = "state", content = "bytes", rename_all = "snake_case")]
enum EncodedTuple {
    Null,
    UnchangedToast,
    Text(Vec<u8>),
}
fn tuples(value: Option<Vec<TupleValue>>) -> Option<Vec<EncodedTuple>> {
    value.map(|values| {
        values
            .into_iter()
            .map(|v| match v {
                TupleValue::Null => EncodedTuple::Null,
                TupleValue::UnchangedToast => EncodedTuple::UnchangedToast,
                TupleValue::Text(b) => EncodedTuple::Text(b),
            })
            .collect()
    })
}
fn encode_row(row: &RowChange, origin: Option<&(u64, String)>) -> Result<Vec<u8>, RuntimeError> {
    serde_json::to_vec(&EncodedRow {
        kind: match row.kind {
            crate::m1_decoder::RowKind::Insert => "insert",
            crate::m1_decoder::RowKind::Update => "update",
            crate::m1_decoder::RowKind::Delete => "delete",
        },
        relation_id: row.relation_id,
        ordinal: row.ordinal,
        old_kind: row.old_kind.map(|v| match v {
            crate::m1_decoder::OldTupleKind::Key => "key",
            crate::m1_decoder::OldTupleKind::Full => "full",
        }),
        old: tuples(row.old.clone()),
        new: tuples(row.new.clone()),
        origin_lsn: origin.map(|v| v.0),
        origin_name: origin.map(|v| v.1.as_str()),
    })
    .map_err(|_| RuntimeError::Protocol("event_encoding"))
}
fn parse_lsn(value: &str) -> Result<u64, RuntimeError> {
    u64::from_str_radix(value, 16).map_err(|_| RuntimeError::Protocol("invalid_durable_lsn"))
}

pub trait RetrySchedule {
    fn next_retry_at_ms(&self) -> Option<u64>;
}
pub fn reconcile_startup(
    schedule: &impl RetrySchedule,
    now_ms: u64,
    ownership_reconciled: bool,
    spool_reconciled: bool,
) -> Result<(), RuntimeError> {
    if !ownership_reconciled || !spool_reconciled {
        return Err(RuntimeError::ReconciliationRequired);
    }
    if let Some(next) = schedule.next_retry_at_ms().filter(|next| *next > now_ms) {
        return Err(RuntimeError::RetryNotDue {
            next_retry_at_ms: next,
        });
    }
    Ok(())
}

/// Production Article-1 transport, consumed directly rather than forked. It opens one CopyBoth
/// generation, delegates every payload to the durable kernel, and sends only queued safe packets.
pub async fn capture_copyboth<J: DurableJournal, S: SpoolFactory, G: FeedbackGate>(
    config: &CaptureConfig,
    cancellation: &CancellationToken,
    runtime: &mut CaptureRuntime<J, S, G>,
) -> Result<(), CaptureFailure> {
    capture_copyboth_until(config, cancellation, runtime, u64::MAX).await
}

pub async fn capture_copyboth_until<J: DurableJournal, S: SpoolFactory, G: FeedbackGate>(
    config: &CaptureConfig,
    cancellation: &CancellationToken,
    runtime: &mut CaptureRuntime<J, S, G>,
    stop_after_commits: u64,
) -> Result<(), CaptureFailure> {
    let (mut connection, contracts) = setup_runtime(config)?;
    runtime.contracts = contracts;
    loop {
        if cancellation.is_cancelled() {
            runtime.graceful_shutdown().map_err(|_| {
                CaptureFailure::at("runtime", "M2_SHUTDOWN_RECONCILIATION_REQUIRED")
            })?;
            return Ok(());
        }
        let frame = connection
            .get_copy_data_async(cancellation)
            .await
            .map_err(|_| {
                runtime.unexpected_eof();
                CaptureFailure::at("runtime", "M2_COPYBOTH_UNEXPECTED_LOSS")
            })?;
        runtime
            .receive(&frame)
            .map_err(|_| CaptureFailure::at("runtime", "M2_CAPTURE_SAFE_STOPPED"))?;
        for packet in runtime.take_feedback() {
            connection
                .send_standby_status_update(
                    packet.write_lsn,
                    packet.flush_lsn,
                    packet.apply_lsn,
                    packet.reply_requested,
                )
                .await
                .map_err(|_| {
                    runtime.unexpected_eof();
                    CaptureFailure::at("runtime", "M2_FEEDBACK_TRANSPORT_LOSS")
                })?;
        }
        if runtime.committed_transactions() >= stop_after_commits {
            runtime.graceful_shutdown().map_err(|_| {
                CaptureFailure::at("runtime", "M2_SHUTDOWN_RECONCILIATION_REQUIRED")
            })?;
            return Ok(());
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::m2_journal::{CommitFault, CommitLimits, SourceIdentity};
    use crate::m2_schema::open_writer;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    static NEXT: AtomicU64 = AtomicU64::new(1);

    #[derive(Default)]
    struct MemorySpools;
    struct MemorySpool(Vec<Vec<u8>>);
    impl RuntimeSpool for MemorySpool {
        fn push(&mut self, b: &[u8]) -> Result<(), SpoolError> {
            self.0.push(b.to_vec());
            Ok(())
        }
        fn drain(&mut self) -> Result<Vec<Vec<u8>>, SpoolError> {
            Ok(std::mem::take(&mut self.0))
        }
        fn finish(self: Box<Self>) -> Result<(), SpoolError> {
            Ok(())
        }
    }
    impl SpoolFactory for MemorySpools {
        fn begin(&mut self, _: u32) -> Result<Box<dyn RuntimeSpool>, RuntimeError> {
            Ok(Box::new(MemorySpool(Vec::new())))
        }
    }
    struct Gate(FeedbackPermit);
    impl FeedbackGate for Gate {
        fn permit(&mut self, _: Option<u64>) -> FeedbackPermit {
            self.0
        }
    }
    fn frame(tag: u8, payload: &[u8]) -> Vec<u8> {
        let mut v = vec![b'w'];
        v.extend_from_slice(&0u64.to_be_bytes());
        v.extend_from_slice(&0u64.to_be_bytes());
        v.extend_from_slice(&0i64.to_be_bytes());
        v.push(tag);
        v.extend_from_slice(payload);
        v
    }
    fn begin(xid: u32, end: u64) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&end.to_be_bytes());
        p.extend_from_slice(&0i64.to_be_bytes());
        p.extend_from_slice(&xid.to_be_bytes());
        frame(b'B', &p)
    }
    fn insert(xid: u32, relation: u32, value: &[u8]) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&relation.to_be_bytes());
        p.push(b'N');
        p.extend_from_slice(&1u16.to_be_bytes());
        p.push(b't');
        p.extend_from_slice(&(value.len() as u32).to_be_bytes());
        p.extend_from_slice(value);
        let _ = xid;
        frame(b'I', &p)
    }
    fn commit(end: u64) -> Vec<u8> {
        let mut p = vec![0];
        p.extend_from_slice(&end.to_be_bytes());
        p.extend_from_slice(&end.to_be_bytes());
        p.extend_from_slice(&0i64.to_be_bytes());
        frame(b'C', &p)
    }
    fn relation() -> (Vec<u8>, RelationContract) {
        let mut p = Vec::new();
        p.extend_from_slice(&7u32.to_be_bytes());
        p.extend_from_slice(b"public\0items\0");
        p.push(b'd');
        p.extend_from_slice(&1u16.to_be_bytes());
        p.push(1);
        p.extend_from_slice(b"id\0");
        p.extend_from_slice(&20u32.to_be_bytes());
        p.extend_from_slice(&(-1i32).to_be_bytes());
        let relation = crate::m1_decoder::Relation {
            id: 7,
            namespace: "public".into(),
            name: "items".into(),
            replica_identity: b'd',
            columns: vec![crate::m1_decoder::Column {
                key: true,
                name: "id".into(),
                type_oid: 20,
                type_modifier: -1,
            }],
        };
        (
            frame(b'R', &p),
            RelationContract {
                relation,
                key_columns: vec![0],
                control: None,
            },
        )
    }
    fn keepalive(wal: u64, requested: bool) -> Vec<u8> {
        let mut v = vec![b'k'];
        v.extend_from_slice(&wal.to_be_bytes());
        v.extend_from_slice(&0i64.to_be_bytes());
        v.push(u8::from(requested));
        v
    }
    fn store(tag: &str) -> (PathBuf, JournalStore) {
        let p = std::env::temp_dir().join(format!(
            "m2-runtime-{tag}-{}-{}.sqlite",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&p);
        let w = open_writer(&p, "run", 1, 1).unwrap();
        let s = JournalStore::new(
            w,
            SourceIdentity {
                capture_epoch: "epoch-a".into(),
                source_system_id: "sys".into(),
                timeline_id: "timeline".into(),
                database_id: "db".into(),
                slot_name: SLOT.into(),
                publication_fingerprint: "publication".into(),
                protocol_fingerprint: "protocol".into(),
            },
            CommitLimits {
                max_events: 16,
                max_copied_bytes: 1 << 20,
                max_writer_hold: Duration::from_secs(2),
            },
        )
        .unwrap();
        (p, s)
    }
    fn runtime(
        gate: FeedbackPermit,
    ) -> (PathBuf, CaptureRuntime<JournalStore, MemorySpools, Gate>) {
        let (p, s) = store("main");
        let (_, c) = relation();
        (
            p,
            CaptureRuntime::new(s, MemorySpools, Gate(gate), BTreeMap::from([(7, c)]), None),
        )
    }

    #[test]
    fn receive_spool_atomic_journal_then_feedback_and_origin() {
        let (p, mut r) = runtime(FeedbackPermit::AllowSafeBoundary { lsn: 0x20 });
        let (rel, _) = relation();
        r.receive(&rel).unwrap();
        r.receive(&begin(9, 0x20)).unwrap();
        let mut o = Vec::new();
        o.extend_from_slice(&0x10u64.to_be_bytes());
        o.extend_from_slice(b"upstream\0");
        r.receive(&frame(b'O', &o)).unwrap();
        r.receive(&insert(9, 7, b"one")).unwrap();
        r.receive(&insert(9, 7, b"two")).unwrap();
        r.receive(&commit(0x20)).unwrap();
        assert_eq!(r.durable_end_lsn(), Some(0x20));
        assert_eq!(
            r.take_feedback(),
            vec![FeedbackPacket {
                write_lsn: 0x20,
                flush_lsn: 0x20,
                apply_lsn: 0x20,
                reply_requested: false
            }]
        );
        let c = rusqlite::Connection::open(&p).unwrap();
        assert_eq!(
            c.query_row("SELECT count(*) FROM journal_events", [], |x| x
                .get::<_, u64>(0))
                .unwrap(),
            2
        );
        assert!(
            c.query_row(
                "SELECT CAST(payload AS TEXT) FROM journal_events LIMIT 1",
                [],
                |x| x.get::<_, String>(0)
            )
            .unwrap()
            .contains("upstream")
        );
        drop(c);
        let _ = std::fs::remove_file(p);
    }
    #[test]
    fn requested_keepalive_never_copies_server_wal_end_and_gate_holds() {
        let (p, mut r) = runtime(FeedbackPermit::AllowSafeBoundary { lsn: 0x10 });
        r.durable_end_lsn = Some(0x10);
        r.receive(&keepalive(0xFFFF, true)).unwrap();
        assert_eq!(r.take_feedback()[0].write_lsn, 0x10);
        r.gate.0 = FeedbackPermit::Hold;
        r.receive(&keepalive(0xFFFF, true)).unwrap();
        assert!(r.take_feedback().is_empty());
        r.gate.0 = FeedbackPermit::Stale;
        r.receive(&keepalive(0xFFFF, true)).unwrap();
        assert!(r.take_feedback().is_empty());
        let _ = std::fs::remove_file(p);
    }
    #[test]
    fn crash_boundaries_never_create_a_feedback_token() {
        let (_p, mut s) = store("fault");
        let payload = b"x".to_vec();
        let e = JournalEvent {
            event_id: "e".into(),
            transaction_ordinal: 0,
            relation_schema_fingerprint: None,
            control_kind: None,
            payload: payload.clone(),
            payload_hash: crate::m2_journal::sha256(&payload),
        };
        let c = SourceCommit {
            transaction_id: "t".into(),
            xid: "1".into(),
            end_lsn: "0000000000000010".into(),
            payload_checksum: transaction_checksum(std::slice::from_ref(&e)),
            schemas: vec![],
            events: vec![e],
        };
        assert_eq!(
            s.commit_atomic(&c, CommitFault::BeforeSqliteCommit),
            Err(JournalError::FaultBeforeCommit)
        );
        assert_eq!(
            s.commit_atomic(&c, CommitFault::AfterSqliteCommit),
            Err(JournalError::AmbiguousAfterCommit)
        );
        assert!(
            s.commit_atomic(&c, CommitFault::None)
                .unwrap()
                .was_duplicate()
        );
    }
    #[test]
    fn deterministic_safe_stop_shutdown_and_no_in_process_reopen() {
        let (p, mut r) = runtime(FeedbackPermit::AllowSafeBoundary { lsn: u64::MAX });
        assert!(r.receive(b"bad").is_err());
        assert_eq!(r.state(), RuntimeState::CaptureSafeStopped);
        assert!(r.receive(&keepalive(1, true)).is_err());
        assert!(r.take_feedback().is_empty());
        let (p2, mut r2) = runtime(FeedbackPermit::Hold);
        r2.receive(&begin(1, 0x10)).unwrap();
        assert_eq!(
            r2.graceful_shutdown(),
            Err(RuntimeError::ReconciliationRequired)
        );
        assert_eq!(r2.state(), RuntimeState::CaptureSafeStopped);
        assert_eq!(r2.unexpected_eof(), RuntimeError::UnexpectedCopyBothLoss);
        assert_eq!(r2.state(), RuntimeState::OwnershipLost);
        for x in [p, p2] {
            let _ = std::fs::remove_file(x);
        }
    }
    #[test]
    fn persisted_retry_schedule_gates_successor_startup() {
        struct S(Option<u64>);
        impl RetrySchedule for S {
            fn next_retry_at_ms(&self) -> Option<u64> {
                self.0
            }
        }
        assert_eq!(
            reconcile_startup(&S(Some(200)), 100, true, true),
            Err(RuntimeError::RetryNotDue {
                next_retry_at_ms: 200
            })
        );
        assert_eq!(reconcile_startup(&S(Some(200)), 200, true, true), Ok(()));
        assert_eq!(
            reconcile_startup(&S(None), 200, false, true),
            Err(RuntimeError::ReconciliationRequired)
        );
    }
}
