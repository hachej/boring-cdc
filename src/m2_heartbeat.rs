//! Published heartbeat scheduling, durable capture, and destination-independent no-op routing.
//!
//! A heartbeat is a real one-row PostgreSQL UPDATE. It is decoded and committed like any
//! other source transaction. Only the durable journal result can authorize feedback; keepalive
//! WAL positions and successful SQL execution cannot.

use crate::m2_journal::{
    CopiedEvent, CopiedRange, DurableCommit, JournalError, JournalStore, JournalWriterService,
    SourceCommit,
};
use std::fmt;

pub const OWNER_BEAD: &str = "boring-cdc-m2-heartbeat";
pub const HEARTBEAT_UPDATE_SQL: &str = "UPDATE boring_cdc_control.heartbeat SET nonce = $1, updated_at = clock_timestamp() WHERE id = 'singleton'";
pub const HEARTBEAT_KEY_CHECK_SQL: &str =
    "SELECT id FROM boring_cdc_control.heartbeat WHERE id = 'singleton'";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeartbeatPolicy {
    pub cadence_ms: u64,
    pub initial_retry_ms: u64,
    pub max_retry_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeartbeatCommand {
    pub nonce: u64,
    pub update_sql: &'static str,
    pub key_check_sql: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeartbeatCondition {
    Healthy,
    Degraded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeartbeatStatus {
    pub condition: HeartbeatCondition,
    pub attempt: u32,
    pub next_attempt_ms: u64,
    pub failure_fingerprint: Option<&'static str>,
    pub wal_headroom_bytes: u64,
    pub feedback_advanced: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub enum HeartbeatError {
    Invalid(&'static str),
    SourceUnavailable,
    Journal(JournalError),
    Feedback(&'static str),
    Destination(&'static str),
}
impl fmt::Display for HeartbeatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for HeartbeatError {}

/// Scheduling state for the least-privilege control writer. A successful source UPDATE does not
/// advance feedback; it merely schedules the next heartbeat while pgoutput captures the result.
pub struct HeartbeatWriter {
    policy: HeartbeatPolicy,
    next_nonce: u64,
    attempt: u32,
    next_attempt_ms: u64,
    status: HeartbeatStatus,
}
impl HeartbeatWriter {
    pub fn new(
        policy: HeartbeatPolicy,
        now_ms: u64,
        persisted_nonce: u64,
    ) -> Result<Self, HeartbeatError> {
        if policy.cadence_ms == 0
            || policy.initial_retry_ms == 0
            || policy.initial_retry_ms > policy.max_retry_ms
            || policy.max_retry_ms > policy.cadence_ms
            || persisted_nonce >= i64::MAX as u64
        {
            return Err(HeartbeatError::Invalid("invalid heartbeat policy or nonce"));
        }
        let next_attempt_ms = now_ms.saturating_add(policy.cadence_ms);
        Ok(Self {
            policy,
            next_nonce: persisted_nonce + 1,
            attempt: 0,
            next_attempt_ms,
            status: HeartbeatStatus {
                condition: HeartbeatCondition::Healthy,
                attempt: 0,
                next_attempt_ms,
                failure_fingerprint: None,
                wal_headroom_bytes: 0,
                feedback_advanced: false,
            },
        })
    }

    pub fn due(&self, now_ms: u64) -> Option<HeartbeatCommand> {
        (now_ms >= self.next_attempt_ms).then_some(HeartbeatCommand {
            nonce: self.next_nonce,
            update_sql: HEARTBEAT_UPDATE_SQL,
            key_check_sql: HEARTBEAT_KEY_CHECK_SQL,
        })
    }

    /// Records the bounded SQL result. Both the UPDATE and immutable-key SELECT must observe the
    /// single administration-seeded row. No INSERT/DELETE/key mutation fallback exists.
    pub fn record_success(
        &mut self,
        now_ms: u64,
        command: HeartbeatCommand,
        affected_rows: u64,
        selected_keys: u64,
    ) -> Result<HeartbeatStatus, HeartbeatError> {
        if command.nonce != self.next_nonce
            || command.update_sql != HEARTBEAT_UPDATE_SQL
            || command.key_check_sql != HEARTBEAT_KEY_CHECK_SQL
        {
            return Err(HeartbeatError::Invalid(
                "stale or altered heartbeat command",
            ));
        }
        if affected_rows != 1 || selected_keys != 1 {
            return Err(HeartbeatError::Invalid("heartbeat control row cardinality"));
        }
        self.next_nonce = self
            .next_nonce
            .checked_add(1)
            .filter(|nonce| *nonce <= i64::MAX as u64)
            .ok_or(HeartbeatError::Invalid("heartbeat nonce exhausted"))?;
        self.attempt = 0;
        self.next_attempt_ms = now_ms.saturating_add(self.policy.cadence_ms);
        self.status = HeartbeatStatus {
            condition: HeartbeatCondition::Healthy,
            attempt: 0,
            next_attempt_ms: self.next_attempt_ms,
            failure_fingerprint: None,
            wal_headroom_bytes: 0,
            feedback_advanced: false,
        };
        Ok(self.status)
    }

    pub fn record_outage(&mut self, now_ms: u64, wal_headroom_bytes: u64) -> HeartbeatStatus {
        self.attempt = self.attempt.saturating_add(1);
        let shift = self.attempt.saturating_sub(1).min(63);
        let delay = self
            .policy
            .initial_retry_ms
            .saturating_mul(1u64 << shift)
            .min(self.policy.max_retry_ms);
        self.next_attempt_ms = now_ms.saturating_add(delay);
        self.status = HeartbeatStatus {
            condition: HeartbeatCondition::Degraded,
            attempt: self.attempt,
            next_attempt_ms: self.next_attempt_ms,
            failure_fingerprint: Some("HEARTBEAT_WRITE_UNAVAILABLE"),
            wal_headroom_bytes,
            feedback_advanced: false,
        };
        self.status
    }

    pub fn status(&self) -> HeartbeatStatus {
        self.status
    }
}

/// Executes one heartbeat through the same pure-Rust PostgreSQL client used by the binary.
/// The generated integer is the only interpolated value; relation, key and columns are fixed.
pub fn publish_once(dsn: &str, nonce: u64) -> Result<(u64, u64), HeartbeatError> {
    if nonce > i64::MAX as u64 {
        return Err(HeartbeatError::Invalid(
            "heartbeat nonce exceeds PostgreSQL bigint",
        ));
    }
    let mut connection = pg_walstream::PgReplicationConnection::connect(dsn)
        .map_err(|_| HeartbeatError::SourceUnavailable)?;
    let update = connection
        .exec(&format!(
            "WITH changed AS (UPDATE boring_cdc_control.heartbeat SET nonce = {nonce}, updated_at = clock_timestamp() WHERE id = 'singleton' RETURNING id) SELECT id FROM changed"
        ))
        .map_err(|_| HeartbeatError::SourceUnavailable)?;
    let selected = connection
        .exec(HEARTBEAT_KEY_CHECK_SQL)
        .map_err(|_| HeartbeatError::SourceUnavailable)?;
    let affected = u64::try_from(update.ntuples())
        .map_err(|_| HeartbeatError::Invalid("heartbeat update cardinality"))?;
    let selected_keys = u64::try_from(selected.ntuples())
        .map_err(|_| HeartbeatError::Invalid("heartbeat key cardinality"))?;
    if affected != 1 || selected_keys != 1 {
        return Err(HeartbeatError::Invalid("heartbeat control row cardinality"));
    }
    Ok((affected, selected_keys))
}

/// Long-lived production scheduler. Source outages degrade this lane and retry with bounded
/// backoff; they never synthesize feedback or terminate the capture stream.
pub struct PublishedHeartbeatLane {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    status: std::sync::Arc<std::sync::Mutex<HeartbeatStatus>>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl PublishedHeartbeatLane {
    pub fn start(
        dsn: String,
        policy: HeartbeatPolicy,
        now_ms: u64,
    ) -> Result<Self, HeartbeatError> {
        let writer = HeartbeatWriter::new(policy, now_ms, now_ms.saturating_sub(1))?;
        let status = std::sync::Arc::new(std::sync::Mutex::new(writer.status()));
        let lane_status = status.clone();
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let lane_stop = stop.clone();
        let worker = std::thread::Builder::new()
            .name("published-heartbeat".into())
            .spawn(move || {
                let mut writer = writer;
                while !lane_stop.load(std::sync::atomic::Ordering::Acquire) {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |value| value.as_millis() as u64);
                    if let Some(command) = writer.due(now) {
                        let next = match publish_once(&dsn, command.nonce) {
                            Ok((affected, selected)) => writer
                                .record_success(now, command, affected, selected)
                                .unwrap_or_else(|_| writer.record_outage(now, 0)),
                            Err(_) => writer.record_outage(now, 0),
                        };
                        if let Ok(mut current) = lane_status.lock() {
                            *current = next;
                        }
                    }
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
            })
            .map_err(|_| HeartbeatError::SourceUnavailable)?;
        Ok(Self {
            stop,
            status,
            worker: Some(worker),
        })
    }
    pub fn status(&self) -> HeartbeatStatus {
        self.status.lock().map_or(
            HeartbeatStatus {
                condition: HeartbeatCondition::Degraded,
                attempt: u32::MAX,
                next_attempt_ms: 0,
                failure_fingerprint: Some("HEARTBEAT_STATUS_UNAVAILABLE"),
                wal_headroom_bytes: 0,
                feedback_advanced: false,
            },
            |status| *status,
        )
    }
}
impl Drop for PublishedHeartbeatLane {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedHeartbeat {
    pub transaction_id: String,
    pub end_lsn: String,
    pub first_seq: u64,
    pub last_seq: u64,
}

pub trait HeartbeatJournal {
    fn commit_heartbeat(
        &mut self,
        commit: &SourceCommit,
    ) -> Result<PersistedHeartbeat, JournalError>;
}
fn persisted(commit: DurableCommit) -> PersistedHeartbeat {
    PersistedHeartbeat {
        transaction_id: commit.transaction_id().to_owned(),
        end_lsn: commit.feedback_eligible_end_lsn().to_owned(),
        first_seq: commit.first_seq(),
        last_seq: commit.last_seq(),
    }
}
impl HeartbeatJournal for JournalStore {
    fn commit_heartbeat(
        &mut self,
        commit: &SourceCommit,
    ) -> Result<PersistedHeartbeat, JournalError> {
        self.commit_atomic(commit, crate::m2_journal::CommitFault::None)
            .map(persisted)
    }
}
impl HeartbeatJournal for JournalWriterService {
    fn commit_heartbeat(
        &mut self,
        commit: &SourceCommit,
    ) -> Result<PersistedHeartbeat, JournalError> {
        crate::m2_capture_runtime::DurableJournal::commit(self, commit).map(persisted)
    }
}

pub trait FeedbackSender {
    fn send_persisted_boundary(
        &mut self,
        boundary: &PersistedHeartbeat,
    ) -> Result<(), &'static str>;
}

/// Commits an exactly-one-event published heartbeat transaction before invoking feedback.
pub fn capture_published_heartbeat<J: HeartbeatJournal, F: FeedbackSender>(
    journal: &mut J,
    feedback: &mut F,
    commit: &SourceCommit,
) -> Result<PersistedHeartbeat, HeartbeatError> {
    if commit.events.len() != 1 || commit.events[0].control_kind.as_deref() != Some("heartbeat") {
        return Err(HeartbeatError::Invalid(
            "not an isolated published heartbeat transaction",
        ));
    }
    let boundary = journal
        .commit_heartbeat(commit)
        .map_err(HeartbeatError::Journal)?;
    if boundary.end_lsn != commit.end_lsn || boundary.first_seq != boundary.last_seq {
        return Err(HeartbeatError::Invalid(
            "durable heartbeat boundary mismatch",
        ));
    }
    feedback
        .send_persisted_boundary(&boundary)
        .map_err(HeartbeatError::Feedback)?;
    Ok(boundary)
}

pub trait TransactionMaterializer {
    fn write_user_transaction(&mut self, events: &[&CopiedEvent]) -> Result<(), &'static str>;
    fn advance_checkpoint(&mut self, complete_transaction_end: u64) -> Result<(), &'static str>;
}

/// Generic destination router. Control events are durable no-ops; a checkpoint advances only
/// after every user event in the complete journal transaction has succeeded.
pub fn route_complete_range<M: TransactionMaterializer>(
    range: &CopiedRange,
    materializer: &mut M,
) -> Result<(), HeartbeatError> {
    if range.events.is_empty()
        || range.events.first().map(|e| e.journal_seq) != Some(range.first_seq)
        || range.events.last().map(|e| e.journal_seq) != Some(range.last_seq)
        || range
            .events
            .windows(2)
            .any(|w| w[1].journal_seq != w[0].journal_seq + 1)
    {
        return Err(HeartbeatError::Invalid("incomplete routed range"));
    }
    let mut start = 0;
    while start < range.events.len() {
        let ordinal = range.events[start].transaction_ordinal;
        let mut end = start + 1;
        while end < range.events.len() && range.events[end].transaction_ordinal == ordinal {
            end += 1;
        }
        let tx = &range.events[start..end];
        if tx
            .iter()
            .any(|event| event.mutation_ordinal as usize >= tx.len())
            || tx
                .iter()
                .enumerate()
                .any(|(i, event)| event.mutation_ordinal as usize != i)
        {
            return Err(HeartbeatError::Invalid("incomplete routed transaction"));
        }
        let user = tx
            .iter()
            .filter(|event| event.control_kind.is_none())
            .collect::<Vec<_>>();
        if !user.is_empty() {
            materializer
                .write_user_transaction(&user)
                .map_err(HeartbeatError::Destination)?;
        }
        materializer
            .advance_checkpoint(tx.last().unwrap().journal_seq)
            .map_err(HeartbeatError::Destination)?;
        start = end;
    }
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::m2_journal::{
        CommitLimits, JournalEvent, SourceIdentity, read_complete_range, sha256,
        transaction_checksum,
    };
    use crate::m2_schema::open_writer;
    use std::time::Duration;

    fn commit() -> SourceCommit {
        let payload = br#"{"control":"heartbeat"}"#.to_vec();
        let event = JournalEvent {
            event_id: "heartbeat-event".into(),
            transaction_ordinal: 0,
            relation_schema_fingerprint: None,
            control_kind: Some("heartbeat".into()),
            payload_hash: sha256(&payload),
            payload,
        };
        SourceCommit {
            transaction_id: "heartbeat-tx".into(),
            xid: "7".into(),
            end_lsn: "0000000000000020".into(),
            payload_checksum: transaction_checksum(std::slice::from_ref(&event)),
            schemas: vec![],
            events: vec![event],
        }
    }

    struct Journal {
        fail: bool,
        committed: bool,
    }
    impl HeartbeatJournal for Journal {
        fn commit_heartbeat(
            &mut self,
            c: &SourceCommit,
        ) -> Result<PersistedHeartbeat, JournalError> {
            if self.fail {
                return Err(JournalError::FaultBeforeCommit);
            }
            self.committed = true;
            Ok(PersistedHeartbeat {
                transaction_id: c.transaction_id.clone(),
                end_lsn: c.end_lsn.clone(),
                first_seq: 4,
                last_seq: 4,
            })
        }
    }
    #[derive(Default)]
    struct Feedback {
        boundaries: Vec<PersistedHeartbeat>,
    }
    impl FeedbackSender for Feedback {
        fn send_persisted_boundary(&mut self, b: &PersistedHeartbeat) -> Result<(), &'static str> {
            self.boundaries.push(b.clone());
            Ok(())
        }
    }

    #[test]
    fn published_heartbeat_commits_before_feedback() {
        let mut journal = Journal {
            fail: false,
            committed: false,
        };
        // Use a shared cell to make the call order observable without manufacturing an LSN.
        use std::cell::Cell;
        struct CellJournal<'a>(&'a Cell<bool>);
        impl HeartbeatJournal for CellJournal<'_> {
            fn commit_heartbeat(
                &mut self,
                c: &SourceCommit,
            ) -> Result<PersistedHeartbeat, JournalError> {
                self.0.set(true);
                Ok(PersistedHeartbeat {
                    transaction_id: c.transaction_id.clone(),
                    end_lsn: c.end_lsn.clone(),
                    first_seq: 1,
                    last_seq: 1,
                })
            }
        }
        struct CellFeedback<'a>(&'a Cell<bool>);
        impl FeedbackSender for CellFeedback<'_> {
            fn send_persisted_boundary(
                &mut self,
                _: &PersistedHeartbeat,
            ) -> Result<(), &'static str> {
                assert!(self.0.get());
                Ok(())
            }
        }
        let committed = Cell::new(false);
        capture_published_heartbeat(
            &mut CellJournal(&committed),
            &mut CellFeedback(&committed),
            &commit(),
        )
        .unwrap();
        assert!(committed.get());
        // Keep ordinary fake implementations covered too.
        let mut feedback = Feedback::default();
        capture_published_heartbeat(&mut journal, &mut feedback, &commit()).unwrap();
        assert_eq!(feedback.boundaries[0].end_lsn, "0000000000000020");
    }

    #[test]
    fn real_journal_preserves_control_route_and_persisted_feedback_boundary() {
        let path = std::env::temp_dir().join(format!(
            "m2-heartbeat-journal-{}-{}.sqlite",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let _ = std::fs::remove_file(&path);
        let writer = open_writer(&path, "heartbeat-run", 1, 1).unwrap();
        let mut journal = JournalStore::new(
            writer,
            SourceIdentity {
                capture_epoch: "heartbeat-epoch".into(),
                source_system_id: "source".into(),
                timeline_id: "timeline".into(),
                database_id: "database".into(),
                slot_name: "slot".into(),
                publication_fingerprint: "publication".into(),
                protocol_fingerprint: "protocol".into(),
            },
            CommitLimits {
                max_events: 4,
                max_copied_bytes: 4096,
                max_writer_hold: Duration::from_secs(2),
            },
        )
        .unwrap();
        let mut feedback = Feedback::default();
        let boundary = capture_published_heartbeat(&mut journal, &mut feedback, &commit()).unwrap();
        assert_eq!(boundary.end_lsn, "0000000000000020");
        let range = read_complete_range(&path, 0, 4, 4096, Duration::from_secs(2))
            .unwrap()
            .unwrap();
        assert_eq!(range.events[0].control_kind.as_deref(), Some("heartbeat"));
        let mut stub = Stub::default();
        route_complete_range(&range, &mut stub).unwrap();
        assert_eq!(stub.writes, 0);
        assert_eq!(stub.checkpoints, vec![1]);
        drop(journal);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn failed_or_non_heartbeat_commit_never_invokes_feedback() {
        let mut feedback = Feedback::default();
        assert!(
            capture_published_heartbeat(
                &mut Journal {
                    fail: true,
                    committed: false
                },
                &mut feedback,
                &commit()
            )
            .is_err()
        );
        assert!(feedback.boundaries.is_empty());
        let mut user = commit();
        user.events[0].control_kind = None;
        assert!(
            capture_published_heartbeat(
                &mut Journal {
                    fail: false,
                    committed: false
                },
                &mut feedback,
                &user
            )
            .is_err()
        );
        assert!(feedback.boundaries.is_empty());
    }

    #[test]
    fn cadence_backoff_cardinality_and_degraded_health_are_bounded() {
        let policy = HeartbeatPolicy {
            cadence_ms: 1_000,
            initial_retry_ms: 100,
            max_retry_ms: 400,
        };
        let mut writer = HeartbeatWriter::new(policy, 0, 9).unwrap();
        assert!(writer.due(999).is_none());
        let command = writer.due(1_000).unwrap();
        assert_eq!(command.nonce, 10);
        let outage = writer.record_outage(1_000, 55);
        assert_eq!(outage.condition, HeartbeatCondition::Degraded);
        assert!(!outage.feedback_advanced);
        assert_eq!(outage.next_attempt_ms, 1_100);
        writer.record_outage(1_100, 44);
        writer.record_outage(1_300, 33);
        assert_eq!(writer.status().next_attempt_ms, 1_700);
        let command = writer.due(1_700).unwrap();
        assert!(writer.record_success(1_700, command, 0, 1).is_err());
        let status = writer.record_success(1_700, command, 1, 1).unwrap();
        assert_eq!(status.condition, HeartbeatCondition::Healthy);
        assert_eq!(status.next_attempt_ms, 2_700);
    }

    #[test]
    fn writer_rejects_policy_nonce_and_altered_command() {
        assert!(
            HeartbeatWriter::new(
                HeartbeatPolicy {
                    cadence_ms: 0,
                    initial_retry_ms: 1,
                    max_retry_ms: 1
                },
                0,
                0
            )
            .is_err()
        );
        assert!(
            HeartbeatWriter::new(
                HeartbeatPolicy {
                    cadence_ms: 10,
                    initial_retry_ms: 11,
                    max_retry_ms: 11
                },
                0,
                0
            )
            .is_err()
        );
        let mut writer = HeartbeatWriter::new(
            HeartbeatPolicy {
                cadence_ms: 10,
                initial_retry_ms: 1,
                max_retry_ms: 5,
            },
            0,
            0,
        )
        .unwrap();
        let mut command = writer.due(10).unwrap();
        command.nonce = 2;
        assert!(writer.record_success(10, command, 1, 1).is_err());
    }

    #[derive(Default)]
    struct Stub {
        writes: usize,
        checkpoints: Vec<u64>,
        fail_write: bool,
    }
    impl TransactionMaterializer for Stub {
        fn write_user_transaction(&mut self, events: &[&CopiedEvent]) -> Result<(), &'static str> {
            if self.fail_write {
                return Err("write_failed");
            }
            self.writes += events.len();
            Ok(())
        }
        fn advance_checkpoint(&mut self, end: u64) -> Result<(), &'static str> {
            self.checkpoints.push(end);
            Ok(())
        }
    }
    fn copied(seq: u64, tx: u64, mutation: u32, control: Option<&str>) -> CopiedEvent {
        CopiedEvent {
            journal_seq: seq,
            transaction_id: format!("t{tx}"),
            transaction_ordinal: tx,
            mutation_ordinal: mutation,
            event_id: format!("e{seq}"),
            relation_schema_fingerprint: None,
            source_relation_id: None,
            control_kind: control.map(str::to_owned),
            payload: vec![seq as u8],
            payload_hash: sha256(&[seq as u8]),
        }
    }

    #[test]
    fn generic_router_noops_complete_control_transaction_and_checkpoints() {
        let range = CopiedRange {
            events: vec![copied(1, 0, 0, Some("heartbeat"))],
            first_seq: 1,
            last_seq: 1,
            copied_bytes: 1,
        };
        let mut stub = Stub::default();
        route_complete_range(&range, &mut stub).unwrap();
        assert_eq!(stub.writes, 0);
        assert_eq!(stub.checkpoints, vec![1]);
    }

    #[test]
    fn generic_router_never_checkpoints_failed_user_transaction() {
        let range = CopiedRange {
            events: vec![copied(1, 0, 0, None), copied(2, 0, 1, Some("heartbeat"))],
            first_seq: 1,
            last_seq: 2,
            copied_bytes: 2,
        };
        let mut stub = Stub {
            fail_write: true,
            ..Default::default()
        };
        assert!(route_complete_range(&range, &mut stub).is_err());
        assert!(stub.checkpoints.is_empty());
    }

    #[test]
    fn generic_router_rejects_gap_and_incomplete_ordinal() {
        let range = CopiedRange {
            events: vec![copied(1, 0, 1, Some("heartbeat"))],
            first_seq: 1,
            last_seq: 1,
            copied_bytes: 1,
        };
        assert!(route_complete_range(&range, &mut Stub::default()).is_err());
        let gap = CopiedRange {
            events: vec![copied(1, 0, 0, None), copied(3, 1, 0, None)],
            first_seq: 1,
            last_seq: 3,
            copied_bytes: 2,
        };
        assert!(route_complete_range(&gap, &mut Stub::default()).is_err());
    }
}
