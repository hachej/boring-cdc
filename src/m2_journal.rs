//! Atomic, capture-priority durable journal commits and bounded copied range reads.

use crate::m2_schema::{WriterConnection, open_reader_with_limits};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::fmt;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceIdentity {
    pub capture_epoch: String,
    pub source_system_id: String,
    pub timeline_id: String,
    pub database_id: String,
    pub slot_name: String,
    pub publication_fingerprint: String,
    pub protocol_fingerprint: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationSchema {
    pub fingerprint: String,
    pub relation_id: String,
    pub canonical_schema: Vec<u8>,
    pub checksum: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalEvent {
    pub event_id: String,
    pub transaction_ordinal: u32,
    pub relation_schema_fingerprint: Option<String>,
    pub control_kind: Option<String>,
    pub payload: Vec<u8>,
    pub payload_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceCommit {
    pub transaction_id: String,
    pub xid: String,
    pub end_lsn: String,
    pub payload_checksum: String,
    pub schemas: Vec<RelationSchema>,
    pub events: Vec<JournalEvent>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableCommit {
    transaction_id: String,
    end_lsn: String,
    first_seq: u64,
    last_seq: u64,
    duplicate: bool,
}

impl DurableCommit {
    pub fn transaction_id(&self) -> &str {
        &self.transaction_id
    }
    pub fn feedback_eligible_end_lsn(&self) -> &str {
        &self.end_lsn
    }
    pub fn first_seq(&self) -> u64 {
        self.first_seq
    }
    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }
    pub fn was_duplicate(&self) -> bool {
        self.duplicate
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitFault {
    None,
    BeforeSqliteCommit,
    AfterSqliteCommit,
    /// Component-only abrupt process termination while the SQLite transaction is open.
    TerminateBeforeSqliteCommit,
    /// Component-only abrupt process termination immediately after SQLite commit returns.
    TerminateAfterSqliteCommit,
    /// Deterministic component hook that delays the SQLite commit syscall path.
    SlowSqliteCommit,
}

#[derive(Debug, Eq, PartialEq)]
pub enum JournalError {
    Invalid(&'static str),
    Conflict(&'static str),
    Limit(&'static str),
    Unavailable(&'static str),
    BusyBoundExceeded,
    BusyBoundExceededAfterCommit,
    FaultBeforeCommit,
    AmbiguousAfterCommit,
    Sqlite(String),
}
impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for JournalError {}
impl From<rusqlite::Error> for JournalError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Sqlite(value.to_string())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CommitLimits {
    pub max_events: usize,
    pub max_copied_bytes: usize,
    pub max_writer_hold: Duration,
}

pub struct JournalStore {
    writer: WriterConnection,
    identity: SourceIdentity,
    limits: CommitLimits,
}

impl JournalStore {
    pub fn new(
        writer: WriterConnection,
        identity: SourceIdentity,
        limits: CommitLimits,
    ) -> Result<Self, JournalError> {
        if limits.max_events == 0
            || limits.max_copied_bytes == 0
            || limits.max_writer_hold.is_zero()
        {
            return Err(JournalError::Invalid("zero journal commit limit"));
        }
        Ok(Self {
            writer,
            identity,
            limits,
        })
    }

    /// Publishes schemas, transaction metadata, events and durable end LSN in one SQLite commit.
    /// The returned value is the only feedback-eligible token; an after-commit fault is ambiguous
    /// and must be reconciled by replaying the same positional transaction.
    fn commit_atomic(
        &mut self,
        commit: &SourceCommit,
        fault: CommitFault,
    ) -> Result<DurableCommit, JournalError> {
        validate_commit(commit, self.limits)?;
        let started = Instant::now();
        let identity = &self.identity;
        let transaction = self
            .writer
            .connection_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

        let existing: Option<(String, String, i64, i64, i64, String)> = transaction.query_row(
            "SELECT transaction_id,xid,first_seq,last_seq,event_count,payload_checksum FROM source_transactions WHERE capture_epoch=?1 AND source_system_id=?2 AND database_id=?3 AND slot_name=?4 AND end_lsn=?5",
            params![identity.capture_epoch, identity.source_system_id, identity.database_id, identity.slot_name, commit.end_lsn],
            |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)),
        ).optional()?;
        if let Some((txid, xid, first, last, count, checksum)) = existing {
            verify_schemas(&transaction, identity, &commit.schemas)?;
            verify_duplicate(
                &transaction,
                commit,
                &txid,
                &xid,
                first,
                last,
                count,
                &checksum,
            )?;
            let durable: Option<(String, String, String, String, String)> = transaction.query_row(
                "SELECT capture_epoch,source_system_id,timeline_id,database_id,slot_name FROM source_state WHERE singleton=1 AND durable_journal_seq>=?1 AND durable_transaction_end_lsn>=?2",
                params![last, commit.end_lsn],
                |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)),
            ).optional()?;
            if durable
                != Some((
                    identity.capture_epoch.clone(),
                    identity.source_system_id.clone(),
                    identity.timeline_id.clone(),
                    identity.database_id.clone(),
                    identity.slot_name.clone(),
                ))
            {
                return Err(JournalError::Conflict(
                    "committed transaction is not covered by durable source state",
                ));
            }
            transaction.rollback()?;
            return Ok(DurableCommit {
                transaction_id: txid,
                end_lsn: commit.end_lsn.clone(),
                first_seq: first as u64,
                last_seq: last as u64,
                duplicate: true,
            });
        }

        if transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM source_transactions WHERE transaction_id=?1)",
            [&commit.transaction_id],
            |r| r.get::<_, bool>(0),
        )? {
            return Err(JournalError::Conflict(
                "transaction ID reused at a different source position",
            ));
        }
        let next: i64 = transaction.query_row(
            "SELECT coalesce(max(last_seq),0)+1 FROM source_transactions",
            [],
            |r| r.get(0),
        )?;
        let last = next
            .checked_add(commit.events.len() as i64 - 1)
            .ok_or(JournalError::Limit("journal sequence overflow"))?;

        for schema in &commit.schemas {
            let row: Option<(String,String,Vec<u8>,String)> = transaction.query_row(
                "SELECT capture_epoch,relation_id,canonical_schema,schema_checksum FROM relation_schemas WHERE schema_fingerprint=?1", [&schema.fingerprint],
                |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)),
            ).optional()?;
            match row {
                Some(existing)
                    if existing
                        == (
                            identity.capture_epoch.clone(),
                            schema.relation_id.clone(),
                            schema.canonical_schema.clone(),
                            schema.checksum.clone(),
                        ) => {}
                Some(_) => {
                    return Err(JournalError::Conflict(
                        "relation schema fingerprint conflict",
                    ));
                }
                None => {
                    transaction.execute(
                        "INSERT INTO relation_schemas VALUES(?1,?2,?3,?4,?5,?6)",
                        params![
                            schema.fingerprint,
                            identity.capture_epoch,
                            schema.relation_id,
                            schema.canonical_schema,
                            schema.checksum,
                            next
                        ],
                    )?;
                }
            }
        }
        transaction.execute(
            "INSERT INTO source_transactions VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,'committed')",
            params![commit.transaction_id,identity.capture_epoch,identity.source_system_id,identity.database_id,identity.slot_name,commit.xid,commit.end_lsn,next,last,commit.events.len() as i64,commit.payload_checksum],
        )?;
        for (offset, event) in commit.events.iter().enumerate() {
            let seq = next + offset as i64;
            transaction
                .execute(
                    "INSERT INTO journal_events VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                    params![
                        seq,
                        event.event_id,
                        commit.transaction_id,
                        event.transaction_ordinal,
                        identity.capture_epoch,
                        event.relation_schema_fingerprint,
                        event.control_kind,
                        event.payload,
                        event.payload_hash
                    ],
                )
                .map_err(|e| {
                    if is_constraint(&e) {
                        JournalError::Conflict("positional event identity conflict")
                    } else {
                        e.into()
                    }
                })?;
        }
        let state: Option<(String,String,String,String,String,Option<String>)> = transaction.query_row(
            "SELECT capture_epoch,source_system_id,timeline_id,database_id,slot_name,durable_transaction_end_lsn FROM source_state WHERE singleton=1", [],
            |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)),
        ).optional()?;
        match state {
            None => {
                transaction.execute("INSERT INTO source_state(singleton,capture_epoch,source_system_id,timeline_id,database_id,slot_name,plugin,publication_fingerprint,protocol_fingerprint,durable_transaction_end_lsn,durable_transaction_id,durable_journal_seq) VALUES(1,?1,?2,?3,?4,?5,'pgoutput',?6,?7,?8,?9,?10)", params![identity.capture_epoch,identity.source_system_id,identity.timeline_id,identity.database_id,identity.slot_name,identity.publication_fingerprint,identity.protocol_fingerprint,commit.end_lsn,commit.transaction_id,last])?;
            }
            Some((epoch, sys, timeline, db, slot, durable)) => {
                if (epoch, sys, timeline, db, slot)
                    != (
                        identity.capture_epoch.clone(),
                        identity.source_system_id.clone(),
                        identity.timeline_id.clone(),
                        identity.database_id.clone(),
                        identity.slot_name.clone(),
                    )
                {
                    return Err(JournalError::Conflict("source state identity mismatch"));
                }
                if durable
                    .as_deref()
                    .is_some_and(|old| old >= commit.end_lsn.as_str())
                {
                    return Err(JournalError::Conflict("non-monotonic durable source end"));
                }
                transaction.execute("UPDATE source_state SET durable_transaction_end_lsn=?1,durable_transaction_id=?2,durable_journal_seq=?3,control_revision=control_revision+1 WHERE singleton=1", params![commit.end_lsn,commit.transaction_id,last])?;
            }
        }
        if started.elapsed() > self.limits.max_writer_hold && fault != CommitFault::SlowSqliteCommit
        {
            return Err(JournalError::BusyBoundExceeded);
        }
        if fault == CommitFault::BeforeSqliteCommit {
            return Err(JournalError::FaultBeforeCommit);
        }
        if fault == CommitFault::TerminateBeforeSqliteCommit {
            std::process::exit(86);
        }
        if fault == CommitFault::SlowSqliteCommit {
            std::thread::sleep(Duration::from_millis(5));
        }
        transaction.commit()?;
        if fault == CommitFault::AfterSqliteCommit {
            return Err(JournalError::AmbiguousAfterCommit);
        }
        if fault == CommitFault::TerminateAfterSqliteCommit {
            std::process::exit(87);
        }
        if started.elapsed() > self.limits.max_writer_hold {
            return Err(JournalError::BusyBoundExceededAfterCommit);
        }
        Ok(DurableCommit {
            transaction_id: commit.transaction_id.clone(),
            end_lsn: commit.end_lsn.clone(),
            first_seq: next as u64,
            last_seq: last as u64,
            duplicate: false,
        })
    }
}

fn is_constraint(error: &rusqlite::Error) -> bool {
    matches!(error, rusqlite::Error::SqliteFailure(e, _) if e.extended_code & 0xff == rusqlite::ffi::SQLITE_CONSTRAINT)
}
fn validate_commit(commit: &SourceCommit, limits: CommitLimits) -> Result<(), JournalError> {
    if commit.events.is_empty() {
        return Err(JournalError::Invalid("empty source transaction"));
    }
    if commit.events.len() > limits.max_events {
        return Err(JournalError::Limit("event count"));
    }
    if commit.end_lsn.len() != 16
        || !commit
            .end_lsn
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_lowercase())
    {
        return Err(JournalError::Invalid("non-canonical end LSN"));
    }
    let mut bytes = 0usize;
    for (index, event) in commit.events.iter().enumerate() {
        if event.transaction_ordinal as usize != index {
            return Err(JournalError::Invalid("non-contiguous transaction ordinal"));
        }
        if event
            .control_kind
            .as_deref()
            .is_some_and(|v| v != "heartbeat" && v != "capture_fence")
        {
            return Err(JournalError::Invalid("unsupported control kind"));
        }
        if sha256(&event.payload) != event.payload_hash {
            return Err(JournalError::Conflict("payload hash mismatch"));
        }
        bytes = bytes
            .checked_add(event.payload.len())
            .ok_or(JournalError::Limit("copied bytes overflow"))?;
    }
    if bytes > limits.max_copied_bytes {
        return Err(JournalError::Limit("copied bytes"));
    }
    let expected = transaction_checksum(&commit.events);
    if expected != commit.payload_checksum {
        return Err(JournalError::Conflict("transaction checksum mismatch"));
    }
    Ok(())
}
fn verify_schemas(
    tx: &rusqlite::Transaction<'_>,
    identity: &SourceIdentity,
    schemas: &[RelationSchema],
) -> Result<(), JournalError> {
    for schema in schemas {
        let row: Option<(String,String,Vec<u8>,String)> = tx.query_row(
            "SELECT capture_epoch,relation_id,canonical_schema,schema_checksum FROM relation_schemas WHERE schema_fingerprint=?1",
            [&schema.fingerprint], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)),
        ).optional()?;
        if row
            != Some((
                identity.capture_epoch.clone(),
                schema.relation_id.clone(),
                schema.canonical_schema.clone(),
                schema.checksum.clone(),
            ))
        {
            return Err(JournalError::Conflict("duplicate relation schema mismatch"));
        }
    }
    Ok(())
}
fn verify_duplicate(
    tx: &rusqlite::Transaction<'_>,
    commit: &SourceCommit,
    txid: &str,
    xid: &str,
    first: i64,
    last: i64,
    count: i64,
    checksum: &str,
) -> Result<(), JournalError> {
    if txid != commit.transaction_id
        || xid != commit.xid
        || count != commit.events.len() as i64
        || checksum != commit.payload_checksum
        || last - first + 1 != count
    {
        return Err(JournalError::Conflict(
            "source transaction positional conflict",
        ));
    }
    let mut stmt=tx.prepare("SELECT event_id,transaction_ordinal,relation_schema_fingerprint,control_kind,payload,payload_hash FROM journal_events WHERE transaction_id=?1 ORDER BY journal_seq")?;
    let stored = stmt
        .query_map([txid], |r| {
            Ok(JournalEvent {
                event_id: r.get(0)?,
                transaction_ordinal: r.get(1)?,
                relation_schema_fingerprint: r.get(2)?,
                control_kind: r.get(3)?,
                payload: r.get(4)?,
                payload_hash: r.get(5)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if stored != commit.events {
        return Err(JournalError::Conflict(
            "duplicate event metadata or payload conflict",
        ));
    }
    Ok(())
}
pub fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub fn transaction_checksum(events: &[JournalEvent]) -> String {
    let mut h = Sha256::new();
    for event in events {
        h.update((event.payload_hash.len() as u64).to_be_bytes());
        h.update(event.payload_hash.as_bytes());
    }
    format!("{:x}", h.finalize())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopiedEvent {
    pub journal_seq: u64,
    pub transaction_id: String,
    pub event_id: String,
    pub payload: Vec<u8>,
    pub payload_hash: String,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CopiedRange {
    pub events: Vec<CopiedEvent>,
    pub first_seq: u64,
    pub last_seq: u64,
    pub copied_bytes: usize,
}

/// Copies only whole complete transactions and drops the physical read handle before return.
pub fn read_complete_range(
    path: &std::path::Path,
    after_seq: u64,
    max_events: usize,
    max_bytes: usize,
    max_age: Duration,
) -> Result<Option<CopiedRange>, JournalError> {
    if max_events == 0 || max_bytes == 0 {
        return Err(JournalError::Invalid("zero range bound"));
    }
    let reader = open_reader_with_limits(path, max_age, max_events)?;
    if after_seq > 0 {
        let boundary: Vec<i64> = reader.query_bounded(
            &format!("SELECT last_seq FROM source_transactions WHERE last_seq={after_seq}"),
            |r| r.get(0),
        )?;
        if boundary != vec![after_seq as i64] {
            return Err(JournalError::Unavailable(
                "requested position is not a complete transaction boundary",
            ));
        }
    }
    let boundaries: Vec<(String,i64,i64)> = reader.query_bounded(
        &format!("SELECT transaction_id,first_seq,last_seq FROM source_transactions WHERE state='committed' AND first_seq>{after_seq} ORDER BY first_seq LIMIT {max_events}"),
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)),
    )?;
    if boundaries.is_empty() {
        return Ok(None);
    }
    if boundaries[0].1 != after_seq as i64 + 1 {
        return Err(JournalError::Unavailable(
            "requested range is no longer contiguous and retained",
        ));
    }
    let mut events = Vec::new();
    let mut copied = 0usize;
    let mut expected_first = after_seq as i64 + 1;
    for (txid, first, last) in boundaries {
        if first != expected_first {
            return Err(JournalError::Unavailable(
                "internal journal range is not contiguous and retained",
            ));
        }
        let expected = (last - first + 1) as usize;
        if events
            .len()
            .checked_add(expected)
            .ok_or(JournalError::Limit("event count overflow"))?
            > max_events
        {
            if events.is_empty() {
                return Err(JournalError::Limit(
                    "next complete transaction exceeds event bound",
                ));
            }
            break;
        }
        let escaped = txid.replace('\'', "''");
        let measures: Vec<(i64,i64,Option<i64>,Option<i64>)> = reader.query_bounded(
            &format!("SELECT count(*),coalesce(sum(length(payload)),0),min(journal_seq),max(journal_seq) FROM journal_events WHERE transaction_id='{escaped}'"),
            |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)),
        )?;
        let (count, tx_bytes, min_seq, max_seq) = measures
            .into_iter()
            .next()
            .ok_or(JournalError::Conflict("missing transaction measures"))?;
        if count as usize != expected || min_seq != Some(first) || max_seq != Some(last) {
            return Err(JournalError::Conflict("incomplete committed transaction"));
        }
        let next_bytes = copied
            .checked_add(tx_bytes as usize)
            .ok_or(JournalError::Limit("copied bytes overflow"))?;
        if next_bytes > max_bytes {
            if events.is_empty() {
                return Err(JournalError::Limit(
                    "next complete transaction exceeds byte bound",
                ));
            }
            break;
        }
        let rows: Vec<(i64,String,String,Vec<u8>,String)> = reader.query_bounded(
            &format!("SELECT journal_seq,transaction_id,event_id,payload,payload_hash FROM journal_events WHERE transaction_id='{escaped}' ORDER BY journal_seq"),
            |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)),
        )?;
        for (seq, transaction_id, event_id, payload, payload_hash) in rows {
            events.push(CopiedEvent {
                journal_seq: seq as u64,
                transaction_id,
                event_id,
                payload,
                payload_hash,
            });
        }
        copied = next_bytes;
        expected_first = last + 1;
    }
    let first_seq = events[0].journal_seq;
    let last_seq = events.last().unwrap().journal_seq;
    drop(reader);
    Ok(Some(CopiedRange {
        events,
        first_seq,
        last_seq,
        copied_bytes: copied,
    }))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkClass {
    Capture,
    FailureControl,
    Checkpoint,
    Gc,
}
#[derive(Debug, Eq, PartialEq)]
pub enum EnqueueError {
    Overloaded(WorkClass),
}
/// Deterministic fairness model used by the sole writer. Capture gets a bounded burst,
/// then one reserved non-capture opportunity is selected round-robin.
pub struct CapturePriorityScheduler<T> {
    capture: VecDeque<T>,
    service: [VecDeque<T>; 3],
    caps: [usize; 4],
    capture_burst: usize,
    capture_since_service: usize,
    next_service: usize,
    tick: u64,
}
impl<T> CapturePriorityScheduler<T> {
    pub fn new(caps: [usize; 4], capture_burst: usize) -> Result<Self, JournalError> {
        if caps.contains(&0) || capture_burst == 0 {
            return Err(JournalError::Invalid("zero scheduler bound"));
        }
        Ok(Self {
            capture: VecDeque::new(),
            service: std::array::from_fn(|_| VecDeque::new()),
            caps,
            capture_burst,
            capture_since_service: 0,
            next_service: 0,
            tick: 0,
        })
    }
    pub fn enqueue(&mut self, class: WorkClass, item: T) -> Result<(), EnqueueError> {
        let (index, queue) = match class {
            WorkClass::Capture => (0, &mut self.capture),
            WorkClass::FailureControl => (1, &mut self.service[0]),
            WorkClass::Checkpoint => (2, &mut self.service[1]),
            WorkClass::Gc => (3, &mut self.service[2]),
        };
        if queue.len() == self.caps[index] {
            return Err(EnqueueError::Overloaded(class));
        }
        queue.push_back(item);
        Ok(())
    }
    pub fn next(&mut self) -> Option<(WorkClass, T, u64)> {
        self.tick += 1;
        let service_pending = self.service.iter().any(|q| !q.is_empty());
        if !self.capture.is_empty()
            && (!service_pending || self.capture_since_service < self.capture_burst)
        {
            self.capture_since_service += 1;
            return self
                .capture
                .pop_front()
                .map(|x| (WorkClass::Capture, x, self.tick));
        }
        for _ in 0..3 {
            let i = self.next_service;
            self.next_service = (self.next_service + 1) % 3;
            if let Some(x) = self.service[i].pop_front() {
                self.capture_since_service = 0;
                return Some((
                    [
                        WorkClass::FailureControl,
                        WorkClass::Checkpoint,
                        WorkClass::Gc,
                    ][i],
                    x,
                    self.tick,
                ));
            }
        }
        self.capture_since_service = 0;
        self.capture
            .pop_front()
            .map(|x| (WorkClass::Capture, x, self.tick))
    }
}

#[derive(Clone, Copy)]
pub struct ServiceDeadline {
    started: Instant,
    max_hold: Duration,
}
impl ServiceDeadline {
    pub fn exceeded(self) -> bool {
        self.started.elapsed() > self.max_hold
    }
    pub fn check(self) -> Result<(), JournalError> {
        if self.exceeded() {
            Err(JournalError::BusyBoundExceeded)
        } else {
            Ok(())
        }
    }
}
pub trait WriterServiceWork: Send {
    fn max_writer_hold(&self) -> Duration;
    fn execute(
        self: Box<Self>,
        writer: &mut WriterConnection,
        deadline: ServiceDeadline,
    ) -> Result<(), JournalError>;
}
pub struct BoundedWriterWork<F> {
    max_hold: Duration,
    work: F,
}
impl<F> BoundedWriterWork<F> {
    pub fn new(max_hold: Duration, work: F) -> Result<Self, JournalError> {
        if max_hold.is_zero() {
            Err(JournalError::Invalid("zero service hold bound"))
        } else {
            Ok(Self { max_hold, work })
        }
    }
}
impl<F> WriterServiceWork for BoundedWriterWork<F>
where
    F: FnOnce(&mut WriterConnection, ServiceDeadline) -> Result<(), JournalError> + Send,
{
    fn max_writer_hold(&self) -> Duration {
        self.max_hold
    }
    fn execute(
        self: Box<Self>,
        writer: &mut WriterConnection,
        deadline: ServiceDeadline,
    ) -> Result<(), JournalError> {
        (self.work)(writer, deadline)
    }
}
enum PendingWork {
    Capture(SourceCommit, CommitFault),
    Service(WorkClass, Box<dyn WriterServiceWork>),
}
pub enum WorkOutcome {
    Durable(DurableCommit),
    Serviced(WorkClass),
}
/// Sole production writer entry point: every capture commit and sibling writer unit is
/// admitted to one bounded fair queue and executed between complete atomic transactions.
pub struct JournalWriterService {
    store: JournalStore,
    scheduler: CapturePriorityScheduler<PendingWork>,
}
impl JournalWriterService {
    pub fn new(
        store: JournalStore,
        queue_caps: [usize; 4],
        capture_burst: usize,
    ) -> Result<Self, JournalError> {
        Ok(Self {
            store,
            scheduler: CapturePriorityScheduler::new(queue_caps, capture_burst)?,
        })
    }
    pub fn enqueue_capture(
        &mut self,
        commit: SourceCommit,
        fault: CommitFault,
    ) -> Result<(), EnqueueError> {
        self.scheduler
            .enqueue(WorkClass::Capture, PendingWork::Capture(commit, fault))
    }
    pub fn enqueue_service<W>(&mut self, class: WorkClass, work: W) -> Result<(), EnqueueError>
    where
        W: WriterServiceWork + 'static,
    {
        assert!(
            class != WorkClass::Capture,
            "capture work must use enqueue_capture"
        );
        self.scheduler
            .enqueue(class, PendingWork::Service(class, Box::new(work)))
    }
    pub fn service_next(&mut self) -> Option<Result<WorkOutcome, JournalError>> {
        let (class, work, _tick) = self.scheduler.next()?;
        Some(match work {
            PendingWork::Capture(commit, fault) => self
                .store
                .commit_atomic(&commit, fault)
                .map(WorkOutcome::Durable),
            PendingWork::Service(expected, work) => {
                if expected != class {
                    return Some(Err(JournalError::Conflict("scheduler class mismatch")));
                }
                let deadline = ServiceDeadline {
                    started: Instant::now(),
                    max_hold: work.max_writer_hold(),
                };
                match work.execute(&mut self.store.writer, deadline) {
                    Ok(()) => deadline.check().map(|()| WorkOutcome::Serviced(class)),
                    Err(error) => Err(error),
                }
            }
        })
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::m2_schema::open_writer;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    fn path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "m2-journal-{name}-{}-{}.sqlite",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }
    fn identity() -> SourceIdentity {
        SourceIdentity {
            capture_epoch: "epoch".into(),
            source_system_id: "sys".into(),
            timeline_id: "tl".into(),
            database_id: "db".into(),
            slot_name: "slot".into(),
            publication_fingerprint: "pub".into(),
            protocol_fingerprint: "proto".into(),
        }
    }
    fn limits() -> CommitLimits {
        CommitLimits {
            max_events: 8,
            max_copied_bytes: 1024,
            max_writer_hold: Duration::from_secs(2),
        }
    }
    fn event(id: &str, ord: u32, payload: &[u8]) -> JournalEvent {
        JournalEvent {
            event_id: id.into(),
            transaction_ordinal: ord,
            relation_schema_fingerprint: Some("fp".into()),
            control_kind: None,
            payload: payload.into(),
            payload_hash: sha256(payload),
        }
    }
    fn commit(id: &str, lsn: &str, events: Vec<JournalEvent>) -> SourceCommit {
        let sum = transaction_checksum(&events);
        SourceCommit {
            transaction_id: id.into(),
            xid: id.into(),
            end_lsn: lsn.into(),
            payload_checksum: sum,
            schemas: vec![RelationSchema {
                fingerprint: "fp".into(),
                relation_id: "rel".into(),
                canonical_schema: b"schema".into(),
                checksum: "schema-sum".into(),
            }],
            events,
        }
    }
    fn store(name: &str) -> (std::path::PathBuf, JournalStore) {
        let p = path(name);
        let w = open_writer(&p, "run", 1, 0).unwrap();
        (p, JournalStore::new(w, identity(), limits()).unwrap())
    }
    #[test]
    fn atomic_commit_publishes_complete_transaction_and_durable_end() {
        let (_p, mut s) = store("atomic");
        let c = commit(
            "tx1",
            "0000000000000010",
            vec![event("e1", 0, b"one"), event("e2", 1, b"two")],
        );
        let d = s.commit_atomic(&c, CommitFault::None).unwrap();
        assert_eq!(
            (d.first_seq(), d.last_seq(), d.feedback_eligible_end_lsn()),
            (1, 2, "0000000000000010")
        );
        let row: (i64, String) = s
            .writer
            .connection()
            .query_row(
                "SELECT count(*),durable_transaction_end_lsn FROM journal_events,source_state",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(row, (2, c.end_lsn));
    }
    #[test]
    fn crash_before_and_after_commit_is_absent_or_complete() {
        let (_p, mut s) = store("crash");
        let c = commit("tx1", "0000000000000010", vec![event("e1", 0, b"one")]);
        assert_eq!(
            s.commit_atomic(&c, CommitFault::BeforeSqliteCommit),
            Err(JournalError::FaultBeforeCommit)
        );
        assert_eq!(
            s.writer
                .connection()
                .query_row("SELECT count(*) FROM source_transactions", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            s.commit_atomic(&c, CommitFault::AfterSqliteCommit),
            Err(JournalError::AmbiguousAfterCommit)
        );
        let d = s.commit_atomic(&c, CommitFault::None).unwrap();
        assert!(d.was_duplicate());
        assert_eq!(d.feedback_eligible_end_lsn(), "0000000000000010");
    }
    #[test]
    fn positional_duplicate_is_idempotent_but_conflict_blocks() {
        let (_p, mut s) = store("dupe");
        let c = commit("tx1", "0000000000000010", vec![event("e1", 0, b"one")]);
        s.commit_atomic(&c, CommitFault::None).unwrap();
        assert!(
            s.commit_atomic(&c, CommitFault::None)
                .unwrap()
                .was_duplicate()
        );
        let mut bad = c.clone();
        bad.events[0].payload = b"other".into();
        bad.events[0].payload_hash = sha256(b"other");
        bad.payload_checksum = transaction_checksum(&bad.events);
        assert!(matches!(
            s.commit_atomic(&bad, CommitFault::None),
            Err(JournalError::Conflict(_))
        ));
    }
    #[test]
    fn invalid_hash_order_lsn_and_limits_fail_before_visibility() {
        let (_p, mut s) = store("invalid");
        let mut c = commit("tx1", "0000000000000010", vec![event("e1", 1, b"one")]);
        assert!(s.commit_atomic(&c, CommitFault::None).is_err());
        c.events[0].transaction_ordinal = 0;
        c.events[0].payload_hash = "bad".into();
        assert!(s.commit_atomic(&c, CommitFault::None).is_err());
        c.events[0].payload_hash = sha256(b"one");
        c.payload_checksum = transaction_checksum(&c.events);
        c.end_lsn = "0/10".into();
        assert!(s.commit_atomic(&c, CommitFault::None).is_err());
        assert_eq!(
            s.writer
                .connection()
                .query_row("SELECT count(*) FROM journal_events", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
    #[test]
    fn relation_schema_conflict_rolls_back_whole_source_commit() {
        let (_p, mut s) = store("schema-conflict");
        let c = commit("tx1", "0000000000000010", vec![event("e1", 0, b"one")]);
        s.commit_atomic(&c, CommitFault::None).unwrap();
        let mut c2 = commit("tx2", "0000000000000020", vec![event("e2", 0, b"two")]);
        c2.schemas[0].canonical_schema = b"changed".into();
        assert!(matches!(
            s.commit_atomic(&c2, CommitFault::None),
            Err(JournalError::Conflict(_))
        ));
        assert_eq!(
            s.writer
                .connection()
                .query_row("SELECT count(*) FROM source_transactions", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }
    #[test]
    fn feedback_token_exists_only_after_commit_return() {
        struct Sender {
            sent: Vec<String>,
        }
        impl Sender {
            fn send(&mut self, d: DurableCommit) {
                self.sent.push(d.feedback_eligible_end_lsn().into())
            }
        }
        let (_p, mut s) = store("feedback");
        let c = commit("tx1", "0000000000000010", vec![event("e1", 0, b"one")]);
        let mut sender = Sender { sent: vec![] };
        assert!(
            s.commit_atomic(&c, CommitFault::BeforeSqliteCommit)
                .is_err()
        );
        assert!(sender.sent.is_empty());
        sender.send(s.commit_atomic(&c, CommitFault::None).unwrap());
        assert_eq!(sender.sent, vec![c.end_lsn]);
    }
    #[test]
    fn bounded_range_copies_complete_transactions_and_releases_reader() {
        let (p, mut s) = store("range");
        s.commit_atomic(
            &commit(
                "tx1",
                "0000000000000010",
                vec![event("e1", 0, b"1"), event("e2", 1, b"22")],
            ),
            CommitFault::None,
        )
        .unwrap();
        s.commit_atomic(
            &commit("tx2", "0000000000000020", vec![event("e3", 0, b"333")]),
            CommitFault::None,
        )
        .unwrap();
        s.commit_atomic(
            &commit("tx3", "0000000000000030", vec![event("e4", 0, b"4")]),
            CommitFault::None,
        )
        .unwrap();
        drop(s);
        assert!(matches!(
            read_complete_range(&p, 0, 1, 100, Duration::from_secs(1)),
            Err(JournalError::Limit(_))
        ));
        let r = read_complete_range(&p, 0, 2, 100, Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!((r.first_seq, r.last_seq, r.copied_bytes), (1, 2, 3));
        let r2 = read_complete_range(&p, 2, 1, 3, Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!((r2.first_seq, r2.last_seq), (3, 3));
        assert!(matches!(
            read_complete_range(&p, 1, 2, 100, Duration::from_secs(1)),
            Err(JournalError::Unavailable(_))
        ));
        let w = open_writer(&p, "gc", 2, 2).unwrap();
        w.connection()
            .execute(
                "UPDATE source_transactions SET state='gc_removed' WHERE transaction_id='tx2'",
                [],
            )
            .unwrap();
        drop(w);
        assert!(matches!(
            read_complete_range(&p, 2, 2, 100, Duration::from_secs(1)),
            Err(JournalError::Unavailable(_))
        ));
        fs::remove_file(p).ok();
    }
    #[test]
    fn capture_priority_reserves_bounded_service_and_overload() {
        let mut q = CapturePriorityScheduler::new([8, 1, 1, 1], 2).unwrap();
        for i in 0..6 {
            q.enqueue(WorkClass::Capture, i).unwrap();
        }
        q.enqueue(WorkClass::FailureControl, 10).unwrap();
        q.enqueue(WorkClass::Checkpoint, 11).unwrap();
        q.enqueue(WorkClass::Gc, 12).unwrap();
        assert_eq!(
            q.enqueue(WorkClass::Gc, 13),
            Err(EnqueueError::Overloaded(WorkClass::Gc))
        );
        let classes: Vec<_> = (0..9).map(|_| q.next().unwrap().0).collect();
        assert_eq!(
            &classes[..3],
            &[
                WorkClass::Capture,
                WorkClass::Capture,
                WorkClass::FailureControl
            ]
        );
        assert!(
            classes
                .iter()
                .position(|x| *x == WorkClass::Checkpoint)
                .unwrap()
                <= 5
        );
        assert!(classes.iter().position(|x| *x == WorkClass::Gc).unwrap() <= 8);
    }
    #[test]
    fn writer_service_serializes_real_capture_and_reserved_work() {
        let (_p, store) = store("writer-service");
        let mut service = JournalWriterService::new(store, [4, 2, 2, 2], 1).unwrap();
        service
            .enqueue_capture(
                commit("tx1", "0000000000000010", vec![event("e1", 0, b"one")]),
                CommitFault::None,
            )
            .unwrap();
        service
            .enqueue_service(
                WorkClass::FailureControl,
                BoundedWriterWork::new(
                    Duration::from_secs(1),
                    |writer: &mut WriterConnection, deadline: ServiceDeadline| {
                        writer.connection().query_row("SELECT 1", [], |_| Ok(()))?;
                        deadline.check()
                    },
                )
                .unwrap(),
            )
            .unwrap();
        service
            .enqueue_capture(
                commit("tx2", "0000000000000020", vec![event("e2", 0, b"two")]),
                CommitFault::None,
            )
            .unwrap();
        assert!(matches!(
            service.service_next().unwrap().unwrap(),
            WorkOutcome::Durable(_)
        ));
        assert!(matches!(
            service.service_next().unwrap().unwrap(),
            WorkOutcome::Serviced(WorkClass::FailureControl)
        ));
        assert!(matches!(
            service.service_next().unwrap().unwrap(),
            WorkOutcome::Durable(_)
        ));
    }
    #[test]
    fn slow_storage_hold_bound_rolls_back_without_partial_visibility() {
        let (_p, mut s) = store("slow");
        s.limits.max_writer_hold = Duration::from_nanos(1);
        let c = commit("tx1", "0000000000000010", vec![event("e1", 0, b"one")]);
        assert_eq!(
            s.commit_atomic(&c, CommitFault::None),
            Err(JournalError::BusyBoundExceeded)
        );
        assert_eq!(
            s.writer
                .connection()
                .query_row("SELECT count(*) FROM source_transactions", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        s.limits.max_writer_hold = Duration::from_millis(1);
        assert_eq!(
            s.commit_atomic(&c, CommitFault::SlowSqliteCommit),
            Err(JournalError::BusyBoundExceededAfterCommit)
        );
        assert!(
            s.commit_atomic(&c, CommitFault::None)
                .unwrap()
                .was_duplicate()
        );
    }
}
