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
            let durable: Option<(String, String, String, String, String, String, String)> = transaction.query_row(
                "SELECT capture_epoch,source_system_id,timeline_id,database_id,slot_name,publication_fingerprint,protocol_fingerprint FROM source_state WHERE singleton=1 AND durable_journal_seq>=?1 AND durable_transaction_end_lsn>=?2",
                params![last, commit.end_lsn],
                |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?)),
            ).optional()?;
            if durable
                != Some((
                    identity.capture_epoch.clone(),
                    identity.source_system_id.clone(),
                    identity.timeline_id.clone(),
                    identity.database_id.clone(),
                    identity.slot_name.clone(),
                    identity.publication_fingerprint.clone(),
                    identity.protocol_fingerprint.clone(),
                ))
            {
                return Err(JournalError::Conflict(
                    "committed transaction is not covered by durable source state",
                ));
            }
            if started.elapsed() > self.limits.max_writer_hold {
                return Err(JournalError::BusyBoundExceeded);
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
        let state: Option<(String,String,String,String,String,String,String,Option<String>)> = transaction.query_row(
            "SELECT capture_epoch,source_system_id,timeline_id,database_id,slot_name,publication_fingerprint,protocol_fingerprint,durable_transaction_end_lsn FROM source_state WHERE singleton=1", [],
            |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?)),
        ).optional()?;
        match state {
            None => {
                transaction.execute("INSERT INTO source_state(singleton,capture_epoch,source_system_id,timeline_id,database_id,slot_name,plugin,publication_fingerprint,protocol_fingerprint,durable_transaction_end_lsn,durable_transaction_id,durable_journal_seq) VALUES(1,?1,?2,?3,?4,?5,'pgoutput',?6,?7,?8,?9,?10)", params![identity.capture_epoch,identity.source_system_id,identity.timeline_id,identity.database_id,identity.slot_name,identity.publication_fingerprint,identity.protocol_fingerprint,commit.end_lsn,commit.transaction_id,last])?;
            }
            Some((epoch, sys, timeline, db, slot, publication, protocol, durable)) => {
                if (epoch, sys, timeline, db, slot, publication, protocol)
                    != (
                        identity.capture_epoch.clone(),
                        identity.source_system_id.clone(),
                        identity.timeline_id.clone(),
                        identity.database_id.clone(),
                        identity.slot_name.clone(),
                        identity.publication_fingerprint.clone(),
                        identity.protocol_fingerprint.clone(),
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
            std::thread::sleep(Duration::from_millis(50));
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
/// Rows stream directly into the result. The byte bound includes the result vector's elements
/// and owned fields plus the one simultaneously-live transaction boundary and SQLite row.
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
        let boundary = reader.query_one_bounded(
            &format!("SELECT last_seq FROM source_transactions WHERE last_seq={after_seq}"),
            |r| r.get::<_, i64>(0),
        )?;
        if boundary != Some(after_seq as i64) {
            return Err(JournalError::Unavailable(
                "requested position is not a complete transaction boundary",
            ));
        }
    }
    let result_slots = max_events
        .checked_mul(std::mem::size_of::<CopiedEvent>())
        .ok_or(JournalError::Limit("copied bytes overflow"))?;
    if result_slots > max_bytes {
        return Err(JournalError::Limit(
            "result vector capacity exceeds byte bound",
        ));
    }
    let mut events = Vec::with_capacity(max_events);
    let mut copied = events
        .capacity()
        .checked_mul(std::mem::size_of::<CopiedEvent>())
        .ok_or(JournalError::Limit("copied bytes overflow"))?;
    let mut expected_first = after_seq as i64 + 1;
    loop {
        let boundary: Option<(String, i64, i64)> = reader.query_one_bounded(
            &format!("SELECT transaction_id,first_seq,last_seq FROM source_transactions WHERE state='committed' AND first_seq>={expected_first} ORDER BY first_seq LIMIT 1"),
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        let Some((txid, first, last)) = boundary else {
            break;
        };
        if first != expected_first {
            return Err(JournalError::Unavailable(
                "requested range is no longer contiguous and retained",
            ));
        }
        let expected = usize::try_from(last - first + 1)
            .map_err(|_| JournalError::Conflict("invalid transaction range"))?;
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
        let measure: Option<(i64, i64, Option<i64>, Option<i64>)> = reader.query_one_bounded(
            &format!("SELECT count(*),coalesce(sum(length(payload)+length(transaction_id)+length(event_id)+length(payload_hash)),0),min(journal_seq),max(journal_seq) FROM journal_events WHERE transaction_id='{escaped}'"),
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?;
        let (count, field_bytes, min_seq, max_seq) =
            measure.ok_or(JournalError::Conflict("missing transaction measures"))?;
        if count as usize != expected || min_seq != Some(first) || max_seq != Some(last) {
            return Err(JournalError::Conflict("incomplete committed transaction"));
        }
        let boundary_bytes = std::mem::size_of::<(String, i64, i64)>()
            .checked_add(txid.len())
            .ok_or(JournalError::Limit("copied bytes overflow"))?;
        let result_bytes = field_bytes as usize;
        let sqlite_row_bytes = std::mem::size_of::<rusqlite::Row<'static>>();
        let peak = copied
            .checked_add(result_bytes)
            .and_then(|v| v.checked_add(boundary_bytes))
            .and_then(|v| v.checked_add(sqlite_row_bytes))
            .ok_or(JournalError::Limit("copied bytes overflow"))?;
        if peak > max_bytes {
            if events.is_empty() {
                return Err(JournalError::Limit(
                    "next complete transaction exceeds byte bound",
                ));
            }
            break;
        }
        let row_count = reader.for_each_bounded(
            &format!("SELECT journal_seq,transaction_id,event_id,payload,payload_hash FROM journal_events WHERE transaction_id='{escaped}' ORDER BY journal_seq"),
            |r| {
                events.push(CopiedEvent {
                    journal_seq: r.get::<_, i64>(0)? as u64,
                    transaction_id: r.get(1)?,
                    event_id: r.get(2)?,
                    payload: r.get(3)?,
                    payload_hash: r.get(4)?,
                });
                Ok(())
            },
        )?;
        if row_count != expected {
            return Err(JournalError::Conflict("incomplete committed transaction"));
        }
        copied = copied
            .checked_add(result_bytes)
            .ok_or(JournalError::Limit("copied bytes overflow"))?;
        expected_first = last + 1;
    }
    if events.is_empty() {
        return Ok(None);
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalInspection {
    pub event_id: String,
    pub transaction_id: String,
    pub journal_seq: u64,
    pub transaction_boundary: (u64, u64),
    pub payload_hash: String,
    pub durable_end_lsn: String,
}

/// Bounded read-only implementation boundary for CMD-JOURNAL-INSPECT-EVENT-ID-ID-EXPLAIN-JSON.
pub fn journal_inspect_event(
    path: &std::path::Path,
    event_id: &str,
    max_age: Duration,
) -> Result<Option<JournalInspection>, JournalError> {
    if event_id.is_empty() {
        return Err(JournalError::Invalid("empty event ID"));
    }
    let escaped = event_id.replace('\'', "''");
    let reader = open_reader_with_limits(path, max_age, 1)?;
    let result = reader.query_one_bounded(
        &format!("SELECT e.event_id,e.transaction_id,e.journal_seq,t.first_seq,t.last_seq,e.payload_hash,t.end_lsn FROM journal_events e JOIN source_transactions t ON t.transaction_id=e.transaction_id WHERE e.event_id='{escaped}' AND t.state='committed'"),
        |r| Ok(JournalInspection { event_id:r.get(0)?, transaction_id:r.get(1)?, journal_seq:r.get::<_,i64>(2)? as u64, transaction_boundary:(r.get::<_,i64>(3)? as u64,r.get::<_,i64>(4)? as u64), payload_hash:r.get(5)?, durable_end_lsn:r.get(6)? }),
    )?;
    drop(reader);
    Ok(result)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalVerification {
    pub transaction_count: u64,
    pub event_count: u64,
    pub durable_seq: u64,
}

/// Bounded read-only implementation boundary for CMD-JOURNAL-VERIFY.
pub fn journal_verify(
    path: &std::path::Path,
    max_events: usize,
    max_age: Duration,
) -> Result<JournalVerification, JournalError> {
    if max_events == 0 {
        return Err(JournalError::Invalid("zero verification event bound"));
    }
    let reader = open_reader_with_limits(path, max_age, max_events)?;
    let quick: Option<String> = reader.query_one_bounded("PRAGMA quick_check", |r| r.get(0))?;
    if quick.as_deref() != Some("ok") {
        return Err(JournalError::Conflict("SQLite quick_check failed"));
    }
    let summary: Option<(i64,i64,i64)> = reader.query_one_bounded(
        "SELECT count(*),coalesce(sum(event_count),0),coalesce((SELECT durable_journal_seq FROM source_state WHERE singleton=1),0) FROM source_transactions WHERE state='committed'",
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)),
    )?;
    let (transactions, expected_events, durable) =
        summary.ok_or(JournalError::Conflict("missing verification summary"))?;
    if expected_events as usize > max_events {
        return Err(JournalError::Limit("verification event bound"));
    }
    let mut global_seq = 1i64;
    let mut seen_events = 0i64;
    let mut seen_transactions = 0i64;
    let mut active: Option<(String, i64, i64, i64, String, Sha256)> = None;
    reader.for_each_bounded(
        "SELECT t.transaction_id,t.first_seq,t.last_seq,t.event_count,t.payload_checksum,e.journal_seq,e.transaction_ordinal,e.payload,e.payload_hash FROM source_transactions t JOIN journal_events e ON e.transaction_id=t.transaction_id WHERE t.state='committed' ORDER BY e.journal_seq",
        |r| {
            let txid:String=r.get(0)?; let first:i64=r.get(1)?; let last:i64=r.get(2)?; let count:i64=r.get(3)?; let checksum:String=r.get(4)?;
            let seq:i64=r.get(5)?; let ordinal:i64=r.get(6)?; let payload:Vec<u8>=r.get(7)?; let payload_hash:String=r.get(8)?;
            if seq!=global_seq || seq<first || seq>last || ordinal!=seq-first || sha256(&payload)!=payload_hash { return Err(rusqlite::Error::InvalidQuery); }
            if active.as_ref().is_none_or(|a|a.0!=txid) {
                if let Some((_,old_first,old_last,old_count,old_checksum,hasher))=active.take() {
                    if old_last-old_first+1!=old_count || format!("{:x}",hasher.finalize())!=old_checksum { return Err(rusqlite::Error::InvalidQuery); }
                }
                if first!=seq || last-first+1!=count { return Err(rusqlite::Error::InvalidQuery); }
                active=Some((txid.clone(),first,last,count,checksum,Sha256::new())); seen_transactions+=1;
            }
            let hasher=&mut active.as_mut().unwrap().5;
            hasher.update((payload_hash.len() as u64).to_be_bytes()); hasher.update(payload_hash.as_bytes());
            global_seq+=1; seen_events+=1; Ok(())
        },
    ).map_err(|_| JournalError::Conflict("journal event sequence, boundary, payload hash, or transaction checksum mismatch"))?;
    if let Some((_, first, last, count, checksum, hasher)) = active.take() {
        if last - first + 1 != count || format!("{:x}", hasher.finalize()) != checksum {
            return Err(JournalError::Conflict("transaction checksum mismatch"));
        }
    }
    if seen_events != expected_events || seen_transactions != transactions || durable != seen_events
    {
        return Err(JournalError::Conflict(
            "journal continuity or durable boundary mismatch",
        ));
    }
    drop(reader);
    Ok(JournalVerification {
        transaction_count: transactions as u64,
        event_count: expected_events as u64,
        durable_seq: durable as u64,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcDryRun {
    pub first_seq: Option<u64>,
    pub last_seq: Option<u64>,
    pub transaction_count: u64,
    pub event_count: u64,
}

/// Inspection-only implementation boundary for CMD-JOURNAL-GC-DRY-RUN; it performs no write.
pub fn journal_gc_dry_run(
    path: &std::path::Path,
    retain_from_seq: u64,
    max_transactions: usize,
    max_age: Duration,
) -> Result<GcDryRun, JournalError> {
    if max_transactions == 0 {
        return Err(JournalError::Invalid("zero GC dry-run bound"));
    }
    let reader = open_reader_with_limits(path, max_age, 1)?;
    let row: Option<(Option<i64>,Option<i64>,i64,i64)> = reader.query_one_bounded(
        &format!("SELECT min(first_seq),max(last_seq),count(*),coalesce(sum(event_count),0) FROM (SELECT first_seq,last_seq,event_count FROM source_transactions WHERE state='committed' AND last_seq<{retain_from_seq} ORDER BY first_seq LIMIT {max_transactions})"),
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)),
    )?;
    let (first, last, transactions, events) =
        row.ok_or(JournalError::Conflict("missing GC dry-run summary"))?;
    drop(reader);
    Ok(GcDryRun {
        first_seq: first.map(|v| v as u64),
        last_seq: last.map(|v| v as u64),
        transaction_count: transactions as u64,
        event_count: events as u64,
    })
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

#[derive(Clone, Debug)]
pub enum ServiceCommand {
    PersistFailureControl {
        failure_id: String,
        fingerprint: String,
        max_writer_hold: Duration,
    },
    CheckpointWal {
        max_writer_hold: Duration,
    },
    GcDryRun {
        retain_from_seq: u64,
        max_transactions: usize,
        max_writer_hold: Duration,
    },
}
impl ServiceCommand {
    fn max_writer_hold(&self) -> Duration {
        match self {
            Self::PersistFailureControl {
                max_writer_hold, ..
            }
            | Self::CheckpointWal { max_writer_hold }
            | Self::GcDryRun {
                max_writer_hold, ..
            } => *max_writer_hold,
        }
    }
    fn execute(self, writer: &mut WriterConnection) -> Result<(), JournalError> {
        let max_writer_hold = self.max_writer_hold();
        if max_writer_hold.is_zero() {
            return Err(JournalError::Invalid("zero service hold bound"));
        }
        let started = Instant::now();
        match self {
            Self::PersistFailureControl {
                failure_id,
                fingerprint,
                ..
            } => {
                writer.connection().execute(
                    "INSERT INTO processing_failures(failure_id,destination_id,component,failure_class,fingerprint,retry_class,attempt,next_retry_at,armed,first_failed_at,last_failed_at) VALUES(?1,NULL,'journal','control',?2,'deterministic',1,NULL,0,'fixture-time','fixture-time')",
                    params![failure_id, fingerprint],
                )?;
            }
            Self::CheckpointWal { .. } => {
                let _: (i64, i64, i64) =
                    writer
                        .connection()
                        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |r| {
                            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                        })?;
            }
            Self::GcDryRun {
                retain_from_seq,
                max_transactions,
                ..
            } => {
                if max_transactions == 0 {
                    return Err(JournalError::Invalid("zero GC dry-run bound"));
                }
                let _: i64 = writer.connection().query_row(
                    &format!("SELECT count(*) FROM (SELECT 1 FROM source_transactions WHERE state='committed' AND last_seq<{retain_from_seq} LIMIT {max_transactions})"),
                    [], |r| r.get(0),
                )?;
            }
        }
        if started.elapsed() > max_writer_hold {
            Err(JournalError::BusyBoundExceeded)
        } else {
            Ok(())
        }
    }
}
enum PendingWork {
    Capture(SourceCommit, CommitFault),
    Service {
        class: WorkClass,
        command: ServiceCommand,
        enqueued_at: Instant,
    },
}
#[derive(Debug)]
pub struct ServiceReport {
    pub class: WorkClass,
    pub queue_wait: Duration,
    pub completed_in: Duration,
}
pub enum WorkOutcome {
    Durable(DurableCommit),
    Serviced(ServiceReport),
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
    pub fn enqueue_service(
        &mut self,
        class: WorkClass,
        command: ServiceCommand,
    ) -> Result<(), EnqueueError> {
        assert!(
            class != WorkClass::Capture,
            "capture work must use enqueue_capture"
        );
        let enqueued_at = Instant::now();
        self.scheduler.enqueue(
            class,
            PendingWork::Service {
                class,
                command,
                enqueued_at,
            },
        )
    }
    pub fn service_next(&mut self) -> Option<Result<WorkOutcome, JournalError>> {
        let (class, work, _tick) = self.scheduler.next()?;
        Some(match work {
            PendingWork::Capture(commit, fault) => self
                .store
                .commit_atomic(&commit, fault)
                .map(WorkOutcome::Durable),
            PendingWork::Service {
                class: expected,
                command,
                enqueued_at,
            } => {
                if expected != class {
                    return Some(Err(JournalError::Conflict("scheduler class mismatch")));
                }
                let queue_wait = enqueued_at.elapsed();
                let started = Instant::now();
                command.execute(&mut self.store.writer).map(|()| {
                    WorkOutcome::Serviced(ServiceReport {
                        class,
                        queue_wait,
                        completed_in: started.elapsed(),
                    })
                })
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
        s.limits.max_writer_hold = Duration::from_nanos(1);
        assert_eq!(
            s.commit_atomic(&c, CommitFault::None),
            Err(JournalError::BusyBoundExceeded)
        );
        s.limits = limits();
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
        s.identity.publication_fingerprint = "changed-publication".into();
        let c3 = commit("tx3", "0000000000000030", vec![event("e3", 0, b"three")]);
        assert!(matches!(
            s.commit_atomic(&c3, CommitFault::None),
            Err(JournalError::Conflict("source state identity mismatch"))
        ));
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
            read_complete_range(&p, 0, 1, 1000, Duration::from_secs(1)),
            Err(JournalError::Limit(_))
        ));
        let r = read_complete_range(&p, 0, 2, 1000, Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!((r.first_seq, r.last_seq), (1, 2));
        assert!(r.copied_bytes > 3);
        let r2 = read_complete_range(&p, 2, 1, 1000, Duration::from_secs(1))
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
            read_complete_range(&p, 2, 2, 1000, Duration::from_secs(1)),
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
                ServiceCommand::PersistFailureControl {
                    failure_id: "failure-1".into(),
                    fingerprint: "fingerprint-1".into(),
                    max_writer_hold: Duration::from_secs(1),
                },
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
            WorkOutcome::Serviced(ServiceReport {
                class: WorkClass::FailureControl,
                ..
            })
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
        s.limits.max_writer_hold = Duration::from_millis(20);
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
    #[test]
    fn saturated_real_writer_service_bounds_slow_capture_and_reserved_work() {
        let (_p, store) = store("saturated-real-service");
        let mut service = JournalWriterService::new(store, [4, 1, 1, 1], 1).unwrap();
        service.store.limits.max_writer_hold = Duration::from_millis(20);
        for i in 1..=4 {
            service
                .enqueue_capture(
                    commit(
                        &format!("tx{i}"),
                        &format!("{i:016X}"),
                        vec![event(&format!("e{i}"), 0, b"payload")],
                    ),
                    if i == 1 {
                        CommitFault::SlowSqliteCommit
                    } else {
                        CommitFault::None
                    },
                )
                .unwrap();
        }
        service
            .enqueue_service(
                WorkClass::FailureControl,
                ServiceCommand::PersistFailureControl {
                    failure_id: "saturated-failure".into(),
                    fingerprint: "saturated-fingerprint".into(),
                    max_writer_hold: Duration::from_secs(1),
                },
            )
            .unwrap();
        service
            .enqueue_service(
                WorkClass::Checkpoint,
                ServiceCommand::CheckpointWal {
                    max_writer_hold: Duration::from_secs(1),
                },
            )
            .unwrap();
        service
            .enqueue_service(
                WorkClass::Gc,
                ServiceCommand::GcDryRun {
                    retain_from_seq: 5,
                    max_transactions: 4,
                    max_writer_hold: Duration::from_secs(1),
                },
            )
            .unwrap();
        assert_eq!(
            service.enqueue_capture(
                commit(
                    "overflow",
                    "0000000000000005",
                    vec![event("overflow", 0, b"x")]
                ),
                CommitFault::None
            ),
            Err(EnqueueError::Overloaded(WorkClass::Capture))
        );
        assert_eq!(
            service.enqueue_service(
                WorkClass::Gc,
                ServiceCommand::GcDryRun {
                    retain_from_seq: 5,
                    max_transactions: 1,
                    max_writer_hold: Duration::from_secs(1)
                }
            ),
            Err(EnqueueError::Overloaded(WorkClass::Gc))
        );
        let started = Instant::now();
        let mut serviced = Vec::new();
        let mut durable = 0;
        let mut ambiguous = 0;
        while let Some(outcome) = service.service_next() {
            match outcome {
                Ok(WorkOutcome::Durable(_)) => durable += 1,
                Ok(WorkOutcome::Serviced(report)) => {
                    assert!(report.queue_wait < Duration::from_secs(1));
                    assert!(report.completed_in < Duration::from_secs(1));
                    serviced.push(report.class);
                }
                Err(JournalError::BusyBoundExceededAfterCommit) => ambiguous += 1,
                Err(other) => panic!("unexpected saturated service outcome: {other:?}"),
            }
        }
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(
            serviced,
            vec![
                WorkClass::FailureControl,
                WorkClass::Checkpoint,
                WorkClass::Gc
            ]
        );
        assert_eq!(durable + ambiguous, 4);
        assert!(ambiguous >= 1);
        assert_eq!(
            service
                .store
                .writer
                .connection()
                .query_row("SELECT count(*) FROM source_transactions", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            4
        );
        assert_eq!(
            service
                .store
                .writer
                .connection()
                .query_row(
                    "SELECT count(*) FROM processing_failures WHERE failure_id='saturated-failure'",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn journal_command_boundaries_are_read_only_bounded_and_asserted() {
        let (p, mut store) = store("commands");
        store
            .commit_atomic(
                &commit("tx1", "0000000000000010", vec![event("event-1", 0, b"one")]),
                CommitFault::None,
            )
            .unwrap();
        store
            .commit_atomic(
                &commit("tx2", "0000000000000020", vec![event("event-2", 0, b"two")]),
                CommitFault::None,
            )
            .unwrap();
        drop(store);
        let before = fs::metadata(&p).unwrap().len();
        let inspect = journal_inspect_event(&p, "event-1", Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!(
            (inspect.event_id.as_str(), inspect.transaction_boundary),
            ("event-1", (1, 1))
        );
        let verify = journal_verify(&p, 8, Duration::from_secs(1)).unwrap();
        assert_eq!(
            (
                verify.transaction_count,
                verify.event_count,
                verify.durable_seq
            ),
            (2, 2, 2)
        );
        let gc = journal_gc_dry_run(&p, 2, 8, Duration::from_secs(1)).unwrap();
        assert_eq!(
            (
                gc.first_seq,
                gc.last_seq,
                gc.transaction_count,
                gc.event_count
            ),
            (Some(1), Some(1), 1, 1)
        );
        assert_eq!(fs::metadata(&p).unwrap().len(), before);
        let corrupt = rusqlite::Connection::open(&p).unwrap();
        corrupt.execute_batch("DROP TRIGGER journal_events_immutable; UPDATE journal_events SET journal_seq=3 WHERE event_id='event-2'").unwrap();
        drop(corrupt);
        assert!(matches!(
            journal_verify(&p, 8, Duration::from_secs(1)),
            Err(JournalError::Conflict(_))
        ));
        assert!(journal_inspect_event(&p, "", Duration::from_secs(1)).is_err());
        assert!(journal_gc_dry_run(&p, 2, 0, Duration::from_secs(1)).is_err());
    }

    #[test]
    fn complete_range_peak_accounts_for_simultaneous_live_data() {
        let (p, mut store) = store("range-peak");
        store
            .commit_atomic(
                &commit(
                    "tx1",
                    "0000000000000010",
                    vec![
                        event("event-1", 0, b"payload"),
                        event("event-2", 1, b"payload-two"),
                        event("event-3", 2, b"payload-three"),
                    ],
                ),
                CommitFault::None,
            )
            .unwrap();
        drop(store);
        let full = read_complete_range(&p, 0, 3, 4096, Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert!(full.copied_bytes > b"payload".len() + std::mem::size_of::<CopiedEvent>());
        assert!(matches!(
            read_complete_range(&p, 0, 3, full.copied_bytes, Duration::from_secs(1)),
            Err(JournalError::Limit(
                "next complete transaction exceeds byte bound"
            ))
        ));
    }
    #[test]
    fn coverage_inventory_is_assertion_aware() {
        let inventory: serde_json::Value =
            serde_json::from_str(include_str!("../contracts/m2/journal-cases.json")).unwrap();
        let cases = inventory["cases"].as_array().unwrap();
        let canonical: serde_json::Value =
            serde_json::from_str(include_str!("../contracts/coverage/plan-to-beads.json")).unwrap();
        let required: std::collections::BTreeSet<_> = canonical["assignments"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|a| a["owner_bead"] == "boring-cdc-m2-journal")
            .map(|a| a["id"].as_str().unwrap())
            .collect();
        let actual: std::collections::BTreeSet<_> =
            cases.iter().map(|c| c["id"].as_str().unwrap()).collect();
        assert!(required.is_subset(&actual));
        for case in cases {
            let assertion = case["assertion"].as_str().unwrap();
            assert!(assertion.len() >= 40);
            match case["status"].as_str().unwrap() {
                "executed" => {
                    assert!(case["test"].is_string());
                    assert!(!assertion.starts_with("provisional:"));
                }
                "provisional_unexecuted" => {
                    assert!(case["test"].is_null());
                    assert!(assertion.starts_with("provisional:"));
                }
                other => panic!("unknown coverage status {other}"),
            }
        }
        let by_id = |id: &str| {
            cases.iter().find(|c| c["id"] == id).unwrap()["test"]
                .as_str()
                .unwrap()
        };
        assert_eq!(
            by_id("CMD-JOURNAL-INSPECT-EVENT-ID-ID-EXPLAIN-JSON"),
            "journal_command_boundaries_are_read_only_bounded_and_asserted"
        );
        assert_eq!(
            by_id("CMD-JOURNAL-VERIFY"),
            "journal_command_boundaries_are_read_only_bounded_and_asserted"
        );
        assert_eq!(
            by_id("CMD-JOURNAL-GC-DRY-RUN"),
            "journal_command_boundaries_are_read_only_bounded_and_asserted"
        );
    }
}
