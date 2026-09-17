//! Persisted keyset backfill planning and bounded worker commits.
//!
//! This module consumes the M2 single-writer and bounded-reader capabilities. It does not own
//! source capture, feedback, schema admission, or destination promotion. Canonical keys are opaque
//! outside this module and are never rendered in status or error text.

use crate::m2_journal::sha256;
use crate::m2_schema::{WriterConnection, open_reader_with_limits};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde::Serialize;
use std::fmt;
use std::path::Path;
use std::time::{Duration, Instant};

pub const OWNER_BEAD: &str = "boring-cdc-m3-planner";
const MAX_KEY_BYTES: usize = 4096;

const INSTALL_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS m3_planner_runs(
 run_id TEXT PRIMARY KEY REFERENCES backfill_runs(run_id), generation_id TEXT NOT NULL UNIQUE REFERENCES backfill_generations(generation_id),
 key_schema_digest TEXT NOT NULL, estimated_rows INTEGER NOT NULL CHECK(estimated_rows>=0), completed_rows INTEGER NOT NULL DEFAULT 0 CHECK(completed_rows>=0),
 completed_bytes INTEGER NOT NULL DEFAULT 0 CHECK(completed_bytes>=0), started_mono_ms INTEGER NOT NULL CHECK(started_mono_ms>=0), next_snapshot_seq INTEGER NOT NULL CHECK(next_snapshot_seq>=0),
 max_concurrency INTEGER NOT NULL CHECK(max_concurrency>0), revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0));
CREATE TABLE IF NOT EXISTS m3_chunk_claims(
 chunk_id TEXT PRIMARY KEY REFERENCES backfill_chunks(chunk_id) ON DELETE CASCADE, generation_id TEXT NOT NULL REFERENCES backfill_generations(generation_id),
 worker_id TEXT NOT NULL, claim_token TEXT NOT NULL UNIQUE, claimed_mono_ms INTEGER NOT NULL, expires_mono_ms INTEGER NOT NULL CHECK(expires_mono_ms>claimed_mono_ms));
CREATE TABLE IF NOT EXISTS m3_snapshot_events(
 generation_id TEXT NOT NULL REFERENCES backfill_generations(generation_id), snapshot_seq INTEGER NOT NULL, chunk_id TEXT NOT NULL REFERENCES backfill_chunks(chunk_id),
 row_ordinal INTEGER NOT NULL CHECK(row_ordinal>=0), canonical_key BLOB NOT NULL, payload BLOB NOT NULL, payload_hash TEXT NOT NULL,
 PRIMARY KEY(generation_id,snapshot_seq), UNIQUE(generation_id,canonical_key), UNIQUE(chunk_id,row_ordinal));
CREATE TABLE IF NOT EXISTS m3_chunk_commits(
 chunk_id TEXT PRIMARY KEY REFERENCES backfill_chunks(chunk_id), generation_id TEXT NOT NULL, row_count INTEGER NOT NULL, byte_count INTEGER NOT NULL,
 elapsed_ms INTEGER NOT NULL, checksum TEXT NOT NULL, committed_mono_ms INTEGER NOT NULL);
CREATE TRIGGER IF NOT EXISTS m3_planner_revision BEFORE UPDATE ON m3_planner_runs
 WHEN NEW.revision!=OLD.revision+1 BEGIN SELECT RAISE(ABORT,'stale m3 planner revision'); END;
"#;

#[derive(Debug)]
pub enum PlannerError {
    Invalid(&'static str),
    Limit(&'static str),
    Conflict(&'static str),
    StaleGeneration,
    Sqlite(rusqlite::Error),
}
impl fmt::Display for PlannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(v) | Self::Limit(v) | Self::Conflict(v) => f.write_str(v),
            Self::StaleGeneration => f.write_str("M3_STALE_GENERATION"),
            Self::Sqlite(_) => f.write_str("M3_PLANNER_STORE_FAILED"),
        }
    }
}
impl std::error::Error for PlannerError {}
impl From<rusqlite::Error> for PlannerError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Sqlite(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyPartType {
    I64,
    Uuid,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeyPart {
    I64(i64),
    Uuid([u8; 16]),
    Null,
}
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct CanonicalKey(Vec<u8>);
impl CanonicalKey {
    pub fn encode(schema: &[KeyPartType], parts: &[KeyPart]) -> Result<Self, PlannerError> {
        if schema.is_empty() || schema.len() != parts.len() {
            return Err(PlannerError::Invalid("M3_KEY_PARTIAL"));
        }
        let mut out = Vec::with_capacity(schema.len() * 17);
        for (kind, part) in schema.iter().zip(parts) {
            match (kind, part) {
                (KeyPartType::I64, KeyPart::I64(v)) => {
                    out.push(1);
                    out.extend_from_slice(&((*v as u64) ^ (1 << 63)).to_be_bytes());
                }
                (KeyPartType::Uuid, KeyPart::Uuid(v)) => {
                    out.push(2);
                    out.extend_from_slice(v);
                }
                (_, KeyPart::Null) => return Err(PlannerError::Invalid("M3_KEY_NULL")),
                _ => return Err(PlannerError::Invalid("M3_KEY_TYPE")),
            }
        }
        if out.len() > MAX_KEY_BYTES {
            return Err(PlannerError::Limit("M3_KEY_BYTES"));
        }
        Ok(Self(out))
    }
    fn from_stored(bytes: Vec<u8>) -> Result<Self, PlannerError> {
        if bytes.is_empty() || bytes.len() > MAX_KEY_BYTES {
            return Err(PlannerError::Conflict("M3_STORED_KEY_INVALID"));
        }
        Ok(Self(bytes))
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyRange {
    pub start: Option<CanonicalKey>,
    pub end: Option<CanonicalKey>,
}
impl KeyRange {
    pub fn contains(&self, key: &CanonicalKey) -> bool {
        self.start.as_ref().is_none_or(|v| key >= v) && self.end.as_ref().is_none_or(|v| key < v)
    }
}

pub fn half_open_ranges(boundaries: &[CanonicalKey]) -> Result<Vec<KeyRange>, PlannerError> {
    if boundaries.windows(2).any(|v| v[0] >= v[1]) {
        return Err(PlannerError::Invalid("M3_BOUNDARIES_NOT_STRICT"));
    }
    let mut ranges = Vec::with_capacity(boundaries.len() + 1);
    let mut start = None;
    for boundary in boundaries {
        ranges.push(KeyRange {
            start,
            end: Some(boundary.clone()),
        });
        start = Some(boundary.clone());
    }
    ranges.push(KeyRange { start, end: None });
    Ok(ranges)
}

#[derive(Clone, Copy, Debug)]
pub struct PlannerLimits {
    pub chunk_rows: usize,
    pub chunk_bytes: usize,
    pub chunk_duration: Duration,
    pub writer_hold: Duration,
    pub concurrency: usize,
    pub source_impact_bytes: usize,
    pub max_rows_per_second: u64,
}
impl PlannerLimits {
    fn validate(self) -> Result<Self, PlannerError> {
        if self.chunk_rows == 0
            || self.chunk_bytes == 0
            || self.chunk_duration.is_zero()
            || self.writer_hold.is_zero()
            || self.concurrency == 0
            || self.source_impact_bytes == 0
            || self.max_rows_per_second == 0
        {
            return Err(PlannerError::Invalid("M3_ZERO_BOUND"));
        }
        if self.chunk_bytes > self.source_impact_bytes {
            return Err(PlannerError::Invalid("M3_SOURCE_BOUND_LT_CHUNK"));
        }
        Ok(self)
    }
}

pub struct PlanInput {
    pub run_id: String,
    pub generation_id: String,
    pub destination_id: String,
    pub capture_epoch: String,
    pub generation: u64,
    pub key_schema_digest: String,
    pub boundaries: Vec<CanonicalKey>,
    pub estimated_rows: u64,
    pub start_seq: u64,
    pub started_mono_ms: u64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkClaim {
    pub chunk_id: String,
    pub generation_id: String,
    pub claim_token: String,
    pub range: KeyRange,
}
#[derive(Clone, Debug)]
pub struct SnapshotRow {
    pub key: CanonicalKey,
    pub payload: Vec<u8>,
}
#[derive(Clone, Debug)]
pub struct ChunkBatch {
    pub rows: Vec<SnapshotRow>,
    pub elapsed: Duration,
    pub observed_source_bytes: usize,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Progress {
    pub run_id: String,
    pub generation: u64,
    pub state: String,
    pub total_chunks: u64,
    pub complete_chunks: u64,
    pub completed_rows: u64,
    pub completed_bytes: u64,
    pub elapsed_ms: u64,
    pub eta_ms: Option<u64>,
}

pub struct PlannerStore {
    writer: WriterConnection,
    limits: PlannerLimits,
}
impl PlannerStore {
    pub fn open(writer: WriterConnection, limits: PlannerLimits) -> Result<Self, PlannerError> {
        let limits = limits.validate()?;
        writer.connection().execute_batch(INSTALL_SQL)?;
        Ok(Self { writer, limits })
    }

    pub fn persist_plan(&mut self, input: &PlanInput) -> Result<usize, PlannerError> {
        if input.run_id.is_empty()
            || input.generation_id.is_empty()
            || input.destination_id.is_empty()
            || input.capture_epoch.is_empty()
            || input.key_schema_digest.is_empty()
            || input.generation == 0
        {
            return Err(PlannerError::Invalid("M3_PLAN_IDENTITY"));
        }
        let ranges = half_open_ranges(&input.boundaries)?;
        let tx = self
            .writer
            .connection_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute("INSERT INTO backfill_runs(run_id,destination_id,capture_epoch,state,revision) VALUES(?1,?2,?3,'running',0)", params![input.run_id,input.destination_id,input.capture_epoch])?;
        tx.execute("INSERT INTO backfill_generations(generation_id,run_id,generation,state) VALUES(?1,?2,?3,'copying')", params![input.generation_id,input.run_id,input.generation])?;
        tx.execute("INSERT INTO m3_planner_runs(run_id,generation_id,key_schema_digest,estimated_rows,started_mono_ms,next_snapshot_seq,max_concurrency) VALUES(?1,?2,?3,?4,?5,?6,?7)", params![input.run_id,input.generation_id,input.key_schema_digest,input.estimated_rows,input.started_mono_ms,input.start_seq,self.limits.concurrency])?;
        for (index, range) in ranges.iter().enumerate() {
            let start = range.start.as_ref().map_or_else(Vec::new, |v| v.0.clone());
            let end = range.end.as_ref().map_or_else(Vec::new, |v| v.0.clone());
            tx.execute("INSERT INTO backfill_chunks(chunk_id,generation_id,range_start,range_end,state) VALUES(?1,?2,?3,?4,'pending')", params![format!("{}:{index:08}",input.generation_id),input.generation_id,start,end])?;
        }
        tx.commit()?;
        Ok(ranges.len())
    }

    pub fn claim_next(
        &mut self,
        generation_id: &str,
        worker_id: &str,
        now_mono_ms: u64,
        lease: Duration,
    ) -> Result<Option<ChunkClaim>, PlannerError> {
        if worker_id.is_empty() || lease.is_zero() {
            return Err(PlannerError::Invalid("M3_CLAIM_INVALID"));
        }
        let expires = now_mono_ms
            .checked_add(lease.as_millis() as u64)
            .ok_or(PlannerError::Limit("M3_TIME_OVERFLOW"))?;
        let tx = self
            .writer
            .connection_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let state: Option<String> = tx
            .query_row(
                "SELECT state FROM backfill_generations WHERE generation_id=?1",
                [generation_id],
                |r| r.get(0),
            )
            .optional()?;
        if state.as_deref() != Some("copying") {
            return Err(PlannerError::StaleGeneration);
        }
        tx.execute(
            "DELETE FROM m3_chunk_claims WHERE generation_id=?1 AND expires_mono_ms<=?2",
            params![generation_id, now_mono_ms],
        )?;
        let active: i64 = tx.query_row(
            "SELECT count(*) FROM m3_chunk_claims WHERE generation_id=?1",
            [generation_id],
            |r| r.get(0),
        )?;
        if active as usize >= self.limits.concurrency {
            tx.rollback()?;
            return Ok(None);
        }
        let row: Option<(String,Vec<u8>,Vec<u8>)> = tx.query_row("SELECT c.chunk_id,c.range_start,c.range_end FROM backfill_chunks c LEFT JOIN m3_chunk_claims q ON q.chunk_id=c.chunk_id WHERE c.generation_id=?1 AND c.state='pending' AND q.chunk_id IS NULL ORDER BY c.chunk_id LIMIT 1",[generation_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        let Some((chunk_id, start, end)) = row else {
            tx.rollback()?;
            return Ok(None);
        };
        let token = format!("{}:{}:{}", generation_id, worker_id, now_mono_ms);
        tx.execute(
            "INSERT INTO m3_chunk_claims VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                chunk_id,
                generation_id,
                worker_id,
                token,
                now_mono_ms,
                expires
            ],
        )?;
        tx.commit()?;
        Ok(Some(ChunkClaim {
            chunk_id,
            generation_id: generation_id.to_owned(),
            claim_token: token,
            range: KeyRange {
                start: if start.is_empty() {
                    None
                } else {
                    Some(CanonicalKey::from_stored(start)?)
                },
                end: if end.is_empty() {
                    None
                } else {
                    Some(CanonicalKey::from_stored(end)?)
                },
            },
        }))
    }

    /// Commits all snapshot events and the chunk completion marker in one capture-priority writer
    /// transaction. A stale generation or claim cannot commit, including after invalidation.
    pub fn commit_chunk(
        &mut self,
        claim: &ChunkClaim,
        batch: &ChunkBatch,
        now_mono_ms: u64,
    ) -> Result<u64, PlannerError> {
        if batch.rows.len() > self.limits.chunk_rows {
            return Err(PlannerError::Limit("M3_CHUNK_ROWS"));
        }
        if batch.elapsed > self.limits.chunk_duration {
            return Err(PlannerError::Limit("M3_CHUNK_TIME"));
        }
        if batch.observed_source_bytes > self.limits.source_impact_bytes {
            return Err(PlannerError::Limit("M3_SOURCE_IMPACT"));
        }
        let payload_bytes = batch.rows.iter().try_fold(0usize, |n, r| {
            n.checked_add(r.payload.len())
                .ok_or(PlannerError::Limit("M3_CHUNK_BYTES"))
        })?;
        if payload_bytes > self.limits.chunk_bytes {
            return Err(PlannerError::Limit("M3_CHUNK_BYTES"));
        }
        let allowed = self
            .limits
            .max_rows_per_second
            .saturating_mul(batch.elapsed.as_millis().max(1) as u64)
            .div_ceil(1000)
            .max(1);
        if batch.rows.len() as u64 > allowed {
            return Err(PlannerError::Limit("M3_RATE_LIMIT"));
        }
        for pair in batch.rows.windows(2) {
            if pair[0].key >= pair[1].key {
                return Err(PlannerError::Invalid("M3_ROWS_NOT_STRICT"));
            }
        }
        if batch.rows.iter().any(|r| !claim.range.contains(&r.key)) {
            return Err(PlannerError::Invalid("M3_ROW_OUTSIDE_RANGE"));
        }
        let started = Instant::now();
        let tx = self
            .writer
            .connection_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let live:Option<(String,i64,i64)>=tx.query_row("SELECT g.state,q.expires_mono_ms,p.next_snapshot_seq FROM backfill_generations g JOIN m3_chunk_claims q ON q.generation_id=g.generation_id JOIN m3_planner_runs p ON p.generation_id=g.generation_id WHERE g.generation_id=?1 AND q.chunk_id=?2 AND q.claim_token=?3",params![claim.generation_id,claim.chunk_id,claim.claim_token],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        let Some((state, expires, next)) = live else {
            return Err(PlannerError::StaleGeneration);
        };
        if state != "copying" || expires < now_mono_ms as i64 {
            return Err(PlannerError::StaleGeneration);
        }
        let mut checksum_material = Vec::new();
        for (ordinal, row) in batch.rows.iter().enumerate() {
            let seq = next
                .checked_add(ordinal as i64 + 1)
                .ok_or(PlannerError::Limit("M3_SEQUENCE_OVERFLOW"))?;
            let hash = sha256(&row.payload);
            checksum_material.extend_from_slice(hash.as_bytes());
            tx.execute(
                "INSERT INTO m3_snapshot_events VALUES(?1,?2,?3,?4,?5,?6,?7)",
                params![
                    claim.generation_id,
                    seq,
                    claim.chunk_id,
                    ordinal,
                    row.key.0,
                    row.payload,
                    hash
                ],
            )?;
        }
        let completed = next
            .checked_add(batch.rows.len() as i64)
            .ok_or(PlannerError::Limit("M3_SEQUENCE_OVERFLOW"))?;
        let checksum = sha256(&checksum_material);
        if started.elapsed() > self.limits.writer_hold {
            return Err(PlannerError::Limit("M3_WRITER_HOLD"));
        }
        let changed=tx.execute("UPDATE backfill_chunks SET state='complete',completed_seq=?2,checksum=?3 WHERE chunk_id=?1 AND generation_id=?4 AND state='pending'",params![claim.chunk_id,completed,checksum,claim.generation_id])?;
        if changed != 1 {
            return Err(PlannerError::StaleGeneration);
        }
        tx.execute(
            "INSERT INTO m3_chunk_commits VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![
                claim.chunk_id,
                claim.generation_id,
                batch.rows.len(),
                payload_bytes,
                batch.elapsed.as_millis() as u64,
                checksum,
                now_mono_ms
            ],
        )?;
        tx.execute(
            "DELETE FROM m3_chunk_claims WHERE chunk_id=?1 AND claim_token=?2",
            params![claim.chunk_id, claim.claim_token],
        )?;
        tx.execute("UPDATE m3_planner_runs SET completed_rows=completed_rows+?2,completed_bytes=completed_bytes+?3,next_snapshot_seq=?4,revision=revision+1 WHERE generation_id=?1",params![claim.generation_id,batch.rows.len(),payload_bytes,completed])?;
        if started.elapsed() > self.limits.writer_hold {
            return Err(PlannerError::Limit("M3_WRITER_HOLD"));
        }
        tx.commit()?;
        Ok(completed as u64)
    }

    /// Hands a fully copied generation to the separately owned capture-fence transition.
    pub fn finish_copy(&mut self, generation_id: &str) -> Result<(), PlannerError> {
        let tx = self
            .writer
            .connection_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let incomplete: i64 = tx.query_row(
            "SELECT count(*) FROM backfill_chunks WHERE generation_id=?1 AND state!='complete'",
            [generation_id],
            |r| r.get(0),
        )?;
        if incomplete != 0 {
            return Err(PlannerError::Conflict("M3_CHUNKS_INCOMPLETE"));
        }
        let changed = tx.execute(
            "UPDATE backfill_generations SET state='fencing' WHERE generation_id=?1 AND state='copying'",
            [generation_id],
        )?;
        if changed != 1 {
            return Err(PlannerError::StaleGeneration);
        }
        tx.commit()?;
        Ok(())
    }

    pub fn invalidate_generation(&mut self, generation_id: &str) -> Result<(), PlannerError> {
        let tx = self
            .writer
            .connection_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed=tx.execute("UPDATE backfill_generations SET state='invalidated' WHERE generation_id=?1 AND state IN('prepared','copying','fencing')",[generation_id])?;
        if changed != 1 {
            return Err(PlannerError::StaleGeneration);
        }
        tx.execute("UPDATE backfill_chunks SET state='invalidated' WHERE generation_id=?1 AND state='pending'",[generation_id])?;
        tx.execute(
            "DELETE FROM m3_chunk_claims WHERE generation_id=?1",
            [generation_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn progress(&self, run_id: &str, now_mono_ms: u64) -> Result<Progress, PlannerError> {
        let row=self.writer.connection().query_row("SELECT g.generation,r.state,count(c.chunk_id),sum(CASE WHEN c.state='complete' THEN 1 ELSE 0 END),p.completed_rows,p.completed_bytes,p.started_mono_ms,p.estimated_rows FROM backfill_runs r JOIN backfill_generations g ON g.run_id=r.run_id JOIN backfill_chunks c ON c.generation_id=g.generation_id JOIN m3_planner_runs p ON p.run_id=r.run_id WHERE r.run_id=?1 GROUP BY g.generation,r.state,p.completed_rows,p.completed_bytes,p.started_mono_ms,p.estimated_rows",[run_id],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,i64>(2)?,r.get::<_,i64>(3)?,r.get::<_,i64>(4)?,r.get::<_,i64>(5)?,r.get::<_,i64>(6)?,r.get::<_,i64>(7)?))).optional()?.ok_or(PlannerError::Conflict("M3_RUN_NOT_FOUND"))?;
        let elapsed = now_mono_ms.saturating_sub(row.6 as u64);
        let remaining = (row.7 as u64).saturating_sub(row.4 as u64);
        let eta = if row.4 > 0 && elapsed > 0 {
            Some(remaining.saturating_mul(elapsed) / (row.4 as u64))
        } else {
            None
        };
        Ok(Progress {
            run_id: run_id.to_owned(),
            generation: row.0 as u64,
            state: row.1,
            total_chunks: row.2 as u64,
            complete_chunks: row.3 as u64,
            completed_rows: row.4 as u64,
            completed_bytes: row.5 as u64,
            elapsed_ms: elapsed,
            eta_ms: eta,
        })
    }
}

/// Reads pending chunk identities through an M2 bounded reader and drops the SQLite snapshot before
/// returning, so callers cannot retain it across PostgreSQL, file, compression, or hash awaits.
pub fn read_pending_chunk_ids(
    path: &Path,
    generation_id: &str,
    max_rows: usize,
    max_age: Duration,
) -> Result<Vec<String>, PlannerError> {
    if generation_id.is_empty() {
        return Err(PlannerError::Invalid("M3_GENERATION_ID"));
    }
    let reader = open_reader_with_limits(path, max_age, max_rows)?;
    let escaped = generation_id.replace('\'', "''");
    let rows=reader.query_bounded(&format!("SELECT chunk_id FROM backfill_chunks WHERE generation_id='{escaped}' AND state='pending' ORDER BY chunk_id"),|r|r.get(0))?;
    drop(reader);
    Ok(rows)
}

/// Builds a PostgreSQL keyset query. Identifiers are restricted rather than quoted from arbitrary
/// input, and OFFSET/ctid cannot be introduced by callers.
pub fn keyset_select_sql(
    table: &str,
    columns: &[&str],
    has_start: bool,
    has_end: bool,
) -> Result<String, PlannerError> {
    fn ident(v: &str) -> bool {
        !v.is_empty()
            && v.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
            && v.as_bytes()[0].is_ascii_alphabetic()
    }
    if !ident(table) || columns.is_empty() || columns.iter().any(|v| !ident(v)) {
        return Err(PlannerError::Invalid("M3_IDENTIFIER"));
    }
    let tuple = format!("({})", columns.join(","));
    let mut next = 1;
    let mut predicates = Vec::new();
    if has_start {
        let p = (0..columns.len())
            .map(|_| {
                let v = format!("${next}");
                next += 1;
                v
            })
            .collect::<Vec<_>>()
            .join(",");
        predicates.push(format!("{tuple} >= ({p})"));
    }
    if has_end {
        let p = (0..columns.len())
            .map(|_| {
                let v = format!("${next}");
                next += 1;
                v
            })
            .collect::<Vec<_>>()
            .join(",");
        predicates.push(format!("{tuple} < ({p})"));
    }
    let where_clause = if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    };
    Ok(format!(
        "SELECT * FROM {table}{where_clause} ORDER BY {} LIMIT ${next}",
        columns.join(",")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m2_schema::open_writer;
    use rand_chacha::{
        ChaCha8Rng,
        rand_core::{RngCore, SeedableRng},
    };
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            let p = std::env::temp_dir().join(format!(
                "m3-planner-{}-{}.db",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            Self(p)
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
            let _ = std::fs::remove_file(self.0.with_extension("db-wal"));
            let _ = std::fs::remove_file(self.0.with_extension("db-shm"));
        }
    }
    fn limits() -> PlannerLimits {
        PlannerLimits {
            chunk_rows: 4,
            chunk_bytes: 1024,
            chunk_duration: Duration::from_secs(1),
            writer_hold: Duration::from_secs(1),
            concurrency: 2,
            source_impact_bytes: 2048,
            max_rows_per_second: 1000,
        }
    }
    fn store() -> (Temp, PlannerStore) {
        let p = Temp::new();
        let w = open_writer(&p.0, "test", 1, 0).unwrap();
        w.connection().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('dest','archive','cfg','epoch',1)",[]).unwrap();
        let s = PlannerStore::open(w, limits()).unwrap();
        (p, s)
    }
    fn key(v: i64) -> CanonicalKey {
        CanonicalKey::encode(&[KeyPartType::I64], &[KeyPart::I64(v)]).unwrap()
    }
    fn plan(s: &mut PlannerStore, b: Vec<CanonicalKey>) {
        s.persist_plan(&PlanInput {
            run_id: "run".into(),
            generation_id: "gen".into(),
            destination_id: "dest".into(),
            capture_epoch: "epoch".into(),
            generation: 1,
            key_schema_digest: "schema".into(),
            boundaries: b,
            estimated_rows: 8,
            start_seq: 10,
            started_mono_ms: 100,
        })
        .unwrap();
    }

    #[test]
    fn canonical_signed_uuid_and_composite_order_property() {
        let mut rng = ChaCha8Rng::seed_from_u64(7);
        let mut values = (0..512).map(|_| rng.next_u64() as i64).collect::<Vec<_>>();
        values.sort();
        let encoded = values.iter().map(|v| key(*v)).collect::<Vec<_>>();
        assert!(encoded.windows(2).all(|v| v[0] <= v[1]));
        let a = CanonicalKey::encode(
            &[KeyPartType::I64, KeyPartType::Uuid],
            &[KeyPart::I64(-1), KeyPart::Uuid([0; 16])],
        )
        .unwrap();
        let b = CanonicalKey::encode(
            &[KeyPartType::I64, KeyPartType::Uuid],
            &[KeyPart::I64(-1), KeyPart::Uuid([1; 16])],
        )
        .unwrap();
        assert!(a < b);
    }
    #[test]
    fn null_partial_and_wrong_type_fail_closed() {
        assert!(CanonicalKey::encode(&[KeyPartType::I64], &[KeyPart::Null]).is_err());
        assert!(
            CanonicalKey::encode(&[KeyPartType::I64, KeyPartType::Uuid], &[KeyPart::I64(1)])
                .is_err()
        );
        assert!(CanonicalKey::encode(&[KeyPartType::I64], &[KeyPart::Uuid([0; 16])]).is_err());
    }
    #[test]
    fn half_open_ranges_cover_sparse_min_max_without_overlap() {
        let points = vec![key(i64::MIN), key(-7), key(0), key(i64::MAX)];
        let ranges = half_open_ranges(&points).unwrap();
        for p in &points {
            assert_eq!(ranges.iter().filter(|r| r.contains(p)).count(), 1)
        }
        assert_eq!(ranges.len(), 5);
        assert!(half_open_ranges(&[key(1), key(1)]).is_err());
        assert_eq!(half_open_ranges(&[]).unwrap().len(), 1);
    }
    #[test]
    fn query_is_keyset_only_and_composite() {
        let q = keyset_select_sql("items", &["tenant_id", "item_id"], true, true).unwrap();
        assert_eq!(
            q,
            "SELECT * FROM items WHERE (tenant_id,item_id) >= ($1,$2) AND (tenant_id,item_id) < ($3,$4) ORDER BY tenant_id,item_id LIMIT $5"
        );
        assert!(!q.to_lowercase().contains("offset"));
        assert!(!q.to_lowercase().contains("ctid"));
        assert!(keyset_select_sql("items;drop", &["id"], false, false).is_err());
    }
    #[test]
    fn persisted_chunks_resume_and_empty_chunk_commits_atomically() {
        let (p, mut s) = store();
        plan(&mut s, vec![key(10)]);
        let c = s
            .claim_next("gen", "w1", 101, Duration::from_secs(2))
            .unwrap()
            .unwrap();
        assert_eq!(
            s.commit_chunk(
                &c,
                &ChunkBatch {
                    rows: vec![],
                    elapsed: Duration::from_millis(1),
                    observed_source_bytes: 0
                },
                102
            )
            .unwrap(),
            10
        );
        drop(s);
        let pending = read_pending_chunk_ids(&p.0, "gen", 10, Duration::from_secs(1)).unwrap();
        assert_eq!(pending.len(), 1);
    }
    #[test]
    fn limits_concurrency_and_stale_generation_are_enforced() {
        let (_p, mut s) = store();
        plan(&mut s, vec![key(10), key(20)]);
        let a = s
            .claim_next("gen", "w1", 101, Duration::from_secs(2))
            .unwrap()
            .unwrap();
        let _b = s
            .claim_next("gen", "w2", 101, Duration::from_secs(2))
            .unwrap()
            .unwrap();
        assert!(
            s.claim_next("gen", "w3", 101, Duration::from_secs(2))
                .unwrap()
                .is_none()
        );
        let too_many = (0..5)
            .map(|v| SnapshotRow {
                key: key(v),
                payload: vec![1],
            })
            .collect();
        assert!(matches!(
            s.commit_chunk(
                &a,
                &ChunkBatch {
                    rows: too_many,
                    elapsed: Duration::from_millis(10),
                    observed_source_bytes: 5
                },
                102
            ),
            Err(PlannerError::Limit("M3_CHUNK_ROWS"))
        ));
        s.invalidate_generation("gen").unwrap();
        assert!(matches!(
            s.commit_chunk(
                &a,
                &ChunkBatch {
                    rows: vec![],
                    elapsed: Duration::from_millis(1),
                    observed_source_bytes: 0
                },
                102
            ),
            Err(PlannerError::StaleGeneration)
        ));
    }
    #[test]
    fn row_byte_time_rate_source_and_range_bounds_are_enforced() {
        let (_p, mut s) = store();
        plan(&mut s, vec![key(10)]);
        let c = s
            .claim_next("gen", "w", 101, Duration::from_secs(2))
            .unwrap()
            .unwrap();
        let one = |k, p| ChunkBatch {
            rows: vec![SnapshotRow {
                key: key(k),
                payload: vec![0; p],
            }],
            elapsed: Duration::from_millis(1),
            observed_source_bytes: p,
        };
        assert!(matches!(
            s.commit_chunk(&c, &one(11, 1), 102),
            Err(PlannerError::Invalid("M3_ROW_OUTSIDE_RANGE"))
        ));
        assert!(matches!(
            s.commit_chunk(&c, &one(1, 1025), 102),
            Err(PlannerError::Limit("M3_CHUNK_BYTES"))
        ));
        let mut slow = one(1, 1);
        slow.elapsed = Duration::from_secs(2);
        assert!(matches!(
            s.commit_chunk(&c, &slow, 102),
            Err(PlannerError::Limit("M3_CHUNK_TIME"))
        ));
        let mut impact = one(1, 1);
        impact.observed_source_bytes = 2049;
        assert!(matches!(
            s.commit_chunk(&c, &impact, 102),
            Err(PlannerError::Limit("M3_SOURCE_IMPACT"))
        ));
    }
    #[test]
    fn progress_eta_is_redacted_and_deterministic() {
        let (_p, mut s) = store();
        plan(&mut s, vec![]);
        let c = s
            .claim_next("gen", "w", 101, Duration::from_secs(2))
            .unwrap()
            .unwrap();
        s.commit_chunk(
            &c,
            &ChunkBatch {
                rows: vec![SnapshotRow {
                    key: key(1),
                    payload: vec![1, 2],
                }],
                elapsed: Duration::from_millis(10),
                observed_source_bytes: 2,
            },
            102,
        )
        .unwrap();
        let p = s.progress("run", 200).unwrap();
        assert_eq!(p.complete_chunks, 1);
        assert_eq!(p.completed_rows, 1);
        assert_eq!(p.eta_ms, Some(700));
        assert!(!serde_json::to_string(&p).unwrap().contains("canonical"));
        s.finish_copy("gen").unwrap();
        assert!(matches!(
            s.claim_next("gen", "late", 201, Duration::from_secs(1)),
            Err(PlannerError::StaleGeneration)
        ));
    }

    #[test]
    fn finish_copy_rejects_incomplete_generation() {
        let (_p, mut s) = store();
        plan(&mut s, vec![key(10)]);
        assert!(matches!(
            s.finish_copy("gen"),
            Err(PlannerError::Conflict("M3_CHUNKS_INCOMPLETE"))
        ));
    }
}
