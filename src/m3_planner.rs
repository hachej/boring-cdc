//! Persisted keyset backfill planning and bounded worker commits.
//!
//! This module consumes the M2 single-writer and bounded-reader capabilities. It does not own
//! source capture, feedback, schema admission, or destination promotion. Canonical keys are opaque
//! outside this module and are never rendered in status or error text.

use crate::m2_journal::sha256;
use crate::m2_schema::{WriterConnection, open_reader_with_limits};
use crate::m3_bootstrap::ImportedSnapshotSession;
use pg_walstream::PgReplicationConnection;
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde::Serialize;
use std::fmt;
use std::path::Path;
use std::time::{Duration, Instant};

pub const OWNER_BEAD: &str = "boring-cdc-m3-planner";
const MAX_KEY_BYTES: usize = 4096;
type ExecutionProofRow = (
    Vec<u8>,
    Vec<u8>,
    Vec<u8>,
    i64,
    String,
    String,
    String,
    String,
    String,
    String,
);
type BootstrapProofRow = (
    i64,
    String,
    i64,
    i64,
    String,
    String,
    String,
    Option<String>,
    String,
);

const INSTALL_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS m3_planner_runs(
 run_id TEXT PRIMARY KEY REFERENCES backfill_runs(run_id), generation_id TEXT NOT NULL UNIQUE REFERENCES backfill_generations(generation_id),
 bootstrap_intent_id TEXT NOT NULL, importer_id TEXT NOT NULL, snapshot_schema_fingerprint TEXT NOT NULL,
 key_schema BLOB NOT NULL, key_schema_digest TEXT NOT NULL, estimated_rows INTEGER NOT NULL CHECK(estimated_rows>=0), completed_rows INTEGER NOT NULL DEFAULT 0 CHECK(completed_rows>=0),
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

    pub fn decode(&self, schema: &[KeyPartType]) -> Result<Vec<KeyPart>, PlannerError> {
        let mut offset = 0;
        let mut parts = Vec::with_capacity(schema.len());
        for kind in schema {
            let width = match kind {
                KeyPartType::I64 => 9,
                KeyPartType::Uuid => 17,
            };
            if self.0.len().saturating_sub(offset) < width {
                return Err(PlannerError::Conflict("M3_STORED_KEY_SCHEMA"));
            }
            let bytes = &self.0[offset..offset + width];
            let part = match kind {
                KeyPartType::I64 if bytes[0] == 1 => {
                    let mut raw = [0u8; 8];
                    raw.copy_from_slice(&bytes[1..]);
                    KeyPart::I64((u64::from_be_bytes(raw) ^ (1 << 63)) as i64)
                }
                KeyPartType::Uuid if bytes[0] == 2 => {
                    let mut raw = [0u8; 16];
                    raw.copy_from_slice(&bytes[1..]);
                    KeyPart::Uuid(raw)
                }
                _ => return Err(PlannerError::Conflict("M3_STORED_KEY_SCHEMA")),
            };
            parts.push(part);
            offset += width;
        }
        if offset != self.0.len() {
            return Err(PlannerError::Conflict("M3_STORED_KEY_SCHEMA"));
        }
        Ok(parts)
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
    pub bootstrap_intent_id: String,
    pub importer_id: String,
    pub snapshot_schema_fingerprint: String,
    pub key_schema: Vec<KeyPartType>,
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
#[derive(Clone, Copy, Debug)]
pub struct ReadBudget {
    pub max_rows: usize,
    pub max_bytes: usize,
    pub max_duration: Duration,
    pub max_source_impact_bytes: usize,
    pub max_rows_per_second: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceBinding {
    importer_id: String,
    assigned_ranges_digest: String,
    schema_fingerprint: String,
}

pub trait BoundedRangeSource {
    fn binding(&self) -> Option<&SourceBinding> {
        None
    }

    /// Reads one complete preplanned range with the supplied hard bounds. Implementations must stop
    /// before exceeding any bound and return `Limit` rather than an incomplete successful range.
    fn read_range(
        &mut self,
        range: &KeyRange,
        key_schema: &[KeyPartType],
        budget: ReadBudget,
    ) -> Result<Vec<SnapshotRow>, PlannerError>;
}

#[derive(Clone, Debug)]
struct ChunkBatch {
    rows: Vec<SnapshotRow>,
    elapsed: Duration,
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
            || input.bootstrap_intent_id.is_empty()
            || input.importer_id.is_empty()
            || input.snapshot_schema_fingerprint.is_empty()
            || input.key_schema.is_empty()
            || input.generation == 0
        {
            return Err(PlannerError::Invalid("M3_PLAN_IDENTITY"));
        }
        for boundary in &input.boundaries {
            boundary.decode(&input.key_schema)?;
        }
        let ranges = half_open_ranges(&input.boundaries)?;
        let schema = input
            .key_schema
            .iter()
            .map(|v| match v {
                KeyPartType::I64 => 1u8,
                KeyPartType::Uuid => 2u8,
            })
            .collect::<Vec<_>>();
        let schema_digest = sha256(&schema);
        let mut assigned = Vec::new();
        for range in &ranges {
            for bound in [&range.start, &range.end] {
                let bytes = bound.as_ref().map_or(&[][..], |v| v.as_bytes());
                assigned.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                assigned.extend_from_slice(bytes);
            }
        }
        let assignment_digest = crate::m3_bootstrap::digest_assignment(&assigned);
        let tx = self
            .writer
            .connection_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let bootstrap:Option<BootstrapProofRow>=tx.query_row(
            "SELECT r.generation,b.capture_epoch,r.start_seq,r.snapshot_promotable,r.guard_liveness,r.state,i.assigned_ranges_digest,i.snapshot_schema_fingerprint,i.state FROM m3_bootstrap_runtime r JOIN bootstrap_intents b USING(intent_id) JOIN m3_bootstrap_importers i USING(intent_id) WHERE r.intent_id=?1 AND i.importer_id=?2",
            params![input.bootstrap_intent_id,input.importer_id],
            |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?))).optional()?;
        let Some(bootstrap) = bootstrap else {
            return Err(PlannerError::Conflict("M3_BOOTSTRAP_PROOF_MISSING"));
        };
        if bootstrap.0 != input.generation as i64
            || bootstrap.1 != input.capture_epoch
            || bootstrap.2 != input.start_seq as i64
            || bootstrap.3 != 1
            || bootstrap.4 != "held"
            || !matches!(
                bootstrap.5.as_str(),
                "exporter_release_permitted" | "exporter_released"
            )
            || bootstrap.6 != assignment_digest
            || bootstrap.7.as_deref() != Some(input.snapshot_schema_fingerprint.as_str())
            || bootstrap.8 != "acknowledged"
        {
            return Err(PlannerError::Conflict("M3_BOOTSTRAP_PROOF_MISMATCH"));
        }
        tx.execute("INSERT INTO backfill_runs(run_id,destination_id,capture_epoch,state,revision) VALUES(?1,?2,?3,'running',0)", params![input.run_id,input.destination_id,input.capture_epoch])?;
        tx.execute("INSERT INTO backfill_generations(generation_id,run_id,generation,state) VALUES(?1,?2,?3,'copying')", params![input.generation_id,input.run_id,input.generation])?;
        tx.execute("INSERT INTO m3_planner_runs(run_id,generation_id,bootstrap_intent_id,importer_id,snapshot_schema_fingerprint,key_schema,key_schema_digest,estimated_rows,started_mono_ms,next_snapshot_seq,max_concurrency) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)", params![input.run_id,input.generation_id,input.bootstrap_intent_id,input.importer_id,input.snapshot_schema_fingerprint,schema,schema_digest,input.estimated_rows,input.started_mono_ms,input.start_seq,self.limits.concurrency])?;
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
        let state: Option<(String,String)> = tx
            .query_row(
                "SELECT g.state,p.importer_id FROM backfill_generations g JOIN m3_planner_runs p USING(generation_id) WHERE g.generation_id=?1",
                [generation_id],
                |r| Ok((r.get(0)?,r.get(1)?)),
            )
            .optional()?;
        if state.as_ref().map(|v| v.0.as_str()) != Some("copying") {
            return Err(PlannerError::StaleGeneration);
        }
        if state.as_ref().map(|v| v.1.as_str()) != Some(worker_id) {
            return Err(PlannerError::Conflict("M3_WORKER_NOT_ASSIGNED"));
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
        let token = format!(
            "{}:{}:{}:{}",
            generation_id, worker_id, chunk_id, now_mono_ms
        );
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

    /// Performs the bounded source read with no SQLite reader alive, then commits its result.
    pub fn execute_claim<S: BoundedRangeSource>(
        &mut self,
        claim: &ChunkClaim,
        source: &mut S,
        now_mono_ms: u64,
    ) -> Result<u64, PlannerError> {
        let persisted: ExecutionProofRow = self.writer.connection().query_row(
            "SELECT p.key_schema,c.range_start,c.range_end,r.snapshot_promotable,r.guard_liveness,r.state,i.state,p.importer_id,i.assigned_ranges_digest,p.snapshot_schema_fingerprint FROM m3_planner_runs p JOIN backfill_chunks c USING(generation_id) JOIN m3_bootstrap_runtime r ON r.intent_id=p.bootstrap_intent_id JOIN m3_bootstrap_importers i ON i.intent_id=p.bootstrap_intent_id AND i.importer_id=p.importer_id WHERE p.generation_id=?1 AND c.chunk_id=?2",
            params![claim.generation_id,claim.chunk_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?,r.get(9)?)))?;
        if persisted.3 != 1
            || persisted.4 != "held"
            || !matches!(
                persisted.5.as_str(),
                "exporter_release_permitted" | "exporter_released"
            )
            || persisted.6 != "acknowledged"
        {
            return Err(PlannerError::StaleGeneration);
        }
        if let Some(binding) = source.binding()
            && (binding.importer_id != persisted.7
                || binding.assigned_ranges_digest != persisted.8
                || binding.schema_fingerprint != persisted.9)
        {
            return Err(PlannerError::Conflict("M3_SOURCE_BINDING_MISMATCH"));
        }
        let schema = persisted
            .0
            .into_iter()
            .map(|v| match v {
                1 => Ok(KeyPartType::I64),
                2 => Ok(KeyPartType::Uuid),
                _ => Err(PlannerError::Conflict("M3_STORED_KEY_SCHEMA")),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let range = KeyRange {
            start: if persisted.1.is_empty() {
                None
            } else {
                Some(CanonicalKey::from_stored(persisted.1)?)
            },
            end: if persisted.2.is_empty() {
                None
            } else {
                Some(CanonicalKey::from_stored(persisted.2)?)
            },
        };
        if range != claim.range {
            return Err(PlannerError::Conflict("M3_CLAIM_RANGE_MISMATCH"));
        }
        range
            .start
            .as_ref()
            .map(|v| v.decode(&schema))
            .transpose()?;
        range.end.as_ref().map(|v| v.decode(&schema)).transpose()?;
        let budget = ReadBudget {
            max_rows: self.limits.chunk_rows,
            max_bytes: self.limits.chunk_bytes,
            max_duration: self.limits.chunk_duration,
            max_source_impact_bytes: self.limits.source_impact_bytes,
            max_rows_per_second: self.limits.max_rows_per_second,
        };
        let started = Instant::now();
        let rows = source.read_range(&range, &schema, budget)?;
        let required = Duration::from_millis(
            (rows.len() as u64)
                .saturating_mul(1000)
                .div_ceil(self.limits.max_rows_per_second),
        );
        if required > self.limits.chunk_duration {
            return Err(PlannerError::Limit("M3_RATE_LIMIT"));
        }
        if started.elapsed() < required {
            std::thread::sleep(required - started.elapsed())
        }
        let elapsed = started.elapsed();
        let completion_now = now_mono_ms
            .checked_add(elapsed.as_millis() as u64)
            .ok_or(PlannerError::Limit("M3_TIME_OVERFLOW"))?;
        self.commit_chunk_measured(claim, &ChunkBatch { rows, elapsed }, completion_now)
    }

    /// Commits all snapshot events and the chunk completion marker in one capture-priority writer
    /// transaction. A stale generation or claim cannot commit, including after invalidation.
    fn commit_chunk_measured(
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
        let payload_bytes = batch.rows.iter().try_fold(0usize, |n, r| {
            n.checked_add(r.payload.len())
                .ok_or(PlannerError::Limit("M3_CHUNK_BYTES"))
        })?;
        if payload_bytes > self.limits.chunk_bytes {
            return Err(PlannerError::Limit("M3_CHUNK_BYTES"));
        }
        let source_bytes = batch.rows.iter().try_fold(0usize, |n, r| {
            n.checked_add(r.key.as_bytes().len())
                .and_then(|v| v.checked_add(r.payload.len()))
                .and_then(|v| v.checked_add(std::mem::size_of::<SnapshotRow>()))
                .ok_or(PlannerError::Limit("M3_SOURCE_IMPACT"))
        })?;
        if source_bytes > self.limits.source_impact_bytes {
            return Err(PlannerError::Limit("M3_SOURCE_IMPACT"));
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
        self.writer
            .connection()
            .busy_timeout(self.limits.writer_hold)?;
        let tx = self
            .writer
            .connection_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        type LiveRow = (
            String,
            i64,
            i64,
            i64,
            String,
            String,
            String,
            Vec<u8>,
            Vec<u8>,
        );
        let live:Option<LiveRow>=tx.query_row("SELECT g.state,q.expires_mono_ms,p.next_snapshot_seq,r.snapshot_promotable,r.guard_liveness,r.state,i.state,c.range_start,c.range_end FROM backfill_generations g JOIN m3_chunk_claims q ON q.generation_id=g.generation_id JOIN m3_planner_runs p ON p.generation_id=g.generation_id JOIN backfill_chunks c ON c.chunk_id=q.chunk_id JOIN m3_bootstrap_runtime r ON r.intent_id=p.bootstrap_intent_id JOIN m3_bootstrap_importers i ON i.intent_id=p.bootstrap_intent_id AND i.importer_id=p.importer_id WHERE g.generation_id=?1 AND q.chunk_id=?2 AND q.claim_token=?3",params![claim.generation_id,claim.chunk_id,claim.claim_token],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?))).optional()?;
        let Some((
            state,
            expires,
            next,
            promotable,
            guard,
            bootstrap,
            importer,
            range_start,
            range_end,
        )) = live
        else {
            return Err(PlannerError::StaleGeneration);
        };
        let claimed_start = claim.range.start.as_ref().map_or(&[][..], |v| v.as_bytes());
        let claimed_end = claim.range.end.as_ref().map_or(&[][..], |v| v.as_bytes());
        if state != "copying"
            || expires <= now_mono_ms as i64
            || promotable != 1
            || guard != "held"
            || !matches!(
                bootstrap.as_str(),
                "exporter_release_permitted" | "exporter_released"
            )
            || importer != "acknowledged"
            || range_start != claimed_start
            || range_end != claimed_end
        {
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
    let mut table_parts = table.split('.');
    let first = table_parts.next().is_some_and(ident);
    let second = table_parts.next();
    let valid_table = first && second.is_none_or(ident) && table_parts.next().is_none();
    if !valid_table || columns.is_empty() || columns.iter().any(|v| !ident(v)) {
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

/// Production source capability wrapping an importer connection which has already executed
/// `SET TRANSACTION SNAPSHOT` through the bootstrap-owned lifecycle.
pub struct PostgresRangeSource {
    connection: PgReplicationConnection,
    binding: SourceBinding,
    table: String,
    key_columns: Vec<String>,
    last_copy_peaks: Option<CopyPeaks>,
    payload_queries: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CopyPeaks {
    metadata: usize,
    payload: usize,
}

fn checked_add_peak(total: &mut usize, value: usize) -> Result<(), PlannerError> {
    *total = total
        .checked_add(value)
        .ok_or(PlannerError::Limit("M3_SOURCE_IMPACT"))?;
    Ok(())
}

fn checked_slots<T>(capacity: usize) -> Result<usize, PlannerError> {
    capacity
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(PlannerError::Limit("M3_SOURCE_IMPACT"))
}

// NativePgResult grows these vectors by Rust's doubling strategy. This is the conservative
// capacity ceiling for a vector populated only with `push`; it is exact at the growth boundaries.
fn pushed_vec_capacity_ceiling(len: usize) -> Result<usize, PlannerError> {
    if len == 0 {
        return Ok(0);
    }
    len.checked_next_power_of_two()
        .map(|capacity| capacity.max(4))
        .ok_or(PlannerError::Limit("M3_SOURCE_IMPACT"))
}

fn native_result_allocation(
    rows: usize,
    columns: usize,
    value_bytes: usize,
    column_name_bytes: usize,
) -> Result<usize, PlannerError> {
    let mut total = std::mem::size_of::<pg_walstream::PgResult>();
    checked_add_peak(
        &mut total,
        checked_slots::<Vec<Option<Vec<u8>>>>(pushed_vec_capacity_ceiling(rows)?)?,
    )?;
    checked_add_peak(
        &mut total,
        checked_slots::<String>(pushed_vec_capacity_ceiling(columns)?)?,
    )?;
    checked_add_peak(&mut total, column_name_bytes)?;
    checked_add_peak(
        &mut total,
        checked_slots::<Option<Vec<u8>>>(
            rows.checked_mul(columns)
                .ok_or(PlannerError::Limit("M3_SOURCE_IMPACT"))?,
        )?,
    )?;
    checked_add_peak(&mut total, value_bytes)?;
    Ok(total)
}

/// Computes the two allocation high-water marks before issuing the payload query. The native
/// result's row/column vectors and value buffers are included; payload hex remains live while all
/// decoded payload vectors are built.
#[derive(Clone, Copy)]
struct ResultFootprint {
    payload_bytes: usize,
    metadata_result_bytes: usize,
    key_text_bytes: usize,
    key_column_name_bytes: usize,
    max_metadata_row_value_bytes: usize,
    max_data_row_value_bytes: usize,
}

fn wire_data_row_allocation(columns: usize, value_bytes: usize) -> Result<usize, PlannerError> {
    let mut total = 1 + 4 + 2;
    checked_add_peak(
        &mut total,
        columns
            .checked_mul(4)
            .ok_or(PlannerError::Limit("M3_SOURCE_IMPACT"))?,
    )?;
    checked_add_peak(&mut total, value_bytes)?;
    Ok(total)
}

fn postgres_copy_peaks(
    expected: &Vec<CanonicalKey>,
    prepared_rows: &Vec<SnapshotRow>,
    payload_lengths: &Vec<usize>,
    schema_parts: usize,
    footprint: ResultFootprint,
) -> Result<CopyPeaks, PlannerError> {
    let ResultFootprint {
        payload_bytes,
        metadata_result_bytes,
        key_text_bytes,
        key_column_name_bytes,
        max_metadata_row_value_bytes,
        max_data_row_value_bytes,
    } = footprint;
    let key_buffers = expected.iter().try_fold(0usize, |mut total, key| {
        checked_add_peak(&mut total, key.0.capacity())?;
        Ok::<usize, PlannerError>(total)
    })?;
    let cloned_key_buffers = prepared_rows.iter().try_fold(0usize, |mut total, row| {
        checked_add_peak(&mut total, row.key.0.capacity())?;
        Ok::<usize, PlannerError>(total)
    })?;
    let payload_buffers = prepared_rows.iter().try_fold(0usize, |mut total, row| {
        checked_add_peak(&mut total, row.payload.capacity())?;
        Ok::<usize, PlannerError>(total)
    })?;
    let expected_vector = checked_slots::<CanonicalKey>(expected.capacity())?;
    let rows_vector = checked_slots::<SnapshotRow>(prepared_rows.capacity())?;
    let lengths_vector = checked_slots::<usize>(payload_lengths.capacity())?;
    let transient_parts = checked_slots::<KeyPart>(schema_parts)?;

    let metadata_columns = schema_parts
        .checked_add(1)
        .ok_or(PlannerError::Limit("M3_SOURCE_IMPACT"))?;
    let metadata_column_names = key_column_name_bytes
        .checked_add("payload_bytes".len())
        .ok_or(PlannerError::Limit("M3_SOURCE_IMPACT"))?;
    let mut metadata = native_result_allocation(
        expected.len(),
        metadata_columns,
        metadata_result_bytes,
        metadata_column_names,
    )?;
    checked_add_peak(
        &mut metadata,
        wire_data_row_allocation(metadata_columns, max_metadata_row_value_bytes)?,
    )?;
    for value in [
        std::mem::size_of::<Vec<CanonicalKey>>(),
        expected_vector,
        key_buffers,
        std::mem::size_of::<Vec<KeyPart>>(),
        transient_parts,
        std::mem::size_of::<Vec<usize>>(),
        lengths_vector,
        std::mem::size_of::<Vec<SnapshotRow>>(),
        rows_vector,
        cloned_key_buffers,
        payload_buffers,
    ] {
        checked_add_peak(&mut metadata, value)?;
    }

    let payload_hex = payload_bytes
        .checked_mul(2)
        .ok_or(PlannerError::Limit("M3_SOURCE_IMPACT"))?;
    let data_result_bytes = key_text_bytes
        .checked_add(payload_hex)
        .ok_or(PlannerError::Limit("M3_SOURCE_IMPACT"))?;
    let data_column_names = key_column_name_bytes
        .checked_add("payload_hex".len())
        .ok_or(PlannerError::Limit("M3_SOURCE_IMPACT"))?;
    let mut payload = native_result_allocation(
        expected.len(),
        metadata_columns,
        data_result_bytes,
        data_column_names,
    )?;
    checked_add_peak(
        &mut payload,
        wire_data_row_allocation(metadata_columns, max_data_row_value_bytes)?,
    )?;
    // The native result already owns the payload hex and key text; add only application buffers.
    for value in [
        std::mem::size_of::<Vec<CanonicalKey>>(),
        expected_vector,
        key_buffers,
        std::mem::size_of::<Vec<SnapshotRow>>(),
        rows_vector,
        cloned_key_buffers,
        payload_buffers,
    ] {
        checked_add_peak(&mut payload, value)?;
    }
    Ok(CopyPeaks { metadata, payload })
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

impl PostgresRangeSource {
    pub fn from_imported(
        session: ImportedSnapshotSession,
        importer_id: &str,
        assigned_ranges_digest: &str,
        schema_fingerprint: &str,
        table: &str,
        key_columns: &[&str],
    ) -> Result<Self, PlannerError> {
        keyset_select_sql(table, key_columns, false, false)?;
        let connection = session
            .into_planner_connection(
                importer_id,
                assigned_ranges_digest,
                schema_fingerprint,
                table,
            )
            .map_err(|_| PlannerError::Conflict("M3_IMPORTER_CAPABILITY_MISMATCH"))?;
        Ok(Self {
            connection,
            binding: SourceBinding {
                importer_id: importer_id.to_owned(),
                assigned_ranges_digest: assigned_ranges_digest.to_owned(),
                schema_fingerprint: schema_fingerprint.to_owned(),
            },
            table: table.to_owned(),
            key_columns: key_columns.iter().map(|v| (*v).to_owned()).collect(),
            last_copy_peaks: None,
            payload_queries: 0,
        })
    }
    pub fn into_inner(self) -> PgReplicationConnection {
        self.connection
    }
}
impl BoundedRangeSource for PostgresRangeSource {
    fn binding(&self) -> Option<&SourceBinding> {
        Some(&self.binding)
    }

    fn read_range(
        &mut self,
        range: &KeyRange,
        schema: &[KeyPartType],
        budget: ReadBudget,
    ) -> Result<Vec<SnapshotRow>, PlannerError> {
        if schema.len() != self.key_columns.len() {
            return Err(PlannerError::Conflict("M3_KEY_SCHEMA_COLUMNS"));
        }
        fn literal(part: KeyPart) -> Result<String, PlannerError> {
            match part {
                KeyPart::I64(v) => Ok(v.to_string()),
                KeyPart::Uuid(v) => {
                    let h = v.iter().map(|b| format!("{b:02x}")).collect::<String>();
                    Ok(format!(
                        "'{}-{}-{}-{}-{}'::uuid",
                        &h[0..8],
                        &h[8..12],
                        &h[12..16],
                        &h[16..20],
                        &h[20..32]
                    ))
                }
                KeyPart::Null => Err(PlannerError::Invalid("M3_KEY_NULL")),
            }
        }
        let tuple = format!("({})", self.key_columns.join(","));
        let render = |key: &CanonicalKey| -> Result<String, PlannerError> {
            Ok(format!(
                "({})",
                key.decode(schema)?
                    .into_iter()
                    .map(literal)
                    .collect::<Result<Vec<_>, _>>()?
                    .join(",")
            ))
        };
        let mut predicates = Vec::new();
        if let Some(v) = &range.start {
            predicates.push(format!("{tuple}>={}", render(v)?));
        }
        if let Some(v) = &range.end {
            predicates.push(format!("{tuple}<{}", render(v)?));
        }
        let where_clause = if predicates.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", predicates.join(" AND "))
        };
        let timeout = budget.max_duration.as_millis().max(1);
        self.connection
            .exec(&format!("SET LOCAL statement_timeout={timeout}"))
            .map_err(|_| PlannerError::Conflict("M3_SOURCE_TIMEOUT_SETUP"))?;
        let keys = self.key_columns.join(",");
        let meta_sql = format!(
            "SELECT {keys},octet_length(convert_to(to_jsonb(t)::text,'UTF8')) AS payload_bytes FROM {} t{where_clause} ORDER BY {keys} LIMIT {}",
            self.table,
            budget.max_rows + 1
        );
        let started = Instant::now();
        let meta = self
            .connection
            .exec(&meta_sql)
            .map_err(|_| PlannerError::Conflict("M3_SOURCE_QUERY_FAILED"))?;
        if meta.ntuples() as usize > budget.max_rows {
            return Err(PlannerError::Limit("M3_CHUNK_ROWS"));
        }
        let row_count = meta.ntuples() as usize;
        let mut expected = Vec::with_capacity(row_count);
        let mut payload_bytes = 0usize;
        let mut payload_lengths = Vec::with_capacity(row_count);
        let mut metadata_result_bytes = 0usize;
        let mut key_text_bytes = 0usize;
        let mut max_metadata_row_value_bytes = 0usize;
        let mut max_data_row_value_bytes = 0usize;
        for row in 0..meta.ntuples() {
            let mut parts = Vec::with_capacity(schema.len());
            let mut row_key_text_bytes = 0usize;
            for (column, kind) in schema.iter().enumerate() {
                let value_bytes = meta
                    .get_bytes(row, column as i32)
                    .ok_or(PlannerError::Conflict("M3_SOURCE_ROW"))?;
                let v = std::str::from_utf8(value_bytes)
                    .map_err(|_| PlannerError::Conflict("M3_SOURCE_ROW"))?;
                metadata_result_bytes = metadata_result_bytes
                    .checked_add(v.len())
                    .ok_or(PlannerError::Limit("M3_SOURCE_IMPACT"))?;
                row_key_text_bytes = row_key_text_bytes
                    .checked_add(v.len())
                    .ok_or(PlannerError::Limit("M3_SOURCE_IMPACT"))?;
                parts.push(match kind {
                    KeyPartType::I64 => KeyPart::I64(
                        v.parse()
                            .map_err(|_| PlannerError::Conflict("M3_SOURCE_ROW"))?,
                    ),
                    KeyPartType::Uuid => {
                        let mut raw = [0u8; 16];
                        let mut hex = v.bytes().filter(|b| *b != b'-');
                        for byte in &mut raw {
                            let hi = hex.next().and_then(hex_nibble);
                            let lo = hex.next().and_then(hex_nibble);
                            *byte = hi
                                .zip(lo)
                                .map(|(hi, lo)| (hi << 4) | lo)
                                .ok_or(PlannerError::Conflict("M3_SOURCE_ROW"))?;
                        }
                        if hex.next().is_some() {
                            return Err(PlannerError::Conflict("M3_SOURCE_ROW"));
                        }
                        KeyPart::Uuid(raw)
                    }
                })
            }
            let key = CanonicalKey::encode(schema, &parts)?;
            let bytes_value = meta
                .get_bytes(row, schema.len() as i32)
                .ok_or(PlannerError::Conflict("M3_SOURCE_ROW"))?;
            let bytes_text = std::str::from_utf8(bytes_value)
                .map_err(|_| PlannerError::Conflict("M3_SOURCE_ROW"))?;
            metadata_result_bytes = metadata_result_bytes
                .checked_add(bytes_text.len())
                .ok_or(PlannerError::Limit("M3_SOURCE_IMPACT"))?;
            key_text_bytes = key_text_bytes
                .checked_add(row_key_text_bytes)
                .ok_or(PlannerError::Limit("M3_SOURCE_IMPACT"))?;
            let metadata_row_value_bytes = row_key_text_bytes
                .checked_add(bytes_text.len())
                .ok_or(PlannerError::Limit("M3_SOURCE_IMPACT"))?;
            max_metadata_row_value_bytes =
                max_metadata_row_value_bytes.max(metadata_row_value_bytes);
            let bytes = bytes_text
                .parse::<usize>()
                .map_err(|_| PlannerError::Conflict("M3_SOURCE_ROW"))?;
            payload_bytes = payload_bytes
                .checked_add(bytes)
                .ok_or(PlannerError::Limit("M3_CHUNK_BYTES"))?;
            let data_row_value_bytes = bytes
                .checked_mul(2)
                .and_then(|value| value.checked_add(row_key_text_bytes))
                .ok_or(PlannerError::Limit("M3_SOURCE_IMPACT"))?;
            max_data_row_value_bytes = max_data_row_value_bytes.max(data_row_value_bytes);
            payload_lengths.push(bytes);
            expected.push(key);
        }
        if payload_bytes > budget.max_bytes {
            return Err(PlannerError::Limit("M3_CHUNK_BYTES"));
        }
        let mut rows = Vec::with_capacity(row_count);
        for (key, payload_len) in expected.iter().zip(&payload_lengths) {
            let mut cloned_key = Vec::with_capacity(key.0.len());
            cloned_key.extend_from_slice(&key.0);
            rows.push(SnapshotRow {
                key: CanonicalKey(cloned_key),
                payload: Vec::with_capacity(*payload_len),
            });
        }
        let peaks = postgres_copy_peaks(
            &expected,
            &rows,
            &payload_lengths,
            schema.len(),
            ResultFootprint {
                payload_bytes,
                metadata_result_bytes,
                key_text_bytes,
                key_column_name_bytes: self.key_columns.iter().map(String::len).sum(),
                max_metadata_row_value_bytes,
                max_data_row_value_bytes,
            },
        )?;
        self.last_copy_peaks = Some(peaks);
        if peaks.metadata > budget.max_source_impact_bytes
            || peaks.payload > budget.max_source_impact_bytes
        {
            // Both peaks are computed solely from the metadata result, so payload is never fetched
            // when either phase could exceed the hard source-impact bound.
            return Err(PlannerError::Limit("M3_SOURCE_IMPACT"));
        }
        if started.elapsed() > budget.max_duration {
            return Err(PlannerError::Limit("M3_CHUNK_TIME"));
        }
        drop(meta);
        drop(payload_lengths);
        let data_sql = format!(
            "SELECT {keys},encode(convert_to(to_jsonb(t)::text,'UTF8'),'hex') AS payload_hex FROM {} t{where_clause} ORDER BY {keys} LIMIT {}",
            self.table, budget.max_rows
        );
        self.payload_queries = self
            .payload_queries
            .checked_add(1)
            .ok_or(PlannerError::Limit("M3_SOURCE_IMPACT"))?;
        let data = self
            .connection
            .exec(&data_sql)
            .map_err(|_| PlannerError::Conflict("M3_SOURCE_QUERY_FAILED"))?;
        if data.ntuples() as usize != expected.len() {
            return Err(PlannerError::Conflict("M3_SOURCE_CHANGED"));
        }
        for row in 0..data.ntuples() {
            let hex = data
                .get_bytes(row, schema.len() as i32)
                .ok_or(PlannerError::Conflict("M3_SOURCE_ROW"))?;
            if hex.len() % 2 != 0 {
                return Err(PlannerError::Conflict("M3_SOURCE_ROW"));
            }
            let output = &mut rows[row as usize].payload;
            if output.capacity() < hex.len() / 2 {
                return Err(PlannerError::Limit("M3_SOURCE_IMPACT"));
            }
            for pair in hex.chunks_exact(2) {
                let hi = hex_nibble(pair[0]).ok_or(PlannerError::Conflict("M3_SOURCE_ROW"))?;
                let lo = hex_nibble(pair[1]).ok_or(PlannerError::Conflict("M3_SOURCE_ROW"))?;
                output.push((hi << 4) | lo);
            }
        }
        if started.elapsed() > budget.max_duration {
            return Err(PlannerError::Limit("M3_CHUNK_TIME"));
        }
        Ok(rows)
    }
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
        let w = open_writer(&p.0, "run", 1, 0).unwrap();
        w.connection().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('dest','archive','cfg','epoch',1)",[]).unwrap();
        w.connection().execute_batch("CREATE TABLE m3_bootstrap_runtime(intent_id TEXT PRIMARY KEY,generation INTEGER, start_seq INTEGER,snapshot_promotable INTEGER,guard_liveness TEXT,state TEXT); CREATE TABLE m3_bootstrap_importers(intent_id TEXT,importer_id TEXT,assigned_ranges_digest TEXT,snapshot_schema_fingerprint TEXT,state TEXT,PRIMARY KEY(intent_id,importer_id));").unwrap();
        let s = PlannerStore::open(w, limits()).unwrap();
        (p, s)
    }
    fn key(v: i64) -> CanonicalKey {
        CanonicalKey::encode(&[KeyPartType::I64], &[KeyPart::I64(v)]).unwrap()
    }
    fn plan_with_schema(
        s: &mut PlannerStore,
        b: Vec<CanonicalKey>,
        schema: Vec<KeyPartType>,
    ) -> String {
        let ranges = half_open_ranges(&b).unwrap();
        let mut assigned = Vec::new();
        for range in &ranges {
            for bound in [&range.start, &range.end] {
                let bytes = bound.as_ref().map_or(&[][..], |v| v.as_bytes());
                assigned.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
                assigned.extend_from_slice(bytes);
            }
        }
        let digest = crate::m3_bootstrap::digest_assignment(&assigned);
        s.writer.connection().execute("INSERT INTO bootstrap_intents(intent_id,capture_epoch,source_system_id,database_id,slot_name,creation_floor_lsn,state,revision,created_at) VALUES('intent','epoch','sys','db','slot','0000000000000001','slot_created',0,'now')",[]).unwrap();
        s.writer.connection().execute("INSERT INTO m3_bootstrap_runtime VALUES('intent',1,10,1,'held','exporter_released')",[]).unwrap();
        s.writer.connection().execute("INSERT INTO m3_bootstrap_importers VALUES('intent','worker',?1,'schema-fp','acknowledged')",[&digest]).unwrap();
        s.persist_plan(&PlanInput {
            run_id: "run".into(),
            generation_id: "gen".into(),
            destination_id: "dest".into(),
            capture_epoch: "epoch".into(),
            generation: 1,
            bootstrap_intent_id: "intent".into(),
            importer_id: "worker".into(),
            snapshot_schema_fingerprint: "schema-fp".into(),
            key_schema: schema,
            boundaries: b,
            estimated_rows: 8,
            start_seq: 10,
            started_mono_ms: 100,
        })
        .unwrap();
        digest
    }
    fn plan(s: &mut PlannerStore, b: Vec<CanonicalKey>) {
        plan_with_schema(s, b, vec![KeyPartType::I64]);
    }
    struct FixtureSource {
        rows: Vec<SnapshotRow>,
        delay: Duration,
    }
    struct BoundFixtureSource {
        binding: SourceBinding,
    }
    impl BoundedRangeSource for BoundFixtureSource {
        fn binding(&self) -> Option<&SourceBinding> {
            Some(&self.binding)
        }

        fn read_range(
            &mut self,
            _: &KeyRange,
            _: &[KeyPartType],
            _: ReadBudget,
        ) -> Result<Vec<SnapshotRow>, PlannerError> {
            panic!("mismatched binding must fail before source read")
        }
    }
    impl BoundedRangeSource for FixtureSource {
        fn read_range(
            &mut self,
            _: &KeyRange,
            _: &[KeyPartType],
            _: ReadBudget,
        ) -> Result<Vec<SnapshotRow>, PlannerError> {
            if !self.delay.is_zero() {
                std::thread::sleep(self.delay)
            }
            Ok(std::mem::take(&mut self.rows))
        }
    }
    fn execute(
        s: &mut PlannerStore,
        claim: &ChunkClaim,
        rows: Vec<SnapshotRow>,
        now: u64,
    ) -> Result<u64, PlannerError> {
        s.execute_claim(
            claim,
            &mut FixtureSource {
                rows,
                delay: Duration::ZERO,
            },
            now,
        )
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
    fn postgres_copy_peak_accounts_for_all_simultaneous_allocations() {
        let mut expected = Vec::with_capacity(3);
        expected.push(key(1));
        expected.push(key(2));
        let payload_bytes = 100;
        let metadata_result_bytes = 17;
        let key_text_bytes = 6;
        let key_column_name_bytes = 2;
        let mut payload_lengths = Vec::with_capacity(3);
        payload_lengths.extend([40usize, 60]);
        let mut prepared_rows = Vec::with_capacity(3);
        for (key, len) in expected.iter().zip(&payload_lengths) {
            prepared_rows.push(SnapshotRow {
                key: key.clone(),
                payload: Vec::with_capacity(*len),
            });
        }
        let peaks = postgres_copy_peaks(
            &expected,
            &prepared_rows,
            &payload_lengths,
            1,
            ResultFootprint {
                payload_bytes,
                metadata_result_bytes,
                key_text_bytes,
                key_column_name_bytes,
                max_metadata_row_value_bytes: 10,
                max_data_row_value_bytes: 120,
            },
        )
        .unwrap();
        let key_buffers = expected.iter().map(|key| key.0.capacity()).sum::<usize>();
        let cloned_keys = prepared_rows
            .iter()
            .map(|row| row.key.0.capacity())
            .sum::<usize>();
        let payload_capacity = prepared_rows
            .iter()
            .map(|row| row.payload.capacity())
            .sum::<usize>();
        assert_eq!(
            peaks.metadata,
            native_result_allocation(
                expected.len(),
                2,
                metadata_result_bytes,
                key_column_name_bytes + "payload_bytes".len(),
            )
            .unwrap()
                + wire_data_row_allocation(2, 10).unwrap()
                + std::mem::size_of::<Vec<CanonicalKey>>()
                + expected.capacity() * std::mem::size_of::<CanonicalKey>()
                + key_buffers
                + std::mem::size_of::<Vec<KeyPart>>()
                + std::mem::size_of::<KeyPart>()
                + std::mem::size_of::<Vec<usize>>()
                + payload_lengths.capacity() * std::mem::size_of::<usize>()
                + std::mem::size_of::<Vec<SnapshotRow>>()
                + prepared_rows.capacity() * std::mem::size_of::<SnapshotRow>()
                + cloned_keys
                + payload_capacity
        );
        assert_eq!(
            peaks.payload,
            native_result_allocation(
                expected.len(),
                2,
                key_text_bytes + payload_bytes * 2,
                key_column_name_bytes + "payload_hex".len(),
            )
            .unwrap()
                + wire_data_row_allocation(2, 120).unwrap()
                + std::mem::size_of::<Vec<CanonicalKey>>()
                + expected.capacity() * std::mem::size_of::<CanonicalKey>()
                + key_buffers
                + std::mem::size_of::<Vec<SnapshotRow>>()
                + prepared_rows.capacity() * std::mem::size_of::<SnapshotRow>()
                + cloned_keys
                + payload_capacity
        );
        assert!(peaks.payload > payload_bytes + 2 * key(1).as_bytes().len());
        assert!(
            postgres_copy_peaks(
                &expected,
                &prepared_rows,
                &payload_lengths,
                1,
                ResultFootprint {
                    payload_bytes: usize::MAX,
                    metadata_result_bytes: 0,
                    key_text_bytes: 0,
                    key_column_name_bytes: 0,
                    max_metadata_row_value_bytes: 0,
                    max_data_row_value_bytes: 0,
                }
            )
            .is_err()
        );
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
    fn execution_rejects_source_capability_for_another_assignment() {
        let (_p, mut store) = store();
        plan(&mut store, vec![]);
        let claim = store
            .claim_next("gen", "worker", 101, Duration::from_secs(2))
            .unwrap()
            .unwrap();
        let mut source = BoundFixtureSource {
            binding: SourceBinding {
                importer_id: "other-worker".into(),
                assigned_ranges_digest: "other-digest".into(),
                schema_fingerprint: "schema-fp".into(),
            },
        };
        assert!(matches!(
            store.execute_claim(&claim, &mut source, 102),
            Err(PlannerError::Conflict("M3_SOURCE_BINDING_MISMATCH"))
        ));
    }

    #[test]
    fn persisted_chunks_resume_and_event_commit_is_atomic() {
        let (p, mut s) = store();
        plan(&mut s, vec![key(10)]);
        let c = s
            .claim_next("gen", "worker", 101, Duration::from_secs(2))
            .unwrap()
            .unwrap();
        assert_eq!(
            execute(
                &mut s,
                &c,
                vec![SnapshotRow {
                    key: key(1),
                    payload: vec![1]
                }],
                102
            )
            .unwrap(),
            11
        );
        let c2 = s
            .claim_next("gen", "worker", 103, Duration::from_secs(2))
            .unwrap()
            .unwrap();
        s.writer.connection().execute_batch("CREATE TRIGGER fault_chunk_commit BEFORE INSERT ON m3_chunk_commits BEGIN SELECT RAISE(ABORT,'fault'); END;").unwrap();
        assert!(
            execute(
                &mut s,
                &c2,
                vec![SnapshotRow {
                    key: key(11),
                    payload: vec![2]
                }],
                104
            )
            .is_err()
        );
        let events: i64 = s
            .writer
            .connection()
            .query_row("SELECT count(*) FROM m3_snapshot_events", [], |r| r.get(0))
            .unwrap();
        let commits: i64 = s
            .writer
            .connection()
            .query_row("SELECT count(*) FROM m3_chunk_commits", [], |r| r.get(0))
            .unwrap();
        assert_eq!((events, commits), (1, 1));
        drop(s);
        let pending = read_pending_chunk_ids(&p.0, "gen", 10, Duration::from_secs(1)).unwrap();
        assert_eq!(pending.len(), 1);
        let con = rusqlite::Connection::open(&p.0).unwrap();
        let busy: i64 = con
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0))
            .unwrap();
        assert_eq!(busy, 0);
        if let Ok(path) = std::env::var("BORING_CDC_M3_ATOMIC_OBSERVATION") {
            std::fs::write(path,serde_json::to_vec(&serde_json::json!({"snapshot_events":events,"chunk_commits":commits,"pending_after_fault":pending.len(),"wal_checkpoint_busy":busy})).unwrap()).unwrap();
        }
    }
    #[test]
    fn empty_range_commits_without_synthetic_row() {
        let (_p, mut s) = store();
        plan(&mut s, vec![]);
        let c = s
            .claim_next("gen", "worker", 101, Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!(execute(&mut s, &c, vec![], 102).unwrap(), 10);
    }
    #[test]
    fn limits_concurrency_and_stale_generation_are_enforced() {
        let (_p, mut s) = store();
        plan(&mut s, vec![key(10), key(20)]);
        let a = s
            .claim_next("gen", "worker", 101, Duration::from_secs(2))
            .unwrap()
            .unwrap();
        let _b = s
            .claim_next("gen", "worker", 101, Duration::from_secs(2))
            .unwrap()
            .unwrap();
        assert!(
            s.claim_next("gen", "worker", 101, Duration::from_secs(2))
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
            execute(&mut s, &a, too_many, 102),
            Err(PlannerError::Limit("M3_CHUNK_ROWS"))
        ));
        s.invalidate_generation("gen").unwrap();
        let stale = matches!(
            execute(&mut s, &a, vec![], 102),
            Err(PlannerError::StaleGeneration)
        );
        assert!(stale);
        if let Ok(path) = std::env::var("BORING_CDC_M3_TEST_OBSERVATION") {
            let state: String = s
                .writer
                .connection()
                .query_row(
                    "SELECT state FROM backfill_generations WHERE generation_id='gen'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            let claims: i64 = s
                .writer
                .connection()
                .query_row("SELECT count(*) FROM m3_chunk_claims", [], |r| r.get(0))
                .unwrap();
            std::fs::write(path,serde_json::to_vec(&serde_json::json!({"generation_state":state,"remaining_claims":claims,"stale_completion_rejected":stale})).unwrap()).unwrap();
        }
    }
    #[test]
    fn execution_reloads_range_bootstrap_and_expiry() {
        let (_p, mut s) = store();
        plan(&mut s, vec![]);
        let c = s
            .claim_next("gen", "worker", 101, Duration::from_millis(1))
            .unwrap()
            .unwrap();
        let mut changed = c.clone();
        changed.range.start = Some(key(7));
        assert!(matches!(
            execute(&mut s, &changed, vec![], 101),
            Err(PlannerError::Conflict("M3_CLAIM_RANGE_MISMATCH"))
        ));
        let mut delayed = FixtureSource {
            rows: vec![],
            delay: Duration::from_millis(2),
        };
        assert!(matches!(
            s.execute_claim(&c, &mut delayed, 101),
            Err(PlannerError::StaleGeneration)
        ));
        let (_p, mut s) = store();
        plan(&mut s, vec![]);
        let c = s
            .claim_next("gen", "worker", 101, Duration::from_secs(2))
            .unwrap()
            .unwrap();
        s.writer
            .connection()
            .execute(
                "UPDATE m3_bootstrap_runtime SET snapshot_promotable=0 WHERE intent_id='intent'",
                [],
            )
            .unwrap();
        assert!(matches!(
            execute(&mut s, &c, vec![], 102),
            Err(PlannerError::StaleGeneration)
        ));
    }
    #[test]
    fn row_byte_time_rate_source_and_range_bounds_are_enforced() {
        let (_p, mut s) = store();
        plan(&mut s, vec![key(10)]);
        let c = s
            .claim_next("gen", "worker", 101, Duration::from_secs(2))
            .unwrap()
            .unwrap();
        let one = |k, p| {
            vec![SnapshotRow {
                key: key(k),
                payload: vec![0; p],
            }]
        };
        assert!(matches!(
            execute(&mut s, &c, one(11, 1), 102),
            Err(PlannerError::Invalid("M3_ROW_OUTSIDE_RANGE"))
        ));
        assert!(matches!(
            execute(&mut s, &c, one(1, 1025), 102),
            Err(PlannerError::Limit("M3_CHUNK_BYTES"))
        ));
        let mut slow = FixtureSource {
            rows: one(1, 1),
            delay: Duration::from_millis(1100),
        };
        assert!(matches!(
            s.execute_claim(&c, &mut slow, 102),
            Err(PlannerError::Limit("M3_CHUNK_TIME"))
        ));
        s.limits.source_impact_bytes = 1050;
        assert!(matches!(
            execute(&mut s, &c, one(1, 1024), 102),
            Err(PlannerError::Limit("M3_SOURCE_IMPACT"))
        ));
        s.limits.max_rows_per_second = 1;
        let rate_rows = vec![
            SnapshotRow {
                key: key(1),
                payload: vec![],
            },
            SnapshotRow {
                key: key(2),
                payload: vec![],
            },
        ];
        assert!(matches!(
            execute(&mut s, &c, rate_rows, 102),
            Err(PlannerError::Limit("M3_RATE_LIMIT"))
        ));
    }
    #[test]
    fn progress_eta_is_redacted_and_deterministic() {
        let (_p, mut s) = store();
        plan(&mut s, vec![]);
        let c = s
            .claim_next("gen", "worker", 101, Duration::from_secs(2))
            .unwrap()
            .unwrap();
        execute(
            &mut s,
            &c,
            vec![SnapshotRow {
                key: key(1),
                payload: vec![1, 2],
            }],
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
            s.claim_next("gen", "worker", 201, Duration::from_secs(1)),
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

    #[test]
    #[ignore = "requires pinned PostgreSQL 17.6 Compose"]
    fn live_postgres_worker_executes_persisted_composite_ranges() {
        let (p, mut s) = store();
        let uuid = |first: u8| {
            let mut v = [0u8; 16];
            v[0] = first;
            v
        };
        let boundary1 = CanonicalKey::encode(
            &[KeyPartType::I64, KeyPartType::Uuid],
            &[KeyPart::I64(-7), KeyPart::Uuid(uuid(0x80))],
        )
        .unwrap();
        let boundary2 = CanonicalKey::encode(
            &[KeyPartType::I64, KeyPartType::Uuid],
            &[KeyPart::I64(i64::MAX), KeyPart::Uuid([0xff; 16])],
        )
        .unwrap();
        let planner_digest = plan_with_schema(
            &mut s,
            vec![boundary1, boundary2],
            vec![KeyPartType::I64, KeyPartType::Uuid],
        );
        let dsn = std::env::var("BORING_CDC_M3_DSN").unwrap();
        let slot = std::env::var("BORING_CDC_M3_SLOT")
            .unwrap_or_else(|_| "boring_cdc_m3_planner_test".into());
        let mut admin = PgReplicationConnection::connect(&dsn).unwrap();
        let _ = admin.exec(&format!("SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name='{slot}')"));
        let bootstrap_path = std::env::temp_dir().join(format!(
            "m3-planner-bootstrap-{}-{slot}.sqlite",
            std::process::id()
        ));
        let bootstrap_writer = open_writer(&bootstrap_path, "m3-planner-live", 1, 0).unwrap();
        bootstrap_writer.connection().execute("INSERT INTO source_state(singleton,capture_epoch,source_system_id,timeline_id,database_id,slot_name,plugin,publication_fingerprint,protocol_fingerprint) VALUES(1,'epoch','system','1','database',?1,'pgoutput','publication','protocol')",[&slot]).unwrap();
        let mut runtime = crate::m3_bootstrap::BootstrapRuntime::start(
            bootstrap_writer,
            crate::m3_bootstrap::PrepareIntent {
                intent_id: "planner-live-intent".into(),
                capture_epoch: "epoch".into(),
                source_system_id: "system".into(),
                database_id: "database".into(),
                slot_name: slot.clone(),
                generation: 1,
                table_set_fingerprint: "schema-fp".into(),
                configuration_fingerprint: "config-fp".into(),
                importers: vec![
                    crate::m3_bootstrap::ImporterAssignment {
                        importer_id: "worker".into(),
                        assigned_ranges_digest: planner_digest.clone(),
                    },
                    crate::m3_bootstrap::ImporterAssignment {
                        importer_id: "memory-probe".into(),
                        assigned_ranges_digest: planner_digest.clone(),
                    },
                    crate::m3_bootstrap::ImporterAssignment {
                        importer_id: "binding-probe".into(),
                        assigned_ranges_digest: planner_digest.clone(),
                    },
                ],
                created_at: "unix-ms:0".into(),
            },
            &dsn,
            &["public.m3_planner_fixture".into()],
            "boring_cdc_m3_planner_pub",
            crate::m3_bootstrap::SessionBounds {
                statement_timeout_ms: 10_000,
                idle_timeout_ms: 30_000,
            },
            0,
        )
        .unwrap();
        let binding_probe = runtime.take_importer("binding-probe").unwrap();
        let capability_mismatch_rejected = matches!(
            PostgresRangeSource::from_imported(
                binding_probe,
                "wrong-worker",
                &planner_digest,
                "schema-fp",
                "public.m3_planner_fixture",
                &["tenant", "id"],
            ),
            Err(PlannerError::Conflict("M3_IMPORTER_CAPABILITY_MISMATCH"))
        );
        assert!(capability_mismatch_rejected);
        let imported = runtime.take_importer("worker").unwrap();
        assert!(runtime.take_importer("worker").is_err());
        let mut source = PostgresRangeSource::from_imported(
            imported,
            "worker",
            &planner_digest,
            "schema-fp",
            "public.m3_planner_fixture",
            &["tenant", "id"],
        )
        .unwrap();
        let pending_before = read_pending_chunk_ids(&p.0, "gen", 10, Duration::from_secs(1))
            .unwrap()
            .len();
        let mut total = 0;
        for _ in 0..3 {
            let claim = s
                .claim_next("gen", "worker", 101, Duration::from_secs(5))
                .unwrap()
                .unwrap();
            let before = Instant::now();
            let completed = s.execute_claim(&claim, &mut source, 102).unwrap();
            assert!(before.elapsed() <= Duration::from_secs(1));
            assert!(completed >= 10);
            total += s
                .writer
                .connection()
                .query_row(
                    "SELECT row_count FROM m3_chunk_commits WHERE chunk_id=?1",
                    [claim.chunk_id],
                    |r| r.get::<_, i64>(0),
                )
                .unwrap();
        }
        assert_eq!(total, 5);
        s.finish_copy("gen").unwrap();
        let probe = runtime.take_importer("memory-probe").unwrap();
        let mut bounded_probe = PostgresRangeSource::from_imported(
            probe,
            "memory-probe",
            &planner_digest,
            "schema-fp",
            "public.m3_planner_fixture",
            &["tenant", "id"],
        )
        .unwrap();
        let full_range = KeyRange {
            start: None,
            end: None,
        };
        let memory_refused_before_payload = matches!(
            bounded_probe.read_range(
                &full_range,
                &[KeyPartType::I64, KeyPartType::Uuid],
                ReadBudget {
                    max_rows: 10,
                    max_bytes: 4096,
                    max_duration: Duration::from_secs(1),
                    max_source_impact_bytes: 1,
                    max_rows_per_second: 1_000,
                },
            ),
            Err(PlannerError::Limit("M3_SOURCE_IMPACT"))
        );
        assert!(memory_refused_before_payload);
        assert_eq!(bounded_probe.payload_queries, 0);
        let exact_peak = bounded_probe
            .last_copy_peaks
            .unwrap()
            .metadata
            .max(bounded_probe.last_copy_peaks.unwrap().payload);
        assert!(matches!(
            bounded_probe.read_range(
                &full_range,
                &[KeyPartType::I64, KeyPartType::Uuid],
                ReadBudget {
                    max_rows: 10,
                    max_bytes: 4096,
                    max_duration: Duration::from_secs(1),
                    max_source_impact_bytes: exact_peak - 1,
                    max_rows_per_second: 1_000,
                },
            ),
            Err(PlannerError::Limit("M3_SOURCE_IMPACT"))
        ));
        assert_eq!(bounded_probe.payload_queries, 0);
        let exact_limit_rows = bounded_probe
            .read_range(
                &full_range,
                &[KeyPartType::I64, KeyPartType::Uuid],
                ReadBudget {
                    max_rows: 10,
                    max_bytes: 4096,
                    max_duration: Duration::from_secs(1),
                    max_source_impact_bytes: exact_peak,
                    max_rows_per_second: 1_000,
                },
            )
            .unwrap();
        assert_eq!(exact_limit_rows.len(), 5);
        assert_eq!(bounded_probe.payload_queries, 1);
        let exact_limit_payload_fetches = bounded_probe.payload_queries;
        drop(source);
        drop(bounded_probe);
        runtime.invalidate("importer").unwrap();
        let mut cleanup = PgReplicationConnection::connect(&dsn).unwrap();
        cleanup
            .exec(&format!("SELECT pg_drop_replication_slot('{slot}')"))
            .unwrap();
        let _ = std::fs::remove_file(&bootstrap_path);
        let _ = std::fs::remove_file(format!("{}-wal", bootstrap_path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", bootstrap_path.display()));
        if let Ok(path) = std::env::var("BORING_CDC_M3_TEST_OBSERVATION") {
            let state: String = s
                .writer
                .connection()
                .query_row(
                    "SELECT state FROM backfill_generations WHERE generation_id='gen'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            let events: i64 = s
                .writer
                .connection()
                .query_row("SELECT count(*) FROM m3_snapshot_events", [], |r| r.get(0))
                .unwrap();
            let chunks: i64 = s
                .writer
                .connection()
                .query_row(
                    "SELECT count(*) FROM backfill_chunks WHERE state='complete'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            let claims: i64 = s
                .writer
                .connection()
                .query_row("SELECT count(*) FROM m3_chunk_claims", [], |r| r.get(0))
                .unwrap();
            std::fs::write(path,serde_json::to_vec(&serde_json::json!({"generation_state":state,"snapshot_events":events,"complete_chunks":chunks,"remaining_claims":claims,"pending_before":pending_before,"atomic_chunk_event_commit":events==total,"exported_snapshot_importer_handoff":true,"capability_mismatch_rejected":capability_mismatch_rejected,"memory_refused_before_payload":memory_refused_before_payload,"near_limit_peak_bytes":exact_peak,"exact_limit_payload_fetches":exact_limit_payload_fetches})).unwrap()).unwrap();
        }
    }
}
