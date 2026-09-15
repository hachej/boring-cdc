//! Durable startup reconciliation and bounded, read-only journal diagnostics.
//!
//! Startup reads and releases its bounded SQLite snapshot before invoking the typed archive
//! reconciliation hook.  The final first-match decision is then persisted before streaming may
//! begin.  Command reports contain identifiers and reason codes only; payloads and credentials
//! are never selected.

use crate::m2_journal::{JournalError, JournalVerification, journal_verify};
use crate::m2_schema::{READER_MAX_AGE, READER_MAX_ROWS, open_reader_with_limits, open_writer};
use rusqlite::params;
use serde::Serialize;
use std::path::Path;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveSourceObservation {
    pub source_system_id: String,
    pub timeline_id: String,
    pub database_id: String,
    pub slot_name: String,
    pub plugin: String,
    pub publication_fingerprint: String,
    pub protocol_fingerprint: String,
    pub slot_exists: bool,
    /// Slot identity/invalidation is valid independently of retained WAL availability.
    pub slot_valid: bool,
    pub invalidation_reason: Option<String>,
    pub wal_status: Option<String>,
    pub resume_wal_available: bool,
    pub confirmed_flush_lsn: Option<String>,
    pub restart_lsn: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalSelectorObservation {
    pub destination_id: String,
    pub highest_fence: u64,
    pub selector_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArchiveReconciliation {
    Compatible,
    ExternalAhead { destination_id: String, fence: u64 },
    Blocked { reason: &'static str },
}

/// Typed integration point owned by the archive component. Implementations must not adopt,
/// quarantine, or mutate marker-less directories through this M2 startup boundary.
pub trait ArchiveReconciler {
    fn inspect(&mut self) -> ArchiveReconciliation;
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StartupOutcome {
    Ready,
    DuplicateReplayExpected,
    CreationFloorOnly,
    BootstrapAmbiguousRequiresRestart,
    RequiresReseed,
    Blocked,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StartupReceipt {
    pub outcome: StartupOutcome,
    pub reason_code: String,
    pub requested_lsn: Option<String>,
    pub effective_restart_lsn: Option<String>,
    pub durable_transaction_end_lsn: Option<String>,
    pub creation_floor_lsn: Option<String>,
}

#[derive(Clone, Debug)]
struct LocalState {
    capture_epoch: String,
    control_revision: i64,
    source_system_id: String,
    timeline_id: String,
    database_id: String,
    slot_name: String,
    plugin: String,
    publication_fingerprint: String,
    protocol_fingerprint: String,
    durable_lsn: Option<String>,
    creation_floor: Option<String>,
    bootstrap_intent_id: Option<String>,
}

fn valid_lsn(value: &str) -> bool {
    value.len() == 16
        && value
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_lowercase())
}

fn maximum_lsn(left: Option<&str>, right: Option<&str>) -> Option<String> {
    match (left, right) {
        (Some(a), Some(b)) => Some(if a >= b { a } else { b }.to_owned()),
        (Some(value), None) | (None, Some(value)) => Some(value.to_owned()),
        (None, None) => None,
    }
}

fn persist(
    path: &Path,
    run_id: &str,
    live: &LiveSourceObservation,
    local: &LocalState,
    receipt: &StartupReceipt,
    persist_live_positions: bool,
) -> Result<(), ReconcileError> {
    let mut writer = open_writer(path, run_id, 1, 0)?;
    let tx = writer.connection_mut().transaction()?;
    tx.execute(
        "INSERT INTO startup_reconciliations(run_id,capture_epoch,outcome,reason_code,requested_lsn,effective_restart_lsn,durable_transaction_end_lsn,creation_floor_lsn,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
        params![run_id, local.capture_epoch, outcome_name(&receipt.outcome), receipt.reason_code, receipt.requested_lsn, receipt.effective_restart_lsn, receipt.durable_transaction_end_lsn, receipt.creation_floor_lsn],
    )?;
    let changed = if persist_live_positions {
        tx.execute("UPDATE source_state SET observed_confirmed_flush_lsn=?1,observed_restart_lsn=?2,control_revision=control_revision+1 WHERE singleton=1 AND control_revision=?3", params![live.confirmed_flush_lsn, live.restart_lsn, local.control_revision])?
    } else {
        tx.execute("UPDATE source_state SET control_revision=control_revision+1 WHERE singleton=1 AND control_revision=?1", [local.control_revision])?
    };
    if changed != 1 {
        return Err(ReconcileError::StaleSnapshot);
    }
    if receipt.outcome == StartupOutcome::BootstrapAmbiguousRequiresRestart {
        tx.execute(
            "UPDATE bootstrap_intents SET state='remote_slot_unknown',revision=revision+1 WHERE state='prepared' AND EXISTS(SELECT 1 FROM source_state s WHERE s.singleton=1 AND bootstrap_intents.capture_epoch=s.capture_epoch AND bootstrap_intents.source_system_id=s.source_system_id AND bootstrap_intents.database_id=s.database_id AND bootstrap_intents.slot_name=s.slot_name)",
            [],
        )?;
    }
    if receipt.outcome == StartupOutcome::RequiresReseed {
        tx.execute("INSERT INTO reseed_intents(intent_id,destination_id,capture_epoch,state,revision,evidence_digest) VALUES('startup-'||?1,NULL,?2,'blocked',0,?3)", params![run_id,local.capture_epoch,receipt.reason_code])?;
    }
    tx.commit()?;
    Ok(())
}

fn persist_corruption(path: &Path, run_id: &str) -> Result<(), ReconcileError> {
    let mut writer = open_writer(path, run_id, 1, 0)?;
    let tx = writer.connection_mut().transaction()?;
    let inserted=tx.execute("INSERT INTO startup_reconciliations(run_id,capture_epoch,outcome,reason_code,requested_lsn,effective_restart_lsn,durable_transaction_end_lsn,creation_floor_lsn,created_at) SELECT ?1,capture_epoch,'requires_reseed','JOURNAL_INTEGRITY_FAILED',NULL,NULL,durable_transaction_end_lsn,slot_creation_floor_lsn,strftime('%Y-%m-%dT%H:%M:%fZ','now') FROM source_state WHERE singleton=1",[run_id])?;
    if inserted != 1 {
        return Err(ReconcileError::MissingSourceState);
    }
    tx.execute("INSERT INTO reseed_intents(intent_id,destination_id,capture_epoch,state,revision,evidence_digest) SELECT 'startup-'||?1,NULL,capture_epoch,'blocked',0,'JOURNAL_INTEGRITY_FAILED' FROM source_state WHERE singleton=1",[run_id])?;
    tx.commit()?;
    Ok(())
}

fn outcome_name(value: &StartupOutcome) -> &'static str {
    match value {
        StartupOutcome::Ready => "ready",
        StartupOutcome::DuplicateReplayExpected => "duplicate_replay_expected",
        StartupOutcome::CreationFloorOnly => "creation_floor_only",
        StartupOutcome::BootstrapAmbiguousRequiresRestart => "bootstrap_ambiguous_requires_restart",
        StartupOutcome::RequiresReseed => "requires_reseed",
        StartupOutcome::Blocked => "blocked",
    }
}

#[derive(Debug)]
pub enum ReconcileError {
    Sqlite(rusqlite::Error),
    Journal(JournalError),
    MissingSourceState,
    InvalidObservation,
    StaleSnapshot,
}
impl From<rusqlite::Error> for ReconcileError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Sqlite(value)
    }
}
impl From<JournalError> for ReconcileError {
    fn from(value: JournalError) -> Self {
        Self::Journal(value)
    }
}

/// Execute the startup protocol and durably record its exact outcome before returning.
pub fn reconcile_startup(
    path: &Path,
    run_id: &str,
    live: &LiveSourceObservation,
    external: &[ExternalSelectorObservation],
    archive: &mut impl ArchiveReconciler,
) -> Result<StartupReceipt, ReconcileError> {
    if run_id.is_empty()
        || live
            .confirmed_flush_lsn
            .as_deref()
            .is_some_and(|v| !valid_lsn(v))
        || live.restart_lsn.as_deref().is_some_and(|v| !valid_lsn(v))
    {
        return Err(ReconcileError::InvalidObservation);
    }

    if let Err(error) = startup_integrity(path) {
        let receipt = StartupReceipt {
            outcome: StartupOutcome::RequiresReseed,
            reason_code: "JOURNAL_INTEGRITY_FAILED".into(),
            requested_lsn: None,
            effective_restart_lsn: None,
            durable_transaction_end_lsn: None,
            creation_floor_lsn: None,
        };
        let _ = receipt;
        let _ = persist_corruption(path, run_id);
        return Err(error);
    }

    let reader = open_reader_with_limits(path, READER_MAX_AGE, READER_MAX_ROWS)?;
    let local = reader.query_one_bounded(
        "SELECT s.capture_epoch,s.control_revision,s.source_system_id,s.timeline_id,s.database_id,s.slot_name,s.plugin,s.publication_fingerprint,s.protocol_fingerprint,s.durable_transaction_end_lsn,s.slot_creation_floor_lsn,coalesce(s.slot_creation_intent_id,(SELECT intent_id FROM bootstrap_intents bi WHERE bi.capture_epoch=s.capture_epoch AND bi.source_system_id=s.source_system_id AND bi.database_id=s.database_id AND bi.slot_name=s.slot_name AND bi.state NOT IN ('complete','invalidated','aborted') ORDER BY bi.created_at,bi.intent_id LIMIT 1)) FROM source_state s WHERE s.singleton=1",
        |r| Ok(LocalState { capture_epoch:r.get(0)?,control_revision:r.get(1)?,source_system_id:r.get(2)?,timeline_id:r.get(3)?,database_id:r.get(4)?,slot_name:r.get(5)?,plugin:r.get(6)?,publication_fingerprint:r.get(7)?,protocol_fingerprint:r.get(8)?,durable_lsn:r.get(9)?,creation_floor:r.get(10)?,bootstrap_intent_id:r.get(11)? })
    )?.ok_or(ReconcileError::MissingSourceState)?;
    let local_fences = reader.query_bounded(
        "SELECT d.destination_id,d.highest_external_fence,(SELECT expected_selector_digest FROM destination_promotion_intents p WHERE p.destination_id=d.destination_id AND p.promotion_fence=d.highest_external_fence) FROM destinations d ORDER BY d.destination_id",
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64, r.get::<_,Option<String>>(2)?)),
    )?;
    drop(reader); // bounded SQLite readers never remain held across archive/external inspection.

    let identity_mismatch = local.source_system_id != live.source_system_id
        || local.timeline_id != live.timeline_id
        || local.database_id != live.database_id
        || local.slot_name != live.slot_name
        || (live.slot_exists && local.plugin != live.plugin)
        || local.publication_fingerprint != live.publication_fingerprint
        || local.protocol_fingerprint != live.protocol_fingerprint;
    let archive_state = if identity_mismatch {
        ArchiveReconciliation::Compatible
    } else {
        archive.inspect()
    };
    let selector_corrupt = external.iter().any(|observed| {
        local_fences
            .iter()
            .find(|(id, _, _)| id == &observed.destination_id)
            .is_some_and(|(_, fence, digest)| {
                observed.highest_fence == *fence
                    && digest.as_deref() != Some(observed.selector_digest.as_str())
            })
    });
    let external_ahead = external.iter().any(|observed| {
        local_fences
            .iter()
            .find(|(id, _, _)| id == &observed.destination_id)
            .is_none_or(|(_, fence, _)| observed.highest_fence > *fence)
    });

    let greatest_local = maximum_lsn(
        local.creation_floor.as_deref(),
        local.durable_lsn.as_deref(),
    );
    let server_ahead = live
        .confirmed_flush_lsn
        .as_deref()
        .is_some_and(|confirmed| {
            greatest_local
                .as_deref()
                .is_none_or(|local| confirmed > local)
        });
    // A slot observed before any durable transaction is not ours merely because its name and
    // plugin match.  Only a durable creation floor tied to a bootstrap intent establishes the
    // provenance needed to resume it.
    let ambiguous =
        local.creation_floor.is_none() && local.durable_lsn.is_none() && live.slot_exists;
    let floor_only = local.bootstrap_intent_id.is_some()
        && local.creation_floor.is_some()
        && local.durable_lsn.is_none()
        && live.slot_exists
        && live.slot_valid
        && live.resume_wal_available
        && live
            .confirmed_flush_lsn
            .as_deref()
            .is_none_or(|v| Some(v) == local.creation_floor.as_deref());

    let (outcome, reason, requested, effective) = if identity_mismatch {
        (
            StartupOutcome::Blocked,
            "SOURCE_IDENTITY_MISMATCH",
            None,
            None,
        )
    } else if ambiguous {
        (
            StartupOutcome::BootstrapAmbiguousRequiresRestart,
            "BOOTSTRAP_PROVENANCE_AMBIGUOUS",
            None,
            None,
        )
    } else if floor_only {
        let floor = local.creation_floor.clone();
        (
            StartupOutcome::CreationFloorOnly,
            "CREATION_FLOOR_NOT_DURABLE_PROGRESS",
            floor.clone(),
            floor,
        )
    } else if server_ahead {
        (
            StartupOutcome::RequiresReseed,
            "SERVER_AHEAD_OF_LOCAL_DURABILITY",
            None,
            None,
        )
    } else if !live.slot_exists {
        (StartupOutcome::RequiresReseed, "SLOT_MISSING", None, None)
    } else if !live.slot_valid {
        let reason = match live.invalidation_reason.as_deref() {
            Some("wal_removed") => "SLOT_INVALID_WAL_REMOVED",
            Some("rows_removed") => "SLOT_INVALID_ROWS_REMOVED",
            Some("wal_level_insufficient") => "SLOT_INVALID_WAL_LEVEL_INSUFFICIENT",
            Some("idle_timeout") => "SLOT_INVALID_IDLE_TIMEOUT",
            Some(_) => "SLOT_INVALID_OTHER",
            None => "SLOT_INVALID",
        };
        (StartupOutcome::RequiresReseed, reason, None, None)
    } else if live.restart_lsn.is_none() {
        (
            StartupOutcome::RequiresReseed,
            "RESUME_RESTART_LSN_UNAVAILABLE",
            None,
            None,
        )
    } else if !live.resume_wal_available {
        (
            StartupOutcome::RequiresReseed,
            "RESUME_WAL_STATUS_UNAVAILABLE",
            None,
            None,
        )
    } else if selector_corrupt {
        (
            StartupOutcome::Blocked,
            "EXTERNAL_SELECTOR_CORRUPT",
            None,
            None,
        )
    } else if external_ahead || matches!(archive_state, ArchiveReconciliation::ExternalAhead { .. })
    {
        (
            StartupOutcome::Blocked,
            "EXTERNAL_SELECTOR_AHEAD",
            None,
            None,
        )
    } else if matches!(archive_state, ArchiveReconciliation::Blocked { .. }) {
        (
            StartupOutcome::Blocked,
            "ARCHIVE_RECONCILIATION_BLOCKED",
            None,
            None,
        )
    } else {
        let requested = local
            .durable_lsn
            .clone()
            .or_else(|| Some("0000000000000000".into()));
        let effective = maximum_lsn(requested.as_deref(), live.confirmed_flush_lsn.as_deref());
        let duplicate = local
            .durable_lsn
            .as_deref()
            .zip(live.confirmed_flush_lsn.as_deref())
            .is_some_and(|(local, server)| server < local);
        (
            if duplicate {
                StartupOutcome::DuplicateReplayExpected
            } else {
                StartupOutcome::Ready
            },
            if duplicate {
                "LOCAL_AHEAD_DUPLICATE_REPLAY"
            } else {
                "POSITIONS_COMPATIBLE"
            },
            requested,
            effective,
        )
    };
    let receipt = StartupReceipt {
        outcome,
        reason_code: reason.into(),
        requested_lsn: requested,
        effective_restart_lsn: effective,
        durable_transaction_end_lsn: local.durable_lsn.clone(),
        creation_floor_lsn: local.creation_floor.clone(),
    };
    persist(path, run_id, live, &local, &receipt, !identity_mismatch)?;
    Ok(receipt)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct JournalReport {
    pub integrity: String,
    pub transaction_count: u64,
    pub event_count: u64,
    pub durable_seq: u64,
    pub replay_floor_seq: u64,
    pub replay_ceiling_seq: u64,
}

pub fn startup_integrity(path: &Path) -> Result<JournalVerification, ReconcileError> {
    // The approved runtime boundary permits quick_check at startup; integrity_check remains
    // maintenance-only. Derive an exact finite row bound from durable transaction metadata, then
    // stream every retained event through journal_verify's continuity/hash/count checks under the
    // reader wall-time bound.
    let reader = open_reader_with_limits(path, READER_MAX_AGE, 1)?;
    let expected_events: i64 = reader
        .query_one_bounded(
            "SELECT coalesce(sum(event_count),0) FROM source_transactions WHERE state='committed'",
            |r| r.get(0),
        )?
        .ok_or(ReconcileError::Journal(JournalError::Conflict(
            "missing startup integrity summary",
        )))?;
    let max_events = usize::try_from(expected_events)
        .ok()
        .filter(|value| *value > 0)
        .unwrap_or(1);
    let foreign: Option<i64> = reader
        .query_one_bounded("SELECT 1 FROM pragma_foreign_key_check LIMIT 1", |r| {
            r.get(0)
        })?;
    if foreign.is_some() {
        return Err(ReconcileError::Journal(JournalError::Conflict(
            "foreign-key integrity mismatch",
        )));
    }
    drop(reader);
    Ok(journal_verify(path, max_events, READER_MAX_AGE)?)
}

pub fn journal_report(path: &Path) -> Result<JournalReport, ReconcileError> {
    let JournalVerification {
        transaction_count,
        event_count,
        durable_seq,
    } = journal_verify(path, READER_MAX_ROWS, READER_MAX_AGE)?;
    let reader = open_reader_with_limits(path, READER_MAX_AGE, READER_MAX_ROWS)?;
    let foreign_violation: Option<i64> = reader
        .query_one_bounded("SELECT 1 FROM pragma_foreign_key_check LIMIT 1", |r| {
            r.get(0)
        })?;
    if foreign_violation.is_some() {
        return Err(ReconcileError::Journal(JournalError::Conflict(
            "foreign-key integrity mismatch",
        )));
    }
    let floor = reader
        .query_one_bounded(
            "SELECT coalesce(min(journal_seq),0),coalesce(max(journal_seq),0) FROM journal_events",
            |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64)),
        )?
        .unwrap_or((0, 0));
    Ok(JournalReport {
        integrity: "ok".into(),
        transaction_count,
        event_count,
        durable_seq,
        replay_floor_seq: floor.0,
        replay_ceiling_seq: floor.1,
    })
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RecoveryReport {
    pub latest_startup_outcome: Option<String>,
    pub latest_reason_code: Option<String>,
    pub nonterminal_bootstrap: u64,
    pub nonterminal_promotion: u64,
    pub nonterminal_reseed: u64,
    pub external_ahead_blocked: bool,
}

pub fn recover_report(path: &Path) -> Result<RecoveryReport, ReconcileError> {
    let reader = open_reader_with_limits(path, READER_MAX_AGE, READER_MAX_ROWS)?;
    let latest = reader.query_one_bounded("SELECT outcome,reason_code FROM startup_reconciliations ORDER BY reconciliation_id DESC LIMIT 1", |r| Ok((r.get(0)?,r.get(1)?)))?;
    let counts = reader.query_one_bounded("SELECT (SELECT count(*) FROM bootstrap_intents WHERE state NOT IN ('complete','invalidated','aborted')),(SELECT count(*) FROM destination_promotion_intents WHERE state NOT IN ('retired')),(SELECT count(*) FROM reseed_intents WHERE state NOT IN ('complete','aborted'))", |r| Ok((r.get::<_,i64>(0)? as u64,r.get::<_,i64>(1)? as u64,r.get::<_,i64>(2)? as u64)))?.unwrap_or((0,0,0));
    Ok(RecoveryReport {
        latest_startup_outcome: latest.as_ref().map(|v: &(String, String)| v.0.clone()),
        latest_reason_code: latest.as_ref().map(|v| v.1.clone()),
        nonterminal_bootstrap: counts.0,
        nonterminal_promotion: counts.1,
        nonterminal_reseed: counts.2,
        external_ahead_blocked: latest
            .as_ref()
            .is_some_and(|v| v.1 == "EXTERNAL_SELECTOR_AHEAD"),
    })
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::m2_journal::{
        CommitFault, CommitLimits, JournalEvent, JournalStore, RelationSchema, SourceCommit,
        SourceIdentity, sha256, transaction_checksum,
    };
    use crate::m2_schema::open_writer;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    static NEXT: AtomicU64 = AtomicU64::new(1);
    fn db(name: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "m2-reconcile-{name}-{}-{}.sqlite",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let w = open_writer(&p, "fixture", 1, 0).unwrap();
        w.connection().execute("INSERT INTO source_state(singleton,capture_epoch,source_system_id,timeline_id,database_id,slot_name,plugin,publication_fingerprint,protocol_fingerprint) VALUES(1,'epoch','sys','tl','db','slot','pgoutput','pub','proto')",[]).unwrap();
        drop(w);
        p
    }
    fn seed_durable(path: &Path) {
        let writer = open_writer(path, "fixture", 2, 0).unwrap();
        let identity = SourceIdentity {
            capture_epoch: "epoch".into(),
            source_system_id: "sys".into(),
            timeline_id: "tl".into(),
            database_id: "db".into(),
            slot_name: "slot".into(),
            publication_fingerprint: "pub".into(),
            protocol_fingerprint: "proto".into(),
        };
        let event = JournalEvent {
            event_id: "event".into(),
            transaction_ordinal: 0,
            relation_schema_fingerprint: Some("schema".into()),
            control_kind: None,
            payload: b"payload".to_vec(),
            payload_hash: sha256(b"payload"),
        };
        let commit = SourceCommit {
            transaction_id: "tx".into(),
            xid: "1".into(),
            end_lsn: "0000000000000008".into(),
            payload_checksum: transaction_checksum(std::slice::from_ref(&event)),
            schemas: vec![RelationSchema {
                fingerprint: "schema".into(),
                relation_id: "public.t".into(),
                canonical_schema: b"schema".to_vec(),
                checksum: "schema-checksum".into(),
            }],
            events: vec![event],
        };
        let mut store = JournalStore::new(
            writer,
            identity,
            CommitLimits {
                max_events: 2,
                max_copied_bytes: 1024,
                max_writer_hold: Duration::from_secs(1),
            },
        )
        .unwrap();
        store.commit_atomic(&commit, CommitFault::None).unwrap();
    }

    fn live() -> LiveSourceObservation {
        LiveSourceObservation {
            source_system_id: "sys".into(),
            timeline_id: "tl".into(),
            database_id: "db".into(),
            slot_name: "slot".into(),
            plugin: "pgoutput".into(),
            publication_fingerprint: "pub".into(),
            protocol_fingerprint: "proto".into(),
            slot_exists: true,
            slot_valid: true,
            invalidation_reason: None,
            wal_status: Some("reserved".into()),
            resume_wal_available: true,
            confirmed_flush_lsn: None,
            restart_lsn: Some("0000000000000008".into()),
        }
    }
    struct Archive(ArchiveReconciliation);
    impl ArchiveReconciler for Archive {
        fn inspect(&mut self) -> ArchiveReconciliation {
            self.0.clone()
        }
    }
    fn run(path: &Path, live: &LiveSourceObservation) -> StartupReceipt {
        reconcile_startup(
            path,
            "run",
            live,
            &[],
            &mut Archive(ArchiveReconciliation::Compatible),
        )
        .unwrap()
    }
    #[test]
    fn compatible_requests_durable_position_and_persists_before_ready() {
        let p = db("ready");
        seed_durable(&p);
        let r = run(&p, &live());
        assert_eq!(r.outcome, StartupOutcome::Ready);
        assert_eq!(r.requested_lsn.as_deref(), Some("0000000000000008"));
        assert_eq!(
            recover_report(&p)
                .unwrap()
                .latest_startup_outcome
                .as_deref(),
            Some("ready")
        );
    }
    #[test]
    fn identity_mismatch_blocks() {
        let p = db("identity");
        seed_durable(&p);
        let mut l = live();
        l.timeline_id = "other".into();
        assert_eq!(run(&p, &l).outcome, StartupOutcome::Blocked);
    }
    #[test]
    fn invalid_slot_and_missing_wal_require_reseed() {
        for (name, valid, wal, expected) in [
            ("slot", false, true, "SLOT_INVALID_WAL_REMOVED"),
            ("wal", true, false, "RESUME_WAL_STATUS_UNAVAILABLE"),
        ] {
            let p = db(name);
            seed_durable(&p);
            let mut l = live();
            l.slot_valid = valid;
            l.invalidation_reason = (!valid).then(|| "wal_removed".into());
            l.resume_wal_available = wal;
            l.wal_status = Some(if wal { "reserved" } else { "unreserved" }.into());
            l.restart_lsn = Some("0000000000000008".into());
            let receipt = run(&p, &l);
            assert_eq!(receipt.outcome, StartupOutcome::RequiresReseed);
            assert_eq!(receipt.reason_code, expected);
        }
    }
    #[test]
    fn fresh_journal_with_preexisting_slot_is_ambiguous_without_provenance() {
        let p = db("fresh-preexisting");
        let receipt = run(&p, &live());
        assert_eq!(
            receipt.outcome,
            StartupOutcome::BootstrapAmbiguousRequiresRestart
        );
        assert_eq!(receipt.reason_code, "BOOTSTRAP_PROVENANCE_AMBIGUOUS");
    }

    #[test]
    fn ambiguous_bootstrap_is_transitioned_and_persisted() {
        let p = db("ambiguous");
        let w = open_writer(&p, "fixture", 2, 0).unwrap();
        w.connection().execute("INSERT INTO bootstrap_intents VALUES('boot','epoch','sys','db','slot',NULL,'prepared',0,'now')",[]).unwrap();
        drop(w);
        let r = run(&p, &live());
        assert_eq!(r.outcome, StartupOutcome::BootstrapAmbiguousRequiresRestart);
        let w = open_writer(&p, "fixture", 3, 0).unwrap();
        let state: String = w
            .connection()
            .query_row(
                "SELECT state FROM bootstrap_intents WHERE intent_id='boot'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(state, "remote_slot_unknown");
    }
    #[test]
    fn server_ahead_requires_reseed() {
        let p = db("server-ahead");
        seed_durable(&p);
        let mut l = live();
        l.confirmed_flush_lsn = Some("0000000000000010".into());
        let r = run(&p, &l);
        assert_eq!(r.outcome, StartupOutcome::RequiresReseed);
        assert_eq!(r.reason_code, "SERVER_AHEAD_OF_LOCAL_DURABILITY");
    }
    #[test]
    fn creation_floor_null_or_equal_never_becomes_durable_progress() {
        for confirmed in [None, Some("0000000000000008".into())] {
            let p = db("floor");
            let w = open_writer(&p, "fixture", 2, 0).unwrap();
            w.connection().execute("INSERT INTO bootstrap_intents VALUES('boot','epoch','sys','db','slot','0000000000000008','slot_created',0,'now')",[]).unwrap();
            w.connection().execute("UPDATE source_state SET slot_creation_floor_lsn='0000000000000008',slot_creation_intent_id='boot',control_revision=control_revision+1 WHERE singleton=1",[]).unwrap();
            drop(w);
            let mut l = live();
            l.confirmed_flush_lsn = confirmed;
            let r = run(&p, &l);
            assert_eq!(r.outcome, StartupOutcome::CreationFloorOnly);
            assert!(r.durable_transaction_end_lsn.is_none());
        }
    }
    #[test]
    fn creation_floor_compound_safety_precedes_floor_resume() {
        for (valid, wal, reason) in [
            (false, true, "SLOT_INVALID_WAL_REMOVED"),
            (true, false, "RESUME_RESTART_LSN_UNAVAILABLE"),
        ] {
            let p = db("floor-safety");
            let w = open_writer(&p, "fixture", 2, 0).unwrap();
            w.connection().execute("INSERT INTO bootstrap_intents VALUES('boot','epoch','sys','db','slot','0000000000000008','slot_created',0,'now')",[]).unwrap();
            w.connection().execute("UPDATE source_state SET slot_creation_floor_lsn='0000000000000008',slot_creation_intent_id='boot',control_revision=control_revision+1 WHERE singleton=1",[]).unwrap();
            drop(w);
            let mut l = live();
            l.slot_valid = valid;
            l.invalidation_reason = (!valid).then(|| "wal_removed".into());
            l.resume_wal_available = wal;
            l.restart_lsn = wal.then(|| "0000000000000008".into());
            let r = run(&p, &l);
            assert_eq!(r.reason_code, reason);
        }
    }
    #[test]
    fn archive_hook_blocks_without_adopting() {
        let p = db("archive-block");
        seed_durable(&p);
        let r = reconcile_startup(
            &p,
            "run",
            &live(),
            &[],
            &mut Archive(ArchiveReconciliation::Blocked {
                reason: "MARKER_MISSING",
            }),
        )
        .unwrap();
        assert_eq!(r.reason_code, "ARCHIVE_RECONCILIATION_BLOCKED");
    }

    #[test]
    fn external_ahead_blocks() {
        let p = db("external");
        seed_durable(&p);
        let w = open_writer(&p, "fixture", 2, 0).unwrap();
        w.connection().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('archive','archive','cfg','epoch',1)",[]).unwrap();
        drop(w);
        let r = reconcile_startup(
            &p,
            "run",
            &live(),
            &[ExternalSelectorObservation {
                destination_id: "archive".into(),
                highest_fence: 2,
                selector_digest: "external".into(),
            }],
            &mut Archive(ArchiveReconciliation::Compatible),
        )
        .unwrap();
        assert_eq!(r.reason_code, "EXTERNAL_SELECTOR_AHEAD");
    }
    #[test]
    fn startup_blocks_structurally_valid_payload_checksum_corruption() {
        let p = db("checksum-corruption");
        seed_durable(&p);
        let connection = rusqlite::Connection::open(&p).unwrap();
        connection.execute_batch("DROP TRIGGER journal_events_immutable; UPDATE journal_events SET payload=x'00' WHERE event_id='event';").unwrap();
        drop(connection);
        assert!(
            reconcile_startup(
                &p,
                "corrupt-run",
                &live(),
                &[],
                &mut Archive(ArchiveReconciliation::Compatible)
            )
            .is_err()
        );
        assert_eq!(
            recover_report(&p).unwrap().latest_reason_code.as_deref(),
            Some("JOURNAL_INTEGRITY_FAILED")
        );
    }

    #[test]
    fn reports_are_bounded_and_read_only() {
        let p = db("reports");
        let before = std::fs::metadata(&p).unwrap().len();
        assert_eq!(journal_report(&p).unwrap().integrity, "ok");
        let _ = recover_report(&p).unwrap();
        assert_eq!(std::fs::metadata(&p).unwrap().len(), before);
    }
}
