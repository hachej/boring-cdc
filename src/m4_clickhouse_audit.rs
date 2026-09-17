//! Bounded, persisted ClickHouse integrity audit.
//!
//! A round freezes the destination/checkpoint/contract identity before remote work. Each pass
//! copies complete journal transactions through the M2 bounded reader (which closes its SQLite
//! snapshot before returning), compares them with freshly reconstructed ClickHouse rows, then
//! persists progress and independently expiring coverage through the sole writer.

use crate::m2_journal::{CopiedRange, JournalError, read_complete_range};
use crate::m2_schema::WriterConnection;
use rusqlite::{OptionalExtension, params};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;
use std::time::Duration;

pub const AUDIT_CONDITION: &str = "clickhouse_durability_unverified";
pub const AUDIT_PHASES: [&str; 5] = [
    "round_frozen",
    "journal_compare",
    "destination_self_check",
    "coverage_persisted",
    "blocked",
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditIdentity {
    pub audit_id: String,
    pub destination_id: String,
    pub configuration_fingerprint: String,
    pub capture_epoch: String,
    pub generation: u64,
    pub target_checkpoint: u64,
    pub selector_fingerprint: String,
    pub object_fingerprint: String,
    pub contract_digest: String,
    pub freshness_started_at: String,
    pub freshness_expires_at: String,
    pub retained_history_start_seq: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuditBudget {
    pub max_events: usize,
    pub max_bytes: usize,
    pub max_reader_age: Duration,
    pub max_milliseconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredAuditEvent {
    pub journal_seq: u64,
    pub connector_event_id: String,
    pub stored_payload_hash: String,
    /// Canonical bytes reconstructed from the actual stored key, operation, complete source
    /// version, schema identity and tagged values. Adapters must never substitute the hash column.
    pub reconstructed_payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredAuditBatch {
    pub first_journal_seq: u64,
    pub last_journal_seq: u64,
    pub event_count: u64,
    pub marker_digest: String,
    pub events: Vec<StoredAuditEvent>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DestinationContractObservation {
    pub selector_fingerprint: String,
    pub object_fingerprint: String,
    pub settings_fingerprint: String,
    pub expected_settings_fingerprint: String,
    pub event_identity_conflicts: u64,
    pub marker_identity_conflicts: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuditDispatchError;

/// Production adapters execute fresh server queries and reconstruct payload bytes from columns.
pub trait ClickHouseAuditExecutor {
    fn inspect_contract(&mut self) -> Result<DestinationContractObservation, AuditDispatchError>;
    fn read_stored_range(
        &mut self,
        identity: &AuditIdentity,
        first_seq: u64,
        last_seq: u64,
    ) -> Result<StoredAuditBatch, AuditDispatchError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuditOutcome {
    Progress {
        next_seq: u64,
    },
    Complete {
        verified_start: u64,
        verified_end: u64,
    },
    Blocked {
        fingerprint: String,
    },
}

#[derive(Debug, Eq, PartialEq)]
pub enum AuditError {
    Invalid(&'static str),
    IdentityConflict,
    DestinationBlocked,
    Dispatch,
    Journal(String),
    Sqlite(String),
}
impl fmt::Display for AuditError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}
impl std::error::Error for AuditError {}
impl From<rusqlite::Error> for AuditError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error.to_string())
    }
}

pub fn round_identity_digest(identity: &AuditIdentity) -> String {
    let mut digest = Sha256::new();
    for value in [
        "boring-cdc/clickhouse-audit-round/v1",
        identity.destination_id.as_str(),
        identity.configuration_fingerprint.as_str(),
        identity.capture_epoch.as_str(),
        &identity.generation.to_string(),
        &identity.target_checkpoint.to_string(),
        identity.selector_fingerprint.as_str(),
        identity.object_fingerprint.as_str(),
        identity.contract_digest.as_str(),
    ] {
        hash_part(&mut digest, value.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

/// Start or resume a frozen round. Identity changes first erase old coverage and cursors, so stale
/// work can never be relabelled as fresh for a replacement epoch/generation/selector/contract.
pub fn freeze_round(
    writer: &mut WriterConnection,
    identity: &AuditIdentity,
) -> Result<(), AuditError> {
    validate_identity(identity)?;
    let tx = writer.connection_mut().transaction()?;
    let destination: Option<(String, u64, String, u64)> = tx.query_row(
        "SELECT capture_epoch,generation,configuration_fingerprint,coalesce((SELECT journal_seq FROM destination_checkpoints c WHERE c.destination_id=d.destination_id),0) FROM destinations d WHERE destination_id=?1 AND kind='clickhouse'",
        [&identity.destination_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    ).optional()?;
    if destination
        != Some((
            identity.capture_epoch.clone(),
            identity.generation,
            identity.configuration_fingerprint.clone(),
            identity.target_checkpoint,
        ))
    {
        return Err(AuditError::IdentityConflict);
    }
    let digest = round_identity_digest(identity);
    let old: Option<String> = tx
        .query_row(
            "SELECT round_identity_digest FROM destination_audits WHERE audit_id=?1",
            [&identity.audit_id],
            |row| row.get(0),
        )
        .optional()?;
    if old.as_deref().is_some_and(|old| old != digest) {
        tx.execute(
            "DELETE FROM audit_coverage_subranges WHERE audit_id=?1",
            [&identity.audit_id],
        )?;
        tx.execute(
            "UPDATE destination_audits SET journal_verified_start_seq=NULL,journal_verified_end_seq=NULL,self_consistent_start_seq=NULL,self_consistent_end_seq=NULL,journal_cursor_seq=?2,self_cursor_seq=?2,budget_bytes_used=0,budget_events_used=0,budget_ms_used=0,evidence_digest=NULL,first_mismatch=NULL,revision=revision+1 WHERE audit_id=?1",
            params![identity.audit_id, identity.retained_history_start_seq],
        )?;
    }
    tx.execute(
        "INSERT INTO destination_audits(audit_id,destination_id,configuration_fingerprint,capture_epoch,generation,round_target_seq,round_identity_digest,journal_cursor_seq,self_cursor_seq,budget_bytes_used,budget_events_used,budget_ms_used,freshness_window_started_at,freshness_expires_at,contract_digest,retained_history_start_seq,unverifiable_before_seq) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?8,0,0,0,?9,?10,?11,?8,?8) ON CONFLICT(audit_id) DO UPDATE SET destination_id=excluded.destination_id,configuration_fingerprint=excluded.configuration_fingerprint,capture_epoch=excluded.capture_epoch,generation=excluded.generation,round_target_seq=excluded.round_target_seq,round_identity_digest=excluded.round_identity_digest,freshness_window_started_at=excluded.freshness_window_started_at,freshness_expires_at=excluded.freshness_expires_at,contract_digest=excluded.contract_digest,retained_history_start_seq=excluded.retained_history_start_seq,unverifiable_before_seq=excluded.unverifiable_before_seq,revision=destination_audits.revision+1",
        params![identity.audit_id, identity.destination_id, identity.configuration_fingerprint, identity.capture_epoch, identity.generation, identity.target_checkpoint, digest, identity.retained_history_start_seq, identity.freshness_started_at, identity.freshness_expires_at, identity.contract_digest],
    )?;
    tx.commit()?;
    Ok(())
}

/// Execute one bounded pass. This function never reads PostgreSQL and never moves a checkpoint.
pub fn run_audit_pass(
    journal_path: &Path,
    writer: &mut WriterConnection,
    executor: &mut impl ClickHouseAuditExecutor,
    identity: &AuditIdentity,
    budget: AuditBudget,
    elapsed_milliseconds: u64,
) -> Result<AuditOutcome, AuditError> {
    if budget.max_events == 0
        || budget.max_bytes == 0
        || budget.max_reader_age.is_zero()
        || budget.max_milliseconds == 0
    {
        return Err(AuditError::Invalid("zero audit budget"));
    }
    freeze_round(writer, identity)?;
    let digest = round_identity_digest(identity);
    let (cursor, mismatch): (u64, Option<String>) = writer.connection().query_row(
        "SELECT journal_cursor_seq,first_mismatch FROM destination_audits WHERE audit_id=?1 AND round_identity_digest=?2",
        params![identity.audit_id, digest], |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if let Some(fingerprint) = mismatch {
        return Ok(AuditOutcome::Blocked { fingerprint });
    }

    let observed = executor
        .inspect_contract()
        .map_err(|_| AuditError::Dispatch)?;
    if observed.selector_fingerprint != identity.selector_fingerprint
        || observed.object_fingerprint != identity.object_fingerprint
        || observed.settings_fingerprint != observed.expected_settings_fingerprint
        || observed.event_identity_conflicts != 0
        || observed.marker_identity_conflicts != 0
    {
        return persist_mismatch(writer, identity, "destination-contract-drift");
    }
    if cursor >= identity.target_checkpoint {
        return persist_complete(
            writer,
            identity,
            cursor,
            elapsed_milliseconds,
            "self-check-only",
        );
    }

    let range = match read_complete_range(
        journal_path,
        cursor,
        budget.max_events,
        budget.max_bytes,
        budget.max_reader_age,
    ) {
        Ok(Some(range)) => range,
        Ok(None) => return persist_mismatch(writer, identity, "journal-range-unavailable"),
        Err(JournalError::Limit(_)) => {
            return persist_mismatch(writer, identity, "audit-unit-exceeds-approved-bound");
        }
        Err(error) => return Err(AuditError::Journal(error.to_string())),
    };
    if range.last_seq > identity.target_checkpoint {
        return persist_mismatch(writer, identity, "frozen-target-splits-transaction");
    }
    let remote = executor
        .read_stored_range(identity, range.first_seq, range.last_seq)
        .map_err(|_| AuditError::Dispatch)?;
    if let Err(code) = verify_range(&range, &remote) {
        return persist_mismatch(writer, identity, code);
    }
    let evidence = range_evidence_digest(&range, &remote);
    persist_progress(
        writer,
        identity,
        &range,
        elapsed_milliseconds.min(budget.max_milliseconds),
        &evidence,
    )?;
    if range.last_seq == identity.target_checkpoint {
        persist_complete(
            writer,
            identity,
            range.last_seq,
            elapsed_milliseconds,
            &evidence,
        )
    } else {
        Ok(AuditOutcome::Progress {
            next_seq: range.last_seq + 1,
        })
    }
}

fn verify_range(range: &CopiedRange, remote: &StoredAuditBatch) -> Result<(), &'static str> {
    if remote.first_journal_seq != range.first_seq || remote.last_journal_seq != range.last_seq {
        return Err("marker-range-mismatch");
    }
    let expected = range
        .events
        .iter()
        .filter(|event| event.control_kind.is_none())
        .map(|event| {
            (
                event.journal_seq,
                (
                    event.event_id.as_str(),
                    event.payload_hash.as_str(),
                    event.payload.as_slice(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if remote.event_count != expected.len() as u64 || remote.events.len() != expected.len() {
        return Err("event-count-mismatch");
    }
    let mut actual = BTreeMap::new();
    for event in &remote.events {
        let recomputed = format!("{:x}", Sha256::digest(&event.reconstructed_payload));
        if recomputed != event.stored_payload_hash {
            return Err("stored-payload-hash-mismatch");
        }
        if actual
            .insert(
                event.journal_seq,
                (
                    event.connector_event_id.as_str(),
                    event.stored_payload_hash.as_str(),
                    event.reconstructed_payload.as_slice(),
                ),
            )
            .is_some()
        {
            return Err("duplicate-journal-event");
        }
    }
    if actual != expected {
        return Err("journal-destination-payload-mismatch");
    }
    let expected_marker = marker_digest(range.first_seq, range.last_seq, &expected);
    if remote.marker_digest != expected_marker {
        return Err("marker-digest-mismatch");
    }
    Ok(())
}

fn marker_digest(first: u64, last: u64, events: &BTreeMap<u64, (&str, &str, &[u8])>) -> String {
    let mut digest = Sha256::new();
    hash_part(&mut digest, b"boring-cdc/clickhouse-audit-marker/v1");
    hash_part(&mut digest, &first.to_be_bytes());
    hash_part(&mut digest, &last.to_be_bytes());
    for (seq, (id, hash, _)) in events {
        hash_part(&mut digest, &seq.to_be_bytes());
        hash_part(&mut digest, id.as_bytes());
        hash_part(&mut digest, hash.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

fn range_evidence_digest(range: &CopiedRange, remote: &StoredAuditBatch) -> String {
    let mut digest = Sha256::new();
    hash_part(&mut digest, b"boring-cdc/clickhouse-audit-evidence/v1");
    hash_part(&mut digest, &range.first_seq.to_be_bytes());
    hash_part(&mut digest, &range.last_seq.to_be_bytes());
    hash_part(&mut digest, remote.marker_digest.as_bytes());
    format!("{:x}", digest.finalize())
}

fn persist_progress(
    writer: &mut WriterConnection,
    identity: &AuditIdentity,
    range: &CopiedRange,
    milliseconds: u64,
    evidence: &str,
) -> Result<(), AuditError> {
    let tx = writer.connection_mut().transaction()?;
    let changed = tx.execute(
        "UPDATE destination_audits SET journal_cursor_seq=?2,self_cursor_seq=?2,budget_bytes_used=budget_bytes_used+?3,budget_events_used=budget_events_used+?4,budget_ms_used=budget_ms_used+?5,evidence_digest=?6,revision=revision+1 WHERE audit_id=?1 AND round_identity_digest=?7 AND journal_cursor_seq=?8 AND first_mismatch IS NULL",
        params![identity.audit_id, range.last_seq, range.copied_bytes as u64, range.events.len() as u64, milliseconds, evidence, round_identity_digest(identity), range.first_seq - 1],
    )?;
    if changed != 1 {
        return Err(AuditError::IdentityConflict);
    }
    for kind in ["journal_verified", "self_consistent"] {
        tx.execute("INSERT INTO audit_coverage_subranges(audit_id,coverage_kind,start_seq,end_seq,fresh_until,evidence_digest) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(audit_id,coverage_kind,start_seq,end_seq) DO UPDATE SET fresh_until=excluded.fresh_until,evidence_digest=excluded.evidence_digest", params![identity.audit_id, kind, range.first_seq, range.last_seq, identity.freshness_expires_at, evidence])?;
    }
    tx.commit()?;
    Ok(())
}

fn persist_complete(
    writer: &mut WriterConnection,
    identity: &AuditIdentity,
    end: u64,
    milliseconds: u64,
    evidence: &str,
) -> Result<AuditOutcome, AuditError> {
    let start = identity.retained_history_start_seq;
    let tx = writer.connection_mut().transaction()?;
    let changed = tx.execute(
        "UPDATE destination_audits SET journal_verified_start_seq=?2,journal_verified_end_seq=?3,self_consistent_start_seq=?2,self_consistent_end_seq=?3,budget_ms_used=budget_ms_used+?4,evidence_digest=?5,revision=revision+1 WHERE audit_id=?1 AND round_identity_digest=?6 AND journal_cursor_seq=?3 AND self_cursor_seq=?3 AND first_mismatch IS NULL",
        params![identity.audit_id, start, end, milliseconds, evidence, round_identity_digest(identity)],
    )?;
    if changed != 1 {
        return Err(AuditError::IdentityConflict);
    }
    tx.commit()?;
    Ok(AuditOutcome::Complete {
        verified_start: start,
        verified_end: end,
    })
}

fn persist_mismatch(
    writer: &mut WriterConnection,
    identity: &AuditIdentity,
    code: &str,
) -> Result<AuditOutcome, AuditError> {
    let mut digest = Sha256::new();
    hash_part(&mut digest, b"boring-cdc/clickhouse-audit-mismatch/v1");
    hash_part(&mut digest, code.as_bytes());
    hash_part(&mut digest, round_identity_digest(identity).as_bytes());
    let fingerprint = format!("{:x}", digest.finalize());
    let tx = writer.connection_mut().transaction()?;
    let changed = tx.execute("UPDATE destination_audits SET first_mismatch=?2,evidence_digest=?2,journal_verified_start_seq=NULL,journal_verified_end_seq=NULL,self_consistent_start_seq=NULL,self_consistent_end_seq=NULL,revision=revision+1 WHERE audit_id=?1 AND round_identity_digest=?3", params![identity.audit_id, fingerprint, round_identity_digest(identity)])?;
    if changed != 1 {
        return Err(AuditError::IdentityConflict);
    }
    tx.execute(
        "DELETE FROM audit_coverage_subranges WHERE audit_id=?1",
        [&identity.audit_id],
    )?;
    // Destination-local fail closed: capture and every other destination remain untouched.
    tx.execute("UPDATE destinations SET current_failure_id=coalesce(current_failure_id,?2),revision=revision+1 WHERE destination_id=?1", params![identity.destination_id, format!("audit:{fingerprint}")]).or_else(|_| {
        // processing_failures owns the FK. Persist a stable integrity row before linking it.
        tx.execute("INSERT OR IGNORE INTO processing_failures(failure_id,destination_id,component,failure_class,fingerprint,failed_boundary_start_seq,failed_boundary_end_seq,retry_class,attempt,next_retry_at,armed,first_failed_at,last_failed_at) VALUES(?1,?2,'clickhouse','integrity',?3,?4,?5,'integrity_mismatch',1,NULL,1,?6,?6)", params![format!("audit:{fingerprint}"), identity.destination_id, fingerprint, identity.retained_history_start_seq, identity.target_checkpoint, identity.freshness_started_at])?;
        tx.execute("UPDATE destinations SET current_failure_id=?2,revision=revision+1 WHERE destination_id=?1 AND current_failure_id IS NULL", params![identity.destination_id, format!("audit:{fingerprint}")])
    })?;
    tx.commit()?;
    Ok(AuditOutcome::Blocked { fingerprint })
}

fn validate_identity(identity: &AuditIdentity) -> Result<(), AuditError> {
    if identity.audit_id.is_empty()
        || identity.destination_id.is_empty()
        || identity.configuration_fingerprint.is_empty()
        || identity.capture_epoch.is_empty()
        || identity.generation == 0
        || identity.selector_fingerprint.is_empty()
        || identity.object_fingerprint.is_empty()
        || identity.contract_digest.is_empty()
        || identity.retained_history_start_seq > identity.target_checkpoint
        || identity.freshness_expires_at <= identity.freshness_started_at
    {
        return Err(AuditError::Invalid("incomplete audit identity"));
    }
    Ok(())
}
fn hash_part(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m2_journal::{
        CommitFault, CommitLimits, JournalEvent, JournalStore, SourceCommit, SourceIdentity,
        sha256, transaction_checksum,
    };
    use crate::m2_schema::open_writer;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct Fake {
        corrupt: bool,
        drift: bool,
    }
    impl ClickHouseAuditExecutor for Fake {
        fn inspect_contract(
            &mut self,
        ) -> Result<DestinationContractObservation, AuditDispatchError> {
            Ok(DestinationContractObservation {
                selector_fingerprint: if self.drift { "wrong" } else { "selector" }.into(),
                object_fingerprint: "objects".into(),
                settings_fingerprint: "settings".into(),
                expected_settings_fingerprint: "settings".into(),
                event_identity_conflicts: 0,
                marker_identity_conflicts: 0,
            })
        }
        fn read_stored_range(
            &mut self,
            _: &AuditIdentity,
            first: u64,
            last: u64,
        ) -> Result<StoredAuditBatch, AuditDispatchError> {
            let payload = if self.corrupt {
                b"evil".to_vec()
            } else {
                b"row".to_vec()
            };
            let hash = sha256(b"row");
            let events = vec![StoredAuditEvent {
                journal_seq: 1,
                connector_event_id: "event".into(),
                stored_payload_hash: hash.clone(),
                reconstructed_payload: payload,
            }];
            let expected = BTreeMap::from([(1, ("event", hash.as_str(), b"row".as_slice()))]);
            Ok(StoredAuditBatch {
                first_journal_seq: first,
                last_journal_seq: last,
                event_count: 1,
                marker_digest: marker_digest(first, last, &expected),
                events,
            })
        }
    }
    fn fixture(name: &str) -> (std::path::PathBuf, WriterConnection, AuditIdentity) {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("m4-audit-{name}-{unique}.sqlite"));
        let mut writer = open_writer(&path, "run", 1, 1000).unwrap();
        let identity_source = SourceIdentity {
            capture_epoch: "7".into(),
            source_system_id: "s".into(),
            timeline_id: "1".into(),
            database_id: "d".into(),
            slot_name: "slot".into(),
            publication_fingerprint: "p".into(),
            protocol_fingerprint: "v".into(),
        };
        let events = vec![JournalEvent {
            event_id: "event".into(),
            transaction_ordinal: 0,
            relation_schema_fingerprint: None,
            control_kind: None,
            payload: b"row".to_vec(),
            payload_hash: sha256(b"row"),
        }];
        let commit = SourceCommit {
            transaction_id: "tx".into(),
            xid: "1".into(),
            end_lsn: "0000000000000001".into(),
            payload_checksum: transaction_checksum(&events),
            schemas: vec![],
            events,
        };
        JournalStore::new(
            writer,
            identity_source,
            CommitLimits {
                max_events: 4,
                max_copied_bytes: 4096,
                max_writer_hold: Duration::from_secs(1),
            },
        )
        .unwrap()
        .commit_atomic(&commit, CommitFault::None)
        .unwrap();
        writer = open_writer(&path, "run", 1, 1000).unwrap();
        writer.connection_mut().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('ch','clickhouse','cfg','7',1)", []).unwrap();
        writer.connection_mut().execute("INSERT INTO destination_checkpoints(destination_id,capture_epoch,anchor_id,configuration_fingerprint,generation,complete_transaction_id,journal_seq,current_failure_id,revision) VALUES('ch','7',NULL,'cfg',1,'tx',1,NULL,0)", []).unwrap();
        let identity = AuditIdentity {
            audit_id: "audit".into(),
            destination_id: "ch".into(),
            configuration_fingerprint: "cfg".into(),
            capture_epoch: "7".into(),
            generation: 1,
            target_checkpoint: 1,
            selector_fingerprint: "selector".into(),
            object_fingerprint: "objects".into(),
            contract_digest: "contract".into(),
            freshness_started_at: "2026-01-01".into(),
            freshness_expires_at: "2026-01-02".into(),
            retained_history_start_seq: 0,
        };
        (path, writer, identity)
    }
    #[test]
    fn bounded_round_persists_fresh_coverage_and_completes() {
        let (path, mut writer, identity) = fixture("complete");
        let outcome = run_audit_pass(
            &path,
            &mut writer,
            &mut Fake {
                corrupt: false,
                drift: false,
            },
            &identity,
            AuditBudget {
                max_events: 4,
                max_bytes: 4096,
                max_reader_age: Duration::from_secs(1),
                max_milliseconds: 100,
            },
            5,
        )
        .unwrap();
        assert_eq!(
            outcome,
            AuditOutcome::Complete {
                verified_start: 0,
                verified_end: 1
            }
        );
        assert_eq!(
            writer
                .connection()
                .query_row("SELECT count(*) FROM audit_coverage_subranges", [], |r| r
                    .get::<_, u64>(
                    0
                ))
                .unwrap(),
            2
        );
    }
    #[test]
    fn payload_only_corruption_and_object_drift_block_only_clickhouse() {
        for (name, mut fake) in [
            (
                "payload",
                Fake {
                    corrupt: true,
                    drift: false,
                },
            ),
            (
                "object",
                Fake {
                    corrupt: false,
                    drift: true,
                },
            ),
        ] {
            let (path, mut writer, identity) = fixture(name);
            let outcome = run_audit_pass(
                &path,
                &mut writer,
                &mut fake,
                &identity,
                AuditBudget {
                    max_events: 4,
                    max_bytes: 4096,
                    max_reader_age: Duration::from_secs(1),
                    max_milliseconds: 100,
                },
                5,
            )
            .unwrap();
            assert!(matches!(outcome, AuditOutcome::Blocked { .. }));
            assert!(writer.connection().query_row("SELECT current_failure_id IS NOT NULL FROM destinations WHERE destination_id='ch'", [], |r| r.get::<_,bool>(0)).unwrap());
        }
    }
    #[test]
    fn identity_change_discards_old_coverage_before_reset() {
        let (path, mut writer, mut identity) = fixture("identity");
        run_audit_pass(
            &path,
            &mut writer,
            &mut Fake {
                corrupt: false,
                drift: false,
            },
            &identity,
            AuditBudget {
                max_events: 4,
                max_bytes: 4096,
                max_reader_age: Duration::from_secs(1),
                max_milliseconds: 100,
            },
            5,
        )
        .unwrap();
        writer
            .connection_mut()
            .execute(
                "UPDATE destinations SET configuration_fingerprint='cfg2',revision=revision+1",
                [],
            )
            .unwrap();
        writer.connection_mut().execute("UPDATE destination_checkpoints SET configuration_fingerprint='cfg2',revision=revision+1", []).unwrap();
        identity.configuration_fingerprint = "cfg2".into();
        freeze_round(&mut writer, &identity).unwrap();
        assert_eq!(
            writer
                .connection()
                .query_row("SELECT count(*) FROM audit_coverage_subranges", [], |r| r
                    .get::<_, u64>(
                    0
                ))
                .unwrap(),
            0
        );
    }
}
