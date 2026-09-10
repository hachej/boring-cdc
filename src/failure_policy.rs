//! Shared, pure failure classification, scheduling, fingerprint, and persistence preparation.
//!
//! Domain runtimes provide clocks, lifecycle fencing, recovery proofs, and the sole SQLite
//! writer. This module does not sleep, reconnect, advance boundaries, or open a database.

use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub const POLICY_VERSION: &str = "failure-policy-v1";
pub const JITTER_SEED: &str = "failure-policy-v1";
// M0-PROVISIONAL: boring-cdc-m2.1
pub const BASE_DELAY_MS: u64 = 1_000;
// M0-PROVISIONAL: boring-cdc-m2.1
pub const MAX_DELAY_MS: u64 = 300_000;
// M0-PROVISIONAL: boring-cdc-m2.1
pub const MAX_ATTEMPTS: u32 = 8;
// M0-PROVISIONAL: boring-cdc-m2.1
pub const JITTER_BASIS_POINTS: u16 = 2_000; // symmetric +/-20%

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    Transient,
    Deterministic,
    Integrity,
    Continuity,
    Configuration,
    ResourcePressure,
    OperatorBlocked,
}

impl FailureClass {
    pub const ALL: [Self; 7] = [
        Self::Transient,
        Self::Deterministic,
        Self::Integrity,
        Self::Continuity,
        Self::Configuration,
        Self::ResourcePressure,
        Self::OperatorBlocked,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Transient => "transient",
            Self::Deterministic => "deterministic",
            Self::Integrity => "integrity",
            Self::Continuity => "continuity",
            Self::Configuration => "configuration",
            Self::ResourcePressure => "resource_pressure",
            Self::OperatorBlocked => "operator_blocked",
        }
    }

    const fn persisted_retry_class(self) -> &'static str {
        match self {
            Self::Transient | Self::ResourcePressure => "transient",
            Self::Integrity | Self::Continuity => "integrity_mismatch",
            Self::Deterministic | Self::Configuration | Self::OperatorBlocked => "deterministic",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StableErrorCode {
    TransportUnavailable,
    DeadlineExceeded,
    InvalidRecord,
    ChecksumMismatch,
    HistoryUnavailable,
    UnsupportedConfiguration,
    ResourceLimit,
    OperatorPause,
}

impl StableErrorCode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::TransportUnavailable => "transport_unavailable",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::InvalidRecord => "invalid_record",
            Self::ChecksumMismatch => "checksum_mismatch",
            Self::HistoryUnavailable => "history_unavailable",
            Self::UnsupportedConfiguration => "unsupported_configuration",
            Self::ResourceLimit => "resource_limit",
            Self::OperatorPause => "operator_pause",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Component {
    Capture,
    ClickHouse,
    Archive,
    Journal,
    Ownership,
}

impl Component {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Capture => "capture",
            Self::ClickHouse => "clickhouse",
            Self::Archive => "archive",
            Self::Journal => "journal",
            Self::Ownership => "ownership",
        }
    }
}

/// Closed allow-list: callers cannot add keys or values containing payloads, DSNs, keys,
/// source identifiers, driver strings, or credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SafeContextKey {
    Operation,
    ProtocolPhase,
    DestinationKind,
    LimitKind,
}

impl SafeContextKey {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Operation => "operation",
            Self::ProtocolPhase => "protocol_phase",
            Self::DestinationKind => "destination_kind",
            Self::LimitKind => "limit_kind",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SafeContextValue {
    Capture,
    Decode,
    Publish,
    Verify,
    Reconcile,
    CopyBoth,
    Pgoutput,
    ClickHouse,
    Archive,
    JournalBytes,
    MemoryBytes,
}

impl SafeContextValue {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Capture => "capture",
            Self::Decode => "decode",
            Self::Publish => "publish",
            Self::Verify => "verify",
            Self::Reconcile => "reconcile",
            Self::CopyBoth => "copy_both",
            Self::Pgoutput => "pgoutput",
            Self::ClickHouse => "clickhouse",
            Self::Archive => "archive",
            Self::JournalBytes => "journal_bytes",
            Self::MemoryBytes => "memory_bytes",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FailedBoundary {
    Capture {
        capture_epoch: String,
        end_lsn: String,
    },
    Destination {
        capture_epoch: String,
        generation: u64,
        first_seq: u64,
        last_seq: u64,
    },
    Control {
        capture_epoch: String,
        generation: u64,
    },
}

impl FailedBoundary {
    fn canonical(&self) -> String {
        match self {
            Self::Capture {
                capture_epoch,
                end_lsn,
            } => format!("capture|{capture_epoch}|{end_lsn}"),
            Self::Destination {
                capture_epoch,
                generation,
                first_seq,
                last_seq,
            } => format!("destination|{capture_epoch}|{generation}|{first_seq}|{last_seq}"),
            Self::Control {
                capture_epoch,
                generation,
            } => format!("control|{capture_epoch}|{generation}"),
        }
    }

    fn seq_range(&self) -> (Option<u64>, Option<u64>) {
        match self {
            Self::Destination {
                first_seq,
                last_seq,
                ..
            } => (Some(*first_seq), Some(*last_seq)),
            Self::Capture { .. } | Self::Control { .. } => (None, None),
        }
    }

    fn capture_epoch(&self) -> &str {
        match self {
            Self::Capture { capture_epoch, .. }
            | Self::Destination { capture_epoch, .. }
            | Self::Control { capture_epoch, .. } => capture_epoch,
        }
    }

    fn generation(&self) -> Option<u64> {
        match self {
            Self::Destination { generation, .. } | Self::Control { generation, .. } => {
                Some(*generation)
            }
            Self::Capture { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FingerprintInput {
    pub component: Component,
    pub class: FailureClass,
    pub code: StableErrorCode,
    pub boundary: FailedBoundary,
    pub relevant_configuration_fingerprint: String,
    pub context: BTreeMap<SafeContextKey, SafeContextValue>,
}

pub fn build_fingerprint(input: &FingerprintInput) -> String {
    let mut hasher = Sha256::new();
    for value in [
        POLICY_VERSION,
        input.component.as_str(),
        input.class.as_str(),
        input.code.as_str(),
        &input.boundary.canonical(),
        &input.relevant_configuration_fingerprint,
    ] {
        hash_framed(&mut hasher, value);
    }
    for (key, value) in &input.context {
        hash_framed(&mut hasher, key.as_str());
        hash_framed(&mut hasher, value.as_str());
    }
    format!("sha256:{:x}", hasher.finalize())
}

fn hash_framed(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureRecord {
    pub failure_id: String,
    pub destination_id: Option<String>,
    pub component: String,
    pub class: FailureClass,
    pub fingerprint: String,
    pub boundary: FailedBoundary,
    pub attempt: u32,
    pub next_retry_at_ms: Option<u64>,
    pub armed: bool,
    pub first_failed_at_ms: u64,
    pub last_failed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureObservation {
    pub fingerprint: FingerprintInput,
    pub destination_id: Option<String>,
    pub observed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionToken {
    pub failure_id: String,
    pub fingerprint: String,
    pub capture_epoch: String,
    pub generation: Option<u64>,
    pub attempt: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyEvent {
    Observe(FailureObservation),
    RetryDue { now_ms: u64 },
    Completed(CompletionToken),
    ProcessRestarted,
    Rearm(RearmRequest),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RearmRequest {
    pub expected_failure_id: String,
    pub expected_fingerprint: String,
    pub new_failure_fingerprint_for_relevant_configuration: Option<String>,
    pub retained_wal_proven: bool,
    pub integrity_recovery_proven: bool,
    pub continuity_recovery_proven: bool,
    pub explicit_operator_authorization: bool,
    pub now_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyAction {
    Persist(FailureRecord),
    RetryNow(FailureRecord),
    Suppressed,
    Clear { failure_id: String },
    StaleCompletion,
    RejectedRearm,
    Noop,
}

pub fn transition(current: Option<&FailureRecord>, event: PolicyEvent) -> PolicyAction {
    match event {
        PolicyEvent::Observe(observation) => observe(current, observation),
        PolicyEvent::RetryDue { now_ms } => match current {
            Some(record)
                if record.armed
                    && record
                        .next_retry_at_ms
                        .is_some_and(|next_retry| now_ms >= next_retry) =>
            {
                PolicyAction::RetryNow(record.clone())
            }
            _ => PolicyAction::Noop,
        },
        PolicyEvent::Completed(token) => match current {
            Some(record)
                if token.failure_id == record.failure_id
                    && token.fingerprint == record.fingerprint
                    && token.capture_epoch == record.boundary.capture_epoch()
                    && token.generation == record.boundary.generation()
                    && token.attempt == record.attempt =>
            {
                PolicyAction::Clear {
                    failure_id: record.failure_id.clone(),
                }
            }
            _ => PolicyAction::StaleCompletion,
        },
        PolicyEvent::ProcessRestarted => current
            .cloned()
            .map(PolicyAction::Persist)
            .unwrap_or(PolicyAction::Noop),
        PolicyEvent::Rearm(request) => rearm(current, request),
    }
}

fn observe(current: Option<&FailureRecord>, observation: FailureObservation) -> PolicyAction {
    let fingerprint = build_fingerprint(&observation.fingerprint);
    if let Some(record) = current
        && record.fingerprint == fingerprint
        && !is_automatic_retry(record.class)
    {
        return PolicyAction::Suppressed;
    }
    let same = current.filter(|record| record.fingerprint == fingerprint);
    let attempt = same.map_or(1, |record| record.attempt.saturating_add(1));
    let first_failed_at_ms = same.map_or(observation.observed_at_ms, |record| {
        record.first_failed_at_ms
    });
    let effective_now = same.map_or(observation.observed_at_ms, |record| {
        observation.observed_at_ms.max(record.last_failed_at_ms)
    });
    let class = observation.fingerprint.class;
    let exhausted = is_automatic_retry(class) && attempt > MAX_ATTEMPTS;
    let next_retry_at_ms = if is_automatic_retry(class) && !exhausted {
        Some(effective_now.saturating_add(retry_delay_ms(&fingerprint, attempt)))
    } else {
        None
    };
    let failure_id = same.map_or_else(
        || format!("failure-{}", &fingerprint[7..]),
        |record| record.failure_id.clone(),
    );
    PolicyAction::Persist(FailureRecord {
        failure_id,
        destination_id: observation.destination_id,
        component: observation.fingerprint.component.as_str().into(),
        class,
        fingerprint,
        boundary: observation.fingerprint.boundary,
        attempt,
        next_retry_at_ms,
        armed: true,
        first_failed_at_ms,
        last_failed_at_ms: effective_now,
    })
}

const fn is_automatic_retry(class: FailureClass) -> bool {
    matches!(
        class,
        FailureClass::Transient | FailureClass::ResourcePressure
    )
}

fn retry_delay_ms(fingerprint: &str, attempt: u32) -> u64 {
    let exponent = attempt.saturating_sub(1).min(62);
    let nominal = BASE_DELAY_MS
        .saturating_mul(1_u64 << exponent)
        .min(MAX_DELAY_MS);
    let width = nominal.saturating_mul(u64::from(JITTER_BASIS_POINTS)) / 10_000;
    let mut hasher = Sha256::new();
    hasher.update(JITTER_SEED.as_bytes());
    hasher.update(fingerprint.as_bytes());
    hasher.update(attempt.to_be_bytes());
    let bytes = hasher.finalize();
    let sample = u64::from_be_bytes(bytes[..8].try_into().expect("fixed digest width"));
    let span = width.saturating_mul(2).saturating_add(1);
    nominal.saturating_sub(width).saturating_add(sample % span)
}

fn rearm(current: Option<&FailureRecord>, request: RearmRequest) -> PolicyAction {
    let Some(record) = current else {
        return PolicyAction::RejectedRearm;
    };
    if request.expected_failure_id != record.failure_id
        || request.expected_fingerprint != record.fingerprint
    {
        return PolicyAction::RejectedRearm;
    }
    let allowed = match record.class {
        FailureClass::Transient | FailureClass::ResourcePressure => true,
        FailureClass::Deterministic | FailureClass::OperatorBlocked => {
            request.explicit_operator_authorization
        }
        FailureClass::Integrity => request.integrity_recovery_proven,
        FailureClass::Continuity => {
            request.continuity_recovery_proven && request.retained_wal_proven
        }
        FailureClass::Configuration => {
            request.retained_wal_proven
                && request
                    .new_failure_fingerprint_for_relevant_configuration
                    .as_ref()
                    .is_some_and(|new| new != &record.fingerprint)
        }
    };
    if !allowed {
        return PolicyAction::RejectedRearm;
    }
    let mut rearmed = record.clone();
    rearmed.attempt = 1;
    rearmed.armed = true;
    rearmed.last_failed_at_ms = request.now_ms.max(record.last_failed_at_ms);
    rearmed.next_retry_at_ms = Some(
        rearmed
            .last_failed_at_ms
            .saturating_add(retry_delay_ms(&record.fingerprint, 1)),
    );
    PolicyAction::Persist(rearmed)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainOutcome {
    CaptureSafeStopped,
    DestinationBlocked,
    RetryEligible,
    IntegrityRecoveryRequired,
    ContinuityRecoveryRequired,
    ExpectedClose,
    UnexpectedClose,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainHookInput {
    pub outcome: DomainOutcome,
    pub capture_epoch: String,
    pub connection_generation: u64,
    pub expected_close_generation: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainProjection {
    SafeStopped,
    Blocked,
    RetryEligible,
    RecoveryRequired,
    ExpectedClose,
    OwnershipLost,
}

pub trait DomainRecoveryHook {
    fn project(&self, input: &DomainHookInput) -> DomainProjection;
}

/// Synthetic shared projection; real transport and destination hooks remain downstream.
pub struct StrictDomainHook;

impl DomainRecoveryHook for StrictDomainHook {
    fn project(&self, input: &DomainHookInput) -> DomainProjection {
        match input.outcome {
            DomainOutcome::CaptureSafeStopped => DomainProjection::SafeStopped,
            DomainOutcome::DestinationBlocked => DomainProjection::Blocked,
            DomainOutcome::RetryEligible => DomainProjection::RetryEligible,
            DomainOutcome::IntegrityRecoveryRequired
            | DomainOutcome::ContinuityRecoveryRequired => DomainProjection::RecoveryRequired,
            DomainOutcome::ExpectedClose
                if input.expected_close_generation == Some(input.connection_generation) =>
            {
                DomainProjection::ExpectedClose
            }
            DomainOutcome::ExpectedClose | DomainOutcome::UnexpectedClose => {
                DomainProjection::OwnershipLost
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreparedFailureOperation {
    StoreAndArm(FailureRecord),
    Clear {
        failure_id: String,
        destination_id: Option<String>,
    },
}

impl PreparedFailureOperation {
    /// Execute only inside the transaction supplied by the capture-priority sole writer.
    pub fn execute(&self, transaction: &Transaction<'_>) -> rusqlite::Result<()> {
        match self {
            Self::StoreAndArm(record) => {
                let (start, end) = record.boundary.seq_range();
                let retry_class =
                    if is_automatic_retry(record.class) && record.attempt > MAX_ATTEMPTS {
                        "exhausted"
                    } else {
                        record.class.persisted_retry_class()
                    };
                transaction.execute(
                    "INSERT INTO processing_failures(failure_id,destination_id,component,failure_class,fingerprint,failed_boundary_start_seq,failed_boundary_end_seq,retry_class,attempt,next_retry_at,armed,first_failed_at,last_failed_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13) ON CONFLICT(failure_id) DO UPDATE SET retry_class=excluded.retry_class,attempt=excluded.attempt,next_retry_at=excluded.next_retry_at,armed=excluded.armed,last_failed_at=excluded.last_failed_at",
                    params![record.failure_id,record.destination_id,record.component,record.class.as_str(),record.fingerprint,start,end,retry_class,record.attempt,record.next_retry_at_ms.map(timestamp),i64::from(record.armed),timestamp(record.first_failed_at_ms),timestamp(record.last_failed_at_ms)],
                )?;
                if let Some(destination_id) = &record.destination_id {
                    transaction.execute(
                        "UPDATE destinations SET current_failure_id=?1,revision=revision+1 WHERE destination_id=?2",
                        params![record.failure_id, destination_id],
                    )?;
                    if transaction.changes() != 1 {
                        return Err(rusqlite::Error::QueryReturnedNoRows);
                    }
                }
            }
            Self::Clear {
                failure_id,
                destination_id,
            } => {
                if let Some(destination_id) = destination_id {
                    transaction.execute(
                        "UPDATE destinations SET current_failure_id=NULL,revision=revision+1 WHERE destination_id=?1 AND current_failure_id=?2",
                        params![destination_id, failure_id],
                    )?;
                    if transaction.changes() != 1 {
                        return Err(rusqlite::Error::QueryReturnedNoRows);
                    }
                }
                transaction.execute(
                    "UPDATE processing_failures SET armed=0,retry_class=CASE WHEN retry_class='transient' THEN 'exhausted' ELSE retry_class END,next_retry_at=NULL WHERE failure_id=?1",
                    [failure_id],
                )?;
                if transaction.changes() != 1 {
                    return Err(rusqlite::Error::QueryReturnedNoRows);
                }
            }
        }
        Ok(())
    }
}

pub fn load_failure(
    connection: &Connection,
    failure_id: &str,
    boundary: FailedBoundary,
) -> rusqlite::Result<Option<FailureRecord>> {
    let row = connection
        .query_row(
            "SELECT destination_id,component,failure_class,fingerprint,failed_boundary_start_seq,failed_boundary_end_seq,attempt,next_retry_at,armed,first_failed_at,last_failed_at FROM processing_failures WHERE failure_id=?1",
            [failure_id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<u64>>(4)?,
                    row.get::<_, Option<u64>>(5)?,
                    row.get::<_, u32>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                ))
            },
        )
        .optional()?;
    let Some((
        destination_id,
        component,
        class,
        fingerprint,
        start,
        end,
        attempt,
        next,
        armed,
        first,
        last,
    )) = row
    else {
        return Ok(None);
    };
    if boundary.seq_range() != (start, end) {
        return Err(rusqlite::Error::InvalidQuery);
    }
    Ok(Some(FailureRecord {
        failure_id: failure_id.into(),
        destination_id,
        component,
        class: parse_class(&class)?,
        fingerprint,
        boundary,
        attempt,
        next_retry_at_ms: next.map(|value| parse_timestamp(&value)).transpose()?,
        armed: armed == 1,
        first_failed_at_ms: parse_timestamp(&first)?,
        last_failed_at_ms: parse_timestamp(&last)?,
    }))
}

fn parse_class(value: &str) -> rusqlite::Result<FailureClass> {
    match value {
        "transient" => Ok(FailureClass::Transient),
        "deterministic" => Ok(FailureClass::Deterministic),
        "integrity" => Ok(FailureClass::Integrity),
        "continuity" => Ok(FailureClass::Continuity),
        "configuration" => Ok(FailureClass::Configuration),
        "resource_pressure" => Ok(FailureClass::ResourcePressure),
        "operator_blocked" => Ok(FailureClass::OperatorBlocked),
        _ => Err(rusqlite::Error::InvalidQuery),
    }
}

fn parse_timestamp(value: &str) -> rusqlite::Result<u64> {
    value
        .strip_prefix("unix-ms:")
        .and_then(|value| value.parse().ok())
        .ok_or(rusqlite::Error::InvalidQuery)
}

fn timestamp(ms: u64) -> String {
    format!("unix-ms:{ms:020}")
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::m2_schema::open_writer;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn boundary() -> FailedBoundary {
        FailedBoundary::Destination {
            capture_epoch: "epoch-a".into(),
            generation: 7,
            first_seq: 10,
            last_seq: 20,
        }
    }

    fn observation(class: FailureClass, at: u64) -> FailureObservation {
        FailureObservation {
            destination_id: Some("destination-a".into()),
            observed_at_ms: at,
            fingerprint: FingerprintInput {
                component: Component::Archive,
                class,
                code: StableErrorCode::TransportUnavailable,
                boundary: boundary(),
                relevant_configuration_fingerprint: "config-a".into(),
                context: BTreeMap::from([(SafeContextKey::Operation, SafeContextValue::Publish)]),
            },
        }
    }

    fn persisted(action: PolicyAction) -> FailureRecord {
        match action {
            PolicyAction::Persist(record) => record,
            other => panic!("expected persisted action, got {other:?}"),
        }
    }

    #[test]
    fn exhaustive_classes_have_closed_stable_serialization() {
        let names: Vec<_> = FailureClass::ALL.iter().map(|c| c.as_str()).collect();
        assert_eq!(
            names,
            [
                "transient",
                "deterministic",
                "integrity",
                "continuity",
                "configuration",
                "resource_pressure",
                "operator_blocked"
            ]
        );
        for class in FailureClass::ALL {
            assert_eq!(
                serde_json::from_str::<FailureClass>(&serde_json::to_string(&class).unwrap())
                    .unwrap(),
                class
            );
        }
    }

    #[test]
    fn deterministic_schedule_caps_jitters_and_ignores_clock_rollback() {
        let first = persisted(transition(
            None,
            PolicyEvent::Observe(observation(FailureClass::Transient, 10_000)),
        ));
        assert!((800..=1_200).contains(&(first.next_retry_at_ms.unwrap() - 10_000)));
        let replay = persisted(transition(
            None,
            PolicyEvent::Observe(observation(FailureClass::Transient, 10_000)),
        ));
        assert_eq!(first, replay);
        let second = persisted(transition(
            Some(&first),
            PolicyEvent::Observe(observation(FailureClass::Transient, 1)),
        ));
        assert_eq!(second.last_failed_at_ms, 10_000);
        assert!((1_600..=2_400).contains(&(second.next_retry_at_ms.unwrap() - 10_000)));
        let mut current = second;
        for _ in 2..MAX_ATTEMPTS {
            current = persisted(transition(
                Some(&current),
                PolicyEvent::Observe(observation(FailureClass::Transient, 10_000)),
            ));
        }
        assert!(current.next_retry_at_ms.unwrap() - 10_000 <= 360_000);
        let exhausted = persisted(transition(
            Some(&current),
            PolicyEvent::Observe(observation(FailureClass::Transient, 10_000)),
        ));
        assert_eq!(exhausted.attempt, MAX_ATTEMPTS + 1);
        assert_eq!(exhausted.next_retry_at_ms, None);
    }

    #[test]
    fn restart_preserves_attempt_and_same_deterministic_failure_is_suppressed() {
        let record = persisted(transition(
            None,
            PolicyEvent::Observe(observation(FailureClass::Deterministic, 50)),
        ));
        assert_eq!(
            transition(Some(&record), PolicyEvent::ProcessRestarted),
            PolicyAction::Persist(record.clone())
        );
        assert_eq!(
            transition(
                Some(&record),
                PolicyEvent::Observe(observation(FailureClass::Deterministic, 60))
            ),
            PolicyAction::Suppressed
        );
    }

    #[test]
    fn fingerprint_changes_only_for_allowed_canonical_inputs_and_contains_no_raw_data() {
        let input = observation(FailureClass::Integrity, 0).fingerprint;
        let one = build_fingerprint(&input);
        let mut changed = input.clone();
        changed
            .context
            .insert(SafeContextKey::LimitKind, SafeContextValue::JournalBytes);
        let two = build_fingerprint(&changed);
        assert_ne!(one, two);
        assert_eq!(one.len(), 71);
        assert!(!one.contains("publish"));
        assert!(!one.contains("epoch-a"));
    }

    #[test]
    fn rearm_requires_class_specific_proofs_and_rejects_unrelated_change() {
        for class in [
            FailureClass::Integrity,
            FailureClass::Continuity,
            FailureClass::Configuration,
            FailureClass::OperatorBlocked,
        ] {
            let record = persisted(transition(
                None,
                PolicyEvent::Observe(observation(class, 100)),
            ));
            let base = RearmRequest {
                expected_failure_id: record.failure_id.clone(),
                expected_fingerprint: record.fingerprint.clone(),
                new_failure_fingerprint_for_relevant_configuration: None,
                retained_wal_proven: false,
                integrity_recovery_proven: false,
                continuity_recovery_proven: false,
                explicit_operator_authorization: false,
                now_ms: 200,
            };
            assert_eq!(
                transition(Some(&record), PolicyEvent::Rearm(base.clone())),
                PolicyAction::RejectedRearm
            );
            let mut allowed = base;
            match class {
                FailureClass::Integrity => allowed.integrity_recovery_proven = true,
                FailureClass::Continuity => {
                    allowed.continuity_recovery_proven = true;
                    allowed.retained_wal_proven = true;
                }
                FailureClass::Configuration => {
                    allowed.retained_wal_proven = true;
                    allowed.new_failure_fingerprint_for_relevant_configuration =
                        Some("sha256:new-policy-fingerprint".into());
                }
                FailureClass::OperatorBlocked => allowed.explicit_operator_authorization = true,
                _ => unreachable!(),
            }
            assert!(matches!(
                transition(Some(&record), PolicyEvent::Rearm(allowed)),
                PolicyAction::Persist(_)
            ));
        }
    }

    #[test]
    fn stale_epoch_generation_attempt_and_fingerprint_completions_cannot_clear() {
        let record = persisted(transition(
            None,
            PolicyEvent::Observe(observation(FailureClass::Transient, 0)),
        ));
        let valid = CompletionToken {
            failure_id: record.failure_id.clone(),
            fingerprint: record.fingerprint.clone(),
            capture_epoch: "epoch-a".into(),
            generation: Some(7),
            attempt: 1,
        };
        assert_eq!(
            transition(Some(&record), PolicyEvent::Completed(valid.clone())),
            PolicyAction::Clear {
                failure_id: record.failure_id.clone()
            }
        );
        for stale in [
            CompletionToken {
                attempt: 2,
                ..valid.clone()
            },
            CompletionToken {
                generation: Some(8),
                ..valid.clone()
            },
            CompletionToken {
                capture_epoch: "epoch-b".into(),
                ..valid.clone()
            },
            CompletionToken {
                fingerprint: "sha256:stale".into(),
                ..valid
            },
        ] {
            assert_eq!(
                transition(Some(&record), PolicyEvent::Completed(stale)),
                PolicyAction::StaleCompletion
            );
        }
    }

    #[test]
    fn synthetic_hooks_fail_closed_on_close_generation_mismatch() {
        let hook = StrictDomainHook;
        let input = DomainHookInput {
            outcome: DomainOutcome::ExpectedClose,
            capture_epoch: "epoch".into(),
            connection_generation: 4,
            expected_close_generation: Some(3),
        };
        assert_eq!(hook.project(&input), DomainProjection::OwnershipLost);
        assert_eq!(
            hook.project(&DomainHookInput {
                expected_close_generation: Some(4),
                ..input
            }),
            DomainProjection::ExpectedClose
        );
    }

    fn temp_path() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "boring-cdc-failure-policy-{}-{nonce}.sqlite",
            std::process::id()
        ))
    }

    #[test]
    fn prepared_adapter_persists_reopens_and_clears_via_supplied_transaction() {
        let path = temp_path();
        let mut writer = open_writer(&path, "run", 1, 0).unwrap();
        writer.connection().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('destination-a','archive','config-a','epoch-a',7)", []).unwrap();
        let record = persisted(transition(
            None,
            PolicyEvent::Observe(observation(FailureClass::Transient, 1_000)),
        ));
        let tx = writer.connection_mut().transaction().unwrap();
        PreparedFailureOperation::StoreAndArm(record.clone())
            .execute(&tx)
            .unwrap();
        tx.commit().unwrap();
        drop(writer);
        let mut writer = open_writer(&path, "run-2", 2, 2_000).unwrap();
        let loaded: (u32, i64, String) = writer.connection().query_row("SELECT f.attempt,f.armed,d.current_failure_id FROM processing_failures f JOIN destinations d ON d.destination_id=f.destination_id WHERE f.failure_id=?1", [&record.failure_id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
        assert_eq!(loaded, (1, 1, record.failure_id.clone()));
        assert_eq!(
            load_failure(writer.connection(), &record.failure_id, boundary()).unwrap(),
            Some(record.clone())
        );
        assert!(
            load_failure(
                writer.connection(),
                &record.failure_id,
                FailedBoundary::Destination {
                    capture_epoch: "epoch-a".into(),
                    generation: 7,
                    first_seq: 11,
                    last_seq: 20,
                }
            )
            .is_err()
        );
        let tx = writer.connection_mut().transaction().unwrap();
        PreparedFailureOperation::Clear {
            failure_id: record.failure_id.clone(),
            destination_id: record.destination_id.clone(),
        }
        .execute(&tx)
        .unwrap();
        tx.commit().unwrap();
        assert_eq!(
            writer
                .connection()
                .query_row(
                    "SELECT armed FROM processing_failures WHERE failure_id=?1",
                    [&record.failure_id],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        drop(writer);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
