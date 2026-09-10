//! Pure source-identity and startup-reconciliation model.
//!
//! This leaf deliberately owns no SQLite schema, source queries, or startup I/O. M2 owners
//! `boring-cdc-m2-schema` and `boring-cdc-m2-reconcile` persist and collect these facts.

use crate::m1_transition_kernel::{
    CaptureEpoch, DurableSourceBoundary, ReceivedLsn, SourceVersion,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::cmp::Ordering;
use std::fmt;

// M0-PROVISIONAL: boring-cdc-d-pg-protocol (RECOMMENDED protocol zero sentinel).
pub const PROTOCOL_ZERO_SENTINEL: ReceivedLsn = ReceivedLsn::from_wire(0);
// M0-PROVISIONAL: boring-cdc-d-pg-protocol (RECOMMENDED origin policy).
pub const ORIGIN_POLICY: &str = "any";
// M0-PROVISIONAL: boring-cdc-d-pg-protocol (RECOMMENDED non-streamed pgoutput options).
pub const START_REPLICATION_OPTIONS: [&str; 4] = [
    "proto_version=1",
    "streaming=false",
    "two_phase=false",
    "binary=false",
];
// M0-PROVISIONAL: boring-cdc-d-pg-protocol (RECOMMENDED canonical lock-key domain/version).
const ADVISORY_LOCK_DOMAIN: &[u8] = b"boring-cdc/source-advisory-lock/v1";

#[derive(Clone, Copy, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Fingerprint([u8; 32]);

impl Fingerprint {
    #[must_use]
    pub fn digest(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    #[must_use]
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for Fingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Fingerprint(")?;
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        formatter.write_str(")")
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceIdentity {
    pub system_identifier: u64,
    pub timeline: u32,
    pub database_identity: u32,
    pub slot_name: String,
    pub plugin: String,
    pub publication_fingerprint: Fingerprint,
    pub protocol_fingerprint: Fingerprint,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityValidationError {
    ZeroSystemIdentifier,
    ZeroTimeline,
    ZeroDatabaseIdentity,
    InvalidSlotName,
    InvalidPlugin,
}

impl SourceIdentity {
    pub fn validate(&self) -> Result<(), IdentityValidationError> {
        if self.system_identifier == 0 {
            return Err(IdentityValidationError::ZeroSystemIdentifier);
        }
        if self.timeline == 0 {
            return Err(IdentityValidationError::ZeroTimeline);
        }
        if self.database_identity == 0 {
            return Err(IdentityValidationError::ZeroDatabaseIdentity);
        }
        if !is_pg_identifier(&self.slot_name) {
            return Err(IdentityValidationError::InvalidSlotName);
        }
        if !is_pg_identifier(&self.plugin) {
            return Err(IdentityValidationError::InvalidPlugin);
        }
        Ok(())
    }

    /// Deterministic signed PostgreSQL advisory-lock key over the source ownership tuple.
    #[must_use]
    pub fn advisory_lock_key(&self) -> i64 {
        let mut hash = Sha256::new();
        hash.update(ADVISORY_LOCK_DOMAIN);
        hash.update(self.system_identifier.to_be_bytes());
        hash.update(self.database_identity.to_be_bytes());
        hash_len_prefixed(&mut hash, self.slot_name.as_bytes());
        hash.update(self.publication_fingerprint.bytes());
        let bytes: [u8; 8] = hash.finalize()[..8]
            .try_into()
            .expect("fixed SHA-256 prefix");
        i64::from_be_bytes(bytes)
    }
}

fn hash_len_prefixed(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

fn is_pg_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value.is_ascii()
        && value
            .bytes()
            .all(|byte| byte == b'_' || byte.is_ascii_lowercase() || byte.is_ascii_digit())
        && !value.as_bytes()[0].is_ascii_digit()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityField {
    SystemIdentifier,
    Timeline,
    DatabaseIdentity,
    SlotName,
    Plugin,
    PublicationFingerprint,
    ProtocolFingerprint,
}

fn first_identity_mismatch(local: &SourceIdentity, live: &SourceIdentity) -> Option<IdentityField> {
    if local.system_identifier != live.system_identifier {
        Some(IdentityField::SystemIdentifier)
    } else if local.timeline != live.timeline {
        Some(IdentityField::Timeline)
    } else if local.database_identity != live.database_identity {
        Some(IdentityField::DatabaseIdentity)
    } else if local.slot_name != live.slot_name {
        Some(IdentityField::SlotName)
    } else if local.plugin != live.plugin {
        Some(IdentityField::Plugin)
    } else if local.publication_fingerprint != live.publication_fingerprint {
        Some(IdentityField::PublicationFingerprint)
    } else if local.protocol_fingerprint != live.protocol_fingerprint {
        Some(IdentityField::ProtocolFingerprint)
    } else {
        None
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapProvenance {
    None,
    PreparedWithoutPersistedFloorOrSnapshot,
    NonterminalWithPersistedFloor,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeedbackPosition {
    ProtocolZero,
    RepeatedCreationFloor(ReceivedLsn),
    DurableTransactionEnd(ReceivedLsn),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateValidationError {
    LocalIdentity(IdentityValidationError),
    LiveIdentity(IdentityValidationError),
    ZeroCaptureEpoch,
    PreparedBootstrapHasProgress,
    NonterminalBootstrapMissingFloor,
    FeedbackDoesNotMatchEvidence,
    DurableAheadOfReceived,
}

#[derive(Clone, Debug)]
pub struct LocalSourceState {
    pub capture_epoch: CaptureEpoch,
    pub identity: SourceIdentity,
    pub bootstrap: BootstrapProvenance,
    pub creation_floor: Option<ReceivedLsn>,
    pub received_lsn: Option<ReceivedLsn>,
    pub durable_transaction: Option<DurableSourceBoundary>,
    pub feedback_position: FeedbackPosition,
}

impl LocalSourceState {
    pub fn validate(&self) -> Result<(), StateValidationError> {
        self.identity
            .validate()
            .map_err(StateValidationError::LocalIdentity)?;
        if self.capture_epoch.get() == 0 {
            return Err(StateValidationError::ZeroCaptureEpoch);
        }
        if self.bootstrap == BootstrapProvenance::PreparedWithoutPersistedFloorOrSnapshot
            && (self.creation_floor.is_some() || self.durable_transaction.is_some())
        {
            return Err(StateValidationError::PreparedBootstrapHasProgress);
        }
        if self.bootstrap == BootstrapProvenance::NonterminalWithPersistedFloor
            && self.creation_floor.is_none()
        {
            return Err(StateValidationError::NonterminalBootstrapMissingFloor);
        }
        let feedback_matches = match self.feedback_position {
            FeedbackPosition::ProtocolZero => self.durable_transaction.is_none(),
            FeedbackPosition::RepeatedCreationFloor(value) => {
                self.durable_transaction.is_none() && self.creation_floor == Some(value)
            }
            FeedbackPosition::DurableTransactionEnd(value) => self
                .durable_transaction
                .is_some_and(|durable| durable.commit_lsn() == value),
        };
        if !feedback_matches {
            return Err(StateValidationError::FeedbackDoesNotMatchEvidence);
        }
        if self.durable_transaction.is_some_and(|durable| {
            self.received_lsn
                .is_none_or(|received| received.get() < durable.commit_lsn().get())
        }) {
            return Err(StateValidationError::DurableAheadOfReceived);
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct LiveSourceState {
    pub identity: SourceIdentity,
    pub slot_exists: bool,
    pub slot_valid: bool,
    pub resume_wal_available: bool,
    pub confirmed_flush_lsn: Option<ReceivedLsn>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestedPosition {
    ProtocolZero,
    CreationFloor(ReceivedLsn),
    DurableTransactionEnd(ReceivedLsn),
}

impl RequestedPosition {
    #[must_use]
    pub const fn lsn(self) -> ReceivedLsn {
        match self {
            Self::ProtocolZero => PROTOCOL_ZERO_SENTINEL,
            Self::CreationFloor(lsn) | Self::DurableTransactionEnd(lsn) => lsn,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReseedReason {
    ServerAheadOfLocalEvidence,
    SlotInvalid,
    ResumeWalUnavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupDecision {
    BlockInvalidState {
        reason: StateValidationError,
    },
    BlockIdentityMismatch {
        field: IdentityField,
    },
    BootstrapAmbiguousRequiresRestart,
    CreationFloorOnly {
        requested: RequestedPosition,
    },
    RequiresReseed {
        reason: ReseedReason,
    },
    Resume {
        requested: RequestedPosition,
        effective_restart_lsn: ReceivedLsn,
        duplicate_delivery_expected: bool,
    },
}

/// Apply the normative first-match startup table without persistence or external effects.
#[must_use]
pub fn reconcile_startup(local: &LocalSourceState, live: &LiveSourceState) -> StartupDecision {
    if let Err(reason) = local.validate() {
        return StartupDecision::BlockInvalidState { reason };
    }
    if let Err(reason) = live.identity.validate() {
        return StartupDecision::BlockInvalidState {
            reason: StateValidationError::LiveIdentity(reason),
        };
    }
    if let Some(field) = first_identity_mismatch(&local.identity, &live.identity) {
        return StartupDecision::BlockIdentityMismatch { field };
    }

    if local.bootstrap == BootstrapProvenance::PreparedWithoutPersistedFloorOrSnapshot
        && live.slot_exists
    {
        return StartupDecision::BootstrapAmbiguousRequiresRestart;
    }

    if local.bootstrap == BootstrapProvenance::NonterminalWithPersistedFloor
        && local.durable_transaction.is_none()
        && live.slot_exists
        && live.slot_valid
        && live.resume_wal_available
        && live
            .confirmed_flush_lsn
            .is_none_or(|confirmed| Some(confirmed) == local.creation_floor)
    {
        return StartupDecision::CreationFloorOnly {
            requested: local.creation_floor.map_or(
                RequestedPosition::ProtocolZero,
                RequestedPosition::CreationFloor,
            ),
        };
    }

    let durable_lsn = local
        .durable_transaction
        .map(DurableSourceBoundary::commit_lsn);
    let greatest_local_evidence = max_lsn(local.creation_floor, durable_lsn);
    if live.confirmed_flush_lsn.is_some_and(|confirmed| {
        greatest_local_evidence.is_none_or(|local_lsn| confirmed.get() > local_lsn.get())
    }) {
        return StartupDecision::RequiresReseed {
            reason: ReseedReason::ServerAheadOfLocalEvidence,
        };
    }

    if !live.slot_exists || !live.slot_valid {
        return StartupDecision::RequiresReseed {
            reason: ReseedReason::SlotInvalid,
        };
    }
    if !live.resume_wal_available {
        return StartupDecision::RequiresReseed {
            reason: ReseedReason::ResumeWalUnavailable,
        };
    }

    let requested = durable_lsn.map_or(
        RequestedPosition::ProtocolZero,
        RequestedPosition::DurableTransactionEnd,
    );
    let requested_lsn = requested.lsn();
    let effective_restart_lsn = max_lsn(Some(requested_lsn), live.confirmed_flush_lsn)
        .expect("requested position is always present");
    StartupDecision::Resume {
        requested,
        effective_restart_lsn,
        duplicate_delivery_expected: live
            .confirmed_flush_lsn
            .is_some_and(|server| server.get() < requested_lsn.get()),
    }
}

fn max_lsn(left: Option<ReceivedLsn>, right: Option<ReceivedLsn>) -> Option<ReceivedLsn> {
    match (left, right) {
        (Some(left), Some(right)) => Some(if left.get() >= right.get() {
            left
        } else {
            right
        }),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodedTransactionPosition {
    pub commit_lsn: ReceivedLsn,
    pub end_lsn: ReceivedLsn,
}

impl DecodedTransactionPosition {
    pub fn validate(self) -> Result<Self, TransactionPositionError> {
        if self.end_lsn.get() < self.commit_lsn.get() {
            Err(TransactionPositionError::EndBeforeCommit)
        } else {
            Ok(self)
        }
    }

    #[must_use]
    pub const fn source_version(
        self,
        capture_epoch: CaptureEpoch,
        transaction_id: u32,
        ordinal: u32,
    ) -> SourceVersion {
        SourceVersion::from_decoded(capture_epoch, self.commit_lsn, transaction_id, ordinal)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionPositionError {
    EndBeforeCommit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StandbyStatus {
    pub write_lsn: ReceivedLsn,
    pub flush_lsn: ReceivedLsn,
    pub apply_lsn: ReceivedLsn,
    pub client_timestamp_micros: i64,
    pub requested_reply: bool,
}

/// Only a validated durable transaction boundary can construct feedback progress.
#[must_use]
pub fn standby_status(
    durable: DurableSourceBoundary,
    client_timestamp_micros: i64,
    requested_reply: bool,
) -> StandbyStatus {
    let end = durable.commit_lsn();
    StandbyStatus {
        write_lsn: end,
        flush_lsn: end,
        apply_lsn: end,
        client_timestamp_micros,
        requested_reply,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VersionComparison {
    Less,
    Equal,
    Greater,
    DifferentCaptureEpoch,
}

#[must_use]
pub fn compare_source_versions(left: SourceVersion, right: SourceVersion) -> VersionComparison {
    if left.capture_epoch() != right.capture_epoch() {
        return VersionComparison::DifferentCaptureEpoch;
    }
    match (
        left.commit_lsn().get(),
        left.transaction_id(),
        left.ordinal(),
    )
        .cmp(&(
            right.commit_lsn().get(),
            right.transaction_id(),
            right.ordinal(),
        )) {
        Ordering::Less => VersionComparison::Less,
        Ordering::Equal => VersionComparison::Equal,
        Ordering::Greater => VersionComparison::Greater,
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct LogicalTableIdentity(pub Fingerprint);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RelationSchemaVersion {
    pub logical_table: LogicalTableIdentity,
    pub relation_id: u32,
    pub schema_fingerprint: Fingerprint,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PhysicalKeyHash(pub Fingerprint);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CanonicalRowIdentity {
    pub logical_table: LogicalTableIdentity,
    pub physical_key_hash: PhysicalKeyHash,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m1_transition_kernel::{JournalCursor, synthetic_durable_boundary};

    const EPOCH: CaptureEpoch = CaptureEpoch::from_store(7);

    fn identity() -> SourceIdentity {
        SourceIdentity {
            system_identifier: 42,
            timeline: 1,
            database_identity: 16_384,
            slot_name: "boring_cdc".into(),
            plugin: "pgoutput".into(),
            publication_fingerprint: Fingerprint::digest(b"publication-v1"),
            protocol_fingerprint: Fingerprint::digest(b"protocol-v1"),
        }
    }

    fn durable(lsn: u64) -> DurableSourceBoundary {
        synthetic_durable_boundary(ReceivedLsn::from_wire(lsn), JournalCursor::from_store(3))
    }

    fn local() -> LocalSourceState {
        LocalSourceState {
            capture_epoch: EPOCH,
            identity: identity(),
            bootstrap: BootstrapProvenance::None,
            creation_floor: None,
            received_lsn: Some(ReceivedLsn::from_wire(120)),
            durable_transaction: Some(durable(100)),
            feedback_position: FeedbackPosition::DurableTransactionEnd(ReceivedLsn::from_wire(100)),
        }
    }

    fn live() -> LiveSourceState {
        LiveSourceState {
            identity: identity(),
            slot_exists: true,
            slot_valid: true,
            resume_wal_available: true,
            confirmed_flush_lsn: Some(ReceivedLsn::from_wire(100)),
        }
    }

    #[test]
    fn identity_validation_and_lock_derivation_are_canonical() {
        let source = identity();
        assert_eq!(source.validate(), Ok(()));
        assert_eq!(source.advisory_lock_key(), identity().advisory_lock_key());
        let mut moved_slot = identity();
        moved_slot.slot_name = "other_slot".into();
        assert_ne!(source.advisory_lock_key(), moved_slot.advisory_lock_key());
        let mut invalid = identity();
        invalid.plugin = "PGOUTPUT".into();
        assert_eq!(
            invalid.validate(),
            Err(IdentityValidationError::InvalidPlugin)
        );
    }

    #[test]
    fn every_identity_mismatch_blocks_before_position_rules() {
        let local = local();
        let mut variants = Vec::new();
        let mut value = identity();
        value.system_identifier += 1;
        variants.push((value, IdentityField::SystemIdentifier));
        let mut value = identity();
        value.timeline += 1;
        variants.push((value, IdentityField::Timeline));
        let mut value = identity();
        value.database_identity += 1;
        variants.push((value, IdentityField::DatabaseIdentity));
        let mut value = identity();
        value.slot_name = "moved_slot".into();
        variants.push((value, IdentityField::SlotName));
        let mut value = identity();
        value.plugin = "other".into();
        variants.push((value, IdentityField::Plugin));
        let mut value = identity();
        value.publication_fingerprint = Fingerprint::digest(b"changed");
        variants.push((value, IdentityField::PublicationFingerprint));
        let mut value = identity();
        value.protocol_fingerprint = Fingerprint::digest(b"changed");
        variants.push((value, IdentityField::ProtocolFingerprint));
        for (changed, field) in variants {
            let observed = LiveSourceState {
                identity: changed,
                confirmed_flush_lsn: Some(ReceivedLsn::from_wire(999)),
                ..live()
            };
            assert_eq!(
                reconcile_startup(&local, &observed),
                StartupDecision::BlockIdentityMismatch { field }
            );
        }
    }

    #[test]
    fn ambiguous_bootstrap_precedes_server_ahead() {
        let mut local = local();
        local.bootstrap = BootstrapProvenance::PreparedWithoutPersistedFloorOrSnapshot;
        local.creation_floor = None;
        local.durable_transaction = None;
        local.feedback_position = FeedbackPosition::ProtocolZero;
        let observed = LiveSourceState {
            confirmed_flush_lsn: Some(ReceivedLsn::from_wire(500)),
            ..live()
        };
        assert_eq!(
            reconcile_startup(&local, &observed),
            StartupDecision::BootstrapAmbiguousRequiresRestart
        );
    }

    #[test]
    fn creation_floor_null_and_equal_are_not_durable_progress() {
        for confirmed in [None, Some(ReceivedLsn::from_wire(80))] {
            let mut local = local();
            local.bootstrap = BootstrapProvenance::NonterminalWithPersistedFloor;
            local.creation_floor = Some(ReceivedLsn::from_wire(80));
            local.durable_transaction = None;
            local.feedback_position =
                FeedbackPosition::RepeatedCreationFloor(ReceivedLsn::from_wire(80));
            let observed = LiveSourceState {
                confirmed_flush_lsn: confirmed,
                ..live()
            };
            assert_eq!(
                reconcile_startup(&local, &observed),
                StartupDecision::CreationFloorOnly {
                    requested: RequestedPosition::CreationFloor(ReceivedLsn::from_wire(80))
                }
            );
            assert!(local.durable_transaction.is_none());
            assert_eq!(local.validate(), Ok(()));
        }
    }

    #[test]
    fn creation_floor_still_requires_valid_slot_and_available_wal() {
        let mut local = local();
        local.bootstrap = BootstrapProvenance::NonterminalWithPersistedFloor;
        local.creation_floor = Some(ReceivedLsn::from_wire(80));
        local.durable_transaction = None;
        local.feedback_position =
            FeedbackPosition::RepeatedCreationFloor(ReceivedLsn::from_wire(80));
        for (slot_valid, wal, reason) in [
            (false, true, ReseedReason::SlotInvalid),
            (true, false, ReseedReason::ResumeWalUnavailable),
        ] {
            let observed = LiveSourceState {
                slot_valid,
                resume_wal_available: wal,
                confirmed_flush_lsn: None,
                ..live()
            };
            assert_eq!(
                reconcile_startup(&local, &observed),
                StartupDecision::RequiresReseed { reason }
            );
        }
    }

    #[test]
    fn server_ahead_requires_reseed_before_generic_slot_checks() {
        let observed = LiveSourceState {
            slot_valid: false,
            confirmed_flush_lsn: Some(ReceivedLsn::from_wire(101)),
            ..live()
        };
        assert_eq!(
            reconcile_startup(&local(), &observed),
            StartupDecision::RequiresReseed {
                reason: ReseedReason::ServerAheadOfLocalEvidence
            }
        );
    }

    #[test]
    fn invalid_slot_and_missing_wal_require_reseed() {
        for (slot_valid, wal, reason) in [
            (false, true, ReseedReason::SlotInvalid),
            (true, false, ReseedReason::ResumeWalUnavailable),
        ] {
            let observed = LiveSourceState {
                slot_valid,
                resume_wal_available: wal,
                ..live()
            };
            assert_eq!(
                reconcile_startup(&local(), &observed),
                StartupDecision::RequiresReseed { reason }
            );
        }
    }

    #[test]
    fn local_ahead_requests_durable_end_and_expects_duplicates() {
        let observed = LiveSourceState {
            confirmed_flush_lsn: Some(ReceivedLsn::from_wire(90)),
            ..live()
        };
        assert_eq!(
            reconcile_startup(&local(), &observed),
            StartupDecision::Resume {
                requested: RequestedPosition::DurableTransactionEnd(ReceivedLsn::from_wire(100)),
                effective_restart_lsn: ReceivedLsn::from_wire(100),
                duplicate_delivery_expected: true,
            }
        );
    }

    #[test]
    fn compatible_positions_apply_postgresql_effective_max_rule() {
        let observed = LiveSourceState {
            confirmed_flush_lsn: Some(ReceivedLsn::from_wire(100)),
            ..live()
        };
        assert_eq!(
            reconcile_startup(&local(), &observed),
            StartupDecision::Resume {
                requested: RequestedPosition::DurableTransactionEnd(ReceivedLsn::from_wire(100)),
                effective_restart_lsn: ReceivedLsn::from_wire(100),
                duplicate_delivery_expected: false,
            }
        );
        let mut empty = local();
        empty.durable_transaction = None;
        empty.feedback_position = FeedbackPosition::ProtocolZero;
        empty.received_lsn = Some(ReceivedLsn::from_wire(999));
        let observed = LiveSourceState {
            confirmed_flush_lsn: None,
            ..live()
        };
        assert_eq!(
            reconcile_startup(&empty, &observed),
            StartupDecision::Resume {
                requested: RequestedPosition::ProtocolZero,
                effective_restart_lsn: PROTOCOL_ZERO_SENTINEL,
                duplicate_delivery_expected: false,
            }
        );
    }

    #[test]
    fn invalid_local_progress_relationships_fail_closed() {
        let mut state = local();
        state.feedback_position =
            FeedbackPosition::RepeatedCreationFloor(ReceivedLsn::from_wire(80));
        assert_eq!(
            reconcile_startup(&state, &live()),
            StartupDecision::BlockInvalidState {
                reason: StateValidationError::FeedbackDoesNotMatchEvidence
            }
        );
        state.feedback_position =
            FeedbackPosition::DurableTransactionEnd(ReceivedLsn::from_wire(100));
        state.received_lsn = Some(ReceivedLsn::from_wire(99));
        assert_eq!(
            state.validate(),
            Err(StateValidationError::DurableAheadOfReceived)
        );
    }

    #[test]
    fn commit_and_end_lsn_roles_remain_distinct() {
        let position = DecodedTransactionPosition {
            commit_lsn: ReceivedLsn::from_wire(120),
            end_lsn: ReceivedLsn::from_wire(128),
        }
        .validate()
        .unwrap();
        assert_eq!(
            position.source_version(EPOCH, 9, 1).commit_lsn(),
            position.commit_lsn
        );
        assert_eq!(position.end_lsn, ReceivedLsn::from_wire(128));
        assert_eq!(
            DecodedTransactionPosition {
                commit_lsn: ReceivedLsn::from_wire(129),
                end_lsn: ReceivedLsn::from_wire(128),
            }
            .validate(),
            Err(TransactionPositionError::EndBeforeCommit)
        );
    }

    #[test]
    fn feedback_uses_only_durable_transaction_end_for_all_three_positions() {
        let status = standby_status(durable(123), 77, true);
        assert_eq!(status.write_lsn, ReceivedLsn::from_wire(123));
        assert_eq!(status.flush_lsn, status.write_lsn);
        assert_eq!(status.apply_lsn, status.write_lsn);
        assert!(status.requested_reply);
    }

    #[test]
    fn source_versions_never_compare_across_capture_epochs() {
        let left = SourceVersion::from_decoded(EPOCH, ReceivedLsn::from_wire(10), 2, 3);
        let later = SourceVersion::from_decoded(EPOCH, ReceivedLsn::from_wire(11), 1, 0);
        let other_epoch = SourceVersion::from_decoded(
            CaptureEpoch::from_store(8),
            ReceivedLsn::from_wire(1),
            1,
            0,
        );
        assert_eq!(
            compare_source_versions(left, later),
            VersionComparison::Less
        );
        assert_eq!(
            compare_source_versions(left, left),
            VersionComparison::Equal
        );
        assert_eq!(
            compare_source_versions(later, left),
            VersionComparison::Greater
        );
        assert_eq!(
            compare_source_versions(later, other_epoch),
            VersionComparison::DifferentCaptureEpoch
        );
    }

    #[test]
    fn table_schema_and_row_identities_are_distinct_dimensions() {
        let table = LogicalTableIdentity(Fingerprint::digest(b"stable-logical-table"));
        let schema_v1 = RelationSchemaVersion {
            logical_table: table,
            relation_id: 12,
            schema_fingerprint: Fingerprint::digest(b"schema-v1"),
        };
        let schema_v2 = RelationSchemaVersion {
            relation_id: 13,
            schema_fingerprint: Fingerprint::digest(b"schema-v2"),
            ..schema_v1
        };
        let row = CanonicalRowIdentity {
            logical_table: table,
            physical_key_hash: PhysicalKeyHash(Fingerprint::digest(b"canonical-key-tuple")),
        };
        assert_ne!(schema_v1, schema_v2);
        assert_eq!(row.logical_table, schema_v2.logical_table);
    }
}
