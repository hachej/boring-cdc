//! Pure initial-bootstrap/restart/re-seed transition specification.
//!
//! This leaf models durable facts and typed completions only. PostgreSQL sessions, SQLite
//! transactions, and runtime crash execution remain owned by M2/M3.

use crate::m1_control_fixtures::JournaledFence;
use crate::m1_ddl_fixtures::{DurableFenceProof, GuardFenceAuthorization, GuardPhase, GuardState};
use crate::m1_source_identity::{Fingerprint, SourceIdentity, VerifiedRetainedSlotContinuity};
use crate::m1_transition_kernel::{
    CaptureEpoch, DurableSourceBoundary, JournalCursor, ReceivedLsn, SlotCreationFloor,
    TransitionContext, TransitionSystem,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

// M0-PROVISIONAL: boring-cdc-d-backfill (RECOMMENDED bounded importer count).
pub const MAX_IMPORTERS: usize = 16;
// M0-PROVISIONAL: boring-cdc-d-backfill (RECOMMENDED explicit fresh-store boundary).
pub const ZERO_START_SEQ: JournalCursor = JournalCursor::from_store(0);
// M0-PROVISIONAL: boring-cdc-d-event-id (RECOMMENDED snapshot/WAL origin ranks).
pub const SNAPSHOT_ORIGIN_RANK: u8 = 0;
// M0-PROVISIONAL: boring-cdc-d-event-id (RECOMMENDED snapshot/WAL origin ranks).
pub const WAL_ORIGIN_RANK: u8 = 1;
const MAX_IDENTITY_BYTES: usize = 256;
const MAX_SNAPSHOT_IDENTIFIER_BYTES: usize = 1_024;

#[derive(Clone, Eq, PartialEq)]
pub struct ExportedSnapshotIdentifier(String);
impl ExportedSnapshotIdentifier {
    pub fn from_server(value: String) -> Result<Self, BootstrapFailure> {
        if value.is_empty() || value.len() > MAX_SNAPSHOT_IDENTIFIER_BYTES || !value.is_ascii() {
            return Err(BootstrapFailure::deterministic(
                "SNAPSHOT_IDENTIFIER_INVALID",
                "snapshot_export",
            ));
        }
        Ok(Self(value))
    }
}
impl fmt::Debug for ExportedSnapshotIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ExportedSnapshotIdentifier([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompleteJournalStart {
    FreshStoreZero,
    Durable(DurableSourceBoundary),
}
impl CompleteJournalStart {
    fn cursor(self) -> JournalCursor {
        match self {
            Self::FreshStoreZero => ZERO_START_SEQ,
            Self::Durable(boundary) => boundary.journal_cursor(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IntentPhase {
    Prepared,
    SnapshotExported,
    ImportsPending,
    ImportsComplete,
    ExporterReleasePermitted,
    ExporterReleased,
    SnapshotUnusable,
    BootstrapAmbiguousRequiresRestart,
    ExistingSlotGenerationRequired,
    RetainedWalDrained,
    FencePending,
    AnchorComplete,
    FullReseedRequired,
    FullReseedConfirmed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ExporterLiveness {
    NotStarted,
    CommandIdle,
    Released,
    Lost,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum GuardLiveness {
    NotAcquired,
    Held,
    Released,
    Lost,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ImporterPhase {
    Assigned,
    TransactionBegun,
    SnapshotSetFirst,
    ContractBound,
    Acknowledged,
    ReadsComplete,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum BackendSession {
    Exporter,
    Guard,
    Importer(u16),
    Capture,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ImporterState {
    pub assigned_range: String,
    pub phase: ImporterPhase,
    pub snapshot_schema_fingerprint: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapFacts {
    pub intent_id: String,
    pub capture_epoch: CaptureEpoch,
    pub generation: u64,
    pub slot_identity: String,
    pub publication_fingerprint: Fingerprint,
    pub expected_source_identity: SourceIdentity,
    pub table_set_fingerprint: String,
    pub config_fingerprint: String,
    pub session_bounds: SessionBounds,
    pub phase: IntentPhase,
    pub exporter: ExporterLiveness,
    pub guard: GuardLiveness,
    pub ownership_locks_held: bool,
    pub remote_slot_exists: bool,
    pub snapshot_token_persisted: bool,
    snapshot_identifier: Option<ExportedSnapshotIdentifier>,
    pub source_identity: Option<SourceIdentity>,
    guard_state: Option<GuardState>,
    pub dead_backends: BTreeSet<BackendSession>,
    pub stale_backend_cleanup_required: bool,
    continuity_verified: bool,
    pub creation_floor: Option<SlotCreationFloor>,
    pub snapshot_boundary: Option<SlotCreationFloor>,
    pub start_seq: Option<JournalCursor>,
    pub capture_connection_started: bool,
    pub importers: BTreeMap<u16, ImporterState>,
    pub snapshot_events_promotable: bool,
    pub importer_feedback_gate: bool,
    pub durable_wal_end: Option<DurableSourceBoundary>,
    pub feedback_lsn: Option<ReceivedLsn>,
    pub chunks_complete: bool,
    pub intended_fence_nonce: Option<u64>,
    pub durable_fence: Option<(u64, ReceivedLsn, JournalCursor)>,
    pub transient_wal_event_ids: BTreeSet<String>,
    pub lower_stitch: Option<DurableSourceBoundary>,
    pub continuity_break_recorded: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionBounds {
    pub exporter_lifetime_ms: u64,
    pub importer_lifetime_ms: u64,
    pub guard_lifetime_ms: u64,
    pub guard_keepalive_ms: u64,
}

impl SessionBounds {
    fn valid(&self) -> bool {
        self.exporter_lifetime_ms > 0
            && self.importer_lifetime_ms > 0
            && self.guard_lifetime_ms > 0
            && self.guard_keepalive_ms > 0
            && self.guard_keepalive_ms < self.guard_lifetime_ms
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrepareInput {
    pub intent_id: String,
    pub capture_epoch: CaptureEpoch,
    pub generation: u64,
    pub slot_identity: String,
    pub publication_fingerprint: Fingerprint,
    pub expected_source_identity: SourceIdentity,
    pub table_set_fingerprint: String,
    pub config_fingerprint: String,
    pub importer_ranges: Vec<String>,
    pub session_bounds: SessionBounds,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatementKind {
    SetTransactionSnapshot(ExportedSnapshotIdentifier),
    CatalogQuery,
    DataQuery,
    Other,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationScope {
    pub intent_id: String,
    pub capture_epoch: CaptureEpoch,
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryDecision {
    RetryPreparedCreation,
    BootstrapAmbiguousRequiresRestart,
    DrainRetainedWalThenExistingSlotSnapshot,
    RequireConfirmedFullReseed,
    ResumeEligibleGeneration,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BootstrapFailure {
    pub class: &'static str,
    pub fingerprint: &'static str,
    pub failed_boundary: &'static str,
    pub allowed_actions: &'static [&'static str],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableFenceCompletion {
    scope: OperationScope,
    table_set_fingerprint: String,
    nonce: u64,
    boundary: DurableSourceBoundary,
}
impl DurableFenceCompletion {
    pub fn from_journaled_fence(fence: JournaledFence) -> Result<Self, BootstrapFailure> {
        let (intent_id, identity, nonce, boundary) = fence.into_parts();
        Ok(Self {
            scope: OperationScope {
                intent_id,
                capture_epoch: CaptureEpoch::from_store(identity.capture_epoch),
                generation: identity.generation,
            },
            table_set_fingerprint: identity.table_set_fingerprint,
            nonce,
            boundary,
        })
    }
}

impl BootstrapFailure {
    const fn deterministic(fingerprint: &'static str, boundary: &'static str) -> Self {
        Self {
            class: "deterministic",
            fingerprint,
            failed_boundary: boundary,
            allowed_actions: &["status", "restart bootstrap", "confirm full reseed"],
        }
    }
}

impl BootstrapFacts {
    #[must_use]
    pub fn scope(&self) -> OperationScope {
        OperationScope {
            intent_id: self.intent_id.clone(),
            capture_epoch: self.capture_epoch,
            generation: self.generation,
        }
    }
    fn scope_matches(&self, scope: &OperationScope) -> bool {
        scope.intent_id == self.intent_id
            && scope.capture_epoch == self.capture_epoch
            && scope.generation == self.generation
    }

    pub fn prepare(input: PrepareInput) -> Result<Self, BootstrapFailure> {
        if input.intent_id.is_empty()
            || input.capture_epoch.get() == 0
            || input.generation == 0
            || input.slot_identity.is_empty()
            || input.expected_source_identity.validate().is_err()
            || input.expected_source_identity.slot_name != input.slot_identity
            || input.expected_source_identity.publication_fingerprint
                != input.publication_fingerprint
            || input.table_set_fingerprint.is_empty()
            || input.config_fingerprint.is_empty()
            || input.importer_ranges.is_empty()
            || input.importer_ranges.len() > MAX_IMPORTERS
            || input
                .importer_ranges
                .iter()
                .any(|v| v.is_empty() || v.len() > MAX_IDENTITY_BYTES)
            || [
                input.intent_id.as_str(),
                input.slot_identity.as_str(),
                input.table_set_fingerprint.as_str(),
                input.config_fingerprint.as_str(),
            ]
            .iter()
            .any(|v| v.len() > MAX_IDENTITY_BYTES || !v.is_ascii())
            || !input.session_bounds.valid()
        {
            return Err(BootstrapFailure::deterministic(
                "BOOTSTRAP_PREPARE_INVALID",
                "intent_before_slot",
            ));
        }
        let importers = input
            .importer_ranges
            .into_iter()
            .enumerate()
            .map(|(id, assigned_range)| {
                (
                    id as u16,
                    ImporterState {
                        assigned_range,
                        phase: ImporterPhase::Assigned,
                        snapshot_schema_fingerprint: None,
                    },
                )
            })
            .collect();
        Ok(Self {
            intent_id: input.intent_id,
            capture_epoch: input.capture_epoch,
            generation: input.generation,
            slot_identity: input.slot_identity,
            publication_fingerprint: input.publication_fingerprint,
            table_set_fingerprint: input.table_set_fingerprint,
            config_fingerprint: input.config_fingerprint,
            session_bounds: input.session_bounds,
            phase: IntentPhase::Prepared,
            exporter: ExporterLiveness::NotStarted,
            guard: GuardLiveness::NotAcquired,
            ownership_locks_held: true,
            remote_slot_exists: false,
            snapshot_token_persisted: false,
            snapshot_identifier: None,
            expected_source_identity: input.expected_source_identity,
            source_identity: None,
            guard_state: None,
            dead_backends: BTreeSet::new(),
            stale_backend_cleanup_required: false,
            continuity_verified: false,
            creation_floor: None,
            snapshot_boundary: None,
            start_seq: None,
            capture_connection_started: false,
            importers,
            snapshot_events_promotable: false,
            importer_feedback_gate: true,
            durable_wal_end: None,
            feedback_lsn: None,
            chunks_complete: false,
            intended_fence_nonce: None,
            durable_fence: None,
            transient_wal_event_ids: BTreeSet::new(),
            lower_stitch: None,
            continuity_break_recorded: false,
        })
    }

    pub fn reacquire_ownership_locks(&mut self) -> Result<(), BootstrapFailure> {
        if self.phase != IntentPhase::Prepared
            || self.remote_slot_exists
            || self.stale_backend_cleanup_required
        {
            return Err(BootstrapFailure::deterministic(
                "OWNERSHIP_REACQUIRE_INVALID",
                "restart_reconciliation",
            ));
        }
        self.ownership_locks_held = true;
        Ok(())
    }

    pub fn acquire_guard(&mut self, guard: GuardState) -> Result<(), BootstrapFailure> {
        if self.phase != IntentPhase::Prepared
            || !self.ownership_locks_held
            || !guard.proves_bootstrap_binding(
                self.capture_epoch.get(),
                self.generation,
                &self.table_set_fingerprint,
            )
        {
            return Err(BootstrapFailure::deterministic(
                "DDL_GUARD_MUST_PRECEDE_EXPORT",
                "guard_before_slot_creation",
            ));
        }
        self.guard = GuardLiveness::Held;
        self.guard_state = Some(guard);
        Ok(())
    }

    pub fn slot_created(&mut self, configured_slot: &str) -> Result<(), BootstrapFailure> {
        if self.phase != IntentPhase::Prepared
            || self.guard != GuardLiveness::Held
            || !self.ownership_locks_held
            || configured_slot != self.slot_identity
            || self.remote_slot_exists
        {
            return Err(BootstrapFailure::deterministic(
                "PERMANENT_SLOT_CREATION_NOT_AUTHORIZED",
                "one_permanent_slot_path",
            ));
        }
        self.guard_state
            .as_mut()
            .ok_or_else(|| {
                BootstrapFailure::deterministic("DDL_GUARD_PROOF_MISSING", "slot_creation")
            })?
            .advance(GuardPhase::Exported)
            .map_err(|_| {
                BootstrapFailure::deterministic("DDL_GUARD_PHASE_MISMATCH", "slot_creation")
            })?;
        self.remote_slot_exists = true;
        self.exporter = ExporterLiveness::CommandIdle;
        Ok(())
    }

    pub fn persist_export_response(
        &mut self,
        consistent_point: SlotCreationFloor,
        start_boundary: CompleteJournalStart,
        snapshot_identifier: ExportedSnapshotIdentifier,
        source_identity: SourceIdentity,
    ) -> Result<(), BootstrapFailure> {
        if !self.remote_slot_exists
            || self.exporter != ExporterLiveness::CommandIdle
            || self.guard != GuardLiveness::Held
            || consistent_point.get() == 0
            || source_identity.validate().is_err()
            || source_identity != self.expected_source_identity
        {
            return Err(BootstrapFailure::deterministic(
                "EXPORT_RESPONSE_NOT_DURABLE",
                "snapshot_token_persistence",
            ));
        }
        self.guard_state
            .as_mut()
            .ok_or_else(|| {
                BootstrapFailure::deterministic("DDL_GUARD_PROOF_MISSING", "snapshot_export")
            })?
            .advance(GuardPhase::Copying)
            .map_err(|_| {
                BootstrapFailure::deterministic("DDL_GUARD_PHASE_MISMATCH", "snapshot_export")
            })?;
        self.creation_floor = Some(consistent_point);
        self.snapshot_boundary = Some(consistent_point);
        self.start_seq = Some(start_boundary.cursor());
        self.snapshot_token_persisted = true;
        self.snapshot_identifier = Some(snapshot_identifier);
        self.source_identity = Some(source_identity);
        self.snapshot_events_promotable = true;
        self.phase = IntentPhase::SnapshotExported;
        self.phase = IntentPhase::ImportsPending;
        Ok(())
    }

    pub fn start_capture(&mut self, separate_connection: bool) -> Result<(), BootstrapFailure> {
        if self.phase != IntentPhase::ImportsPending
            || !self.snapshot_token_persisted
            || self.exporter != ExporterLiveness::CommandIdle
            || !separate_connection
        {
            return Err(BootstrapFailure::deterministic(
                "CAPTURE_CONNECTION_NOT_SEPARATE_OR_TOO_EARLY",
                "capture_start",
            ));
        }
        self.capture_connection_started = true;
        Ok(())
    }

    pub fn begin_importer_transaction(
        &mut self,
        scope: &OperationScope,
        worker: u16,
        read_only: bool,
        repeatable_read: bool,
    ) -> Result<(), BootstrapFailure> {
        if !self.scope_matches(scope) {
            return Err(BootstrapFailure::deterministic(
                "STALE_IMPORTER_COMPLETION",
                "snapshot_import",
            ));
        }
        let importer = self.importers.get_mut(&worker).ok_or_else(|| {
            BootstrapFailure::deterministic("IMPORTER_UNKNOWN", "snapshot_import")
        })?;
        if importer.phase != ImporterPhase::Assigned || !read_only || !repeatable_read {
            return Err(BootstrapFailure::deterministic(
                "IMPORTER_TRANSACTION_NOT_READ_ONLY_REPEATABLE_READ",
                "snapshot_import",
            ));
        }
        importer.phase = ImporterPhase::TransactionBegun;
        Ok(())
    }

    pub fn importer_statement(
        &mut self,
        scope: &OperationScope,
        worker: u16,
        statement: StatementKind,
    ) -> Result<(), BootstrapFailure> {
        if !self.scope_matches(scope) {
            return Err(BootstrapFailure::deterministic(
                "STALE_IMPORTER_COMPLETION",
                "snapshot_import",
            ));
        }
        let importer = self.importers.get_mut(&worker).ok_or_else(|| {
            BootstrapFailure::deterministic("IMPORTER_UNKNOWN", "snapshot_import")
        })?;
        match (importer.phase, statement) {
            (
                ImporterPhase::TransactionBegun,
                StatementKind::SetTransactionSnapshot(identifier),
            ) if self.snapshot_identifier.as_ref() == Some(&identifier) => {
                importer.phase = ImporterPhase::SnapshotSetFirst;
                Ok(())
            }
            (ImporterPhase::TransactionBegun, _) => {
                self.invalidate("SNAPSHOT_IMPORT_NOT_FIRST_STATEMENT");
                Err(BootstrapFailure::deterministic(
                    "SNAPSHOT_IMPORT_NOT_FIRST_STATEMENT",
                    "snapshot_import",
                ))
            }
            _ => Err(BootstrapFailure::deterministic(
                "IMPORTER_STATEMENT_PHASE_INVALID",
                "snapshot_import",
            )),
        }
    }

    pub fn bind_import_contract(
        &mut self,
        scope: &OperationScope,
        worker: u16,
        snapshot_identifier: &ExportedSnapshotIdentifier,
        snapshot_schema_fingerprint: &str,
    ) -> Result<(), BootstrapFailure> {
        if !self.scope_matches(scope) {
            return Err(BootstrapFailure::deterministic(
                "STALE_IMPORTER_COMPLETION",
                "contract_bind",
            ));
        }
        let importer = self
            .importers
            .get_mut(&worker)
            .ok_or_else(|| BootstrapFailure::deterministic("IMPORTER_UNKNOWN", "contract_bind"))?;
        if self.phase != IntentPhase::ImportsPending
            || importer.phase != ImporterPhase::SnapshotSetFirst
            || self.snapshot_identifier.as_ref() != Some(snapshot_identifier)
            || snapshot_schema_fingerprint.is_empty()
            || snapshot_schema_fingerprint.len() > MAX_IDENTITY_BYTES
        {
            return Err(BootstrapFailure::deterministic(
                "IMPORT_CONTRACT_BINDING_MISMATCH",
                "contract_bind",
            ));
        }
        importer.snapshot_schema_fingerprint = Some(snapshot_schema_fingerprint.into());
        importer.phase = ImporterPhase::ContractBound;
        Ok(())
    }

    pub fn acknowledge_import(
        &mut self,
        scope: &OperationScope,
        worker: u16,
        assigned_range: &str,
        snapshot_schema_fingerprint: &str,
    ) -> Result<(), BootstrapFailure> {
        if !self.scope_matches(scope) {
            return Err(BootstrapFailure::deterministic(
                "STALE_IMPORTER_COMPLETION",
                "import_ack",
            ));
        }
        let importer = self
            .importers
            .get_mut(&worker)
            .ok_or_else(|| BootstrapFailure::deterministic("IMPORTER_UNKNOWN", "import_ack"))?;
        if self.phase != IntentPhase::ImportsPending
            || importer.phase != ImporterPhase::ContractBound
            || importer.assigned_range != assigned_range
            || importer.snapshot_schema_fingerprint.as_deref() != Some(snapshot_schema_fingerprint)
        {
            return Err(BootstrapFailure::deterministic(
                "IMPORT_ACK_CONTRACT_MISMATCH",
                "import_ack",
            ));
        }
        importer.phase = ImporterPhase::Acknowledged;
        if self
            .importers
            .values()
            .all(|i| i.phase == ImporterPhase::Acknowledged)
        {
            self.phase = IntentPhase::ImportsComplete;
            self.phase = IntentPhase::ExporterReleasePermitted;
            self.importer_feedback_gate = false;
        }
        Ok(())
    }

    pub fn record_durable_wal(
        &mut self,
        scope: &OperationScope,
        boundary: DurableSourceBoundary,
        event_id: Option<&str>,
    ) -> Result<(), BootstrapFailure> {
        if !self.scope_matches(scope) {
            return Err(BootstrapFailure::deterministic(
                "STALE_WAL_COMPLETION",
                "journal_transaction_boundary",
            ));
        }
        if !self.capture_connection_started {
            return Err(BootstrapFailure::deterministic(
                "WAL_NOT_DURABLE",
                "journal_transaction_boundary",
            ));
        }
        if self.durable_wal_end.is_some_and(|prior| {
            prior.transaction_end_lsn().get() > boundary.transaction_end_lsn().get()
                || prior.journal_cursor().get() > boundary.journal_cursor().get()
                || (prior.transaction_end_lsn().get() == boundary.transaction_end_lsn().get()
                    && prior.journal_cursor().get() != boundary.journal_cursor().get())
                || (prior.journal_cursor().get() == boundary.journal_cursor().get()
                    && prior.transaction_end_lsn().get() != boundary.transaction_end_lsn().get())
        }) {
            return Err(BootstrapFailure::deterministic(
                "DURABLE_WAL_REGRESSION",
                "journal_transaction_boundary",
            ));
        }
        if event_id
            .is_some_and(|id| id.is_empty() || id.len() > MAX_IDENTITY_BYTES || !id.is_ascii())
        {
            return Err(BootstrapFailure::deterministic(
                "WAL_EVENT_ID_INVALID",
                "journal_transaction_boundary",
            ));
        }
        self.durable_wal_end = Some(boundary);
        if let Some(id) = event_id {
            self.transient_wal_event_ids.insert(id.into());
        }
        Ok(())
    }

    pub fn feedback(&mut self, requested: bool) -> Result<Option<ReceivedLsn>, BootstrapFailure> {
        if self.importer_feedback_gate {
            // Requested replies may use the protocol zero sentinel; never proactively ack a floor.
            return Ok(requested.then(|| ReceivedLsn::from_wire(0)));
        }
        let end = self.durable_wal_end.ok_or_else(|| {
            BootstrapFailure::deterministic("NO_DURABLE_WAL_FOR_FEEDBACK", "feedback")
        })?;
        let end_lsn = end.transaction_end_lsn();
        self.feedback_lsn = Some(end_lsn);
        Ok(Some(end_lsn))
    }

    pub fn release_exporter(&mut self, scope: &OperationScope) -> Result<(), BootstrapFailure> {
        if !self.scope_matches(scope) {
            return Err(BootstrapFailure::deterministic(
                "STALE_EXPORTER_COMPLETION",
                "exporter_release",
            ));
        }
        if self.phase != IntentPhase::ExporterReleasePermitted {
            return Err(BootstrapFailure::deterministic(
                "EXPORTER_RELEASE_TOO_EARLY",
                "exporter_release",
            ));
        }
        self.exporter = ExporterLiveness::Released;
        self.phase = IntentPhase::ExporterReleased;
        Ok(())
    }

    pub fn exporter_lost(&mut self, scope: &OperationScope) {
        if !self.scope_matches(scope) {
            return;
        }
        self.exporter = ExporterLiveness::Lost;
        if self.phase != IntentPhase::ExporterReleasePermitted
            && self.phase != IntentPhase::ExporterReleased
        {
            self.invalidate("EXPORTER_LOST_BEFORE_IMPORT_ACKS");
        }
    }

    pub fn importer_reads_complete(
        &mut self,
        scope: &OperationScope,
        worker: u16,
    ) -> Result<(), BootstrapFailure> {
        if !self.scope_matches(scope) {
            return Err(BootstrapFailure::deterministic(
                "STALE_IMPORTER_COMPLETION",
                "import_reads",
            ));
        }
        let importer = self
            .importers
            .get_mut(&worker)
            .ok_or_else(|| BootstrapFailure::deterministic("IMPORTER_UNKNOWN", "import_reads"))?;
        if importer.phase != ImporterPhase::Acknowledged {
            self.invalidate("IMPORTER_DIED_OR_COMPLETED_WITHOUT_ACK");
            return Err(BootstrapFailure::deterministic(
                "IMPORTER_READS_WITHOUT_ACK",
                "import_reads",
            ));
        }
        importer.phase = ImporterPhase::ReadsComplete;
        Ok(())
    }

    pub fn guard_lost(&mut self, scope: &OperationScope) {
        if !self.scope_matches(scope) {
            return;
        }
        if self.phase == IntentPhase::AnchorComplete {
            return;
        }
        self.guard = GuardLiveness::Lost;
        self.invalidate("DDL_GUARD_LOST_BEFORE_FENCE");
    }

    pub fn importer_lost(
        &mut self,
        scope: &OperationScope,
        worker: u16,
    ) -> Result<(), BootstrapFailure> {
        if !self.scope_matches(scope) {
            return Ok(());
        }
        let importer = self
            .importers
            .get(&worker)
            .ok_or_else(|| BootstrapFailure::deterministic("IMPORTER_UNKNOWN", "import_reads"))?;
        if importer.phase != ImporterPhase::ReadsComplete {
            self.invalidate("IMPORTER_LOST_BEFORE_ASSIGNED_READS_COMPLETE");
        }
        Ok(())
    }

    pub fn mark_chunks_complete(&mut self) -> Result<(), BootstrapFailure> {
        if !self
            .importers
            .values()
            .all(|i| i.phase == ImporterPhase::ReadsComplete)
            || self.exporter != ExporterLiveness::Released
            || !self.snapshot_events_promotable
        {
            return Err(BootstrapFailure::deterministic(
                "CHUNKS_BEFORE_IMPORT_READS_COMPLETE",
                "snapshot_chunks",
            ));
        }
        self.chunks_complete = true;
        Ok(())
    }

    pub fn intend_fence(
        &mut self,
        nonce: u64,
    ) -> Result<GuardFenceAuthorization, BootstrapFailure> {
        if !self.chunks_complete
            || nonce == 0
            || self.guard != GuardLiveness::Held
            || self.intended_fence_nonce.is_some()
        {
            return Err(BootstrapFailure::deterministic(
                "FENCE_INTENT_INVALID",
                "post_copy_fence",
            ));
        }
        let authorization = self
            .guard_state
            .as_ref()
            .ok_or_else(|| {
                BootstrapFailure::deterministic("DDL_GUARD_PROOF_MISSING", "post_copy_fence")
            })?
            .authorize_fence_intent(
                self.capture_epoch.get(),
                self.generation,
                &self.table_set_fingerprint,
                nonce,
            )
            .map_err(|_| {
                BootstrapFailure::deterministic("FENCE_INTENT_INVALID", "post_copy_fence")
            })?;
        self.guard_state
            .as_mut()
            .expect("guard authorization checked above")
            .advance(GuardPhase::AwaitingDurableFence)
            .map_err(|_| {
                BootstrapFailure::deterministic("DDL_GUARD_PHASE_MISMATCH", "post_copy_fence")
            })?;
        self.intended_fence_nonce = Some(nonce);
        self.phase = IntentPhase::FencePending;
        Ok(authorization)
    }

    pub fn observe_durable_fence(
        &mut self,
        completion: DurableFenceCompletion,
    ) -> Result<(), BootstrapFailure> {
        if self.phase != IntentPhase::FencePending
            || !self.scope_matches(&completion.scope)
            || completion.table_set_fingerprint != self.table_set_fingerprint
            || self.intended_fence_nonce != Some(completion.nonce)
            || self.guard != GuardLiveness::Held
        {
            return Err(BootstrapFailure::deterministic(
                "DURABLE_FENCE_MISMATCH",
                "post_copy_fence",
            ));
        }
        let boundary_not_below = |lower: DurableSourceBoundary| {
            completion.boundary.transaction_end_lsn().get() >= lower.transaction_end_lsn().get()
                && completion.boundary.journal_cursor().get() >= lower.journal_cursor().get()
        };
        if self
            .durable_wal_end
            .is_some_and(|lower| !boundary_not_below(lower))
            || self
                .lower_stitch
                .is_some_and(|lower| !boundary_not_below(lower))
            || self
                .start_seq
                .is_some_and(|start| completion.boundary.journal_cursor().get() < start.get())
            || self
                .snapshot_boundary
                .is_some_and(|floor| completion.boundary.transaction_end_lsn().get() < floor.get())
        {
            return Err(BootstrapFailure::deterministic(
                "DURABLE_FENCE_BOUNDARY_REGRESSION",
                "post_copy_fence",
            ));
        }
        let guard_proof = DurableFenceProof::from_journal_commit(
            self.capture_epoch.get(),
            self.generation,
            self.table_set_fingerprint.clone(),
            completion.nonce,
            completion.boundary,
        );
        self.guard_state
            .as_mut()
            .ok_or_else(|| {
                BootstrapFailure::deterministic("DDL_GUARD_PROOF_MISSING", "post_copy_fence")
            })?
            .durable_fence_observed(&guard_proof)
            .map_err(|_| {
                BootstrapFailure::deterministic("DDL_GUARD_FENCE_MISMATCH", "post_copy_fence")
            })?;
        self.durable_fence = Some((
            completion.nonce,
            completion.boundary.transaction_end_lsn(),
            completion.boundary.journal_cursor(),
        ));
        self.guard = GuardLiveness::Released;
        self.phase = IntentPhase::AnchorComplete;
        Ok(())
    }

    pub fn restart_decision(
        &mut self,
        proof: Option<VerifiedRetainedSlotContinuity>,
    ) -> RecoveryDecision {
        let pretoken_guard_held =
            self.guard == GuardLiveness::Held && !self.snapshot_token_persisted;
        self.record_dead_sessions(true);
        if pretoken_guard_held {
            self.guard_state = None;
            self.ownership_locks_held = false;
        }
        if self.snapshot_token_persisted
            && self.snapshot_events_promotable
            && self.phase != IntentPhase::AnchorComplete
        {
            self.exporter = ExporterLiveness::Lost;
            self.guard = GuardLiveness::Lost;
            self.invalidate("PROCESS_RESTART_DESTROYED_SNAPSHOT_SESSIONS");
        }
        if self.phase == IntentPhase::Prepared && !self.remote_slot_exists {
            return RecoveryDecision::RetryPreparedCreation;
        }
        if self.phase == IntentPhase::Prepared
            && self.remote_slot_exists
            && !self.snapshot_token_persisted
        {
            self.phase = IntentPhase::BootstrapAmbiguousRequiresRestart;
            return RecoveryDecision::BootstrapAmbiguousRequiresRestart;
        }
        if self.phase == IntentPhase::SnapshotUnusable
            || self.phase == IntentPhase::BootstrapAmbiguousRequiresRestart
        {
            if proof.is_some_and(|proof| {
                proof.matches(
                    self.capture_epoch,
                    &self.intent_id,
                    &self.expected_source_identity,
                )
            }) {
                self.continuity_verified = true;
                self.phase = IntentPhase::ExistingSlotGenerationRequired;
                return RecoveryDecision::DrainRetainedWalThenExistingSlotSnapshot;
            }
            self.phase = IntentPhase::FullReseedRequired;
            return RecoveryDecision::RequireConfirmedFullReseed;
        }
        if matches!(
            self.phase,
            IntentPhase::FullReseedRequired | IntentPhase::FullReseedConfirmed
        ) {
            return RecoveryDecision::RequireConfirmedFullReseed;
        }
        RecoveryDecision::ResumeEligibleGeneration
    }

    /// Complete bounded reconciliation of every backend known dead before any replacement
    /// snapshot/session is opened.
    pub fn complete_stale_backend_cleanup(
        &mut self,
        scope: &OperationScope,
        elapsed_ms: u64,
        all_confirmed_terminated: bool,
    ) -> Result<(), BootstrapFailure> {
        let cleanup_bound = self
            .session_bounds
            .exporter_lifetime_ms
            .max(self.session_bounds.importer_lifetime_ms)
            .max(self.session_bounds.guard_lifetime_ms);
        if !self.scope_matches(scope)
            || !self.stale_backend_cleanup_required
            || !all_confirmed_terminated
            || elapsed_ms > cleanup_bound
        {
            return Err(BootstrapFailure::deterministic(
                "STALE_BACKEND_CLEANUP_INCOMPLETE",
                "restart_reconciliation",
            ));
        }
        self.dead_backends.clear();
        self.stale_backend_cleanup_required = false;
        Ok(())
    }

    /// Lost-token recovery starts CopyBoth on the retained permanent slot, never on the dead
    /// creation exporter. This path is intentionally distinct from snapshot-generation capture.
    pub fn start_retained_slot_capture(
        &mut self,
        scope: &OperationScope,
        separate_connection: bool,
    ) -> Result<(), BootstrapFailure> {
        if !self.scope_matches(scope)
            || self.phase != IntentPhase::ExistingSlotGenerationRequired
            || !self.continuity_verified
            || self.stale_backend_cleanup_required
            || self.capture_connection_started
            || !separate_connection
        {
            return Err(BootstrapFailure::deterministic(
                "RETAINED_SLOT_CAPTURE_NOT_AUTHORIZED",
                "retained_slot_drain",
            ));
        }
        self.capture_connection_started = true;
        Ok(())
    }

    pub fn retained_wal_drained(
        &mut self,
        scope: &OperationScope,
        lower_boundary: DurableSourceBoundary,
    ) -> Result<(), BootstrapFailure> {
        if !self.scope_matches(scope)
            || self.phase != IntentPhase::ExistingSlotGenerationRequired
            || !self.continuity_verified
            || !self.capture_connection_started
            || self.stale_backend_cleanup_required
            || self.durable_wal_end != Some(lower_boundary)
        {
            return Err(BootstrapFailure::deterministic(
                "LOWER_STITCH_NOT_DURABLE",
                "retained_slot_drain",
            ));
        }
        self.lower_stitch = Some(lower_boundary);
        self.generation = self.generation.checked_add(1).ok_or_else(|| {
            BootstrapFailure::deterministic("GENERATION_OVERFLOW", "retained_slot_drain")
        })?;
        self.phase = IntentPhase::RetainedWalDrained;
        Ok(())
    }

    pub fn begin_existing_slot_snapshot(
        &mut self,
        guard: GuardState,
        snapshot_identifier: ExportedSnapshotIdentifier,
    ) -> Result<(), BootstrapFailure> {
        if self.phase != IntentPhase::RetainedWalDrained
            || self.stale_backend_cleanup_required
            || !guard.proves_bootstrap_binding(
                self.capture_epoch.get(),
                self.generation,
                &self.table_set_fingerprint,
            )
        {
            return Err(BootstrapFailure::deterministic(
                "EXISTING_SLOT_SNAPSHOT_PROOF_INVALID",
                "existing_slot_snapshot",
            ));
        }
        self.guard_state = Some(guard);
        self.guard = GuardLiveness::Held;
        self.guard_state
            .as_mut()
            .expect("set above")
            .advance(GuardPhase::Exported)
            .and_then(|_| {
                self.guard_state
                    .as_mut()
                    .expect("set above")
                    .advance(GuardPhase::Copying)
            })
            .map_err(|_| {
                BootstrapFailure::deterministic(
                    "DDL_GUARD_PHASE_MISMATCH",
                    "existing_slot_snapshot",
                )
            })?;
        self.snapshot_identifier = Some(snapshot_identifier);
        self.snapshot_token_persisted = true;
        self.snapshot_boundary = None;
        self.start_seq = None;
        self.exporter = ExporterLiveness::CommandIdle;
        self.snapshot_events_promotable = true;
        self.importer_feedback_gate = false;
        for importer in self.importers.values_mut() {
            importer.phase = ImporterPhase::Assigned;
            importer.snapshot_schema_fingerprint = None;
        }
        self.phase = IntentPhase::ImportsPending;
        Ok(())
    }

    pub fn confirm_full_reseed(&mut self, new_epoch: CaptureEpoch) -> Result<(), BootstrapFailure> {
        if self.phase != IntentPhase::FullReseedRequired
            || new_epoch.get() == 0
            || new_epoch.get() <= self.capture_epoch.get()
        {
            return Err(BootstrapFailure::deterministic(
                "FULL_RESEED_CONFIRMATION_INVALID",
                "continuity_break",
            ));
        }
        self.capture_epoch = new_epoch;
        self.continuity_break_recorded = true;
        self.phase = IntentPhase::FullReseedConfirmed;
        Ok(())
    }

    fn record_dead_sessions(&mut self, include_capture: bool) {
        if self.remote_slot_exists
            && matches!(
                self.exporter,
                ExporterLiveness::CommandIdle | ExporterLiveness::Lost
            )
        {
            self.dead_backends.insert(BackendSession::Exporter);
            self.exporter = ExporterLiveness::Lost;
        }
        if matches!(self.guard, GuardLiveness::Held | GuardLiveness::Lost) {
            self.dead_backends.insert(BackendSession::Guard);
            self.guard = GuardLiveness::Lost;
        }
        for (&worker, importer) in &self.importers {
            if importer.phase != ImporterPhase::ReadsComplete {
                self.dead_backends.insert(BackendSession::Importer(worker));
            }
        }
        if include_capture && self.capture_connection_started {
            self.dead_backends.insert(BackendSession::Capture);
            self.capture_connection_started = false;
        }
        self.stale_backend_cleanup_required = !self.dead_backends.is_empty();
    }

    fn invalidate(&mut self, _reason: &'static str) {
        self.record_dead_sessions(false);
        self.phase = IntentPhase::SnapshotUnusable;
        self.snapshot_events_promotable = false;
        self.importer_feedback_gate = false;
    }

    #[must_use]
    pub fn invariant_violation(&self) -> Option<&'static str> {
        if self.feedback_lsn.is_some() && self.importer_feedback_gate {
            return Some("FEEDBACK_CROSSED_IMPORTER_GATE");
        }
        if self.phase == IntentPhase::AnchorComplete
            && (!self.chunks_complete
                || self.durable_fence.is_none()
                || self.guard != GuardLiveness::Released
                || self.lower_anchor().is_none())
        {
            return Some("ANCHOR_WITHOUT_COMPLETE_PROOF");
        }
        if self.snapshot_events_promotable && self.phase == IntentPhase::SnapshotUnusable {
            return Some("INVALID_SNAPSHOT_PROMOTABLE");
        }
        if self.creation_floor.is_some() && self.snapshot_boundary != self.creation_floor {
            return Some("CREATION_FLOOR_BOUNDARY_MISMATCH");
        }
        None
    }

    fn lower_anchor(&self) -> Option<(u64, u64)> {
        self.snapshot_boundary
            .zip(self.start_seq)
            .map(|(lsn, seq)| (lsn.get(), seq.get()))
            .or_else(|| {
                self.lower_stitch.map(|boundary| {
                    (
                        boundary.transaction_end_lsn().get(),
                        boundary.journal_cursor().get(),
                    )
                })
            })
    }
}

#[derive(Clone, Debug)]
pub enum BootstrapEvent {
    ExporterLost(OperationScope),
    GuardLost(OperationScope),
}
#[derive(Clone, Debug)]
pub enum BootstrapCompletion {
    DurableWal {
        scope: OperationScope,
        boundary: DurableSourceBoundary,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BootstrapEffect {
    Persist,
}

pub struct BootstrapDomain;
impl TransitionSystem for BootstrapDomain {
    type Facts = BootstrapFacts;
    type Event = BootstrapEvent;
    type Effect = BootstrapEffect;
    type Completion = BootstrapCompletion;

    fn on_event(
        &self,
        facts: &mut Self::Facts,
        event: Self::Event,
        _context: &mut TransitionContext<'_>,
    ) -> Vec<Self::Effect> {
        match event {
            BootstrapEvent::ExporterLost(scope) => facts.exporter_lost(&scope),
            BootstrapEvent::GuardLost(scope) => facts.guard_lost(&scope),
        }
        vec![BootstrapEffect::Persist]
    }
    fn on_completion(
        &self,
        facts: &mut Self::Facts,
        completion: Self::Completion,
        _context: &mut TransitionContext<'_>,
    ) -> Vec<Self::Effect> {
        match completion {
            BootstrapCompletion::DurableWal { scope, boundary } => {
                let _ = facts.record_durable_wal(&scope, boundary, None);
            }
        }
        vec![BootstrapEffect::Persist]
    }
    fn on_expiry(&self, facts: &mut Self::Facts, _context: &mut TransitionContext<'_>) {
        facts.invalidate("SESSION_LIFETIME_EXPIRED");
        facts.record_dead_sessions(true);
    }
    fn on_cancel(&self, facts: &mut Self::Facts, _context: &mut TransitionContext<'_>) {
        facts.invalidate("GENERATION_CANCELLED");
        facts.record_dead_sessions(true);
    }
    fn on_crash_restart(&self, facts: &mut Self::Facts, _context: &mut TransitionContext<'_>) {
        let _ = facts.restart_decision(None);
    }
    fn invariant_violation(&self, facts: &Self::Facts) -> Option<String> {
        facts.invariant_violation().map(str::to_owned)
    }
    fn redacted_state(&self, facts: &Self::Facts) -> String {
        format!(
            "phase={:?};exporter={:?};guard={:?};gate={}",
            facts.phase, facts.exporter, facts.guard, facts.importer_feedback_gate
        )
    }
    fn facts_size_bytes(&self, facts: &Self::Facts) -> usize {
        const TREE_OVERHEAD_PER_ENTRY: usize = 3 * std::mem::size_of::<usize>();
        std::mem::size_of::<Self::Facts>()
            + facts.intent_id.capacity()
            + facts.slot_identity.capacity()
            + facts.table_set_fingerprint.capacity()
            + facts.config_fingerprint.capacity()
            + facts.expected_source_identity.slot_name.capacity()
            + facts.expected_source_identity.plugin.capacity()
            + facts.source_identity.as_ref().map_or(0, |identity| {
                identity.slot_name.capacity() + identity.plugin.capacity()
            })
            + facts
                .snapshot_identifier
                .as_ref()
                .map_or(0, |v| v.0.capacity())
            + facts
                .guard_state
                .as_ref()
                .map_or(0, GuardState::owned_allocation_bytes)
            + facts.importers.len() * TREE_OVERHEAD_PER_ENTRY
            + facts.dead_backends.len()
                * (TREE_OVERHEAD_PER_ENTRY + std::mem::size_of::<BackendSession>())
            + facts
                .importers
                .values()
                .map(|v| {
                    TREE_OVERHEAD_PER_ENTRY
                        + std::mem::size_of_val(v)
                        + v.assigned_range.capacity()
                        + v.snapshot_schema_fingerprint
                            .as_ref()
                            .map_or(0, String::capacity)
                })
                .sum::<usize>()
            + facts
                .transient_wal_event_ids
                .iter()
                .map(|v| TREE_OVERHEAD_PER_ENTRY + std::mem::size_of::<String>() + v.capacity())
                .sum::<usize>()
    }
    fn event_size_bytes(&self, event: &Self::Event) -> usize {
        std::mem::size_of::<Self::Event>()
            + match event {
                BootstrapEvent::ExporterLost(scope) | BootstrapEvent::GuardLost(scope) => {
                    scope.intent_id.capacity()
                }
            }
    }
    fn completion_size_bytes(&self, completion: &Self::Completion) -> usize {
        std::mem::size_of::<Self::Completion>()
            + match completion {
                BootstrapCompletion::DurableWal { scope, .. } => scope.intent_id.capacity(),
            }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m1_ddl_fixtures::GuardState;
    use crate::m1_transition_kernel::{
        Harness, HarnessBudget, ScheduleSeed, ScheduledAction, ScheduledStep, SplitMix64,
        VirtualClock, synthetic_durable_boundary,
    };

    fn prepared(workers: usize) -> BootstrapFacts {
        BootstrapFacts::prepare(PrepareInput {
            intent_id: "intent-1".into(),
            capture_epoch: CaptureEpoch::from_store(7),
            generation: 1,
            slot_identity: "boring_slot".into(),
            publication_fingerprint: test_identity().publication_fingerprint,
            expected_source_identity: test_identity(),
            table_set_fingerprint: "tables-fp".into(),
            config_fingerprint: "config-fp".into(),
            importer_ranges: (0..workers).map(|n| format!("range-{n}")).collect(),
            session_bounds: SessionBounds {
                exporter_lifetime_ms: 60_000,
                importer_lifetime_ms: 60_000,
                guard_lifetime_ms: 60_000,
                guard_keepalive_ms: 5_000,
            },
        })
        .unwrap()
    }

    fn exported(workers: usize) -> BootstrapFacts {
        let mut f = prepared(workers);
        f.acquire_guard(guard_proof()).unwrap();
        f.slot_created("boring_slot").unwrap();
        f.persist_export_response(
            SlotCreationFloor::from_server(100),
            CompleteJournalStart::FreshStoreZero,
            snapshot_id(),
            test_identity(),
        )
        .unwrap();
        f.start_capture(true).unwrap();
        f
    }

    fn acknowledge_all(f: &mut BootstrapFacts) {
        for id in 0..f.importers.len() as u16 {
            let range = format!("range-{id}");
            begin(f, id, true, true).unwrap();
            statement(f, id, StatementKind::SetTransactionSnapshot(snapshot_id())).unwrap();
            bind(f, id, &snapshot_id(), "schema-fp").unwrap();
            ack(f, id, &range, "schema-fp").unwrap();
        }
    }

    #[test]
    fn intent_guard_and_one_permanent_slot_precede_export() {
        let mut f = prepared(1);
        assert!(f.acquire_guard(guard_for("wrong-fp")).is_err());
        assert_eq!(
            f.slot_created("boring_slot").unwrap_err().fingerprint,
            "PERMANENT_SLOT_CREATION_NOT_AUTHORIZED"
        );
        f.acquire_guard(guard_proof()).unwrap();
        assert_eq!(
            f.slot_created("auxiliary_slot").unwrap_err().fingerprint,
            "PERMANENT_SLOT_CREATION_NOT_AUTHORIZED"
        );
        f.slot_created("boring_slot").unwrap();
        assert_eq!(f.exporter, ExporterLiveness::CommandIdle);
    }

    #[test]
    fn creation_floor_is_separate_and_capture_connection_is_separate() {
        let mut f = prepared(1);
        f.acquire_guard(guard_proof()).unwrap();
        f.slot_created("boring_slot").unwrap();
        f.persist_export_response(
            SlotCreationFloor::from_server(100),
            CompleteJournalStart::FreshStoreZero,
            snapshot_id(),
            test_identity(),
        )
        .unwrap();
        assert_eq!(f.creation_floor, Some(SlotCreationFloor::from_server(100)));
        assert!(f.durable_wal_end.is_none());
        assert!(f.start_capture(false).is_err());
        f.start_capture(true).unwrap();
        assert_eq!(f.exporter, ExporterLiveness::CommandIdle);
    }

    #[test]
    fn every_importer_requires_snapshot_as_first_statement_and_bound_ack() {
        let mut violated = exported(1);
        begin(&mut violated, 0, true, true).unwrap();
        assert_eq!(
            statement(&mut violated, 0, StatementKind::CatalogQuery)
                .unwrap_err()
                .fingerprint,
            "SNAPSHOT_IMPORT_NOT_FIRST_STATEMENT"
        );
        assert_eq!(violated.phase, IntentPhase::SnapshotUnusable);
        let mut wrong_snapshot = exported(1);
        begin(&mut wrong_snapshot, 0, true, true).unwrap();
        let other = ExportedSnapshotIdentifier::from_server("other-snapshot".into()).unwrap();
        assert!(
            statement(
                &mut wrong_snapshot,
                0,
                StatementKind::SetTransactionSnapshot(other)
            )
            .is_err()
        );
        assert_eq!(wrong_snapshot.phase, IntentPhase::SnapshotUnusable);
        let mut f = exported(2);
        begin(&mut f, 0, true, true).unwrap();
        statement(
            &mut f,
            0,
            StatementKind::SetTransactionSnapshot(snapshot_id()),
        )
        .unwrap();
        bind(&mut f, 0, &snapshot_id(), "schema-fp").unwrap();
        assert!(ack(&mut f, 0, "wrong-range", "schema-fp").is_err());
        ack(&mut f, 0, "range-0", "schema-fp").unwrap();
        assert!(f.importer_feedback_gate);
        begin(&mut f, 1, true, true).unwrap();
        statement(
            &mut f,
            1,
            StatementKind::SetTransactionSnapshot(snapshot_id()),
        )
        .unwrap();
        bind(&mut f, 1, &snapshot_id(), "schema-fp").unwrap();
        ack(&mut f, 1, "range-1", "schema-fp").unwrap();
        assert_eq!(f.phase, IntentPhase::ExporterReleasePermitted);
        assert!(!f.importer_feedback_gate);
    }

    #[test]
    fn feedback_is_gated_until_all_import_acks_then_uses_durable_wal_only() {
        let mut f = exported(1);
        record(&mut f, boundary(120, 2), None).unwrap();
        assert_eq!(
            record(&mut f, boundary(119, 3), None)
                .unwrap_err()
                .fingerprint,
            "DURABLE_WAL_REGRESSION"
        );
        assert_eq!(f.feedback(false).unwrap(), None);
        assert_eq!(f.feedback(true).unwrap(), Some(ReceivedLsn::from_wire(0)));
        assert!(f.feedback_lsn.is_none());
        acknowledge_all(&mut f);
        assert_eq!(
            f.feedback(false).unwrap(),
            Some(ReceivedLsn::from_wire(120))
        );
    }

    #[test]
    fn exporter_loss_before_each_ack_invalidates_and_releases_gate() {
        for acknowledged in 0..3 {
            let mut f = exported(3);
            for id in 0..acknowledged {
                begin(&mut f, id, true, true).unwrap();
                statement(
                    &mut f,
                    id,
                    StatementKind::SetTransactionSnapshot(snapshot_id()),
                )
                .unwrap();
                bind(&mut f, id, &snapshot_id(), "schema-fp").unwrap();
                ack(&mut f, id, &format!("range-{id}"), "schema-fp").unwrap();
            }
            lose_exporter(&mut f);
            assert_eq!(f.phase, IntentPhase::SnapshotUnusable);
            assert!(!f.snapshot_events_promotable);
            assert!(!f.importer_feedback_gate);
        }
    }

    #[test]
    fn normal_exporter_release_after_readback_does_not_invalidate() {
        let mut f = exported(2);
        acknowledge_all(&mut f);
        release(&mut f).unwrap();
        assert_eq!(f.phase, IntentPhase::ExporterReleased);
        assert!(f.snapshot_events_promotable);
    }

    #[test]
    fn crash_matrix_retries_or_marks_ambiguous_without_silent_attach() {
        let mut before = prepared(1);
        assert_eq!(
            before.restart_decision(no_proof()),
            RecoveryDecision::RetryPreparedCreation
        );
        let mut after_guard = prepared(1);
        after_guard.acquire_guard(guard_proof()).unwrap();
        assert_eq!(
            after_guard.restart_decision(no_proof()),
            RecoveryDecision::RetryPreparedCreation
        );
        assert_eq!(after_guard.guard, GuardLiveness::Lost);
        assert!(!after_guard.ownership_locks_held);
        assert!(after_guard.slot_created("boring_slot").is_err());
        cleanup(&mut after_guard, 1).unwrap();
        after_guard.reacquire_ownership_locks().unwrap();
        after_guard.acquire_guard(guard_proof()).unwrap();

        let mut after_response = prepared(1);
        after_response.acquire_guard(guard_proof()).unwrap();
        after_response.slot_created("boring_slot").unwrap();
        assert_eq!(
            after_response.restart_decision(no_proof()),
            RecoveryDecision::BootstrapAmbiguousRequiresRestart
        );
        assert_eq!(
            after_response.phase,
            IntentPhase::BootstrapAmbiguousRequiresRestart
        );

        let mut crash_points = Vec::new();
        crash_points.push(exported(2)); // token persisted / imports pending
        let mut after_ack = exported(2);
        begin(&mut after_ack, 0, true, true).unwrap();
        statement(
            &mut after_ack,
            0,
            StatementKind::SetTransactionSnapshot(snapshot_id()),
        )
        .unwrap();
        bind(&mut after_ack, 0, &snapshot_id(), "schema-fp").unwrap();
        ack(&mut after_ack, 0, "range-0", "schema-fp").unwrap();
        crash_points.push(after_ack);
        let mut after_all_acks = exported(1);
        acknowledge_all(&mut after_all_acks);
        crash_points.push(after_all_acks.clone());
        release(&mut after_all_acks).unwrap();
        crash_points.push(after_all_acks.clone());
        record(&mut after_all_acks, boundary(120, 2), None).unwrap();
        after_all_acks.feedback(false).unwrap();
        crash_points.push(after_all_acks.clone());
        finish_reads(&mut after_all_acks, 0).unwrap();
        after_all_acks.mark_chunks_complete().unwrap();
        after_all_acks.intend_fence(77).unwrap();
        crash_points.push(after_all_acks);
        for mut crashed in crash_points {
            assert_eq!(
                crashed.restart_decision(all_proof()),
                RecoveryDecision::DrainRetainedWalThenExistingSlotSnapshot
            );
            assert!(!crashed.snapshot_events_promotable);
            assert!(!crashed.importer_feedback_gate);
        }
    }

    #[test]
    fn retained_slot_recovery_requires_all_proofs_and_preserves_transient_wal() {
        let mut f = exported(1);
        record(&mut f, boundary(110, 1), Some("insert-42")).unwrap();
        record(&mut f, boundary(120, 2), Some("delete-42")).unwrap();
        lose_exporter(&mut f);
        assert_eq!(
            f.restart_decision(all_proof()),
            RecoveryDecision::DrainRetainedWalThenExistingSlotSnapshot
        );
        cleanup(&mut f, 1).unwrap();
        let scope = f.scope();
        f.start_retained_slot_capture(&scope, true).unwrap();
        drain_retained(&mut f, boundary(120, 2)).unwrap();
        assert_eq!(
            f.transient_wal_event_ids,
            BTreeSet::from(["delete-42".into(), "insert-42".into()])
        );
        assert_eq!(f.lower_stitch, Some(boundary(120, 2)));
        assert_eq!(f.capture_epoch, CaptureEpoch::from_store(7));
        assert_eq!(f.generation, 2);
        let guard = guard_for_facts(&f);
        f.begin_existing_slot_snapshot(guard, snapshot_id())
            .unwrap();
        assert_eq!(f.phase, IntentPhase::ImportsPending);
        assert_eq!(f.exporter, ExporterLiveness::CommandIdle);
        assert!(!f.importer_feedback_gate);
    }

    #[test]
    fn lost_creation_token_starts_retained_capture_and_replacement_snapshot() {
        let mut f = prepared(1);
        f.acquire_guard(guard_proof()).unwrap();
        f.slot_created("boring_slot").unwrap();
        assert_eq!(
            f.restart_decision(no_proof()),
            RecoveryDecision::BootstrapAmbiguousRequiresRestart
        );
        assert_eq!(
            f.restart_decision(lost_token_proof()),
            RecoveryDecision::DrainRetainedWalThenExistingSlotSnapshot
        );
        assert!(f.start_retained_slot_capture(&f.scope(), true).is_err());
        assert!(cleanup(&mut f, 60_001).is_err());
        cleanup(&mut f, 60_000).unwrap();
        let scope = f.scope();
        assert!(f.start_retained_slot_capture(&scope, false).is_err());
        f.start_retained_slot_capture(&scope, true).unwrap();
        record(&mut f, boundary(120, 2), Some("retained-event")).unwrap();
        drain_retained(&mut f, boundary(120, 2)).unwrap();
        f.begin_existing_slot_snapshot(guard_for_facts(&f), snapshot_id())
            .unwrap();
        assert_eq!(f.phase, IntentPhase::ImportsPending);
        assert_eq!(f.capture_epoch, CaptureEpoch::from_store(7));
        assert_eq!(f.generation, 2);
    }

    #[test]
    fn continuity_capability_binds_full_identity_and_active_intent() {
        let mut wrong_identity = prepared(1);
        wrong_identity.acquire_guard(guard_proof()).unwrap();
        wrong_identity.slot_created("boring_slot").unwrap();
        wrong_identity.restart_decision(no_proof());
        let mut identity = test_identity();
        identity.timeline = 2;
        assert_eq!(
            wrong_identity.restart_decision(proof_for(identity, "intent-1")),
            RecoveryDecision::RequireConfirmedFullReseed
        );

        let mut wrong_intent = prepared(1);
        wrong_intent.acquire_guard(guard_proof()).unwrap();
        wrong_intent.slot_created("boring_slot").unwrap();
        wrong_intent.restart_decision(no_proof());
        assert_eq!(
            wrong_intent.restart_decision(proof_for(test_identity(), "other-intent")),
            RecoveryDecision::RequireConfirmedFullReseed
        );
    }

    #[test]
    fn fence_nonce_and_complete_boundary_fail_closed_before_anchor() {
        let mut f = exported(1);
        acknowledge_all(&mut f);
        release(&mut f).unwrap();
        finish_reads(&mut f, 0).unwrap();
        f.mark_chunks_complete().unwrap();
        assert_eq!(
            f.intend_fence(78).unwrap_err().fingerprint,
            "FENCE_INTENT_INVALID"
        );
        assert_eq!(f.phase, IntentPhase::ExporterReleased);
        let authorization = f.intend_fence(77).unwrap();
        record(&mut f, boundary(160, 4), None).unwrap();
        assert_eq!(
            observe_fence(&mut f, authorization.clone(), 77, 159, 5)
                .unwrap_err()
                .fingerprint,
            "DURABLE_FENCE_BOUNDARY_REGRESSION"
        );
        assert_eq!(
            observe_fence(&mut f, authorization.clone(), 77, 160, 3)
                .unwrap_err()
                .fingerprint,
            "DURABLE_FENCE_BOUNDARY_REGRESSION"
        );
        observe_fence(&mut f, authorization, 77, 160, 4).unwrap();
    }

    #[test]
    fn durable_wal_rejects_lsn_and_journal_cursor_divergence() {
        let mut f = exported(1);
        record(&mut f, boundary(120, 2), None).unwrap();
        assert_eq!(
            record(&mut f, boundary(121, 1), None)
                .unwrap_err()
                .fingerprint,
            "DURABLE_WAL_REGRESSION"
        );
        assert_eq!(
            record(&mut f, boundary(120, 3), None)
                .unwrap_err()
                .fingerprint,
            "DURABLE_WAL_REGRESSION"
        );
        assert_eq!(f.durable_wal_end, Some(boundary(120, 2)));
    }

    #[test]
    fn harness_accounting_includes_guard_vector_capacity_and_dead_sessions() {
        let mut relations = Vec::with_capacity(8);
        relations.push(crate::m1_ddl_fixtures::LogicalRelationId {
            database_oid: 1,
            relation_oid: 1,
            logical_table_id: "table-1".into(),
        });
        let mut guard =
            GuardState::acquire_before_export(7, 1, "tables-fp".into(), 77, relations).unwrap();
        guard.verify_catalog_fingerprint("tables-fp").unwrap();
        let guard_allocated = guard.owned_allocation_bytes();
        assert!(
            guard_allocated >= 8 * std::mem::size_of::<crate::m1_ddl_fixtures::LogicalRelationId>()
        );
        let mut f = prepared(1);
        f.acquire_guard(guard).unwrap();
        let before = BootstrapDomain.facts_size_bytes(&f);
        f.record_dead_sessions(true);
        let after = BootstrapDomain.facts_size_bytes(&f);
        assert!(after >= before + std::mem::size_of::<BackendSession>());
    }

    #[test]
    fn feedback_position_alone_never_selects_reseed_or_slot_drop() {
        for durable in [None, Some(boundary(100, 1)), Some(boundary(999, 2))] {
            let mut f = exported(1);
            f.durable_wal_end = durable;
            lose_exporter(&mut f);
            assert_eq!(
                f.restart_decision(all_proof()),
                RecoveryDecision::DrainRetainedWalThenExistingSlotSnapshot
            );
            assert!(f.remote_slot_exists);
        }
    }

    #[test]
    fn failed_continuity_requires_confirmed_new_epoch_full_reseed() {
        let mut f = exported(1);
        lose_exporter(&mut f);
        assert_eq!(
            f.restart_decision(no_proof()),
            RecoveryDecision::RequireConfirmedFullReseed
        );
        assert_eq!(
            f.restart_decision(all_proof()),
            RecoveryDecision::RequireConfirmedFullReseed
        );
        assert!(f.confirm_full_reseed(CaptureEpoch::from_store(7)).is_err());
        assert!(f.confirm_full_reseed(CaptureEpoch::from_store(6)).is_err());
        f.confirm_full_reseed(CaptureEpoch::from_store(8)).unwrap();
        assert!(f.continuity_break_recorded);
        assert!(
            f.remote_slot_exists,
            "state machine never silently drops the slot"
        );
    }

    #[test]
    fn guard_is_not_an_importer_and_loss_invalidates_through_fence() {
        let mut f = exported(1);
        assert_eq!(f.importers.len(), 1);
        assert_eq!(f.guard, GuardLiveness::Held);
        lose_guard(&mut f);
        assert_eq!(f.phase, IntentPhase::SnapshotUnusable);
        assert!(!f.importer_feedback_gate);
    }

    #[test]
    fn importer_ack_reordering_and_stale_inputs_preserve_gate() {
        let mut f = exported(3);
        for id in [2, 0, 1] {
            begin(&mut f, id, true, true).unwrap();
            statement(
                &mut f,
                id,
                StatementKind::SetTransactionSnapshot(snapshot_id()),
            )
            .unwrap();
            bind(&mut f, id, &snapshot_id(), "schema-fp").unwrap();
            ack(&mut f, id, &format!("range-{id}"), "schema-fp").unwrap();
        }
        assert_eq!(f.phase, IntentPhase::ExporterReleasePermitted);
        assert!(ack(&mut f, 2, "range-2", "schema-fp").is_err());
        assert_eq!(f.phase, IntentPhase::ExporterReleasePermitted);
    }

    #[test]
    fn anchor_requires_reads_lower_stitch_and_matching_durable_fence() {
        let mut f = exported(2);
        acknowledge_all(&mut f);
        release(&mut f).unwrap();
        for id in 0..2 {
            finish_reads(&mut f, id).unwrap();
        }
        f.mark_chunks_complete().unwrap();
        let authorization = f.intend_fence(77).unwrap();
        observe_fence(&mut f, authorization, 77, 150, 3).unwrap();
        assert_eq!(f.phase, IntentPhase::AnchorComplete);
        assert_eq!(f.guard, GuardLiveness::Released);
        assert_eq!(f.invariant_violation(), None);
    }

    #[test]
    fn stale_completions_and_fence_proofs_cannot_cross_generation() {
        let mut f = exported(1);
        let stale = OperationScope {
            intent_id: f.intent_id.clone(),
            capture_epoch: CaptureEpoch::from_store(6),
            generation: f.generation,
        };
        assert_eq!(
            f.record_durable_wal(&stale, boundary(120, 2), None)
                .unwrap_err()
                .fingerprint,
            "STALE_WAL_COMPLETION"
        );
        let phase = f.phase;
        f.exporter_lost(&stale);
        assert_eq!(f.phase, phase);
        acknowledge_all(&mut f);
        release(&mut f).unwrap();
        finish_reads(&mut f, 0).unwrap();
        f.mark_chunks_complete().unwrap();
        let authorization = f.intend_fence(77).unwrap();
        assert!(f.intend_fence(78).is_err());
        // A different guard can mint a stale capability, but bootstrap rejects its scoped journal proof.
        let stale_authorization = fence_authorization_for(6, f.generation, "wrong", 77);
        let bad = fence_completion_with(stale, "wrong".into(), stale_authorization, 77, 150, 3);
        assert!(f.observe_durable_fence(bad).is_err());
        observe_fence(&mut f, authorization, 77, 150, 3).unwrap();
    }

    #[test]
    fn snapshot_and_wal_origin_order_is_explicit() {
        assert_eq!(SNAPSHOT_ORIGIN_RANK, 0);
        assert_eq!(WAL_ORIGIN_RANK, 1);
        assert_eq!((SNAPSHOT_ORIGIN_RANK, WAL_ORIGIN_RANK), (0, 1));
    }

    #[test]
    fn bounded_harness_replays_exporter_and_guard_faults() {
        let f = exported(1);
        let budget = HarnessBudget {
            max_steps: 8,
            max_trace_entries: 8,
            max_minimizer_runs: 8,
            max_redacted_bytes: 512,
            max_state_bytes: 16_384,
            max_scheduled_payload_bytes: 4_096,
        };
        let steps = vec![
            ScheduledStep::new(
                "SCN-M1-BOOTSTRAP-EXPORTER-LOSS",
                ScheduledAction::Event(BootstrapEvent::ExporterLost(f.scope())),
            ),
            ScheduledStep::new(
                "SCN-M1-BOOTSTRAP-GUARD-LOSS",
                ScheduledAction::Event(BootstrapEvent::GuardLost(f.scope())),
            ),
        ];
        let first = Harness::new(budget)
            .unwrap()
            .execute(
                &BootstrapDomain,
                f.clone(),
                &steps,
                ScheduleSeed::splitmix64(0xB007),
            )
            .unwrap();
        let second = Harness::new(budget)
            .unwrap()
            .execute(
                &BootstrapDomain,
                f,
                &steps,
                ScheduleSeed::splitmix64(0xB007),
            )
            .unwrap();
        assert_eq!(first, second);
        assert!(first.entries.iter().all(|e| e.violation.is_none()));
    }

    #[test]
    fn importer_loss_after_ack_but_before_reads_complete_invalidates() {
        let mut f = exported(1);
        acknowledge_all(&mut f);
        assert_eq!(f.phase, IntentPhase::ExporterReleasePermitted);
        lose_importer(&mut f, 0).unwrap();
        assert_eq!(f.phase, IntentPhase::SnapshotUnusable);
        assert!(!f.snapshot_events_promotable);
        assert!(!f.importer_feedback_gate);
    }

    #[test]
    fn invalid_session_bounds_and_cancellation_are_bounded() {
        let mut input = PrepareInput {
            intent_id: "intent-1".into(),
            capture_epoch: CaptureEpoch::from_store(1),
            generation: 1,
            slot_identity: "slot".into(),
            publication_fingerprint: test_identity().publication_fingerprint,
            expected_source_identity: SourceIdentity {
                slot_name: "slot".into(),
                ..test_identity()
            },
            table_set_fingerprint: "tables".into(),
            config_fingerprint: "config".into(),
            importer_ranges: vec!["all".into()],
            session_bounds: SessionBounds {
                exporter_lifetime_ms: 10,
                importer_lifetime_ms: 10,
                guard_lifetime_ms: 10,
                guard_keepalive_ms: 10,
            },
        };
        assert!(BootstrapFacts::prepare(input.clone()).is_err());
        input.session_bounds.guard_keepalive_ms = 1;
        BootstrapFacts::prepare(input).unwrap();

        let mut cancelled = exported(1);
        let mut randomness = SplitMix64::new(1);
        let clock = VirtualClock::new(0);
        let mut context = TransitionContext {
            clock: &clock,
            randomness: &mut randomness,
        };
        BootstrapDomain.on_cancel(&mut cancelled, &mut context);
        assert_eq!(cancelled.phase, IntentPhase::SnapshotUnusable);
        assert!(cancelled.stale_backend_cleanup_required);
        assert!(cancelled.dead_backends.contains(&BackendSession::Capture));
        assert!(
            cancelled
                .begin_existing_slot_snapshot(guard_for_facts(&cancelled), snapshot_id())
                .is_err()
        );
        cleanup(&mut cancelled, 1).unwrap();

        let mut expired = exported(1);
        BootstrapDomain.on_expiry(&mut expired, &mut context);
        assert_eq!(expired.phase, IntentPhase::SnapshotUnusable);
        assert!(expired.stale_backend_cleanup_required);
        assert!(expired.dead_backends.contains(&BackendSession::Exporter));
        assert!(expired.dead_backends.contains(&BackendSession::Guard));
    }

    fn begin(
        f: &mut BootstrapFacts,
        worker: u16,
        read_only: bool,
        repeatable_read: bool,
    ) -> Result<(), BootstrapFailure> {
        let scope = f.scope();
        f.begin_importer_transaction(&scope, worker, read_only, repeatable_read)
    }
    fn statement(
        f: &mut BootstrapFacts,
        worker: u16,
        statement: StatementKind,
    ) -> Result<(), BootstrapFailure> {
        let scope = f.scope();
        f.importer_statement(&scope, worker, statement)
    }
    fn bind(
        f: &mut BootstrapFacts,
        worker: u16,
        snapshot: &ExportedSnapshotIdentifier,
        schema: &str,
    ) -> Result<(), BootstrapFailure> {
        let scope = f.scope();
        f.bind_import_contract(&scope, worker, snapshot, schema)
    }
    fn ack(
        f: &mut BootstrapFacts,
        worker: u16,
        range: &str,
        schema: &str,
    ) -> Result<(), BootstrapFailure> {
        let scope = f.scope();
        f.acknowledge_import(&scope, worker, range, schema)
    }
    fn release(f: &mut BootstrapFacts) -> Result<(), BootstrapFailure> {
        let scope = f.scope();
        f.release_exporter(&scope)
    }
    fn finish_reads(f: &mut BootstrapFacts, worker: u16) -> Result<(), BootstrapFailure> {
        let scope = f.scope();
        f.importer_reads_complete(&scope, worker)
    }

    fn record(
        f: &mut BootstrapFacts,
        boundary: DurableSourceBoundary,
        event_id: Option<&str>,
    ) -> Result<(), BootstrapFailure> {
        let scope = f.scope();
        f.record_durable_wal(&scope, boundary, event_id)
    }
    fn lose_exporter(f: &mut BootstrapFacts) {
        let scope = f.scope();
        f.exporter_lost(&scope);
    }
    fn lose_guard(f: &mut BootstrapFacts) {
        let scope = f.scope();
        f.guard_lost(&scope);
    }
    fn lose_importer(f: &mut BootstrapFacts, worker: u16) -> Result<(), BootstrapFailure> {
        let scope = f.scope();
        f.importer_lost(&scope, worker)
    }
    fn cleanup(f: &mut BootstrapFacts, elapsed_ms: u64) -> Result<(), BootstrapFailure> {
        let scope = f.scope();
        f.complete_stale_backend_cleanup(&scope, elapsed_ms, true)
    }
    fn drain_retained(
        f: &mut BootstrapFacts,
        boundary: DurableSourceBoundary,
    ) -> Result<(), BootstrapFailure> {
        let scope = f.scope();
        f.retained_wal_drained(&scope, boundary)
    }
    fn guard_for_facts(f: &BootstrapFacts) -> GuardState {
        let relation = crate::m1_ddl_fixtures::LogicalRelationId {
            database_oid: 1,
            relation_oid: 1,
            logical_table_id: "table-1".into(),
        };
        let mut guard = GuardState::acquire_before_export(
            f.capture_epoch.get(),
            f.generation,
            f.table_set_fingerprint.clone(),
            77,
            vec![relation],
        )
        .unwrap();
        guard
            .verify_catalog_fingerprint(&f.table_set_fingerprint)
            .unwrap();
        guard
    }
    fn fence_authorization_for(
        epoch: u64,
        generation: u64,
        table_fingerprint: &str,
        nonce: u64,
    ) -> GuardFenceAuthorization {
        let relation = crate::m1_ddl_fixtures::LogicalRelationId {
            database_oid: 1,
            relation_oid: 1,
            logical_table_id: "table-1".into(),
        };
        let mut guard = GuardState::acquire_before_export(
            epoch,
            generation,
            table_fingerprint.into(),
            nonce,
            vec![relation],
        )
        .unwrap();
        guard.verify_catalog_fingerprint(table_fingerprint).unwrap();
        guard.advance(GuardPhase::Exported).unwrap();
        guard.advance(GuardPhase::Copying).unwrap();
        guard
            .authorize_fence_intent(epoch, generation, table_fingerprint, nonce)
            .unwrap()
    }
    fn fence_completion_with(
        scope: OperationScope,
        table_set_fingerprint: String,
        authorization: GuardFenceAuthorization,
        nonce: u64,
        lsn: u64,
        seq: u64,
    ) -> DurableFenceCompletion {
        let identity = crate::m1_control_fixtures::FenceIdentity {
            capture_epoch: scope.capture_epoch.get(),
            generation: scope.generation,
            table_set_fingerprint: table_set_fingerprint.clone(),
        };
        let mut writer = crate::m1_control_fixtures::ControlWriterState::default();
        writer
            .intend_fence(&scope.intent_id, authorization)
            .unwrap();
        let update = crate::m1_control_fixtures::ObservedControlUpdate::fence(nonce, identity);
        let journaled = writer
            .observe_durable_fence(
                &update,
                boundary(lsn, seq),
                &crate::m1_control_fixtures::committed_for_fixture(),
            )
            .unwrap();
        DurableFenceCompletion::from_journaled_fence(journaled).unwrap()
    }
    fn observe_fence(
        f: &mut BootstrapFacts,
        authorization: GuardFenceAuthorization,
        nonce: u64,
        lsn: u64,
        seq: u64,
    ) -> Result<(), BootstrapFailure> {
        let completion = fence_completion_with(
            f.scope(),
            f.table_set_fingerprint.clone(),
            authorization,
            nonce,
            lsn,
            seq,
        );
        f.observe_durable_fence(completion)
    }

    fn test_identity() -> SourceIdentity {
        SourceIdentity {
            system_identifier: 1,
            timeline: 1,
            database_identity: 1,
            slot_name: "boring_slot".into(),
            plugin: "pgoutput".into(),
            publication_fingerprint: crate::m1_source_identity::publication_fingerprint(
                "boring_pub",
                b"definition",
            ),
            protocol_fingerprint: crate::m1_source_identity::supported_protocol_fingerprint(),
        }
    }
    fn snapshot_id() -> ExportedSnapshotIdentifier {
        ExportedSnapshotIdentifier::from_server("00000003-0000001B-1".into()).unwrap()
    }
    fn guard_proof() -> GuardState {
        guard_for("tables-fp")
    }
    fn guard_for(table_fingerprint: &str) -> GuardState {
        let relation = crate::m1_ddl_fixtures::LogicalRelationId {
            database_oid: 1,
            relation_oid: 1,
            logical_table_id: "table-1".into(),
        };
        let mut guard =
            GuardState::acquire_before_export(7, 1, table_fingerprint.into(), 77, vec![relation])
                .unwrap();
        guard.verify_catalog_fingerprint(table_fingerprint).unwrap();
        guard
    }
    fn boundary(lsn: u64, seq: u64) -> DurableSourceBoundary {
        synthetic_durable_boundary(ReceivedLsn::from_wire(lsn), JournalCursor::from_store(seq))
    }

    fn proof_for(
        identity: SourceIdentity,
        intent_id: &str,
    ) -> Option<VerifiedRetainedSlotContinuity> {
        let local_identity = test_identity();
        let local = crate::m1_source_identity::LocalSourceState {
            capture_epoch: CaptureEpoch::from_store(7),
            identity: local_identity,
            bootstrap: crate::m1_source_identity::BootstrapProvenance::PreparedWithoutPersistedFloorOrSnapshot,
            creation_floor: None,
            received_lsn: None,
            durable_transaction: None,
            feedback_position: crate::m1_source_identity::FeedbackPosition::ProtocolZero,
        };
        let live = crate::m1_source_identity::LiveSourceState {
            identity,
            slot_exists: true,
            slot_valid: true,
            resume_wal_available: true,
            confirmed_flush_lsn: None,
        };
        crate::m1_source_identity::verify_retained_slot_continuity(&local, &live, intent_id).ok()
    }

    fn lost_token_proof() -> Option<VerifiedRetainedSlotContinuity> {
        proof_for(test_identity(), "intent-1")
    }

    fn all_proof() -> Option<VerifiedRetainedSlotContinuity> {
        let identity = test_identity();
        let local = crate::m1_source_identity::LocalSourceState {
            capture_epoch: CaptureEpoch::from_store(7),
            identity: identity.clone(),
            bootstrap: crate::m1_source_identity::BootstrapProvenance::None,
            creation_floor: None,
            received_lsn: None,
            durable_transaction: None,
            feedback_position: crate::m1_source_identity::FeedbackPosition::ProtocolZero,
        };
        let live = crate::m1_source_identity::LiveSourceState {
            identity,
            slot_exists: true,
            slot_valid: true,
            resume_wal_available: true,
            confirmed_flush_lsn: None,
        };
        Some(
            crate::m1_source_identity::verify_retained_slot_continuity(&local, &live, "intent-1")
                .unwrap(),
        )
    }
    fn no_proof() -> Option<VerifiedRetainedSlotContinuity> {
        None
    }
}
