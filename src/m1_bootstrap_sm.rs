//! Pure initial-bootstrap/restart/re-seed transition specification.
//!
//! This leaf models durable facts and typed completions only. PostgreSQL sessions, SQLite
//! transactions, and runtime crash execution remain owned by M2/M3.

use crate::m1_transition_kernel::{
    CaptureEpoch, JournalCursor, ReceivedLsn, SlotCreationFloor, TransitionContext,
    TransitionSystem,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

// M0-PROVISIONAL: boring-cdc-d-backfill (RECOMMENDED bounded importer count).
pub const MAX_IMPORTERS: usize = 16;
// M0-PROVISIONAL: boring-cdc-d-backfill (RECOMMENDED explicit fresh-store boundary).
pub const ZERO_START_SEQ: JournalCursor = JournalCursor::from_store(0);
// M0-PROVISIONAL: boring-cdc-d-event-id (RECOMMENDED snapshot/WAL origin ranks).
pub const SNAPSHOT_ORIGIN_RANK: u8 = 0;
// M0-PROVISIONAL: boring-cdc-d-event-id (RECOMMENDED snapshot/WAL origin ranks).
pub const WAL_ORIGIN_RANK: u8 = 1;

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
    pub publication_fingerprint: String,
    pub table_set_fingerprint: String,
    pub config_fingerprint: String,
    pub session_bounds: SessionBounds,
    pub phase: IntentPhase,
    pub exporter: ExporterLiveness,
    pub guard: GuardLiveness,
    pub ownership_locks_held: bool,
    pub remote_slot_exists: bool,
    pub snapshot_token_persisted: bool,
    pub creation_floor: Option<SlotCreationFloor>,
    pub snapshot_boundary: Option<SlotCreationFloor>,
    pub start_seq: Option<JournalCursor>,
    pub capture_connection_started: bool,
    pub importers: BTreeMap<u16, ImporterState>,
    pub snapshot_events_promotable: bool,
    pub importer_feedback_gate: bool,
    pub durable_wal_end: Option<ReceivedLsn>,
    pub feedback_lsn: Option<ReceivedLsn>,
    pub chunks_complete: bool,
    pub intended_fence_nonce: Option<u64>,
    pub durable_fence: Option<(u64, ReceivedLsn, JournalCursor)>,
    pub transient_wal_event_ids: BTreeSet<String>,
    pub lower_stitch: Option<(ReceivedLsn, JournalCursor)>,
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
    pub publication_fingerprint: String,
    pub table_set_fingerprint: String,
    pub config_fingerprint: String,
    pub importer_ranges: Vec<String>,
    pub session_bounds: SessionBounds,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatementKind {
    SetTransactionSnapshot,
    CatalogQuery,
    DataQuery,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryProof {
    pub source_identity_matches: bool,
    pub slot_provenance_matches: bool,
    pub publication_protocol_matches: bool,
    pub required_wal_available: bool,
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
    pub fn prepare(input: PrepareInput) -> Result<Self, BootstrapFailure> {
        if input.intent_id.is_empty()
            || input.capture_epoch.get() == 0
            || input.generation == 0
            || input.slot_identity.is_empty()
            || input.publication_fingerprint.is_empty()
            || input.table_set_fingerprint.is_empty()
            || input.config_fingerprint.is_empty()
            || input.importer_ranges.is_empty()
            || input.importer_ranges.len() > MAX_IMPORTERS
            || input.importer_ranges.iter().any(String::is_empty)
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

    pub fn acquire_guard(&mut self) -> Result<(), BootstrapFailure> {
        if self.phase != IntentPhase::Prepared || !self.ownership_locks_held {
            return Err(BootstrapFailure::deterministic(
                "DDL_GUARD_MUST_PRECEDE_EXPORT",
                "guard_before_slot_creation",
            ));
        }
        self.guard = GuardLiveness::Held;
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
        self.remote_slot_exists = true;
        self.exporter = ExporterLiveness::CommandIdle;
        Ok(())
    }

    pub fn persist_export_response(
        &mut self,
        consistent_point: SlotCreationFloor,
        start_seq: JournalCursor,
    ) -> Result<(), BootstrapFailure> {
        if !self.remote_slot_exists
            || self.exporter != ExporterLiveness::CommandIdle
            || self.guard != GuardLiveness::Held
            || consistent_point.get() == 0
        {
            return Err(BootstrapFailure::deterministic(
                "EXPORT_RESPONSE_NOT_DURABLE",
                "snapshot_token_persistence",
            ));
        }
        self.creation_floor = Some(consistent_point);
        self.snapshot_boundary = Some(consistent_point);
        self.start_seq = Some(start_seq);
        self.snapshot_token_persisted = true;
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
        worker: u16,
        read_only: bool,
        repeatable_read: bool,
    ) -> Result<(), BootstrapFailure> {
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
        worker: u16,
        statement: StatementKind,
    ) -> Result<(), BootstrapFailure> {
        let importer = self.importers.get_mut(&worker).ok_or_else(|| {
            BootstrapFailure::deterministic("IMPORTER_UNKNOWN", "snapshot_import")
        })?;
        match (importer.phase, statement) {
            (ImporterPhase::TransactionBegun, StatementKind::SetTransactionSnapshot) => {
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

    pub fn acknowledge_import(
        &mut self,
        worker: u16,
        assigned_range: &str,
        snapshot_schema_fingerprint: &str,
    ) -> Result<(), BootstrapFailure> {
        let importer = self
            .importers
            .get_mut(&worker)
            .ok_or_else(|| BootstrapFailure::deterministic("IMPORTER_UNKNOWN", "import_ack"))?;
        if importer.phase != ImporterPhase::SnapshotSetFirst
            || importer.assigned_range != assigned_range
            || snapshot_schema_fingerprint.is_empty()
        {
            return Err(BootstrapFailure::deterministic(
                "IMPORT_ACK_CONTRACT_MISMATCH",
                "import_ack",
            ));
        }
        importer.snapshot_schema_fingerprint = Some(snapshot_schema_fingerprint.into());
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
        end_lsn: ReceivedLsn,
        event_id: Option<&str>,
    ) -> Result<(), BootstrapFailure> {
        if !self.capture_connection_started || end_lsn.get() == 0 {
            return Err(BootstrapFailure::deterministic(
                "WAL_NOT_DURABLE",
                "journal_transaction_boundary",
            ));
        }
        self.durable_wal_end = Some(end_lsn);
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
        self.feedback_lsn = Some(end);
        Ok(Some(end))
    }

    pub fn release_exporter(&mut self) -> Result<(), BootstrapFailure> {
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

    pub fn exporter_lost(&mut self) {
        self.exporter = ExporterLiveness::Lost;
        if self.phase != IntentPhase::ExporterReleasePermitted
            && self.phase != IntentPhase::ExporterReleased
        {
            self.invalidate("EXPORTER_LOST_BEFORE_IMPORT_ACKS");
        }
    }

    pub fn importer_reads_complete(&mut self, worker: u16) -> Result<(), BootstrapFailure> {
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

    pub fn guard_lost(&mut self) {
        if self.phase == IntentPhase::AnchorComplete {
            return;
        }
        self.guard = GuardLiveness::Lost;
        self.invalidate("DDL_GUARD_LOST_BEFORE_FENCE");
    }

    pub fn importer_lost(&mut self, worker: u16) -> Result<(), BootstrapFailure> {
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

    pub fn intend_fence(&mut self, nonce: u64) -> Result<(), BootstrapFailure> {
        if !self.chunks_complete || nonce == 0 || self.guard != GuardLiveness::Held {
            return Err(BootstrapFailure::deterministic(
                "FENCE_INTENT_INVALID",
                "post_copy_fence",
            ));
        }
        self.intended_fence_nonce = Some(nonce);
        self.phase = IntentPhase::FencePending;
        Ok(())
    }

    pub fn observe_durable_fence(
        &mut self,
        nonce: u64,
        end_lsn: ReceivedLsn,
        end_seq: JournalCursor,
    ) -> Result<(), BootstrapFailure> {
        if self.phase != IntentPhase::FencePending
            || self.intended_fence_nonce != Some(nonce)
            || self.guard != GuardLiveness::Held
            || end_lsn.get() == 0
        {
            return Err(BootstrapFailure::deterministic(
                "DURABLE_FENCE_MISMATCH",
                "post_copy_fence",
            ));
        }
        self.durable_fence = Some((nonce, end_lsn, end_seq));
        self.guard = GuardLiveness::Released;
        self.phase = IntentPhase::AnchorComplete;
        Ok(())
    }

    pub fn restart_decision(&mut self, proof: RecoveryProof) -> RecoveryDecision {
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
            if proof.source_identity_matches
                && proof.slot_provenance_matches
                && proof.publication_protocol_matches
                && proof.required_wal_available
            {
                self.phase = IntentPhase::ExistingSlotGenerationRequired;
                return RecoveryDecision::DrainRetainedWalThenExistingSlotSnapshot;
            }
            self.phase = IntentPhase::FullReseedRequired;
            return RecoveryDecision::RequireConfirmedFullReseed;
        }
        RecoveryDecision::ResumeEligibleGeneration
    }

    pub fn retained_wal_drained(
        &mut self,
        lower_lsn: ReceivedLsn,
        lower_seq: JournalCursor,
    ) -> Result<(), BootstrapFailure> {
        if self.phase != IntentPhase::ExistingSlotGenerationRequired || lower_lsn.get() == 0 {
            return Err(BootstrapFailure::deterministic(
                "LOWER_STITCH_NOT_DURABLE",
                "retained_slot_drain",
            ));
        }
        self.lower_stitch = Some((lower_lsn, lower_seq));
        self.phase = IntentPhase::RetainedWalDrained;
        Ok(())
    }

    pub fn confirm_full_reseed(&mut self, new_epoch: CaptureEpoch) -> Result<(), BootstrapFailure> {
        if self.phase != IntentPhase::FullReseedRequired
            || new_epoch.get() == 0
            || new_epoch == self.capture_epoch
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

    fn invalidate(&mut self, _reason: &'static str) {
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
            .or_else(|| self.lower_stitch.map(|(lsn, seq)| (lsn.get(), seq.get())))
    }
}

#[derive(Clone, Debug)]
pub enum BootstrapEvent {
    ExporterLost,
    GuardLost,
}
#[derive(Clone, Debug)]
pub enum BootstrapCompletion {
    DurableWal(ReceivedLsn),
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
            BootstrapEvent::ExporterLost => facts.exporter_lost(),
            BootstrapEvent::GuardLost => facts.guard_lost(),
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
            BootstrapCompletion::DurableWal(lsn) => {
                let _ = facts.record_durable_wal(lsn, None);
            }
        }
        vec![BootstrapEffect::Persist]
    }
    fn on_expiry(&self, facts: &mut Self::Facts, _context: &mut TransitionContext<'_>) {
        facts.invalidate("SESSION_LIFETIME_EXPIRED");
    }
    fn on_cancel(&self, facts: &mut Self::Facts, _context: &mut TransitionContext<'_>) {
        facts.invalidate("GENERATION_CANCELLED");
    }
    fn on_crash_restart(&self, facts: &mut Self::Facts, _context: &mut TransitionContext<'_>) {
        let _ = facts.restart_decision(RecoveryProof {
            source_identity_matches: false,
            slot_provenance_matches: false,
            publication_protocol_matches: false,
            required_wal_available: false,
        });
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
        std::mem::size_of::<Self::Facts>()
            + facts.intent_id.len()
            + facts
                .importers
                .values()
                .map(|v| v.assigned_range.len())
                .sum::<usize>()
    }
    fn event_size_bytes(&self, _event: &Self::Event) -> usize {
        std::mem::size_of::<Self::Event>()
    }
    fn completion_size_bytes(&self, _completion: &Self::Completion) -> usize {
        std::mem::size_of::<Self::Completion>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m1_transition_kernel::{
        Harness, HarnessBudget, ScheduleSeed, ScheduledAction, ScheduledStep,
    };

    fn prepared(workers: usize) -> BootstrapFacts {
        BootstrapFacts::prepare(PrepareInput {
            intent_id: "intent-1".into(),
            capture_epoch: CaptureEpoch::from_store(7),
            generation: 1,
            slot_identity: "boring_slot".into(),
            publication_fingerprint: "pub-fp".into(),
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
        f.acquire_guard().unwrap();
        f.slot_created("boring_slot").unwrap();
        f.persist_export_response(SlotCreationFloor::from_server(100), ZERO_START_SEQ)
            .unwrap();
        f.start_capture(true).unwrap();
        f
    }

    fn acknowledge_all(f: &mut BootstrapFacts) {
        for id in 0..f.importers.len() as u16 {
            let range = format!("range-{id}");
            f.begin_importer_transaction(id, true, true).unwrap();
            f.importer_statement(id, StatementKind::SetTransactionSnapshot)
                .unwrap();
            f.acknowledge_import(id, &range, "schema-fp").unwrap();
        }
    }

    #[test]
    fn intent_guard_and_one_permanent_slot_precede_export() {
        let mut f = prepared(1);
        assert_eq!(
            f.slot_created("boring_slot").unwrap_err().fingerprint,
            "PERMANENT_SLOT_CREATION_NOT_AUTHORIZED"
        );
        f.acquire_guard().unwrap();
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
        f.acquire_guard().unwrap();
        f.slot_created("boring_slot").unwrap();
        f.persist_export_response(SlotCreationFloor::from_server(100), ZERO_START_SEQ)
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
        violated.begin_importer_transaction(0, true, true).unwrap();
        assert_eq!(
            violated
                .importer_statement(0, StatementKind::CatalogQuery)
                .unwrap_err()
                .fingerprint,
            "SNAPSHOT_IMPORT_NOT_FIRST_STATEMENT"
        );
        assert_eq!(violated.phase, IntentPhase::SnapshotUnusable);
        let mut f = exported(2);
        f.begin_importer_transaction(0, true, true).unwrap();
        f.importer_statement(0, StatementKind::SetTransactionSnapshot)
            .unwrap();
        assert!(f.acknowledge_import(0, "wrong-range", "schema-fp").is_err());
        f.acknowledge_import(0, "range-0", "schema-fp").unwrap();
        assert!(f.importer_feedback_gate);
        f.begin_importer_transaction(1, true, true).unwrap();
        f.importer_statement(1, StatementKind::SetTransactionSnapshot)
            .unwrap();
        f.acknowledge_import(1, "range-1", "schema-fp").unwrap();
        assert_eq!(f.phase, IntentPhase::ExporterReleasePermitted);
        assert!(!f.importer_feedback_gate);
    }

    #[test]
    fn feedback_is_gated_until_all_import_acks_then_uses_durable_wal_only() {
        let mut f = exported(1);
        f.record_durable_wal(ReceivedLsn::from_wire(120), None)
            .unwrap();
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
                f.begin_importer_transaction(id, true, true).unwrap();
                f.importer_statement(id, StatementKind::SetTransactionSnapshot)
                    .unwrap();
                f.acknowledge_import(id, &format!("range-{id}"), "schema-fp")
                    .unwrap();
            }
            f.exporter_lost();
            assert_eq!(f.phase, IntentPhase::SnapshotUnusable);
            assert!(!f.snapshot_events_promotable);
            assert!(!f.importer_feedback_gate);
        }
    }

    #[test]
    fn normal_exporter_release_after_readback_does_not_invalidate() {
        let mut f = exported(2);
        acknowledge_all(&mut f);
        f.release_exporter().unwrap();
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
        let mut after_response = prepared(1);
        after_response.acquire_guard().unwrap();
        after_response.slot_created("boring_slot").unwrap();
        assert_eq!(
            after_response.restart_decision(no_proof()),
            RecoveryDecision::BootstrapAmbiguousRequiresRestart
        );
        assert_eq!(
            after_response.phase,
            IntentPhase::BootstrapAmbiguousRequiresRestart
        );
    }

    #[test]
    fn retained_slot_recovery_requires_all_proofs_and_preserves_transient_wal() {
        let mut f = exported(1);
        f.record_durable_wal(ReceivedLsn::from_wire(110), Some("insert-42"))
            .unwrap();
        f.record_durable_wal(ReceivedLsn::from_wire(120), Some("delete-42"))
            .unwrap();
        f.exporter_lost();
        assert_eq!(
            f.restart_decision(all_proof()),
            RecoveryDecision::DrainRetainedWalThenExistingSlotSnapshot
        );
        f.retained_wal_drained(ReceivedLsn::from_wire(120), JournalCursor::from_store(2))
            .unwrap();
        assert_eq!(
            f.transient_wal_event_ids,
            BTreeSet::from(["delete-42".into(), "insert-42".into()])
        );
        assert_eq!(
            f.lower_stitch,
            Some((ReceivedLsn::from_wire(120), JournalCursor::from_store(2)))
        );
    }

    #[test]
    fn feedback_position_alone_never_selects_reseed_or_slot_drop() {
        for durable in [
            None,
            Some(ReceivedLsn::from_wire(100)),
            Some(ReceivedLsn::from_wire(999)),
        ] {
            let mut f = exported(1);
            f.durable_wal_end = durable;
            f.exporter_lost();
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
        f.exporter_lost();
        assert_eq!(
            f.restart_decision(no_proof()),
            RecoveryDecision::RequireConfirmedFullReseed
        );
        assert!(f.confirm_full_reseed(CaptureEpoch::from_store(7)).is_err());
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
        f.guard_lost();
        assert_eq!(f.phase, IntentPhase::SnapshotUnusable);
        assert!(!f.importer_feedback_gate);
    }

    #[test]
    fn importer_ack_reordering_and_stale_inputs_preserve_gate() {
        let mut f = exported(3);
        for id in [2, 0, 1] {
            f.begin_importer_transaction(id, true, true).unwrap();
            f.importer_statement(id, StatementKind::SetTransactionSnapshot)
                .unwrap();
            f.acknowledge_import(id, &format!("range-{id}"), "schema-fp")
                .unwrap();
        }
        assert_eq!(f.phase, IntentPhase::ExporterReleasePermitted);
        assert!(f.acknowledge_import(2, "range-2", "schema-fp").is_err());
        assert_eq!(f.phase, IntentPhase::ExporterReleasePermitted);
    }

    #[test]
    fn anchor_requires_reads_lower_stitch_and_matching_durable_fence() {
        let mut f = exported(2);
        acknowledge_all(&mut f);
        f.release_exporter().unwrap();
        for id in 0..2 {
            f.importer_reads_complete(id).unwrap();
        }
        f.mark_chunks_complete().unwrap();
        f.intend_fence(77).unwrap();
        assert!(
            f.observe_durable_fence(
                78,
                ReceivedLsn::from_wire(150),
                JournalCursor::from_store(3)
            )
            .is_err()
        );
        f.observe_durable_fence(
            77,
            ReceivedLsn::from_wire(150),
            JournalCursor::from_store(3),
        )
        .unwrap();
        assert_eq!(f.phase, IntentPhase::AnchorComplete);
        assert_eq!(f.guard, GuardLiveness::Released);
        assert_eq!(f.invariant_violation(), None);
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
                ScheduledAction::Event(BootstrapEvent::ExporterLost),
            ),
            ScheduledStep::new(
                "SCN-M1-BOOTSTRAP-GUARD-LOSS",
                ScheduledAction::Event(BootstrapEvent::GuardLost),
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
        f.importer_lost(0).unwrap();
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
            publication_fingerprint: "pub".into(),
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
        let mut f = BootstrapFacts::prepare(input).unwrap();
        f.invalidate("GENERATION_CANCELLED");
        assert_eq!(f.phase, IntentPhase::SnapshotUnusable);
        assert!(!f.importer_feedback_gate);
    }

    fn all_proof() -> RecoveryProof {
        RecoveryProof {
            source_identity_matches: true,
            slot_provenance_matches: true,
            publication_protocol_matches: true,
            required_wal_available: true,
        }
    }
    fn no_proof() -> RecoveryProof {
        RecoveryProof {
            source_identity_matches: false,
            slot_provenance_matches: false,
            publication_protocol_matches: false,
            required_wal_available: false,
        }
    }
}
