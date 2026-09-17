//! Durable CopyBoth capture orchestration.
//!
//! This is the single integration boundary between the M1 decoder, the capped M2 spool,
//! SQLite publication and PostgreSQL feedback.  A connection generation is consumed once:
//! every unexpected transport or ownership loss returns to the external supervisor and is
//! never repaired in process.

use crate::article1_capture::{CaptureConfig, CaptureFailure, setup_runtime};
use crate::m1_decoder::{
    CopyBothEvent, Decoder, PgoutputEvent, RelationContract, RowChange, TupleValue, WireLimits,
};
use crate::m2_journal::{
    CommitFault, DurableCommit, JournalError, JournalEvent, JournalStore, JournalWriterService,
    SourceCommit, WorkOutcome, transaction_checksum,
};
use crate::m2_spool::{SpoolError, TxnBuffer};
use pg_walstream::{CancellationToken, PgReplicationConnection};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;

pub const OWNER_BEAD: &str = "boring-cdc-m2-capture-runtime";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FeedbackPacket {
    pub write_lsn: u64,
    pub flush_lsn: u64,
    pub apply_lsn: u64,
    pub reply_requested: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FeedbackPermit {
    /// No snapshot generation is attached; ordinary durable transaction progress is safe.
    AllowSafeBoundary { lsn: u64 },
    /// A downstream importer gate is closed. M2 does not persist or mutate that gate.
    Hold,
    /// The supplied generation/permit is stale and must not produce feedback.
    Stale,
}

pub trait FeedbackGate {
    fn permit(&mut self, durable_end_lsn: Option<u64>) -> FeedbackPermit;
}

pub struct NoSnapshotGate;
impl FeedbackGate for NoSnapshotGate {
    fn permit(&mut self, durable_end_lsn: Option<u64>) -> FeedbackPermit {
        FeedbackPermit::AllowSafeBoundary {
            lsn: durable_end_lsn.unwrap_or(0),
        }
    }
}

pub trait RuntimeSpool {
    fn push(&mut self, event: &[u8]) -> Result<(), SpoolError>;
    fn drain(&mut self) -> Result<Vec<Vec<u8>>, SpoolError>;
    fn finish(self: Box<Self>) -> Result<(), SpoolError>;
}
impl RuntimeSpool for TxnBuffer {
    fn push(&mut self, event: &[u8]) -> Result<(), SpoolError> {
        let mut receive = self.admit_receive(event.len())?;
        receive.extend_from_slice(event)?;
        self.push_received(receive)
    }
    fn drain(&mut self) -> Result<Vec<Vec<u8>>, SpoolError> {
        self.collect_for_commit()
    }
    fn finish(self: Box<Self>) -> Result<(), SpoolError> {
        (*self).finish()
    }
}

pub trait SpoolFactory {
    fn begin(&mut self, xid: u32) -> Result<Box<dyn RuntimeSpool>, RuntimeError>;
}
impl<F> SpoolFactory for F
where
    F: FnMut(u32) -> Result<Box<dyn RuntimeSpool>, RuntimeError>,
{
    fn begin(&mut self, xid: u32) -> Result<Box<dyn RuntimeSpool>, RuntimeError> {
        self(xid)
    }
}

pub trait DurableJournal {
    fn commit(&mut self, commit: &SourceCommit) -> Result<DurableCommit, JournalError>;
    fn load_failure(
        &self,
        _failure_id: &str,
        _boundary: crate::failure_policy::FailedBoundary,
    ) -> Result<Option<crate::failure_policy::FailureRecord>, JournalError> {
        Ok(None)
    }
    fn persist_failure(
        &mut self,
        _operation: crate::failure_policy::PreparedFailureOperation,
    ) -> Result<(), JournalError> {
        Ok(())
    }
    fn active_capture_failure_id(&self) -> Result<Option<String>, JournalError> {
        Ok(None)
    }
    fn pressure_tick(
        &mut self,
        _thresholds: crate::m2_pressure::PressureThresholds,
        _observation: crate::m2_pressure::PressureObservation<'_>,
    ) -> Result<Option<crate::m2_pressure::PressureServiceResult>, JournalError> {
        Ok(None)
    }
}
impl DurableJournal for JournalStore {
    fn commit(&mut self, commit: &SourceCommit) -> Result<DurableCommit, JournalError> {
        self.commit_atomic(commit, crate::m2_journal::CommitFault::None)
    }
}
impl DurableJournal for JournalWriterService {
    fn commit(&mut self, commit: &SourceCommit) -> Result<DurableCommit, JournalError> {
        self.enqueue_capture(commit.clone(), CommitFault::None)
            .map_err(|_| JournalError::BusyBoundExceeded)?;
        loop {
            match self
                .service_next()
                .ok_or(JournalError::Conflict("writer queue lost capture"))??
            {
                WorkOutcome::Durable(durable) => return Ok(durable),
                WorkOutcome::Serviced(_) => continue,
            }
        }
    }
    fn load_failure(
        &self,
        failure_id: &str,
        boundary: crate::failure_policy::FailedBoundary,
    ) -> Result<Option<crate::failure_policy::FailureRecord>, JournalError> {
        JournalWriterService::load_failure(self, failure_id, boundary)
    }
    fn persist_failure(
        &mut self,
        operation: crate::failure_policy::PreparedFailureOperation,
    ) -> Result<(), JournalError> {
        JournalWriterService::persist_failure(self, operation)
    }
    fn active_capture_failure_id(&self) -> Result<Option<String>, JournalError> {
        JournalWriterService::active_capture_failure_id(self)
    }
    fn pressure_tick(
        &mut self,
        thresholds: crate::m2_pressure::PressureThresholds,
        observation: crate::m2_pressure::PressureObservation<'_>,
    ) -> Result<Option<crate::m2_pressure::PressureServiceResult>, JournalError> {
        self.pressure_tick_reserved(thresholds, observation)
            .map(Some)
            .map_err(|_| JournalError::BusyBoundExceeded)
    }
}

impl<J: DurableJournal, S: SpoolFactory, G: FeedbackGate> CaptureRuntime<J, S, G> {
    /// Persist a capture failure through the shared policy and sole fair writer before the
    /// generation is returned to the supervisor. The persisted deadline is therefore authoritative
    /// after process restart.
    pub fn enable_failure_policy(&mut self, capture_epoch: String, config_fingerprint: String) {
        let boundary = crate::failure_policy::FailedBoundary::Capture {
            capture_epoch: capture_epoch.clone(),
            end_lsn: format!("{:016X}", self.durable_end_lsn.unwrap_or(0)),
        };
        self.active_failure = self
            .journal
            .active_capture_failure_id()
            .ok()
            .flatten()
            .and_then(|id| self.journal.load_failure(&id, boundary).ok().flatten());
        self.failure_policy_identity = Some((capture_epoch, config_fingerprint));
    }
    fn persist_if_enabled(
        &mut self,
        class: crate::failure_policy::FailureClass,
        code: crate::failure_policy::StableErrorCode,
    ) -> Result<(), RuntimeError> {
        if self.failure_policy_identity.is_none() {
            Ok(())
        } else {
            self.persist_capture_failure(class, code)
        }
    }
    fn persist_capture_failure(
        &mut self,
        class: crate::failure_policy::FailureClass,
        code: crate::failure_policy::StableErrorCode,
    ) -> Result<(), RuntimeError> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let (capture_epoch, config_fingerprint) =
            self.failure_policy_identity.clone().ok_or_else(|| {
                RuntimeError::JournalTransient("failure policy identity unavailable".into())
            })?;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| RuntimeError::JournalTransient("failure policy clock invalid".into()))?
            .as_millis() as u64;
        let mut random_bytes = [0_u8; 8];
        std::fs::File::open("/dev/urandom")
            .and_then(|mut source| std::io::Read::read_exact(&mut source, &mut random_bytes))
            .map_err(|_| {
                RuntimeError::JournalTransient("failure policy randomness unavailable".into())
            })?;
        let random_sample = u64::from_be_bytes(random_bytes);
        use crate::failure_policy::{
            Component, FailedBoundary, FailureObservation, FingerprintInput, PolicyAction,
            PolicyEvent, PreparedFailureOperation,
        };
        use crate::m1_transition_kernel::{Randomness, TransitionContext, VirtualClock};
        struct One(u64);
        impl Randomness for One {
            fn next_u64(&mut self) -> u64 {
                self.0
            }
        }
        let boundary = FailedBoundary::Capture {
            capture_epoch: capture_epoch.clone(),
            end_lsn: format!("{:016X}", self.durable_end_lsn.unwrap_or(0)),
        };
        let observation = FailureObservation {
            destination_id: None,
            fingerprint: FingerprintInput {
                component: Component::Capture,
                class,
                code,
                boundary: boundary.clone(),
                relevant_configuration_fingerprint: config_fingerprint.clone(),
                context: BTreeMap::from([(
                    crate::failure_policy::SafeContextKey::Operation,
                    crate::failure_policy::SafeContextValue::Capture,
                )]),
            },
        };
        let clock = VirtualClock::new(now_ms);
        let mut random = One(random_sample);
        let mut context = TransitionContext {
            clock: &clock,
            randomness: &mut random,
        };
        let candidate = crate::failure_policy::transition(
            None,
            PolicyEvent::Observe(observation.clone()),
            &mut context,
        );
        let PolicyAction::Persist(candidate_record) = candidate else {
            return Err(RuntimeError::JournalTransient(
                "failure policy did not persist".into(),
            ));
        };
        let current = self
            .journal
            .load_failure(&candidate_record.failure_id, boundary)
            .map_err(|e| RuntimeError::JournalTransient(e.to_string()))?;
        let action = crate::failure_policy::transition(
            current.as_ref(),
            PolicyEvent::Observe(observation),
            &mut context,
        );
        let operation = PreparedFailureOperation::from_policy_action(
            action,
            current.as_ref().map(|record| record.failure_id.clone()),
        )
        .ok_or_else(|| {
            RuntimeError::JournalTransient("failure policy suppressed persistence".into())
        })?;
        let persisted = match &operation {
            PreparedFailureOperation::StoreAndArm { record, .. }
            | PreparedFailureOperation::Rearm { record, .. } => Some(record.clone()),
            PreparedFailureOperation::Clear { .. } => None,
        };
        self.journal
            .persist_failure(operation)
            .map_err(|e| RuntimeError::JournalTransient(e.to_string()))?;
        self.active_failure = persisted;
        Ok(())
    }
    fn clear_completed_failure(&mut self) -> Result<(), RuntimeError> {
        use crate::failure_policy::{CompletionToken, PolicyEvent, PreparedFailureOperation};
        use crate::m1_transition_kernel::{Randomness, TransitionContext, VirtualClock};
        let Some(record) = self.active_failure.clone() else {
            return Ok(());
        };
        struct Zero;
        impl Randomness for Zero {
            fn next_u64(&mut self) -> u64 {
                0
            }
        }
        let clock = VirtualClock::new(record.last_failed_at_ms.saturating_add(1));
        let mut random = Zero;
        let mut context = TransitionContext {
            clock: &clock,
            randomness: &mut random,
        };
        let action = crate::failure_policy::transition(
            Some(&record),
            PolicyEvent::Completed(CompletionToken {
                failure_id: record.failure_id.clone(),
                fingerprint: record.fingerprint.clone(),
                capture_epoch: record.boundary.capture_epoch().to_owned(),
                generation: record.boundary.generation(),
                attempt: record.attempt,
            }),
            &mut context,
        );
        let operation = PreparedFailureOperation::from_policy_action(action, None)
            .ok_or(RuntimeError::Protocol("capture_failure_clear_rejected"))?;
        self.journal
            .persist_failure(operation)
            .map_err(|e| RuntimeError::JournalTransient(e.to_string()))?;
        self.active_failure = None;
        Ok(())
    }
    pub fn rearm_capture_failure(
        &mut self,
        current: &crate::failure_policy::FailureRecord,
        request: crate::failure_policy::RearmRequest,
        now_ms: u64,
    ) -> Result<(), RuntimeError> {
        use crate::failure_policy::{PolicyAction, PolicyEvent, PreparedFailureOperation};
        use crate::m1_transition_kernel::{Randomness, TransitionContext, VirtualClock};
        struct Zero;
        impl Randomness for Zero {
            fn next_u64(&mut self) -> u64 {
                0
            }
        }
        let clock = VirtualClock::new(now_ms);
        let mut randomness = Zero;
        let mut context = TransitionContext {
            clock: &clock,
            randomness: &mut randomness,
        };
        let action = crate::failure_policy::transition(
            Some(current),
            PolicyEvent::Rearm(request),
            &mut context,
        );
        if !matches!(action, PolicyAction::Rearmed { .. }) {
            return Err(RuntimeError::Protocol("capture_rearm_rejected"));
        }
        let operation = PreparedFailureOperation::from_policy_action(action, None)
            .ok_or(RuntimeError::Protocol("capture_rearm_missing_operation"))?;
        self.journal
            .persist_failure(operation)
            .map_err(|error| RuntimeError::JournalTransient(error.to_string()))?;
        // Re-arm authorizes only a supervised successor. Frames were deliberately discarded while
        // fenced, so resuming this CopyBoth generation would be unsafe.
        self.state = RuntimeState::ExpectedClose;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeState {
    Starting,
    Capturing,
    CaptureSafeStopped,
    ExpectedClose,
    OwnershipLost,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeError {
    Decode(&'static str),
    Spool(String),
    JournalTransient(String),
    JournalIntegrity(String),
    Protocol(&'static str),
    Feedback,
    UnexpectedCopyBothLoss,
    OwnershipLost,
    RetryNotDue { next_retry_at_ms: u64 },
    ReconciliationRequired,
}
impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for RuntimeError {}

struct ActiveTransaction {
    xid: u32,
    origin: Option<(u64, String)>,
    spool: Box<dyn RuntimeSpool>,
}

#[derive(Clone, Debug)]
struct RuntimePressureFilesystem {
    observation_path: std::path::PathBuf,
    thresholds: crate::m2_pressure::PressureThresholds,
    capture_critical: bool,
}

#[derive(Clone, Debug)]
struct RuntimePressureConfig {
    filesystems: Vec<RuntimePressureFilesystem>,
    capture_epoch: String,
    replay_window_ms: u64,
}

pub struct CaptureRuntime<J, S, G> {
    decoder: Decoder,
    contracts: BTreeMap<u32, RelationContract>,
    journal: J,
    spools: S,
    gate: G,
    active: Option<ActiveTransaction>,
    durable_end_lsn: Option<u64>,
    state: RuntimeState,
    feedback: Vec<FeedbackPacket>,
    committed_transactions: u64,
    failure_policy_identity: Option<(String, String)>,
    active_failure: Option<crate::failure_policy::FailureRecord>,
    pressure: Option<RuntimePressureConfig>,
}

impl<J: DurableJournal, S: SpoolFactory, G: FeedbackGate> CaptureRuntime<J, S, G> {
    pub fn new(
        journal: J,
        spools: S,
        gate: G,
        contracts: BTreeMap<u32, RelationContract>,
        durable_end_lsn: Option<u64>,
    ) -> Self {
        Self {
            decoder: Decoder::new(WireLimits::default()),
            contracts,
            journal,
            spools,
            gate,
            active: None,
            durable_end_lsn,
            state: RuntimeState::Starting,
            feedback: Vec::new(),
            committed_transactions: 0,
            failure_policy_identity: None,
            active_failure: None,
            pressure: None,
        }
    }
    pub fn state(&self) -> RuntimeState {
        self.state
    }
    pub fn durable_end_lsn(&self) -> Option<u64> {
        self.durable_end_lsn
    }
    pub fn committed_transactions(&self) -> u64 {
        self.committed_transactions
    }
    pub fn take_feedback(&mut self) -> Vec<FeedbackPacket> {
        std::mem::take(&mut self.feedback)
    }

    pub fn enable_pressure_service(
        &mut self,
        journal_path: std::path::PathBuf,
        thresholds: crate::m2_pressure::PressureThresholds,
        capture_epoch: String,
        replay_window_ms: u64,
    ) {
        self.enable_pressure_filesystems(
            vec![RuntimePressureFilesystem {
                observation_path: journal_path,
                thresholds,
                capture_critical: true,
            }],
            capture_epoch,
            replay_window_ms,
        );
    }
    fn enable_pressure_filesystems(
        &mut self,
        filesystems: Vec<RuntimePressureFilesystem>,
        capture_epoch: String,
        replay_window_ms: u64,
    ) {
        self.pressure = Some(RuntimePressureConfig {
            filesystems,
            capture_epoch,
            replay_window_ms,
        });
    }
    fn service_pressure(&mut self) -> Result<(), RuntimeError> {
        let Some(config) = self.pressure.clone() else {
            return Ok(());
        };
        let observations = config
            .filesystems
            .iter()
            .map(|filesystem| {
                filesystem_free_bytes(&filesystem.observation_path)
                    .map(|free| (free, filesystem.thresholds))
                    .map_err(|_| {
                        RuntimeError::JournalTransient(
                            "pressure filesystem observation failed".into(),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (worst_index, decision) =
            crate::m2_pressure::decide_pressure_filesystems(&observations)
                .map_err(|error| RuntimeError::JournalTransient(error.to_string()))?;
        let capture_hard = capture_filesystem_is_hard(&config.filesystems, &observations)?;
        let free = decision.free_bytes;
        let thresholds = observations[worst_index].1;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| RuntimeError::JournalTransient("pressure clock invalid".into()))?
            .as_millis() as i64;
        let cutoff = now.saturating_sub(config.replay_window_ms.min(i64::MAX as u64) as i64);
        let result = self
            .journal
            .pressure_tick(
                thresholds,
                crate::m2_pressure::PressureObservation {
                    free_bytes: free,
                    capture_epoch: &config.capture_epoch,
                    replay_from_seq: 1,
                    replay_cutoff_unix_ms: Some(cutoff),
                    now_unix_ms: now,
                },
            )
            .map_err(|e| RuntimeError::JournalTransient(e.to_string()))?;
        let result = result
            .ok_or_else(|| RuntimeError::JournalTransient("pressure service unavailable".into()))?;
        if pressure_stops_capture(&result.decision, capture_hard) {
            self.state = RuntimeState::CaptureSafeStopped;
        }
        Ok(())
    }

    pub fn receive(&mut self, frame: &[u8]) -> Result<(), RuntimeError> {
        if self.state == RuntimeState::CaptureSafeStopped {
            if frame.len() == 18 && frame[0] == b'k' && frame[17] == 1 {
                self.queue_feedback(true);
                return Ok(());
            }
            return Ok(()); // alive and fenced: drain transport without decoding or dispatch
        }
        if matches!(
            self.state,
            RuntimeState::OwnershipLost | RuntimeState::ExpectedClose
        ) {
            return Err(RuntimeError::Protocol("runtime_not_receiving"));
        }
        self.state = RuntimeState::Capturing;
        if frame.len() > WireLimits::default().max_copy_data_bytes {
            return self.safe_stop(RuntimeError::Spool(
                "transport frame exceeds admitted limit".into(),
            ));
        }
        let decoded = self.decoder.decode_copy_data(frame).map_err(|failure| {
            self.state = RuntimeState::CaptureSafeStopped;
            RuntimeError::Decode(failure.fingerprint)
        })?;
        let result = self.route(decoded);
        if result.is_err() && self.state == RuntimeState::Capturing {
            self.state = RuntimeState::CaptureSafeStopped;
        }
        result
    }

    fn route(&mut self, decoded: CopyBothEvent) -> Result<(), RuntimeError> {
        match decoded {
            CopyBothEvent::Keepalive {
                reply_requested, ..
            } => {
                if reply_requested {
                    self.queue_feedback(true);
                }
                Ok(())
            }
            CopyBothEvent::XLogData { event, .. } => self.route_pgoutput(event),
        }
    }

    fn route_pgoutput(&mut self, event: PgoutputEvent) -> Result<(), RuntimeError> {
        match event {
            PgoutputEvent::Begin { xid, .. } => {
                if self.active.is_some() {
                    return self.safe_stop(RuntimeError::Protocol("nested_transaction"));
                }
                let spool = self.spools.begin(xid)?;
                self.active = Some(ActiveTransaction {
                    xid,
                    origin: None,
                    spool,
                });
                Ok(())
            }
            PgoutputEvent::RelationNeedsValidation(relation) => {
                let contract = self
                    .contracts
                    .get(&relation.id)
                    .filter(|c| c.relation == relation)
                    .cloned()
                    .ok_or(RuntimeError::Protocol("relation_contract_mismatch"))?;
                self.decoder
                    .admit_relation(contract)
                    .map_err(|failure| RuntimeError::Decode(failure.fingerprint))
            }
            PgoutputEvent::RelationMetadata(_) => Ok(()),
            PgoutputEvent::Origin { origin_lsn, name } => {
                let active = self
                    .active
                    .as_mut()
                    .ok_or(RuntimeError::Protocol("origin_without_transaction"))?;
                active.origin = Some((origin_lsn, name));
                Ok(())
            }
            PgoutputEvent::Row(row) => {
                let control_kind = self
                    .contracts
                    .get(&row.relation_id)
                    .and_then(|contract| contract.control.as_ref())
                    .map(|_| {
                        if self.contracts[&row.relation_id].relation.name == "heartbeat" {
                            "heartbeat"
                        } else {
                            "capture_fence"
                        }
                    });
                let payload =
                    encode_row(&row, self.active.as_ref().and_then(|a| a.origin.as_ref()))?;
                let staged = serde_json::to_vec(&StagedEvent {
                    relation_id: row.relation_id,
                    control_kind,
                    payload,
                })
                .map_err(|_| RuntimeError::Protocol("event_encoding"))?;
                self.active
                    .as_mut()
                    .ok_or(RuntimeError::Protocol("row_without_transaction"))?
                    .spool
                    .push(&staged)
                    .map_err(|e| {
                        self.state = RuntimeState::CaptureSafeStopped;
                        RuntimeError::Spool(e.to_string())
                    })
            }
            PgoutputEvent::Commit {
                commit_lsn,
                end_lsn,
                row_count,
                ..
            } => self.publish(commit_lsn, end_lsn, row_count),
        }
    }

    fn publish(
        &mut self,
        commit_lsn: u64,
        end_lsn: u64,
        row_count: u64,
    ) -> Result<(), RuntimeError> {
        let mut active = self
            .active
            .take()
            .ok_or(RuntimeError::Protocol("commit_without_spool"))?;
        let payloads = active
            .spool
            .drain()
            .map_err(|e| RuntimeError::Spool(e.to_string()))?;
        if payloads.len() as u64 != row_count {
            return self.safe_stop(RuntimeError::Protocol("row_count_mismatch"));
        }
        let events = payloads
            .into_iter()
            .enumerate()
            .map(|(ordinal, staged)| {
                let staged: StagedEventOwned = serde_json::from_slice(&staged)
                    .map_err(|_| RuntimeError::Protocol("staged_event_corrupt"))?;
                let payload_hash = format!("{:x}", Sha256::digest(&staged.payload));
                Ok(JournalEvent {
                    event_id: format!("wal:{end_lsn:016X}:{ordinal}"),
                    transaction_ordinal: ordinal as u32,
                    relation_schema_fingerprint: None,
                    control_kind: staged.control_kind,
                    payload: staged.payload,
                    payload_hash,
                })
            })
            .collect::<Result<Vec<_>, RuntimeError>>()?;
        if events.is_empty() {
            return self.safe_stop(RuntimeError::Protocol("empty_transaction"));
        }
        let commit = SourceCommit {
            transaction_id: format!("wal:{end_lsn:016X}"),
            xid: active.xid.to_string(),
            end_lsn: format!("{end_lsn:016X}"),
            payload_checksum: transaction_checksum(&events),
            schemas: Vec::new(),
            events,
        };
        let durable = self.journal.commit(&commit).map_err(|error| {
            self.state = RuntimeState::CaptureSafeStopped;
            match error {
                JournalError::Sqlite(_)
                | JournalError::BusyBoundExceeded
                | JournalError::BusyBoundExceededAfterCommit
                | JournalError::AmbiguousAfterCommit => {
                    RuntimeError::JournalTransient(error.to_string())
                }
                _ => RuntimeError::JournalIntegrity(error.to_string()),
            }
        })?;
        let durable_lsn = parse_lsn(durable.feedback_eligible_end_lsn())?;
        if durable_lsn != end_lsn || commit_lsn > end_lsn {
            return self.safe_stop(RuntimeError::Protocol("durable_boundary_mismatch"));
        }
        active
            .spool
            .finish()
            .map_err(|e| RuntimeError::Spool(e.to_string()))?;
        self.committed_transactions = self.committed_transactions.saturating_add(1);
        self.durable_end_lsn = Some(
            self.durable_end_lsn
                .map_or(durable_lsn, |old| old.max(durable_lsn)),
        );
        self.clear_completed_failure()?;
        self.queue_feedback(false);
        Ok(())
    }

    fn queue_feedback(&mut self, _requested: bool) {
        let durable = self.durable_end_lsn;
        if let FeedbackPermit::AllowSafeBoundary { lsn: permitted } = self.gate.permit(durable) {
            let safe = durable.map_or(0, |value| value.min(permitted));
            self.feedback.push(FeedbackPacket {
                write_lsn: safe,
                flush_lsn: safe,
                apply_lsn: safe,
                reply_requested: false,
            });
        }
    }
    fn safe_stop<T>(&mut self, error: RuntimeError) -> Result<T, RuntimeError> {
        self.state = RuntimeState::CaptureSafeStopped;
        Err(error)
    }
    pub fn graceful_shutdown(&mut self) -> Result<(), RuntimeError> {
        if self.active.is_some() {
            return self.safe_stop(RuntimeError::ReconciliationRequired);
        }
        self.state = RuntimeState::ExpectedClose;
        Ok(())
    }
    pub fn unexpected_eof(&mut self) -> RuntimeError {
        self.state = RuntimeState::OwnershipLost;
        RuntimeError::UnexpectedCopyBothLoss
    }
    pub fn ownership_lost(&mut self) -> RuntimeError {
        crate::m2_fault_status::fault_hook(crate::m2_fault_status::FaultHook::OwnershipLost);
        self.state = RuntimeState::OwnershipLost;
        RuntimeError::OwnershipLost
    }
}

#[derive(Serialize)]
struct StagedEvent<'a> {
    relation_id: u32,
    control_kind: Option<&'a str>,
    payload: Vec<u8>,
}
#[derive(Deserialize)]
struct StagedEventOwned {
    control_kind: Option<String>,
    payload: Vec<u8>,
}

#[derive(Serialize)]
struct EncodedRow<'a> {
    kind: &'static str,
    relation_id: u32,
    ordinal: u64,
    old_kind: Option<&'static str>,
    old: Option<Vec<EncodedTuple>>,
    new: Option<Vec<EncodedTuple>>,
    origin_lsn: Option<u64>,
    origin_name: Option<&'a str>,
}
#[derive(Serialize)]
#[serde(tag = "state", content = "bytes", rename_all = "snake_case")]
enum EncodedTuple {
    Null,
    UnchangedToast,
    Text(Vec<u8>),
}
fn tuples(value: Option<Vec<TupleValue>>) -> Option<Vec<EncodedTuple>> {
    value.map(|values| {
        values
            .into_iter()
            .map(|v| match v {
                TupleValue::Null => EncodedTuple::Null,
                TupleValue::UnchangedToast => EncodedTuple::UnchangedToast,
                TupleValue::Text(b) => EncodedTuple::Text(b),
            })
            .collect()
    })
}
fn encode_row(row: &RowChange, origin: Option<&(u64, String)>) -> Result<Vec<u8>, RuntimeError> {
    serde_json::to_vec(&EncodedRow {
        kind: match row.kind {
            crate::m1_decoder::RowKind::Insert => "insert",
            crate::m1_decoder::RowKind::Update => "update",
            crate::m1_decoder::RowKind::Delete => "delete",
        },
        relation_id: row.relation_id,
        ordinal: row.ordinal,
        old_kind: row.old_kind.map(|v| match v {
            crate::m1_decoder::OldTupleKind::Key => "key",
            crate::m1_decoder::OldTupleKind::Full => "full",
        }),
        old: tuples(row.old.clone()),
        new: tuples(row.new.clone()),
        origin_lsn: origin.map(|v| v.0),
        origin_name: origin.map(|v| v.1.as_str()),
    })
    .map_err(|_| RuntimeError::Protocol("event_encoding"))
}
fn parse_lsn(value: &str) -> Result<u64, RuntimeError> {
    if let Some((high, low)) = value.split_once('/') {
        let high = u64::from_str_radix(high, 16)
            .map_err(|_| RuntimeError::Protocol("invalid_durable_lsn"))?;
        let low = u64::from_str_radix(low, 16)
            .map_err(|_| RuntimeError::Protocol("invalid_durable_lsn"))?;
        return high
            .checked_shl(32)
            .and_then(|prefix| prefix.checked_add(low))
            .ok_or(RuntimeError::Protocol("invalid_durable_lsn"));
    }
    u64::from_str_radix(value, 16).map_err(|_| RuntimeError::Protocol("invalid_durable_lsn"))
}

pub trait RetrySchedule {
    fn next_retry_at_ms(&self) -> Option<u64>;
}
pub fn reconcile_startup(
    schedule: &impl RetrySchedule,
    now_ms: u64,
    ownership_reconciled: bool,
    spool_reconciled: bool,
) -> Result<(), RuntimeError> {
    if !ownership_reconciled || !spool_reconciled {
        return Err(RuntimeError::ReconciliationRequired);
    }
    if let Some(next) = schedule.next_retry_at_ms().filter(|next| *next > now_ms) {
        return Err(RuntimeError::RetryNotDue {
            next_retry_at_ms: next,
        });
    }
    Ok(())
}

/// Production Article-1 transport, consumed directly rather than forked. It opens one CopyBoth
/// generation, delegates every payload to the durable kernel, and sends only queued safe packets.
pub async fn capture_copyboth<J: DurableJournal, S: SpoolFactory, G: FeedbackGate>(
    config: &CaptureConfig,
    cancellation: &CancellationToken,
    runtime: &mut CaptureRuntime<J, S, G>,
) -> Result<(), CaptureFailure> {
    capture_copyboth_until(config, cancellation, runtime, u64::MAX).await
}

pub async fn capture_copyboth_until<J: DurableJournal, S: SpoolFactory, G: FeedbackGate>(
    config: &CaptureConfig,
    cancellation: &CancellationToken,
    runtime: &mut CaptureRuntime<J, S, G>,
    stop_after_commits: u64,
) -> Result<(), CaptureFailure> {
    capture_copyboth_until_with_probe(
        config,
        cancellation,
        runtime,
        stop_after_commits,
        std::time::Duration::from_millis(250),
        || true,
    )
    .await
}
fn classify_runtime_failure(
    error: &RuntimeError,
) -> (
    crate::failure_policy::FailureClass,
    crate::failure_policy::StableErrorCode,
    bool,
) {
    use crate::failure_policy::{FailureClass as Class, StableErrorCode as Code};
    match error {
        RuntimeError::JournalTransient(_) => (Class::TransientIo, Code::TransportUnavailable, true),
        RuntimeError::JournalIntegrity(_) => (Class::Integrity, Code::InvalidRecord, false),
        RuntimeError::Spool(message) if message.contains("Io(") || message.contains("Enospc") => {
            (Class::TransientIo, Code::TransportUnavailable, true)
        }
        RuntimeError::OwnershipLost | RuntimeError::UnexpectedCopyBothLoss => {
            (Class::OwnershipLost, Code::TransportUnavailable, false)
        }
        RuntimeError::Spool(_) => (Class::Integrity, Code::ResourceLimit, false),
        RuntimeError::Decode(_)
        | RuntimeError::Protocol(_)
        | RuntimeError::ReconciliationRequired => (Class::Integrity, Code::InvalidRecord, false),
        RuntimeError::Feedback => (Class::TransientSource, Code::TransportUnavailable, true),
        RuntimeError::RetryNotDue { .. } => (Class::TransientSource, Code::DeadlineExceeded, true),
    }
}

fn capture_filesystem_is_hard(
    filesystems: &[RuntimePressureFilesystem],
    observations: &[(u64, crate::m2_pressure::PressureThresholds)],
) -> Result<bool, RuntimeError> {
    if filesystems.len() != observations.len() {
        return Err(RuntimeError::JournalTransient(
            "pressure filesystem profile mismatch".into(),
        ));
    }
    filesystems
        .iter()
        .zip(observations)
        .filter(|(filesystem, _)| filesystem.capture_critical)
        .try_fold(false, |hard, (_, (free, thresholds))| {
            crate::m2_pressure::decide_pressure(*free, *thresholds)
                .map(|decision| hard || decision.state == crate::m2_pressure::PressureState::Hard)
                .map_err(|error| RuntimeError::JournalTransient(error.to_string()))
        })
}

fn pressure_stops_capture(
    decision: &crate::m2_pressure::PressureDecision,
    capture_filesystem_hard: bool,
) -> bool {
    decision.actions.safe_stop_capture && capture_filesystem_hard
}

fn validate_pressure_role_devices(
    budget_devices: &[u64],
    roles: &[(usize, u64, bool)],
) -> Result<std::collections::BTreeSet<usize>, &'static str> {
    let mut capture_budget_indexes = std::collections::BTreeSet::new();
    for &(budget_index, role_device, capture_critical) in roles {
        if budget_devices.get(budget_index).copied() != Some(role_device) {
            return Err(
                "pressure role path is on a different physical filesystem than its budget root",
            );
        }
        if capture_critical {
            capture_budget_indexes.insert(budget_index);
        }
    }
    Ok(capture_budget_indexes)
}

fn configured_pressure_filesystems(
    config: &crate::m1_config::PublicConfig,
) -> Result<Vec<RuntimePressureFilesystem>, CaptureFailure> {
    use std::os::unix::fs::MetadataExt;
    let sqlite_path = std::path::Path::new(&config.storage.sqlite_path);
    let sqlite_root = sqlite_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let role_paths = [
        (sqlite_root, true),
        (std::path::Path::new(&config.storage.sqlite_temp_path), true),
        (std::path::Path::new(&config.storage.spool_path), true),
        (std::path::Path::new(&config.archive.root), false),
    ];
    let budget_devices = config
        .budgets
        .iter()
        .map(|budget| {
            std::fs::metadata(&budget.root)
                .map(|metadata| metadata.dev())
                .map_err(|_| CaptureFailure::at("storage", "M2_PRESSURE_BUDGET_PATH_UNAVAILABLE"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let roles = role_paths
        .iter()
        .map(|(path, capture_critical)| {
            let budget_index = crate::m1_config::filesystem_budget_for_path(&config.budgets, path)
                .map_err(|_| {
                    CaptureFailure::at("configuration", "M2_PRESSURE_BUDGET_PATH_UNCOVERED")
                })?;
            let device = std::fs::metadata(path)
                .map_err(|_| CaptureFailure::at("storage", "M2_PRESSURE_ROLE_PATH_UNAVAILABLE"))?
                .dev();
            Ok((budget_index, device, *capture_critical))
        })
        .collect::<Result<Vec<_>, CaptureFailure>>()?;
    let capture_budget_indexes =
        validate_pressure_role_devices(&budget_devices, &roles).map_err(|_| {
            CaptureFailure::at(
                "configuration",
                "M2_PRESSURE_FILESYSTEM_ASSOCIATION_MISMATCH",
            )
        })?;

    let mut paths = BTreeMap::new();
    let mut budgets = Vec::with_capacity(config.budgets.len());
    for (index, budget) in config.budgets.iter().enumerate() {
        let device = budget_devices[index];
        paths
            .entry(device)
            .or_insert_with(|| std::path::PathBuf::from(&budget.root));
        budgets.push(crate::m2_pressure::FilesystemBudget {
            filesystem_id: device,
            total_bytes: budget.total_bytes.0,
            reserved_free_bytes: budget.reserved_free_bytes.0,
            capture_critical: capture_budget_indexes.contains(&index),
        });
    }
    let derived = crate::m2_pressure::derive_physical_filesystem_thresholds(
        &budgets,
        [
            config.conditions.warning,
            config.conditions.action,
            config.conditions.critical,
            config.conditions.hard,
        ],
    )
    .map_err(|_| CaptureFailure::at("configuration", "M2_PRESSURE_BUDGET_INVALID"))?;
    derived
        .into_iter()
        .map(|profile| {
            let observation_path = paths
                .remove(&profile.filesystem_id)
                .ok_or_else(|| CaptureFailure::at("configuration", "M2_PRESSURE_DEVICE_MISSING"))?;
            Ok(RuntimePressureFilesystem {
                observation_path,
                thresholds: profile.thresholds,
                capture_critical: profile.capture_critical,
            })
        })
        .collect()
}

fn filesystem_free_bytes(path: &std::path::Path) -> std::io::Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(c.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };
    Ok(stat.f_bavail.saturating_mul(stat.f_frsize))
}

struct TransportReceiveLane {
    admitted_bytes: usize,
    _reservation: Vec<u8>,
}
impl TransportReceiveLane {
    fn admit(max_frame_bytes: usize) -> Result<Self, CaptureFailure> {
        if max_frame_bytes == 0 {
            return Err(CaptureFailure::at(
                "admission",
                "M2_RECEIVE_ADMISSION_INVALID",
            ));
        }
        let mut reservation = Vec::new();
        reservation
            .try_reserve_exact(max_frame_bytes)
            .map_err(|_| CaptureFailure::at("admission", "M2_RECEIVE_ADMISSION_DENIED"))?;
        Ok(Self {
            admitted_bytes: max_frame_bytes,
            _reservation: reservation,
        })
    }
    fn validate(self, observed: usize) -> Result<(), CaptureFailure> {
        if observed > self.admitted_bytes {
            Err(CaptureFailure::at("admission", "M2_RECEIVE_FRAME_LIMIT"))
        } else {
            Ok(())
        }
    }
}
async fn capture_copyboth_until_with_probe<J: DurableJournal, S: SpoolFactory, G: FeedbackGate>(
    config: &CaptureConfig,
    cancellation: &CancellationToken,
    runtime: &mut CaptureRuntime<J, S, G>,
    stop_after_commits: u64,
    control_cadence: std::time::Duration,
    mut ownership_probe: impl FnMut() -> bool,
) -> Result<(), CaptureFailure> {
    let (mut connection, contracts) = setup_runtime(config)?;
    runtime.contracts = contracts;
    loop {
        runtime
            .service_pressure()
            .map_err(|_| CaptureFailure::at("pressure", "M2_PRESSURE_SERVICE_FAILED"))?;
        if !ownership_probe() {
            runtime.ownership_lost();
            runtime
                .persist_if_enabled(
                    crate::failure_policy::FailureClass::OwnershipLost,
                    crate::failure_policy::StableErrorCode::TransportUnavailable,
                )
                .map_err(|_| CaptureFailure::at("failure_policy", "M2_FAILURE_PERSIST_FAILED"))?;
            return Err(CaptureFailure::at("ownership", "M2_OWNERSHIP_LOST"));
        }
        if cancellation.is_cancelled() {
            runtime.graceful_shutdown().map_err(|_| {
                CaptureFailure::at("runtime", "M2_SHUTDOWN_RECONCILIATION_REQUIRED")
            })?;
            return Ok(());
        }
        // Admit a maximum-sized CopyData allocation before transport receive. The read future is
        // cancelled (not dropped) at every control tick, so a quiet source cannot starve ownership.
        let admission = TransportReceiveLane::admit(WireLimits::default().max_copy_data_bytes)?;
        let read_cancellation = cancellation.child_token();
        let mut read = Box::pin(connection.get_copy_data_async(&read_cancellation));
        let control_tick = tokio::time::sleep(control_cadence);
        tokio::pin!(control_tick);
        let received = tokio::select! {
            result = &mut read => Some(result),
            _ = &mut control_tick => {
                read_cancellation.cancel();
                let result = read.as_mut().await;
                if cancellation.is_cancelled() { Some(result) } else {
                    if !ownership_probe() {
                        runtime.ownership_lost();
                        runtime.persist_if_enabled(
                            crate::failure_policy::FailureClass::OwnershipLost,
                            crate::failure_policy::StableErrorCode::TransportUnavailable,
                        ).map_err(|_| CaptureFailure::at("failure_policy", "M2_FAILURE_PERSIST_FAILED"))?;
                        return Err(CaptureFailure::at("ownership", "M2_OWNERSHIP_LOST"));
                    }
                    match result { Ok(frame) => Some(Ok(frame)), Err(_) => None }
                }
            }
        };
        let Some(received) = received else { continue };
        drop(read); // completed or cooperatively cancelled and awaited above
        let frame = match received {
            Ok(frame) => frame,
            Err(_) if cancellation.is_cancelled() => {
                runtime.graceful_shutdown().map_err(|_| {
                    CaptureFailure::at("runtime", "M2_SHUTDOWN_RECONCILIATION_REQUIRED")
                })?;
                return Ok(());
            }
            Err(_) => {
                runtime.unexpected_eof();
                runtime
                    .persist_if_enabled(
                        crate::failure_policy::FailureClass::TransientSource,
                        crate::failure_policy::StableErrorCode::TransportUnavailable,
                    )
                    .map_err(|_| {
                        CaptureFailure::at("failure_policy", "M2_FAILURE_PERSIST_FAILED")
                    })?;
                return Err(CaptureFailure::at("runtime", "M2_COPYBOTH_UNEXPECTED_LOSS"));
            }
        };
        admission.validate(frame.len())?;
        if let Err(error) = runtime.receive(&frame) {
            let (class, code, supervisor_retry) = classify_runtime_failure(&error);
            runtime
                .persist_if_enabled(class, code)
                .map_err(|_| CaptureFailure::at("failure_policy", "M2_FAILURE_PERSIST_FAILED"))?;
            if supervisor_retry {
                return Err(CaptureFailure::at("runtime", "M2_CAPTURE_FAILED"));
            }
            // Deterministic failures stay connected but fenced: subsequent frames are drained,
            // requested keepalives reply only at the durable boundary, and an explicit re-arm API
            // may restore decoding after external recovery proof.
            continue;
        }
        for packet in runtime.take_feedback() {
            if !ownership_probe() {
                runtime.ownership_lost();
                runtime
                    .persist_if_enabled(
                        crate::failure_policy::FailureClass::OwnershipLost,
                        crate::failure_policy::StableErrorCode::TransportUnavailable,
                    )
                    .map_err(|_| {
                        CaptureFailure::at("failure_policy", "M2_FAILURE_PERSIST_FAILED")
                    })?;
                return Err(CaptureFailure::at("ownership", "M2_OWNERSHIP_LOST"));
            }
            crate::m2_fault_status::fault_hook(crate::m2_fault_status::FaultHook::BeforeFeedback);
            if connection
                .send_standby_status_update(
                    packet.write_lsn,
                    packet.flush_lsn,
                    packet.apply_lsn,
                    packet.reply_requested,
                )
                .await
                .is_err()
            {
                runtime.unexpected_eof();
                runtime
                    .persist_if_enabled(
                        crate::failure_policy::FailureClass::TransientSource,
                        crate::failure_policy::StableErrorCode::TransportUnavailable,
                    )
                    .map_err(|_| {
                        CaptureFailure::at("failure_policy", "M2_FAILURE_PERSIST_FAILED")
                    })?;
                return Err(CaptureFailure::at("runtime", "M2_FEEDBACK_TRANSPORT_LOSS"));
            }
            crate::m2_fault_status::fault_hook(crate::m2_fault_status::FaultHook::AfterFeedback);
        }
        if runtime.committed_transactions() >= stop_after_commits {
            runtime.graceful_shutdown().map_err(|_| {
                CaptureFailure::at("runtime", "M2_SHUTDOWN_RECONCILIATION_REQUIRED")
            })?;
            return Ok(());
        }
    }
}

pub struct PgSourceLock {
    connection: PgReplicationConnection,
    backend_pid: i32,
    nonce: String,
    key: i64,
}
impl crate::m2_ownership::SourceLockSession for PgSourceLock {
    fn backend_pid(&self) -> i32 {
        self.backend_pid
    }
    fn connection_nonce(&self) -> &str {
        &self.nonce
    }
    fn advisory_lock_key(&self) -> i64 {
        self.key
    }
    fn try_lock(&mut self) -> Result<bool, crate::m2_ownership::OwnershipError> {
        self.connection
            .exec(&format!("SELECT pg_try_advisory_lock({})::int", self.key))
            .ok()
            .and_then(|r| r.get_value(0, 0))
            .map(|v| v == "1")
            .ok_or(crate::m2_ownership::OwnershipError::SourceSessionLost)
    }
    fn healthy(&mut self) -> bool {
        self.connection.is_alive() && self.connection.exec("SELECT 1").is_ok()
    }
    fn unlock(&mut self) -> Result<(), crate::m2_ownership::OwnershipError> {
        self.connection
            .exec(&format!("SELECT pg_advisory_unlock({})::int", self.key))
            .map(|_| ())
            .map_err(|_| crate::m2_ownership::OwnershipError::SourceSessionLost)
    }
}

pub fn acquire_production_ownership(
    config: &crate::m1_config::LoadedConfig,
    run_id: &str,
) -> Result<crate::m2_ownership::OwnershipGuard<PgSourceLock>, CaptureFailure> {
    // Read the persisted schedule before any source connection or lock attempt. A supervisor
    // restart therefore cannot shorten a transient delay or bypass deterministic safe-stop.
    let journal_path = std::path::Path::new(&config.public().storage.sqlite_path);
    if journal_path.exists() {
        let connection = rusqlite::Connection::open_with_flags(
            journal_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .map_err(|_| CaptureFailure::at("failure_policy", "M2_FAILURE_LOAD_FAILED"))?;
        let gate = connection.query_row(
            "SELECT retry_class,next_retry_at FROM processing_failures WHERE component='capture' AND armed=1",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        ).optional().map_err(|_| CaptureFailure::at("failure_policy", "M2_FAILURE_LOAD_FAILED"))?;
        if let Some((class, next)) = gate {
            if class != "transient" && class != "rearmed" {
                return Err(CaptureFailure::at(
                    "failure_policy",
                    "M2_EXPLICIT_REARM_REQUIRED",
                ));
            }
            let deadline = if class == "transient" {
                Some(
                    next.and_then(|value| value.strip_prefix("unix-ms:")?.parse::<u64>().ok())
                        .ok_or_else(|| {
                            CaptureFailure::at("failure_policy", "M2_FAILURE_LOAD_FAILED")
                        })?,
                )
            } else {
                None
            };
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| CaptureFailure::at("clock", "M2_CLOCK_INVALID"))?
                .as_millis() as u64;
            if deadline.is_some_and(|deadline| deadline > now) {
                return Err(CaptureFailure::at("failure_policy", "M2_RETRY_NOT_DUE"));
            }
        }
    }
    use crate::m2_ownership::{OwnerKind, OwnershipGuard};
    use rusqlite::OptionalExtension;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let dsn = config
        .runtime_dsn()
        .ok_or_else(|| CaptureFailure::at("ownership", "M2_RUNTIME_DSN_UNAVAILABLE"))?;
    let mut connection = PgReplicationConnection::connect(dsn)
        .map_err(|_| CaptureFailure::at("ownership", "M2_SOURCE_LOCK_CONNECTION_FAILED"))?;
    let backend_pid = connection
        .exec("SELECT pg_backend_pid()::text")
        .ok()
        .and_then(|r| r.get_value(0, 0))
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| CaptureFailure::at("ownership", "M2_SOURCE_LOCK_PID_FAILED"))?;
    let digest = Sha256::digest(config.fingerprints().source.as_bytes());
    let key = i64::from_be_bytes(digest[..8].try_into().expect("sha256 width"));
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CaptureFailure::at("ownership", "M2_CLOCK_INVALID"))?
        .as_nanos();
    let nonce = format!(
        "{:x}",
        Sha256::digest(format!("{run_id}:{backend_pid}:{now}").as_bytes())
    );
    let source = PgSourceLock {
        connection,
        backend_pid,
        nonce,
        key,
    };
    let mut guard = OwnershipGuard::acquire(
        std::path::Path::new(&config.public().storage.sqlite_path),
        run_id.into(),
        OwnerKind::Runtime,
        Duration::from_millis(config.public().source.ownership_deadline_ms.0),
        source,
    )
    .map_err(|_| CaptureFailure::at("ownership", "M2_OWNERSHIP_UNAVAILABLE"))?;
    guard
        .reconcile_after_unclean_release(|| true)
        .map_err(|_| CaptureFailure::at("ownership", "M2_STARTUP_RECONCILIATION_FAILED"))?;
    Ok(guard)
}

struct ProductionControlLane {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ownership_lost: std::sync::Arc<std::sync::atomic::AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl ProductionControlLane {
    fn start(
        dsn: String,
        backend_pid: i32,
        cadence: std::time::Duration,
    ) -> Result<Self, CaptureFailure> {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        if cadence.is_zero() {
            return Err(CaptureFailure::at(
                "control_lane",
                "M2_CONTROL_CADENCE_INVALID",
            ));
        }
        let stop = Arc::new(AtomicBool::new(false));
        let ownership_lost = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let worker_lost = ownership_lost.clone();
        let worker = std::thread::Builder::new().name("capture-control-lane".into()).spawn(move || {
            let mut connection = match PgReplicationConnection::connect(&dsn) {
                Ok(connection) => connection,
                Err(_) => { worker_lost.store(true, Ordering::Release); return; }
            };
            while !worker_stop.load(Ordering::Acquire) {
                let healthy = connection.exec(&format!(
                    "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid={backend_pid} AND granted)::int"
                )).ok().and_then(|r| r.get_value(0, 0)).is_some_and(|v| v == "1");
                if !healthy { worker_lost.store(true, Ordering::Release); return; }
                std::thread::sleep(cadence);
            }
        }).map_err(|_| CaptureFailure::at("control_lane", "M2_CONTROL_LANE_START_FAILED"))?;
        Ok(Self {
            stop,
            ownership_lost,
            worker: Some(worker),
        })
    }
    fn ownership_live(&self) -> bool {
        !self
            .ownership_lost
            .load(std::sync::atomic::Ordering::Acquire)
    }
}
impl Drop for ProductionControlLane {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

pub fn observe_live_source(
    dsn: &str,
    publication_name: &str,
    slot_name: &str,
) -> Result<crate::m2_reconcile::LiveSourceObservation, CaptureFailure> {
    use crate::m1_control_fixtures::PublicationSpec;
    use std::collections::BTreeSet;

    if !crate::article1_capture::valid_pg_identifier(publication_name)
        || !crate::article1_capture::valid_pg_slot_name(slot_name)
    {
        return Err(CaptureFailure::at(
            "configuration",
            "M2_PROTOCOL_CONFIG_INVALID",
        ));
    }
    let has_replication_parameter = dsn.split_once('?').is_some_and(|(_, query)| {
        query.split('&').any(|item| {
            item.split_once('=')
                .is_some_and(|(key, _)| key == "replication")
        })
    });
    let observation_dsn = if has_replication_parameter {
        dsn.to_owned()
    } else if dsn.contains('?') {
        format!("{dsn}&replication=database")
    } else {
        format!("{dsn}?replication=database")
    };
    let mut connection = PgReplicationConnection::connect(&observation_dsn).map_err(|_| {
        CaptureFailure::at("reconciliation", "M2_SOURCE_OBSERVATION_CONNECT_FAILED")
    })?;
    let identity = connection
        .exec("IDENTIFY_SYSTEM")
        .map_err(|_| CaptureFailure::at("reconciliation", "M2_SOURCE_IDENTITY_QUERY_FAILED"))?;
    let value = |column| {
        identity
            .get_value(0, column)
            .ok_or_else(|| CaptureFailure::at("reconciliation", "M2_SOURCE_IDENTITY_QUERY_FAILED"))
    };
    let source_system_id = value(0)?;
    let timeline_id = value(1)?;
    // The database OID, unlike a database name or IDENTIFY_SYSTEM's xlog position, is the
    // immutable database identity bound by the source contract. pg_database is readable by the
    // runtime role without granting administrative mutation privileges.
    let database = connection
        .exec("SELECT oid::text FROM pg_database WHERE datname=current_database()")
        .map_err(|_| CaptureFailure::at("reconciliation", "M2_DATABASE_IDENTITY_QUERY_FAILED"))?;
    let database_id = database
        .get_value(0, 0)
        .ok_or_else(|| CaptureFailure::at("reconciliation", "M2_DATABASE_IDENTITY_QUERY_FAILED"))?;

    // Fingerprint the observed catalog definition, not the configured expectation. The two
    // catalog reads use stable order and the same typed canonical representation as init.
    let publication = connection.exec(&format!(
        "SELECT p.pubname,pg_get_userbyid(p.pubowner),concat_ws(',',CASE WHEN p.pubdelete THEN 'delete' END,CASE WHEN p.pubinsert THEN 'insert' END,CASE WHEN p.pubtruncate THEN 'truncate' END,CASE WHEN p.pubupdate THEN 'update' END) FROM pg_publication p WHERE p.pubname='{publication_name}'"
    )).map_err(|_| CaptureFailure::at("reconciliation", "M2_PUBLICATION_OBSERVATION_FAILED"))?;
    let publication_value = |column| {
        publication.get_value(0, column).ok_or_else(|| {
            CaptureFailure::at("reconciliation", "M2_PUBLICATION_OBSERVATION_FAILED")
        })
    };
    let publication_name = publication_value(0)?;
    let owner_role = publication_value(1)?;
    let operations = publication_value(2)?;
    let relations = connection.exec(&format!(
        "SELECT coalesce(string_agg(schemaname||'.'||tablename,chr(31) ORDER BY schemaname,tablename),'') FROM pg_publication_tables WHERE pubname='{publication_name}'"
    )).map_err(|_| CaptureFailure::at("reconciliation", "M2_PUBLICATION_OBSERVATION_FAILED"))?
        .get_value(0, 0)
        .ok_or_else(|| CaptureFailure::at("reconciliation", "M2_PUBLICATION_OBSERVATION_FAILED"))?;
    let observed_publication = PublicationSpec {
        name: publication_name,
        owner_role,
        relations: relations
            .split('\u{1f}')
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .collect::<BTreeSet<_>>(),
        operations: operations
            .split(',')
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .collect::<BTreeSet<_>>(),
    };

    let slot = connection.exec(&format!("SELECT plugin,coalesce(confirmed_flush_lsn::text,''),coalesce(restart_lsn::text,''),coalesce(wal_status,''),coalesce(invalidation_reason,'') FROM pg_replication_slots WHERE slot_name='{slot_name}'"))
        .map_err(|_| CaptureFailure::at("reconciliation", "M2_SLOT_OBSERVATION_FAILED"))?;
    let plugin = slot.get_value(0, 0);
    let position = |column| {
        slot.get_value(0, column)
            .filter(|v| !v.is_empty())
            .and_then(|v| parse_lsn(&v).ok())
            .map(|v| format!("{v:016X}"))
    };
    let wal_status = slot.get_value(0, 3).filter(|value| !value.is_empty());
    let invalidation_reason = slot.get_value(0, 4).filter(|value| !value.is_empty());
    let restart_lsn = position(2);
    let slot_exists = plugin.is_some();
    let slot_valid =
        slot_exists && plugin.as_deref() == Some("pgoutput") && invalidation_reason.is_none();
    let resume_wal_available = slot_valid
        && restart_lsn.is_some()
        && wal_status
            .as_deref()
            .is_some_and(|value| matches!(value, "reserved" | "extended"));
    Ok(crate::m2_reconcile::LiveSourceObservation {
        source_system_id,
        timeline_id,
        database_id,
        slot_name: slot_name.into(),
        plugin: plugin.clone().unwrap_or_default(),
        publication_fingerprint: observed_publication.fingerprint(),
        protocol_fingerprint: "pgoutput-v1".into(),
        slot_exists,
        slot_valid,
        invalidation_reason,
        wal_status,
        resume_wal_available,
        confirmed_flush_lsn: position(1),
        restart_lsn,
    })
}

struct ProductionArchiveInspection;
impl crate::m2_reconcile::ArchiveReconciler for ProductionArchiveInspection {
    fn inspect(&mut self) -> crate::m2_reconcile::ArchiveReconciliation {
        crate::m2_reconcile::ArchiveReconciliation::Compatible
    }
}

/// Builds the bounded durable runtime from the canonical loaded configuration. The caller owns
/// process supervision and the source advisory-lock guard for the full future lifetime.
pub async fn run_loaded_config(
    config: &crate::m1_config::LoadedConfig,
    cancellation: &CancellationToken,
    ownership: &mut crate::m2_ownership::OwnershipGuard<PgSourceLock>,
) -> Result<(), CaptureFailure> {
    use crate::m2_journal::{CommitLimits, SourceIdentity};
    use crate::m2_schema::open_writer;
    use crate::m2_spool::{
        DiskAdmission, FilesystemAdmissionController, FilesystemLimit, MemoryBudget, MemoryLimits,
        PosixAllocation, SpoolLimits, StatvfsSpace,
    };
    use std::os::unix::fs::MetadataExt;
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let public = config.public();
    let dsn = config
        .runtime_dsn()
        .ok_or_else(|| CaptureFailure::at("configuration", "M2_RUNTIME_DSN_UNAVAILABLE"))?
        .to_owned();
    let capture_config = CaptureConfig::production(
        dsn.clone(),
        public.source.publication.clone(),
        public.source.slot.clone(),
        public
            .tables
            .iter()
            .map(|table| table.source_relation.clone()),
    )?;
    let heartbeat_dsn = config
        .control_writer_dsn()
        .ok_or_else(|| CaptureFailure::at("configuration", "M2_CONTROL_DSN_UNAVAILABLE"))?
        .to_owned();
    let journal_path = PathBuf::from(&public.storage.sqlite_path);
    let spool_path = PathBuf::from(&public.storage.spool_path);
    if let Some(parent) = journal_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|_| CaptureFailure::at("storage", "M2_STATE_DIRECTORY_UNAVAILABLE"))?;
    }
    std::fs::create_dir_all(&spool_path)
        .map_err(|_| CaptureFailure::at("storage", "M2_SPOOL_DIRECTORY_UNAVAILABLE"))?;
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CaptureFailure::at("clock", "M2_CLOCK_INVALID"))?;
    let now = elapsed.as_millis() as i64;
    let startup_run_id = format!("production-startup-{}", elapsed.as_nanos());
    let live_source = observe_live_source(&dsn, &public.source.publication, &public.source.slot)?;
    // Initialization is keyed by durable schema/source state, never by path existence. A crash
    // after migrations but before this transaction leaves no partial identity receipt; retrying
    // observes zero rows and atomically writes the full live identity.
    {
        let mut initial = open_writer(&journal_path, &startup_run_id, 1, now)
            .map_err(|_| CaptureFailure::at("journal", "M2_JOURNAL_OPEN_FAILED"))?;
        let source_rows: i64 = initial
            .connection()
            .query_row("SELECT count(*) FROM source_state", [], |row| row.get(0))
            .map_err(|_| CaptureFailure::at("journal", "M2_SOURCE_STATE_READ_FAILED"))?;
        if source_rows == 0 {
            #[cfg(debug_assertions)]
            if let (Some(marker), Some(release)) = (
                std::env::var_os("M2_RECONCILE_FAULT_BEFORE_SOURCE_RECEIPT_MARKER"),
                std::env::var_os("M2_RECONCILE_FAULT_BEFORE_SOURCE_RECEIPT_RELEASE"),
            ) {
                std::fs::write(&marker, b"before-source-state-receipt").map_err(|_| {
                    CaptureFailure::at("journal", "M2_SOURCE_STATE_FAULT_MARKER_FAILED")
                })?;
                while !std::path::Path::new(&release).exists() {
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
            let tx = initial
                .connection_mut()
                .transaction()
                .map_err(|_| CaptureFailure::at("journal", "M2_SOURCE_STATE_INIT_FAILED"))?;
            tx.execute("INSERT INTO source_state(singleton,capture_epoch,source_system_id,timeline_id,database_id,slot_name,plugin,publication_fingerprint,protocol_fingerprint) VALUES(1,?1,?2,?3,?4,?5,?6,?7,?8)",rusqlite::params![config.fingerprints().runtime,live_source.source_system_id,live_source.timeline_id,live_source.database_id,live_source.slot_name,live_source.plugin,live_source.publication_fingerprint,live_source.protocol_fingerprint]).map_err(|_|CaptureFailure::at("journal","M2_SOURCE_STATE_INIT_FAILED"))?;
            tx.commit()
                .map_err(|_| CaptureFailure::at("journal", "M2_SOURCE_STATE_INIT_FAILED"))?;
        } else if source_rows != 1 {
            return Err(CaptureFailure::at(
                "journal",
                "M2_SOURCE_STATE_CARDINALITY_INVALID",
            ));
        }
    }
    let receipt = crate::m2_reconcile::reconcile_startup(
        &journal_path,
        &startup_run_id,
        &live_source,
        &[],
        &mut ProductionArchiveInspection,
    )
    .map_err(|_| CaptureFailure::at("reconciliation", "M2_STARTUP_RECONCILIATION_FAILED"))?;
    if matches!(
        receipt.outcome,
        crate::m2_reconcile::StartupOutcome::Blocked
            | crate::m2_reconcile::StartupOutcome::RequiresReseed
            | crate::m2_reconcile::StartupOutcome::BootstrapAmbiguousRequiresRestart
    ) {
        return Err(CaptureFailure::at("reconciliation", "M2_STARTUP_BLOCKED"));
    }
    let cadence_ms = public.source.heartbeat_cadence_ms.0;
    let initial_retry_ms = (cadence_ms / 10).max(1);
    let heartbeat_lane = crate::m2_heartbeat::PublishedHeartbeatLane::start(
        heartbeat_dsn,
        crate::m2_heartbeat::HeartbeatPolicy {
            cadence_ms,
            initial_retry_ms,
            max_retry_ms: (cadence_ms / 2).max(initial_retry_ms),
        },
        now as u64,
        Duration::from_millis(public.source.maximum_operation_ms.0),
        crate::m2_heartbeat::HeartbeatLogContext {
            scenario_id: "SCN-HEARTBEAT-PERMISSION-OUTAGE".into(),
            correlation_id: format!("heartbeat:{startup_run_id}"),
            run_id: startup_run_id.clone(),
            capture_epoch: config.fingerprints().runtime.clone(),
            config_fingerprint: config.fingerprints().runtime.clone(),
        },
    )
    .map_err(|_| CaptureFailure::at("heartbeat", "M2_HEARTBEAT_LANE_INVALID"))?;
    let writer = open_writer(&journal_path, "production-run", 1, now)
        .map_err(|_| CaptureFailure::at("journal", "M2_JOURNAL_OPEN_FAILED"))?;
    let durable = writer
        .connection()
        .query_row(
            "SELECT durable_transaction_end_lsn FROM source_state WHERE singleton=1",
            [],
            |r| r.get::<_, Option<String>>(0),
        )
        .ok()
        .flatten()
        .map(|v| parse_lsn(&v))
        .transpose()
        .map_err(|_| CaptureFailure::at("journal", "M2_DURABLE_LSN_INVALID"))?;
    let mut startup_memory = MemoryBudget::new(MemoryLimits {
        process_limit: public.limits.process_memory_bytes.0 as usize,
        runtime_fixed: 1,
        receive: public.limits.max_wire_frame_bytes.0 as usize,
        decoder: public.limits.max_event_bytes.0 as usize,
        staging: public.limits.max_transaction_bytes.0 as usize,
    })
    .map_err(|_| CaptureFailure::at("reconciliation", "M2_STARTUP_MEMORY_INVALID"))?;
    crate::m2_spool::classify_startup_spools(
        ownership.state_lock(),
        &journal_path,
        &spool_path,
        config.fingerprints().runtime.as_str(),
        "production-run",
        public.limits.max_event_bytes.0 as usize,
        public.limits.max_transaction_bytes.0,
        public.limits.max_transaction_events,
        1024,
        &mut startup_memory,
        |epoch, xid| {
            let count = writer
                .connection()
                .query_row(
                    "SELECT count(*) FROM source_transactions WHERE capture_epoch=?1 AND xid=?2",
                    rusqlite::params![epoch, xid],
                    |r| r.get::<_, u64>(0),
                )
                .unwrap_or(u64::MAX);
            if count == 0 {
                crate::m2_spool::ExistingTransaction::Uncommitted
            } else if count == 1 {
                crate::m2_spool::ExistingTransaction::CommittedSame
            } else {
                crate::m2_spool::ExistingTransaction::Contradictory
            }
        },
    )
    .map_err(|_| CaptureFailure::at("reconciliation", "M2_STARTUP_SPOOL_RECONCILIATION_FAILED"))?;
    let store = JournalStore::new(
        writer,
        SourceIdentity {
            capture_epoch: config.fingerprints().runtime.clone(),
            source_system_id: live_source.source_system_id,
            timeline_id: live_source.timeline_id,
            database_id: live_source.database_id,
            slot_name: public.source.slot.clone(),
            publication_fingerprint: live_source.publication_fingerprint.clone(),
            protocol_fingerprint: "pgoutput-v1".into(),
        },
        CommitLimits {
            max_events: public.limits.max_transaction_events as usize,
            max_copied_bytes: public.limits.max_transaction_bytes.0 as usize,
            max_writer_hold: Duration::from_millis(public.source.maximum_operation_ms.0),
        },
    )
    .map_err(|_| CaptureFailure::at("journal", "M2_JOURNAL_CONFIG_INVALID"))?;
    let dev = std::fs::metadata(&spool_path)
        .map_err(|_| CaptureFailure::at("storage", "M2_SPOOL_METADATA_FAILED"))?
        .dev();
    let budget = public
        .budgets
        .iter()
        .filter(|b| b.capture_spool_bytes.0 > 0)
        .max_by_key(|b| b.capture_spool_bytes.0)
        .ok_or_else(|| CaptureFailure::at("configuration", "M2_SPOOL_BUDGET_MISSING"))?;
    let disk = FilesystemAdmissionController::new(DiskAdmission::default());
    disk.configure(
        dev,
        FilesystemLimit {
            total_budget: budget.capture_spool_bytes.0,
            emergency_reserve: budget.reserved_free_bytes.0,
        },
    )
    .map_err(|_| CaptureFailure::at("configuration", "M2_SPOOL_BUDGET_INVALID"))?;
    let limits = public.limits.clone();
    let make_disk = disk.clone();
    let make_path = spool_path.clone();
    let epoch = config.fingerprints().runtime.clone();
    let factory = move |xid: u32| -> Result<Box<dyn RuntimeSpool>, RuntimeError> {
        let memory = MemoryBudget::new(MemoryLimits {
            process_limit: limits.process_memory_bytes.0 as usize,
            runtime_fixed: 1,
            receive: limits.max_wire_frame_bytes.0 as usize,
            decoder: limits.max_event_bytes.0 as usize,
            staging: limits.max_transaction_bytes.0 as usize,
        })
        .map_err(|e| RuntimeError::Spool(e.to_string()))?;
        TxnBuffer::new(
            make_path.clone(),
            epoch.clone(),
            xid.to_string(),
            "production-run".into(),
            SpoolLimits {
                max_frame_bytes: limits.max_wire_frame_bytes.0 as usize,
                max_event_bytes: limits.max_event_bytes.0 as usize,
                max_transaction_bytes: limits.max_transaction_bytes.0,
                max_transaction_events: limits.max_transaction_events,
                memory_prefix_bytes: limits.max_event_bytes.0 as usize,
            },
            memory,
            make_disk.clone(),
            Box::new(StatvfsSpace),
            Box::new(PosixAllocation),
        )
        .map(|v| Box::new(v) as Box<dyn RuntimeSpool>)
        .map_err(|e| RuntimeError::Spool(e.to_string()))
    };
    // M0-PROVISIONAL: boring-cdc-m2-capture-runtime.1 (writer queue caps and capture burst).
    let writer_service = JournalWriterService::new(store, [8, 4, 2, 1], 4)
        .map_err(|_| CaptureFailure::at("journal", "M2_WRITER_SERVICE_INVALID"))?;
    if let Some((retry_class, next_retry_at_ms)) = writer_service
        .capture_startup_gate()
        .map_err(|_| CaptureFailure::at("failure_policy", "M2_FAILURE_LOAD_FAILED"))?
    {
        let now_ms =
            u64::try_from(now).map_err(|_| CaptureFailure::at("clock", "M2_CLOCK_INVALID"))?;
        if retry_class != "transient" && retry_class != "rearmed" {
            return Err(CaptureFailure::at(
                "failure_policy",
                "M2_EXPLICIT_REARM_REQUIRED",
            ));
        }
        if next_retry_at_ms.is_some_and(|deadline| deadline > now_ms) {
            return Err(CaptureFailure::at("failure_policy", "M2_RETRY_NOT_DUE"));
        }
    }
    let mut runtime = CaptureRuntime::new(
        writer_service,
        factory,
        NoSnapshotGate,
        Default::default(),
        durable,
    );
    runtime.enable_failure_policy(
        config.fingerprints().runtime.clone(),
        config.fingerprints().runtime.clone(),
    );
    runtime.enable_pressure_filesystems(
        configured_pressure_filesystems(public)?,
        config.fingerprints().runtime.clone(),
        public.retention.replay_window_ms.0,
    );
    let control_lane = ProductionControlLane::start(
        dsn.clone(),
        ownership.backend_pid(),
        Duration::from_millis(public.source.lock_probe_interval_ms.0),
    )?;
    let result = capture_copyboth_until_with_probe(
        &capture_config,
        cancellation,
        &mut runtime,
        u64::MAX,
        Duration::from_millis(public.source.lock_probe_interval_ms.0),
        || {
            control_lane.ownership_live()
                && ownership
                    .probe(std::time::Duration::from_millis(
                        public.source.ownership_deadline_ms.0,
                    ))
                    .is_ok()
        },
    )
    .await;
    if result.is_err() {
        let _ = ownership.unexpected_transport_loss();
    }
    drop(heartbeat_lane);
    result
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::m2_journal::{CommitFault, CommitLimits, SourceIdentity};
    use crate::m2_schema::open_writer;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    static NEXT: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn postgresql_and_canonical_lsn_text_parse_identically() {
        assert_eq!(parse_lsn("0/193A370").unwrap(), 0x193A370);
        assert_eq!(parse_lsn("000000000193A370").unwrap(), 0x193A370);
        assert!(parse_lsn("0/not-hex").is_err());
    }

    #[derive(Default)]
    struct MemorySpools;
    struct MemorySpool(Vec<Vec<u8>>);
    impl RuntimeSpool for MemorySpool {
        fn push(&mut self, b: &[u8]) -> Result<(), SpoolError> {
            self.0.push(b.to_vec());
            Ok(())
        }
        fn drain(&mut self) -> Result<Vec<Vec<u8>>, SpoolError> {
            Ok(std::mem::take(&mut self.0))
        }
        fn finish(self: Box<Self>) -> Result<(), SpoolError> {
            Ok(())
        }
    }
    impl SpoolFactory for MemorySpools {
        fn begin(&mut self, _: u32) -> Result<Box<dyn RuntimeSpool>, RuntimeError> {
            Ok(Box::new(MemorySpool(Vec::new())))
        }
    }
    struct Gate(FeedbackPermit);
    impl FeedbackGate for Gate {
        fn permit(&mut self, _: Option<u64>) -> FeedbackPermit {
            self.0
        }
    }
    fn frame(tag: u8, payload: &[u8]) -> Vec<u8> {
        let mut v = vec![b'w'];
        v.extend_from_slice(&0u64.to_be_bytes());
        v.extend_from_slice(&0u64.to_be_bytes());
        v.extend_from_slice(&0i64.to_be_bytes());
        v.push(tag);
        v.extend_from_slice(payload);
        v
    }
    fn begin(xid: u32, end: u64) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&end.to_be_bytes());
        p.extend_from_slice(&0i64.to_be_bytes());
        p.extend_from_slice(&xid.to_be_bytes());
        frame(b'B', &p)
    }
    fn insert(xid: u32, relation: u32, value: &[u8]) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&relation.to_be_bytes());
        p.push(b'N');
        p.extend_from_slice(&1u16.to_be_bytes());
        p.push(b't');
        p.extend_from_slice(&(value.len() as u32).to_be_bytes());
        p.extend_from_slice(value);
        let _ = xid;
        frame(b'I', &p)
    }
    fn commit(end: u64) -> Vec<u8> {
        let mut p = vec![0];
        p.extend_from_slice(&end.to_be_bytes());
        p.extend_from_slice(&end.to_be_bytes());
        p.extend_from_slice(&0i64.to_be_bytes());
        frame(b'C', &p)
    }
    fn relation() -> (Vec<u8>, RelationContract) {
        let mut p = Vec::new();
        p.extend_from_slice(&7u32.to_be_bytes());
        p.extend_from_slice(b"public\0items\0");
        p.push(b'd');
        p.extend_from_slice(&1u16.to_be_bytes());
        p.push(1);
        p.extend_from_slice(b"id\0");
        p.extend_from_slice(&20u32.to_be_bytes());
        p.extend_from_slice(&(-1i32).to_be_bytes());
        let relation = crate::m1_decoder::Relation {
            id: 7,
            namespace: "public".into(),
            name: "items".into(),
            replica_identity: b'd',
            columns: vec![crate::m1_decoder::Column {
                key: true,
                name: "id".into(),
                type_oid: 20,
                type_modifier: -1,
            }],
        };
        (
            frame(b'R', &p),
            RelationContract {
                relation,
                key_columns: vec![0],
                control: None,
            },
        )
    }
    fn keepalive(wal: u64, requested: bool) -> Vec<u8> {
        let mut v = vec![b'k'];
        v.extend_from_slice(&wal.to_be_bytes());
        v.extend_from_slice(&0i64.to_be_bytes());
        v.push(u8::from(requested));
        v
    }
    fn store(tag: &str) -> (PathBuf, JournalStore) {
        let p = std::env::temp_dir().join(format!(
            "m2-runtime-{tag}-{}-{}.sqlite",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&p);
        let w = open_writer(&p, "run", 1, 1).unwrap();
        let s = JournalStore::new(
            w,
            SourceIdentity {
                capture_epoch: "epoch-a".into(),
                source_system_id: "sys".into(),
                timeline_id: "timeline".into(),
                database_id: "db".into(),
                slot_name: crate::article1_capture::SLOT.into(),
                publication_fingerprint: "publication".into(),
                protocol_fingerprint: "protocol".into(),
            },
            CommitLimits {
                max_events: 16,
                max_copied_bytes: 1 << 20,
                max_writer_hold: Duration::from_secs(2),
            },
        )
        .unwrap();
        (p, s)
    }
    fn runtime(
        gate: FeedbackPermit,
    ) -> (PathBuf, CaptureRuntime<JournalStore, MemorySpools, Gate>) {
        let (p, s) = store("main");
        let (_, c) = relation();
        (
            p,
            CaptureRuntime::new(s, MemorySpools, Gate(gate), BTreeMap::from([(7, c)]), None),
        )
    }

    #[test]
    fn receive_spool_atomic_journal_then_feedback_and_origin() {
        let (p, mut r) = runtime(FeedbackPermit::AllowSafeBoundary { lsn: 0x20 });
        let (rel, _) = relation();
        r.receive(&rel).unwrap();
        r.receive(&begin(9, 0x20)).unwrap();
        let mut o = Vec::new();
        o.extend_from_slice(&0x10u64.to_be_bytes());
        o.extend_from_slice(b"upstream\0");
        r.receive(&frame(b'O', &o)).unwrap();
        r.receive(&insert(9, 7, b"one")).unwrap();
        r.receive(&insert(9, 7, b"two")).unwrap();
        r.receive(&commit(0x20)).unwrap();
        assert_eq!(r.durable_end_lsn(), Some(0x20));
        assert_eq!(
            r.take_feedback(),
            vec![FeedbackPacket {
                write_lsn: 0x20,
                flush_lsn: 0x20,
                apply_lsn: 0x20,
                reply_requested: false
            }]
        );
        let c = rusqlite::Connection::open(&p).unwrap();
        assert_eq!(
            c.query_row("SELECT count(*) FROM journal_events", [], |x| x
                .get::<_, u64>(0))
                .unwrap(),
            2
        );
        assert!(
            c.query_row(
                "SELECT CAST(payload AS TEXT) FROM journal_events LIMIT 1",
                [],
                |x| x.get::<_, String>(0)
            )
            .unwrap()
            .contains("upstream")
        );
        drop(c);
        let _ = std::fs::remove_file(p);
    }
    #[test]
    fn requested_keepalive_never_copies_server_wal_end_and_gate_holds() {
        let (p, mut r) = runtime(FeedbackPermit::AllowSafeBoundary { lsn: 0x10 });
        r.durable_end_lsn = Some(0x10);
        r.receive(&keepalive(0xFFFF, true)).unwrap();
        assert_eq!(r.take_feedback()[0].write_lsn, 0x10);
        r.gate.0 = FeedbackPermit::Hold;
        r.receive(&keepalive(0xFFFF, true)).unwrap();
        assert!(r.take_feedback().is_empty());
        r.gate.0 = FeedbackPermit::Stale;
        r.receive(&keepalive(0xFFFF, true)).unwrap();
        assert!(r.take_feedback().is_empty());
        let _ = std::fs::remove_file(p);
    }
    #[test]
    fn crash_boundaries_never_create_a_feedback_token() {
        let (_p, mut s) = store("fault");
        let payload = b"x".to_vec();
        let e = JournalEvent {
            event_id: "e".into(),
            transaction_ordinal: 0,
            relation_schema_fingerprint: None,
            control_kind: None,
            payload: payload.clone(),
            payload_hash: crate::m2_journal::sha256(&payload),
        };
        let c = SourceCommit {
            transaction_id: "t".into(),
            xid: "1".into(),
            end_lsn: "0000000000000010".into(),
            payload_checksum: transaction_checksum(std::slice::from_ref(&e)),
            schemas: vec![],
            events: vec![e],
        };
        assert_eq!(
            s.commit_atomic(&c, CommitFault::BeforeSqliteCommit),
            Err(JournalError::FaultBeforeCommit)
        );
        assert_eq!(
            s.commit_atomic(&c, CommitFault::AfterSqliteCommit),
            Err(JournalError::AmbiguousAfterCommit)
        );
        assert!(
            s.commit_atomic(&c, CommitFault::None)
                .unwrap()
                .was_duplicate()
        );
    }
    #[test]
    fn deterministic_safe_stop_shutdown_and_no_in_process_reopen() {
        let (p, mut r) = runtime(FeedbackPermit::AllowSafeBoundary { lsn: u64::MAX });
        assert!(r.receive(b"bad").is_err());
        assert_eq!(r.state(), RuntimeState::CaptureSafeStopped);
        assert!(r.receive(&keepalive(1, true)).is_ok());
        let safe_reply = r.take_feedback();
        assert_eq!(safe_reply.len(), 1);
        assert_eq!(safe_reply[0].write_lsn, 0);
        let (p2, mut r2) = runtime(FeedbackPermit::Hold);
        r2.receive(&begin(1, 0x10)).unwrap();
        assert_eq!(
            r2.graceful_shutdown(),
            Err(RuntimeError::ReconciliationRequired)
        );
        assert_eq!(r2.state(), RuntimeState::CaptureSafeStopped);
        assert_eq!(r2.unexpected_eof(), RuntimeError::UnexpectedCopyBothLoss);
        assert_eq!(r2.state(), RuntimeState::OwnershipLost);
        for x in [p, p2] {
            let _ = std::fs::remove_file(x);
        }
    }
    #[test]
    fn production_writer_persists_retry_deadline_for_successor() {
        let (p, store) = store("failure-policy");
        let service = JournalWriterService::new(store, [2, 2, 1, 1], 1).unwrap();
        let (_, contract) = relation();
        let mut runtime = CaptureRuntime::new(
            service,
            MemorySpools,
            Gate(FeedbackPermit::Hold),
            BTreeMap::from([(7, contract)]),
            Some(0x10),
        );
        runtime.enable_failure_policy("epoch-a".into(), "config-a".into());
        runtime
            .persist_capture_failure(
                crate::failure_policy::FailureClass::TransientSource,
                crate::failure_policy::StableErrorCode::TransportUnavailable,
            )
            .unwrap();
        let deadline: String = rusqlite::Connection::open(&p).unwrap().query_row(
            "SELECT next_retry_at FROM processing_failures WHERE component='capture' AND armed=1",
            [], |row| row.get(0),
        ).unwrap();
        assert!(deadline.starts_with("unix-ms:"));
        assert!(
            runtime
                .journal
                .active_capture_retry_at_ms()
                .unwrap()
                .is_some()
        );
        runtime.clear_completed_failure().unwrap();
        let armed: i64 = rusqlite::Connection::open(&p)
            .unwrap()
            .query_row(
                "SELECT armed FROM processing_failures WHERE component='capture'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(armed, 0);
        drop(runtime);
        let _ = std::fs::remove_file(p);
    }
    #[test]
    fn archive_only_hard_pressure_does_not_safe_stop_capture() {
        let thresholds = crate::m2_pressure::PressureThresholds {
            warning: 100,
            action: 80,
            critical: 60,
            hard: 40,
            reserve: 40,
        };
        let hard = crate::m2_pressure::decide_pressure(40, thresholds).unwrap();
        let archive = RuntimePressureFilesystem {
            observation_path: PathBuf::from("archive"),
            thresholds,
            capture_critical: false,
        };
        let shared_state_archive = RuntimePressureFilesystem {
            observation_path: PathBuf::from("state"),
            thresholds,
            capture_critical: true,
        };
        assert!(!pressure_stops_capture(&hard, false));
        assert!(hard.actions.stop_new_archive_backfill);
        assert!(pressure_stops_capture(&hard, true));
        assert!(
            capture_filesystem_is_hard(
                &[archive, shared_state_archive],
                &[(40, thresholds), (40, thresholds)],
            )
            .unwrap()
        );
        assert_eq!(
            validate_pressure_role_devices(&[7, 9], &[(0, 7, true), (1, 9, false)]),
            Ok(std::collections::BTreeSet::from([0]))
        );
        assert!(validate_pressure_role_devices(&[7], &[(0, 9, true)]).is_err());
    }

    #[test]
    fn receive_lane_is_admitted_before_frame_validation() {
        assert!(TransportReceiveLane::admit(0).is_err());
        assert!(TransportReceiveLane::admit(8).unwrap().validate(8).is_ok());
        assert_eq!(
            TransportReceiveLane::admit(8)
                .unwrap()
                .validate(9)
                .unwrap_err()
                .code,
            "M2_RECEIVE_FRAME_LIMIT"
        );
    }
    #[test]
    fn ownership_lost_hook_crosses_runtime_boundary() {
        let (_path, mut runtime) = runtime(FeedbackPermit::Hold);
        assert_eq!(runtime.ownership_lost(), RuntimeError::OwnershipLost);
    }
    #[test]
    fn persisted_retry_schedule_gates_successor_startup() {
        struct S(Option<u64>);
        impl RetrySchedule for S {
            fn next_retry_at_ms(&self) -> Option<u64> {
                self.0
            }
        }
        assert_eq!(
            reconcile_startup(&S(Some(200)), 100, true, true),
            Err(RuntimeError::RetryNotDue {
                next_retry_at_ms: 200
            })
        );
        assert_eq!(reconcile_startup(&S(Some(200)), 200, true, true), Ok(()));
        assert_eq!(
            reconcile_startup(&S(None), 200, false, true),
            Err(RuntimeError::ReconciliationRequired)
        );
    }
}
