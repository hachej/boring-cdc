//! Small typed boundary kernel and deterministic transition-test harness.
//!
//! This module deliberately contains no domain state machine. Domain owners implement
//! [`TransitionSystem`] on their production transition entry points and use [`Harness`]
//! to explore bounded schedules of those same functions.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::marker::PhantomData;

macro_rules! boundary_type {
    ($name:ident, $constructor:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(u64);

        impl $name {
            #[must_use]
            pub const fn $constructor(value: u64) -> Self {
                Self(value)
            }

            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }
        }
    };
}

boundary_type!(ReceivedLsn, from_wire);
boundary_type!(SlotCreationFloor, from_server);
boundary_type!(JournalCursor, from_store);
boundary_type!(CaptureEpoch, from_store);
boundary_type!(DestinationGeneration, from_store);

/// A source position is meaningful only within its capture epoch.
/// Ordering and canonical hashing remain owned by the M1 ordering domain.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct SourceVersion {
    capture_epoch: CaptureEpoch,
    commit_lsn: ReceivedLsn,
    transaction_id: u32,
    ordinal: u32,
}

impl SourceVersion {
    #[must_use]
    pub const fn from_decoded(
        capture_epoch: CaptureEpoch,
        commit_lsn: ReceivedLsn,
        transaction_id: u32,
        ordinal: u32,
    ) -> Self {
        Self {
            capture_epoch,
            commit_lsn,
            transaction_id,
            ordinal,
        }
    }

    #[must_use]
    pub const fn capture_epoch(self) -> CaptureEpoch {
        self.capture_epoch
    }

    #[must_use]
    pub const fn commit_lsn(self) -> ReceivedLsn {
        self.commit_lsn
    }

    #[must_use]
    pub const fn transaction_id(self) -> u32 {
        self.transaction_id
    }

    #[must_use]
    pub const fn ordinal(self) -> u32 {
        self.ordinal
    }
}

#[doc(hidden)]
pub mod evidence_kind {
    pub trait Sealed {}

    #[derive(Debug)]
    pub struct Production(());
    #[derive(Debug)]
    pub struct Synthetic(());

    impl Sealed for Production {}
    impl Sealed for Synthetic {}
}

/// Marker implemented only by kernel-owned evidence classes.
pub trait EvidenceKind: evidence_kind::Sealed {}
impl EvidenceKind for evidence_kind::Production {}
impl EvidenceKind for evidence_kind::Synthetic {}

/// Evidence that a complete source transaction and its cursor were validated together.
/// The evidence class prevents test-only evidence from entering production constructors.
#[derive(Debug)]
pub struct ValidatedCommit<K: EvidenceKind> {
    received_lsn: ReceivedLsn,
    journal_cursor: JournalCursor,
    _kind: PhantomData<K>,
}

impl<K: EvidenceKind> ValidatedCommit<K> {
    #[must_use]
    pub const fn received_lsn(&self) -> ReceivedLsn {
        self.received_lsn
    }

    #[must_use]
    pub const fn journal_cursor(&self) -> JournalCursor {
        self.journal_cursor
    }
}

/// Durable progress validated at a complete transaction/reconciliation boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct DurableSourceBoundary {
    commit_lsn: ReceivedLsn,
    journal_cursor: JournalCursor,
}

impl DurableSourceBoundary {
    /// There is intentionally no integer or received-LSN constructor.
    #[must_use]
    pub fn from_commit(evidence: ValidatedCommit<evidence_kind::Production>) -> Self {
        Self {
            commit_lsn: evidence.received_lsn,
            journal_cursor: evidence.journal_cursor,
        }
    }

    #[must_use]
    pub const fn commit_lsn(self) -> ReceivedLsn {
        self.commit_lsn
    }

    #[must_use]
    pub const fn journal_cursor(self) -> JournalCursor {
        self.journal_cursor
    }
}

/// Test-only stand-in for a future production validator owned inside this module.
/// Production persistence owners must add their checked construction path here rather than
/// receiving a crate-wide raw-value minting function.
#[cfg(test)]
fn validated_production_commit(
    received_lsn: ReceivedLsn,
    journal_cursor: JournalCursor,
) -> ValidatedCommit<evidence_kind::Production> {
    ValidatedCommit {
        received_lsn,
        journal_cursor,
        _kind: PhantomData,
    }
}

/// Unmistakably test-only evidence constructors.
pub mod synthetic {
    use super::{JournalCursor, PhantomData, ReceivedLsn, ValidatedCommit, evidence_kind};

    #[must_use]
    pub fn commit_evidence(
        received_lsn: ReceivedLsn,
        journal_cursor: JournalCursor,
    ) -> ValidatedCommit<evidence_kind::Synthetic> {
        ValidatedCommit {
            received_lsn,
            journal_cursor,
            _kind: PhantomData,
        }
    }
}

/// Typed production effect port. Domain adapters preserve their own effect/completion types.
pub trait EffectPort<Effect> {
    type Completion;
    type Error;

    fn dispatch(&mut self, effect: Effect) -> Result<Self::Completion, Self::Error>;
}

/// Injected time source. Ticks are domain-defined monotonic units.
pub trait Clock {
    fn now(&self) -> u64;
}

/// Injected randomness source. Production and test owners choose their implementation.
pub trait Randomness {
    fn next_u64(&mut self) -> u64;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VirtualClock {
    now: u64,
}

impl VirtualClock {
    #[must_use]
    pub const fn new(now: u64) -> Self {
        Self { now }
    }

    pub fn advance(&mut self, ticks: u64) -> Result<(), HarnessError> {
        self.now = self
            .now
            .checked_add(ticks)
            .ok_or(HarnessError::ClockOverflow)?;
        Ok(())
    }
}

impl Clock for VirtualClock {
    fn now(&self) -> u64 {
        self.now
    }
}

/// Small reproducible generator; its algorithm/version is part of serialized seed data.
#[derive(Clone, Debug)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }
}

impl Randomness for SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

pub struct TransitionContext<'a> {
    pub clock: &'a dyn Clock,
    pub randomness: &'a mut dyn Randomness,
}

/// A domain implementation delegates these methods to its production transition functions.
pub trait TransitionSystem {
    type Facts;
    type Event;
    type Effect;
    type Completion;

    fn on_event(
        &self,
        facts: &mut Self::Facts,
        event: Self::Event,
        context: &mut TransitionContext<'_>,
    ) -> Vec<Self::Effect>;

    fn on_completion(
        &self,
        facts: &mut Self::Facts,
        completion: Self::Completion,
        context: &mut TransitionContext<'_>,
    ) -> Vec<Self::Effect>;

    fn on_expiry(&self, facts: &mut Self::Facts, context: &mut TransitionContext<'_>);
    fn on_cancel(&self, facts: &mut Self::Facts, context: &mut TransitionContext<'_>);
    fn on_crash_restart(&self, facts: &mut Self::Facts, context: &mut TransitionContext<'_>);

    /// Returns a stable, redacted invariant fingerprint on failure.
    fn invariant_violation(&self, facts: &Self::Facts) -> Option<String>;

    /// Must contain only non-secret diagnostic state; the harness enforces its byte budget.
    fn redacted_state(&self, facts: &Self::Facts) -> String;

    /// Conservative owned-memory accounting supplied by the domain owner.
    fn facts_size_bytes(&self, facts: &Self::Facts) -> usize;

    /// Conservative scheduled-payload accounting supplied by the domain owner.
    fn event_size_bytes(&self, event: &Self::Event) -> usize;
    fn completion_size_bytes(&self, completion: &Self::Completion) -> usize;
}

#[derive(Clone, Debug)]
pub enum ScheduledAction<Event, Completion> {
    Event(Event),
    Completion(Completion),
    AdvanceClock(u64),
    Expire,
    Cancel,
    CrashRestart,
}

impl<Event, Completion> ScheduledAction<Event, Completion> {
    fn kind(&self) -> &'static str {
        match self {
            Self::Event(_) => "event",
            Self::Completion(_) => "completion",
            Self::AdvanceClock(_) => "advance_clock",
            Self::Expire => "expiry",
            Self::Cancel => "cancel",
            Self::CrashRestart => "crash_restart",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ScheduledStep<Event, Completion> {
    pub fixture_ref: String,
    pub action: ScheduledAction<Event, Completion>,
}

impl<Event, Completion> ScheduledStep<Event, Completion> {
    #[must_use]
    pub fn new(fixture_ref: impl Into<String>, action: ScheduledAction<Event, Completion>) -> Self {
        Self {
            fixture_ref: fixture_ref.into(),
            action,
        }
    }

    fn kind(&self) -> &'static str {
        self.action.kind()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HarnessBudget {
    pub max_steps: usize,
    pub max_trace_entries: usize,
    pub max_minimizer_runs: usize,
    pub max_redacted_bytes: usize,
    pub max_state_bytes: usize,
    pub max_scheduled_payload_bytes: usize,
}

impl HarnessBudget {
    pub const MAX_STEPS: usize = 4_096;
    pub const MAX_TRACE_ENTRIES: usize = 4_096;
    pub const MAX_MINIMIZER_RUNS: usize = 4_096;
    pub const MAX_REDACTED_BYTES: usize = 16_384;
    pub const MAX_STATE_BYTES: usize = 16 * 1024 * 1024;
    pub const MAX_SCHEDULED_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
    pub const MAX_FIXTURE_REF_BYTES: usize = 256;

    pub fn validate(self) -> Result<Self, HarnessError> {
        if self.max_steps == 0
            || self.max_trace_entries == 0
            || self.max_minimizer_runs == 0
            || self.max_redacted_bytes == 0
            || self.max_state_bytes == 0
            || self.max_scheduled_payload_bytes == 0
        {
            return Err(HarnessError::ZeroBudget);
        }
        if self.max_steps > Self::MAX_STEPS
            || self.max_trace_entries > Self::MAX_TRACE_ENTRIES
            || self.max_minimizer_runs > Self::MAX_MINIMIZER_RUNS
            || self.max_redacted_bytes > Self::MAX_REDACTED_BYTES
            || self.max_state_bytes > Self::MAX_STATE_BYTES
            || self.max_scheduled_payload_bytes > Self::MAX_SCHEDULED_PAYLOAD_BYTES
        {
            return Err(HarnessError::BudgetAboveKernelLimit);
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScheduleSeed {
    pub schema_version: String,
    pub algorithm: String,
    pub seed: u64,
    pub fair_scheduler_assumed: bool,
    pub restored_resources_assumed: bool,
}

impl ScheduleSeed {
    #[must_use]
    pub fn splitmix64(seed: u64) -> Self {
        Self {
            schema_version: "boring-cdc/schedule-seed/v1".into(),
            algorithm: "splitmix64-v1".into(),
            seed,
            fair_scheduler_assumed: false,
            restored_resources_assumed: false,
        }
    }

    pub fn validate(&self) -> Result<(), HarnessError> {
        if self.schema_version != "boring-cdc/schedule-seed/v1" || self.algorithm != "splitmix64-v1"
        {
            return Err(HarnessError::UnsupportedSeedContract);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TraceEntry {
    pub step: usize,
    pub fixture_ref: String,
    pub action: String,
    pub redacted_state: String,
    pub emitted_effects: usize,
    pub violation: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionTrace {
    pub schema_version: String,
    pub synthetic: bool,
    pub seed: ScheduleSeed,
    pub entries: Vec<TraceEntry>,
    pub violation: Option<String>,
    #[serde(skip)]
    violation_identity: Option<String>,
}

impl ExecutionTrace {
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HarnessError {
    ZeroBudget,
    BudgetAboveKernelLimit,
    StepBudgetExceeded { supplied: usize, maximum: usize },
    TraceBudgetExceeded,
    FixtureReferenceTooLarge,
    FixtureResolutionFailed,
    FixtureKindMismatch,
    UnsupportedSeedContract,
    UnsupportedFixtureContract,
    InvalidFixture,
    ScheduledPayloadBudgetExceeded,
    StateBudgetExceeded,
    DiagnosticBudgetExceeded,
    ClockOverflow,
    ViolationNotReproduced,
}

impl fmt::Display for HarnessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for HarnessError {}

pub struct MinimizedTrace<Event, Completion> {
    pub steps: Vec<ScheduledStep<Event, Completion>>,
    pub trace: ExecutionTrace,
    budget: HarnessBudget,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FixtureStep {
    pub kind: String,
    pub fixture_ref: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransitionFixture {
    pub schema_version: String,
    pub scenario_id: String,
    pub synthetic: bool,
    pub seed: ScheduleSeed,
    pub budget: HarnessBudget,
    pub steps: Vec<FixtureStep>,
    pub expected_violation: Option<String>,
}

impl TransitionFixture {
    pub fn validate(&self) -> Result<(), HarnessError> {
        if self.schema_version != "boring-cdc/transition-fixture/v1" || !self.synthetic {
            return Err(HarnessError::UnsupportedFixtureContract);
        }
        self.seed.validate()?;
        self.budget.validate()?;
        if !valid_scenario_id(&self.scenario_id)
            || self.steps.len() > self.budget.max_steps
            || self.steps.len() > HarnessBudget::MAX_STEPS
            || self
                .expected_violation
                .as_ref()
                .is_some_and(|value| value.len() > self.budget.max_redacted_bytes)
        {
            return Err(HarnessError::InvalidFixture);
        }
        for step in &self.steps {
            if step.fixture_ref.is_empty()
                || step.fixture_ref.len() > HarnessBudget::MAX_FIXTURE_REF_BYTES
                || !matches!(
                    step.kind.as_str(),
                    "event"
                        | "completion"
                        | "advance_clock"
                        | "expiry"
                        | "cancel"
                        | "crash_restart"
                )
            {
                return Err(HarnessError::InvalidFixture);
            }
        }
        Ok(())
    }

    /// Resolves redacted fixture references through the domain-owned fixture corpus.
    /// Payloads are intentionally not copied into the shared trace contract.
    pub fn resolve<Event, Completion, Resolve>(
        &self,
        mut resolve: Resolve,
    ) -> Result<Vec<ScheduledStep<Event, Completion>>, HarnessError>
    where
        Resolve: FnMut(&str) -> Option<ScheduledAction<Event, Completion>>,
    {
        self.validate()?;
        let mut resolved = Vec::with_capacity(self.steps.len());
        for step in &self.steps {
            let action = resolve(&step.fixture_ref).ok_or(HarnessError::FixtureResolutionFailed)?;
            if action.kind() != step.kind {
                return Err(HarnessError::FixtureKindMismatch);
            }
            resolved.push(ScheduledStep::new(step.fixture_ref.clone(), action));
        }
        Ok(resolved)
    }
}

impl<Event, Completion> MinimizedTrace<Event, Completion> {
    pub fn to_fixture(
        &self,
        scenario_id: impl Into<String>,
    ) -> Result<TransitionFixture, HarnessError> {
        let fixture = TransitionFixture {
            schema_version: "boring-cdc/transition-fixture/v1".into(),
            scenario_id: scenario_id.into(),
            synthetic: true,
            seed: self.trace.seed.clone(),
            budget: self.budget,
            steps: self
                .steps
                .iter()
                .map(|step| FixtureStep {
                    kind: step.kind().into(),
                    fixture_ref: step.fixture_ref.clone(),
                })
                .collect(),
            expected_violation: self.trace.violation.clone(),
        };
        fixture.validate()?;
        Ok(fixture)
    }
}

fn valid_scenario_id(value: &str) -> bool {
    value.starts_with("SCN-")
        && value.len() <= 128
        && value.len() > 4
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'-')
}

pub struct Harness {
    budget: HarnessBudget,
}

impl Harness {
    pub fn new(budget: HarnessBudget) -> Result<Self, HarnessError> {
        Ok(Self {
            budget: budget.validate()?,
        })
    }

    pub fn execute<D>(
        &self,
        domain: &D,
        initial_facts: D::Facts,
        steps: &[ScheduledStep<D::Event, D::Completion>],
        seed: ScheduleSeed,
    ) -> Result<ExecutionTrace, HarnessError>
    where
        D: TransitionSystem,
        D::Event: Clone,
        D::Completion: Clone,
    {
        seed.validate()?;
        if steps.len() > self.budget.max_steps {
            return Err(HarnessError::StepBudgetExceeded {
                supplied: steps.len(),
                maximum: self.budget.max_steps,
            });
        }
        let mut scheduled_bytes = 0usize;
        for step in steps {
            if step.fixture_ref.len() > HarnessBudget::MAX_FIXTURE_REF_BYTES {
                return Err(HarnessError::FixtureReferenceTooLarge);
            }
            let payload_bytes = match &step.action {
                ScheduledAction::Event(event) => domain.event_size_bytes(event),
                ScheduledAction::Completion(completion) => domain.completion_size_bytes(completion),
                ScheduledAction::AdvanceClock(_) => std::mem::size_of::<u64>(),
                ScheduledAction::Expire
                | ScheduledAction::Cancel
                | ScheduledAction::CrashRestart => 0,
            };
            scheduled_bytes = scheduled_bytes
                .checked_add(step.fixture_ref.len())
                .and_then(|value| value.checked_add(payload_bytes))
                .ok_or(HarnessError::ScheduledPayloadBudgetExceeded)?;
        }
        if scheduled_bytes > self.budget.max_scheduled_payload_bytes {
            return Err(HarnessError::ScheduledPayloadBudgetExceeded);
        }

        let mut facts = initial_facts;
        if domain.facts_size_bytes(&facts) > self.budget.max_state_bytes {
            return Err(HarnessError::StateBudgetExceeded);
        }
        let mut clock = VirtualClock::new(0);
        let mut scheduler_rng = SplitMix64::new(seed.seed);
        let mut domain_rng = SplitMix64::new(seed.seed ^ 0xd0a1_5eed_5eed_d0a1);
        let mut remaining = steps.to_vec();
        let mut entries = Vec::with_capacity(steps.len());
        let mut final_violation = None;

        while !remaining.is_empty() {
            if entries.len() == self.budget.max_trace_entries {
                return Err(HarnessError::TraceBudgetExceeded);
            }
            let index = (scheduler_rng.next_u64() as usize) % remaining.len();
            let scheduled = remaining.remove(index);
            let fixture_ref = scheduled.fixture_ref;
            let (action, effects) = {
                let mut context = TransitionContext {
                    clock: &clock,
                    randomness: &mut domain_rng,
                };
                match scheduled.action {
                    ScheduledAction::Event(event) => (
                        "event",
                        domain.on_event(&mut facts, event, &mut context).len(),
                    ),
                    ScheduledAction::Completion(completion) => (
                        "completion",
                        domain
                            .on_completion(&mut facts, completion, &mut context)
                            .len(),
                    ),
                    ScheduledAction::AdvanceClock(ticks) => {
                        clock.advance(ticks)?;
                        ("advance_clock", 0)
                    }
                    ScheduledAction::Expire => {
                        domain.on_expiry(&mut facts, &mut context);
                        ("expiry", 0)
                    }
                    ScheduledAction::Cancel => {
                        domain.on_cancel(&mut facts, &mut context);
                        ("cancel", 0)
                    }
                    ScheduledAction::CrashRestart => {
                        domain.on_crash_restart(&mut facts, &mut context);
                        ("crash_restart", 0)
                    }
                }
            };
            if domain.facts_size_bytes(&facts) > self.budget.max_state_bytes {
                return Err(HarnessError::StateBudgetExceeded);
            }
            let violation = domain.invariant_violation(&facts);
            let redacted_state = domain.redacted_state(&facts);
            if redacted_state.len() > self.budget.max_redacted_bytes
                || violation
                    .as_ref()
                    .is_some_and(|value| value.len() > self.budget.max_redacted_bytes)
            {
                return Err(HarnessError::DiagnosticBudgetExceeded);
            }
            entries.push(TraceEntry {
                step: entries.len(),
                fixture_ref,
                action: action.into(),
                redacted_state,
                emitted_effects: effects,
                violation: violation.clone(),
            });
            if violation.is_some() {
                final_violation = violation;
                break;
            }
        }

        Ok(ExecutionTrace {
            schema_version: "boring-cdc/execution-trace/v1".into(),
            synthetic: true,
            seed,
            entries,
            violation: final_violation.clone(),
            violation_identity: final_violation,
        })
    }

    /// Greedily removes steps while retaining the same stable violation fingerprint.
    pub fn minimize<D, F>(
        &self,
        steps: &[ScheduledStep<D::Event, D::Completion>],
        seed: &ScheduleSeed,
        mut initial_facts: F,
        domain: &D,
    ) -> Result<MinimizedTrace<D::Event, D::Completion>, HarnessError>
    where
        D: TransitionSystem,
        D::Event: Clone,
        D::Completion: Clone,
        F: FnMut() -> D::Facts,
    {
        let baseline = self.execute(domain, initial_facts(), steps, seed.clone())?;
        let expected = baseline
            .violation_identity
            .as_ref()
            .ok_or(HarnessError::ViolationNotReproduced)?
            .clone();
        let mut minimized = steps.to_vec();
        let mut runs = 0;
        let mut index = 0;

        while index < minimized.len() && runs < self.budget.max_minimizer_runs {
            let mut candidate = minimized.clone();
            candidate.remove(index);
            let trace = self.execute(domain, initial_facts(), &candidate, seed.clone())?;
            runs += 1;
            if trace.violation_identity.as_deref() == Some(expected.as_str()) {
                minimized = candidate;
            } else {
                index += 1;
            }
        }

        let trace = self.execute(domain, initial_facts(), &minimized, seed.clone())?;
        if trace.violation_identity.as_deref() != Some(expected.as_str()) {
            return Err(HarnessError::ViolationNotReproduced);
        }
        Ok(MinimizedTrace {
            steps: minimized,
            trace,
            budget: self.budget,
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn distinct_boundaries_round_trip_without_cross_construction() {
        let received = ReceivedLsn::from_wire(99);
        let cursor = JournalCursor::from_store(7);
        let durable =
            DurableSourceBoundary::from_commit(validated_production_commit(received, cursor));
        assert_eq!(durable.commit_lsn(), received);
        assert_eq!(durable.journal_cursor(), cursor);
        assert_eq!(SlotCreationFloor::from_server(99).get(), 99);
    }

    #[derive(Clone, Debug)]
    enum Event {
        Add(i64),
        BreakInvariant,
    }

    #[derive(Clone, Debug)]
    struct Completion(i64);

    #[derive(Clone, Debug, Default)]
    struct Facts {
        total: i64,
        broken: bool,
        restarts: usize,
    }

    struct SyntheticDomain;

    impl TransitionSystem for SyntheticDomain {
        type Facts = Facts;
        type Event = Event;
        type Effect = i64;
        type Completion = Completion;

        fn on_event(
            &self,
            facts: &mut Facts,
            event: Event,
            context: &mut TransitionContext<'_>,
        ) -> Vec<i64> {
            match event {
                Event::Add(value) => facts.total += value,
                Event::BreakInvariant => facts.broken = true,
            }
            vec![(context.randomness.next_u64() & 7) as i64]
        }

        fn on_completion(
            &self,
            facts: &mut Facts,
            completion: Completion,
            _context: &mut TransitionContext<'_>,
        ) -> Vec<i64> {
            facts.total += completion.0;
            Vec::new()
        }

        fn on_expiry(&self, facts: &mut Facts, context: &mut TransitionContext<'_>) {
            facts.total += context.clock.now() as i64;
        }

        fn on_cancel(&self, facts: &mut Facts, _context: &mut TransitionContext<'_>) {
            facts.total = 0;
        }

        fn on_crash_restart(&self, facts: &mut Facts, _context: &mut TransitionContext<'_>) {
            facts.restarts += 1;
        }

        fn invariant_violation(&self, facts: &Facts) -> Option<String> {
            facts.broken.then(|| "synthetic-invariant-v1".into())
        }

        fn redacted_state(&self, facts: &Facts) -> String {
            format!(
                "total={};broken={};restarts={}",
                facts.total, facts.broken, facts.restarts
            )
        }

        fn facts_size_bytes(&self, _facts: &Facts) -> usize {
            std::mem::size_of::<Facts>()
        }

        fn event_size_bytes(&self, _event: &Event) -> usize {
            std::mem::size_of::<Event>()
        }

        fn completion_size_bytes(&self, _completion: &Completion) -> usize {
            std::mem::size_of::<Completion>()
        }
    }

    fn budget() -> HarnessBudget {
        HarnessBudget {
            max_steps: 32,
            max_trace_entries: 32,
            max_minimizer_runs: 64,
            max_redacted_bytes: 128,
            max_state_bytes: 1_024,
            max_scheduled_payload_bytes: 1_024,
        }
    }

    fn scenario() -> Vec<ScheduledStep<Event, Completion>> {
        vec![
            ScheduledStep::new("event-add", ScheduledAction::Event(Event::Add(2))),
            ScheduledStep::new("completion-add", ScheduledAction::Completion(Completion(3))),
            ScheduledStep::new("clock-5", ScheduledAction::AdvanceClock(5)),
            ScheduledStep::new("expiry", ScheduledAction::Expire),
            ScheduledStep::new("restart", ScheduledAction::CrashRestart),
            ScheduledStep::new("break", ScheduledAction::Event(Event::BreakInvariant)),
            ScheduledStep::new("cancel", ScheduledAction::Cancel),
        ]
    }

    #[test]
    fn seeded_schedule_replays_byte_identical_trace() {
        let harness = Harness::new(budget()).unwrap();
        let seed = ScheduleSeed::splitmix64(0x5eed);
        let first = harness
            .execute(
                &SyntheticDomain,
                Facts::default(),
                &scenario(),
                seed.clone(),
            )
            .unwrap();
        let second = harness
            .execute(&SyntheticDomain, Facts::default(), &scenario(), seed)
            .unwrap();
        assert_eq!(first.to_json().unwrap(), second.to_json().unwrap());
        assert!(first.synthetic);
    }

    #[test]
    fn minimizer_preserves_violation_and_bounded_redacted_trace() {
        let harness = Harness::new(budget()).unwrap();
        let minimized = harness
            .minimize(
                &scenario(),
                &ScheduleSeed::splitmix64(17),
                Facts::default,
                &SyntheticDomain,
            )
            .unwrap();
        assert!(minimized.steps.len() < scenario().len());
        assert_eq!(
            minimized.trace.violation.as_deref(),
            Some("synthetic-invariant-v1")
        );
        assert!(
            minimized
                .trace
                .entries
                .iter()
                .all(|entry| entry.redacted_state.len() <= 128)
        );
        assert!(minimized.trace.to_json().unwrap().len() < 4096);
        let fixture = minimized.to_fixture("SCN-M1-INJECTED-VIOLATION").unwrap();
        let fixture_json = serde_json::to_string_pretty(&fixture).unwrap();
        assert!(fixture_json.contains("event-add") || fixture_json.contains("break"));
        assert!(fixture_json.len() < 4096);
        let restored: TransitionFixture = serde_json::from_str(&fixture_json).unwrap();
        let corpus = scenario();
        let resolved = restored
            .resolve(|reference| {
                corpus
                    .iter()
                    .find(|step| step.fixture_ref == reference)
                    .map(|step| step.action.clone())
            })
            .unwrap();
        let replay = harness
            .execute(
                &SyntheticDomain,
                Facts::default(),
                &resolved,
                restored.seed.clone(),
            )
            .unwrap();
        assert_eq!(replay.violation, restored.expected_violation);
    }

    #[test]
    fn published_fixture_deserializes_and_validates() {
        let fixture: TransitionFixture = serde_json::from_str(include_str!(
            "../tests/fixtures/m1-transition/valid/minimized.json"
        ))
        .unwrap();
        fixture.validate().unwrap();
        let resolved = fixture
            .resolve(|reference| match reference {
                "break" => Some(ScheduledAction::Event(Event::BreakInvariant)),
                _ => None,
            })
            .unwrap();
        let trace = Harness::new(fixture.budget)
            .unwrap()
            .execute(&SyntheticDomain, Facts::default(), &resolved, fixture.seed)
            .unwrap();
        assert_eq!(trace.violation, fixture.expected_violation);
    }

    #[test]
    fn unsupported_seed_and_fixture_contracts_fail_closed() {
        let harness = Harness::new(budget()).unwrap();
        let mut seed = ScheduleSeed::splitmix64(1);
        seed.algorithm = "unknown".into();
        assert_eq!(
            harness
                .execute(&SyntheticDomain, Facts::default(), &scenario(), seed)
                .unwrap_err(),
            HarnessError::UnsupportedSeedContract
        );

        let invalid = TransitionFixture {
            schema_version: "boring-cdc/transition-fixture/v1".into(),
            scenario_id: "invalid".into(),
            synthetic: true,
            seed: ScheduleSeed::splitmix64(1),
            budget: budget(),
            steps: vec![],
            expected_violation: None,
        };
        assert_eq!(invalid.validate(), Err(HarnessError::InvalidFixture));
    }

    #[test]
    fn state_payload_and_diagnostic_budgets_fail_closed() {
        let tiny_payload = Harness::new(HarnessBudget {
            max_scheduled_payload_bytes: 1,
            ..budget()
        })
        .unwrap();
        assert_eq!(
            tiny_payload
                .execute(
                    &SyntheticDomain,
                    Facts::default(),
                    &scenario(),
                    ScheduleSeed::splitmix64(1),
                )
                .unwrap_err(),
            HarnessError::ScheduledPayloadBudgetExceeded
        );

        let tiny_state = Harness::new(HarnessBudget {
            max_state_bytes: 1,
            ..budget()
        })
        .unwrap();
        assert_eq!(
            tiny_state
                .execute(
                    &SyntheticDomain,
                    Facts::default(),
                    &[],
                    ScheduleSeed::splitmix64(1),
                )
                .unwrap_err(),
            HarnessError::StateBudgetExceeded
        );
    }

    #[test]
    fn zero_and_step_budgets_fail_closed() {
        let invalid = HarnessBudget {
            max_steps: 0,
            ..budget()
        };
        assert_eq!(Harness::new(invalid).err(), Some(HarnessError::ZeroBudget));

        let harness = Harness::new(HarnessBudget {
            max_steps: 1,
            ..budget()
        })
        .unwrap();
        assert_eq!(
            harness
                .execute(
                    &SyntheticDomain,
                    Facts::default(),
                    &scenario(),
                    ScheduleSeed::splitmix64(1),
                )
                .unwrap_err(),
            HarnessError::StepBudgetExceeded {
                supplied: 7,
                maximum: 1,
            }
        );
    }
}
