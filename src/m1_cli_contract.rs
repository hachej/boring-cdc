//! Versioned, side-effect-free command and operator-envelope contract.
//!
//! Domain handlers consume this registry. This module deliberately performs no
//! source, destination, socket, or state-store I/O.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;

pub const CLI_SCHEMA_VERSION: u32 = 1;
pub const SNAPSHOT_SCHEMA_VERSION: u32 = 1;
pub const ACTION_PLAN_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandClass {
    ReadOnly,
    Mutation,
}
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Ownership {
    None,
    LiveOwner,
    OfflineEligible,
    Maintenance,
}
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Confirmation {
    None,
    DryRunThenConfirm,
    Confirm,
    ConfirmDataGap,
    ConfirmReplay,
    DryRunOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Grammar {
    Check,
    Init,
    Run,
    RunBootstrap,
    Status,
    BackfillStart,
    BackfillPause,
    BackfillResume,
    BackfillStatus,
    BackfillRestart,
    DestinationList,
    DestinationAdd,
    DestinationPause,
    DestinationResume,
    DestinationDetach,
    DestinationPromote,
    DestinationRetire,
    DestinationVerify,
    ArchiveReconstruct,
    ArchiveVerify,
    Replay,
    JournalInspect,
    JournalInspectExplain,
    JournalVerify,
    JournalGc,
    RecoverInspect,
    RecoverPromotion,
    RecoverReseed,
}

impl Grammar {
    pub fn usage(self) -> &'static str {
        match self {
            Grammar::Check => "check [--json]",
            Grammar::Init => "init [--dry-run|--confirm] --confirm-token TOKEN [--json]",
            Grammar::Run => "run",
            Grammar::RunBootstrap => "run --bootstrap",
            Grammar::Status => "status [--json]",
            Grammar::BackfillStart => {
                "backfill start [--dry-run|--confirm] --confirm-token TOKEN [--json]"
            }
            Grammar::BackfillPause => {
                "backfill pause [--dry-run|--confirm] --confirm-token TOKEN [--json]"
            }
            Grammar::BackfillResume => {
                "backfill resume [--dry-run|--confirm] --confirm-token TOKEN [--json]"
            }
            Grammar::BackfillStatus => "backfill status [--json]",
            Grammar::BackfillRestart => "backfill restart --confirm --confirm-token TOKEN [--json]",
            Grammar::DestinationList => "destination list [--json]",
            Grammar::DestinationAdd => {
                "destination add DESTINATION --archive-root PATH --continuity-break --from-seq SEQ (--dry-run|--confirm-data-gap) [--json]"
            }
            Grammar::DestinationPause => {
                "destination pause DESTINATION [--dry-run|--confirm] [--json]"
            }
            Grammar::DestinationResume => {
                "destination resume DESTINATION [--dry-run|--confirm] [--json]"
            }
            Grammar::DestinationDetach => {
                "destination detach DESTINATION --confirm --confirm-token TOKEN [--json]"
            }
            Grammar::DestinationPromote => {
                "destination promote DESTINATION --generation ID --confirm [--json]"
            }
            Grammar::DestinationRetire => {
                "destination retire DESTINATION --generation ID --confirm [--json]"
            }
            Grammar::DestinationVerify => {
                "destination verify DESTINATION [--from-seq SEQ] [--json]"
            }
            Grammar::ArchiveReconstruct => {
                "archive reconstruct DESTINATION --selector-fence FENCE --output PATH [--json]"
            }
            Grammar::ArchiveVerify => {
                "archive verify DESTINATION --selector-fence FENCE --oracle-manifest PATH [--json]"
            }
            Grammar::Replay => {
                "replay DESTINATION (--from-anchor ANCHOR|--from-seq SEQ|--since TIME) --new-generation ID (--dry-run|--confirm-replay) [--json]"
            }
            Grammar::JournalInspect => "journal inspect [--event-id ID] [--json]",
            Grammar::JournalInspectExplain => "journal inspect --event-id ID --explain [--json]",
            Grammar::JournalVerify => "journal verify [--json]",
            Grammar::JournalGc => "journal gc --dry-run [--json]",
            Grammar::RecoverInspect => "recover inspect [--json]",
            Grammar::RecoverPromotion => {
                "recover promotion DESTINATION --adopt-external-fence --confirm [--json]"
            }
            Grammar::RecoverReseed => {
                "recover reseed [--add-table SCHEMA.TABLE|--resume RESEED_ID] --recreate-publication --recreate-slot (--confirm-data-gap|--confirm) [--json]"
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct CommandSpec {
    pub id: &'static str,
    pub path: &'static [&'static str],
    pub operation_variant: &'static str,
    pub owner_bead: &'static str,
    pub class: CommandClass,
    pub ownership: Ownership,
    pub confirmation: Confirmation,
    pub json: bool,
    pub exit_codes: &'static [u8],
    pub runbook_ids: &'static [&'static str],
    pub redaction: &'static [&'static str],
    pub control_revisions: &'static [&'static str],
    pub predicates: &'static [&'static str],
    pub grammar: Grammar,
}

const ALL_CODES: &[u8] = &[0, 2, 3, 4, 5, 6];
const READ_CODES: &[u8] = &[0, 2, 4, 5, 6];
const REDACT: &[&str] = &[
    "confirm_token",
    "nonce",
    "dsn",
    "raw_path",
    "raw_driver_error",
];
const OWNER: &[&str] = &["ownership_revision", "run_id"];
const SOURCE: &[&str] = &[
    "source_identity",
    "capture_epoch",
    "configuration_fingerprint",
    "table_set_fingerprint",
];
const OWNER_SOURCE: &[&str] = &[
    "ownership_revision",
    "run_id",
    "source_identity",
    "capture_epoch",
    "configuration_fingerprint",
    "table_set_fingerprint",
];
const OWNER_DEST: &[&str] = &[
    "ownership_revision",
    "run_id",
    "destination_revision",
    "destination_generation",
    "selector_fence",
];
const OWNER_SOURCE_DEST: &[&str] = &[
    "ownership_revision",
    "run_id",
    "source_identity",
    "capture_epoch",
    "configuration_fingerprint",
    "table_set_fingerprint",
    "destination_revision",
    "destination_generation",
    "selector_fence",
];
const SAFE: &[&str] = &[
    "ownership_current",
    "plan_unexpired",
    "fingerprints_match",
    "resources_sufficient",
];

macro_rules! spec {
    ($id:literal,$path:expr,$variant:literal,$owner:literal,$class:ident,$ownership:ident,$confirmation:ident,$json:expr,$revs:expr,$pred:expr,$grammar:ident $(,)?) => {
        CommandSpec {
            id: $id,
            path: $path,
            operation_variant: $variant,
            owner_bead: $owner,
            class: CommandClass::$class,
            ownership: Ownership::$ownership,
            confirmation: Confirmation::$confirmation,
            json: $json,
            exit_codes: if matches!(CommandClass::$class, CommandClass::ReadOnly) {
                READ_CODES
            } else {
                ALL_CODES
            },
            runbook_ids: &["RB-OPERATOR-COMMAND"],
            redaction: REDACT,
            control_revisions: $revs,
            predicates: $pred,
            grammar: Grammar::$grammar,
        }
    };
}

/// Canonical registry. A path/variant pair appears exactly once.
pub static COMMANDS: &[CommandSpec] = &[
    spec!(
        "CMD-CHECK",
        &["check"],
        "base",
        "boring-cdc-m1-preflight",
        ReadOnly,
        None,
        None,
        true,
        &[],
        &[],
        Check,
    ),
    spec!(
        "CMD-INIT",
        &["init"],
        "base",
        "boring-cdc-m2-schema",
        Mutation,
        Maintenance,
        DryRunThenConfirm,
        true,
        SOURCE,
        SAFE,
        Init,
    ),
    spec!(
        "CMD-RUN",
        &["run"],
        "base",
        "boring-cdc-m2-capture-runtime",
        Mutation,
        LiveOwner,
        None,
        false,
        SOURCE,
        SAFE,
        Run,
    ),
    spec!(
        "CMD-RUN-BOOTSTRAP",
        &["run"],
        "bootstrap",
        "boring-cdc-m1-bootstrap-sm",
        Mutation,
        LiveOwner,
        None,
        false,
        SOURCE,
        SAFE,
        RunBootstrap,
    ),
    spec!(
        "CMD-STATUS",
        &["status"],
        "base",
        "boring-cdc-m2-fault-status",
        ReadOnly,
        None,
        None,
        true,
        &[],
        &[],
        Status,
    ),
    spec!(
        "CMD-BACKFILL-START",
        &["backfill", "start"],
        "base",
        "boring-cdc-m3-controls",
        Mutation,
        LiveOwner,
        DryRunThenConfirm,
        true,
        OWNER_SOURCE,
        SAFE,
        BackfillStart,
    ),
    spec!(
        "CMD-BACKFILL-PAUSE",
        &["backfill", "pause"],
        "base",
        "boring-cdc-m3-controls",
        Mutation,
        OfflineEligible,
        DryRunThenConfirm,
        true,
        OWNER,
        SAFE,
        BackfillPause,
    ),
    spec!(
        "CMD-BACKFILL-RESUME",
        &["backfill", "resume"],
        "base",
        "boring-cdc-m3-controls",
        Mutation,
        LiveOwner,
        DryRunThenConfirm,
        true,
        OWNER,
        SAFE,
        BackfillResume,
    ),
    spec!(
        "CMD-BACKFILL-STATUS",
        &["backfill", "status"],
        "base",
        "boring-cdc-m3-controls",
        ReadOnly,
        None,
        None,
        true,
        &[],
        &[],
        BackfillStatus,
    ),
    spec!(
        "CMD-BACKFILL-RESTART",
        &["backfill", "restart"],
        "base",
        "boring-cdc-m3-controls",
        Mutation,
        LiveOwner,
        Confirm,
        true,
        OWNER_SOURCE,
        SAFE,
        BackfillRestart,
    ),
    spec!(
        "CMD-DESTINATION-LIST",
        &["destination", "list"],
        "base",
        "boring-cdc-m2-reconcile",
        ReadOnly,
        None,
        None,
        true,
        &[],
        &[],
        DestinationList,
    ),
    spec!(
        "CMD-DESTINATION-ADD",
        &["destination", "add"],
        "base",
        "boring-cdc-m5-loops",
        Mutation,
        LiveOwner,
        ConfirmDataGap,
        true,
        OWNER_SOURCE,
        SAFE,
        DestinationAdd,
    ),
    spec!(
        "CMD-DESTINATION-PAUSE",
        &["destination", "pause"],
        "base",
        "boring-cdc-m2-reconcile",
        Mutation,
        OfflineEligible,
        DryRunThenConfirm,
        true,
        OWNER,
        SAFE,
        DestinationPause,
    ),
    spec!(
        "CMD-DESTINATION-RESUME",
        &["destination", "resume"],
        "base",
        "boring-cdc-m2-reconcile",
        Mutation,
        LiveOwner,
        DryRunThenConfirm,
        true,
        OWNER,
        SAFE,
        DestinationResume,
    ),
    spec!(
        "CMD-DESTINATION-DETACH",
        &["destination", "detach"],
        "base",
        "boring-cdc-m2-reconcile",
        Mutation,
        OfflineEligible,
        Confirm,
        true,
        OWNER_DEST,
        SAFE,
        DestinationDetach,
    ),
    spec!(
        "CMD-DESTINATION-PROMOTE",
        &["destination", "promote"],
        "base",
        "boring-cdc-m4-promotion",
        Mutation,
        LiveOwner,
        Confirm,
        true,
        OWNER_DEST,
        SAFE,
        DestinationPromote,
    ),
    spec!(
        "CMD-DESTINATION-RETIRE",
        &["destination", "retire"],
        "base",
        "boring-cdc-m4-promotion",
        Mutation,
        Maintenance,
        Confirm,
        true,
        OWNER_DEST,
        SAFE,
        DestinationRetire,
    ),
    spec!(
        "CMD-DESTINATION-VERIFY",
        &["destination", "verify"],
        "base",
        "boring-cdc-m4-durability",
        ReadOnly,
        None,
        None,
        true,
        &[],
        &[],
        DestinationVerify,
    ),
    spec!(
        "CMD-ARCHIVE-RECONSTRUCT",
        &["archive", "reconstruct"],
        "base",
        "boring-cdc-m5-segment-set",
        ReadOnly,
        None,
        None,
        true,
        &[],
        &[],
        ArchiveReconstruct,
    ),
    spec!(
        "CMD-ARCHIVE-VERIFY",
        &["archive", "verify"],
        "base",
        "boring-cdc-m5-segment-set",
        ReadOnly,
        None,
        None,
        true,
        &[],
        &[],
        ArchiveVerify,
    ),
    spec!(
        "CMD-REPLAY",
        &["replay"],
        "base",
        "boring-cdc-m5-ops-cli",
        Mutation,
        LiveOwner,
        ConfirmReplay,
        true,
        OWNER_SOURCE_DEST,
        SAFE,
        Replay,
    ),
    spec!(
        "CMD-JOURNAL-INSPECT",
        &["journal", "inspect"],
        "base",
        "boring-cdc-m2-reconcile",
        ReadOnly,
        None,
        None,
        true,
        &[],
        &[],
        JournalInspect,
    ),
    spec!(
        "CMD-JOURNAL-INSPECT-EXPLAIN",
        &["journal", "inspect"],
        "explain",
        "boring-cdc-m5.1",
        ReadOnly,
        None,
        None,
        true,
        &[],
        &[],
        JournalInspectExplain,
    ),
    spec!(
        "CMD-JOURNAL-VERIFY",
        &["journal", "verify"],
        "base",
        "boring-cdc-m2-reconcile",
        ReadOnly,
        None,
        None,
        true,
        &[],
        &[],
        JournalVerify,
    ),
    spec!(
        "CMD-JOURNAL-GC",
        &["journal", "gc"],
        "base",
        "boring-cdc-m5-gc",
        Mutation,
        LiveOwner,
        DryRunOnly,
        true,
        OWNER_SOURCE,
        SAFE,
        JournalGc,
    ),
    spec!(
        "CMD-RECOVER-INSPECT",
        &["recover", "inspect"],
        "base",
        "boring-cdc-m2-init-recovery",
        ReadOnly,
        None,
        None,
        true,
        &[],
        &[],
        RecoverInspect,
    ),
    spec!(
        "CMD-RECOVER-PROMOTION",
        &["recover", "promotion"],
        "base",
        "boring-cdc-m2-init-recovery",
        Mutation,
        Maintenance,
        Confirm,
        true,
        OWNER_DEST,
        SAFE,
        RecoverPromotion,
    ),
    spec!(
        "CMD-RECOVER-RESEED",
        &["recover", "reseed"],
        "base",
        "boring-cdc-m2-init-recovery",
        Mutation,
        Maintenance,
        ConfirmDataGap,
        true,
        OWNER_SOURCE,
        SAFE,
        RecoverReseed,
    ),
];

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Condition {
    pub code: String,
    pub severity: String,
    pub runbook_id: String,
    pub evidence_digest: Option<String>,
}
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct NextCommand {
    pub command_id: String,
    pub argv: Vec<String>,
}

/// A projection of domain-owned facts, never an independent state machine.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SystemSnapshot {
    pub schema_version: u32,
    pub snapshot_id: String,
    pub state_revision: u64,
    pub observed_at: String,
    pub fresh_until: String,
    pub freshness: String,
    pub source_identity: Option<String>,
    pub capture_epoch: Option<String>,
    pub run_id: Option<String>,
    pub ownership: Value,
    pub fingerprints: BTreeMap<String, String>,
    pub durability_boundaries: Value,
    pub budgets: Value,
    pub destinations: Vec<Value>,
    pub conditions: Vec<Condition>,
    pub blocked_by: Vec<String>,
    pub allowed_actions: Vec<String>,
    pub next_commands: Vec<NextCommand>,
    pub evidence_digest: String,
    #[serde(flatten)]
    pub extensions: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BoundRevision {
    pub name: String,
    pub value: String,
    pub owner_bead: String,
}
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ConfirmationPolicy {
    pub kind: Confirmation,
    pub expires_at: Option<String>,
    pub canonical_argv: Vec<String>,
    /// Present only in the direct JSON dry-run response. Callers must pass the
    /// envelope through `redact_json` before logging or artifact persistence.
    pub confirm_token: Option<String>,
}

/// Immutable inspect-plan-apply contract. Volatile observation progress is not
/// part of `bound_control_revisions`; current resource facts are predicates.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ActionPlan {
    pub schema_version: u32,
    pub plan_id: String,
    pub plan_digest: String,
    pub request_id: String,
    pub canonical_payload_digest: String,
    pub command_id: String,
    pub observed_snapshot_id: String,
    pub observed_state_revision: u64,
    pub bound_control_revisions: Vec<BoundRevision>,
    pub bound_fingerprints: BTreeMap<String, String>,
    pub preconditions: Vec<String>,
    pub current_predicates: Vec<String>,
    pub intended_transitions: Vec<String>,
    pub immutable_intents: Vec<String>,
    pub external_effects: Vec<String>,
    pub resource_consequences: Vec<String>,
    pub continuity_consequences: Vec<String>,
    pub affected_objects: Vec<String>,
    pub allowed_remaining_actions: Vec<String>,
    pub rollback_boundary: String,
    pub expected_postconditions: Vec<String>,
    pub evidence_requirements: Vec<String>,
    pub confirmation: ConfirmationPolicy,
    pub expires_at: String,
    #[serde(flatten)]
    pub extensions: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfirmationAttempt {
    pub confirm_token: String,
    pub request_id: String,
    pub canonical_payload_digest: String,
    pub plan_digest: String,
}

impl ActionPlan {
    pub fn authorize_confirmation(
        &self,
        attempt: &ConfirmationAttempt,
        current: &BTreeMap<String, String>,
        fingerprints: &BTreeMap<String, String>,
        now: &str,
        predicates_hold: bool,
        verify_opaque_token: impl FnOnce(&ConfirmationAttempt, &ActionPlan) -> bool,
    ) -> bool {
        !attempt.confirm_token.is_empty()
            && attempt.request_id == self.request_id
            && attempt.canonical_payload_digest == self.canonical_payload_digest
            && attempt.plan_digest == self.plan_digest
            && self.authorization_is_current(current, fingerprints, now, predicates_hold)
            && verify_opaque_token(attempt, self)
    }

    pub fn authorization_is_current(
        &self,
        current: &BTreeMap<String, String>,
        fingerprints: &BTreeMap<String, String>,
        now: &str,
        predicates_hold: bool,
    ) -> bool {
        let confirmation_expiry = self.confirmation.expires_at.as_deref();
        predicates_hold
            && canonical_utc_timestamp(now)
            && canonical_utc_timestamp(&self.expires_at)
            && now <= self.expires_at.as_str()
            && confirmation_expiry
                .is_none_or(|expiry| canonical_utc_timestamp(expiry) && now <= expiry)
            && self
                .bound_control_revisions
                .iter()
                .all(|r| current.get(&r.name) == Some(&r.value))
            && self
                .bound_fingerprints
                .iter()
                .all(|(name, value)| fingerprints.get(name) == Some(value))
    }
}

/// Authorization timestamps use one canonical UTC form so byte ordering is
/// chronological and alternate-offset spellings cannot bypass expiry.
fn canonical_utc_timestamp(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'Z'
        || !bytes
            .iter()
            .enumerate()
            .all(|(i, byte)| matches!(i, 4 | 7 | 10 | 13 | 16 | 19) || byte.is_ascii_digit())
    {
        return false;
    }
    let number = |range: std::ops::Range<usize>| value[range].parse::<u32>().ok();
    let (Some(year), Some(month), Some(day), Some(hour), Some(minute), Some(second)) = (
        number(0..4),
        number(5..7),
        number(8..10),
        number(11..13),
        number(14..16),
        number(17..19),
    ) else {
        return false;
    };
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => 0,
    };
    day >= 1 && day <= days && hour <= 23 && minute <= 59 && second <= 59
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationTerminal {
    AfterSnapshot { after_snapshot_id: String },
    SafeTerminal { safe_terminal_state: String },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MutationTrace {
    pub before_snapshot_id: String,
    pub plan_digest: String,
    pub immutable_intent_id: String,
    pub external_effect_evidence_digest: Option<String>,
    pub terminal: MutationTerminal,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ForecastResult {
    pub observed_snapshot_id: String,
    pub profile_id: String,
    pub recoverable_until: Option<String>,
    pub assumptions: Vec<String>,
    pub evidence_digest: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AttestationResult {
    pub subject_id: String,
    pub status: String,
    pub facts: BTreeMap<String, String>,
    pub evidence_digest: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EventExplanation {
    pub event_id: String,
    pub locally_durable: String,
    pub destination_checkpoint_coverage: String,
    pub current_audit_coverage: String,
    pub baseline_eligibility: String,
    pub candidate_eligibility: String,
    pub live_generation_selection: String,
    pub unavailable_evidence: Vec<String>,
    pub reason_codes: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CliEnvelope {
    pub schema_version: u32,
    pub command: String,
    pub outcome: String,
    pub code: String,
    pub message: String,
    pub request_id: Option<String>,
    pub run_id: Option<String>,
    pub capture_epoch: Option<String>,
    pub condition: Option<String>,
    pub runbook_id: Option<String>,
    pub data: Value,
    pub warnings: Vec<String>,
    pub next_commands: Vec<NextCommand>,
    pub plan_digest: Option<String>,
    pub postcondition_evidence_digest: Option<String>,
    pub mutation_trace: Option<MutationTrace>,
}

impl CliEnvelope {
    pub fn validate_for(&self, spec: &CommandSpec) -> bool {
        if self.schema_version != CLI_SCHEMA_VERSION || self.command != spec.id {
            return false;
        }
        if spec.class == CommandClass::Mutation && self.outcome == "success" {
            let Some(trace) = &self.mutation_trace else {
                return false;
            };
            self.plan_digest.as_deref() == Some(trace.plan_digest.as_str())
                && self.postcondition_evidence_digest.is_some()
                && !trace.before_snapshot_id.is_empty()
                && !trace.immutable_intent_id.is_empty()
        } else {
            true
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ExitCode {
    Success = 0,
    Invalid = 2,
    SafetyBlocked = 3,
    Unavailable = 4,
    Integrity = 5,
    Invariant = 6,
}

#[derive(Debug, Eq, PartialEq)]
pub struct CliError {
    pub code: &'static str,
    pub message: &'static str,
    pub exit: ExitCode,
}
impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for CliError {}

#[derive(Debug)]
pub struct ParsedCommand {
    pub spec: &'static CommandSpec,
    pub argv: Vec<String>,
    pub json: bool,
    pub help: bool,
}

pub fn parse(argv: &[String]) -> Result<ParsedCommand, CliError> {
    if argv.is_empty() {
        return Err(invalid("CLI_COMMAND_REQUIRED", "a command is required"));
    }
    if argv[0] == "-h" || argv[0] == "--help" {
        return Err(invalid("CLI_ROOT_HELP", "root help requested"));
    }
    let explain = argv.iter().any(|a| a == "--explain");
    let bootstrap = argv.iter().any(|a| a == "--bootstrap");
    let mut candidates: Vec<_> = COMMANDS
        .iter()
        .filter(|s| argv.len() >= s.path.len() && s.path.iter().zip(argv).all(|(a, b)| *a == b))
        .collect();
    candidates.retain(|s| match s.operation_variant {
        "explain" => explain,
        "bootstrap" => bootstrap,
        "base" => !(explain && s.path == ["journal", "inspect"] || bootstrap && s.path == ["run"]),
        _ => true,
    });
    let spec = candidates
        .into_iter()
        .max_by_key(|s| s.path.len())
        .ok_or_else(|| invalid("CLI_UNKNOWN_COMMAND", "unknown command"))?;
    let json = argv.iter().any(|a| a == "--json");
    let help = argv.iter().any(|a| a == "-h" || a == "--help");
    if json && !spec.json {
        return Err(invalid(
            "CLI_JSON_UNSUPPORTED",
            "--json is not supported for this command",
        ));
    }
    if !help {
        spec.grammar.validate(spec, argv)?;
    }
    Ok(ParsedCommand {
        spec,
        argv: argv.to_vec(),
        json,
        help,
    })
}

fn invalid(code: &'static str, message: &'static str) -> CliError {
    CliError {
        code,
        message,
        exit: ExitCode::Invalid,
    }
}
fn has(argv: &[String], flag: &str) -> bool {
    argv.iter().any(|v| v == flag)
}
fn value_after(argv: &[String], flag: &str) -> bool {
    argv.iter()
        .position(|v| v == flag)
        .is_some_and(|i| argv.get(i + 1).is_some_and(|v| !v.starts_with('-')))
}
fn positional_after_path(spec: &CommandSpec, argv: &[String]) -> bool {
    argv.get(spec.path.len())
        .is_some_and(|v| !v.starts_with('-'))
}
fn exactly_one(argv: &[String], flags: &[&str]) -> bool {
    flags.iter().filter(|f| has(argv, f)).count() == 1
}
fn confirmed(argv: &[String], flag: &str) -> bool {
    has(argv, flag) && value_after(argv, "--confirm-token")
}
fn dry_or_confirmed(argv: &[String], dry: &str, confirm: &str) -> bool {
    exactly_one(argv, &[dry, confirm])
        && if has(argv, confirm) {
            value_after(argv, "--confirm-token")
        } else {
            !has(argv, "--confirm-token")
        }
}

impl Grammar {
    fn validate_token_shape(self, spec: &CommandSpec, argv: &[String]) -> Result<(), CliError> {
        let (allowed, positional): (&[&str], usize) = match self {
            Grammar::Check
            | Grammar::Status
            | Grammar::BackfillStatus
            | Grammar::DestinationList
            | Grammar::JournalVerify
            | Grammar::RecoverInspect => (&["--json"], 0),
            Grammar::Run => (&[], 0),
            Grammar::RunBootstrap => (&["--bootstrap"], 0),
            Grammar::Init
            | Grammar::BackfillStart
            | Grammar::BackfillPause
            | Grammar::BackfillResume => {
                (&["--dry-run", "--confirm", "--confirm-token", "--json"], 0)
            }
            Grammar::BackfillRestart => (&["--confirm", "--confirm-token", "--json"], 0),
            Grammar::DestinationAdd => (
                &[
                    "--archive-root",
                    "--continuity-break",
                    "--from-seq",
                    "--dry-run",
                    "--confirm-data-gap",
                    "--confirm-token",
                    "--json",
                ],
                1,
            ),
            Grammar::DestinationPause | Grammar::DestinationResume => {
                (&["--dry-run", "--confirm", "--confirm-token", "--json"], 1)
            }
            Grammar::DestinationDetach => (&["--confirm", "--confirm-token", "--json"], 1),
            Grammar::DestinationPromote | Grammar::DestinationRetire => (
                &["--generation", "--confirm", "--confirm-token", "--json"],
                1,
            ),
            Grammar::DestinationVerify => (&["--from-seq", "--json"], 1),
            Grammar::ArchiveReconstruct => (&["--selector-fence", "--output", "--json"], 1),
            Grammar::ArchiveVerify => (&["--selector-fence", "--oracle-manifest", "--json"], 1),
            Grammar::Replay => (
                &[
                    "--from-anchor",
                    "--from-seq",
                    "--since",
                    "--new-generation",
                    "--dry-run",
                    "--confirm-replay",
                    "--confirm-token",
                    "--json",
                ],
                1,
            ),
            Grammar::JournalInspect => (&["--event-id", "--json"], 0),
            Grammar::JournalInspectExplain => (&["--event-id", "--explain", "--json"], 0),
            Grammar::JournalGc => (&["--dry-run", "--json"], 0),
            Grammar::RecoverPromotion => (
                &[
                    "--adopt-external-fence",
                    "--confirm",
                    "--confirm-token",
                    "--json",
                ],
                1,
            ),
            Grammar::RecoverReseed => (
                &[
                    "--add-table",
                    "--resume",
                    "--recreate-publication",
                    "--recreate-slot",
                    "--confirm-data-gap",
                    "--confirm",
                    "--confirm-token",
                    "--json",
                ],
                0,
            ),
        };
        let valued = [
            "--archive-root",
            "--from-seq",
            "--generation",
            "--selector-fence",
            "--output",
            "--oracle-manifest",
            "--from-anchor",
            "--since",
            "--new-generation",
            "--event-id",
            "--add-table",
            "--resume",
            "--confirm-token",
        ];
        let mut positionals = 0;
        let mut seen = std::collections::BTreeSet::new();
        let mut i = spec.path.len();
        while i < argv.len() {
            let token = &argv[i];
            if token.starts_with('-') {
                if !allowed.contains(&token.as_str()) {
                    return Err(invalid("CLI_UNKNOWN_ARGUMENT", "unknown argument"));
                }
                if !seen.insert(token.as_str()) {
                    return Err(invalid(
                        "CLI_DUPLICATE_ARGUMENT",
                        "argument supplied more than once",
                    ));
                }
                if valued.contains(&token.as_str()) {
                    if argv.get(i + 1).is_none_or(|v| v.starts_with('-')) {
                        return Err(invalid(
                            "CLI_ARGUMENT_VALUE_REQUIRED",
                            "argument value required",
                        ));
                    }
                    i += 1;
                }
            } else {
                positionals += 1;
            }
            i += 1;
        }
        if positionals != positional {
            return Err(invalid(
                "CLI_INVALID_INVOCATION",
                "wrong number of positional arguments",
            ));
        }
        Ok(())
    }

    fn validate(self, spec: &CommandSpec, a: &[String]) -> Result<(), CliError> {
        self.validate_token_shape(spec, a)?;
        let ok = match self {
            Grammar::Check
            | Grammar::Status
            | Grammar::BackfillStatus
            | Grammar::DestinationList
            | Grammar::JournalVerify
            | Grammar::RecoverInspect => true,
            Grammar::Run => !has(a, "--bootstrap"),
            Grammar::RunBootstrap => has(a, "--bootstrap"),
            Grammar::Init
            | Grammar::BackfillStart
            | Grammar::BackfillPause
            | Grammar::BackfillResume => dry_or_confirmed(a, "--dry-run", "--confirm"),
            Grammar::BackfillRestart => confirmed(a, "--confirm"),
            Grammar::DestinationAdd => {
                positional_after_path(spec, a)
                    && value_after(a, "--archive-root")
                    && has(a, "--continuity-break")
                    && value_after(a, "--from-seq")
                    && dry_or_confirmed(a, "--dry-run", "--confirm-data-gap")
            }
            Grammar::DestinationPause | Grammar::DestinationResume => {
                positional_after_path(spec, a) && dry_or_confirmed(a, "--dry-run", "--confirm")
            }
            Grammar::DestinationDetach => {
                positional_after_path(spec, a) && confirmed(a, "--confirm")
            }
            Grammar::DestinationPromote | Grammar::DestinationRetire => {
                positional_after_path(spec, a)
                    && value_after(a, "--generation")
                    && confirmed(a, "--confirm")
            }
            Grammar::DestinationVerify => {
                positional_after_path(spec, a)
                    && (!has(a, "--from-seq") || value_after(a, "--from-seq"))
            }
            Grammar::ArchiveReconstruct => {
                positional_after_path(spec, a)
                    && value_after(a, "--selector-fence")
                    && value_after(a, "--output")
            }
            Grammar::ArchiveVerify => {
                positional_after_path(spec, a)
                    && value_after(a, "--selector-fence")
                    && value_after(a, "--oracle-manifest")
            }
            Grammar::Replay => {
                positional_after_path(spec, a)
                    && exactly_one(a, &["--from-anchor", "--from-seq", "--since"])
                    && ["--from-anchor", "--from-seq", "--since"]
                        .iter()
                        .all(|f| !has(a, f) || value_after(a, f))
                    && value_after(a, "--new-generation")
                    && dry_or_confirmed(a, "--dry-run", "--confirm-replay")
            }
            Grammar::JournalInspect => {
                !has(a, "--explain") && (!has(a, "--event-id") || value_after(a, "--event-id"))
            }
            Grammar::JournalInspectExplain => has(a, "--explain") && value_after(a, "--event-id"),
            Grammar::JournalGc => has(a, "--dry-run"),
            Grammar::RecoverPromotion => {
                positional_after_path(spec, a)
                    && has(a, "--adopt-external-fence")
                    && confirmed(a, "--confirm")
            }
            Grammar::RecoverReseed => {
                if has(a, "--resume") {
                    value_after(a, "--resume")
                        && confirmed(a, "--confirm")
                        && !has(a, "--confirm-data-gap")
                        && !has(a, "--add-table")
                        && !has(a, "--recreate-publication")
                        && !has(a, "--recreate-slot")
                } else {
                    (!has(a, "--add-table") || value_after(a, "--add-table"))
                        && !has(a, "--confirm")
                        && has(a, "--recreate-publication")
                        && has(a, "--recreate-slot")
                        && confirmed(a, "--confirm-data-gap")
                }
            }
        };
        if ok {
            Ok(())
        } else {
            Err(invalid(
                "CLI_INVALID_INVOCATION",
                "arguments do not match the command contract",
            ))
        }
    }
}

pub fn root_help() -> String {
    let mut out = String::from("boring-cdc\n\nCommands:\n");
    for s in COMMANDS {
        out.push_str(&format!("  {:<30} {}\n", s.id, s.grammar.usage()));
    }
    out.push_str("\nExit codes: 0 success; 2 invalid; 3 safety blocked; 4 unavailable; 5 integrity; 6 invariant.\n");
    out
}
pub fn command_help(spec: &CommandSpec) -> String {
    format!(
        "{}\nowner: {}\noperation: {}\n",
        spec.grammar.usage(),
        spec.owner_bead,
        spec.operation_variant
    )
}

pub fn shell_escape(value: &str) -> String {
    if !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_@%+=:,./-".contains(&b))
    {
        value.into()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}
pub fn confirmation_command(argv: &[String]) -> String {
    std::iter::once("boring-cdc".to_string())
        .chain(argv.iter().map(|v| shell_escape(v)))
        .collect::<Vec<_>>()
        .join(" ")
}
pub fn redact_json(value: &mut Value) {
    match value {
        Value::Object(m) => {
            for (k, v) in m {
                if k == "canonical_argv" || k == "argv" {
                    *v = serde_json::json!(["[REDACTED:argv]"]);
                } else if REDACT.iter().any(|r| k.contains(r)) {
                    *v = Value::String("[REDACTED]".into())
                } else {
                    redact_json(v)
                }
            }
        }
        Value::Array(a) => {
            for v in a {
                redact_json(v)
            }
        }
        _ => {}
    }
}
pub fn digest(value: &impl Serialize) -> String {
    let bytes = serde_json::to_vec(value).expect("serializable CLI contract");
    format!("{:x}", Sha256::digest(bytes))
}

pub fn error_envelope(error: &CliError) -> CliEnvelope {
    CliEnvelope {
        schema_version: CLI_SCHEMA_VERSION,
        command: "CLI".into(),
        outcome: "error".into(),
        code: error.code.into(),
        message: error.message.into(),
        request_id: None,
        run_id: None,
        capture_epoch: None,
        condition: Some("invocation_invalid".into()),
        runbook_id: Some("RB-OPERATOR-COMMAND".into()),
        data: Value::Object(Default::default()),
        warnings: vec![],
        next_commands: vec![],
        plan_digest: None,
        postcondition_evidence_digest: None,
        mutation_trace: None,
    }
}

pub fn unavailable(spec: &CommandSpec) -> CliEnvelope {
    CliEnvelope {
        schema_version: CLI_SCHEMA_VERSION,
        command: spec.id.into(),
        outcome: "error".into(),
        code: "CLI_HANDLER_UNAVAILABLE".into(),
        message: "command handler is not implemented in this milestone".into(),
        request_id: None,
        run_id: None,
        capture_epoch: None,
        condition: Some("dependency_unavailable".into()),
        runbook_id: Some("RB-OPERATOR-COMMAND".into()),
        data: Value::Object(Default::default()),
        warnings: vec![],
        next_commands: vec![],
        plan_digest: None,
        postcondition_evidence_digest: None,
        mutation_trace: None,
    }
}

/// Synthetic compatibility vectors freeze shared shapes without claiming later
/// domain integration or measured evidence.
pub fn compatibility_fixture() -> Value {
    serde_json::json!({
      "schema_version": 1,
      "fact_freshness": ["fresh", "stale", "unknown"],
      "concurrent_conditions": [
        {"code":"journal_pressure","severity":"warning","runbook_id":"RB-JOURNAL-PRESSURE","evidence_digest":"e1"},
        {"code":"destination_audit_stale","severity":"blocked","runbook_id":"RB-AUDIT-STALE","evidence_digest":null}
      ],
      "ownership_classes": ["none","live_owner","offline_eligible","maintenance"],
      "terminal_traces": [
        MutationTrace { before_snapshot_id:"snapshot-before".into(), plan_digest:"plan-digest".into(), immutable_intent_id:"intent-1".into(), external_effect_evidence_digest:Some("effect-digest".into()), terminal:MutationTerminal::AfterSnapshot { after_snapshot_id:"snapshot-after".into() } },
        MutationTrace { before_snapshot_id:"snapshot-before".into(), plan_digest:"plan-digest-safe".into(), immutable_intent_id:"intent-2".into(), external_effect_evidence_digest:None, terminal:MutationTerminal::SafeTerminal { safe_terminal_state:"aborted_by_restart".into() } }
      ],
      "authorization_matrix": ["unchanged_relevant_accept", "unrelated_progress_accept", "changed_revision_reject", "changed_fingerprint_reject", "expired_reject", "invalid_predicate_reject"],
      "forecast": ForecastResult { observed_snapshot_id:"snapshot-before".into(), profile_id:"synthetic-profile".into(), recoverable_until:None, assumptions:vec!["synthetic_only".into()], evidence_digest:"forecast-digest".into() },
      "attestation": AttestationResult { subject_id:"destination-1".into(), status:"unknown".into(), facts:BTreeMap::from([("audit_coverage".into(),"unavailable".into())]), evidence_digest:"attestation-digest".into() },
      "event_explanation": EventExplanation { event_id:"event-1".into(), locally_durable:"true".into(), destination_checkpoint_coverage:"covered".into(), current_audit_coverage:"unknown".into(), baseline_eligibility:"eligible".into(), candidate_eligibility:"unknown".into(), live_generation_selection:"selected".into(), unavailable_evidence:vec!["audit_gc_expired".into()], reason_codes:vec!["EXPLAIN_AUDIT_UNAVAILABLE".into()] }
    })
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::collections::BTreeSet;
    fn args(s: &str) -> Vec<String> {
        s.split_whitespace().map(Into::into).collect()
    }
    #[test]
    fn registry_ids_paths_and_variants_are_unique_and_complete() {
        let ids: BTreeSet<_> = COMMANDS.iter().map(|s| s.id).collect();
        let keys: BTreeSet<_> = COMMANDS
            .iter()
            .map(|s| (s.path, s.operation_variant))
            .collect();
        assert_eq!(ids.len(), COMMANDS.len());
        assert_eq!(keys.len(), COMMANDS.len());
        assert_eq!(COMMANDS.len(), 28);
        for s in COMMANDS {
            assert!(s.id.starts_with("CMD-"));
            assert!(s.owner_bead.starts_with("boring-cdc-"));
            assert!(!s.exit_codes.is_empty());
            assert!(!s.redaction.is_empty());
        }
    }
    #[test]
    fn generated_registry_matches_compatibility_golden() {
        let generated = serde_json::to_string_pretty(COMMANDS).unwrap() + "\n";
        assert_eq!(
            generated,
            include_str!("../tests/fixtures/m1_cli/registry-v1.json")
        );
    }

    #[test]
    fn shared_envelope_shapes_match_compatibility_golden() {
        let generated = serde_json::to_string_pretty(&compatibility_fixture()).unwrap() + "\n";
        assert_eq!(
            generated,
            include_str!("../tests/fixtures/m1_cli/envelopes-v1.json")
        );
        let fixture = compatibility_fixture();
        assert_eq!(
            fixture["fact_freshness"],
            serde_json::json!(["fresh", "stale", "unknown"])
        );
        assert_eq!(fixture["terminal_traces"][0]["plan_digest"], "plan-digest");
        assert!(fixture["event_explanation"]["reason_codes"].is_array());
    }

    #[test]
    fn every_help_path_resolves() {
        for s in COMMANDS {
            let mut a: Vec<String> = s.path.iter().map(|v| (*v).into()).collect();
            if s.operation_variant == "bootstrap" {
                a.push("--bootstrap".into())
            }
            if s.operation_variant == "explain" {
                a.extend(args("--event-id e --explain"))
            }
            a.push("--help".into());
            let p = parse(&a).unwrap();
            assert_eq!(p.spec.id, s.id);
            assert!(command_help(p.spec).contains(s.owner_bead));
        }
    }
    #[test]
    fn representative_inventory_parses() {
        for s in [
            "check --json",
            "init --dry-run --json",
            "run",
            "run --bootstrap",
            "status --json",
            "backfill restart --confirm --confirm-token t",
            "destination add d --archive-root 'p' --continuity-break --from-seq 2 --dry-run",
            "destination detach d --confirm --confirm-token t",
            "destination verify d --from-seq 1 --json",
            "archive reconstruct d --selector-fence 3 --output out",
            "replay d --since now --new-generation 2 --confirm-replay --confirm-token t",
            "journal inspect --event-id e --explain --json",
            "journal gc --dry-run",
            "recover promotion d --adopt-external-fence --confirm --confirm-token t",
            "recover reseed --add-table s.t --recreate-publication --recreate-slot --confirm-data-gap --confirm-token t",
            "recover reseed --resume r --confirm --confirm-token t",
        ] {
            assert!(parse(&args(s)).is_ok(), "{s}");
        }
    }
    #[test]
    fn invalid_and_ambiguous_invocations_fail_with_two() {
        for s in [
            "wat",
            "status --secret",
            "run --json",
            "destination add d --archive-root p --continuity-break --from-seq 2 --dry-run --confirm-data-gap",
            "replay d --from-seq 1 --since x --new-generation 2 --dry-run",
            "journal inspect --explain",
            "recover reseed --resume r --confirm-data-gap",
            "recover reseed --resume r --confirm --confirm-data-gap",
            "recover reseed --resume r --add-table s.t --confirm",
            "backfill restart --confirm --confirm",
            "status unexpected",
            "run --confirm",
        ] {
            let e = parse(&args(s)).unwrap_err();
            assert_eq!(e.exit, ExitCode::Invalid, "{s}");
        }
    }
    #[test]
    fn shell_rendering_is_copy_pasteable_and_does_not_interpolate() {
        let argv = vec![
            "destination".into(),
            "add".into(),
            "name with ' quote;$(x)".into(),
            "--confirm-data-gap".into(),
        ];
        let rendered = confirmation_command(&argv);
        assert_eq!(
            rendered,
            "boring-cdc destination add 'name with '\\'' quote;$(x)' --confirm-data-gap"
        );
        assert!(!rendered.contains("confirm_token"));
    }
    #[test]
    fn redaction_is_recursive() {
        let mut v = serde_json::json!({"confirm_token":"secret","canonical_argv":["destination","/private/path"],"nested":{"nonce_value":"n","dsn":"postgres://secret","safe":"ok"}});
        redact_json(&mut v);
        let s = v.to_string();
        assert!(!s.contains("secret"));
        assert!(!s.contains("postgres"));
        assert!(s.contains("ok"));
        assert!(!s.contains("private"));
    }
    #[test]
    fn action_authorization_ignores_unrelated_observation_progress_but_rejects_relevant_change() {
        let plan = ActionPlan {
            schema_version: 1,
            plan_id: "p".into(),
            plan_digest: "d".into(),
            request_id: "request-1".into(),
            canonical_payload_digest: "payload-digest".into(),
            command_id: "CMD-REPLAY".into(),
            observed_snapshot_id: "s1".into(),
            observed_state_revision: 10,
            bound_control_revisions: vec![BoundRevision {
                name: "destination_revision".into(),
                value: "7".into(),
                owner_bead: "boring-cdc-m5-ops-cli".into(),
            }],
            bound_fingerprints: BTreeMap::from([("source".into(), "a".into())]),
            preconditions: vec![],
            current_predicates: vec!["resources_sufficient".into()],
            intended_transitions: vec![],
            immutable_intents: vec![],
            external_effects: vec![],
            resource_consequences: vec![],
            continuity_consequences: vec![],
            affected_objects: vec![],
            allowed_remaining_actions: vec![],
            rollback_boundary: "before dispatch".into(),
            expected_postconditions: vec![],
            evidence_requirements: vec![],
            confirmation: ConfirmationPolicy {
                kind: Confirmation::ConfirmReplay,
                expires_at: Some("2026-09-09T12:00:00Z".into()),
                canonical_argv: vec![],
                confirm_token: Some("secret".into()),
            },
            expires_at: "2026-09-09T13:00:00Z".into(),
            extensions: BTreeMap::new(),
        };
        let revisions = BTreeMap::from([
            ("destination_revision".into(), "7".into()),
            ("telemetry_revision".into(), "999".into()),
        ]);
        let fp = BTreeMap::from([
            ("source".into(), "a".into()),
            ("unrelated_observation".into(), "999".into()),
        ]);
        assert!(plan.authorization_is_current(&revisions, &fp, "2026-09-09T11:00:00Z", true));
        let changed = BTreeMap::from([("destination_revision".into(), "8".into())]);
        assert!(!plan.authorization_is_current(&changed, &fp, "2026-09-09T11:00:00Z", true));
        let changed_fp = BTreeMap::from([("source".into(), "changed".into())]);
        assert!(!plan.authorization_is_current(
            &revisions,
            &changed_fp,
            "2026-09-09T11:00:00Z",
            true
        ));
        assert!(!plan.authorization_is_current(&revisions, &fp, "2026-09-09T11:00:00Z", false));
        assert!(!plan.authorization_is_current(&revisions, &fp, "2026-09-09T12:00:01Z", true));
        assert!(!plan.authorization_is_current(&revisions, &fp, "2026-09-09T11:00:00+01:00", true));
        assert!(!plan.authorization_is_current(&revisions, &fp, "2026-99-99T99:99:99Z", true));
        let attempt = ConfirmationAttempt {
            confirm_token: "opaque".into(),
            request_id: "request-1".into(),
            canonical_payload_digest: "payload-digest".into(),
            plan_digest: "d".into(),
        };
        assert!(plan.authorize_confirmation(
            &attempt,
            &revisions,
            &fp,
            "2026-09-09T11:00:00Z",
            true,
            |attempt, _| attempt.confirm_token == "opaque"
        ));
        let mut changed_attempt = attempt.clone();
        changed_attempt.canonical_payload_digest = "changed".into();
        assert!(!plan.authorize_confirmation(
            &changed_attempt,
            &revisions,
            &fp,
            "2026-09-09T11:00:00Z",
            true,
            |_, _| true
        ));
        let direct_json = serde_json::to_value(&plan).unwrap();
        assert_eq!(direct_json["confirmation"]["confirm_token"], "secret");
        let mut artifact = direct_json;
        redact_json(&mut artifact);
        assert_eq!(artifact["confirmation"]["confirm_token"], "[REDACTED]");
    }
    #[test]
    fn terminal_mutation_requires_one_typed_causal_end() {
        let spec = COMMANDS.iter().find(|s| s.id == "CMD-INIT").unwrap();
        let mut result = unavailable(spec);
        result.outcome = "success".into();
        result.plan_digest = Some("plan".into());
        result.postcondition_evidence_digest = Some("post".into());
        assert!(!result.validate_for(spec));
        result.mutation_trace = Some(MutationTrace {
            before_snapshot_id: "before".into(),
            plan_digest: "plan".into(),
            immutable_intent_id: "intent".into(),
            external_effect_evidence_digest: None,
            terminal: MutationTerminal::SafeTerminal {
                safe_terminal_state: "aborted_by_restart".into(),
            },
        });
        assert!(result.validate_for(spec));
    }

    #[test]
    fn forward_unknown_snapshot_fields_round_trip() {
        let input = serde_json::json!({"schema_version":1,"snapshot_id":"s","state_revision":1,"observed_at":"a","fresh_until":"b","freshness":"fresh","source_identity":null,"capture_epoch":null,"run_id":null,"ownership":{},"fingerprints":{},"durability_boundaries":{},"budgets":{},"destinations":[],"conditions":[],"blocked_by":[],"allowed_actions":[],"next_commands":[],"evidence_digest":"d","future_field":{"x":1}});
        let parsed: SystemSnapshot = serde_json::from_value(input).unwrap();
        assert!(parsed.extensions.contains_key("future_field"));
    }
    #[test]
    fn machine_envelope_has_frozen_fields_and_no_color() {
        let text = serde_json::to_string(&unavailable(&COMMANDS[0])).unwrap();
        for field in [
            "schema_version",
            "command",
            "outcome",
            "code",
            "message",
            "request_id",
            "run_id",
            "capture_epoch",
            "condition",
            "runbook_id",
            "data",
            "warnings",
            "next_commands",
        ] {
            assert!(text.contains(field));
        }
        assert!(!text.contains("\u{1b}["));
        assert_eq!(ExitCode::Success as u8, 0);
        assert_eq!(ExitCode::SafetyBlocked as u8, 3);
        assert_eq!(ExitCode::Integrity as u8, 5);
        assert_eq!(ExitCode::Invariant as u8, 6);
    }
}
