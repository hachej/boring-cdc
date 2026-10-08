//! Fresh read-only M2 status projection plus explicit debug-only crash hooks.
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
pub const SNAPSHOT_SCHEMA: &str = "system-snapshot/v1";
pub const FRESHNESS_SECONDS: u64 = 30;
const MAX_CONTROL_OBJECTS_PER_KIND: usize = 1000;
pub const CONDITION_NAMES: [&str; 14] = [
    "healthy",
    "degraded",
    "destination_blocked",
    "schema_blocked",
    "capture_safe_stopped",
    "slot_invalid_requires_reseed",
    "journal_corrupt_requires_reseed",
    "publication_drift_requires_reseed",
    "bootstrap_ambiguous_requires_restart",
    "heartbeat_degraded",
    "promotion_recovery_required",
    "source_identity_blocked",
    "ownership_lost",
    "unsafe_durability",
];
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultHook {
    BeforeSourceCommit,
    AfterSourceCommitBeforeFeedback,
    BeforeFeedback,
    AfterFeedback,
    BootstrapIntentDurable,
    SpoolCreated,
    SpoolSynced,
    ArchiveIntentDurable,
    ArchiveFileSynced,
    ArchiveDirectorySynced,
    LeaseFenced,
    CheckpointBeforeCommit,
    CheckpointAfterCommit,
    PromotionBeforeSelector,
    PromotionAfterSelector,
    OwnershipLost,
}
impl FaultHook {
    pub const ALL: [Self; 16] = [
        Self::BeforeSourceCommit,
        Self::AfterSourceCommitBeforeFeedback,
        Self::BeforeFeedback,
        Self::AfterFeedback,
        Self::BootstrapIntentDurable,
        Self::SpoolCreated,
        Self::SpoolSynced,
        Self::ArchiveIntentDurable,
        Self::ArchiveFileSynced,
        Self::ArchiveDirectorySynced,
        Self::LeaseFenced,
        Self::CheckpointBeforeCommit,
        Self::CheckpointAfterCommit,
        Self::PromotionBeforeSelector,
        Self::PromotionAfterSelector,
        Self::OwnershipLost,
    ];
    pub const fn name(self) -> &'static str {
        match self {
            Self::BeforeSourceCommit => "before_source_commit",
            Self::AfterSourceCommitBeforeFeedback => "after_source_commit_before_feedback",
            Self::BeforeFeedback => "before_feedback",
            Self::AfterFeedback => "after_feedback",
            Self::BootstrapIntentDurable => "bootstrap_intent_durable",
            Self::SpoolCreated => "spool_created",
            Self::SpoolSynced => "spool_synced",
            Self::ArchiveIntentDurable => "archive_intent_durable",
            Self::ArchiveFileSynced => "archive_file_synced",
            Self::ArchiveDirectorySynced => "archive_directory_synced",
            Self::LeaseFenced => "lease_fenced",
            Self::CheckpointBeforeCommit => "checkpoint_before_commit",
            Self::CheckpointAfterCommit => "checkpoint_after_commit",
            Self::PromotionBeforeSelector => "promotion_before_selector",
            Self::PromotionAfterSelector => "promotion_after_selector",
            Self::OwnershipLost => "ownership_lost",
        }
    }
}
/// Release builds never inspect the local-experiment environment control.
pub fn fault_hook(h: FaultHook) {
    #[cfg(debug_assertions)]
    if std::env::var("BORING_CDC_M2_FAULT_HOOK").ok().as_deref() == Some(h.name()) {
        std::process::abort()
    }
    #[cfg(not(debug_assertions))]
    let _ = h;
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StatusCondition {
    pub condition: String,
    pub condition_id: String,
    pub severity: String,
    pub reason: String,
    pub observed_at: String,
    pub fresh_until: String,
    pub runbook_id: String,
    pub runbook_version: u32,
    pub procedure_status: String,
    pub evidence_digest: Option<String>,
    pub allowed_actions: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FailureStatus {
    pub component: String,
    pub failure_class: String,
    pub fingerprint: String,
    pub first_failed_at: String,
    pub last_failed_at: String,
    pub attempt: u64,
    pub next_retry_at: Option<String>,
    pub retry_armed: bool,
    pub failed_journal_range: Option<[u64; 2]>,
    pub failed_xid: Option<String>,
    pub failed_final_lsn: Option<String>,
    pub observed_transaction_bytes: Option<u64>,
    pub observed_transaction_events: Option<u64>,
    pub limit_bytes: Option<u64>,
    pub limit_events: Option<u64>,
    pub wal_headroom_consequence: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct M2SystemSnapshot {
    pub schema_version: String,
    pub snapshot_id: String,
    pub state_revision: u64,
    pub observed_at: String,
    pub fresh_until: String,
    pub freshness: String,
    pub overall_health: String,
    pub source_identity_fingerprint: Option<String>,
    pub capture_epoch: Option<String>,
    pub run_id: Option<String>,
    pub ownership: Value,
    pub configuration_fingerprint: String,
    pub control_revisions: BTreeMap<String, u64>,
    pub boundaries: Value,
    pub budgets: Value,
    pub destinations: Vec<Value>,
    pub failures: Vec<FailureStatus>,
    pub conditions: Vec<StatusCondition>,
    pub blocked_by: Vec<String>,
    pub allowed_actions: Vec<String>,
    pub next_commands: Vec<Value>,
    pub action_causality: Vec<Value>,
    pub evidence_digest: String,
}
#[derive(Debug)]
pub enum StatusError {
    Sqlite(rusqlite::Error),
    Clock,
    MissingState,
    TooManyControlObjects,
}
impl From<rusqlite::Error> for StatusError {
    fn from(v: rusqlite::Error) -> Self {
        Self::Sqlite(v)
    }
}
fn ts(v: u64) -> String {
    format!("unix:{v}")
}
fn hash(parts: &[&str]) -> String {
    let mut h = Sha256::new();
    for p in parts {
        h.update((p.len() as u64).to_be_bytes());
        h.update(p.as_bytes())
    }
    format!("sha256:{:x}", h.finalize())
}
fn public_causal_digest(value: Option<String>) -> Option<String> {
    value.filter(|value| {
        value.strip_prefix("sha256:").is_some_and(|hex| {
            hex.len() == 64
                && hex
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        })
    })
}

fn add_object_revisions(
    connection: &Connection,
    revisions: &mut BTreeMap<String, u64>,
    namespace: &str,
    query: &str,
) -> Result<(), StatusError> {
    let mut statement = connection.prepare(query)?;
    for (index, row) in statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
        })?
        .enumerate()
    {
        if index == MAX_CONTROL_OBJECTS_PER_KIND {
            return Err(StatusError::TooManyControlObjects);
        }
        let (id, revision) = row?;
        revisions.insert(format!("{namespace}:{}", hash(&[&id])), revision);
    }
    Ok(())
}

fn add_current_audit_revisions(
    connection: &Connection,
    revisions: &mut BTreeMap<String, u64>,
) -> Result<(), StatusError> {
    let mut statement = connection.prepare(
        "SELECT a.audit_id,a.incarnation,a.revision \
         FROM destination_current_audits current \
         JOIN destination_audits a ON a.audit_id=current.audit_id \
           AND a.destination_id=current.destination_id AND a.incarnation=current.incarnation \
         JOIN destinations d ON d.destination_id=current.destination_id \
           AND d.configuration_fingerprint=a.configuration_fingerprint \
           AND d.capture_epoch=a.capture_epoch AND d.generation=a.generation \
         LEFT JOIN destination_checkpoints cp ON cp.destination_id=d.destination_id \
         WHERE a.incarnation>0 AND a.round_target_seq=coalesce(cp.journal_seq,0) \
           AND (cp.destination_id IS NULL OR (cp.configuration_fingerprint=d.configuration_fingerprint \
             AND cp.capture_epoch=d.capture_epoch AND cp.generation=d.generation))",
    )?;
    for (index, row) in statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, u64>(2)?,
            ))
        })?
        .enumerate()
    {
        if index == MAX_CONTROL_OBJECTS_PER_KIND {
            return Err(StatusError::TooManyControlObjects);
        }
        let (id, incarnation, revision) = row?;
        revisions.insert(
            format!(
                "destination_audit:{}",
                hash(&[&id, &incarnation.to_string()])
            ),
            revision,
        );
    }
    Ok(())
}
fn cid(n: &str) -> String {
    format!("COND-{}", n.replace('_', "-").to_ascii_uppercase())
}
fn actions(n: &str) -> Vec<String> {
    let command = match n {
        "healthy" => "CMD-STATUS",
        "degraded" => "CMD-CHECK",
        "destination_blocked" => "CMD-DESTINATION-LIST",
        "schema_blocked" => "CMD-JOURNAL-INSPECT",
        "capture_safe_stopped" => "CMD-JOURNAL-VERIFY",
        "slot_invalid_requires_reseed" => "CMD-RECOVER-RESEED",
        "journal_corrupt_requires_reseed" => "CMD-JOURNAL-GC",
        "publication_drift_requires_reseed" => "CMD-INIT",
        "bootstrap_ambiguous_requires_restart" => "CMD-RECOVER-INSPECT",
        "heartbeat_degraded" => "CMD-RUN",
        "promotion_recovery_required" => "CMD-RECOVER-PROMOTION",
        "source_identity_blocked" => "CMD-ARCHIVE-VERIFY",
        "ownership_lost" => "CMD-RUN-BOOTSTRAP",
        "unsafe_durability" => "CMD-ARCHIVE-RECONSTRUCT",
        _ => "CMD-STATUS",
    };
    vec![command.into()]
}
fn add(
    v: &mut Vec<(String, String, String, Option<String>)>,
    n: &str,
    s: &str,
    r: &str,
    e: Option<String>,
) {
    if !v.iter().any(|x| x.0 == n) {
        v.push((n.into(), s.into(), r.into(), e))
    }
}
type Source = (
    String,
    String,
    String,
    String,
    String,
    Option<String>,
    Option<u64>,
    Option<String>,
    Option<String>,
    u64,
    Option<String>,
    Option<String>,
);
type Owner = (String, String, u64, u64);
pub fn snapshot(
    path: &Path,
    config: &str,
    now: SystemTime,
) -> Result<M2SystemSnapshot, StatusError> {
    let now = now
        .duration_since(UNIX_EPOCH)
        .map_err(|_| StatusError::Clock)?
        .as_secs();
    let c = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    c.busy_timeout(Duration::from_millis(250))?;
    c.execute_batch("PRAGMA query_only=ON;PRAGMA foreign_keys=ON;BEGIN;")?;
    let s:Source=c.query_row("SELECT capture_epoch,source_system_id,database_id,slot_name,publication_fingerprint,durable_transaction_end_lsn,durable_journal_seq,last_feedback_lsn,slot_creation_floor_lsn,control_revision,observed_confirmed_flush_lsn,observed_restart_lsn FROM source_state WHERE singleton=1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?,r.get(9)?,r.get(10)?,r.get(11)?))).optional()?.ok_or(StatusError::MissingState)?;
    let owner:Option<Owner>=c.query_row("SELECT run_id,state,connection_generation,revision FROM runtime_ownership ORDER BY revision DESC,run_id DESC LIMIT 1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    let latest:Option<(String,String,String,String,Option<u64>)>=c.query_row("SELECT run_id,outcome,reason_code,created_at,unixepoch(created_at) FROM startup_reconciliations ORDER BY reconciliation_id DESC LIMIT 1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
    let heartbeat: Option<(u64, u64, String)> = if let Some((run_id, outcome, _, _, _)) = &latest {
        if matches!(
            outcome.as_str(),
            "ready" | "duplicate_replay_expected" | "creation_floor_only"
        ) {
            c.query_row("SELECT observed_at_unix_seconds,last_heartbeat_seq,last_heartbeat_end_lsn FROM capture_health_observations WHERE run_id=?1 AND capture_epoch=?2",rusqlite::params![run_id,&s.0],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?
        } else {
            None
        }
    } else {
        None
    };
    let fact_seconds = heartbeat
        .as_ref()
        .map(|v| v.0)
        .filter(|seconds| *seconds <= now)
        .unwrap_or(0);
    let startup_seconds = latest
        .as_ref()
        .and_then(|v| v.4)
        .filter(|seconds| *seconds <= now)
        .unwrap_or(0);
    let observed = if fact_seconds == 0 {
        "unknown".into()
    } else {
        ts(fact_seconds)
    };
    let fresh = if fact_seconds == 0 {
        "unknown".into()
    } else {
        ts(fact_seconds.saturating_add(FRESHNESS_SECONDS))
    };
    let freshness = if fact_seconds == 0 {
        if startup_seconds > 0 && now > startup_seconds.saturating_add(FRESHNESS_SECONDS) {
            "stale"
        } else {
            "unknown"
        }
    } else if now <= fact_seconds.saturating_add(FRESHNESS_SECONDS) {
        "fresh"
    } else {
        "stale"
    };
    let mut failures = Vec::new();
    let mut q=c.prepare("SELECT component,failure_class,fingerprint,first_failed_at,last_failed_at,attempt,next_retry_at,armed,failed_boundary_start_seq,failed_boundary_end_seq,retry_class,failed_xid,failed_final_lsn,observed_transaction_bytes,observed_transaction_events,limit_bytes,limit_events FROM processing_failures ORDER BY component,fingerprint LIMIT 100")?;
    for row in q.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, String>(4)?,
            r.get::<_, u64>(5)?,
            r.get::<_, Option<String>>(6)?,
            r.get::<_, i64>(7)?,
            r.get::<_, Option<u64>>(8)?,
            r.get::<_, Option<u64>>(9)?,
            r.get::<_, String>(10)?,
            r.get::<_, Option<String>>(11)?,
            r.get::<_, Option<String>>(12)?,
            r.get::<_, Option<u64>>(13)?,
            r.get::<_, Option<u64>>(14)?,
            r.get::<_, Option<u64>>(15)?,
            r.get::<_, Option<u64>>(16)?,
        ))
    })? {
        let x = row?;
        failures.push(FailureStatus {
            component: x.0,
            failure_class: x.1,
            fingerprint: x.2,
            first_failed_at: x.3,
            last_failed_at: x.4,
            attempt: x.5,
            next_retry_at: x.6,
            retry_armed: x.7 == 1,
            failed_journal_range: x.8.zip(x.9).map(|(a, b)| [a, b]),
            failed_xid: x
                .11
                .filter(|x| !x.is_empty() && x.bytes().all(|b| b.is_ascii_digit())),
            failed_final_lsn: x.12,
            observed_transaction_bytes: x.13,
            observed_transaction_events: x.14,
            limit_bytes: x.15,
            limit_events: x.16,
            wal_headroom_consequence: if x.7 == 0 {
                "resolved historical failure; no current action".into()
            } else if x.10 == "transient" {
                "retry may consume retained WAL headroom".into()
            } else {
                "checkpoint remains fixed; operator action required".into()
            },
        })
    }
    let mut raw = Vec::new();
    if freshness != "fresh" {
        add(
            &mut raw,
            "heartbeat_degraded",
            "degraded",
            if freshness == "stale" {
                "STATUS_PROVENANCE_STALE"
            } else {
                "STATUS_PROVENANCE_UNKNOWN"
            },
            None,
        );
    }
    if s.7
        .as_ref()
        .zip(s.5.as_ref())
        .is_some_and(|(feedback, durable)| feedback > durable)
    {
        add(
            &mut raw,
            "unsafe_durability",
            "critical",
            "FEEDBACK_OUTRUNS_DURABILITY",
            None,
        );
    }
    if let Some((_, o, r, _, _)) = &latest {
        if r.starts_with("SCHEMA_") {
            add(&mut raw, "schema_blocked", "blocked", r, None);
        }
        match (o.as_str(), r.as_str()) {
            (_, "JOURNAL_INTEGRITY_FAILED") => add(
                &mut raw,
                "journal_corrupt_requires_reseed",
                "critical",
                r,
                None,
            ),
            (_, "SOURCE_IDENTITY_MISMATCH") => {
                add(&mut raw, "source_identity_blocked", "critical", r, None)
            }
            (_, "PUBLICATION_IDENTITY_MISMATCH") => add(
                &mut raw,
                "publication_drift_requires_reseed",
                "critical",
                r,
                None,
            ),
            (_, "SLOT_INVALID") | (_, "RESUME_WAL_UNAVAILABLE") | (_, "SLOT_MISSING") => add(
                &mut raw,
                "slot_invalid_requires_reseed",
                "critical",
                r,
                None,
            ),
            ("bootstrap_ambiguous_requires_restart", _) => add(
                &mut raw,
                "bootstrap_ambiguous_requires_restart",
                "blocked",
                r,
                None,
            ),
            ("blocked", _) | ("requires_reseed", _) => {
                add(&mut raw, "capture_safe_stopped", "blocked", r, None)
            }
            _ => {}
        }
    }
    let mut alert_query = c.prepare("SELECT condition_id,evidence_digest FROM alerts WHERE state='active' ORDER BY condition_id LIMIT 100")?;
    for row in alert_query.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
    })? {
        let (id, evidence) = row?;
        let name = id
            .strip_prefix("COND-")
            .unwrap_or("")
            .to_ascii_lowercase()
            .replace('-', "_");
        if CONDITION_NAMES.contains(&name.as_str()) {
            add(&mut raw, &name, "blocked", "ACTIVE_DOMAIN_ALERT", evidence);
        }
    }
    if c.query_row("SELECT EXISTS(SELECT 1 FROM destination_promotion_intents WHERE state='promotion_recovery_required')",[],|r|r.get::<_,i64>(0))?==1{add(&mut raw,"promotion_recovery_required","blocked","PROMOTION_RECOVERY_REQUIRED",None)}
    if owner.as_ref().is_some_and(|x| x.1 == "lost") {
        add(
            &mut raw,
            "ownership_lost",
            "critical",
            "OWNERSHIP_LOST",
            None,
        )
    }
    for f in &failures {
        if f.retry_armed {
            let n = if f.component == "capture" {
                "capture_safe_stopped"
            } else {
                "destination_blocked"
            };
            let sev = if f.failure_class.starts_with("transient_") {
                "degraded"
            } else {
                "blocked"
            };
            add(
                &mut raw,
                n,
                sev,
                &f.failure_class,
                Some(f.fingerprint.clone()),
            )
        }
    }
    if raw.is_empty() {
        add(&mut raw, "healthy", "healthy", "M2_HEALTHY", None)
    } else {
        add(
            &mut raw,
            "degraded",
            "degraded",
            "CONCURRENT_M2_CONDITIONS",
            None,
        )
    }
    raw.sort_by(|a, b| a.0.cmp(&b.0));
    let conditions = raw
        .into_iter()
        .map(|(n, severity, reason, evidence)| {
            let id = cid(&n);
            StatusCondition {
                condition: n,
                condition_id: id.clone(),
                severity,
                reason,
                observed_at: observed.clone(),
                fresh_until: fresh.clone(),
                runbook_id: format!("RUNBOOK-{}-V1", id.trim_start_matches("COND-")),
                runbook_version: 1,
                procedure_status: "pending_m6".into(),
                evidence_digest: evidence,
                allowed_actions: actions(
                    &id.to_ascii_lowercase()
                        .replace("cond-", "")
                        .replace('-', "_"),
                ),
            }
        })
        .collect::<Vec<_>>();
    let overall = conditions
        .iter()
        .max_by_key(|x| match x.severity.as_str() {
            "critical" => 4,
            "blocked" => 3,
            "degraded" => 2,
            _ => 1,
        })
        .map(|x| x.severity.clone())
        .unwrap_or_else(|| "healthy".into());
    let db_bytes = std::fs::metadata(path).map(|x| x.len()).unwrap_or(0);
    let mut dest = Vec::new();
    let mut dq=c.prepare("SELECT d.destination_id,d.kind,d.generation,d.highest_external_fence,coalesce(cp.journal_seq,0),coalesce(cp.revision,0) FROM destinations d LEFT JOIN destination_checkpoints cp USING(destination_id) ORDER BY d.destination_id LIMIT 100")?;
    for row in dq.query_map([],|r|{let id:String=r.get(0)?;Ok(json!({"destination_fingerprint":hash(&[&id]),"kind":r.get::<_,String>(1)?,"generation":r.get::<_,u64>(2)?,"highest_external_fence":r.get::<_,u64>(3)?,"checkpoint_seq":r.get::<_,u64>(4)?,"checkpoint_revision":r.get::<_,u64>(5)?}))})?{dest.push(row?)}
    let mut action_causality = Vec::new();
    let mut aq = c.prepare("SELECT request_id,payload_digest,run_id,state,observation_revision,control_revision,plan_digest,immutable_intent_id,external_effect_evidence_digest,postcondition_evidence_digest FROM operator_command_requests ORDER BY request_id LIMIT 100")?;
    for row in aq.query_map([], |r| Ok(json!({"request_id":r.get::<_,String>(0)?,"canonical_payload_digest":r.get::<_,String>(1)?,"plan_digest":public_causal_digest(r.get::<_,Option<String>>(6)?),"immutable_intent_id":public_causal_digest(r.get::<_,Option<String>>(7)?),"external_effect_evidence_digest":public_causal_digest(r.get::<_,Option<String>>(8)?),"postcondition_evidence_digest":public_causal_digest(r.get::<_,Option<String>>(9)?),"run_id":r.get::<_,String>(2)?,"terminal_state":r.get::<_,String>(3)?,"before_state_revision":r.get::<_,u64>(4)?,"bound_control_revision":r.get::<_,u64>(5)?})))? { action_causality.push(row?); }
    let mut control_revisions = BTreeMap::from([
        ("source".into(), s.9),
        ("ownership".into(), owner.as_ref().map(|x| x.3).unwrap_or(0)),
    ]);
    for (namespace, query) in [
        (
            "bootstrap_intent",
            "SELECT intent_id,revision FROM bootstrap_intents WHERE state NOT IN ('complete','invalidated','aborted')",
        ),
        (
            "destination_lease",
            "SELECT l.lease_id,l.revision FROM destination_generation_leases l JOIN destinations d ON d.destination_id=l.destination_id AND d.capture_epoch=l.capture_epoch AND d.generation=l.generation WHERE l.state='held'",
        ),
        (
            "destination_promotion",
            "SELECT intent_id,revision FROM destination_promotion_intents WHERE state NOT IN ('verified','retirement_eligible','retired')",
        ),
        (
            "destination",
            "SELECT destination_id,revision FROM destinations",
        ),
    ] {
        add_object_revisions(&c, &mut control_revisions, namespace, query)?;
    }
    add_current_audit_revisions(&c, &mut control_revisions)?;
    for item in &dest {
        if let (Some(id), Some(rev)) = (
            item["destination_fingerprint"].as_str(),
            item["checkpoint_revision"].as_u64(),
        ) {
            control_revisions.insert(format!("destination_checkpoint:{id}"), rev);
        }
    }
    let stable_conditions = conditions
        .iter()
        .map(|c| (&c.condition, &c.severity, &c.reason, &c.evidence_digest))
        .collect::<Vec<_>>();
    let canon = json!({"source":[&s.0,&s.4,&s.5,&s.6,&s.7,&s.8,s.9,&s.10,&s.11],"ownership":&owner,"latest":&latest,"heartbeat":&heartbeat,"destinations":&dest,"failures":&failures,"conditions":stable_conditions,"control_revisions":&control_revisions,"action_causality":&action_causality,"config":config});
    let evidence = format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(&canon).unwrap())
    );
    let revision = u64::from_be_bytes(Sha256::digest(evidence.as_bytes())[..8].try_into().unwrap());
    let identity = hash(&[&s.1, &s.2, &s.3]);
    let blocked = conditions
        .iter()
        .filter(|x| x.severity != "healthy" && x.condition != "degraded")
        .map(|x| x.condition_id.clone())
        .collect();
    let allowed = conditions
        .iter()
        .flat_map(|x| x.allowed_actions.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    Ok(M2SystemSnapshot{schema_version:SNAPSHOT_SCHEMA.into(),snapshot_id:hash(&[&evidence,&revision.to_string()]),state_revision:revision,observed_at:observed,fresh_until:fresh,freshness:freshness.into(),overall_health:overall,source_identity_fingerprint:Some(identity),capture_epoch:Some(s.0),run_id:owner.as_ref().map(|x|x.0.clone()),ownership:owner.map_or(json!({"state":"unowned","actual_writer":"unknown"}),|x|json!({"state":x.1,"connection_generation":x.2,"revision":x.3,"actual_writer":"unknown","actual_writer_freshness":"unavailable"})),configuration_fingerprint:config.into(),control_revisions,boundaries:json!({"source_confirmed_flush_lsn":s.10,"source_restart_lsn":s.11,"durable_transaction_end_lsn":s.5,"durable_journal_seq":s.6,"feedback_lsn":s.7,"creation_floor_lsn":s.8}),budgets:json!({"sqlite_bytes":db_bytes,"pressure":"derived_by_m2_pressure","source_wal_headroom":"unknown"}),destinations:dest,failures,conditions,blocked_by:blocked,allowed_actions:allowed,next_commands:vec![json!({"command_id":"CMD-STATUS","argv":["status","--json"]})],action_causality,evidence_digest:evidence})
}
#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::m2_schema::open_writer;
    fn fixture() -> (std::path::PathBuf, crate::m2_schema::WriterConnection) {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let p = std::env::temp_dir().join(format!(
            "m2-fault-status-{}-{}.db",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&p);
        let w = open_writer(&p, "status-test", 1, 0).unwrap();
        w.connection().execute("INSERT INTO source_state(singleton,capture_epoch,source_system_id,timeline_id,database_id,slot_name,plugin,publication_fingerprint,protocol_fingerprint) VALUES(1,'epoch','secret-system','tl','secret-db','secret-slot','pgoutput','pub','proto')",[]).unwrap();
        (p, w)
    }
    #[test]
    fn stable_restart_projection_redacts() {
        let (p, w) = fixture();
        w.connection().execute("INSERT INTO processing_failures(failure_id,component,failure_class,fingerprint,retry_class,attempt,armed,first_failed_at,last_failed_at,failed_xid) VALUES('bad','capture','unsupported','fingerprint','deterministic',1,0,'old','old','secret-xid')",[]).unwrap();
        drop(w);
        let a = snapshot(&p, "cfg", UNIX_EPOCH + Duration::from_secs(10)).unwrap();
        let b = snapshot(&p, "cfg", UNIX_EPOCH + Duration::from_secs(20)).unwrap();
        assert_eq!(a.snapshot_id, b.snapshot_id);
        assert_eq!(a.state_revision, b.state_revision);
        let t = serde_json::to_string(&a).unwrap();
        assert!(
            !t.contains("secret-system")
                && !t.contains("secret-db")
                && !t.contains("secret-slot")
                && !t.contains("secret-xid")
        );
        assert!(
            a.conditions
                .iter()
                .any(|c| c.condition == "heartbeat_degraded")
        );
        assert!(!a.conditions.iter().any(|c| c.condition == "healthy"));
    }
    #[test]
    fn failure_and_reseed_are_concurrent() {
        let (p, w) = fixture();
        w.connection().execute("INSERT INTO startup_reconciliations(run_id,capture_epoch,outcome,reason_code,created_at) VALUES('r','epoch','requires_reseed','JOURNAL_INTEGRITY_FAILED','now')",[]).unwrap();
        w.connection().execute("INSERT INTO processing_failures(failure_id,component,failure_class,fingerprint,failed_boundary_start_seq,failed_boundary_end_seq,retry_class,attempt,next_retry_at,armed,first_failed_at,last_failed_at) VALUES('f','capture','transient_source','sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',2,3,'transient',2,'unix:99',1,'unix:1','unix:2')",[]).unwrap();
        w.connection().execute("UPDATE processing_failures SET failed_capture_epoch='epoch',failed_xid='42',failed_final_lsn='000000000000002A',observed_transaction_bytes=101,observed_transaction_events=3,limit_bytes=100,limit_events=2 WHERE failure_id='f'",[]).unwrap();
        drop(w);
        let s = snapshot(&p, "cfg", UNIX_EPOCH + Duration::from_secs(10)).unwrap();
        assert!(
            s.conditions
                .iter()
                .any(|x| x.condition == "journal_corrupt_requires_reseed")
        );
        assert!(
            s.conditions
                .iter()
                .any(|x| x.condition == "capture_safe_stopped")
        );
        assert_eq!(s.failures[0].attempt, 2);
        assert_eq!(s.failures[0].failed_journal_range, Some([2, 3]));
        assert_eq!(s.failures[0].failed_xid.as_deref(), Some("42"));
        assert_eq!(
            s.failures[0].failed_final_lsn.as_deref(),
            Some("000000000000002A")
        );
        assert_eq!(s.failures[0].observed_transaction_bytes, Some(101));
        assert_eq!(s.failures[0].observed_transaction_events, Some(3));
        assert_eq!(s.failures[0].limit_bytes, Some(100));
        assert_eq!(s.failures[0].limit_events, Some(2));
    }
    #[test]
    fn freshness_expires_and_causal_records_exclude_payloads() {
        let (p, w) = fixture();
        w.connection().execute("INSERT INTO operator_command_requests(request_id,dry_run_nonce,canonical_payload,payload_digest,run_id,peer_identity,state,result,observation_revision,control_revision,expires_at) VALUES('request-safe','nonce-secret',x'0102','sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','run-safe','peer-safe','completed',x'03',7,2,'unix:99')",[]).unwrap();
        let links: Vec<String> = (1..=4).map(|n| format!("sha256:{n:064x}")).collect();
        w.connection().execute("UPDATE operator_command_requests SET plan_digest=?1,immutable_intent_id=?2,external_effect_evidence_digest=?3,postcondition_evidence_digest=?4,request_revision=request_revision+1 WHERE request_id='request-safe'",rusqlite::params![links[0],links[1],links[2],links[3]]).unwrap();
        w.connection().execute("INSERT INTO operator_command_requests(request_id,dry_run_nonce,canonical_payload,payload_digest,run_id,peer_identity,state,result,observation_revision,control_revision,expires_at,plan_digest,immutable_intent_id) VALUES('request-unsafe','nonce-other',x'0102','digest','run-safe','peer-safe','completed',x'03',7,2,'unix:99','secret-plan','secret-intent')",[]).unwrap();
        w.connection().execute("INSERT INTO startup_reconciliations(run_id,capture_epoch,outcome,reason_code,created_at) VALUES('old-run','epoch','ready','READY','2000-01-01T00:00:00Z')",[]).unwrap();
        drop(w);
        let s = snapshot(&p, "cfg", SystemTime::now() + Duration::from_secs(60)).unwrap();
        assert_eq!(s.freshness, "stale");
        assert_eq!(s.action_causality[0]["request_id"], "request-safe");
        assert_eq!(s.action_causality[0]["plan_digest"], links[0]);
        assert_eq!(s.action_causality[0]["immutable_intent_id"], links[1]);
        assert_eq!(
            s.action_causality[0]["external_effect_evidence_digest"],
            links[2]
        );
        assert_eq!(
            s.action_causality[0]["postcondition_evidence_digest"],
            links[3]
        );
        assert!(s.action_causality[1]["plan_digest"].is_null());
        let text = serde_json::to_string(&s).unwrap();
        assert!(
            !text.contains("nonce-secret")
                && !text.contains("0102")
                && !text.contains("secret-plan")
                && !text.contains("secret-intent")
        );
    }
    #[test]
    fn domain_alerts_project_declared_conditions_and_revisions() {
        let (p, w) = fixture();
        for (i, name) in ["SCHEMA-BLOCKED", "HEARTBEAT-DEGRADED", "UNSAFE-DURABILITY"]
            .into_iter()
            .enumerate()
        {
            w.connection().execute("INSERT INTO alerts(alert_id,condition_id,state,opened_at,evidence_digest) VALUES(?1,?2,'active','now',?3)",rusqlite::params![format!("alert-{i}"),format!("COND-{name}"),format!("sha256:{:064x}",i+1)]).unwrap();
        }
        drop(w);
        let s = snapshot(&p, "cfg", SystemTime::now()).unwrap();
        for name in ["schema_blocked", "heartbeat_degraded", "unsafe_durability"] {
            assert!(s.conditions.iter().any(|c| c.condition == name));
        }
        assert!(s.control_revisions.contains_key("source"));
        assert!(s.control_revisions.contains_key("ownership"));
        assert!(!s.control_revisions.contains_key("bootstrap"));
        assert!(!s.control_revisions.contains_key("lease"));
        assert!(!s.control_revisions.contains_key("promotion"));
    }
    #[test]
    fn revisions_bind_independent_objects_across_restart_and_concurrent_reads() {
        let (p, w) = fixture();
        for id in ["boot-a", "boot-b"] {
            w.connection().execute("INSERT INTO bootstrap_intents(intent_id,capture_epoch,source_system_id,database_id,slot_name,state,created_at) VALUES(?1,'epoch','system','database','slot','prepared','unix:1')",[id]).unwrap();
        }
        for id in ["dest-a", "dest-b"] {
            w.connection().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES(?1,'clickhouse','cfg','epoch',1)",[id]).unwrap();
        }
        w.connection().execute("INSERT INTO bootstrap_anchors(anchor_id,capture_epoch,generation,start_seq,snapshot_boundary_lsn,table_set_fingerprint,snapshot_schema_fingerprints,state,expires_at) VALUES('anchor','epoch',1,0,'0000000000000000','set','[\"schema\"]','building','unix:99')",[]).unwrap();
        for (destination, lease, promotion) in [
            ("dest-a", "lease-a", "promotion-a"),
            ("dest-b", "lease-b", "promotion-b"),
        ] {
            w.connection().execute("INSERT INTO destination_generation_leases(lease_id,destination_id,capture_epoch,generation,configuration_fingerprint,run_id,expires_mono_ms,state) VALUES(?1,?2,'epoch',1,'cfg','run',99,'held')",rusqlite::params![lease,destination]).unwrap();
            w.connection().execute("INSERT INTO destination_promotion_intents(intent_id,destination_id,capture_epoch,candidate_generation,anchor_id,configuration_fingerprint,promotion_fence,expected_selector_digest,state) VALUES(?1,?2,'epoch',1,'anchor','cfg',1,'selector','prepared')",rusqlite::params![promotion,destination]).unwrap();
        }
        for (audit, destination, incarnation) in
            [("audit-a", "dest-a", 1), ("audit-b", "dest-b", 2)]
        {
            w.connection().execute("INSERT INTO destination_audits(audit_id,destination_id,configuration_fingerprint,capture_epoch,generation,round_target_seq,round_identity_digest,journal_cursor_seq,self_cursor_seq,freshness_window_started_at,freshness_expires_at,contract_digest,incarnation) VALUES(?1,?2,'cfg','epoch',1,0,'round',0,0,'unix:1','unix:2','contract',?3)",rusqlite::params![audit,destination,incarnation]).unwrap();
            w.connection().execute("INSERT INTO destination_current_audits(destination_id,audit_id,incarnation) VALUES(?1,?2,?3)",rusqlite::params![destination,audit,incarnation]).unwrap();
        }
        let before = snapshot(&p, "cfg", UNIX_EPOCH + Duration::from_secs(10)).unwrap();
        let key = |namespace: &str, id: &str| {
            let identity = if namespace == "destination_audit" {
                hash(&[id, if id == "audit-a" { "1" } else { "2" }])
            } else {
                hash(&[id])
            };
            format!("{namespace}:{identity}")
        };
        for (namespace, id) in [
            ("bootstrap_intent", "boot-a"),
            ("bootstrap_intent", "boot-b"),
            ("destination", "dest-a"),
            ("destination", "dest-b"),
            ("destination_lease", "lease-a"),
            ("destination_lease", "lease-b"),
            ("destination_promotion", "promotion-a"),
            ("destination_promotion", "promotion-b"),
            ("destination_audit", "audit-a"),
            ("destination_audit", "audit-b"),
        ] {
            let expected = u64::from(namespace == "destination");
            assert_eq!(
                before.control_revisions.get(&key(namespace, id)),
                Some(&expected)
            );
        }
        w.connection().execute_batch("BEGIN IMMEDIATE; UPDATE bootstrap_intents SET revision=revision+1 WHERE intent_id='boot-a'; UPDATE destinations SET revision=revision+1 WHERE destination_id='dest-b'; UPDATE destination_generation_leases SET revision=revision+1 WHERE lease_id='lease-a'; UPDATE destination_promotion_intents SET revision=revision+1 WHERE intent_id='promotion-b'; UPDATE destination_audits SET revision=revision+1 WHERE audit_id='audit-a';").unwrap();
        let during = snapshot(&p, "cfg", UNIX_EPOCH + Duration::from_secs(10)).unwrap();
        assert_eq!(during.control_revisions, before.control_revisions);
        w.connection().execute_batch("COMMIT;").unwrap();
        let after = snapshot(&p, "cfg", UNIX_EPOCH + Duration::from_secs(10)).unwrap();
        for (namespace, id, expected) in [
            ("bootstrap_intent", "boot-a", 1),
            ("bootstrap_intent", "boot-b", 0),
            ("destination", "dest-a", 1),
            ("destination", "dest-b", 2),
            ("destination_lease", "lease-a", 1),
            ("destination_lease", "lease-b", 0),
            ("destination_promotion", "promotion-a", 0),
            ("destination_promotion", "promotion-b", 1),
            ("destination_audit", "audit-a", 1),
            ("destination_audit", "audit-b", 0),
        ] {
            assert_eq!(
                after.control_revisions.get(&key(namespace, id)),
                Some(&expected)
            );
        }
        assert_ne!(before.snapshot_id, after.snapshot_id);
        w.connection()
            .execute(
                "DELETE FROM destination_audits WHERE audit_id='audit-b'",
                [],
            )
            .unwrap();
        w.connection().execute("INSERT INTO destination_audits(audit_id,destination_id,configuration_fingerprint,capture_epoch,generation,round_target_seq,round_identity_digest,journal_cursor_seq,self_cursor_seq,freshness_window_started_at,freshness_expires_at,contract_digest,incarnation) VALUES('audit-b','dest-b','cfg','epoch',1,0,'round',0,0,'unix:1','unix:2','contract',3)",[]).unwrap();
        w.connection().execute("INSERT INTO destination_current_audits(destination_id,audit_id,incarnation) VALUES('dest-b','audit-b',3)",[]).unwrap();
        let replaced = snapshot(&p, "cfg", UNIX_EPOCH + Duration::from_secs(10)).unwrap();
        assert!(
            !replaced
                .control_revisions
                .contains_key(&key("destination_audit", "audit-b"))
        );
        assert_eq!(
            replaced
                .control_revisions
                .get(&format!("destination_audit:{}", hash(&["audit-b", "3"]))),
            Some(&0)
        );
        drop(w);
        let restarted = snapshot(&p, "cfg", UNIX_EPOCH + Duration::from_secs(10)).unwrap();
        assert_eq!(restarted.control_revisions, replaced.control_revisions);
        let redacted = serde_json::to_string(&restarted).unwrap();
        assert!(!redacted.contains("boot-a") && !redacted.contains("audit-a"));
    }
    #[test]
    fn status_fails_closed_when_object_revision_projection_exceeds_its_bound() {
        let (p, w) = fixture();
        w.connection().execute_batch("WITH RECURSIVE ids(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM ids WHERE n<1001) INSERT INTO bootstrap_intents(intent_id,capture_epoch,source_system_id,database_id,slot_name,state,created_at) SELECT printf('boot-%04d',n),'epoch','system','database','slot','prepared','unix:1' FROM ids;").unwrap();
        assert!(matches!(
            snapshot(&p, "cfg", UNIX_EPOCH + Duration::from_secs(10)),
            Err(StatusError::TooManyControlObjects)
        ));
    }
    #[test]
    fn retained_audit_history_does_not_hide_current_status() {
        let (p, w) = fixture();
        w.connection().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('dest','clickhouse','cfg','epoch',1)",[]).unwrap();
        w.connection().execute_batch("WITH RECURSIVE ids(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM ids WHERE n<1001) INSERT INTO destination_audits(audit_id,destination_id,configuration_fingerprint,capture_epoch,generation,round_target_seq,round_identity_digest,journal_cursor_seq,self_cursor_seq,freshness_window_started_at,freshness_expires_at,contract_digest,incarnation) SELECT printf('audit-%04d',n),'dest','cfg','epoch',1,0,printf('round-%04d',n),0,0,printf('unix:%d',n),'unix:zzzz','contract',n FROM ids; INSERT INTO destination_current_audits(destination_id,audit_id,incarnation) VALUES('dest','audit-1001',1001);").unwrap();
        let status = snapshot(&p, "cfg", UNIX_EPOCH + Duration::from_secs(10)).unwrap();
        assert_eq!(
            status
                .control_revisions
                .iter()
                .filter(|(key, _)| key.starts_with("destination_audit:"))
                .count(),
            1
        );
        assert!(status.control_revisions.contains_key(&format!(
            "destination_audit:{}",
            hash(&["audit-1001", "1001"])
        )));
    }
    #[test]
    fn deterministic_unsupported_capture_failure_reports_capture_safe_stopped() {
        // Regression guard for the WAL-retention operator-visibility gap: a capture safe-stop
        // caused by an unsupported (deterministic, non-retryable) schema change must still be
        // surfaced as a non-healthy condition, since PostgreSQL keeps the replication slot
        // `active = true` and `restart_lsn` pinned the entire time, so nothing else warns the
        // operator that WAL is accruing.
        let (p, w) = fixture();
        w.connection().execute("INSERT INTO processing_failures(failure_id,component,failure_class,fingerprint,failed_boundary_start_seq,failed_boundary_end_seq,retry_class,attempt,next_retry_at,armed,first_failed_at,last_failed_at) VALUES('f','capture','unsupported','sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',2,3,'deterministic',1,NULL,1,'unix:1','unix:2')",[]).unwrap();
        drop(w);
        let s = snapshot(&p, "cfg", UNIX_EPOCH + Duration::from_secs(10)).unwrap();
        assert!(
            s.conditions
                .iter()
                .any(|c| c.condition == "capture_safe_stopped" && c.severity == "blocked"),
            "deterministic capture safe-stop must produce a blocked capture_safe_stopped condition, got: {:?}",
            s.conditions
        );
        assert_ne!(s.overall_health, "healthy");
        assert!(s.failures[0].retry_armed);
    }
    #[test]
    fn inventories_are_exact() {
        assert_eq!(
            CONDITION_NAMES.into_iter().collect::<BTreeSet<_>>().len(),
            14
        );
        assert_eq!(
            FaultHook::ALL
                .map(FaultHook::name)
                .into_iter()
                .collect::<BTreeSet<_>>()
                .len(),
            16
        )
    }
}
