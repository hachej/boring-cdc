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
    c.execute_batch("PRAGMA query_only=ON;PRAGMA foreign_keys=ON;")?;
    let s:Source=c.query_row("SELECT capture_epoch,source_system_id,database_id,slot_name,publication_fingerprint,durable_transaction_end_lsn,durable_journal_seq,last_feedback_lsn,slot_creation_floor_lsn,control_revision,observed_confirmed_flush_lsn,observed_restart_lsn FROM source_state WHERE singleton=1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?,r.get(9)?,r.get(10)?,r.get(11)?))).optional()?.ok_or(StatusError::MissingState)?;
    let owner:Option<Owner>=c.query_row("SELECT run_id,state,connection_generation,revision FROM runtime_ownership ORDER BY revision DESC,run_id DESC LIMIT 1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    let latest:Option<(String,String,String,Option<u64>)>=c.query_row("SELECT outcome,reason_code,created_at,unixepoch(created_at) FROM startup_reconciliations ORDER BY reconciliation_id DESC LIMIT 1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    let fact_seconds = latest.as_ref().and_then(|v| v.3).unwrap_or(0).min(now);
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
        "unknown"
    } else if now <= fact_seconds.saturating_add(FRESHNESS_SECONDS) {
        "fresh"
    } else {
        "stale"
    };
    let mut failures = Vec::new();
    let mut q=c.prepare("SELECT component,failure_class,fingerprint,first_failed_at,last_failed_at,attempt,next_retry_at,armed,failed_boundary_start_seq,failed_boundary_end_seq,retry_class FROM processing_failures ORDER BY component,fingerprint LIMIT 100")?;
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
            failed_xid: None,
            failed_final_lsn: None,
            observed_transaction_bytes: None,
            observed_transaction_events: None,
            limit_bytes: None,
            limit_events: None,
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
    if let Some((o, r, _, _)) = &latest {
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
    let mut aq = c.prepare("SELECT request_id,payload_digest,run_id,state,observation_revision,control_revision FROM operator_command_requests ORDER BY request_id LIMIT 100")?;
    for row in aq.query_map([], |r| Ok(json!({"request_id":r.get::<_,String>(0)?,"canonical_payload_digest":r.get::<_,String>(1)?,"plan_digest":Value::Null,"immutable_intent_id":Value::Null,"external_effect_evidence_digest":Value::Null,"postcondition_evidence_digest":Value::Null,"run_id":r.get::<_,String>(2)?,"terminal_state":r.get::<_,String>(3)?,"before_state_revision":r.get::<_,u64>(4)?,"bound_control_revision":r.get::<_,u64>(5)?})))? { action_causality.push(row?); }
    let mut control_revisions = BTreeMap::from([
        ("source".into(), s.9),
        ("ownership".into(), owner.as_ref().map(|x| x.3).unwrap_or(0)),
        (
            "bootstrap".into(),
            c.query_row(
                "SELECT coalesce(max(revision),0) FROM bootstrap_intents",
                [],
                |r| r.get::<_, u64>(0),
            )?,
        ),
        (
            "lease".into(),
            c.query_row(
                "SELECT coalesce(max(revision),0) FROM destination_generation_leases",
                [],
                |r| r.get::<_, u64>(0),
            )?,
        ),
        (
            "promotion".into(),
            c.query_row(
                "SELECT coalesce(max(revision),0) FROM destination_promotion_intents",
                [],
                |r| r.get::<_, u64>(0),
            )?,
        ),
    ]);
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
    let canon = json!({"source":[&s.0,&s.4,&s.5,&s.6,&s.7,&s.8,s.9,&s.10,&s.11],"ownership":&owner,"latest":&latest,"destinations":&dest,"failures":&failures,"conditions":stable_conditions,"control_revisions":&control_revisions,"action_causality":&action_causality,"config":config});
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
        drop(w);
        let a = snapshot(&p, "cfg", UNIX_EPOCH + Duration::from_secs(10)).unwrap();
        let b = snapshot(&p, "cfg", UNIX_EPOCH + Duration::from_secs(20)).unwrap();
        assert_eq!(a.snapshot_id, b.snapshot_id);
        assert_eq!(a.state_revision, b.state_revision);
        let t = serde_json::to_string(&a).unwrap();
        assert!(
            !t.contains("secret-system") && !t.contains("secret-db") && !t.contains("secret-slot")
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
    }
    #[test]
    fn freshness_expires_and_causal_records_exclude_payloads() {
        let (p, w) = fixture();
        w.connection().execute("INSERT INTO operator_command_requests(request_id,dry_run_nonce,canonical_payload,payload_digest,run_id,peer_identity,state,result,observation_revision,control_revision,expires_at) VALUES('request-safe','nonce-secret',x'0102','sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','run-safe','peer-safe','completed',x'03',7,2,'unix:99')",[]).unwrap();
        w.connection().execute("INSERT INTO startup_reconciliations(run_id,capture_epoch,outcome,reason_code,created_at) VALUES('old-run','epoch','ready','READY','2000-01-01T00:00:00Z')",[]).unwrap();
        drop(w);
        let s = snapshot(&p, "cfg", SystemTime::now() + Duration::from_secs(60)).unwrap();
        assert_eq!(s.freshness, "stale");
        assert_eq!(s.action_causality[0]["request_id"], "request-safe");
        let text = serde_json::to_string(&s).unwrap();
        assert!(!text.contains("nonce-secret") && !text.contains("0102"));
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
        assert!(
            s.control_revisions.contains_key("bootstrap")
                && s.control_revisions.contains_key("lease")
                && s.control_revisions.contains_key("promotion")
        );
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
