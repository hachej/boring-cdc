//! Owner-only recovery of a legacy exhausted transient destination failure.

use crate::failure_policy::{
    FailedBoundary, FailureClass, FailureRecord, PolicyEvent, PreparedFailureOperation,
    RearmAuthorizationToken, RearmRequest, load_failure, transition,
};
use crate::m1_cli_contract::{
    ActionPlan, BoundRevision, CliEnvelope, Confirmation, ConfirmationPolicy, MutationTerminal,
    MutationTrace, OperatorWireRequest, OperatorWireResponse,
};
use crate::m1_transition_kernel::{Randomness, TransitionContext, VirtualClock};
use crate::m2_schema::WriterConnection;
use chrono::{TimeZone, Utc};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Read;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Target {
    destination_id: String,
    capture_epoch: String,
    generation: u64,
    destination_revision: u64,
    failure_id: String,
    fingerprint: String,
    attempt: u32,
    first_seq: u64,
    last_seq: u64,
    last_failed_at_ms: u64,
    last_rearm_token_digest: Option<String>,
}

struct ZeroRandom;
impl Randomness for ZeroRandom {
    fn next_u64(&mut self) -> u64 {
        0
    }
}

fn error(code: &str) -> OperatorWireResponse {
    OperatorWireResponse::Error {
        schema_version: 1,
        code: code.into(),
        message: "destination re-arm unavailable or stale".into(),
    }
}

fn digest(value: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(value))
}

fn timestamp(ms: u64) -> Option<String> {
    Utc.timestamp_millis_opt(i64::try_from(ms).ok()?)
        .single()
        .map(|value| value.format("%Y-%m-%dT%H:%M:%SZ").to_string())
}

fn now_ms() -> Option<u64> {
    Some(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_millis() as u64,
    )
}

fn owner_is_current(owner: &WriterConnection, run_id: &str) -> bool {
    let mut time = std::mem::MaybeUninit::<libc::timespec>::uninit();
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, time.as_mut_ptr()) } != 0 {
        return false;
    }
    let time = unsafe { time.assume_init() };
    let Some(now) = u64::try_from(time.tv_sec)
        .ok()
        .and_then(|seconds| seconds.checked_mul(1000))
        .and_then(|millis| {
            u64::try_from(time.tv_nsec / 1_000_000)
                .ok()
                .and_then(|fraction| millis.checked_add(fraction))
        })
    else {
        return false;
    };
    owner.connection().query_row(
        "SELECT ownership_deadline_mono_ms FROM runtime_ownership WHERE run_id=?1 AND state='held'",
        [run_id], |row| row.get::<_,u64>(0),
    ).optional().ok().flatten().is_some_and(|deadline| now < deadline)
}

fn target(owner: &WriterConnection, destination_id: &str) -> Option<(Target, FailureRecord)> {
    let (epoch, generation, revision, failure_id, first, last):
        (String, u64, u64, String, u64, u64) = owner.connection().query_row(
        "SELECT d.capture_epoch,d.generation,d.revision,f.failure_id,f.failed_boundary_start_seq,f.failed_boundary_end_seq FROM destinations d JOIN processing_failures f ON f.failure_id=d.current_failure_id AND f.destination_id=d.destination_id WHERE d.destination_id=?1 AND d.kind='clickhouse' AND f.failure_class='transient_destination' AND f.retry_class='exhausted' AND f.next_retry_at IS NULL AND f.armed=1",
        [destination_id],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?)),
    ).optional().ok()??;
    let record = load_failure(
        owner.connection(),
        &failure_id,
        FailedBoundary::Destination {
            capture_epoch: epoch.clone(),
            generation,
            first_seq: first,
            last_seq: last,
        },
    )
    .ok()??;
    if record.class != FailureClass::TransientDestination
        || !record.armed
        || record.next_retry_at_ms.is_some()
    {
        return None;
    }
    Some((
        Target {
            destination_id: destination_id.into(),
            capture_epoch: epoch,
            generation,
            destination_revision: revision,
            failure_id,
            fingerprint: record.fingerprint.clone(),
            attempt: record.attempt,
            first_seq: first,
            last_seq: last,
            last_failed_at_ms: record.last_failed_at_ms,
            last_rearm_token_digest: record.last_rearm_token_digest.clone(),
        },
        record,
    ))
}

fn dry_run(owner: &mut WriterConnection, run_id: &str, argv: Vec<String>) -> OperatorWireResponse {
    if argv.len() < 4
        || argv[0] != "destination"
        || argv[1] != "rearm"
        || !argv.iter().any(|arg| arg == "--dry-run")
    {
        return error("CLI_OPERATOR_COMMAND_NOT_ROUTABLE");
    }
    let Some((target, _)) = target(owner, &argv[2]) else {
        return error("M2_DESTINATION_REARM_NOT_STUCK");
    };
    let Some(now) = now_ms() else {
        return error("M2_OPERATOR_CLOCK_UNAVAILABLE");
    };
    let Some(expires_at) = timestamp(now.saturating_add(300_000)) else {
        return error("M2_OPERATOR_CLOCK_UNAVAILABLE");
    };
    let mut random = [0u8; 24];
    if std::fs::File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut random))
        .is_err()
    {
        return error("M2_OPERATOR_RANDOM_UNAVAILABLE");
    }
    let nonce = random
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let Ok(payload) = serde_json::to_vec(&target) else {
        return error("M2_OPERATOR_PLAN_INVALID");
    };
    let payload_digest = digest(&payload);
    let request_id = digest(format!("destination-rearm:{nonce}:{payload_digest}").as_bytes());
    let plan_digest = digest(format!("{request_id}:{payload_digest}:{run_id}").as_bytes());
    let secret = digest(format!("{nonce}:{plan_digest}").as_bytes());
    let token = format!(
        "{}.{request_id}.{payload_digest}.{plan_digest}.{secret}",
        digest(target.destination_id.as_bytes())
    );
    let affected = format!("destination:{}", target.destination_id);
    let plan = ActionPlan {
        schema_version: 1,
        plan_id: request_id.clone(),
        plan_digest: plan_digest.clone(),
        request_id: request_id.clone(),
        canonical_payload_digest: payload_digest.clone(),
        command_id: "CMD-DESTINATION-REARM".into(),
        observed_snapshot_id: digest(&payload),
        observed_state_revision: target.destination_revision,
        bound_control_revisions: vec![BoundRevision {
            name: "destination_revision".into(),
            value: target.destination_revision.to_string(),
            owner_bead: "boring-cdc-m5-loops.1".into(),
        }],
        bound_fingerprints: BTreeMap::from([("failure".into(), target.fingerprint.clone())]),
        preconditions: vec!["current armed transient destination failure is exhausted".into()],
        current_predicates: vec![
            "same destination, epoch, generation, failed boundary, attempt and run".into(),
        ],
        intended_transitions: vec!["exhausted -> transient with immediate retry timestamp".into()],
        immutable_intents: vec![format!("rearm:{request_id}")],
        external_effects: vec![],
        resource_consequences: vec!["destination worker retries the failed batch".into()],
        continuity_consequences: vec!["journal and source cursor remain unchanged".into()],
        affected_objects: vec![affected],
        allowed_remaining_actions: vec![],
        rollback_boundary: "confirmation consumes the nonce; retry is idempotent".into(),
        expected_postconditions: vec!["destination failure remains armed and scheduled".into()],
        evidence_requirements: vec!["same failed boundary and destination checkpoint".into()],
        confirmation: ConfirmationPolicy {
            kind: Confirmation::DryRunThenConfirm,
            expires_at: Some(expires_at.clone()),
            canonical_argv: vec![
                "destination".into(),
                "rearm".into(),
                target.destination_id.clone(),
                "--confirm".into(),
            ],
            confirm_token: Some(token),
        },
        expires_at: expires_at.clone(),
        extensions: BTreeMap::new(),
    };
    let peer = format!("uid:{}", unsafe { libc::geteuid() });
    if owner.connection().execute(
        "INSERT INTO operator_command_requests(request_id,dry_run_nonce,canonical_payload,payload_digest,run_id,peer_identity,state,observation_revision,control_revision,expires_at,plan_digest,immutable_intent_id) VALUES(?1,?2,?3,?4,?5,?6,'accepted',?7,?7,?8,?9,?10)",
        params![request_id,nonce,payload,payload_digest,run_id,peer,target.destination_revision,expires_at,plan_digest,format!("rearm:{request_id}")],
    ).is_err() { return error("M2_DESTINATION_REARM_PLAN_STORE_FAILED"); }
    OperatorWireResponse::Plan {
        schema_version: 1,
        plan,
    }
}

fn apply(
    owner: &mut WriterConnection,
    run_id: &str,
    request_id: String,
    payload_digest: String,
    plan_digest: String,
    confirm_token: String,
) -> OperatorWireResponse {
    let row = owner.connection().query_row(
        "SELECT dry_run_nonce,canonical_payload,payload_digest,run_id,peer_identity,state,result,expires_at,plan_digest FROM operator_command_requests WHERE request_id=?1",
        [&request_id],
        |row| Ok((row.get::<_,String>(0)?,row.get::<_,Vec<u8>>(1)?,row.get::<_,String>(2)?,row.get::<_,String>(3)?,row.get::<_,String>(4)?,row.get::<_,String>(5)?,row.get::<_,Option<Vec<u8>>>(6)?,row.get::<_,String>(7)?,row.get::<_,String>(8)?)),
    ).optional();
    let Ok(Some((
        nonce,
        payload,
        stored_digest,
        stored_run,
        peer,
        state,
        result,
        expires,
        stored_plan,
    ))) = row
    else {
        return error("M2_DESTINATION_REARM_REQUEST_UNKNOWN");
    };
    let Ok(saved) = serde_json::from_slice::<Target>(&payload) else {
        return error("M2_DESTINATION_REARM_REQUEST_INVALID");
    };
    let expected_peer = format!("uid:{}", unsafe { libc::geteuid() });
    if stored_run != run_id
        || peer != expected_peer
        || stored_digest != payload_digest
        || stored_plan != plan_digest
        || digest(&payload) != payload_digest
        || confirm_token
            != format!(
                "{}.{request_id}.{payload_digest}.{plan_digest}.{}",
                digest(saved.destination_id.as_bytes()),
                digest(format!("{nonce}:{plan_digest}").as_bytes())
            )
    {
        return error("M2_DESTINATION_REARM_CONFIRMATION_INVALID");
    }
    if state == "completed" {
        return result
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .map(|result| OperatorWireResponse::Result {
                schema_version: 1,
                result,
            })
            .unwrap_or_else(|| error("M2_DESTINATION_REARM_RESULT_INVALID"));
    }
    let Some(now) = now_ms() else {
        return error("M2_OPERATOR_CLOCK_UNAVAILABLE");
    };
    if state != "accepted" || timestamp(now).is_none_or(|now| now > expires) {
        return error("M2_DESTINATION_REARM_EXPIRED");
    }
    let Some((current, record)) = target(owner, &saved.destination_id) else {
        return error("M2_DESTINATION_REARM_STALE");
    };
    if current != saved {
        return error("M2_DESTINATION_REARM_STALE");
    }
    let clock = VirtualClock::new(now.max(record.last_failed_at_ms.saturating_add(1)));
    let mut random = ZeroRandom;
    let mut context = TransitionContext {
        clock: &clock,
        randomness: &mut random,
    };
    let action = transition(
        Some(&record),
        PolicyEvent::Rearm(RearmRequest {
            expected_failure_id: record.failure_id.clone(),
            expected_fingerprint: record.fingerprint.clone(),
            authorization_token: RearmAuthorizationToken {
                token_id: request_id.clone(),
                expected_last_rearm_token_digest: record.last_rearm_token_digest.clone(),
            },
            relevant_configuration_change: None,
            retained_wal_proven: false,
            integrity_recovery_proven: false,
            continuity_recovery_proven: false,
            explicit_operator_authorization: true,
        }),
        &mut context,
    );
    let Some(operation) = PreparedFailureOperation::from_policy_action(action, None) else {
        return error("M2_DESTINATION_REARM_REJECTED");
    };
    let Ok(tx) = owner.connection_mut().transaction() else {
        return error("M2_DESTINATION_REARM_WRITE_FAILED");
    };
    if operation.execute(&tx).is_err() {
        return error("M2_DESTINATION_REARM_WRITE_FAILED");
    }
    let postcondition = tx.query_row(
        "SELECT retry_class,attempt,next_retry_at,armed,failed_boundary_start_seq,failed_boundary_end_seq FROM processing_failures WHERE failure_id=?1",
        [&saved.failure_id],
        |row| Ok((row.get::<_,String>(0)?,row.get::<_,u32>(1)?,row.get::<_,Option<String>>(2)?,row.get::<_,bool>(3)?,row.get::<_,u64>(4)?,row.get::<_,u64>(5)?)),
    );
    let Ok((retry_class, attempt, next_retry_at, armed, first_seq, last_seq)) = postcondition
    else {
        return error("M2_DESTINATION_REARM_POSTCONDITION_FAILED");
    };
    if retry_class != "transient"
        || attempt != saved.attempt
        || next_retry_at.is_none()
        || !armed
        || first_seq != saved.first_seq
        || last_seq != saved.last_seq
    {
        return error("M2_DESTINATION_REARM_POSTCONDITION_FAILED");
    }
    let postcondition_bytes = serde_json::to_vec(&json!({
        "failure_id":saved.failure_id,"retry_class":retry_class,"attempt":attempt,
        "next_retry_at":next_retry_at,"armed":armed,"first_seq":first_seq,"last_seq":last_seq,
    }))
    .expect("postcondition values serialize");
    let evidence_digest = digest(&postcondition_bytes);
    let result = CliEnvelope {
        schema_version: 1,
        command: "CMD-DESTINATION-REARM".into(),
        outcome: "success".into(),
        code: "M2_DESTINATION_REARMED".into(),
        message: "destination retry scheduled".into(),
        request_id: Some(request_id.clone()),
        run_id: Some(run_id.into()),
        capture_epoch: Some(saved.capture_epoch.clone()),
        condition: None,
        runbook_id: None,
        data: json!({"destination_id":saved.destination_id,"failure_id":saved.failure_id,"attempt":saved.attempt}),
        warnings: vec![],
        next_commands: vec![],
        plan_digest: Some(plan_digest.clone()),
        postcondition_evidence_digest: Some(evidence_digest.clone()),
        mutation_trace: Some(MutationTrace {
            before_snapshot_id: digest(&payload),
            plan_digest: plan_digest.clone(),
            immutable_intent_id: format!("rearm:{request_id}"),
            external_effect_evidence_digest: None,
            terminal: MutationTerminal::AfterSnapshot {
                after_snapshot_id: evidence_digest,
            },
        }),
    };
    let Ok(bytes) = serde_json::to_vec(&result) else {
        return error("M2_DESTINATION_REARM_RESULT_INVALID");
    };
    if tx.execute(
        "UPDATE operator_command_requests SET state='completed',result=?1,postcondition_evidence_digest=?2,request_revision=request_revision+1 WHERE request_id=?3 AND state='accepted' AND run_id=?4",
        params![bytes,result.postcondition_evidence_digest,request_id,run_id],
    ).ok() != Some(1) || tx.commit().is_err() { return error("M2_DESTINATION_REARM_WRITE_FAILED"); }
    OperatorWireResponse::Result {
        schema_version: 1,
        result,
    }
}

pub fn handle(
    owner: &mut WriterConnection,
    run_id: &str,
    request: OperatorWireRequest,
) -> OperatorWireResponse {
    if !owner_is_current(owner, run_id) {
        return error("M2_DESTINATION_REARM_OWNER_STALE");
    }
    match request {
        OperatorWireRequest::Invoke {
            expected_run_id,
            argv,
            ..
        } if expected_run_id == run_id => dry_run(owner, run_id, argv),
        OperatorWireRequest::Apply {
            expected_run_id,
            request_id,
            canonical_payload_digest,
            plan_digest,
            confirm_token,
            ..
        } if expected_run_id == run_id => apply(
            owner,
            run_id,
            request_id,
            canonical_payload_digest,
            plan_digest,
            confirm_token,
        ),
        _ => error("M2_DESTINATION_REARM_RUN_STALE"),
    }
}
