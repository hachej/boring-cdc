//! Read-only lookup of durable operator results by the live owner.

use crate::m1_cli_contract::CliEnvelope;
use crate::m2_schema::WriterConnection;
use rusqlite::{OptionalExtension, params};

#[derive(Debug, PartialEq)]
pub enum StoredResult {
    Pending,
    Terminal(Box<CliEnvelope>),
    AbortedByRestart,
}

#[derive(Debug, PartialEq)]
pub enum ResultLookupError {
    Stale,
    Corrupt,
    Store,
}

struct RequestRow {
    run_id: String,
    peer: String,
    digest: String,
    state: String,
    id: String,
    result: Option<Vec<u8>>,
    plan_digest: Option<String>,
}

fn valid_field(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}

/// A result request never dispatches an action. The current owner and all persisted request
/// identity fields must match before any terminal bytes are returned to the socket.
pub fn lookup_result(
    writer: &WriterConnection,
    expected_run_id: &str,
    request_id: &str,
    canonical_payload_digest: &str,
    peer_identity: &str,
    now_mono_ms: u64,
) -> Result<StoredResult, ResultLookupError> {
    if [
        expected_run_id,
        request_id,
        canonical_payload_digest,
        peer_identity,
    ]
    .iter()
    .any(|value| !valid_field(value))
    {
        return Err(ResultLookupError::Stale);
    }
    let now = i64::try_from(now_mono_ms).map_err(|_| ResultLookupError::Stale)?;
    let generation = i64::try_from(writer.attestation().connection_generation)
        .map_err(|_| ResultLookupError::Stale)?;
    let owner_held: bool = writer
        .connection()
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM runtime_ownership WHERE run_id=?1 AND connection_generation=?2 AND state='held' AND ownership_deadline_mono_ms>=?3)",
            params![writer.attestation().run_id, generation, now],
            |row| row.get(0),
        )
        .map_err(|_| ResultLookupError::Store)?;
    if !owner_held {
        return Err(ResultLookupError::Stale);
    }
    let stored: Option<RequestRow> = writer
        .connection()
        .query_row(
            "SELECT run_id,peer_identity,payload_digest,state,request_id,result,plan_digest FROM operator_command_requests WHERE request_id=?1",
            [request_id],
            |row| {
                Ok(RequestRow {
                    run_id: row.get(0)?,
                    peer: row.get(1)?,
                    digest: row.get(2)?,
                    state: row.get(3)?,
                    id: row.get(4)?,
                    result: row.get(5)?,
                    plan_digest: row.get(6)?,
                })
            },
        )
        .optional()
        .map_err(|_| ResultLookupError::Store)?;
    let Some(stored) = stored else {
        return Err(ResultLookupError::Stale);
    };
    if stored.run_id != expected_run_id
        || stored.peer != peer_identity
        || stored.digest != canonical_payload_digest
        || stored.id != request_id
    {
        return Err(ResultLookupError::Stale);
    }
    match (stored.state.as_str(), stored.result) {
        ("accepted" | "reconciling", None) => Ok(StoredResult::Pending),
        ("aborted_by_restart", None) => Ok(StoredResult::AbortedByRestart),
        ("completed" | "failed", Some(bytes)) => {
            let envelope: CliEnvelope =
                serde_json::from_slice(&bytes).map_err(|_| ResultLookupError::Corrupt)?;
            if envelope.schema_version != 1
                || envelope.request_id.as_deref() != Some(request_id)
                || envelope.run_id.as_deref() != Some(expected_run_id)
                || stored.plan_digest.is_some() && envelope.plan_digest != stored.plan_digest
            {
                return Err(ResultLookupError::Corrupt);
            }
            Ok(StoredResult::Terminal(Box::new(envelope)))
        }
        _ => Err(ResultLookupError::Corrupt),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m2_schema::open_writer;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn terminal_result_requires_exact_request_and_live_owner() {
        let path = std::env::temp_dir().join(format!(
            "m2-operator-result-{}-{}.sqlite",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let writer = open_writer(&path, "new-run", 2, 1).unwrap();
        writer.connection().execute("INSERT INTO runtime_ownership(run_id,backend_pid,connection_nonce,ownership_deadline_mono_ms,state,connection_generation) VALUES('new-run',1,'nonce',100,'held',2)",[]).unwrap();
        let envelope = CliEnvelope {
            schema_version: 1,
            command: "backfill pause".into(),
            outcome: "success".into(),
            code: "M3_PAUSED".into(),
            message: "backfill paused".into(),
            request_id: Some("request".into()),
            run_id: Some("old-run".into()),
            capture_epoch: None,
            condition: None,
            runbook_id: None,
            data: serde_json::Value::Null,
            warnings: Vec::new(),
            next_commands: Vec::new(),
            plan_digest: Some("plan".into()),
            postcondition_evidence_digest: None,
            mutation_trace: None,
        };
        let bytes = serde_json::to_vec(&envelope).unwrap();
        writer.connection().execute("INSERT INTO operator_command_requests(request_id,dry_run_nonce,canonical_payload,payload_digest,run_id,peer_identity,state,result,observation_revision,control_revision,expires_at,plan_digest) VALUES('request','dry-nonce',X'01','payload','old-run','uid:1000','completed',?1,1,1,'later','plan')",[bytes]).unwrap();
        assert_eq!(
            lookup_result(&writer, "old-run", "request", "payload", "uid:1000", 10),
            Ok(StoredResult::Terminal(Box::new(envelope)))
        );
        assert_eq!(
            lookup_result(&writer, "old-run", "request", "changed", "uid:1000", 10),
            Err(ResultLookupError::Stale)
        );
        assert_eq!(
            lookup_result(&writer, "old-run", "request", "payload", "uid:1001", 10),
            Err(ResultLookupError::Stale)
        );
        assert_eq!(
            lookup_result(&writer, "old-run", "request", "payload", "uid:1000", 101),
            Err(ResultLookupError::Stale)
        );
        writer.connection().execute("INSERT INTO operator_command_requests(request_id,dry_run_nonce,canonical_payload,payload_digest,run_id,peer_identity,state,observation_revision,control_revision,expires_at) VALUES('pending','pending-nonce',X'02','payload','old-run','uid:1000','accepted',1,1,'later')",[]).unwrap();
        assert_eq!(
            lookup_result(&writer, "old-run", "pending", "payload", "uid:1000", 10),
            Ok(StoredResult::Pending)
        );
        writer.connection().execute("UPDATE operator_command_requests SET state='aborted_by_restart',request_revision=request_revision+1 WHERE request_id='pending'",[]).unwrap();
        assert_eq!(
            lookup_result(&writer, "old-run", "pending", "payload", "uid:1000", 10),
            Ok(StoredResult::AbortedByRestart)
        );
        writer.connection().execute("INSERT INTO operator_command_requests(request_id,dry_run_nonce,canonical_payload,payload_digest,run_id,peer_identity,state,result,observation_revision,control_revision,expires_at) VALUES('corrupt','corrupt-nonce',X'03','payload','old-run','uid:1000','completed',X'00',1,1,'later')",[]).unwrap();
        assert_eq!(
            lookup_result(&writer, "old-run", "corrupt", "payload", "uid:1000", 10),
            Err(ResultLookupError::Corrupt)
        );
        drop(writer);
        let _ = std::fs::remove_file(path);
    }
}
