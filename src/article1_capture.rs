//! Article 1's deliberately small live logical-replication boundary.
//!
//! The selected client is used only for connection setup, `START_REPLICATION`, and
//! receiving CopyBoth payloads. Every payload is decoded by [`crate::m1_decoder::Decoder`].
//! This module sends no standby-status packet, performs no retry, and persists nothing.

use crate::m1_decoder::{
    CopyBothEvent, Decoder, OldTupleKind, PgoutputEvent, RelationContract, RowKind, TupleValue,
    WireLimits,
};
use pg_walstream::{CancellationToken, PgReplicationConnection};
use serde_json::{Value, json};
use std::fmt;

// ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol
pub const POSTGRES_VERSION_NUM: i32 = 170_006;
// ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol
pub const PUBLICATION: &str = "article1_publication";
// ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol
pub const SLOT: &str = "article1_slot";
// ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol
pub const PROTO_VERSION: &str = "1";
// ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol
pub const ORIGIN: &str = "any";
// ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol
pub const STREAMING: &str = "false";
// ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol
pub const TWO_PHASE: &str = "false";
// ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol
pub const BINARY: &str = "false";

const START_OPTIONS: [(&str, &str); 6] = [
    ("proto_version", PROTO_VERSION), // ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol
    ("publication_names", PUBLICATION), // ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol
    ("origin", ORIGIN),               // ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol
    ("streaming", STREAMING),         // ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol
    ("two_phase", TWO_PHASE),         // ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol
    ("binary", BINARY),               // ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureFailure {
    pub boundary: &'static str,
    pub code: &'static str,
}

impl CaptureFailure {
    fn at(boundary: &'static str, code: &'static str) -> Self {
        Self { boundary, code }
    }
}

impl fmt::Display for CaptureFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.boundary, self.code)
    }
}

impl std::error::Error for CaptureFailure {}

/// Configuration intentionally permits only connection location and a caller-owned finite stop.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureConfig {
    pub dsn: String,
    pub stop_after_commits: usize,
}

impl CaptureConfig {
    pub fn article1(
        dsn: impl Into<String>,
        stop_after_commits: usize,
    ) -> Result<Self, CaptureFailure> {
        let dsn = dsn.into();
        if dsn.trim().is_empty()
            || dsn.contains("replication=")
            || dsn.contains('\n')
            || stop_after_commits == 0
        {
            return Err(CaptureFailure::at(
                "configuration",
                "ARTICLE1_CONFIG_INVALID",
            ));
        }
        Ok(Self {
            dsn,
            stop_after_commits,
        })
    }
}

/// Capture complete transactions as stable JSONL, stopping after `stop_after_commits`.
///
/// This function deliberately has no retry loop and never calls a feedback API.
pub fn capture_jsonl(config: &CaptureConfig) -> Result<Vec<String>, CaptureFailure> {
    preflight(config)?;
    let replication_dsn = if config.dsn.contains('?') {
        format!("{}&replication=database", config.dsn)
    } else {
        format!("{}?replication=database", config.dsn)
    };
    let mut connection = PgReplicationConnection::connect(&replication_dsn)
        .map_err(|error| classify_connection(&error.to_string()))?;
    if connection.server_version() != POSTGRES_VERSION_NUM {
        return Err(CaptureFailure::at(
            "server_version",
            "ARTICLE1_VERSION_MISMATCH",
        ));
    }
    connection
        .start_replication(SLOT, 0, &START_OPTIONS)
        .map_err(|_| {
            CaptureFailure::at("start_replication", "ARTICLE1_START_REPLICATION_FAILED")
        })?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| CaptureFailure::at("connection", "ARTICLE1_RUNTIME_FAILED"))?;
    let cancellation = CancellationToken::new();
    let mut decoder = Decoder::new(WireLimits::default());
    let mut lines = Vec::new();
    let mut commits = 0usize;

    while commits < config.stop_after_commits {
        let frame = runtime
            .block_on(connection.get_copy_data_async(&cancellation))
            .map_err(|_| CaptureFailure::at("connection", "ARTICLE1_COPYBOTH_DISCONNECTED"))?;
        let decoded = decoder.decode_copy_data(&frame).map_err(|_error| {
            #[cfg(test)]
            eprintln!("decoder fingerprint={}", _error.fingerprint);
            CaptureFailure::at("protocol", "ARTICLE1_PROTOCOL_REJECTED")
        })?;
        match decoded {
            CopyBothEvent::Keepalive { .. } => {
                // Strict Article 1 rule: observe and send no feedback, even when requested.
            }
            CopyBothEvent::XLogData {
                wal_start,
                wal_end,
                event,
                ..
            } => match event {
                PgoutputEvent::RelationNeedsValidation(relation) => decoder
                    .admit_relation(RelationContract {
                        key_columns: relation
                            .columns
                            .iter()
                            .enumerate()
                            .filter_map(|(index, column)| column.key.then_some(index))
                            .collect(),
                        relation,
                        control: None,
                    })
                    .map_err(|_| CaptureFailure::at("protocol", "ARTICLE1_RELATION_REJECTED"))?,
                PgoutputEvent::RelationMetadata(_) | PgoutputEvent::Origin { .. } => {}
                event => {
                    let commit = matches!(event, PgoutputEvent::Commit { .. });
                    lines.push(render(wal_start, wal_end, event)?);
                    if commit {
                        commits += 1;
                    }
                }
            },
        }
    }
    Ok(lines)
}

fn preflight(config: &CaptureConfig) -> Result<(), CaptureFailure> {
    let mut connection = PgReplicationConnection::connect(&config.dsn)
        .map_err(|error| classify_connection(&error.to_string()))?;
    if connection.server_version() != POSTGRES_VERSION_NUM {
        return Err(CaptureFailure::at(
            "server_version",
            "ARTICLE1_VERSION_MISMATCH",
        ));
    }
    let publication_query = format!(
        "SELECT pubinsert::int || ',' || pubupdate::int || ',' || pubdelete::int || ',' || pubtruncate::int FROM pg_publication WHERE pubname = '{PUBLICATION}'"
    );
    let publication = connection
        .exec(&publication_query)
        .map_err(|_| CaptureFailure::at("publication", "ARTICLE1_PUBLICATION_QUERY_FAILED"))?;
    if publication.ntuples() != 1 || publication.get_value(0, 0).as_deref() != Some("1,1,1,0") {
        return Err(CaptureFailure::at(
            "publication",
            "ARTICLE1_PUBLICATION_MISMATCH",
        ));
    }
    let slot_query = format!(
        "SELECT plugin || ',' || slot_type || ',' || database || ',' || active::int, (restart_lsn IS NOT NULL)::int FROM pg_replication_slots WHERE slot_name = '{SLOT}'"
    );
    let slot = connection
        .exec(&slot_query)
        .map_err(|_| CaptureFailure::at("slot", "ARTICLE1_SLOT_QUERY_FAILED"))?;
    let expected = connection
        .exec("SELECT 'pgoutput,logical,' || current_database() || ',0'")
        .map_err(|_| CaptureFailure::at("slot", "ARTICLE1_SLOT_QUERY_FAILED"))?
        .get_value(0, 0);
    if slot.ntuples() != 1 || slot.get_value(0, 0) != expected {
        return Err(CaptureFailure::at("slot", "ARTICLE1_SLOT_MISMATCH"));
    }
    if slot.get_value(0, 1).as_deref() != Some("1") {
        return Err(CaptureFailure::at(
            "wal_continuity",
            "ARTICLE1_CONTINUITY_UNAVAILABLE",
        ));
    }
    Ok(())
}

fn classify_connection(message: &str) -> CaptureFailure {
    if message.to_ascii_lowercase().contains("authentication") {
        CaptureFailure::at("authentication", "ARTICLE1_AUTH_FAILED")
    } else {
        CaptureFailure::at("connection", "ARTICLE1_CONNECTION_FAILED")
    }
}

fn render(wal_start: u64, wal_end: u64, event: PgoutputEvent) -> Result<String, CaptureFailure> {
    let body = match event {
        PgoutputEvent::Begin {
            final_lsn,
            commit_time,
            xid,
        } => {
            json!({"event":"BEGIN","final_lsn":lsn(final_lsn),"transaction":{"commit_time":commit_time,"xid":xid},"wal_end":lsn(wal_end),"wal_start":lsn(wal_start)})
        }
        PgoutputEvent::Row(row) => json!({
            "event": match row.kind { RowKind::Insert => "INSERT", RowKind::Update => "UPDATE", RowKind::Delete => "DELETE" },
            "new": tuple(row.new.as_deref())?,
            "old": tuple(row.old.as_deref())?,
            "old_state": match row.old_kind { None => "absent", Some(OldTupleKind::Key) => "key", Some(OldTupleKind::Full) => "full" },
            "relation_id": row.relation_id,
            "transaction":{"ordinal":row.ordinal,"xid":row.xid},
            "wal_end":lsn(wal_end),"wal_start":lsn(wal_start)
        }),
        PgoutputEvent::Commit {
            commit_lsn,
            commit_time,
            end_lsn,
            row_count,
            ..
        } => {
            json!({"commit_lsn":lsn(commit_lsn),"end_lsn":lsn(end_lsn),"event":"COMMIT","row_count":row_count,"transaction":{"commit_time":commit_time},"wal_end":lsn(wal_end),"wal_start":lsn(wal_start)})
        }
        _ => return Err(CaptureFailure::at("protocol", "ARTICLE1_EVENT_UNEXPECTED")),
    };
    serde_json::to_string(&body)
        .map_err(|_| CaptureFailure::at("protocol", "ARTICLE1_JSON_ENCODING_FAILED"))
}

fn tuple(values: Option<&[TupleValue]>) -> Result<Value, CaptureFailure> {
    match values {
        None => Ok(Value::Null),
        Some(values) => values
            .iter()
            .map(|value| match value {
                TupleValue::Null => Ok(Value::Null),
                TupleValue::UnchangedToast => Ok(json!({"unchanged_toast":true})),
                TupleValue::Text(bytes) => std::str::from_utf8(bytes)
                    .map(|text| Value::String(text.to_owned()))
                    .map_err(|_| CaptureFailure::at("protocol", "ARTICLE1_TEXT_UTF8_INVALID")),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
    }
}

fn lsn(value: u64) -> String {
    format!("{:X}/{:X}", value >> 32, value & 0xffff_ffff)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_options_and_fail_closed_config_are_stable() {
        assert_eq!(
            START_OPTIONS,
            [
                ("proto_version", "1"),
                ("publication_names", "article1_publication"),
                ("origin", "any"),
                ("streaming", "false"),
                ("two_phase", "false"),
                ("binary", "false"),
            ]
        );
        assert_eq!(
            CaptureConfig::article1("", 1).unwrap_err().code,
            "ARTICLE1_CONFIG_INVALID"
        );
        assert_eq!(
            CaptureConfig::article1("postgresql://x/y?replication=database", 1)
                .unwrap_err()
                .boundary,
            "configuration"
        );
        assert_eq!(
            CaptureConfig::article1("postgresql://x/y", 0)
                .unwrap_err()
                .code,
            "ARTICLE1_CONFIG_INVALID"
        );
    }

    #[test]
    fn connection_and_authentication_are_distinct_redacted_boundaries() {
        assert_eq!(
            classify_connection("password authentication failed").code,
            "ARTICLE1_AUTH_FAILED"
        );
        assert_eq!(
            classify_connection("tcp refused").code,
            "ARTICLE1_CONNECTION_FAILED"
        );
    }

    /// Real-fixture proof only; run explicitly with `ARTICLE1_DSN`.
    #[test]
    #[ignore = "requires the pinned PostgreSQL 17.6 Article 1 fixture"]
    fn live_pg17_copyboth_insert_update_delete_uses_m1_decoder() {
        let dsn = std::env::var("ARTICLE1_DSN").expect("ARTICLE1_DSN is required");
        PgReplicationConnection::connect(&dsn)
            .unwrap_or_else(|error| panic!("fixture connection failed: {error}"));
        let mutation_dsn = dsn.clone();
        let mutation = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(750));
            let mut connection = PgReplicationConnection::connect(&mutation_dsn).unwrap();
            connection.exec("BEGIN; INSERT INTO customers VALUES (9001, 'Live', 1); UPDATE customers SET tier = 2 WHERE id = 9001; DELETE FROM customers WHERE id = 9001; COMMIT;").unwrap();
        });
        let lines = capture_jsonl(&CaptureConfig::article1(dsn, 1).unwrap()).unwrap();
        mutation.join().unwrap();
        let events = lines
            .iter()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            events
                .iter()
                .map(|event| event["event"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["BEGIN", "INSERT", "UPDATE", "DELETE", "COMMIT"]
        );
        assert_eq!(events[1]["old_state"], "absent");
        assert_eq!(events[2]["old_state"], "absent");
        assert_eq!(events[3]["old_state"], "key");
        assert_eq!(events[4]["row_count"], 3);
        assert!(events.iter().all(|event| event["wal_start"].is_string()));
        for line in lines {
            println!("{line}");
        }
    }
}
