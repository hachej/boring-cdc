//! Article 1's deliberately small live logical-replication boundary.
//!
//! The selected client is used only for connection setup, `START_REPLICATION`, and
//! receiving CopyBoth payloads. Every payload is decoded by [`crate::m1_decoder::Decoder`].
//! This module sends no standby-status packet, performs no retry, and persists nothing.

use crate::article1_row_view::{Article1RowView, RowViewChange};
use crate::m1_decoder::{
    Column, CopyBothEvent, Decoder, OldTupleKind, PgoutputEvent, Relation, RelationContract,
    RowKind, TupleValue, WireLimits,
};
use pg_walstream::{CancellationToken, PgReplicationConnection};
use serde_json::{Value, json};
use std::collections::BTreeMap;
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
// ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol
pub const TABLES_CSV: &str = "public.customers,public.order_items,public.orders,public.products";

#[derive(Clone, Copy)]
struct Expectations<'a> {
    version: i32,
    publication: &'a str,
    slot: &'a str,
    tables_csv: &'a str,
    continuity_available: bool,
}

const PROVISIONAL_EXPECTATIONS: Expectations<'static> = Expectations {
    version: POSTGRES_VERSION_NUM,
    publication: PUBLICATION,
    slot: SLOT,
    tables_csv: TABLES_CSV,
    continuity_available: true,
};

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

/// Stream stable JSONL through a caller-owned sink, stopping after `stop_after_commits`.
///
/// Setup runs on Tokio's blocking pool, the caller owns cancellation, and rows are emitted
/// incrementally. This function has no retry loop and never calls a feedback API.
pub async fn capture_jsonl<F>(
    config: &CaptureConfig,
    cancellation: &CancellationToken,
    mut emit: F,
) -> Result<usize, CaptureFailure>
where
    F: FnMut(String) -> Result<(), CaptureFailure>,
{
    let owned = config.clone();
    let (mut connection, contracts) =
        tokio::task::spawn_blocking(move || setup(&owned))
            .await
            .map_err(|_| CaptureFailure::at("connection", "ARTICLE1_SETUP_TASK_FAILED"))??;
    let mut decoder = Decoder::new(WireLimits::default());
    let mut row_view = Article1RowView::new(&contracts);
    let mut commits = 0usize;

    while commits < config.stop_after_commits {
        if cancellation.is_cancelled() {
            return Err(CaptureFailure::at("connection", "ARTICLE1_CANCELLED"));
        }
        let frame = connection
            .get_copy_data_async(cancellation)
            .await
            .map_err(|_| CaptureFailure::at("connection", "ARTICLE1_COPYBOTH_DISCONNECTED"))?;
        let decoded = decode_frame(&mut decoder, &frame)?;
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
                PgoutputEvent::RelationNeedsValidation(relation) => {
                    let contract = contracts.get(&relation.id).ok_or_else(|| {
                        CaptureFailure::at("publication", "ARTICLE1_RELATION_NOT_PUBLISHED")
                    })?;
                    if contract.relation != relation {
                        return Err(CaptureFailure::at(
                            "protocol",
                            "ARTICLE1_RELATION_SCHEMA_MISMATCH",
                        ));
                    }
                    decoder
                        .admit_relation(contract.clone())
                        .map_err(|_| CaptureFailure::at("protocol", "ARTICLE1_RELATION_REJECTED"))?
                }
                PgoutputEvent::RelationMetadata(_) | PgoutputEvent::Origin { .. } => {}
                event => {
                    let commit = matches!(event, PgoutputEvent::Commit { .. });
                    let view_change = match &event {
                        PgoutputEvent::Row(row) => {
                            Some(row_view.apply(row).map_err(|failure| {
                                CaptureFailure::at("article1_row_view", failure.code)
                            })?)
                        }
                        _ => None,
                    };
                    emit(render(wal_start, wal_end, &event, view_change.as_ref())?)?;
                    if commit {
                        commits += 1;
                    }
                }
            },
        }
    }
    Ok(commits)
}

fn setup(
    config: &CaptureConfig,
) -> Result<(PgReplicationConnection, BTreeMap<u32, RelationContract>), CaptureFailure> {
    let contracts = preflight(config, &PROVISIONAL_EXPECTATIONS)?;
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
    Ok((connection, contracts))
}

fn preflight(
    config: &CaptureConfig,
    expected: &Expectations<'_>,
) -> Result<BTreeMap<u32, RelationContract>, CaptureFailure> {
    let mut connection = PgReplicationConnection::connect(&config.dsn)
        .map_err(|error| classify_connection(&error.to_string()))?;
    if connection.server_version() != expected.version {
        return Err(CaptureFailure::at(
            "server_version",
            "ARTICLE1_VERSION_MISMATCH",
        ));
    }
    let publication_query = format!(
        "SELECT pubinsert::int || ',' || pubupdate::int || ',' || pubdelete::int || ',' || pubtruncate::int FROM pg_publication WHERE pubname = '{}'",
        expected.publication
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
    let membership = connection
        .exec(&format!("SELECT string_agg(schemaname || '.' || tablename, ',' ORDER BY schemaname,tablename) FROM pg_publication_tables WHERE pubname = '{}'", expected.publication))
        .map_err(|_| CaptureFailure::at("publication", "ARTICLE1_PUBLICATION_QUERY_FAILED"))?;
    if membership.get_value(0, 0).as_deref() != Some(expected.tables_csv) {
        return Err(CaptureFailure::at(
            "publication",
            "ARTICLE1_PUBLICATION_TABLES_MISMATCH",
        ));
    }
    let slot_query = format!(
        "SELECT plugin || ',' || slot_type || ',' || database || ',' || active::int, (restart_lsn IS NOT NULL AND invalidation_reason IS NULL AND wal_status IN ('reserved','extended'))::int FROM pg_replication_slots WHERE slot_name = '{}'",
        expected.slot
    );
    let slot = connection
        .exec(&slot_query)
        .map_err(|_| CaptureFailure::at("slot", "ARTICLE1_SLOT_QUERY_FAILED"))?;
    let expected_slot_identity = connection
        .exec("SELECT 'pgoutput,logical,' || current_database() || ',0'")
        .map_err(|_| CaptureFailure::at("slot", "ARTICLE1_SLOT_QUERY_FAILED"))?
        .get_value(0, 0);
    if slot.ntuples() != 1 || slot.get_value(0, 0) != expected_slot_identity {
        return Err(CaptureFailure::at("slot", "ARTICLE1_SLOT_MISMATCH"));
    }
    if (slot.get_value(0, 1).as_deref() == Some("1")) != expected.continuity_available {
        return Err(CaptureFailure::at(
            "wal_continuity",
            "ARTICLE1_CONTINUITY_UNAVAILABLE",
        ));
    }

    let catalog = connection
        .exec(&format!("SELECT c.oid::text,n.nspname,c.relname,c.relreplident,a.attname,a.atttypid::text,a.atttypmod::text,(c.relreplident='f' OR EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid=c.oid AND (i.indisreplident OR (c.relreplident='d' AND i.indisprimary)) AND a.attnum=ANY(i.indkey)))::int::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_publication_tables p ON p.schemaname=n.nspname AND p.tablename=c.relname JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped WHERE p.pubname='{}' ORDER BY c.oid,a.attnum", expected.publication))
        .map_err(|_| CaptureFailure::at("publication", "ARTICLE1_CATALOG_QUERY_FAILED"))?;
    let mut relations: BTreeMap<u32, Relation> = BTreeMap::new();
    for row in 0..catalog.ntuples() {
        let value = |column| {
            catalog
                .get_value(row, column)
                .ok_or_else(|| CaptureFailure::at("protocol", "ARTICLE1_CATALOG_VALUE_INVALID"))
        };
        let id = value(0)?
            .parse()
            .map_err(|_| CaptureFailure::at("protocol", "ARTICLE1_CATALOG_VALUE_INVALID"))?;
        let relation = relations.entry(id).or_insert_with(|| Relation {
            id,
            namespace: value(1).unwrap_or_default(),
            name: value(2).unwrap_or_default(),
            replica_identity: value(3)
                .unwrap_or_default()
                .as_bytes()
                .first()
                .copied()
                .unwrap_or(0),
            columns: Vec::new(),
        });
        relation.columns.push(Column {
            name: value(4)?,
            type_oid: value(5)?
                .parse()
                .map_err(|_| CaptureFailure::at("protocol", "ARTICLE1_CATALOG_VALUE_INVALID"))?,
            type_modifier: value(6)?
                .parse()
                .map_err(|_| CaptureFailure::at("protocol", "ARTICLE1_CATALOG_VALUE_INVALID"))?,
            key: value(7)?.as_str() == "1",
        });
    }
    relations
        .into_iter()
        .map(|(id, relation)| relation_contract(id, relation))
        .collect()
}

fn relation_contract(
    id: u32,
    relation: Relation,
) -> Result<(u32, RelationContract), CaptureFailure> {
    let key_columns = relation
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| column.key.then_some(index))
        .collect::<Vec<_>>();
    if relation.replica_identity == b'f' && key_columns.len() != relation.columns.len() {
        return Err(CaptureFailure::at(
            "protocol",
            "ARTICLE1_CATALOG_VALUE_INVALID",
        ));
    }
    if key_columns.is_empty() {
        return Err(CaptureFailure::at(
            "protocol",
            "ARTICLE1_RELATION_KEY_MISSING",
        ));
    }
    Ok((
        id,
        RelationContract {
            relation,
            key_columns,
            control: None,
        },
    ))
}

fn decode_frame(decoder: &mut Decoder, frame: &[u8]) -> Result<CopyBothEvent, CaptureFailure> {
    decoder
        .decode_copy_data(frame)
        .map_err(|_| CaptureFailure::at("protocol", "ARTICLE1_PROTOCOL_REJECTED"))
}

fn classify_connection(message: &str) -> CaptureFailure {
    if message.to_ascii_lowercase().contains("authentication") {
        CaptureFailure::at("authentication", "ARTICLE1_AUTH_FAILED")
    } else {
        CaptureFailure::at("connection", "ARTICLE1_CONNECTION_FAILED")
    }
}

fn render(
    wal_start: u64,
    wal_end: u64,
    event: &PgoutputEvent,
    view_change: Option<&RowViewChange>,
) -> Result<String, CaptureFailure> {
    let mut body = match event {
        PgoutputEvent::Begin {
            final_lsn,
            commit_time,
            xid,
        } => {
            json!({"event":"BEGIN","final_lsn":lsn(*final_lsn),"transaction":{"commit_time":commit_time,"xid":xid},"wal_end":lsn(wal_end),"wal_start":lsn(wal_start)})
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
            json!({"commit_lsn":lsn(*commit_lsn),"end_lsn":lsn(*end_lsn),"event":"COMMIT","row_count":row_count,"transaction":{"commit_time":commit_time},"wal_end":lsn(wal_end),"wal_start":lsn(wal_start)})
        }
        _ => return Err(CaptureFailure::at("protocol", "ARTICLE1_EVENT_UNEXPECTED")),
    };
    body.as_object_mut()
        .ok_or_else(|| CaptureFailure::at("protocol", "ARTICLE1_JSON_ENCODING_FAILED"))?
        .insert("article1_row_view".into(), render_row_view(view_change)?);
    serde_json::to_string(&body)
        .map_err(|_| CaptureFailure::at("protocol", "ARTICLE1_JSON_ENCODING_FAILED"))
}

fn render_row_view(change: Option<&RowViewChange>) -> Result<Value, CaptureFailure> {
    let result = match change {
        None => json!({"action":"transaction_boundary"}),
        Some(RowViewChange::Current { key, row }) => json!({
            "action":"current_row",
            "key":tuple(Some(key))?,
            "row":tuple(Some(row))?,
        }),
        Some(RowViewChange::Removed { key, row }) => json!({
            "action":"removed",
            "key":tuple(Some(key))?,
            "removed_row":tuple(Some(row))?,
            "row":Value::Null,
        }),
    };
    Ok(json!({
        "label":"TEACHING VIEW",
        "disclaimer":"NOT ClickHouse; NOT durable; NOT exactly-once; NOT checkpointed; NOT a materializer; NOT production state; NOT M4; ClickHouse and destination guarantees are deferred to Article 4/M4",
        "result":result,
    }))
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
        assert_eq!(
            decode_frame(&mut Decoder::new(WireLimits::default()), b"?")
                .unwrap_err()
                .code,
            "ARTICLE1_PROTOCOL_REJECTED"
        );
    }

    #[test]
    fn full_identity_contract_requires_and_admits_every_column() {
        let relation = Relation {
            id: 42,
            namespace: "public".into(),
            name: "customers".into(),
            replica_identity: b'f',
            columns: vec![
                Column {
                    name: "id".into(),
                    type_oid: 20,
                    type_modifier: -1,
                    key: true,
                },
                Column {
                    name: "name".into(),
                    type_oid: 25,
                    type_modifier: -1,
                    key: true,
                },
            ],
        };
        let (_, contract) = relation_contract(42, relation.clone()).unwrap();
        assert_eq!(contract.key_columns, vec![0, 1]);

        let mut incomplete = relation;
        incomplete.columns[1].key = false;
        assert_eq!(
            relation_contract(42, incomplete).unwrap_err().code,
            "ARTICLE1_CATALOG_VALUE_INVALID"
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

    #[test]
    #[ignore = "requires the pinned PostgreSQL 17.6 Article 1 fixture"]
    fn live_pg17_preflight_negatives_use_capture_boundaries() {
        let config = CaptureConfig::article1(
            std::env::var("ARTICLE1_DSN").expect("ARTICLE1_DSN is required"),
            1,
        )
        .unwrap();
        for (expected, boundary, code) in [
            (
                Expectations {
                    version: 170_005,
                    ..PROVISIONAL_EXPECTATIONS
                },
                "server_version",
                "ARTICLE1_VERSION_MISMATCH",
            ),
            (
                Expectations {
                    publication: "wrong_publication",
                    ..PROVISIONAL_EXPECTATIONS
                },
                "publication",
                "ARTICLE1_PUBLICATION_MISMATCH",
            ),
            (
                Expectations {
                    slot: "wrong_slot",
                    ..PROVISIONAL_EXPECTATIONS
                },
                "slot",
                "ARTICLE1_SLOT_MISMATCH",
            ),
            (
                Expectations {
                    tables_csv: "other.customers",
                    ..PROVISIONAL_EXPECTATIONS
                },
                "publication",
                "ARTICLE1_PUBLICATION_TABLES_MISMATCH",
            ),
            (
                Expectations {
                    continuity_available: false,
                    ..PROVISIONAL_EXPECTATIONS
                },
                "wal_continuity",
                "ARTICLE1_CONTINUITY_UNAVAILABLE",
            ),
        ] {
            let error = preflight(&config, &expected).unwrap_err();
            assert_eq!((error.boundary, error.code), (boundary, code));
        }
    }

    #[test]
    #[ignore = "requires the pinned PostgreSQL 17.6 Article 1 fixture"]
    fn live_pg17_connection_fails_closed() {
        let dsn = std::env::var("ARTICLE1_DSN").expect("ARTICLE1_DSN is required");
        let port = std::env::var("ARTICLE1_PG_PORT").expect("ARTICLE1_PG_PORT is required");
        let wrong = dsn.replace(&format!(":{port}/"), ":1/");
        assert_ne!(wrong, dsn, "fixture port marker missing");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let error = runtime
            .block_on(capture_jsonl(
                &CaptureConfig::article1(wrong, 1).unwrap(),
                &CancellationToken::new(),
                |_| Ok(()),
            ))
            .unwrap_err();
        assert_eq!(error.boundary, "connection");
        assert_eq!(error.code, "ARTICLE1_CONNECTION_FAILED");
    }

    #[test]
    #[ignore = "requires the pinned PostgreSQL 17.6 Article 1 fixture"]
    fn live_pg17_wrong_auth_fails_closed() {
        let dsn = std::env::var("ARTICLE1_DSN").expect("ARTICLE1_DSN is required");
        let wrong = dsn.replace("article1_fixture_only", "definitely_wrong");
        assert_ne!(wrong, dsn, "fixture password marker missing");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let error = runtime
            .block_on(capture_jsonl(
                &CaptureConfig::article1(wrong, 1).unwrap(),
                &CancellationToken::new(),
                |_| Ok(()),
            ))
            .unwrap_err();
        assert_eq!(error.boundary, "authentication");
        assert_eq!(error.code, "ARTICLE1_AUTH_FAILED");
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
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut lines = Vec::new();
        let commits = runtime
            .block_on(capture_jsonl(
                &CaptureConfig::article1(dsn, 1).unwrap(),
                &CancellationToken::new(),
                |line| {
                    lines.push(line);
                    Ok(())
                },
            ))
            .unwrap();
        assert_eq!(commits, 1);
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

    /// Real-fixture proof that catalog-derived FULL identity reaches the existing decoder.
    #[test]
    #[ignore = "requires the pinned PostgreSQL 17.6 Article 1 fixture"]
    fn live_pg17_full_identity_renders_full_old_tuples() {
        let dsn = std::env::var("ARTICLE1_DSN").expect("ARTICLE1_DSN is required");
        let mut setup = PgReplicationConnection::connect(&dsn).unwrap();
        setup
            .exec("ALTER TABLE public.customers ALTER COLUMN name DROP NOT NULL; ALTER TABLE public.customers REPLICA IDENTITY FULL")
            .unwrap();
        let mutation_dsn = dsn.clone();
        let mutation = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(750));
            let mut connection = PgReplicationConnection::connect(&mutation_dsn).unwrap();
            connection.exec("BEGIN; INSERT INTO customers VALUES (9002, NULL, 1); UPDATE customers SET tier = 2 WHERE id = 9002; DELETE FROM customers WHERE id = 9002; COMMIT;").unwrap();
        });
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let mut lines = Vec::new();
        let commits = runtime
            .block_on(capture_jsonl(
                &CaptureConfig::article1(dsn, 1).unwrap(),
                &CancellationToken::new(),
                |line| {
                    lines.push(line);
                    Ok(())
                },
            ))
            .unwrap();
        assert_eq!(commits, 1);
        mutation.join().unwrap();
        let events = lines
            .iter()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(events[1]["old_state"], "absent");
        assert_eq!(events[2]["old_state"], "full");
        assert_eq!(events[3]["old_state"], "full");
        assert_eq!(events[2]["old"], json!(["9002", null, "1"]));
        assert_eq!(events[3]["old"], json!(["9002", null, "2"]));
        for line in lines {
            println!("{line}");
        }
    }
}
