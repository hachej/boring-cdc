//! ClickHouse event-history schema and canonical query boundary.
//!
//! The SQL files are the accepted contract. This module deliberately embeds them instead of
//! maintaining a second spelling, and keeps maintenance DDL unavailable to ordinary runtime code.
use sha2::{Digest, Sha256};

pub const SERVER_VERSION: &str = "25.8.2.29";
pub const DATABASE: &str = "boring_cdc";
pub const EVENT_HISTORY: &str = "boring_cdc.event_history_v1";
pub const BATCH_MARKERS: &str = "boring_cdc.batch_markers_v1";
pub const GENERATION_SELECTORS: &str = "boring_cdc.generation_selectors_v1";
pub const DDL_SQL: &str = include_str!("../contracts/clickhouse/ddl.sql");
pub const CANONICAL_CURRENT_STATE_SQL: &str =
    include_str!("../contracts/clickhouse/canonical-query.sql");

/// Settings that are part of the object fingerprint and must accompany every history insert.
pub const INSERT_SETTINGS: [(&str, u8); 6] = [
    ("async_insert", 0),
    ("wait_for_async_insert", 1),
    ("insert_quorum", 1),
    ("fsync_after_insert", 1),
    ("fsync_part_directory", 1),
    ("insert_deduplicate", 0),
];

/// Capabilities provisioned out-of-band for the credential used by ordinary `run`.
pub const RUNTIME_PRIVILEGES: [&str; 5] = [
    "INSERT ON boring_cdc.event_history_v1",
    "INSERT ON boring_cdc.batch_markers_v1",
    "SELECT ON boring_cdc.event_history_v1",
    "SELECT ON boring_cdc.batch_markers_v1",
    "SELECT ON boring_cdc.*_v1",
];

/// Maintenance owns object creation, grants, selector publication, and bounded retirement.
pub const MAINTENANCE_PRIVILEGES: [&str; 5] = [
    "CREATE DATABASE",
    "CREATE TABLE ON boring_cdc.*",
    "CREATE VIEW ON boring_cdc.*",
    "INSERT ON boring_cdc.generation_selectors_v1",
    "ALTER DELETE ON boring_cdc.event_history_v1,boring_cdc.batch_markers_v1",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Principal {
    Runtime,
    Maintenance,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlClass {
    Read,
    Insert,
    Ddl,
    Mutation,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizationError {
    pub code: &'static str,
    pub boundary: &'static str,
}

/// The only production-facing way to obtain the accepted DDL text.
///
/// Ordinary runtime code cannot accidentally receive or execute it.
pub fn maintenance_ddl(principal: Principal) -> Result<&'static str, AuthorizationError> {
    match principal {
        Principal::Maintenance => Ok(DDL_SQL),
        Principal::Runtime => Err(AuthorizationError {
            code: "M4_CH_DDL_FORBIDDEN",
            boundary: "before_clickhouse_dispatch",
        }),
    }
}

/// Fail closed before dispatch when a statement exceeds the principal's fixed capability set.
pub fn authorize(principal: Principal, sql: &str) -> Result<SqlClass, AuthorizationError> {
    let class = classify(sql);
    let allowed = match principal {
        Principal::Maintenance => !matches!(class, SqlClass::Unknown),
        Principal::Runtime => matches!(class, SqlClass::Read | SqlClass::Insert),
    };
    if allowed {
        Ok(class)
    } else {
        Err(AuthorizationError {
            code: "M4_CH_SQL_FORBIDDEN",
            boundary: "before_clickhouse_dispatch",
        })
    }
}

fn classify(sql: &str) -> SqlClass {
    let mut sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace() || c == ';');
    while sql.starts_with("--") {
        sql = sql.split_once('\n').map_or("", |(_, rest)| rest);
        sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace() || c == ';');
    }
    while sql.starts_with("/*") {
        sql = sql.split_once("*/").map_or("", |(_, rest)| rest);
        sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace() || c == ';');
    }
    let keyword = sql
        .split(|c: char| c.is_ascii_whitespace() || c == '(')
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    match keyword.as_str() {
        "SELECT" | "WITH" | "EXPLAIN" => SqlClass::Read,
        "INSERT" => SqlClass::Insert,
        "CREATE" | "ATTACH" | "DETACH" | "DROP" | "RENAME" | "GRANT" | "REVOKE" => SqlClass::Ddl,
        "ALTER" | "OPTIMIZE" | "TRUNCATE" | "SYSTEM" | "KILL" => SqlClass::Mutation,
        _ => SqlClass::Unknown,
    }
}

/// Canonical append-only history interface. Source versions are compared only after fixing the
/// capture epoch and generation; `journal_seq` is intentionally absent from source ordering.
pub fn history_query() -> &'static str {
    "SELECT capture_epoch,generation,logical_table_id,relation_schema_fingerprint,canonical_key,key_hash,before_key,connector_event_id,payload_hash,operation,mutation_kind,lsn_u64,origin_rank,transaction_ordinal,mutation_ordinal,journal_seq,batch_id,columns FROM boring_cdc.event_history_v1 WHERE capture_epoch={capture_epoch:UInt64} AND generation={generation:UInt64} AND logical_table_id={logical_table_id:FixedString(64)} ORDER BY canonical_key,lsn_u64,origin_rank,transaction_ordinal,mutation_ordinal,connector_event_id"
}

pub fn current_state_query() -> &'static str {
    CANONICAL_CURRENT_STATE_SQL
}

/// Fingerprint every correctness-bearing object/query and the fixed insert settings.
/// Length framing prevents concatenation ambiguity and preserves the accepted bytes exactly.
pub fn object_fingerprint() -> String {
    let mut digest = Sha256::new();
    for part in [
        b"clickhouse-object-fingerprint/v1".as_slice(),
        SERVER_VERSION.as_bytes(),
        DDL_SQL.as_bytes(),
        CANONICAL_CURRENT_STATE_SQL.as_bytes(),
        history_query().as_bytes(),
    ] {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    for (name, value) in INSERT_SETTINGS {
        digest.update((name.len() as u64).to_be_bytes());
        digest.update(name.as_bytes());
        digest.update([value]);
    }
    format!("{:x}", digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_run_never_receives_ddl_or_mutations() {
        assert_eq!(
            maintenance_ddl(Principal::Runtime).unwrap_err().code,
            "M4_CH_DDL_FORBIDDEN"
        );
        for statement in [
            DDL_SQL,
            "ALTER TABLE boring_cdc.event_history_v1 DROP PARTITION tuple(1,1)",
            "OPTIMIZE TABLE boring_cdc.event_history_v1 FINAL",
            "SYSTEM STOP MERGES boring_cdc.event_history_v1",
        ] {
            assert!(authorize(Principal::Runtime, statement).is_err());
        }
        assert_eq!(
            authorize(Principal::Runtime, history_query()),
            Ok(SqlClass::Read)
        );
        assert_eq!(
            authorize(
                Principal::Runtime,
                "INSERT INTO boring_cdc.event_history_v1 FORMAT RowBinary"
            ),
            Ok(SqlClass::Insert)
        );
    }

    #[test]
    fn maintenance_is_the_only_ddl_owner() {
        assert_eq!(maintenance_ddl(Principal::Maintenance), Ok(DDL_SQL));
        assert_eq!(
            authorize(Principal::Maintenance, DDL_SQL),
            Ok(SqlClass::Ddl)
        );
    }

    #[test]
    fn fingerprint_is_stable_and_covers_queries_and_settings() {
        let fingerprint = object_fingerprint();
        assert_eq!(fingerprint.len(), 64);
        assert!(fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_ne!(
            Sha256::digest(DDL_SQL.as_bytes()).as_slice(),
            hex_for_test(&fingerprint).as_slice()
        );
        assert_eq!(object_fingerprint(), fingerprint);
    }

    fn hex_for_test(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                u8::from_str_radix(std::str::from_utf8(pair).expect("ASCII hex"), 16)
                    .expect("valid hex")
            })
            .collect()
    }

    #[test]
    fn interfaces_fix_epoch_before_source_version_ordering() {
        let history = history_query();
        assert!(history.contains("capture_epoch={capture_epoch:UInt64}"));
        assert!(history.contains(
            "ORDER BY canonical_key,lsn_u64,origin_rank,transaction_ordinal,mutation_ordinal,connector_event_id"
        ));
        assert!(
            !history
                .split("ORDER BY")
                .nth(1)
                .unwrap()
                .contains("journal_seq")
        );
        assert!(
            !current_state_query()
                .lines()
                .filter(|line| !line.trim_start().starts_with("/*"))
                .any(|line| line.contains(" FINAL "))
        );
        assert!(current_state_query().contains("event_identity_conflicts_v1"));
        assert!(current_state_query().contains("selector_conflicts_v1"));
    }
}
