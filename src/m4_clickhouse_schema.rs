//! ClickHouse event-history schema and canonical query boundary.
//!
//! The accepted SQL is embedded instead of respelled. Physical objects and semantic views are
//! dispatched only through a maintenance-only migration plan; ordinary runtime can read history
//! and append history/batch rows, but cannot obtain DDL or mutate shared objects.
use sha2::{Digest, Sha256};

pub const SERVER_VERSION: &str = "25.8.2.29";
pub const DATABASE: &str = "boring_cdc";
pub const EVENT_HISTORY: &str = "boring_cdc.event_history_v1";
pub const BATCH_MARKERS: &str = "boring_cdc.batch_markers_v1";
pub const GENERATION_SELECTORS: &str = "boring_cdc.generation_selectors_v1";
pub const DDL_SQL: &str = include_str!("../contracts/clickhouse/ddl.sql");
pub const CANONICAL_CURRENT_STATE_SQL: &str =
    include_str!("../contracts/clickhouse/canonical-query.sql");

/// Typed ABI between this physical-schema owner and the semantic current-state view owner.
pub const CURRENT_VIEW_INPUT_SIGNATURE: &str = "capture_epoch UInt64";
pub const CURRENT_VIEW_OUTPUT_SIGNATURE: &str = "canonical_key String, explicit_cells Array(Tuple(column_id UInt32,state String,type_oid UInt32,typmod Int32,value_base64 String))";

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

/// The maintenance call sites that are permitted to install the shared contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationInvocation {
    InitialInit,
    AddedTableReseedStep3,
}

/// Hash-pinned semantic SQL supplied by the M4 TOAST/current-state owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SemanticViewFragment<'a> {
    pub logical_table_id: &'a str,
    pub display_name: &'a str,
    pub view_name: &'a str,
    pub input_signature: &'a str,
    pub output_signature: &'a str,
    pub sql: &'a str,
    pub sha256: &'a str,
}

/// Non-correctness display metadata. Its digest is included in migration identity so a rename is
/// explicit, while correctness continues to group solely by `logical_table_id`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DisplayViewMapping<'a> {
    pub logical_table_id: &'a str,
    pub display_name: &'a str,
    pub view_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationError {
    pub code: &'static str,
    pub boundary: &'static str,
}

/// Narrow dispatch boundary implemented by the ClickHouse adapter. Driver errors never cross this
/// interface (and therefore cannot accidentally expose a DSN, credential, or raw server error).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaintenanceDispatchError;

pub trait MaintenanceExecutor {
    fn execute_batch(&mut self, sql: &str) -> Result<(), MaintenanceDispatchError>;
}

/// The only production-facing way to obtain the accepted physical DDL text.
pub fn maintenance_ddl(principal: Principal) -> Result<&'static str, AuthorizationError> {
    match principal {
        Principal::Maintenance => Ok(DDL_SQL),
        Principal::Runtime => Err(AuthorizationError {
            code: "M4_CH_DDL_FORBIDDEN",
            boundary: "before_clickhouse_dispatch",
        }),
    }
}

/// Allocate a ClickHouse-safe opaque name without incorporating a source schema/table identifier.
pub fn current_view_name(logical_table_id: &str) -> Result<String, MigrationError> {
    if !is_lower_hex_64(logical_table_id) {
        return Err(MigrationError {
            code: "M4_CH_LOGICAL_TABLE_ID_INVALID",
            boundary: "before_clickhouse_dispatch",
        });
    }
    let mut digest = Sha256::new();
    digest.update(b"boring-cdc/current-view-name/v1");
    digest.update((logical_table_id.len() as u64).to_be_bytes());
    digest.update(logical_table_id.as_bytes());
    Ok(format!("current_{:x}_v1", digest.finalize()))
}

pub fn display_view_mapping<'a>(
    logical_table_id: &'a str,
    display_name: &'a str,
) -> Result<DisplayViewMapping<'a>, MigrationError> {
    Ok(DisplayViewMapping {
        logical_table_id,
        display_name,
        view_name: current_view_name(logical_table_id)?,
    })
}

/// Deterministically hash the mapping shape. Ordering by logical identity makes catalog iteration
/// order irrelevant; duplicate identities fail closed rather than silently picking a display name.
pub fn display_mapping_fingerprint(
    mappings: &[DisplayViewMapping<'_>],
) -> Result<String, MigrationError> {
    let mut ordered: Vec<_> = mappings.iter().collect();
    ordered.sort_by_key(|mapping| mapping.logical_table_id);
    if ordered
        .windows(2)
        .any(|pair| pair[0].logical_table_id == pair[1].logical_table_id)
    {
        return Err(MigrationError {
            code: "M4_CH_DISPLAY_MAPPING_DUPLICATE",
            boundary: "before_clickhouse_dispatch",
        });
    }
    let mut digest = Sha256::new();
    hash_part(&mut digest, b"boring-cdc/display-view-mapping/v1");
    for mapping in ordered {
        if current_view_name(mapping.logical_table_id)? != mapping.view_name {
            return Err(MigrationError {
                code: "M4_CH_VIEW_NAME_MISMATCH",
                boundary: "before_clickhouse_dispatch",
            });
        }
        hash_part(&mut digest, mapping.logical_table_id.as_bytes());
        hash_part(&mut digest, mapping.display_name.as_bytes());
        hash_part(&mut digest, mapping.view_name.as_bytes());
    }
    Ok(format!("{:x}", digest.finalize()))
}

/// Execute the exact same physical migration for initial `init` and table-add re-seed step 3,
/// followed by the typed, hash-pinned semantic fragment. The complete plan is validated before the
/// first external effect, so a missing/wrong fragment cannot leave a partially accepted migration.
pub fn execute_maintenance_migration(
    principal: Principal,
    _invocation: MigrationInvocation,
    fragment: &SemanticViewFragment<'_>,
    executor: &mut impl MaintenanceExecutor,
) -> Result<(), MigrationError> {
    if principal != Principal::Maintenance {
        return Err(MigrationError {
            code: "M4_CH_DDL_FORBIDDEN",
            boundary: "before_clickhouse_dispatch",
        });
    }
    validate_semantic_fragment(fragment)?;
    executor
        .execute_batch(DDL_SQL)
        .map_err(|MaintenanceDispatchError| MigrationError {
            code: "M4_CH_PHYSICAL_MIGRATION_FAILED",
            boundary: "clickhouse_physical_ddl",
        })?;
    executor
        .execute_batch(fragment.sql)
        .map_err(|MaintenanceDispatchError| MigrationError {
            code: "M4_CH_SEMANTIC_VIEW_MIGRATION_FAILED",
            boundary: "clickhouse_semantic_view_ddl",
        })
}

fn validate_semantic_fragment(fragment: &SemanticViewFragment<'_>) -> Result<(), MigrationError> {
    let expected_name = current_view_name(fragment.logical_table_id)?;
    let expected_qualified = format!("{DATABASE}.{expected_name}");
    if fragment.view_name != expected_name
        || fragment.input_signature != CURRENT_VIEW_INPUT_SIGNATURE
        || fragment.output_signature != CURRENT_VIEW_OUTPUT_SIGNATURE
        || semantic_view_target(fragment.sql) != Some(expected_qualified.as_str())
        || has_additional_statement(fragment.sql)
    {
        return Err(MigrationError {
            code: "M4_CH_SEMANTIC_VIEW_SIGNATURE_INVALID",
            boundary: "before_clickhouse_dispatch",
        });
    }
    let actual = format!("{:x}", Sha256::digest(fragment.sql.as_bytes()));
    if !is_lower_hex_64(fragment.sha256) || actual != fragment.sha256 {
        return Err(MigrationError {
            code: "M4_CH_SEMANTIC_VIEW_HASH_MISMATCH",
            boundary: "before_clickhouse_dispatch",
        });
    }
    Ok(())
}

/// Fail closed before dispatch when a statement exceeds the principal's fixed capability set.
pub fn authorize(principal: Principal, sql: &str) -> Result<SqlClass, AuthorizationError> {
    let class = classify(sql);
    let allowed = match principal {
        Principal::Maintenance => !matches!(class, SqlClass::Unknown),
        Principal::Runtime => match class {
            SqlClass::Read => !has_additional_statement(sql),
            SqlClass::Insert => {
                !has_additional_statement(sql)
                    && runtime_insert_target(sql)
                        .is_some_and(|target| target == EVENT_HISTORY || target == BATCH_MARKERS)
            }
            _ => false,
        },
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
    let sql = leading_statement(sql);
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

fn leading_statement(mut sql: &str) -> &str {
    sql = sql.trim_start_matches(|c: char| c.is_ascii_whitespace() || c == ';');
    loop {
        if sql.starts_with("--") {
            sql = sql.split_once('\n').map_or("", |(_, rest)| rest);
        } else if sql.starts_with("/*") {
            sql = sql.split_once("*/").map_or("", |(_, rest)| rest);
        } else {
            return sql.trim_start_matches(|c: char| c.is_ascii_whitespace() || c == ';');
        }
    }
}

fn has_additional_statement(sql: &str) -> bool {
    leading_statement(sql)
        .trim_end_matches(|c: char| c.is_ascii_whitespace() || c == ';')
        .contains(';')
}

fn runtime_insert_target(sql: &str) -> Option<&str> {
    let mut words = leading_statement(sql).split_ascii_whitespace();
    if !words.next()?.eq_ignore_ascii_case("INSERT") || !words.next()?.eq_ignore_ascii_case("INTO")
    {
        return None;
    }
    words.next()?.split(['(', ';']).next()
}

fn semantic_view_target(sql: &str) -> Option<&str> {
    let mut words = sql.trim_start().split_ascii_whitespace();
    for expected in ["CREATE", "VIEW", "IF", "NOT", "EXISTS"] {
        if !words.next()?.eq_ignore_ascii_case(expected) {
            return None;
        }
    }
    words.next()?.split(['(', ';']).next()
}

fn is_lower_hex_64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn hash_part(digest: &mut Sha256, part: &[u8]) {
    digest.update((part.len() as u64).to_be_bytes());
    digest.update(part);
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
pub fn object_fingerprint() -> String {
    let mut digest = Sha256::new();
    for part in [
        b"clickhouse-object-fingerprint/v1".as_slice(),
        SERVER_VERSION.as_bytes(),
        DDL_SQL.as_bytes(),
        CANONICAL_CURRENT_STATE_SQL.as_bytes(),
        history_query().as_bytes(),
        CURRENT_VIEW_INPUT_SIGNATURE.as_bytes(),
        CURRENT_VIEW_OUTPUT_SIGNATURE.as_bytes(),
    ] {
        hash_part(&mut digest, part);
    }
    for (name, value) in INSERT_SETTINGS {
        hash_part(&mut digest, name.as_bytes());
        digest.update([value]);
    }
    format!("{:x}", digest.finalize())
}

#[cfg(test)]
mod m4_ddl {
    mod tests {
        use super::super::*;

        #[derive(Default)]
        struct Recorder(Vec<String>);
        impl MaintenanceExecutor for Recorder {
            fn execute_batch(&mut self, sql: &str) -> Result<(), MaintenanceDispatchError> {
                self.0.push(sql.to_owned());
                Ok(())
            }
        }

        fn fragment<'a>(
            logical: &'a str,
            name: &'a str,
            sql: &'a str,
            hash: &'a str,
        ) -> SemanticViewFragment<'a> {
            SemanticViewFragment {
                logical_table_id: logical,
                display_name: "hostile `schema.table`; DROP TABLE x",
                view_name: name,
                input_signature: CURRENT_VIEW_INPUT_SIGNATURE,
                output_signature: CURRENT_VIEW_OUTPUT_SIGNATURE,
                sql,
                sha256: hash,
            }
        }

        #[test]
        fn ordinary_run_never_receives_ddl_or_writes_unowned_objects() {
            assert_eq!(
                maintenance_ddl(Principal::Runtime).unwrap_err().code,
                "M4_CH_DDL_FORBIDDEN"
            );
            for statement in [
                DDL_SQL,
                "ALTER TABLE boring_cdc.event_history_v1 DROP PARTITION tuple(1,1)",
                "OPTIMIZE TABLE boring_cdc.event_history_v1 FINAL",
                "SYSTEM STOP MERGES boring_cdc.event_history_v1",
                "INSERT INTO boring_cdc.generation_selectors_v1 VALUES (1,2,3,'a','b','c')",
                "SELECT 1; DROP TABLE boring_cdc.event_history_v1",
                "INSERT INTO boring_cdc.event_history_v1 VALUES (); DROP TABLE boring_cdc.event_history_v1",
            ] {
                assert!(
                    authorize(Principal::Runtime, statement).is_err(),
                    "{statement}"
                );
            }
            assert_eq!(
                authorize(Principal::Runtime, history_query()),
                Ok(SqlClass::Read)
            );
            for statement in [
                format!("INSERT INTO {EVENT_HISTORY} FORMAT RowBinary"),
                format!("INSERT INTO {BATCH_MARKERS}(capture_epoch) VALUES (1)"),
            ] {
                assert_eq!(
                    authorize(Principal::Runtime, &statement),
                    Ok(SqlClass::Insert)
                );
            }
        }

        #[test]
        fn physical_contract_has_full_namespaced_append_only_shape() {
            for token in [
                "capture_epoch UInt64",
                "generation UInt64",
                "logical_table_id FixedString(64)",
                "relation_schema_fingerprint FixedString(64)",
                "connector_event_id FixedString(64)",
                "payload_hash FixedString(64)",
                "lsn_u64 UInt64",
                "origin_rank UInt8",
                "transaction_ordinal UInt64",
                "mutation_ordinal UInt8",
                "journal_seq UInt64",
                "absent_for_schema",
                "explicit_null",
                "unchanged_toast",
                "explicit_value",
                "PARTITION BY (capture_epoch,generation)",
                "ORDER BY (capture_epoch,generation,logical_table_id,canonical_key,lsn_u64,origin_rank,transaction_ordinal,mutation_ordinal,connector_event_id)",
                "generation_selectors_v1",
            ] {
                assert!(DDL_SQL.contains(token), "missing {token}");
            }
            for forbidden in ["ReplacingMergeTree", "CollapsingMergeTree", " TTL "] {
                assert!(!DDL_SQL.contains(forbidden), "forbidden {forbidden}");
            }
        }

        #[test]
        fn opaque_names_ignore_hostile_display_names_and_mapping_is_order_stable() {
            let a = "01".repeat(32);
            let b = "02".repeat(32);
            let first = display_view_mapping(&a, "public.users").unwrap();
            let renamed = display_view_mapping(&a, "renamed`; DROP TABLE x").unwrap();
            assert_eq!(first.view_name, renamed.view_name);
            assert!(!first.view_name.contains("users"));
            let second = display_view_mapping(&b, "other").unwrap();
            assert_eq!(
                display_mapping_fingerprint(&[first.clone(), second.clone()]).unwrap(),
                display_mapping_fingerprint(&[second, first]).unwrap()
            );
            assert_ne!(
                display_mapping_fingerprint(&[renamed]).unwrap(),
                display_mapping_fingerprint(&[display_view_mapping(&a, "public.users").unwrap()])
                    .unwrap()
            );
            assert!(current_view_name("ABC").is_err());
        }

        #[test]
        fn init_and_table_add_use_identical_validated_migration_order() {
            let logical = "ab".repeat(32);
            let name = current_view_name(&logical).unwrap();
            let sql = format!(
                "CREATE VIEW IF NOT EXISTS {DATABASE}.{name} AS SELECT canonical_key,[] AS explicit_cells FROM {EVENT_HISTORY}"
            );
            let hash = format!("{:x}", Sha256::digest(sql.as_bytes()));
            let binding = fragment(&logical, &name, &sql, &hash);
            let mut init = Recorder::default();
            let mut add = Recorder::default();
            execute_maintenance_migration(
                Principal::Maintenance,
                MigrationInvocation::InitialInit,
                &binding,
                &mut init,
            )
            .unwrap();
            execute_maintenance_migration(
                Principal::Maintenance,
                MigrationInvocation::AddedTableReseedStep3,
                &binding,
                &mut add,
            )
            .unwrap();
            assert_eq!(init.0, add.0);
            assert_eq!(init.0, vec![DDL_SQL.to_owned(), sql]);
        }

        #[test]
        fn semantic_fragment_is_required_and_hash_pinned_before_dispatch() {
            let logical = "cd".repeat(32);
            let name = current_view_name(&logical).unwrap();
            let sql = format!(
                "CREATE VIEW IF NOT EXISTS {DATABASE}.{name} AS SELECT canonical_key,[] AS explicit_cells FROM {EVENT_HISTORY}"
            );
            let mut recorder = Recorder::default();
            let wrong_hash = "0".repeat(64);
            let bad = fragment(&logical, &name, &sql, &wrong_hash);
            assert_eq!(
                execute_maintenance_migration(
                    Principal::Maintenance,
                    MigrationInvocation::InitialInit,
                    &bad,
                    &mut recorder
                )
                .unwrap_err()
                .code,
                "M4_CH_SEMANTIC_VIEW_HASH_MISMATCH"
            );
            assert!(recorder.0.is_empty());
            let wrong_sql = format!(
                "CREATE VIEW IF NOT EXISTS {DATABASE}.unowned AS SELECT '{DATABASE}.{name}'"
            );
            let wrong_sql_hash = format!("{:x}", Sha256::digest(wrong_sql.as_bytes()));
            let wrong_target = fragment(&logical, &name, &wrong_sql, &wrong_sql_hash);
            assert_eq!(
                execute_maintenance_migration(
                    Principal::Maintenance,
                    MigrationInvocation::InitialInit,
                    &wrong_target,
                    &mut recorder
                )
                .unwrap_err()
                .code,
                "M4_CH_SEMANTIC_VIEW_SIGNATURE_INVALID"
            );
            assert!(recorder.0.is_empty());
            assert_eq!(
                execute_maintenance_migration(
                    Principal::Runtime,
                    MigrationInvocation::InitialInit,
                    &bad,
                    &mut recorder
                )
                .unwrap_err()
                .code,
                "M4_CH_DDL_FORBIDDEN"
            );
            assert!(recorder.0.is_empty());
        }

        #[test]
        fn interfaces_fix_epoch_before_source_version_ordering() {
            let history = history_query();
            assert!(history.contains("capture_epoch={capture_epoch:UInt64}"));
            assert!(history.contains("ORDER BY canonical_key,lsn_u64,origin_rank,transaction_ordinal,mutation_ordinal,connector_event_id"));
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

        #[test]
        fn fingerprint_is_stable_and_covers_interfaces_and_settings() {
            let fingerprint = object_fingerprint();
            assert_eq!(fingerprint.len(), 64);
            assert!(fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit()));
            assert_ne!(
                format!("{:x}", Sha256::digest(DDL_SQL.as_bytes())),
                fingerprint
            );
            assert_eq!(object_fingerprint(), fingerprint);
        }
    }
}
