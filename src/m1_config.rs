//! Shared, side-effect-free TOML configuration boundary.
//!
//! Parsing, approved overrides, validation, secret resolution, redacted diagnostics and
//! domain fingerprints live here so command handlers cannot grow divergent loaders.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::SocketAddr;
use std::path::{Component, Path};

const SCHEMA_VERSION: u32 = 1;
const APPROVED_OVERRIDES: &[&str] = &[
    "BORING_CDC_STATUS_LISTEN_ADDR",
    "BORING_CDC_PROMETHEUS_LISTEN_ADDR",
    "BORING_CDC_LOG_LEVEL",
];

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Bytes(pub u64);

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Milliseconds(pub u64);

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    schema_version: u32,
    source: RawSource,
    tables: Vec<Table>,
    storage: Storage,
    limits: Limits,
    budgets: Vec<FilesystemBudget>,
    retention: Retention,
    wal: WalHeadroom,
    backfill: Backfill,
    clickhouse: ClickHouse,
    archive: Archive,
    conditions: Conditions,
    #[serde(default)]
    observability: Observability,
    #[serde(default)]
    operator: OperatorEndpoint,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSource {
    runtime_dsn_env: String,
    control_writer_dsn_env: String,
    administration_dsn_env: String,
    publication: String,
    slot: String,
    copy_both_transport: String,
    #[serde(default = "default_start_replication_options")]
    start_replication_options: Vec<String>,
    #[serde(default = "default_origin_policy")]
    origin_policy: String,
    advisory_lock_derivation: String,
    lock_probe_interval_ms: Milliseconds,
    ownership_deadline_ms: Milliseconds,
    maximum_operation_ms: Milliseconds,
    stale_owner_takeover_ms: Milliseconds,
    heartbeat_cadence_ms: Milliseconds,
    #[serde(default = "default_verify_tls")]
    verify_tls: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Table {
    pub logical_id: String,
    pub source_relation: String,
    pub replica_key: Vec<String>,
    pub key_type_policy: String,
    pub relation_contract: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Storage {
    pub sqlite_path: String,
    pub sqlite_temp_path: String,
    pub spool_path: String,
    pub filesystem: String,
    pub journal_mode: String,
    pub synchronous: String,
    pub auto_vacuum: String,
    pub checkpoint_pages: u64,
    pub vacuum_pages: u64,
    #[serde(default)]
    pub unsafe_local_experiment: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub max_wire_frame_bytes: Bytes,
    pub max_event_bytes: Bytes,
    pub max_row_bytes: Bytes,
    pub max_transaction_bytes: Bytes,
    pub max_transaction_events: u64,
    pub process_memory_bytes: Bytes,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemBudget {
    pub name: String,
    pub root: String,
    pub total_bytes: Bytes,
    pub reserved_free_bytes: Bytes,
    pub sqlite_wal_temp_bytes: Bytes,
    pub capture_spool_bytes: Bytes,
    pub concurrent_backfill_bytes: Bytes,
    pub archive_segment_bytes: Bytes,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Retention {
    pub replay_window_ms: Milliseconds,
    pub usable_anchor_count: u64,
    pub invalid_generation_ms: Milliseconds,
    pub metadata_gc_ms: Milliseconds,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WalHeadroom {
    pub max_slot_wal_keep_bytes: Bytes,
    pub source_free_bytes: Bytes,
    pub production_bytes_per_second: Bytes,
    pub monitor_delay_ms: Milliseconds,
    pub reaction_reserve_bytes: Bytes,
    pub missing_metric_policy: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Backfill {
    pub chunk_rows: u64,
    pub chunk_bytes: Bytes,
    pub chunk_duration_ms: Milliseconds,
    pub concurrency: u64,
    pub exporter_lifetime_ms: Milliseconds,
    pub importer_lifetime_ms: Milliseconds,
    pub guard_lifetime_ms: Milliseconds,
    pub guard_keepalive_ms: Milliseconds,
    pub session_timeout_ms: Milliseconds,
    pub ddl_waiter_bound_ms: Milliseconds,
    pub source_impact_bytes: Bytes,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClickHouse {
    endpoint: String,
    server_version: String,
    client_version: String,
    runtime_dsn_env: String,
    maintenance_dsn_env: String,
    contract_id: String,
    history_object: String,
    selector_object: String,
    history_quota_bytes: Bytes,
    synchronous_insert: bool,
    fsync_after_insert: bool,
    rust_insert_finalization: String,
    destination_generation: u64,
    selector_policy: String,
    adopted_external_fence: u64,
    retirement_grace_ms: Milliseconds,
    audit_bytes: Bytes,
    audit_events: u64,
    audit_time_ms: Milliseconds,
    audit_cadence_ms: Milliseconds,
    audit_freshness_ms: Milliseconds,
    #[serde(default = "default_verify_tls")]
    verify_tls: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Archive {
    pub root: String,
    pub filesystem: String,
    pub budget_bytes: Bytes,
    pub schedule_ms: Milliseconds,
    pub formats: BTreeSet<String>,
    pub writer_crate: String,
    pub writer_version: String,
    pub compression: String,
    pub segment_bytes: Bytes,
    pub path_encoding: String,
    pub filesystem_policy: String,
    pub ready_marker: String,
    pub selector_policy: String,
    pub continuity_break_policy: String,
    pub audit_bytes: Bytes,
    pub audit_events: u64,
    pub audit_time_ms: Milliseconds,
    pub audit_cadence_ms: Milliseconds,
    pub audit_freshness_ms: Milliseconds,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Conditions {
    pub warning: u64,
    pub action: u64,
    pub critical: u64,
    pub hard: u64,
    pub freshness_ms: Milliseconds,
    pub hysteresis_ms: Milliseconds,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Observability {
    pub log_level: String,
    pub status_listen_addr: String,
    pub prometheus_listen_addr: String,
    pub authentication: bool,
    pub tls: bool,
}

impl Default for Observability {
    fn default() -> Self {
        Self {
            log_level: "info".into(),
            status_listen_addr: "127.0.0.1:8787".into(),
            prometheus_listen_addr: "127.0.0.1:8788".into(),
            authentication: false,
            tls: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct OperatorEndpoint {
    pub socket_path: String,
    pub directory_mode: u32,
    pub socket_mode: u32,
    pub peer_credentials: bool,
    pub max_request_bytes: Bytes,
    pub max_response_bytes: Bytes,
    pub read_timeout_ms: Milliseconds,
    pub write_timeout_ms: Milliseconds,
    pub result_retention_ms: Milliseconds,
    pub confirmation_expiry_ms: Milliseconds,
}

impl Default for OperatorEndpoint {
    fn default() -> Self {
        Self {
            socket_path: "run/boring-cdc/operator.sock".into(),
            directory_mode: 0o700,
            socket_mode: 0o600,
            peer_credentials: true,
            max_request_bytes: Bytes(1_048_576),
            max_response_bytes: Bytes(4_194_304),
            read_timeout_ms: Milliseconds(10_000),
            write_timeout_ms: Milliseconds(30_000),
            result_retention_ms: Milliseconds(86_400_000),
            confirmation_expiry_ms: Milliseconds(300_000),
        }
    }
}

fn default_start_replication_options() -> Vec<String> {
    // M0-PROVISIONAL: boring-cdc-d-pg-protocol (RECOMMENDED non-streamed pgoutput policy).
    vec![
        "proto_version=1".into(),
        "streaming=false".into(),
        "two_phase=false".into(),
        "binary=false".into(),
    ]
}
fn default_origin_policy() -> String {
    // M0-PROVISIONAL: boring-cdc-d-pg-protocol (RECOMMENDED origin='any').
    "any".into()
}
fn default_verify_tls() -> bool {
    true
}

/// Secret material is intentionally neither serializable nor printable.
pub struct SecretString(String);
impl SecretString {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

struct Secrets {
    runtime: Option<SecretString>,
    control_writer: Option<SecretString>,
    administration: Option<SecretString>,
    clickhouse_runtime: Option<SecretString>,
    clickhouse_maintenance: Option<SecretString>,
}

/// Selects the least-privilege secret set resolved by the shared loader.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LoadPurpose {
    Check,
    Status,
    Run,
    PostgresAdmin,
    ClickHouseMaintenance,
}

#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub publication: String,
    pub slot: String,
    pub copy_both_transport: String,
    pub start_replication_options: Vec<String>,
    pub origin_policy: String,
    pub advisory_lock_derivation: String,
    pub lock_probe_interval_ms: Milliseconds,
    pub ownership_deadline_ms: Milliseconds,
    pub maximum_operation_ms: Milliseconds,
    pub stale_owner_takeover_ms: Milliseconds,
    pub heartbeat_cadence_ms: Milliseconds,
    pub verify_tls: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseConfig {
    pub endpoint: String,
    pub server_version: String,
    pub client_version: String,
    pub contract_id: String,
    pub history_object: String,
    pub selector_object: String,
    pub history_quota_bytes: Bytes,
    pub synchronous_insert: bool,
    pub fsync_after_insert: bool,
    pub rust_insert_finalization: String,
    pub destination_generation: u64,
    pub selector_policy: String,
    pub adopted_external_fence: u64,
    pub retirement_grace_ms: Milliseconds,
    pub audit_bytes: Bytes,
    pub audit_events: u64,
    pub audit_time_ms: Milliseconds,
    pub audit_cadence_ms: Milliseconds,
    pub audit_freshness_ms: Milliseconds,
    pub verify_tls: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublicConfig {
    pub schema_version: u32,
    pub source: SourceConfig,
    pub tables: Vec<Table>,
    pub storage: Storage,
    pub limits: Limits,
    pub budgets: Vec<FilesystemBudget>,
    pub retention: Retention,
    pub wal: WalHeadroom,
    pub backfill: Backfill,
    pub clickhouse: ClickHouseConfig,
    pub archive: Archive,
    pub conditions: Conditions,
    pub observability: Observability,
    pub operator: OperatorEndpoint,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Fingerprints {
    pub source: String,
    pub table_set: String,
    pub destination: String,
    pub archive: String,
    pub promotion: String,
    pub backfill: String,
    pub runtime: String,
}

pub struct LoadedConfig {
    public: PublicConfig,
    secrets: Secrets,
    fingerprints: Fingerprints,
}

impl fmt::Debug for LoadedConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoadedConfig")
            .field("diagnostics", &self.redacted_diagnostics())
            .field("secrets", &"[REDACTED]")
            .field("fingerprints", &self.fingerprints)
            .finish()
    }
}

impl LoadedConfig {
    pub fn public(&self) -> &PublicConfig {
        &self.public
    }
    pub fn fingerprints(&self) -> &Fingerprints {
        &self.fingerprints
    }
    pub fn runtime_dsn(&self) -> Option<&str> {
        self.secrets.runtime.as_ref().map(SecretString::expose)
    }
    pub fn control_writer_dsn(&self) -> Option<&str> {
        self.secrets
            .control_writer
            .as_ref()
            .map(SecretString::expose)
    }
    pub fn administration_dsn(&self) -> Option<&str> {
        self.secrets
            .administration
            .as_ref()
            .map(SecretString::expose)
    }
    pub fn clickhouse_runtime_dsn(&self) -> Option<&str> {
        self.secrets
            .clickhouse_runtime
            .as_ref()
            .map(SecretString::expose)
    }
    pub fn clickhouse_maintenance_dsn(&self) -> Option<&str> {
        self.secrets
            .clickhouse_maintenance
            .as_ref()
            .map(SecretString::expose)
    }

    /// Safe diagnostic projection: fixed secret role labels, never environment names or values.
    pub fn redacted_diagnostics(&self) -> serde_json::Value {
        serde_json::json!({
            "schema_version": self.public.schema_version,
            "fingerprints": self.fingerprints,
            "table_count": self.public.tables.len(),
            "unsafe_local_experiment": self.public.storage.unsafe_local_experiment,
            "listeners": {
                "status_exposure": listener_exposure(&self.public.observability.status_listen_addr),
                "prometheus_exposure": listener_exposure(&self.public.observability.prometheus_listen_addr),
                "authentication": self.public.observability.authentication,
                "tls": self.public.observability.tls
            },
            "secrets": {
                "runtime_capture": secret_status(&self.secrets.runtime),
                "control_writer": secret_status(&self.secrets.control_writer),
                "administration": secret_status(&self.secrets.administration),
                "clickhouse_runtime": secret_status(&self.secrets.clickhouse_runtime),
                "clickhouse_maintenance": secret_status(&self.secrets.clickhouse_maintenance)
            }
        })
    }
}

fn listener_exposure(value: &str) -> &'static str {
    value.parse::<SocketAddr>().map_or("invalid", |address| {
        if address.ip().is_loopback() {
            "loopback"
        } else {
            "non_loopback"
        }
    })
}

fn secret_status(secret: &Option<SecretString>) -> &'static str {
    if secret.is_some() {
        "[REDACTED:available]"
    } else {
        "[REDACTED:not-loaded]"
    }
}

pub trait Environment {
    fn get(&self, name: &str) -> Option<String>;
    fn names(&self) -> Vec<String> {
        Vec::new()
    }
}

pub struct ProcessEnvironment;
impl Environment for ProcessEnvironment {
    fn get(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }
    fn names(&self) -> Vec<String> {
        std::env::vars().map(|(k, _)| k).collect()
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct ConfigError {
    pub code: &'static str,
    pub field: &'static str,
}
impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at {}", self.code, self.field)
    }
}
impl std::error::Error for ConfigError {}

/// Parse, apply the closed override list, resolve secrets and validate without I/O beyond env reads.
pub fn load_str(input: &str, env: &dyn Environment) -> Result<LoadedConfig, ConfigError> {
    load_str_for(input, env, LoadPurpose::Run)
}

pub fn load_str_for(
    input: &str,
    env: &dyn Environment,
    purpose: LoadPurpose,
) -> Result<LoadedConfig, ConfigError> {
    let mut raw: RawConfig = toml::from_str(input).map_err(|_| ConfigError {
        code: "CONFIG_INVALID_TOML_OR_UNKNOWN_FIELD",
        field: "config",
    })?;
    reject_unapproved_overrides(
        env,
        [
            &raw.source.runtime_dsn_env,
            &raw.source.control_writer_dsn_env,
            &raw.source.administration_dsn_env,
            &raw.clickhouse.runtime_dsn_env,
            &raw.clickhouse.maintenance_dsn_env,
        ],
    )?;
    validate_secret_references([
        &raw.source.runtime_dsn_env,
        &raw.source.control_writer_dsn_env,
        &raw.source.administration_dsn_env,
        &raw.clickhouse.runtime_dsn_env,
        &raw.clickhouse.maintenance_dsn_env,
    ])?;
    apply_overrides(&mut raw, env)?;
    validate(&mut raw)?;
    let secrets = Secrets {
        runtime: matches!(purpose, LoadPurpose::Check | LoadPurpose::Run)
            .then(|| resolve_secret(env, &raw.source.runtime_dsn_env, "source.runtime_dsn_env"))
            .transpose()?,
        control_writer: matches!(purpose, LoadPurpose::Check | LoadPurpose::Run)
            .then(|| {
                resolve_secret(
                    env,
                    &raw.source.control_writer_dsn_env,
                    "source.control_writer_dsn_env",
                )
            })
            .transpose()?,
        administration: matches!(purpose, LoadPurpose::PostgresAdmin)
            .then(|| {
                resolve_secret(
                    env,
                    &raw.source.administration_dsn_env,
                    "source.administration_dsn_env",
                )
            })
            .transpose()?,
        clickhouse_runtime: matches!(purpose, LoadPurpose::Check | LoadPurpose::Run)
            .then(|| {
                resolve_secret(
                    env,
                    &raw.clickhouse.runtime_dsn_env,
                    "clickhouse.runtime_dsn_env",
                )
            })
            .transpose()?,
        clickhouse_maintenance: matches!(purpose, LoadPurpose::ClickHouseMaintenance)
            .then(|| {
                resolve_secret(
                    env,
                    &raw.clickhouse.maintenance_dsn_env,
                    "clickhouse.maintenance_dsn_env",
                )
            })
            .transpose()?,
    };
    let public = into_public(raw);
    let fingerprints = fingerprints(&public)?;
    Ok(LoadedConfig {
        public,
        secrets,
        fingerprints,
    })
}

fn reject_unapproved_overrides(
    env: &dyn Environment,
    secret_names: [&String; 5],
) -> Result<(), ConfigError> {
    for name in env.names() {
        if name.starts_with("BORING_CDC_")
            && !APPROVED_OVERRIDES.contains(&name.as_str())
            && !secret_names.iter().any(|secret| secret.as_str() == name)
        {
            return Err(ConfigError {
                code: "CONFIG_UNAPPROVED_ENV_OVERRIDE",
                field: "environment",
            });
        }
    }
    Ok(())
}

fn apply_overrides(raw: &mut RawConfig, env: &dyn Environment) -> Result<(), ConfigError> {
    if let Some(v) = env.get("BORING_CDC_STATUS_LISTEN_ADDR") {
        raw.observability.status_listen_addr = nonempty(v, "observability.status_listen_addr")?;
    }
    if let Some(v) = env.get("BORING_CDC_PROMETHEUS_LISTEN_ADDR") {
        raw.observability.prometheus_listen_addr =
            nonempty(v, "observability.prometheus_listen_addr")?;
    }
    if let Some(v) = env.get("BORING_CDC_LOG_LEVEL") {
        raw.observability.log_level = nonempty(v, "observability.log_level")?;
    }
    Ok(())
}

fn nonempty(v: String, field: &'static str) -> Result<String, ConfigError> {
    if v.trim().is_empty() {
        Err(ConfigError {
            code: "CONFIG_EMPTY_OVERRIDE",
            field,
        })
    } else {
        Ok(v)
    }
}

fn resolve_secret(
    env: &dyn Environment,
    name: &str,
    field: &'static str,
) -> Result<SecretString, ConfigError> {
    if !valid_env_name(name) {
        return Err(ConfigError {
            code: "CONFIG_INVALID_SECRET_REFERENCE",
            field,
        });
    }
    match env.get(name) {
        Some(value) if !value.is_empty() => Ok(SecretString(value)),
        _ => Err(ConfigError {
            code: "CONFIG_MISSING_SECRET",
            field,
        }),
    }
}

fn valid_env_name(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some('A'..='Z' | '_'))
        && chars.all(|c| matches!(c, 'A'..='Z' | '0'..='9' | '_'))
}

fn validate(raw: &mut RawConfig) -> Result<(), ConfigError> {
    if raw.schema_version != SCHEMA_VERSION {
        return err("CONFIG_UNSUPPORTED_SCHEMA", "schema_version");
    }
    if raw.tables.is_empty() {
        return err("CONFIG_EMPTY_TABLE_SET", "tables");
    }
    raw.source.start_replication_options.sort();
    raw.tables.sort_by(|a, b| a.logical_id.cmp(&b.logical_id));
    unique(
        raw.tables.iter().map(|t| t.logical_id.as_str()),
        "tables.logical_id",
    )?;
    for table in &raw.tables {
        if table.logical_id.is_empty()
            || table.source_relation.is_empty()
            || table.relation_contract.is_empty()
            || table.key_type_policy != "canonical-v1"
        {
            return err("CONFIG_INVALID_TABLE_IDENTITY", "tables");
        }
        validate_replica_key(table)?;
    }
    if raw.source.publication.is_empty()
        || raw.source.slot.is_empty()
        || raw.source.copy_both_transport != "postgres-replication-copyboth-v1"
        || raw.source.advisory_lock_derivation != "sha256-system-database-publication-slot-v1"
    {
        return err("CONFIG_EMPTY_SOURCE_IDENTITY", "source");
    }
    if raw.source.lock_probe_interval_ms.0 == 0
        || raw.source.ownership_deadline_ms.0 == 0
        || raw.source.maximum_operation_ms.0 == 0
        || raw.source.stale_owner_takeover_ms.0 == 0
        || raw.source.heartbeat_cadence_ms.0 == 0
    {
        return err("CONFIG_ZERO_BOUND", "source");
    }
    if raw.budgets.is_empty() {
        return err("CONFIG_EMPTY_FILESYSTEM_BUDGETS", "budgets");
    }
    raw.budgets.sort_by(|a, b| a.name.cmp(&b.name));
    unique(raw.budgets.iter().map(|b| b.name.as_str()), "budgets.name")?;
    unique(raw.budgets.iter().map(|b| b.root.as_str()), "budgets.root")?;
    for budget in &raw.budgets {
        if budget.name.trim().is_empty()
            || budget.total_bytes.0 == 0
            || budget.reserved_free_bytes.0 == 0
        {
            return err("CONFIG_INVALID_FILESYSTEM_BUDGET", "budgets");
        }
        let used = budget
            .sqlite_wal_temp_bytes
            .0
            .checked_add(budget.capture_spool_bytes.0)
            .and_then(|v| v.checked_add(budget.concurrent_backfill_bytes.0))
            .and_then(|v| v.checked_add(budget.archive_segment_bytes.0))
            .and_then(|v| v.checked_add(budget.reserved_free_bytes.0))
            .ok_or(ConfigError {
                code: "CONFIG_NUMERIC_OVERFLOW",
                field: "budgets",
            })?;
        if used > budget.total_bytes.0 {
            return err("CONFIG_BUDGET_EXCEEDED", "budgets");
        }
        safe_path(&budget.root, "budgets.root")?;
    }

    // Budget roots are the explicit path-to-physical-filesystem association. The
    // most-specific root wins so a nested mount (for example state/tmp) can have
    // an independent budget without double-counting it against state.
    let sqlite_path = Path::new(&raw.storage.sqlite_path);
    let sqlite_root = sqlite_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let sqlite_budget = filesystem_budget_for_path(&raw.budgets, sqlite_root)?;
    let temp_budget =
        filesystem_budget_for_path(&raw.budgets, Path::new(&raw.storage.sqlite_temp_path))?;
    let spool_budget =
        filesystem_budget_for_path(&raw.budgets, Path::new(&raw.storage.spool_path))?;
    let archive_budget = filesystem_budget_for_path(&raw.budgets, Path::new(&raw.archive.root))?;

    let associations = [sqlite_budget, temp_budget, spool_budget, archive_budget];
    let associated: BTreeSet<_> = associations.into_iter().collect();
    if associated.len() != raw.budgets.len() {
        return err("CONFIG_UNASSOCIATED_FILESYSTEM_BUDGET", "budgets.root");
    }
    if raw.budgets[sqlite_budget].sqlite_wal_temp_bytes.0 == 0
        || raw.budgets[temp_budget].sqlite_wal_temp_bytes.0 == 0
        || raw.budgets[sqlite_budget].concurrent_backfill_bytes.0 == 0
        || raw.budgets[spool_budget].capture_spool_bytes.0 == 0
        || raw.budgets[archive_budget].archive_segment_bytes.0 == 0
    {
        return err("CONFIG_ZERO_FILESYSTEM_RESERVATION", "budgets");
    }
    if [sqlite_budget, temp_budget, spool_budget].contains(&archive_budget)
        && raw.storage.filesystem != raw.archive.filesystem
    {
        return err(
            "CONFIG_FILESYSTEM_ASSOCIATION_MISMATCH",
            "archive.filesystem",
        );
    }
    if raw.limits.max_wire_frame_bytes.0 == 0
        || raw.limits.max_event_bytes.0 == 0
        || raw.limits.max_row_bytes.0 == 0
        || raw.limits.max_transaction_bytes.0 == 0
        || raw.limits.process_memory_bytes.0 == 0
    {
        return err("CONFIG_ZERO_BOUND", "limits");
    }
    if raw.limits.max_row_bytes.0 > raw.limits.max_event_bytes.0
        || raw.limits.max_event_bytes.0 > raw.limits.max_transaction_bytes.0
        || raw.limits.max_wire_frame_bytes.0 > raw.limits.process_memory_bytes.0
        || raw.limits.max_transaction_events == 0
    {
        return err("CONFIG_INCOMPATIBLE_LIMITS", "limits");
    }
    if raw.conditions.freshness_ms.0 == 0 || raw.conditions.hysteresis_ms.0 == 0 {
        return err("CONFIG_ZERO_BOUND", "conditions");
    }
    if raw.operator.max_request_bytes.0 == 0
        || raw.operator.max_response_bytes.0 == 0
        || raw.operator.read_timeout_ms.0 == 0
        || raw.operator.write_timeout_ms.0 == 0
        || raw.operator.result_retention_ms.0 == 0
        || raw.operator.confirmation_expiry_ms.0 == 0
    {
        return err("CONFIG_ZERO_BOUND", "operator");
    }
    if !matches!(
        raw.observability.log_level.as_str(),
        "error" | "warn" | "info" | "debug"
    ) {
        return err("CONFIG_UNSUPPORTED_LOG_LEVEL", "observability.log_level");
    }
    if !(raw.conditions.warning < raw.conditions.action
        && raw.conditions.action < raw.conditions.critical
        && raw.conditions.critical < raw.conditions.hard)
    {
        return err("CONFIG_INVALID_THRESHOLDS", "conditions");
    }
    if raw.retention.replay_window_ms.0 == 0
        || raw.retention.usable_anchor_count == 0
        || raw.retention.invalid_generation_ms.0 == 0
        || raw.retention.metadata_gc_ms.0 == 0
    {
        return err("CONFIG_ZERO_BOUND", "retention");
    }
    if raw.wal.max_slot_wal_keep_bytes.0 == 0
        || raw.wal.source_free_bytes.0 == 0
        || raw.wal.production_bytes_per_second.0 == 0
        || raw.wal.monitor_delay_ms.0 == 0
        || raw.wal.reaction_reserve_bytes.0 == 0
        || raw.wal.missing_metric_policy != "block_new_bootstrap"
    {
        return err("CONFIG_UNSUPPORTED_WAL_POLICY", "wal");
    }
    if raw.backfill.concurrency == 0
        || raw.backfill.chunk_rows == 0
        || raw.backfill.chunk_bytes.0 == 0
        || raw.backfill.chunk_duration_ms.0 == 0
        || raw.backfill.exporter_lifetime_ms.0 == 0
        || raw.backfill.importer_lifetime_ms.0 == 0
        || raw.backfill.guard_lifetime_ms.0 == 0
        || raw.backfill.guard_keepalive_ms.0 == 0
        || raw.backfill.session_timeout_ms.0 == 0
        || raw.backfill.ddl_waiter_bound_ms.0 == 0
        || raw.backfill.source_impact_bytes.0 == 0
        || raw.backfill.guard_keepalive_ms.0 >= raw.backfill.session_timeout_ms.0
    {
        return err("CONFIG_INVALID_BACKFILL_BOUNDS", "backfill");
    }
    if raw.source.lock_probe_interval_ms.0 >= raw.source.ownership_deadline_ms.0
        || raw.source.ownership_deadline_ms.0 > raw.source.stale_owner_takeover_ms.0
        || raw.source.maximum_operation_ms.0 > raw.source.stale_owner_takeover_ms.0
    {
        return err("CONFIG_INVALID_OWNERSHIP_BOUNDS", "source");
    }
    let expected_replication_options = [
        "binary=false",
        "proto_version=1",
        "streaming=false",
        "two_phase=false",
    ];
    if raw.source.origin_policy != "any"
        || raw
            .source
            .start_replication_options
            .iter()
            .map(String::as_str)
            .ne(expected_replication_options)
    {
        return err("CONFIG_UNSUPPORTED_REPLICATION_OPTIONS", "source");
    }
    for path in [
        &raw.storage.sqlite_path,
        &raw.storage.sqlite_temp_path,
        &raw.storage.spool_path,
        &raw.archive.root,
        &raw.operator.socket_path,
    ] {
        safe_path(path, "path")?;
    }
    safe_operator_path(&raw.operator.socket_path)?;
    if !matches!(raw.storage.filesystem.as_str(), "ext4" | "xfs")
        || raw.storage.checkpoint_pages == 0
        || raw.storage.vacuum_pages == 0
    {
        return err("CONFIG_UNSUPPORTED_STORAGE_POLICY", "storage");
    }
    if raw.storage.journal_mode != "WAL"
        || raw.storage.synchronous != "FULL"
        || raw.storage.auto_vacuum != "INCREMENTAL"
    {
        return err("CONFIG_UNSAFE_SQLITE_POLICY", "storage");
    }
    if raw.operator.directory_mode != 0o700
        || raw.operator.socket_mode != 0o600
        || !raw.operator.peer_credentials
    {
        return err("CONFIG_UNSAFE_OPERATOR_ENDPOINT", "operator");
    }
    if (!raw.source.verify_tls || !raw.clickhouse.verify_tls)
        && !raw.storage.unsafe_local_experiment
    {
        return err("CONFIG_TLS_REQUIRED", "source_or_clickhouse");
    }
    validate_listener(&raw.observability.status_listen_addr, &raw.observability)?;
    validate_listener(
        &raw.observability.prometheus_listen_addr,
        &raw.observability,
    )?;
    // M0-PROVISIONAL: boring-cdc-d-ch-accept recommended concrete pins pending approval.
    if raw.clickhouse.server_version != "25.8.2.29"
        || raw.clickhouse.client_version != "0.2.0"
        || raw.clickhouse.contract_id != "clickhouse-v1"
        || raw.clickhouse.selector_policy != "greatest-valid-fence"
        || raw.clickhouse.rust_insert_finalization != "end-and-wait"
        || raw.clickhouse.history_object.is_empty()
        || raw.clickhouse.selector_object.is_empty()
        || raw.clickhouse.history_quota_bytes.0 == 0
        || raw.clickhouse.destination_generation == 0
        || raw.clickhouse.retirement_grace_ms.0 == 0
        || raw.clickhouse.audit_bytes.0 == 0
        || raw.clickhouse.audit_events == 0
        || raw.clickhouse.audit_time_ms.0 == 0
        || raw.clickhouse.audit_cadence_ms.0 == 0
        || raw.clickhouse.audit_freshness_ms.0 == 0
    {
        return err("CONFIG_UNSUPPORTED_CLICKHOUSE_POLICY", "clickhouse");
    }
    if !raw.clickhouse.endpoint.starts_with("https://")
        || raw.clickhouse.endpoint.contains('@')
        || raw.clickhouse.endpoint.contains('?')
    {
        return err("CONFIG_UNSAFE_CLICKHOUSE_ENDPOINT", "clickhouse.endpoint");
    }
    if !raw.clickhouse.synchronous_insert || !raw.clickhouse.fsync_after_insert {
        return err("CONFIG_UNSAFE_CLICKHOUSE_DURABILITY", "clickhouse");
    }
    // M0-PROVISIONAL: boring-cdc-d-archive-durability recommended concrete writer pins.
    if !matches!(raw.archive.filesystem.as_str(), "ext4" | "xfs")
        || raw.archive.budget_bytes.0 == 0
        || raw.archive.schedule_ms.0 == 0
        || raw.archive.writer_crate != "parquet"
        || raw.archive.writer_version != "57.0.0"
        || raw.archive.compression != "zstd:3"
        || raw.archive.segment_bytes.0 == 0
        || raw.archive.selector_policy != "immutable_promotion_fence"
        || raw.archive.continuity_break_policy != "new_destination_identity"
        || raw.archive.audit_bytes.0 == 0
        || raw.archive.audit_events == 0
        || raw.archive.audit_time_ms.0 == 0
        || raw.archive.audit_cadence_ms.0 == 0
        || raw.archive.audit_freshness_ms.0 == 0
    {
        return err("CONFIG_UNSUPPORTED_ARCHIVE_POLICY", "archive");
    }
    if raw.archive.formats != BTreeSet::from(["jsonl".into(), "parquet".into()])
        || raw.archive.ready_marker != "SEGMENT_READY"
        || raw.archive.path_encoding != "opaque_ascii"
        || raw.archive.filesystem_policy != "descriptor_relative_no_follow_exclusive"
    {
        return err("CONFIG_UNSUPPORTED_ARCHIVE_POLICY", "archive");
    }
    Ok(())
}

fn validate_replica_key(table: &Table) -> Result<(), ConfigError> {
    if table.replica_key.is_empty() {
        return err("CONFIG_EMPTY_REPLICA_KEY", "tables.replica_key");
    }

    let mut components = BTreeSet::new();
    for component in &table.replica_key {
        if component.trim().is_empty() {
            return err("CONFIG_EMPTY_REPLICA_KEY_COMPONENT", "tables.replica_key");
        }
        if !components.insert(component.as_str()) {
            return err(
                "CONFIG_DUPLICATE_REPLICA_KEY_COMPONENT",
                "tables.replica_key",
            );
        }
        let Some(column_contract) = table.relation_contract.get(component) else {
            return err(
                "CONFIG_REPLICA_KEY_COMPONENT_NOT_IN_RELATION_CONTRACT",
                "tables.replica_key",
            );
        };
        if !column_contract.ends_with(":not-null") {
            return err(
                "CONFIG_NULLABLE_REPLICA_KEY_COMPONENT",
                "tables.replica_key",
            );
        }
    }
    Ok(())
}

fn validate_secret_references(references: [&str; 5]) -> Result<(), ConfigError> {
    if references
        .iter()
        .any(|reference| !valid_env_name(reference))
    {
        return err("CONFIG_INVALID_SECRET_REFERENCE", "secret_reference");
    }
    let mut seen = BTreeSet::new();
    if references.iter().any(|reference| !seen.insert(*reference)) {
        return err("CONFIG_ALIASED_SECRET_REFERENCE", "secret_reference");
    }
    if references
        .iter()
        .any(|reference| APPROVED_OVERRIDES.contains(reference))
    {
        return err(
            "CONFIG_SECRET_REFERENCE_COLLIDES_WITH_OVERRIDE",
            "secret_reference",
        );
    }
    Ok(())
}

fn validate_listener(value: &str, config: &Observability) -> Result<(), ConfigError> {
    let address: SocketAddr = value.parse().map_err(|_| ConfigError {
        code: "CONFIG_INVALID_LISTENER",
        field: "observability",
    })?;
    if !address.ip().is_loopback() && (!config.authentication || !config.tls) {
        return err("CONFIG_EXPOSED_LISTENER_REQUIRES_AUTH_TLS", "observability");
    }
    Ok(())
}

fn safe_operator_path(value: &str) -> Result<(), ConfigError> {
    let path = Path::new(value);
    safe_path(value, "operator.socket_path")?;
    let normal_components = path
        .components()
        .filter(|part| matches!(part, Component::Normal(_)))
        .count();
    if path.is_absolute()
        || normal_components < 3
        || path.extension().and_then(|v| v.to_str()) != Some("sock")
    {
        return err("CONFIG_UNSAFE_OPERATOR_ENDPOINT", "operator.socket_path");
    }
    Ok(())
}

fn safe_path(value: &str, field: &'static str) -> Result<(), ConfigError> {
    if value.is_empty()
        || value.contains('\0')
        || Path::new(value)
            .components()
            .any(|c| matches!(c, Component::ParentDir))
    {
        return Err(ConfigError {
            code: "CONFIG_UNSAFE_PATH",
            field,
        });
    }
    Ok(())
}

fn filesystem_budget_for_path(
    budgets: &[FilesystemBudget],
    path: &Path,
) -> Result<usize, ConfigError> {
    budgets
        .iter()
        .enumerate()
        .filter(|(_, budget)| {
            (budget.root == "." && !path.is_absolute()) || path.starts_with(Path::new(&budget.root))
        })
        .max_by_key(|(_, budget)| {
            if budget.root == "." {
                0
            } else {
                Path::new(&budget.root).components().count()
            }
        })
        .map(|(index, _)| index)
        .ok_or(ConfigError {
            code: "CONFIG_UNCOVERED_FILESYSTEM_PATH",
            field: "budgets.root",
        })
}

fn unique<'a>(
    mut values: impl Iterator<Item = &'a str>,
    field: &'static str,
) -> Result<(), ConfigError> {
    let mut seen = BTreeSet::new();
    if values.any(|v| !seen.insert(v)) {
        err("CONFIG_DUPLICATE_ID", field)
    } else {
        Ok(())
    }
}

fn err<T>(code: &'static str, field: &'static str) -> Result<T, ConfigError> {
    Err(ConfigError { code, field })
}

fn into_public(raw: RawConfig) -> PublicConfig {
    PublicConfig {
        schema_version: raw.schema_version,
        source: SourceConfig {
            publication: raw.source.publication,
            slot: raw.source.slot,
            copy_both_transport: raw.source.copy_both_transport,
            start_replication_options: raw.source.start_replication_options,
            origin_policy: raw.source.origin_policy,
            advisory_lock_derivation: raw.source.advisory_lock_derivation,
            lock_probe_interval_ms: raw.source.lock_probe_interval_ms,
            ownership_deadline_ms: raw.source.ownership_deadline_ms,
            maximum_operation_ms: raw.source.maximum_operation_ms,
            stale_owner_takeover_ms: raw.source.stale_owner_takeover_ms,
            heartbeat_cadence_ms: raw.source.heartbeat_cadence_ms,
            verify_tls: raw.source.verify_tls,
        },
        tables: raw.tables,
        storage: raw.storage,
        limits: raw.limits,
        budgets: raw.budgets,
        retention: raw.retention,
        wal: raw.wal,
        backfill: raw.backfill,
        clickhouse: ClickHouseConfig {
            endpoint: raw.clickhouse.endpoint,
            server_version: raw.clickhouse.server_version,
            client_version: raw.clickhouse.client_version,
            contract_id: raw.clickhouse.contract_id,
            history_object: raw.clickhouse.history_object,
            selector_object: raw.clickhouse.selector_object,
            history_quota_bytes: raw.clickhouse.history_quota_bytes,
            synchronous_insert: raw.clickhouse.synchronous_insert,
            fsync_after_insert: raw.clickhouse.fsync_after_insert,
            rust_insert_finalization: raw.clickhouse.rust_insert_finalization,
            destination_generation: raw.clickhouse.destination_generation,
            selector_policy: raw.clickhouse.selector_policy,
            adopted_external_fence: raw.clickhouse.adopted_external_fence,
            retirement_grace_ms: raw.clickhouse.retirement_grace_ms,
            audit_bytes: raw.clickhouse.audit_bytes,
            audit_events: raw.clickhouse.audit_events,
            audit_time_ms: raw.clickhouse.audit_time_ms,
            audit_cadence_ms: raw.clickhouse.audit_cadence_ms,
            audit_freshness_ms: raw.clickhouse.audit_freshness_ms,
            verify_tls: raw.clickhouse.verify_tls,
        },
        archive: raw.archive,
        conditions: raw.conditions,
        observability: raw.observability,
        operator: raw.operator,
    }
}

fn fingerprints(config: &PublicConfig) -> Result<Fingerprints, ConfigError> {
    let sqlite_path = Path::new(&config.storage.sqlite_path);
    let sqlite_root = sqlite_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let state_budget = &config.budgets[filesystem_budget_for_path(&config.budgets, sqlite_root)?];
    let archive_budget = &config.budgets
        [filesystem_budget_for_path(&config.budgets, Path::new(&config.archive.root))?];

    Ok(Fingerprints {
        source: digest(&(&config.source, &config.tables))?,
        table_set: digest(&config.tables)?,
        destination: digest(&config.clickhouse)?,
        archive: digest(&(&config.archive, archive_budget))?,
        promotion: digest(&serde_json::json!({
            "clickhouse_generation": config.clickhouse.destination_generation,
            "clickhouse_selector": config.clickhouse.selector_policy,
            "adopted_external_fence": config.clickhouse.adopted_external_fence,
            "retirement_grace_ms": config.clickhouse.retirement_grace_ms,
            "archive_selector": config.archive.selector_policy,
            "archive_continuity": config.archive.continuity_break_policy,
        }))?,
        backfill: digest(&(&config.backfill, &config.retention, state_budget))?,
        runtime: digest(config)?,
    })
}

fn digest(value: &impl Serialize) -> Result<String, ConfigError> {
    let bytes = serde_json::to_vec(value).map_err(|_| ConfigError {
        code: "CONFIG_CANONICALIZATION_FAILED",
        field: "config",
    })?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[derive(Default)]
    struct Env(BTreeMap<String, String>);
    impl Environment for Env {
        fn get(&self, name: &str) -> Option<String> {
            self.0.get(name).cloned()
        }
        fn names(&self) -> Vec<String> {
            self.0.keys().cloned().collect()
        }
    }

    fn env() -> Env {
        Env(BTreeMap::from([
            (
                "PG_RUNTIME".into(),
                "postgres://runtime:secret@source/db".into(),
            ),
            (
                "PG_CONTROL".into(),
                "postgres://control:secret@source/db".into(),
            ),
            (
                "PG_ADMIN".into(),
                "postgres://admin:secret@source/db".into(),
            ),
            (
                "CH_RUNTIME".into(),
                "https://runtime:secret@clickhouse".into(),
            ),
            ("CH_MAINT".into(), "https://maint:secret@clickhouse".into()),
        ]))
    }

    fn fixture() -> String {
        include_str!("../tests/fixtures/m1_config/representative.toml").into()
    }

    fn separate_filesystem_fixture() -> String {
        let separate_budgets = r#"[[budgets]]
name = "state"
root = "state"
total_bytes = 500000000
reserved_free_bytes = 100000000
sqlite_wal_temp_bytes = 100000000
capture_spool_bytes = 0
concurrent_backfill_bytes = 200000000
archive_segment_bytes = 0

[[budgets]]
name = "temp"
root = "state/tmp"
total_bytes = 200000000
reserved_free_bytes = 100000000
sqlite_wal_temp_bytes = 100000000
capture_spool_bytes = 0
concurrent_backfill_bytes = 0
archive_segment_bytes = 0

[[budgets]]
name = "spool"
root = "state/spool"
total_bytes = 300000000
reserved_free_bytes = 100000000
sqlite_wal_temp_bytes = 0
capture_spool_bytes = 200000000
concurrent_backfill_bytes = 0
archive_segment_bytes = 0

[[budgets]]
name = "archive"
root = "archive/root"
total_bytes = 200000000
reserved_free_bytes = 100000000
sqlite_wal_temp_bytes = 0
capture_spool_bytes = 0
concurrent_backfill_bytes = 0
archive_segment_bytes = 100000000
"#;
        fixture().replacen(
            r#"[[budgets]]
name = "state"
root = "."
total_bytes = 1000000000
reserved_free_bytes = 100000000
sqlite_wal_temp_bytes = 100000000
capture_spool_bytes = 200000000
concurrent_backfill_bytes = 200000000
archive_segment_bytes = 100000000
"#,
            separate_budgets,
            1,
        )
    }

    #[test]
    fn equivalent_toml_and_table_order_have_stable_fingerprints() {
        let first = load_str(&fixture(), &env()).unwrap();
        let equivalent = fixture().replace(
            "relation_contract = { id = \"int8:not-null\", total = \"numeric:nullable\" }",
            "relation_contract = { total = \"numeric:nullable\", id = \"int8:not-null\" }",
        );
        let second = load_str(&equivalent, &env()).unwrap();
        assert_eq!(first.fingerprints(), second.fingerprints());
    }

    #[test]
    fn archive_and_promotion_have_distinct_compatibility_fingerprints() {
        let base = load_str(&fixture(), &env()).unwrap();
        let archive = load_str(
            &fixture().replace("segment_bytes = 67108864", "segment_bytes = 67108865"),
            &env(),
        )
        .unwrap();
        assert_ne!(base.fingerprints().archive, archive.fingerprints().archive);
        assert_eq!(
            base.fingerprints().promotion,
            archive.fingerprints().promotion
        );
        assert_eq!(
            base.fingerprints().destination,
            archive.fingerprints().destination
        );

        let promotion = load_str(
            &fixture().replace("adopted_external_fence = 0", "adopted_external_fence = 1"),
            &env(),
        )
        .unwrap();
        assert_ne!(
            base.fingerprints().promotion,
            promotion.fingerprints().promotion
        );
        assert_ne!(
            base.fingerprints().destination,
            promotion.fingerprints().destination
        );
        assert_eq!(
            base.fingerprints().archive,
            promotion.fingerprints().archive
        );
    }

    #[test]
    fn only_applicable_fingerprints_change() {
        let base = load_str(&fixture(), &env()).unwrap();
        let changed = load_str(
            &fixture().replace("chunk_rows = 1000", "chunk_rows = 1001"),
            &env(),
        )
        .unwrap();
        assert_eq!(base.fingerprints().source, changed.fingerprints().source);
        assert_eq!(
            base.fingerprints().destination,
            changed.fingerprints().destination
        );
        assert_ne!(
            base.fingerprints().backfill,
            changed.fingerprints().backfill
        );
        assert_ne!(base.fingerprints().runtime, changed.fingerprints().runtime);
    }

    #[test]
    fn precedence_is_defaults_then_toml_then_approved_environment() {
        let defaults = load_str(&fixture(), &env()).unwrap();
        assert_eq!(defaults.public().observability.log_level, "info");
        let mut overridden = env();
        overridden
            .0
            .insert("BORING_CDC_LOG_LEVEL".into(), "warn".into());
        let loaded = load_str(&fixture(), &overridden).unwrap();
        assert_eq!(loaded.public().observability.log_level, "warn");
    }

    #[test]
    fn rejects_unknown_deprecated_malformed_and_truncated_input() {
        for bad in [
            fixture().replace(
                "schema_version = 1",
                "schema_version = 1\ndynamic_reload = true",
            ),
            fixture().replace("schema_version = 1", "schema_version = 1\nplugin = \"x\""),
            "schema_version =".into(),
            fixture()[..fixture().len() / 2].into(),
        ] {
            assert_eq!(
                load_str(&bad, &env()).unwrap_err().code,
                "CONFIG_INVALID_TOML_OR_UNKNOWN_FIELD"
            );
        }
    }

    #[test]
    fn missing_secret_and_unapproved_override_fail_without_disclosing_names() {
        let missing = Env::default();
        let error = load_str(&fixture(), &missing).unwrap_err();
        assert_eq!(error.code, "CONFIG_MISSING_SECRET");
        assert!(!error.to_string().contains("PG_RUNTIME"));
        let mut bad = env();
        bad.0.insert("BORING_CDC_PLUGIN".into(), "bad".into());
        assert_eq!(
            load_str(&fixture(), &bad).unwrap_err().code,
            "CONFIG_UNAPPROVED_ENV_OVERRIDE"
        );
    }

    #[test]
    fn redacted_output_and_debug_never_contain_secret_names_or_values() {
        let loaded = load_str(&fixture(), &env()).unwrap();
        let output = format!("{:?}\n{}", loaded, loaded.redacted_diagnostics());
        for forbidden in [
            "PG_RUNTIME",
            "PG_CONTROL",
            "PG_ADMIN",
            "CH_RUNTIME",
            "CH_MAINT",
            "runtime:secret",
            "admin:secret",
            "boring_publication",
            "public.orders",
            "state/boring.db",
            "archive/root",
        ] {
            assert!(!output.contains(forbidden), "leaked {forbidden}");
        }
    }

    #[test]
    fn unicode_identifiers_are_canonical_but_parent_and_nul_paths_fail() {
        let unicode = load_str(&fixture().replace("orders.public", "订单.public"), &env()).unwrap();
        assert!(!unicode.fingerprints().table_set.is_empty());
        let parent = fixture().replace("state/boring.db", "state/../boring.db");
        assert_eq!(
            load_str(&parent, &env()).unwrap_err().code,
            "CONFIG_UNSAFE_PATH"
        );
    }

    #[test]
    fn numeric_overflow_and_budget_overcommit_fail_closed() {
        let overflow = fixture()
            .replace(
                "total_bytes = 1000000000",
                "total_bytes = 18446744073709551615",
            )
            .replace(
                "reserved_free_bytes = 100000000",
                "reserved_free_bytes = 18446744073709551615",
            );
        assert_eq!(
            load_str(&overflow, &env()).unwrap_err().code,
            "CONFIG_NUMERIC_OVERFLOW"
        );
        let over = fixture().replace("total_bytes = 1000000000", "total_bytes = 100");
        assert_eq!(
            load_str(&over, &env()).unwrap_err().code,
            "CONFIG_BUDGET_EXCEEDED"
        );
    }

    #[test]
    fn shared_and_separate_filesystem_budgets_cover_every_configured_path() {
        let shared = load_str(&fixture(), &env()).unwrap();
        assert_eq!(shared.public().budgets.len(), 1);

        let loaded = load_str(&separate_filesystem_fixture(), &env()).unwrap();
        assert_eq!(loaded.public().budgets.len(), 4);
    }

    #[test]
    fn filesystem_budget_changes_affect_only_applicable_domain_fingerprints() {
        let base = load_str(&separate_filesystem_fixture(), &env()).unwrap();
        let archive_changed = load_str(
            &separate_filesystem_fixture().replace(
                "total_bytes = 200000000\nreserved_free_bytes = 100000000\nsqlite_wal_temp_bytes = 0",
                "total_bytes = 200000001\nreserved_free_bytes = 100000000\nsqlite_wal_temp_bytes = 0",
            ),
            &env(),
        )
        .unwrap();
        assert_ne!(
            base.fingerprints().archive,
            archive_changed.fingerprints().archive
        );
        assert_eq!(
            base.fingerprints().backfill,
            archive_changed.fingerprints().backfill
        );
        assert_ne!(
            base.fingerprints().runtime,
            archive_changed.fingerprints().runtime
        );

        let state_changed = load_str(
            &separate_filesystem_fixture()
                .replace("total_bytes = 500000000", "total_bytes = 500000001"),
            &env(),
        )
        .unwrap();
        assert_eq!(
            base.fingerprints().archive,
            state_changed.fingerprints().archive
        );
        assert_ne!(
            base.fingerprints().backfill,
            state_changed.fingerprints().backfill
        );
        assert_ne!(
            base.fingerprints().runtime,
            state_changed.fingerprints().runtime
        );

        for name in ["temp", "spool"] {
            let from = format!(
                "name = {name:?}\nroot = {root:?}\ntotal_bytes = {total}",
                root = if name == "temp" {
                    "state/tmp"
                } else {
                    "state/spool"
                },
                total = if name == "temp" { 200000000 } else { 300000000 }
            );
            let to = from.replace(
                if name == "temp" {
                    "200000000"
                } else {
                    "300000000"
                },
                if name == "temp" {
                    "200000001"
                } else {
                    "300000001"
                },
            );
            let changed =
                load_str(&separate_filesystem_fixture().replace(&from, &to), &env()).unwrap();
            assert_eq!(base.fingerprints().source, changed.fingerprints().source);
            assert_eq!(
                base.fingerprints().table_set,
                changed.fingerprints().table_set
            );
            assert_eq!(
                base.fingerprints().destination,
                changed.fingerprints().destination
            );
            assert_eq!(base.fingerprints().archive, changed.fingerprints().archive);
            assert_eq!(
                base.fingerprints().promotion,
                changed.fingerprints().promotion
            );
            assert_eq!(
                base.fingerprints().backfill,
                changed.fingerprints().backfill
            );
            assert_ne!(base.fingerprints().runtime, changed.fingerprints().runtime);
        }
    }

    #[test]
    fn filesystem_budgets_are_nonempty_named_and_uniquely_rooted() {
        let budget_block = r#"[[budgets]]
name = "state"
root = "."
total_bytes = 1000000000
reserved_free_bytes = 100000000
sqlite_wal_temp_bytes = 100000000
capture_spool_bytes = 200000000
concurrent_backfill_bytes = 200000000
archive_segment_bytes = 100000000

"#;
        let empty = fixture()
            .replace("schema_version = 1", "schema_version = 1\nbudgets = []")
            .replace(budget_block, "");
        assert_eq!(
            load_str(&empty, &env()).unwrap_err().code,
            "CONFIG_EMPTY_FILESYSTEM_BUDGETS"
        );
        let unnamed = fixture().replace("name = \"state\"", "name = \"\"");
        assert_eq!(
            load_str(&unnamed, &env()).unwrap_err().code,
            "CONFIG_INVALID_FILESYSTEM_BUDGET"
        );
        let zero_total = fixture().replace("total_bytes = 1000000000", "total_bytes = 0");
        assert_eq!(
            load_str(&zero_total, &env()).unwrap_err().code,
            "CONFIG_INVALID_FILESYSTEM_BUDGET"
        );
        let duplicate_name =
            fixture().replace("[retention]", &format!("{}\n[retention]", budget_block));
        assert_eq!(
            load_str(&duplicate_name, &env()).unwrap_err().code,
            "CONFIG_DUPLICATE_ID"
        );
        let duplicate_root = fixture().replace(
            "[retention]",
            &format!(
                "{}\n[retention]",
                budget_block.replace("name = \"state\"", "name = \"other\"")
            ),
        );
        assert_eq!(
            load_str(&duplicate_root, &env()).unwrap_err().code,
            "CONFIG_DUPLICATE_ID"
        );
    }

    #[test]
    fn filesystem_budget_reservations_are_nonzero_for_associated_roles() {
        for (term, code) in [
            (
                "reserved_free_bytes = 100000000",
                "CONFIG_INVALID_FILESYSTEM_BUDGET",
            ),
            (
                "sqlite_wal_temp_bytes = 100000000",
                "CONFIG_ZERO_FILESYSTEM_RESERVATION",
            ),
            (
                "capture_spool_bytes = 200000000",
                "CONFIG_ZERO_FILESYSTEM_RESERVATION",
            ),
            (
                "concurrent_backfill_bytes = 200000000",
                "CONFIG_ZERO_FILESYSTEM_RESERVATION",
            ),
            (
                "archive_segment_bytes = 100000000",
                "CONFIG_ZERO_FILESYSTEM_RESERVATION",
            ),
        ] {
            let candidate =
                fixture().replace(term, &format!("{} = 0", term.split(" = ").next().unwrap()));
            assert_eq!(
                load_str(&candidate, &env()).unwrap_err().code,
                code,
                "{term}"
            );
        }
    }

    #[test]
    fn uncovered_unassociated_and_mismatched_filesystems_fail_closed() {
        let uncovered = fixture().replace("root = \".\"", "root = \"state\"");
        assert_eq!(
            load_str(&uncovered, &env()).unwrap_err().code,
            "CONFIG_UNCOVERED_FILESYSTEM_PATH"
        );

        let database_file_is_not_a_filesystem_root =
            fixture().replace("root = \".\"", "root = \"state/boring.db\"");
        assert_eq!(
            load_str(&database_file_is_not_a_filesystem_root, &env())
                .unwrap_err()
                .code,
            "CONFIG_UNCOVERED_FILESYSTEM_PATH"
        );
        let relative_root_does_not_cover_absolute_paths =
            fixture().replace("root = \"archive/root\"", "root = \"/archive/root\"");
        assert_eq!(
            load_str(&relative_root_does_not_cover_absolute_paths, &env())
                .unwrap_err()
                .code,
            "CONFIG_UNCOVERED_FILESYSTEM_PATH"
        );

        let unrelated = fixture().replace(
            "[retention]",
            r#"[[budgets]]
name = "unrelated"
root = "elsewhere"
total_bytes = 2
reserved_free_bytes = 1
sqlite_wal_temp_bytes = 0
capture_spool_bytes = 0
concurrent_backfill_bytes = 0
archive_segment_bytes = 0

[retention]"#,
        );
        assert_eq!(
            load_str(&unrelated, &env()).unwrap_err().code,
            "CONFIG_UNASSOCIATED_FILESYSTEM_BUDGET"
        );

        let mismatched = fixture().replacen("filesystem = \"ext4\"", "filesystem = \"xfs\"", 1);
        assert_eq!(
            load_str(&mismatched, &env()).unwrap_err().code,
            "CONFIG_FILESYSTEM_ASSOCIATION_MISMATCH"
        );
    }

    #[test]
    fn exposed_listener_and_insecure_connections_require_explicit_safety() {
        let mut exposed_env = env();
        exposed_env.0.insert(
            "BORING_CDC_STATUS_LISTEN_ADDR".into(),
            "0.0.0.0:8787".into(),
        );
        assert_eq!(
            load_str(&fixture(), &exposed_env).unwrap_err().code,
            "CONFIG_EXPOSED_LISTENER_REQUIRES_AUTH_TLS"
        );
        let insecure = fixture().replace("verify_tls = true", "verify_tls = false");
        assert_eq!(
            load_str(&insecure, &env()).unwrap_err().code,
            "CONFIG_TLS_REQUIRED"
        );
    }

    #[test]
    fn command_shapes_share_parser_but_load_only_authorized_secrets() {
        let status = load_str_for(&fixture(), &Env::default(), LoadPurpose::Status).unwrap();
        assert!(status.runtime_dsn().is_none());
        assert!(status.administration_dsn().is_none());

        let run = load_str_for(&fixture(), &env(), LoadPurpose::Run).unwrap();
        assert!(run.runtime_dsn().is_some());
        assert!(run.administration_dsn().is_none());
        assert!(run.clickhouse_maintenance_dsn().is_none());

        let postgres_admin = load_str_for(&fixture(), &env(), LoadPurpose::PostgresAdmin).unwrap();
        assert!(postgres_admin.administration_dsn().is_some());
        assert!(postgres_admin.runtime_dsn().is_none());
        assert!(postgres_admin.control_writer_dsn().is_none());
        assert!(postgres_admin.clickhouse_maintenance_dsn().is_none());

        let clickhouse =
            load_str_for(&fixture(), &env(), LoadPurpose::ClickHouseMaintenance).unwrap();
        assert!(clickhouse.clickhouse_maintenance_dsn().is_some());
        assert!(clickhouse.administration_dsn().is_none());
        assert!(clickhouse.runtime_dsn().is_none());
    }

    #[test]
    fn secret_rotation_does_not_change_any_fingerprint() {
        let first = load_str(&fixture(), &env()).unwrap();
        let mut rotated = env();
        rotated
            .0
            .insert("PG_RUNTIME".into(), "postgres://rotated".into());
        let second = load_str(&fixture(), &rotated).unwrap();
        assert_eq!(first.fingerprints(), second.fingerprints());
    }

    #[test]
    fn semantic_validation_matrix_fails_closed_with_stable_codes() {
        let cases = [
            (
                "schema_version = 1",
                "schema_version = 2",
                "CONFIG_UNSUPPORTED_SCHEMA",
            ),
            (
                "logical_id = \"orders\"",
                "logical_id = \"\"",
                "CONFIG_INVALID_TABLE_IDENTITY",
            ),
            (
                "journal_mode = \"WAL\"",
                "journal_mode = \"DELETE\"",
                "CONFIG_UNSAFE_SQLITE_POLICY",
            ),
            (
                "max_row_bytes = 2097152",
                "max_row_bytes = 0",
                "CONFIG_ZERO_BOUND",
            ),
            ("warning = 60", "warning = 99", "CONFIG_INVALID_THRESHOLDS"),
            (
                "concurrency = 2",
                "concurrency = 0",
                "CONFIG_INVALID_BACKFILL_BOUNDS",
            ),
            (
                "origin_policy = \"any\"",
                "origin_policy = \"none\"",
                "CONFIG_UNSUPPORTED_REPLICATION_OPTIONS",
            ),
            (
                "socket_mode = 384",
                "socket_mode = 420",
                "CONFIG_UNSAFE_OPERATOR_ENDPOINT",
            ),
            (
                "synchronous_insert = true",
                "synchronous_insert = false",
                "CONFIG_UNSAFE_CLICKHOUSE_DURABILITY",
            ),
            (
                "ready_marker = \"SEGMENT_READY\"",
                "ready_marker = \"READY\"",
                "CONFIG_UNSUPPORTED_ARCHIVE_POLICY",
            ),
        ];
        for (from, to, code) in cases {
            let candidate = fixture().replace(from, to);
            assert_eq!(
                load_str(&candidate, &env()).unwrap_err().code,
                code,
                "{from}"
            );
        }
    }

    #[test]
    fn composite_replica_key_matches_its_relation_contract() {
        let composite = fixture().replace(
            "replica_key = [\"id\"]",
            "replica_key = [\"tenant_id\", \"id\"]",
        ).replace(
            "relation_contract = { id = \"int8:not-null\", total = \"numeric:nullable\" }",
            "relation_contract = { total = \"numeric:nullable\", id = \"int8:not-null\", tenant_id = \"uuid:not-null\" }",
        );
        let loaded = load_str(&composite, &env()).unwrap();
        assert_eq!(loaded.public().tables[0].replica_key, ["tenant_id", "id"]);
    }

    #[test]
    fn replica_key_components_fail_closed_with_specific_redacted_codes() {
        let cases = [
            (
                "replica_key = [\"id\"]",
                "replica_key = []",
                "CONFIG_EMPTY_REPLICA_KEY",
            ),
            (
                "replica_key = [\"id\"]",
                "replica_key = [\"id\", \"\"]",
                "CONFIG_EMPTY_REPLICA_KEY_COMPONENT",
            ),
            (
                "replica_key = [\"id\"]",
                "replica_key = [\"id\", \"id\"]",
                "CONFIG_DUPLICATE_REPLICA_KEY_COMPONENT",
            ),
            (
                "replica_key = [\"id\"]",
                "replica_key = [\"id\", \"tenant_id\"]",
                "CONFIG_REPLICA_KEY_COMPONENT_NOT_IN_RELATION_CONTRACT",
            ),
            (
                "replica_key = [\"id\"]",
                "replica_key = [\"total\"]",
                "CONFIG_NULLABLE_REPLICA_KEY_COMPONENT",
            ),
        ];

        for (from, to, code) in cases {
            let error = load_str(&fixture().replace(from, to), &env()).unwrap_err();
            assert_eq!(error.code, code);
            assert_eq!(error.field, "tables.replica_key");
            let rendered = error.to_string();
            assert!(!rendered.contains("id"));
            assert!(!rendered.contains("tenant_id"));
            assert!(!rendered.contains("total"));
        }
    }

    #[test]
    fn table_canonicalization_preserves_composite_key_order_and_fingerprints() {
        let first_table = fixture().replace(
            "replica_key = [\"id\"]",
            "replica_key = [\"tenant_id\", \"id\"]",
        ).replace(
            "relation_contract = { id = \"int8:not-null\", total = \"numeric:nullable\" }",
            "relation_contract = { tenant_id = \"uuid:not-null\", id = \"int8:not-null\", total = \"numeric:nullable\" }",
        );
        let second_table = r#"[[tables]]
logical_id = "customers"
source_relation = "public.customers"
replica_key = ["region", "customer_id"]
key_type_policy = "canonical-v1"
relation_contract = { customer_id = "int8:not-null", region = "text:not-null", name = "text:nullable" }

"#;
        let forward = first_table.replace("[storage]", &format!("{second_table}[storage]"));
        // Build the reverse order without changing either table's composite-key order.
        let table_start = first_table.find("[[tables]]").unwrap();
        let storage_start = first_table.find("[storage]").unwrap();
        let prefix = &first_table[..table_start];
        let orders_table = &first_table[table_start..storage_start];
        let suffix = &first_table[storage_start..];
        let reverse = format!("{prefix}{second_table}{orders_table}{suffix}");

        let forward_loaded = load_str(&forward, &env()).unwrap();
        let reverse_loaded = load_str(&reverse, &env()).unwrap();
        assert_eq!(forward_loaded.fingerprints(), reverse_loaded.fingerprints());
        assert_eq!(
            forward_loaded.public().tables[1].replica_key,
            ["tenant_id", "id"]
        );
    }

    #[test]
    fn every_pair_of_secret_roles_requires_a_distinct_reference() {
        let roles = [
            ("runtime_dsn_env = \"PG_RUNTIME\"", "PG_RUNTIME"),
            ("control_writer_dsn_env = \"PG_CONTROL\"", "PG_CONTROL"),
            ("administration_dsn_env = \"PG_ADMIN\"", "PG_ADMIN"),
            ("runtime_dsn_env = \"CH_RUNTIME\"", "CH_RUNTIME"),
            ("maintenance_dsn_env = \"CH_MAINT\"", "CH_MAINT"),
        ];

        let mut tested_pairs = 0;
        for first in 0..roles.len() {
            for second in (first + 1)..roles.len() {
                let candidate = fixture().replacen(
                    roles[second].0,
                    &format!(
                        "{} = \"{}\"",
                        roles[second].0.split(" = ").next().unwrap(),
                        roles[first].1
                    ),
                    1,
                );
                let error =
                    load_str_for(&candidate, &Env::default(), LoadPurpose::Status).unwrap_err();
                assert_eq!(error.code, "CONFIG_ALIASED_SECRET_REFERENCE");
                assert_eq!(error.field, "secret_reference");
                let rendered = error.to_string();
                for (_, secret_name) in roles {
                    assert!(!rendered.contains(secret_name), "leaked {secret_name}");
                }
                tested_pairs += 1;
            }
        }
        assert_eq!(tested_pairs, 10);
    }

    #[test]
    fn secret_references_cannot_alias_public_override_names() {
        for override_name in APPROVED_OVERRIDES {
            let candidate = fixture().replace(
                "runtime_dsn_env = \"PG_RUNTIME\"",
                &format!("runtime_dsn_env = \"{override_name}\""),
            );
            let mut candidate_env = env();
            candidate_env
                .0
                .insert((*override_name).into(), "credential-value".into());
            let error = load_str(&candidate, &candidate_env).unwrap_err();
            assert_eq!(error.code, "CONFIG_SECRET_REFERENCE_COLLIDES_WITH_OVERRIDE");
            assert_eq!(error.field, "secret_reference");
            assert!(!error.to_string().contains(override_name));
            assert!(!error.to_string().contains("credential-value"));
        }

        let aliased = fixture()
            .replace(
                "runtime_dsn_env = \"PG_RUNTIME\"",
                "runtime_dsn_env = \"BORING_CDC_LOG_LEVEL\"",
            )
            .replace(
                "control_writer_dsn_env = \"PG_CONTROL\"",
                "control_writer_dsn_env = \"BORING_CDC_LOG_LEVEL\"",
            );
        assert_eq!(
            load_str_for(&aliased, &Env::default(), LoadPurpose::Status)
                .unwrap_err()
                .code,
            "CONFIG_ALIASED_SECRET_REFERENCE"
        );
    }

    #[test]
    fn secret_reference_names_and_values_do_not_affect_outputs() {
        let original = load_str(&fixture(), &env()).unwrap();
        let replacements = [
            ("PG_RUNTIME", "ALT_PG_RUNTIME"),
            ("PG_CONTROL", "ALT_PG_CONTROL"),
            ("PG_ADMIN", "ALT_PG_ADMIN"),
            ("CH_RUNTIME", "ALT_CH_RUNTIME"),
            ("CH_MAINT", "ALT_CH_MAINT"),
        ];
        let mut renamed_fixture = fixture();
        let mut renamed_env = Env::default();
        for (old_name, new_name) in replacements {
            renamed_fixture = renamed_fixture.replace(old_name, new_name);
            renamed_env
                .0
                .insert(new_name.into(), format!("secret-value-for-{new_name}"));
        }
        let renamed = load_str(&renamed_fixture, &renamed_env).unwrap();
        assert_eq!(original.fingerprints(), renamed.fingerprints());

        let output = format!("{:?}\n{}", renamed, renamed.redacted_diagnostics());
        for (_, secret_name) in replacements {
            assert!(!output.contains(secret_name), "leaked {secret_name}");
        }
        assert!(!output.contains("secret-value-for-"));
    }

    #[test]
    fn every_secret_reference_is_validated_even_when_not_loaded() {
        let malformed_admin = fixture().replace(
            "administration_dsn_env = \"PG_ADMIN\"",
            "administration_dsn_env = \"bad-name\"",
        );
        let error = load_str_for(&malformed_admin, &env(), LoadPurpose::Status).unwrap_err();
        assert_eq!(error.code, "CONFIG_INVALID_SECRET_REFERENCE");

        let malformed_maintenance = fixture().replace(
            "maintenance_dsn_env = \"CH_MAINT\"",
            "maintenance_dsn_env = \"also-bad\"",
        );
        let error = load_str_for(&malformed_maintenance, &env(), LoadPurpose::Run).unwrap_err();
        assert_eq!(error.code, "CONFIG_INVALID_SECRET_REFERENCE");
    }

    #[test]
    fn replication_options_are_an_exact_canonical_set() {
        for options in [
            r#"start_replication_options = ["streaming=false"]
"#,
            r#"start_replication_options = ["proto_version=1", "streaming=false", "two_phase=false", "binary=false", "messages=true"]
"#,
            r#"start_replication_options = ["proto_version=1", "streaming=false", "streaming=true", "two_phase=false", "binary=false"]
"#,
        ] {
            let candidate = fixture().replace(
                "start_replication_options = [\"proto_version=1\", \"streaming=false\", \"two_phase=false\", \"binary=false\"]\n",
                options,
            );
            assert_eq!(
                load_str(&candidate, &env()).unwrap_err().code,
                "CONFIG_UNSUPPORTED_REPLICATION_OPTIONS"
            );
        }
    }

    #[test]
    fn operator_socket_must_be_nested_relative_and_sock_typed() {
        for path in [
            "/tmp/operator.sock",
            "operator.sock",
            "run/operator",
            "../run/operator.sock",
        ] {
            let candidate = fixture().replace(
                "socket_path = \"run/boring-cdc/operator.sock\"",
                &format!("socket_path = {path:?}"),
            );
            let code = load_str(&candidate, &env()).unwrap_err().code;
            assert!(matches!(
                code,
                "CONFIG_UNSAFE_OPERATOR_ENDPOINT" | "CONFIG_UNSAFE_PATH"
            ));
        }
    }

    fn inventory_value_mutation(group: &str, field: &str, mutation: &str) -> String {
        let mut document: toml::Value = toml::from_str(&fixture()).unwrap();
        let target = if group == "boundary" {
            document.get_mut(field).unwrap()
        } else {
            let group_value = document.get_mut(group).unwrap();
            let table = match group_value {
                toml::Value::Array(values) => values[0].as_table_mut().unwrap(),
                toml::Value::Table(table) => table,
                _ => panic!("{group} is not a configuration group"),
            };
            table.get_mut(field).unwrap()
        };
        match mutation {
            "wrong_type" => {
                *target = match target {
                    toml::Value::String(_) => toml::Value::Integer(1),
                    toml::Value::Integer(_) => toml::Value::String("wrong-type".into()),
                    toml::Value::Boolean(_) => toml::Value::String("wrong-type".into()),
                    toml::Value::Array(_) | toml::Value::Table(_) => {
                        toml::Value::String("wrong-type".into())
                    }
                    value => panic!("unsupported inventory value type: {value:?}"),
                }
            }
            "constraint_violation" => match (group, field) {
                ("conditions", "warning") => *target = toml::Value::Integer(75),
                ("conditions", "action") => *target = toml::Value::Integer(60),
                ("conditions", "critical") => *target = toml::Value::Integer(75),
                ("conditions", "hard") => *target = toml::Value::Integer(90),
                ("observability", "authentication" | "tls") => {
                    let observability = document
                        .get_mut("observability")
                        .unwrap()
                        .as_table_mut()
                        .unwrap();
                    observability.insert(
                        "status_listen_addr".into(),
                        toml::Value::String("0.0.0.0:8787".into()),
                    );
                }
                _ => {
                    *target = match target {
                        toml::Value::String(_) => toml::Value::String(String::new()),
                        toml::Value::Integer(_) => toml::Value::Integer(0),
                        toml::Value::Boolean(_) => toml::Value::Boolean(false),
                        toml::Value::Array(_) => toml::Value::Array(Vec::new()),
                        toml::Value::Table(_) => toml::Value::Table(Default::default()),
                        value => panic!("unsupported constraint value type: {value:?}"),
                    }
                }
            },
            "unsupported_schema" => *target = toml::Value::Integer(2),
            value => panic!("unknown inventory mutation {value}"),
        }
        toml::to_string(&document).unwrap()
    }

    fn inventory_type(value: &toml::Value) -> &'static str {
        match value {
            toml::Value::String(_) => "string",
            toml::Value::Integer(_) => "integer",
            toml::Value::Boolean(_) => "boolean",
            toml::Value::Array(_) => "array",
            toml::Value::Table(_) => "table",
            value => panic!("unsupported inventory type: {value:?}"),
        }
    }

    fn fixture_value<'a>(document: &'a toml::Value, group: &str, field: &str) -> &'a toml::Value {
        if group == "boundary" {
            return document.get(field).unwrap();
        }
        let group_value = document.get(group).unwrap();
        let table = match group_value {
            toml::Value::Array(values) => values[0].as_table().unwrap(),
            toml::Value::Table(table) => table,
            _ => panic!("{group} is not a configuration group"),
        };
        table.get(field).unwrap()
    }

    fn inventory_fingerprint_probe(group: &str, field: &str) -> Fingerprints {
        let mut document: toml::Value = toml::from_str(&fixture()).unwrap();
        let target = if group == "boundary" {
            document.get_mut(field).unwrap()
        } else {
            let group_value = document.get_mut(group).unwrap();
            let table = match group_value {
                toml::Value::Array(values) => values[0].as_table_mut().unwrap(),
                toml::Value::Table(table) => table,
                _ => panic!("{group} is not a configuration group"),
            };
            table.get_mut(field).unwrap()
        };
        match target {
            toml::Value::String(value) => value.push_str("-fingerprint-probe"),
            toml::Value::Integer(value) => *value = value.checked_add(1).unwrap(),
            toml::Value::Boolean(value) => *value = !*value,
            toml::Value::Array(values) => {
                values.push(toml::Value::String("fingerprint-probe".into()))
            }
            toml::Value::Table(values) => {
                values.insert(
                    "fingerprint_probe".into(),
                    toml::Value::String("text:nullable".into()),
                );
            }
            value => panic!("unsupported fingerprint probe type: {value:?}"),
        }
        if (group, field) == ("budgets", "root") {
            let storage = document.get_mut("storage").unwrap().as_table_mut().unwrap();
            storage.insert(
                "sqlite_path".into(),
                toml::Value::String(".-fingerprint-probe/state/boring.db".into()),
            );
            storage.insert(
                "sqlite_temp_path".into(),
                toml::Value::String(".-fingerprint-probe/state/tmp".into()),
            );
            storage.insert(
                "spool_path".into(),
                toml::Value::String(".-fingerprint-probe/state/spool".into()),
            );
            document
                .get_mut("archive")
                .unwrap()
                .as_table_mut()
                .unwrap()
                .insert(
                    "root".into(),
                    toml::Value::String(".-fingerprint-probe/archive/root".into()),
                );
        }
        let mut raw: RawConfig = toml::from_str(&toml::to_string(&document).unwrap()).unwrap();
        raw.source.start_replication_options.sort();
        raw.tables.sort_by(|a, b| a.logical_id.cmp(&b.logical_id));
        raw.budgets.sort_by(|a, b| a.name.cmp(&b.name));
        fingerprints(&into_public(raw))
            .unwrap_or_else(|error| panic!("fingerprint probe failed for {group}.{field}: {error}"))
    }

    fn changed_fingerprint_domains(
        before: &Fingerprints,
        after: &Fingerprints,
    ) -> Vec<&'static str> {
        [
            ("source", before.source != after.source),
            ("table_set", before.table_set != after.table_set),
            ("destination", before.destination != after.destination),
            ("archive", before.archive != after.archive),
            ("promotion", before.promotion != after.promotion),
            ("backfill", before.backfill != after.backfill),
            ("runtime", before.runtime != after.runtime),
        ]
        .into_iter()
        .filter_map(|(domain, changed)| changed.then_some(domain))
        .collect()
    }

    #[test]
    fn executable_case_inventory_matches_loader() {
        let inventory: serde_json::Value =
            serde_json::from_str(include_str!("../contracts/m1/config-cases.json")).unwrap();
        assert_eq!(inventory["schema_version"], "m1-config-cases/v2");
        assert_eq!(inventory["owner_bead"], "boring-cdc-m1-config");
        let cases = inventory["cases"].as_array().unwrap();
        assert_eq!(cases.len(), 128);
        assert!(inventory["status_log_projection"]["status_code"].is_null());
        assert!(inventory["status_log_projection"]["log_code"].is_null());
        assert!(
            inventory["status_log_projection"]["reason"]
                .as_str()
                .unwrap()
                .contains("later CLI/runtime owners")
        );
        let document: toml::Value = toml::from_str(&fixture()).unwrap();
        let mut ids = BTreeSet::new();
        let mut owners = BTreeSet::new();

        for case in cases {
            let id = case["id"].as_str().unwrap();
            let group = case["group"].as_str().unwrap();
            let field = case["field"].as_str().unwrap();
            assert!(ids.insert(id), "duplicate case ID {id}");
            assert!(
                owners.insert(format!("{group}.{field}")),
                "duplicate field owner"
            );
            assert_eq!(
                case["expected_type"],
                inventory_type(fixture_value(&document, group, field)),
                "stale type metadata for {group}.{field}"
            );
            assert_eq!(case["vectors"]["accepted"]["mutation"], "fixture");
            assert_eq!(case["vectors"]["accepted"]["expected"], "accepted");
            let accepted = load_str(&fixture(), &env()).unwrap();
            let observed_changed = changed_fingerprint_domains(
                accepted.fingerprints(),
                &inventory_fingerprint_probe(group, field),
            );
            assert_eq!(
                case["fingerprint_probe"]["expected_changed"],
                serde_json::json!(observed_changed),
                "stale fingerprint metadata for {group}.{field}"
            );
            assert_eq!(
                case["fingerprint_probe"]["mutation"],
                "same_type_change_in_representative_fixture"
            );
            if group == "budgets" {
                assert_eq!(
                    case["fingerprint_impact"]["always"],
                    serde_json::json!(["runtime"])
                );
                assert_eq!(
                    case["fingerprint_impact"]["conditional"],
                    serde_json::json!({
                        "archive": "when the budget is selected for archive.root",
                        "backfill": "when the budget is selected for the SQLite state root"
                    })
                );
            } else {
                assert_eq!(
                    case["fingerprint_impact"]["always"],
                    case["fingerprint_probe"]["expected_changed"]
                );
                assert_eq!(
                    case["fingerprint_impact"]["conditional"],
                    serde_json::json!({})
                );
            }

            let mutation = case["vectors"]["rejected"]["mutation"].as_str().unwrap();
            let expected = case["vectors"]["rejected"]["expected_error_code"]
                .as_str()
                .unwrap();
            let error =
                load_str(&inventory_value_mutation(group, field, mutation), &env()).expect_err(id);
            assert_eq!(error.code, expected, "stale error metadata for {id}");
            assert_eq!(case["error_codes"], serde_json::json!([error.code]));
            assert_eq!(
                case["constraint"],
                serde_json::json!({
                    "negative_mutation": mutation,
                    "expected_error_code": error.code
                })
            );
            assert_eq!(
                case["accepted_examples"],
                serde_json::json!([format!("fixture::{group}.{field}::accepted")])
            );
            assert_eq!(
                case["rejected_examples"],
                serde_json::json!([format!("{mutation}::{group}.{field}::{}", error.code)])
            );
            assert_eq!(
                case["redaction"],
                "shared_config_error_contains_only_stable_code_and_field"
            );
            let rendered_error = error.to_string();
            assert_eq!(rendered_error, format!("{} at {}", error.code, error.field));
            for secret in [
                "PG_RUNTIME",
                "PG_CONTROL",
                "PG_ADMIN",
                "CH_RUNTIME",
                "CH_MAINT",
            ] {
                assert!(!rendered_error.contains(secret), "{id} leaked {secret}");
            }
            assert!(case["status_code"].is_null());
            assert!(case["log_code"].is_null());
            assert_eq!(
                case["unit_target"],
                "m1_config::tests::executable_case_inventory_matches_loader"
            );
            assert_eq!(
                case["executable_scenario"],
                "cargo test --locked m1_config::tests::executable_case_inventory_matches_loader"
            );
        }

        let groups: BTreeSet<_> = cases
            .iter()
            .map(|case| case["group"].as_str().unwrap())
            .collect();
        assert_eq!(
            groups,
            BTreeSet::from([
                "source",
                "tables",
                "storage",
                "limits",
                "budgets",
                "retention",
                "wal",
                "backfill",
                "clickhouse",
                "archive",
                "conditions",
                "observability",
                "operator",
                "boundary",
            ])
        );
    }

    #[test]
    fn executable_boundary_scenarios_match_loader() {
        let inventory: serde_json::Value =
            serde_json::from_str(include_str!("../contracts/m1/config-cases.json")).unwrap();
        let scenarios = inventory["boundary_scenarios"].as_array().unwrap();
        assert_eq!(scenarios.len(), 5);
        let by_id: BTreeMap<_, _> = scenarios
            .iter()
            .map(|scenario| (scenario["id"].as_str().unwrap(), scenario))
            .collect();
        assert_eq!(by_id.len(), scenarios.len());
        let unit_target = "m1_config::tests::executable_boundary_scenarios_match_loader";

        let precedence = by_id["SCN-M1-CONFIG-BOUNDARY-PRECEDENCE"];
        assert_eq!(
            precedence,
            &serde_json::json!({
                "id": "SCN-M1-CONFIG-BOUNDARY-PRECEDENCE",
                "clause": "precedence",
                "vectors": {
                    "accepted": {
                        "mutation": "approved_status_listener_override",
                        "expected_status_listen_addr": "127.0.0.1:9999"
                    },
                    "rejected": {
                        "mutation": "unapproved_boring_cdc_override",
                        "expected_error_code": "CONFIG_UNAPPROVED_ENV_OVERRIDE"
                    }
                },
                "unit_target": unit_target
            })
        );
        let mut approved = env();
        approved.0.insert(
            "BORING_CDC_STATUS_LISTEN_ADDR".into(),
            "127.0.0.1:9999".into(),
        );
        assert_eq!(
            load_str(&fixture(), &approved)
                .unwrap()
                .public()
                .observability
                .status_listen_addr,
            precedence["vectors"]["accepted"]["expected_status_listen_addr"]
        );
        let mut unapproved = env();
        unapproved
            .0
            .insert("BORING_CDC_NOT_APPROVED".into(), "x".into());
        assert_eq!(
            load_str(&fixture(), &unapproved).unwrap_err().code,
            precedence["vectors"]["rejected"]["expected_error_code"]
        );

        let overrides = by_id["SCN-M1-CONFIG-BOUNDARY-APPROVED-ENVIRONMENT-OVERRIDES"];
        assert_eq!(
            overrides,
            &serde_json::json!({
                "id": "SCN-M1-CONFIG-BOUNDARY-APPROVED-ENVIRONMENT-OVERRIDES",
                "clause": "approved_environment_overrides",
                "vectors": {
                    "accepted": {
                        "mutation": "nonempty_approved_log_level_override",
                        "expected_log_level": "debug"
                    },
                    "rejected": {
                        "mutation": "empty_approved_log_level_override",
                        "expected_error_code": "CONFIG_EMPTY_OVERRIDE"
                    }
                },
                "unit_target": unit_target
            })
        );
        let mut approved_log = env();
        approved_log
            .0
            .insert("BORING_CDC_LOG_LEVEL".into(), "debug".into());
        assert_eq!(
            load_str(&fixture(), &approved_log)
                .unwrap()
                .public()
                .observability
                .log_level,
            overrides["vectors"]["accepted"]["expected_log_level"]
        );
        approved_log
            .0
            .insert("BORING_CDC_LOG_LEVEL".into(), String::new());
        assert_eq!(
            load_str(&fixture(), &approved_log).unwrap_err().code,
            overrides["vectors"]["rejected"]["expected_error_code"]
        );

        let purpose = by_id["SCN-M1-CONFIG-BOUNDARY-PURPOSE-SCOPED-SECRETS"];
        assert_eq!(
            purpose,
            &serde_json::json!({
                "id": "SCN-M1-CONFIG-BOUNDARY-PURPOSE-SCOPED-SECRETS",
                "clause": "purpose_scoped_secrets",
                "vectors": {
                    "accepted": {
                        "mutation": "status_load_without_secret_values",
                        "expected_secret_status": "[REDACTED:not-loaded]"
                    },
                    "rejected": {
                        "mutation": "malformed_secret_reference",
                        "expected_error_code": "CONFIG_INVALID_SECRET_REFERENCE"
                    }
                },
                "unit_target": unit_target
            })
        );
        let status = load_str_for(&fixture(), &Env::default(), LoadPurpose::Status).unwrap();
        for secret_status in status.redacted_diagnostics()["secrets"]
            .as_object()
            .unwrap()
            .values()
        {
            assert_eq!(
                secret_status,
                &purpose["vectors"]["accepted"]["expected_secret_status"]
            );
        }
        let malformed = fixture().replace(
            "administration_dsn_env = \"PG_ADMIN\"",
            "administration_dsn_env = \"bad-name\"",
        );
        assert_eq!(
            load_str_for(&malformed, &Env::default(), LoadPurpose::Status)
                .unwrap_err()
                .code,
            purpose["vectors"]["rejected"]["expected_error_code"]
        );

        let canonical = by_id["SCN-M1-CONFIG-BOUNDARY-CANONICAL-FINGERPRINTS"];
        assert_eq!(
            canonical,
            &serde_json::json!({
                "id": "SCN-M1-CONFIG-BOUNDARY-CANONICAL-FINGERPRINTS",
                "clause": "canonical_fingerprints",
                "vectors": {
                    "accepted": {
                        "mutation": "identical_canonical_inputs",
                        "expected_equal": true
                    },
                    "rejected": {
                        "mutation": "semantic_log_level_change",
                        "expected_changed": ["runtime"]
                    }
                },
                "unit_target": unit_target
            })
        );
        let first = load_str(&fixture(), &env()).unwrap();
        let second = load_str(&fixture(), &env()).unwrap();
        assert_eq!(
            first.fingerprints() == second.fingerprints(),
            canonical["vectors"]["accepted"]["expected_equal"]
        );
        let changed = load_str(
            &fixture().replace("log_level = \"info\"", "log_level = \"debug\""),
            &env(),
        )
        .unwrap();
        assert_eq!(
            serde_json::json!(changed_fingerprint_domains(
                first.fingerprints(),
                changed.fingerprints()
            )),
            canonical["vectors"]["rejected"]["expected_changed"]
        );

        let redaction = by_id["SCN-M1-CONFIG-BOUNDARY-REDACTED-DIAGNOSTICS"];
        assert_eq!(
            redaction,
            &serde_json::json!({
                "id": "SCN-M1-CONFIG-BOUNDARY-REDACTED-DIAGNOSTICS",
                "clause": "redacted_diagnostics",
                "vectors": {
                    "accepted": {
                        "mutation": "render_debug_and_redacted_diagnostics",
                        "expected_secret_statuses": {
                            "runtime_capture": "[REDACTED:available]",
                            "control_writer": "[REDACTED:available]",
                            "administration": "[REDACTED:not-loaded]",
                            "clickhouse_runtime": "[REDACTED:available]",
                            "clickhouse_maintenance": "[REDACTED:not-loaded]"
                        }
                    },
                    "rejected": {
                        "mutation": "scan_diagnostic_surfaces_for_secret_tokens",
                        "expected_absent": [
                            "PG_RUNTIME", "PG_CONTROL", "PG_ADMIN", "CH_RUNTIME", "CH_MAINT",
                            "postgres://runtime:secret@source/db",
                            "https://runtime:secret@clickhouse"
                        ]
                    }
                },
                "unit_target": unit_target
            })
        );
        let diagnostics = first.redacted_diagnostics();
        assert_eq!(
            diagnostics["secrets"],
            redaction["vectors"]["accepted"]["expected_secret_statuses"]
        );
        let surfaces = [format!("{first:?}"), diagnostics.to_string()];
        for token in redaction["vectors"]["rejected"]["expected_absent"]
            .as_array()
            .unwrap()
        {
            let token = token.as_str().unwrap();
            for surface in &surfaces {
                assert!(!surface.contains(token), "diagnostic leaked {token}");
            }
        }
    }
}
