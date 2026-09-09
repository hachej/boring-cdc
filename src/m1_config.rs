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
            // M0-PROVISIONAL: boring-cdc-d-security (RECOMMENDED loopback default).
            status_listen_addr: "127.0.0.1:8787".into(),
            // M0-PROVISIONAL: boring-cdc-d-security (RECOMMENDED loopback default).
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
    pub timeout_ms: Milliseconds,
    pub result_retention_ms: Milliseconds,
    pub confirmation_expiry_ms: Milliseconds,
}

impl Default for OperatorEndpoint {
    fn default() -> Self {
        Self {
            socket_path: "run/boring-cdc/operator.sock".into(),
            // M0-PROVISIONAL: boring-cdc-d-security (RECOMMENDED 0700/0600 endpoint).
            directory_mode: 0o700,
            socket_mode: 0o600,
            peer_credentials: true,
            max_request_bytes: Bytes(65_536),
            max_response_bytes: Bytes(65_536),
            timeout_ms: Milliseconds(5_000),
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
    Maintenance,
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
            .field("public", &self.public)
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
    apply_overrides(&mut raw, env)?;
    validate(&mut raw)?;
    let secrets = Secrets {
        runtime: matches!(
            purpose,
            LoadPurpose::Check | LoadPurpose::Run | LoadPurpose::Maintenance
        )
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
        administration: matches!(purpose, LoadPurpose::Maintenance)
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
        clickhouse_maintenance: matches!(purpose, LoadPurpose::Maintenance)
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
    raw.tables.sort_by(|a, b| a.logical_id.cmp(&b.logical_id));
    unique(
        raw.tables.iter().map(|t| t.logical_id.as_str()),
        "tables.logical_id",
    )?;
    for table in &raw.tables {
        if table.logical_id.is_empty()
            || table.replica_key.is_empty()
            || table.replica_key.iter().any(|k| k.is_empty())
        {
            return err("CONFIG_INVALID_TABLE_IDENTITY", "tables");
        }
    }
    if raw.source.publication.is_empty()
        || raw.source.slot.is_empty()
        || raw.source.copy_both_transport.is_empty()
        || raw.source.advisory_lock_derivation.is_empty()
    {
        return err("CONFIG_EMPTY_SOURCE_IDENTITY", "source");
    }
    raw.budgets.sort_by(|a, b| a.name.cmp(&b.name));
    unique(raw.budgets.iter().map(|b| b.name.as_str()), "budgets.name")?;
    for budget in &raw.budgets {
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
    if !(raw.conditions.warning < raw.conditions.action
        && raw.conditions.action < raw.conditions.critical
        && raw.conditions.critical < raw.conditions.hard)
    {
        return err("CONFIG_INVALID_THRESHOLDS", "conditions");
    }
    if raw.backfill.concurrency == 0
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
    if raw.source.origin_policy != "any"
        || !raw
            .source
            .start_replication_options
            .iter()
            .any(|v| v == "streaming=false")
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
    if !raw.clickhouse.endpoint.starts_with("https://")
        || raw.clickhouse.endpoint.contains('@')
        || raw.clickhouse.endpoint.contains('?')
    {
        return err("CONFIG_UNSAFE_CLICKHOUSE_ENDPOINT", "clickhouse.endpoint");
    }
    if !raw.clickhouse.synchronous_insert || !raw.clickhouse.fsync_after_insert {
        return err("CONFIG_UNSAFE_CLICKHOUSE_DURABILITY", "clickhouse");
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
    Ok(Fingerprints {
        source: digest(&(&config.source, &config.tables))?,
        table_set: digest(&config.tables)?,
        destination: digest(&(&config.clickhouse, &config.archive))?,
        backfill: digest(&(&config.backfill, &config.retention, &config.budgets))?,
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

        let maintenance = load_str_for(&fixture(), &env(), LoadPurpose::Maintenance).unwrap();
        assert!(maintenance.administration_dsn().is_some());
        assert!(maintenance.control_writer_dsn().is_none());
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
            let candidate = if from.starts_with("origin_policy") || from.starts_with("socket_mode")
            {
                let mut text = fixture();
                if from.starts_with("origin_policy") {
                    text = text.replace(
                        "advisory_lock_derivation =",
                        "origin_policy = \"none\"\nadvisory_lock_derivation =",
                    );
                } else {
                    text.push_str("\n[operator]\nsocket_mode = 420\n");
                }
                text
            } else {
                fixture().replace(from, to)
            };
            assert_eq!(
                load_str(&candidate, &env()).unwrap_err().code,
                code,
                "{from}"
            );
        }
    }

    #[test]
    fn exhaustive_case_inventory_covers_every_configuration_group() {
        let inventory: serde_json::Value =
            serde_json::from_str(include_str!("../contracts/m1/config-cases.json")).unwrap();
        assert_eq!(inventory["schema_version"], "m1-config-cases/v1");
        assert_eq!(inventory["owner_bead"], "boring-cdc-m1-config");
        let cases = inventory["cases"].as_array().unwrap();
        let groups: BTreeSet<_> = cases
            .iter()
            .map(|case| case["group"].as_str().unwrap())
            .collect();
        let expected = BTreeSet::from([
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
        ]);
        assert_eq!(groups, expected);
        let ids: BTreeSet<_> = cases
            .iter()
            .map(|case| case["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids.len(), cases.len());
        for case in cases {
            assert!(case["id"].as_str().unwrap().starts_with("SCN-M1-CONFIG-"));
            for field in [
                "accepted_examples",
                "rejected_examples",
                "fingerprint_impact",
                "error_codes",
            ] {
                assert!(!case[field].as_array().unwrap().is_empty(), "{field}");
            }
            assert_eq!(case["unit_target"], "m1_config::tests");
        }
    }
}
