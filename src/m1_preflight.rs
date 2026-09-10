//! Read-only preflight classification for `boring-cdc check`.
//!
//! Adapters collect observations; this module only compares them with the loaded
//! configuration. It never opens PostgreSQL, SQLite, ClickHouse, sockets, or files.

use crate::m1_cli_contract::{CLI_SCHEMA_VERSION, CliEnvelope, NextCommand};
use crate::m1_config::PublicConfig;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub const PREFLIGHT_SCHEMA_VERSION: &str = "m1-preflight/v1";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckStatus {
    Healthy,
    Degraded,
    Unverified,
    Blocked,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WriterAttestation {
    pub run_id: String,
    pub connection_generation: u64,
    pub fresh: bool,
    pub synchronous: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceObservation {
    pub server_major: u16,
    pub wal_level: String,
    pub protocol: String,
    pub copy_both_available: bool,
    pub publication_name_matches: bool,
    pub publication_fingerprint_matches: bool,
    pub publication_owned_by_admin: bool,
    pub runtime_can_alter_publication: bool,
    pub role_has_replication: bool,
    pub slot_name_matches: bool,
    pub slot_plugin: String,
    pub slot_intent_no_drop_bound: bool,
    pub slot_positions_valid: bool,
    pub slot_active_state_safe: bool,
    pub slot_wal_status_safe: bool,
    pub max_slot_wal_keep_size_matches: bool,
    pub publication_row_filters_absent: bool,
    pub publication_column_lists_absent: bool,
    pub publication_truncate_policy_matches: bool,
    pub origin_policy_matches: bool,
    pub source_tls_verified: bool,
    pub advisory_lock_derivation_matches: bool,
    pub table_contracts_match: bool,
    pub ddl_policy_matches: bool,
    pub keys_supported: bool,
    pub types_supported: bool,
    pub partitions_supported: bool,
    pub replica_identity_complete: bool,
    pub control_rows_each: u64,
    pub control_select_key_only: bool,
    pub control_update_value_only: bool,
    pub control_insert: bool,
    pub control_delete: bool,
    pub control_update_key: bool,
    pub grants_sufficient: bool,
    pub logical_decoding_work_mem_bytes: u64,
    pub spill_counters_available: bool,
    pub streaming_option: String,
    pub copy_both_receive_bytes: u64,
    pub decoder_bytes: u64,
    pub staging_bytes: u64,
    pub worker_bytes: u64,
    pub destination_worker_bytes: u64,
    pub fixed_runtime_bytes: u64,
    pub telemetry_bytes: u64,
    pub command_headroom_bytes: u64,
    pub sqlite_writer_staging_bytes: u64,
    pub process_limit_bytes: u64,
    pub cgroup_limit_bytes: Option<u64>,
    pub idle_in_transaction_session_timeout_ms: u64,
    pub statement_timeout_ms: u64,
    pub lock_timeout_ms: u64,
    pub tcp_keepalive_ms: u64,
    pub client_connection_check_interval_ms: u64,
    pub zombie_detection_bound_ms: u64,
    pub source_free_disk_bytes: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FilesystemCapacity {
    pub configured_total_bytes: u64,
    pub available_bytes: u64,
    pub metadata_and_rounding_bytes: u64,
    pub archive_publication_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StorageObservation {
    pub before_state_sha256: String,
    pub after_state_sha256: String,
    pub journal_mode: String,
    pub synchronous: String,
    pub auto_vacuum: String,
    pub filesystem: String,
    pub disposable_probe_succeeded: bool,
    pub filesystem_capacities: BTreeMap<String, FilesystemCapacity>,
    pub reader_budget_units: u64,
    pub scheduler_budget_units: u64,
    pub audit_budget_units: u64,
    pub writer_attestation: Option<WriterAttestation>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationObservation {
    pub clickhouse_mapping_matches: bool,
    pub clickhouse_capabilities_match: bool,
    pub archive_mapping_matches: bool,
    pub archive_capabilities_match: bool,
    pub tls_verified: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SecurityObservation {
    pub socket_parent_secure: bool,
    pub socket_mode: u32,
    pub peer_policy_enforced: bool,
    pub status_read_only: bool,
    pub prometheus_read_only: bool,
    pub listeners_match: bool,
    pub redaction_probe_passed: bool,
    pub lock_probe_configured: bool,
    pub supervised_restart_configured: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreflightObservation {
    pub schema_version: String,
    pub collector: String,
    pub config_fingerprint: String,
    pub current_run_id: Option<String>,
    pub current_connection_generation: Option<u64>,
    pub source: SourceObservation,
    pub storage: StorageObservation,
    pub destination: DestinationObservation,
    pub security: SecurityObservation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CheckResult {
    pub scenario_id: &'static str,
    pub status: CheckStatus,
    pub reason: &'static str,
    pub units: Option<&'static str>,
}

#[derive(Clone, Debug, Serialize)]
pub struct PreflightReport {
    pub schema_version: &'static str,
    pub outcome: CheckStatus,
    pub checks: Vec<CheckResult>,
}

fn push(checks: &mut Vec<CheckResult>, scenario_id: &'static str, ok: bool, reason: &'static str) {
    checks.push(CheckResult {
        scenario_id,
        status: if ok {
            CheckStatus::Healthy
        } else {
            CheckStatus::Blocked
        },
        reason: if ok { "PREFLIGHT_OK" } else { reason },
        units: None,
    });
}

fn checked_sum(values: &[u64]) -> Option<u64> {
    values
        .iter()
        .try_fold(0_u64, |sum, value| sum.checked_add(*value))
}

#[derive(Clone, Copy)]
struct LiveCollectorCapability(());

#[cfg(test)]
fn test_live_collector_capability() -> LiveCollectorCapability {
    LiveCollectorCapability(())
}

/// Classify untrusted/synthetic input. It can exercise every failure branch but can
/// never attest writer settings, real-store immutability, or live health.
pub fn evaluate_untrusted(
    config: &PublicConfig,
    expected_config_fingerprint: &str,
    observed: &PreflightObservation,
) -> PreflightReport {
    evaluate_with_capability(config, expected_config_fingerprint, observed, None)
}

fn evaluate_with_capability(
    config: &PublicConfig,
    expected_config_fingerprint: &str,
    observed: &PreflightObservation,
    live_capability: Option<LiveCollectorCapability>,
) -> PreflightReport {
    let trusted_live = live_capability.is_some();
    let mut c = Vec::new();
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-SCHEMA",
        observed.schema_version == PREFLIGHT_SCHEMA_VERSION,
        "PREFLIGHT_OBSERVATION_SCHEMA_UNSUPPORTED",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-PROVENANCE",
        observed.config_fingerprint == expected_config_fingerprint
            && !observed.collector.is_empty(),
        "PREFLIGHT_OBSERVATION_PROVENANCE_MISMATCH",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-PG-VERSION",
        matches!(observed.source.server_major, 15..=17),
        "PREFLIGHT_POSTGRES_VERSION_UNSUPPORTED",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-WAL",
        observed.source.wal_level == "logical",
        "PREFLIGHT_WAL_LEVEL_NOT_LOGICAL",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-COPYBOTH",
        observed.source.protocol == config.source.copy_both_transport
            && observed.source.copy_both_available,
        "PREFLIGHT_COPYBOTH_UNAVAILABLE",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-PUBLICATION",
        observed.source.publication_name_matches && observed.source.publication_fingerprint_matches,
        "PREFLIGHT_PUBLICATION_FINGERPRINT_MISMATCH",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-PUBLICATION-OWNERSHIP",
        observed.source.publication_owned_by_admin
            && !observed.source.runtime_can_alter_publication,
        "PREFLIGHT_PUBLICATION_PRIVILEGE_UNSAFE",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-REPLICATION-ROLE",
        observed.source.role_has_replication,
        "PREFLIGHT_REPLICATION_PRIVILEGE_MISSING",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-SLOT",
        observed.source.slot_name_matches
            && observed.source.slot_plugin == "pgoutput"
            && observed.source.slot_positions_valid,
        "PREFLIGHT_SLOT_BINDING_MISMATCH",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-SLOT-INTENT",
        observed.source.slot_intent_no_drop_bound,
        "PREFLIGHT_SLOT_INTENT_NOT_BOUND",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-SLOT-STATE",
        observed.source.slot_active_state_safe
            && observed.source.slot_wal_status_safe
            && observed.source.max_slot_wal_keep_size_matches,
        "PREFLIGHT_SLOT_STATE_UNSAFE",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-PUBLICATION-SHAPE",
        observed.source.publication_row_filters_absent
            && observed.source.publication_column_lists_absent
            && observed.source.publication_truncate_policy_matches,
        "PREFLIGHT_PUBLICATION_SHAPE_UNSUPPORTED",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-SOURCE-POLICY",
        observed.source.origin_policy_matches
            && observed.source.source_tls_verified
            && observed.source.advisory_lock_derivation_matches,
        "PREFLIGHT_SOURCE_POLICY_MISMATCH",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-TABLES",
        observed.source.table_contracts_match,
        "PREFLIGHT_TABLE_CONTRACT_MISMATCH",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-DDL",
        observed.source.ddl_policy_matches,
        "PREFLIGHT_DDL_POLICY_UNSUPPORTED",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-KEYS",
        observed.source.keys_supported,
        "PREFLIGHT_KEY_UNSUPPORTED",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-TYPES",
        observed.source.types_supported,
        "PREFLIGHT_TYPE_UNSUPPORTED",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-PARTITIONS",
        observed.source.partitions_supported,
        "PREFLIGHT_PARTITION_UNSUPPORTED",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-REPLICA-IDENTITY",
        observed.source.replica_identity_complete,
        "PREFLIGHT_REPLICA_IDENTITY_INCOMPLETE",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-CONTROL-CARDINALITY",
        observed.source.control_rows_each == 1,
        "PREFLIGHT_CONTROL_ROW_CARDINALITY",
    );
    let least_privilege = observed.source.control_select_key_only
        && observed.source.control_update_value_only
        && !observed.source.control_insert
        && !observed.source.control_delete
        && !observed.source.control_update_key;
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-CONTROL-PRIVILEGES",
        least_privilege,
        "PREFLIGHT_CONTROL_PRIVILEGE_EXCESS",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-GRANTS",
        observed.source.grants_sufficient,
        "PREFLIGHT_REQUIRED_GRANT_MISSING",
    );

    let expected_options = [
        "binary=false",
        "proto_version=1",
        "streaming=false",
        "two_phase=false",
    ];
    let mut configured = config
        .source
        .start_replication_options
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    configured.sort_unstable();
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-PROTOCOL-FINGERPRINT",
        configured == expected_options && observed.source.streaming_option == "streaming=false",
        "PREFLIGHT_PROTOCOL_FINGERPRINT_MISMATCH",
    );

    let aggregate = checked_sum(&[
        observed.source.copy_both_receive_bytes,
        observed.source.decoder_bytes,
        observed.source.staging_bytes,
        observed
            .source
            .worker_bytes
            .saturating_mul(config.backfill.concurrency),
        observed.source.destination_worker_bytes,
        observed.source.fixed_runtime_bytes,
        observed.source.telemetry_bytes,
        observed.source.command_headroom_bytes,
        observed.source.sqlite_writer_staging_bytes,
    ]);
    let effective_limit = observed
        .source
        .cgroup_limit_bytes
        .map_or(observed.source.process_limit_bytes, |v| {
            v.min(observed.source.process_limit_bytes)
        });
    let memory_components = [
        observed.source.copy_both_receive_bytes,
        observed.source.decoder_bytes,
        observed.source.staging_bytes,
        observed.source.worker_bytes,
        observed.source.destination_worker_bytes,
        observed.source.fixed_runtime_bytes,
        observed.source.telemetry_bytes,
        observed.source.command_headroom_bytes,
        observed.source.sqlite_writer_staging_bytes,
    ];
    let memory_ok = memory_components.iter().all(|value| *value > 0)
        && aggregate
            .is_some_and(|v| v <= effective_limit && v <= config.limits.process_memory_bytes.0)
        && observed.source.logical_decoding_work_mem_bytes >= config.limits.max_transaction_bytes.0;
    c.push(CheckResult {
        scenario_id: "SCN-M1-PREFLIGHT-MEMORY",
        status: if memory_ok {
            CheckStatus::Healthy
        } else {
            CheckStatus::Blocked
        },
        reason: if memory_ok {
            "PREFLIGHT_OK"
        } else {
            "PREFLIGHT_MEMORY_BUDGET_INSUFFICIENT"
        },
        units: Some("bytes"),
    });
    c.push(CheckResult {
        scenario_id: "SCN-M1-PREFLIGHT-SPILL-COUNTERS",
        status: if observed.source.spill_counters_available {
            CheckStatus::Healthy
        } else {
            CheckStatus::Unverified
        },
        reason: if observed.source.spill_counters_available {
            "PREFLIGHT_OK"
        } else {
            "PREFLIGHT_SPILL_COUNTERS_UNAVAILABLE"
        },
        units: Some("bytes"),
    });

    let timeout_ok = observed.source.idle_in_transaction_session_timeout_ms
        > config.backfill.guard_keepalive_ms.0
        && observed.source.statement_timeout_ms >= config.source.maximum_operation_ms.0
        && observed.source.lock_timeout_ms <= config.backfill.ddl_waiter_bound_ms.0
        && observed.source.tcp_keepalive_ms <= config.source.heartbeat_cadence_ms.0
        && observed.source.client_connection_check_interval_ms
            <= config.source.heartbeat_cadence_ms.0
        && observed.source.zombie_detection_bound_ms <= config.source.stale_owner_takeover_ms.0;
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-TIMEOUTS",
        timeout_ok,
        "PREFLIGHT_TIMEOUT_KEEPALIVE_INCOMPATIBLE",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-OWNERSHIP",
        observed.security.lock_probe_configured && observed.security.supervised_restart_configured,
        "PREFLIGHT_OWNERSHIP_RECOVERY_UNSAFE",
    );

    let hashes_equal = valid_sha256(&observed.storage.before_state_sha256)
        && observed.storage.before_state_sha256 == observed.storage.after_state_sha256;
    c.push(CheckResult {
        scenario_id: "SCN-M1-PREFLIGHT-NO-MUTATION",
        status: if !hashes_equal {
            CheckStatus::Blocked
        } else if trusted_live {
            CheckStatus::Healthy
        } else {
            CheckStatus::Unverified
        },
        reason: if !hashes_equal {
            "PREFLIGHT_STATE_MUTATED"
        } else if trusted_live {
            "PREFLIGHT_OK"
        } else {
            "PREFLIGHT_STATE_HASH_UNVERIFIED"
        },
        units: None,
    });
    let sqlite_ok = observed.storage.journal_mode == config.storage.journal_mode
        && observed.storage.auto_vacuum == config.storage.auto_vacuum;
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-SQLITE-PRAGMAS",
        sqlite_ok,
        "PREFLIGHT_SQLITE_PRAGMA_MISMATCH",
    );
    match &observed.storage.writer_attestation {
        Some(a)
            if trusted_live
                && a.fresh
                && observed.current_run_id.as_deref() == Some(a.run_id.as_str())
                && observed.current_connection_generation == Some(a.connection_generation)
                && a.connection_generation > 0
                && a.synchronous == config.storage.synchronous =>
        {
            push(&mut c, "SCN-M1-PREFLIGHT-WRITER-ATTESTATION", true, "")
        }
        _ => c.push(CheckResult {
            scenario_id: "SCN-M1-PREFLIGHT-WRITER-ATTESTATION",
            status: CheckStatus::Unverified,
            reason: "PREFLIGHT_WRITER_SYNCHRONOUS_UNVERIFIED",
            units: None,
        }),
    }
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-FILESYSTEM",
        observed.storage.filesystem == config.storage.filesystem
            && observed.storage.disposable_probe_succeeded,
        "PREFLIGHT_FILESYSTEM_UNSUPPORTED",
    );
    let budgets_ok = config.budgets.iter().all(|budget| {
        observed
            .storage
            .filesystem_capacities
            .get(&budget.name)
            .is_some_and(|capacity| {
                let admitted = checked_sum(&[
                    budget.reserved_free_bytes.0,
                    budget.sqlite_wal_temp_bytes.0,
                    budget.capture_spool_bytes.0,
                    budget.concurrent_backfill_bytes.0,
                    budget.archive_segment_bytes.0,
                ]);
                let required = admitted
                    .and_then(|v| v.checked_add(capacity.metadata_and_rounding_bytes))
                    .and_then(|v| v.checked_add(capacity.archive_publication_bytes));
                capacity.configured_total_bytes == budget.total_bytes.0
                    && capacity.metadata_and_rounding_bytes > 0
                    && capacity.archive_publication_bytes > 0
                    && required.is_some_and(|required| capacity.available_bytes >= required)
            })
    }) && observed.storage.reader_budget_units > 0
        && observed.storage.scheduler_budget_units > 0
        && observed.storage.audit_budget_units > 0;
    c.push(CheckResult {
        scenario_id: "SCN-M1-PREFLIGHT-BUDGETS",
        status: if budgets_ok {
            CheckStatus::Healthy
        } else {
            CheckStatus::Blocked
        },
        reason: if budgets_ok {
            "PREFLIGHT_OK"
        } else {
            "PREFLIGHT_UNIT_BUDGET_INVALID"
        },
        units: Some("bytes_and_units"),
    });

    push(
        &mut c,
        "SCN-M1-PREFLIGHT-CLICKHOUSE-MAPPING",
        observed.destination.clickhouse_mapping_matches
            && observed.destination.clickhouse_capabilities_match,
        "PREFLIGHT_CLICKHOUSE_MAPPING_UNSUPPORTED",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-ARCHIVE-MAPPING",
        observed.destination.archive_mapping_matches
            && observed.destination.archive_capabilities_match,
        "PREFLIGHT_ARCHIVE_MAPPING_UNSUPPORTED",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-DESTINATION-TLS",
        observed.destination.tls_verified,
        "PREFLIGHT_DESTINATION_TLS_UNVERIFIED",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-SOCKET",
        observed.security.socket_parent_secure
            && observed.security.socket_mode == config.operator.socket_mode
            && observed.security.peer_policy_enforced,
        "PREFLIGHT_OPERATOR_SOCKET_UNSAFE",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-READONLY-ROUTES",
        observed.security.status_read_only && observed.security.prometheus_read_only,
        "PREFLIGHT_MUTATING_OBSERVABILITY_ROUTE",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-LISTENERS",
        observed.security.listeners_match,
        "PREFLIGHT_LISTENER_POLICY_MISMATCH",
    );
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-REDACTION",
        observed.security.redaction_probe_passed,
        "PREFLIGHT_REDACTION_POLICY_FAILED",
    );

    let wal_growth = config
        .wal
        .production_bytes_per_second
        .0
        .checked_mul(config.wal.monitor_delay_ms.0)
        .and_then(|v| v.checked_add(999))
        .map(|v| v / 1_000);
    let wal_required = wal_growth
        .and_then(|v| v.checked_add(config.wal.reaction_reserve_bytes.0))
        .and_then(|v| v.checked_add(config.wal.max_slot_wal_keep_bytes.0))
        .map(|v| v.max(config.wal.source_free_bytes.0));
    let source_disk_status = match (observed.source.source_free_disk_bytes, wal_required) {
        (_, None) => CheckStatus::Blocked,
        (None, Some(_)) => CheckStatus::Degraded,
        (Some(free), Some(required)) if free >= required => CheckStatus::Healthy,
        (Some(_), Some(_)) => CheckStatus::Blocked,
    };
    c.push(CheckResult {
        scenario_id: "SCN-M1-PREFLIGHT-SOURCE-FREE-DISK",
        status: source_disk_status,
        reason: match (observed.source.source_free_disk_bytes, wal_required) {
            (_, None) => "PREFLIGHT_WAL_EQUATION_OVERFLOW",
            (None, Some(_)) => "PREFLIGHT_SOURCE_FREE_DISK_UNKNOWN",
            (Some(free), Some(required)) if free >= required => "PREFLIGHT_OK",
            (Some(_), Some(_)) => "PREFLIGHT_SOURCE_FREE_DISK_INSUFFICIENT",
        },
        units: Some("bytes"),
    });

    c.push(CheckResult {
        scenario_id: "SCN-M1-PREFLIGHT-LIVE-COLLECTOR",
        status: if trusted_live {
            CheckStatus::Healthy
        } else {
            CheckStatus::Unverified
        },
        reason: if trusted_live {
            "PREFLIGHT_OK"
        } else {
            "PREFLIGHT_LIVE_COLLECTION_UNVERIFIED"
        },
        units: None,
    });
    let outcome = if c.iter().any(|x| x.status == CheckStatus::Blocked) {
        CheckStatus::Blocked
    } else if c
        .iter()
        .any(|x| matches!(x.status, CheckStatus::Degraded | CheckStatus::Unverified))
    {
        CheckStatus::Degraded
    } else {
        CheckStatus::Healthy
    };
    PreflightReport {
        schema_version: PREFLIGHT_SCHEMA_VERSION,
        outcome,
        checks: c,
    }
}

pub fn envelope(report: &PreflightReport) -> CliEnvelope {
    let (outcome, code, message) = match report.outcome {
        CheckStatus::Healthy => (
            "success",
            "PREFLIGHT_HEALTHY",
            "all preflight checks passed",
        ),
        CheckStatus::Degraded | CheckStatus::Unverified => (
            "degraded",
            "PREFLIGHT_DEGRADED",
            "preflight has unknown or unverified capabilities",
        ),
        CheckStatus::Blocked => (
            "blocked",
            "PREFLIGHT_BLOCKED",
            "preflight rejected one or more required capabilities",
        ),
    };
    CliEnvelope {
        schema_version: CLI_SCHEMA_VERSION,
        command: "CMD-CHECK".into(),
        outcome: outcome.into(),
        code: code.into(),
        message: message.into(),
        request_id: None,
        run_id: None,
        capture_epoch: None,
        condition: Some(format!("{:?}", report.outcome).to_lowercase()),
        runbook_id: Some("RB-OPERATOR-COMMAND".into()),
        data: serde_json::to_value(report).unwrap_or(Value::Null),
        warnings: report
            .checks
            .iter()
            .filter(|x| matches!(x.status, CheckStatus::Degraded | CheckStatus::Unverified))
            .map(|x| x.reason.into())
            .collect(),
        next_commands: Vec::<NextCommand>::new(),
        plan_digest: None,
        postcondition_evidence_digest: None,
        mutation_trace: None,
    }
}

pub fn stable_reason_inventory() -> Value {
    json!(evaluate_reason_inventory())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

fn evaluate_reason_inventory() -> Vec<&'static str> {
    vec![
        "PREFLIGHT_ARCHIVE_MAPPING_UNSUPPORTED",
        "PREFLIGHT_CLICKHOUSE_MAPPING_UNSUPPORTED",
        "PREFLIGHT_CONTROL_PRIVILEGE_EXCESS",
        "PREFLIGHT_CONTROL_ROW_CARDINALITY",
        "PREFLIGHT_COPYBOTH_UNAVAILABLE",
        "PREFLIGHT_DESTINATION_TLS_UNVERIFIED",
        "PREFLIGHT_FILESYSTEM_UNSUPPORTED",
        "PREFLIGHT_KEY_UNSUPPORTED",
        "PREFLIGHT_LISTENER_POLICY_MISMATCH",
        "PREFLIGHT_MEMORY_BUDGET_INSUFFICIENT",
        "PREFLIGHT_LIVE_COLLECTION_UNVERIFIED",
        "PREFLIGHT_MUTATING_OBSERVABILITY_ROUTE",
        "PREFLIGHT_OBSERVATION_PROVENANCE_MISMATCH",
        "PREFLIGHT_OBSERVATION_SCHEMA_UNSUPPORTED",
        "PREFLIGHT_OPERATOR_SOCKET_UNSAFE",
        "PREFLIGHT_OWNERSHIP_RECOVERY_UNSAFE",
        "PREFLIGHT_PARTITION_UNSUPPORTED",
        "PREFLIGHT_POSTGRES_VERSION_UNSUPPORTED",
        "PREFLIGHT_PROTOCOL_FINGERPRINT_MISMATCH",
        "PREFLIGHT_PUBLICATION_SHAPE_UNSUPPORTED",
        "PREFLIGHT_PUBLICATION_FINGERPRINT_MISMATCH",
        "PREFLIGHT_PUBLICATION_PRIVILEGE_UNSAFE",
        "PREFLIGHT_REDACTION_POLICY_FAILED",
        "PREFLIGHT_REPLICA_IDENTITY_INCOMPLETE",
        "PREFLIGHT_REPLICATION_PRIVILEGE_MISSING",
        "PREFLIGHT_REQUIRED_GRANT_MISSING",
        "PREFLIGHT_SLOT_BINDING_MISMATCH",
        "PREFLIGHT_SLOT_STATE_UNSAFE",
        "PREFLIGHT_SLOT_INTENT_NOT_BOUND",
        "PREFLIGHT_SOURCE_POLICY_MISMATCH",
        "PREFLIGHT_DDL_POLICY_UNSUPPORTED",
        "PREFLIGHT_SOURCE_FREE_DISK_INSUFFICIENT",
        "PREFLIGHT_SOURCE_FREE_DISK_UNKNOWN",
        "PREFLIGHT_SPILL_COUNTERS_UNAVAILABLE",
        "PREFLIGHT_SQLITE_PRAGMA_MISMATCH",
        "PREFLIGHT_STATE_MUTATED",
        "PREFLIGHT_STATE_HASH_UNVERIFIED",
        "PREFLIGHT_TABLE_CONTRACT_MISMATCH",
        "PREFLIGHT_TIMEOUT_KEEPALIVE_INCOMPATIBLE",
        "PREFLIGHT_TYPE_UNSUPPORTED",
        "PREFLIGHT_UNIT_BUDGET_INVALID",
        "PREFLIGHT_WAL_LEVEL_NOT_LOGICAL",
        "PREFLIGHT_WAL_EQUATION_OVERFLOW",
        "PREFLIGHT_WRITER_SYNCHRONOUS_UNVERIFIED",
    ]
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::m1_config::{Environment, LoadPurpose, load_str_for};
    use std::collections::BTreeMap;

    struct Env(BTreeMap<String, String>);
    impl Environment for Env {
        fn get(&self, name: &str) -> Option<String> {
            self.0.get(name).cloned()
        }
        fn names(&self) -> Vec<String> {
            self.0.keys().cloned().collect()
        }
    }
    fn config() -> crate::m1_config::LoadedConfig {
        let values = [
            ("PG_RUNTIME", "postgres://redacted"),
            ("PG_CONTROL", "postgres://redacted"),
            ("PG_ADMIN", "postgres://redacted"),
            ("CH_RUNTIME", "https://redacted"),
            ("CH_MAINT", "https://redacted"),
        ]
        .into_iter()
        .map(|(k, v)| (k.into(), v.into()))
        .collect();
        load_str_for(
            include_str!("../tests/fixtures/m1_config/representative.toml"),
            &Env(values),
            LoadPurpose::Check,
        )
        .unwrap()
    }
    pub fn supported() -> PreflightObservation {
        PreflightObservation {
            schema_version: PREFLIGHT_SCHEMA_VERSION.into(),
            collector: "boring-cdc-live-preflight-v1".into(),
            config_fingerprint: config().fingerprints().runtime.clone(),
            current_run_id: Some("run-fixture".into()),
            current_connection_generation: Some(1),
            source: SourceObservation {
                server_major: 16,
                wal_level: "logical".into(),
                protocol: "postgres-replication-copyboth-v1".into(),
                copy_both_available: true,
                publication_name_matches: true,
                publication_fingerprint_matches: true,
                publication_owned_by_admin: true,
                runtime_can_alter_publication: false,
                role_has_replication: true,
                slot_name_matches: true,
                slot_plugin: "pgoutput".into(),
                slot_intent_no_drop_bound: true,
                slot_positions_valid: true,
                slot_active_state_safe: true,
                slot_wal_status_safe: true,
                max_slot_wal_keep_size_matches: true,
                publication_row_filters_absent: true,
                publication_column_lists_absent: true,
                publication_truncate_policy_matches: true,
                origin_policy_matches: true,
                source_tls_verified: true,
                advisory_lock_derivation_matches: true,
                table_contracts_match: true,
                ddl_policy_matches: true,
                keys_supported: true,
                types_supported: true,
                partitions_supported: true,
                replica_identity_complete: true,
                control_rows_each: 1,
                control_select_key_only: true,
                control_update_value_only: true,
                control_insert: false,
                control_delete: false,
                control_update_key: false,
                grants_sufficient: true,
                logical_decoding_work_mem_bytes: 67_108_864,
                spill_counters_available: true,
                streaming_option: "streaming=false".into(),
                copy_both_receive_bytes: 1_048_576,
                decoder_bytes: 67_108_864,
                staging_bytes: 67_108_864,
                worker_bytes: 20_000_000,
                destination_worker_bytes: 20_000_000,
                fixed_runtime_bytes: 20_000_000,
                telemetry_bytes: 5_000_000,
                command_headroom_bytes: 5_000_000,
                sqlite_writer_staging_bytes: 5_000_000,
                process_limit_bytes: 268_435_456,
                cgroup_limit_bytes: Some(268_435_456),
                idle_in_transaction_session_timeout_ms: 30_000,
                statement_timeout_ms: 10_000,
                lock_timeout_ms: 5_000,
                tcp_keepalive_ms: 5_000,
                client_connection_check_interval_ms: 5_000,
                zombie_detection_bound_ms: 15_000,
                source_free_disk_bytes: Some(21_474_836_480),
            },
            storage: StorageObservation {
                before_state_sha256:
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                after_state_sha256:
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                journal_mode: "WAL".into(),
                synchronous: "FULL".into(),
                auto_vacuum: "INCREMENTAL".into(),
                filesystem: "ext4".into(),
                disposable_probe_succeeded: true,
                filesystem_capacities: BTreeMap::from([(
                    "state".into(),
                    FilesystemCapacity {
                        configured_total_bytes: 1_000_000_000,
                        available_bytes: 710_000_000,
                        metadata_and_rounding_bytes: 5_000_000,
                        archive_publication_bytes: 5_000_000,
                    },
                )]),
                reader_budget_units: 1,
                scheduler_budget_units: 1,
                audit_budget_units: 1,
                writer_attestation: Some(WriterAttestation {
                    run_id: "run-fixture".into(),
                    connection_generation: 1,
                    fresh: true,
                    synchronous: "FULL".into(),
                }),
            },
            destination: DestinationObservation {
                clickhouse_mapping_matches: true,
                clickhouse_capabilities_match: true,
                archive_mapping_matches: true,
                archive_capabilities_match: true,
                tls_verified: true,
            },
            security: SecurityObservation {
                socket_parent_secure: true,
                socket_mode: 0o600,
                peer_policy_enforced: true,
                status_read_only: true,
                prometheus_read_only: true,
                listeners_match: true,
                redaction_probe_passed: true,
                lock_probe_configured: true,
                supervised_restart_configured: true,
            },
        }
    }
    fn reason(report: &PreflightReport, code: &str) -> bool {
        report.checks.iter().any(|x| x.reason == code)
    }

    #[test]
    fn supported_fixture_passes_without_mutation() {
        let cfg = config();
        let report = evaluate_with_capability(
            cfg.public(),
            cfg.fingerprints().runtime.as_str(),
            &supported(),
            Some(test_live_collector_capability()),
        );
        assert_eq!(report.outcome, CheckStatus::Healthy);
        assert!(
            report
                .checks
                .iter()
                .all(|x| x.status == CheckStatus::Healthy)
        );
    }

    #[test]
    fn unknown_metrics_and_stale_writer_are_degraded_not_healthy() {
        let cfg = config();
        let mut o = supported();
        o.source.source_free_disk_bytes = None;
        o.source.spill_counters_available = false;
        o.storage.writer_attestation = None;
        let report = evaluate_with_capability(
            cfg.public(),
            cfg.fingerprints().runtime.as_str(),
            &o,
            Some(test_live_collector_capability()),
        );
        assert_eq!(report.outcome, CheckStatus::Degraded);
        assert!(reason(&report, "PREFLIGHT_SOURCE_FREE_DISK_UNKNOWN"));
        assert!(reason(&report, "PREFLIGHT_SPILL_COUNTERS_UNAVAILABLE"));
        assert!(reason(&report, "PREFLIGHT_WRITER_SYNCHRONOUS_UNVERIFIED"));
    }

    #[test]
    fn mutation_privilege_socket_route_and_timeout_fail_independently() {
        let cfg = config();
        for (o, expected) in [
            {
                let mut o = supported();
                o.source.control_rows_each = 0;
                (o, "PREFLIGHT_CONTROL_ROW_CARDINALITY")
            },
            {
                let mut o = supported();
                o.source.control_insert = true;
                (o, "PREFLIGHT_CONTROL_PRIVILEGE_EXCESS")
            },
            {
                let mut o = supported();
                o.source.runtime_can_alter_publication = true;
                (o, "PREFLIGHT_PUBLICATION_PRIVILEGE_UNSAFE")
            },
            {
                let mut o = supported();
                o.security.socket_parent_secure = false;
                (o, "PREFLIGHT_OPERATOR_SOCKET_UNSAFE")
            },
            {
                let mut o = supported();
                o.security.status_read_only = false;
                (o, "PREFLIGHT_MUTATING_OBSERVABILITY_ROUTE")
            },
            {
                let mut o = supported();
                o.source.statement_timeout_ms = 9_999;
                (o, "PREFLIGHT_TIMEOUT_KEEPALIVE_INCOMPATIBLE")
            },
        ] {
            let report = evaluate_with_capability(
                cfg.public(),
                cfg.fingerprints().runtime.as_str(),
                &o,
                Some(test_live_collector_capability()),
            );
            assert_eq!(report.outcome, CheckStatus::Blocked);
            assert!(reason(&report, expected), "{expected}");
        }
    }

    #[test]
    fn protocol_memory_storage_and_mappings_fail_closed() {
        let cfg = config();
        for (o, expected) in [
            {
                let mut o = supported();
                o.source.streaming_option = "streaming=true".into();
                (o, "PREFLIGHT_PROTOCOL_FINGERPRINT_MISMATCH")
            },
            {
                let mut o = supported();
                o.source.cgroup_limit_bytes = Some(10);
                (o, "PREFLIGHT_MEMORY_BUDGET_INSUFFICIENT")
            },
            {
                let mut o = supported();
                o.storage.after_state_sha256 = "changed".into();
                (o, "PREFLIGHT_STATE_MUTATED")
            },
            {
                let mut o = supported();
                o.storage.journal_mode = "DELETE".into();
                (o, "PREFLIGHT_SQLITE_PRAGMA_MISMATCH")
            },
            {
                let mut o = supported();
                o.destination.clickhouse_mapping_matches = false;
                (o, "PREFLIGHT_CLICKHOUSE_MAPPING_UNSUPPORTED")
            },
            {
                let mut o = supported();
                o.destination.archive_mapping_matches = false;
                (o, "PREFLIGHT_ARCHIVE_MAPPING_UNSUPPORTED")
            },
        ] {
            let report = evaluate_with_capability(
                cfg.public(),
                cfg.fingerprints().runtime.as_str(),
                &o,
                Some(test_live_collector_capability()),
            );
            assert_eq!(report.outcome, CheckStatus::Blocked);
            assert!(reason(&report, expected), "{expected}");
        }
    }

    #[test]
    fn zero_multiple_each_forbidden_grant_and_replayed_attestation_fail() {
        let cfg = config();
        for count in [0, 2] {
            let mut o = supported();
            o.source.control_rows_each = count;
            assert!(reason(
                &evaluate_with_capability(
                    cfg.public(),
                    cfg.fingerprints().runtime.as_str(),
                    &o,
                    Some(test_live_collector_capability())
                ),
                "PREFLIGHT_CONTROL_ROW_CARDINALITY"
            ));
        }
        for mutation in 0..4 {
            let mut o = supported();
            match mutation {
                0 => o.source.control_insert = true,
                1 => o.source.control_delete = true,
                2 => o.source.control_update_key = true,
                _ => o.source.control_select_key_only = false,
            }
            assert!(reason(
                &evaluate_with_capability(
                    cfg.public(),
                    cfg.fingerprints().runtime.as_str(),
                    &o,
                    Some(test_live_collector_capability())
                ),
                "PREFLIGHT_CONTROL_PRIVILEGE_EXCESS"
            ));
        }
        let mut replayed = supported();
        replayed.current_connection_generation = Some(2);
        let report = evaluate_with_capability(
            cfg.public(),
            cfg.fingerprints().runtime.as_str(),
            &replayed,
            Some(test_live_collector_capability()),
        );
        assert_eq!(report.outcome, CheckStatus::Degraded);
        assert!(reason(&report, "PREFLIGHT_WRITER_SYNCHRONOUS_UNVERIFIED"));
    }

    #[test]
    fn every_timeout_and_keepalive_boundary_is_enforced() {
        let cfg = config();
        for mutation in 0..6 {
            let mut o = supported();
            match mutation {
                0 => o.source.idle_in_transaction_session_timeout_ms = 5_000,
                1 => o.source.statement_timeout_ms = 9_999,
                2 => o.source.lock_timeout_ms = 5_001,
                3 => o.source.tcp_keepalive_ms = 5_001,
                4 => o.source.client_connection_check_interval_ms = 5_001,
                _ => o.source.zombie_detection_bound_ms = 15_001,
            }
            assert!(reason(
                &evaluate_with_capability(
                    cfg.public(),
                    cfg.fingerprints().runtime.as_str(),
                    &o,
                    Some(test_live_collector_capability())
                ),
                "PREFLIGHT_TIMEOUT_KEEPALIVE_INCOMPATIBLE"
            ));
        }
    }

    #[test]
    fn resource_equations_reject_zero_overhead_and_configured_wal_floor() {
        let cfg = config();
        let mut zero = supported();
        zero.source.sqlite_writer_staging_bytes = 0;
        assert!(reason(
            &evaluate_with_capability(
                cfg.public(),
                cfg.fingerprints().runtime.as_str(),
                &zero,
                Some(test_live_collector_capability())
            ),
            "PREFLIGHT_MEMORY_BUDGET_INSUFFICIENT"
        ));
        let mut metadata = supported();
        metadata
            .storage
            .filesystem_capacities
            .get_mut("state")
            .unwrap()
            .metadata_and_rounding_bytes = 0;
        assert!(reason(
            &evaluate_with_capability(
                cfg.public(),
                cfg.fingerprints().runtime.as_str(),
                &metadata,
                Some(test_live_collector_capability())
            ),
            "PREFLIGHT_UNIT_BUDGET_INVALID"
        ));
        let mut publication = supported();
        publication
            .storage
            .filesystem_capacities
            .get_mut("state")
            .unwrap()
            .available_bytes = 700_000_001;
        assert!(reason(
            &evaluate_with_capability(
                cfg.public(),
                cfg.fingerprints().runtime.as_str(),
                &publication,
                Some(test_live_collector_capability())
            ),
            "PREFLIGHT_UNIT_BUDGET_INVALID"
        ));
        let mut wal = supported();
        wal.source.source_free_disk_bytes = Some(cfg.public().wal.source_free_bytes.0 - 1);
        assert!(reason(
            &evaluate_with_capability(
                cfg.public(),
                cfg.fingerprints().runtime.as_str(),
                &wal,
                Some(test_live_collector_capability())
            ),
            "PREFLIGHT_SOURCE_FREE_DISK_INSUFFICIENT"
        ));
    }

    #[test]
    fn untrusted_transport_cannot_attest_writer_or_real_store() {
        let cfg = config();
        let report = evaluate_untrusted(
            cfg.public(),
            cfg.fingerprints().runtime.as_str(),
            &supported(),
        );
        assert_eq!(report.outcome, CheckStatus::Degraded);
        assert!(reason(&report, "PREFLIGHT_WRITER_SYNCHRONOUS_UNVERIFIED"));
        assert!(reason(&report, "PREFLIGHT_STATE_HASH_UNVERIFIED"));
        assert!(reason(&report, "PREFLIGHT_LIVE_COLLECTION_UNVERIFIED"));
    }

    #[test]
    fn scenario_inventory_exactly_matches_report() {
        let cfg = config();
        let report = evaluate_with_capability(
            cfg.public(),
            cfg.fingerprints().runtime.as_str(),
            &supported(),
            Some(test_live_collector_capability()),
        );
        let contract: Value =
            serde_json::from_str(include_str!("../contracts/m1/preflight-cases.json")).unwrap();
        let expected = contract["cases"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x["id"].as_str().unwrap())
            .collect::<std::collections::BTreeSet<_>>();
        let actual = report
            .checks
            .iter()
            .map(|x| x.scenario_id)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(expected, actual);
    }

    #[test]
    fn envelope_is_cli_compatible_and_redacted() {
        let cfg = config();
        let report = evaluate_with_capability(
            cfg.public(),
            cfg.fingerprints().runtime.as_str(),
            &supported(),
            Some(test_live_collector_capability()),
        );
        let value = serde_json::to_value(envelope(&report)).unwrap();
        assert_eq!(value["command"], "CMD-CHECK");
        assert_eq!(value["code"], "PREFLIGHT_HEALTHY");
        let text = value.to_string();
        assert!(!text.contains("postgres://"));
        assert!(!text.contains("run-fixture"));
    }
}
