//! Read-only preflight classification for `boring-cdc check`.
//!
//! Adapters collect observations; this module only compares them with the loaded
//! configuration. It never opens PostgreSQL, SQLite, ClickHouse, sockets, or files.

use crate::m1_cli_contract::{CLI_SCHEMA_VERSION, CliEnvelope, NextCommand};
use crate::m1_config::PublicConfig;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

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
    pub table_contracts_match: bool,
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
pub struct StorageObservation {
    pub before_state_sha256: String,
    pub after_state_sha256: String,
    pub journal_mode: String,
    pub synchronous: String,
    pub auto_vacuum: String,
    pub filesystem: String,
    pub disposable_probe_succeeded: bool,
    pub sqlite_budget_bytes: u64,
    pub spool_budget_bytes: u64,
    pub archive_budget_bytes: u64,
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

/// Classify a complete immutable observation. No mutation or I/O occurs here.
pub fn evaluate(config: &PublicConfig, observed: &PreflightObservation) -> PreflightReport {
    let mut c = Vec::new();
    push(
        &mut c,
        "SCN-M1-PREFLIGHT-SCHEMA",
        observed.schema_version == PREFLIGHT_SCHEMA_VERSION,
        "PREFLIGHT_OBSERVATION_SCHEMA_UNSUPPORTED",
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
        "SCN-M1-PREFLIGHT-TABLES",
        observed.source.table_contracts_match,
        "PREFLIGHT_TABLE_CONTRACT_MISMATCH",
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
        observed.source.worker_bytes,
    ]);
    let effective_limit = observed
        .source
        .cgroup_limit_bytes
        .map_or(observed.source.process_limit_bytes, |v| {
            v.min(observed.source.process_limit_bytes)
        });
    let memory_ok = aggregate
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

    push(
        &mut c,
        "SCN-M1-PREFLIGHT-NO-MUTATION",
        !observed.storage.before_state_sha256.is_empty()
            && observed.storage.before_state_sha256 == observed.storage.after_state_sha256,
        "PREFLIGHT_STATE_MUTATED",
    );
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
            if a.fresh
                && !a.run_id.is_empty()
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
    let budgets_ok = observed.storage.sqlite_budget_bytes > 0
        && observed.storage.spool_budget_bytes > 0
        && observed.storage.archive_budget_bytes > 0
        && observed.storage.reader_budget_units > 0
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

    c.push(CheckResult {
        scenario_id: "SCN-M1-PREFLIGHT-SOURCE-FREE-DISK",
        status: match observed.source.source_free_disk_bytes {
            Some(v) if v >= config.wal.source_free_bytes.0 => CheckStatus::Healthy,
            Some(_) => CheckStatus::Blocked,
            None => CheckStatus::Degraded,
        },
        reason: match observed.source.source_free_disk_bytes {
            Some(v) if v >= config.wal.source_free_bytes.0 => "PREFLIGHT_OK",
            Some(_) => "PREFLIGHT_SOURCE_FREE_DISK_INSUFFICIENT",
            None => "PREFLIGHT_SOURCE_FREE_DISK_UNKNOWN",
        },
        units: Some("bytes"),
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
        "PREFLIGHT_MUTATING_OBSERVABILITY_ROUTE",
        "PREFLIGHT_OBSERVATION_SCHEMA_UNSUPPORTED",
        "PREFLIGHT_OPERATOR_SOCKET_UNSAFE",
        "PREFLIGHT_OWNERSHIP_RECOVERY_UNSAFE",
        "PREFLIGHT_PARTITION_UNSUPPORTED",
        "PREFLIGHT_POSTGRES_VERSION_UNSUPPORTED",
        "PREFLIGHT_PROTOCOL_FINGERPRINT_MISMATCH",
        "PREFLIGHT_PUBLICATION_FINGERPRINT_MISMATCH",
        "PREFLIGHT_PUBLICATION_PRIVILEGE_UNSAFE",
        "PREFLIGHT_REDACTION_POLICY_FAILED",
        "PREFLIGHT_REPLICA_IDENTITY_INCOMPLETE",
        "PREFLIGHT_REPLICATION_PRIVILEGE_MISSING",
        "PREFLIGHT_REQUIRED_GRANT_MISSING",
        "PREFLIGHT_SLOT_BINDING_MISMATCH",
        "PREFLIGHT_SLOT_INTENT_NOT_BOUND",
        "PREFLIGHT_SOURCE_FREE_DISK_INSUFFICIENT",
        "PREFLIGHT_SOURCE_FREE_DISK_UNKNOWN",
        "PREFLIGHT_SPILL_COUNTERS_UNAVAILABLE",
        "PREFLIGHT_SQLITE_PRAGMA_MISMATCH",
        "PREFLIGHT_STATE_MUTATED",
        "PREFLIGHT_TABLE_CONTRACT_MISMATCH",
        "PREFLIGHT_TIMEOUT_KEEPALIVE_INCOMPATIBLE",
        "PREFLIGHT_TYPE_UNSUPPORTED",
        "PREFLIGHT_UNIT_BUDGET_INVALID",
        "PREFLIGHT_WAL_LEVEL_NOT_LOGICAL",
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
                table_contracts_match: true,
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
                worker_bytes: 67_108_864,
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
                before_state_sha256: "same".into(),
                after_state_sha256: "same".into(),
                journal_mode: "WAL".into(),
                synchronous: "FULL".into(),
                auto_vacuum: "INCREMENTAL".into(),
                filesystem: "ext4".into(),
                disposable_probe_succeeded: true,
                sqlite_budget_bytes: 1,
                spool_budget_bytes: 1,
                archive_budget_bytes: 1,
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
        let report = evaluate(cfg.public(), &supported());
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
        let report = evaluate(cfg.public(), &o);
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
            let report = evaluate(cfg.public(), &o);
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
            let report = evaluate(cfg.public(), &o);
            assert_eq!(report.outcome, CheckStatus::Blocked);
            assert!(reason(&report, expected), "{expected}");
        }
    }

    #[test]
    fn envelope_is_cli_compatible_and_redacted() {
        let cfg = config();
        let report = evaluate(cfg.public(), &supported());
        let value = serde_json::to_value(envelope(&report)).unwrap();
        assert_eq!(value["command"], "CMD-CHECK");
        assert_eq!(value["code"], "PREFLIGHT_HEALTHY");
        let text = value.to_string();
        assert!(!text.contains("postgres://"));
        assert!(!text.contains("run-fixture"));
    }
}
