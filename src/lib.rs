//! Boring CDC library surfaces shared by the single `boring-cdc` binary.

pub mod article1_capture;
pub mod article1_row_view;
pub mod m1_bootstrap_sm;
pub mod m1_config;
pub mod m1_control_fixtures;
pub mod m1_ddl_fixtures;
pub mod m1_decoder;
pub mod m1_ordering;
pub mod m1_preflight;
pub mod m1_raw_demo;
pub mod m1_source_identity;
pub mod m1_transition_kernel;
pub mod m1_workload;

pub mod m1_cli_contract;

pub mod m2_capture_runtime;
pub mod m2_fault_status;
pub mod m2_heartbeat;
pub mod m2_init_recovery;
pub mod m2_journal;
pub mod m2_jsonl;
pub mod m2_leases;
pub mod m2_ownership;
pub mod m2_pressure;
pub mod m2_reconcile;
pub mod m2_schema;
pub mod m2_spool;

pub mod m3_bootstrap;
pub mod m3_planner;
pub mod m4_clickhouse_adapter;
pub mod m4_clickhouse_audit;
pub mod m4_clickhouse_durability;
pub mod m4_clickhouse_schema;
pub mod m4_mutations;

pub mod failure_policy;
