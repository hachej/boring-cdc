//! Boring CDC library surfaces shared by the single `boring-cdc` binary.

pub mod m1_bootstrap_sm;
pub mod m1_config;
pub mod m1_control_fixtures;
pub mod m1_ddl_fixtures;
pub mod m1_decoder;
pub mod m1_ordering;
pub mod m1_preflight;
pub mod m1_source_identity;
pub mod m1_transition_kernel;

pub mod m1_cli_contract;

pub mod m2_journal;
pub mod m2_ownership;
pub mod m2_schema;
pub mod m2_spool;

pub mod failure_policy;
