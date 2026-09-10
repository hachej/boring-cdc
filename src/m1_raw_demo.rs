//! M1 milestone inspection inventory.
//!
//! This module does not reimplement leaf state machines. It provides the typed exact-set gate
//! used by the raw-demo scripts to prove that every M1 exit family was observed once.

use std::collections::BTreeMap;

pub const REQUIRED_CASES: [(&str, &str, &str); 15] = [
    ("SCN-M1-RAW-FIXED-SEED", "boring-cdc-m1-decoder", "decoded"),
    (
        "SCN-M1-RAW-COPYBOTH-RESTART",
        "boring-cdc-m1-decoder",
        "resume_safe",
    ),
    (
        "SCN-M1-RAW-HEARTBEAT",
        "boring-cdc-m1-control-fixtures",
        "durable_noop",
    ),
    (
        "SCN-M1-RAW-BOOTSTRAP",
        "boring-cdc-m1-bootstrap-sm",
        "feedback_gated",
    ),
    (
        "SCN-M1-RAW-EXACT-SET",
        "boring-cdc-m1-workload",
        "oracle_pass",
    ),
    (
        "SCN-M1-RAW-FULL-RESEED",
        "boring-cdc-m1-bootstrap-sm",
        "requires_reseed",
    ),
    (
        "SCN-M1-RAW-TRUNCATE",
        "boring-cdc-m1-control-fixtures",
        "requires_reseed",
    ),
    (
        "SCN-M1-RAW-PUBLICATION-DRIFT",
        "boring-cdc-m1-control-fixtures",
        "requires_reseed",
    ),
    (
        "SCN-M1-RAW-IDLE-DDL",
        "boring-cdc-m1-ddl-fixtures",
        "blocked",
    ),
    (
        "SCN-M1-RAW-IMMEDIATE-DDL",
        "boring-cdc-m1-ddl-fixtures",
        "blocked",
    ),
    (
        "SCN-M1-RAW-UNSUPPORTED-PROTOCOL",
        "boring-cdc-m1-decoder",
        "blocked",
    ),
    (
        "SCN-M1-RAW-UNSUPPORTED-TABLE",
        "boring-cdc-m1-ddl-fixtures",
        "blocked",
    ),
    (
        "SCN-M1-RAW-UNSUPPORTED-TYPE",
        "boring-cdc-m1-ddl-fixtures",
        "blocked",
    ),
    (
        "SCN-M1-RAW-IDENTITY-CONFLICT",
        "boring-cdc-m1-ordering",
        "blocked",
    ),
    (
        "SCN-M1-RAW-NO-ONLINE-TABLE-ADD",
        "boring-cdc-m1-control-fixtures",
        "requires_reseed",
    ),
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaseObservation<'a> {
    pub scenario_id: &'a str,
    pub consumed_owner: &'a str,
    pub outcome: &'a str,
    pub checkpoint_advanced: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExitReport {
    pub observed: usize,
    pub required: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExitFailure {
    pub fingerprint: &'static str,
    pub failed_boundary: &'static str,
}

/// Accepts the milestone exact set only. Duplicate, missing, extra, owner-mismatched, or
/// fail-closed cases that claim checkpoint movement are rejected.
pub fn verify_exit(observations: &[CaseObservation<'_>]) -> Result<ExitReport, ExitFailure> {
    let expected: BTreeMap<_, _> = REQUIRED_CASES
        .into_iter()
        .map(|(id, owner, outcome)| (id, (owner, outcome)))
        .collect();
    let mut actual = BTreeMap::new();
    for observation in observations {
        if actual
            .insert(observation.scenario_id, observation)
            .is_some()
        {
            return Err(failure("M1_EXIT_DUPLICATE_SCENARIO"));
        }
    }
    if actual.len() != expected.len() || actual.keys().any(|id| !expected.contains_key(id)) {
        return Err(failure("M1_EXIT_EXACT_SET_MISMATCH"));
    }
    for (id, (owner, outcome)) in expected {
        let observation = actual[id];
        if observation.consumed_owner != owner || observation.outcome != outcome {
            return Err(failure("M1_EXIT_ASSERTION_MISMATCH"));
        }
        if matches!(outcome, "blocked" | "requires_reseed") && observation.checkpoint_advanced {
            return Err(failure("M1_EXIT_BLOCKED_CHECKPOINT_ADVANCED"));
        }
    }
    Ok(ExitReport {
        observed: actual.len(),
        required: REQUIRED_CASES.len(),
    })
}

const fn failure(fingerprint: &'static str) -> ExitFailure {
    ExitFailure {
        fingerprint,
        failed_boundary: "before_m1_milestone_acceptance",
    }
}

/// Emit a structured milestone observation only after checking all fields against the
/// product-owned assertion inventory. Tests call this after their behavioral assertions, so
/// evidence comes from the executing product test rather than the JSON evidence contract.
#[cfg(test)]
pub fn emit_asserted_case(scenario_id: &str, state: &str, checkpoint: &str, log: &str) {
    let expected = match scenario_id {
        "SCN-M1-RAW-FIXED-SEED" => ("decoded", "unchanged_until_durable", "raw_event_normalized"),
        "SCN-M1-RAW-COPYBOTH-RESTART" => ("resume_safe", "durable_only", "copyboth_restart_safe"),
        "SCN-M1-RAW-HEARTBEAT" => ("durable_noop", "durable_only", "heartbeat_control_noop"),
        "SCN-M1-RAW-BOOTSTRAP" => (
            "feedback_gated",
            "creation_floor_not_progress",
            "bootstrap_gate",
        ),
        "SCN-M1-RAW-EXACT-SET" => ("oracle_pass", "fence_observed", "exact_set_pass"),
        "SCN-M1-RAW-FULL-RESEED" => ("requires_reseed", "unchanged", "full_reseed_required"),
        "SCN-M1-RAW-TRUNCATE" => ("requires_reseed", "unchanged", "truncate_requires_reseed"),
        "SCN-M1-RAW-PUBLICATION-DRIFT" => ("requires_reseed", "unchanged", "publication_drift"),
        "SCN-M1-RAW-IDLE-DDL" => ("blocked", "unchanged", "idle_ddl_blocked"),
        "SCN-M1-RAW-IMMEDIATE-DDL" => ("blocked", "unchanged", "immediate_ddl_blocked"),
        "SCN-M1-RAW-UNSUPPORTED-PROTOCOL" => ("blocked", "unchanged", "unsupported_protocol"),
        "SCN-M1-RAW-UNSUPPORTED-TABLE" => ("blocked", "unchanged", "unsupported_table"),
        "SCN-M1-RAW-UNSUPPORTED-TYPE" => ("blocked", "unchanged", "unsupported_type"),
        "SCN-M1-RAW-IDENTITY-CONFLICT" => ("blocked", "unchanged", "identity_payload_conflict"),
        "SCN-M1-RAW-NO-ONLINE-TABLE-ADD" => ("requires_reseed", "unchanged", "no_online_table_add"),
        other => panic!("unexpected M1 raw observation: {other}"),
    };
    assert_eq!((state, checkpoint, log), expected);
    println!("CASE {scenario_id} state={state} checkpoint={checkpoint} log={log}");
}

#[cfg(test)]
pub mod tests {
    use super::*;

    fn valid() -> Vec<CaseObservation<'static>> {
        REQUIRED_CASES
            .iter()
            .map(|(scenario_id, consumed_owner, outcome)| CaseObservation {
                scenario_id,
                consumed_owner,
                outcome,
                checkpoint_advanced: false,
            })
            .collect()
    }

    #[test]
    fn exact_m1_exit_set_passes_once() {
        let report = verify_exit(&valid()).unwrap();
        assert_eq!(report.observed, 15);
        assert_eq!(report.required, 15);
    }

    #[test]
    fn missing_extra_and_duplicate_scenarios_fail_closed() {
        let mut rows = valid();
        rows.pop();
        assert_eq!(
            verify_exit(&rows).unwrap_err().fingerprint,
            "M1_EXIT_EXACT_SET_MISMATCH"
        );
        let mut rows = valid();
        rows.push(rows[0].clone());
        assert_eq!(
            verify_exit(&rows).unwrap_err().fingerprint,
            "M1_EXIT_DUPLICATE_SCENARIO"
        );
        let mut rows = valid();
        rows[0].scenario_id = "SCN-M1-RAW-EXTRA";
        assert_eq!(
            verify_exit(&rows).unwrap_err().fingerprint,
            "M1_EXIT_EXACT_SET_MISMATCH"
        );
    }

    #[test]
    fn owner_and_outcome_mismatches_fail_closed() {
        let mut rows = valid();
        rows[0].consumed_owner = "wrong";
        assert_eq!(
            verify_exit(&rows).unwrap_err().fingerprint,
            "M1_EXIT_ASSERTION_MISMATCH"
        );
        let mut rows = valid();
        rows[0].outcome = "best_effort";
        assert_eq!(
            verify_exit(&rows).unwrap_err().fingerprint,
            "M1_EXIT_ASSERTION_MISMATCH"
        );
    }

    #[test]
    fn blocked_and_reseed_outcomes_never_advance_checkpoint() {
        for index in [5, 6, 7, 8, 9, 10, 11, 12, 13, 14] {
            let mut rows = valid();
            rows[index].checkpoint_advanced = true;
            assert_eq!(
                verify_exit(&rows).unwrap_err().fingerprint,
                "M1_EXIT_BLOCKED_CHECKPOINT_ADVANCED"
            );
        }
    }

    #[test]
    fn fixed_seed_exact_set_oracle_smoke_passes() {
        use crate::m1_workload::{
            Profile, accepted_contract_digests, deterministic_fixture, evaluate,
        };
        let fixture =
            deterministic_fixture(7, Profile::Smoke, accepted_contract_digests()).unwrap();
        let report = evaluate(
            &fixture,
            &fixture.ledger,
            Some(&fixture.business_events),
            Some(&fixture.fence),
            &fixture.final_state,
        )
        .unwrap();
        assert!(report.passed());

        crate::m1_raw_demo::emit_asserted_case(
            "SCN-M1-RAW-EXACT-SET",
            "oracle_pass",
            "fence_observed",
            "exact_set_pass",
        );
    }
}
