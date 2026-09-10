//! Relation-contract fingerprinting and fail-closed DDL guard fixture kernel.
//!
//! Database adapters provide catalog facts and lock-waiter observations. This module owns the
//! deterministic contract comparison and guard lifecycle, but deliberately performs no I/O.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

// M0-PROVISIONAL: boring-cdc-d-ddl (RECOMMENDED catalog poll interval).
pub const CATALOG_POLL_INTERVAL_MS: u64 = 5_000;
// M0-PROVISIONAL: boring-cdc-d-ddl (RECOMMENDED DDL waiter source-impact bound).
pub const DDL_WAITER_BOUND_MS: u64 = 5_000;
// M0-PROVISIONAL: boring-cdc-d-ddl (RECOMMENDED supported PostgreSQL majors).
pub const SUPPORTED_POSTGRES_MAJORS: [u16; 3] = [15, 16, 17];

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct LogicalRelationId {
    pub database_oid: u32,
    pub relation_oid: u32,
    pub logical_table_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColumnContract {
    pub attnum: i16,
    pub logical_order: u16,
    pub physical_order: u16,
    pub name: String,
    pub dropped: bool,
    pub type_oid: u32,
    pub typmod: i32,
    pub collation_oid: u32,
    pub nullable: bool,
    pub default_expression_hash: Option<String>,
    pub generated_expression_hash: Option<String>,
    pub identity_expression_hash: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KeyContract {
    pub primary_attnums: Vec<i16>,
    pub unique_indexes: BTreeMap<String, Vec<i16>>,
    pub replica_identity_mode: String,
    pub replica_identity_index: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionContract {
    pub routing: String,
    pub root_relation_oid: Option<u32>,
    pub key_expression_hash: Option<String>,
    pub bounds_expression_hash: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RelationContract {
    pub schema_version: u16,
    pub identity: LogicalRelationId,
    pub namespace: String,
    pub relation_name: String,
    pub columns: Vec<ColumnContract>,
    pub key: KeyContract,
    pub partition: PartitionContract,
    pub publication_member: bool,
    pub publication_attnums: BTreeSet<i16>,
}

impl RelationContract {
    /// Hashes canonical typed JSON. Callers must supply columns in physical order.
    pub fn fingerprint(&self) -> Result<String, DdlFailure> {
        if self.schema_version != 1
            || self.identity.logical_table_id.is_empty()
            || self
                .columns
                .windows(2)
                .any(|w| w[0].physical_order >= w[1].physical_order)
            || self
                .columns
                .iter()
                .map(|c| c.attnum)
                .collect::<BTreeSet<_>>()
                .len()
                != self.columns.len()
        {
            return Err(DdlFailure::contract("RELATION_CONTRACT_NON_CANONICAL"));
        }
        let bytes = serde_json::to_vec(self)
            .map_err(|_| DdlFailure::contract("RELATION_CONTRACT_SERIALIZATION"))?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }

    pub fn compare(&self, next: &Self, active_generation: bool) -> ContractDecision {
        if self == next {
            return ContractDecision::Unchanged;
        }
        if self.identity != next.identity || self.schema_version != next.schema_version {
            return ContractDecision::BlockAndRequireReseed("RELATION_IDENTITY_CHANGED");
        }
        if !active_generation && is_nullable_no_default_addition(self, next) {
            return ContractDecision::AdmitNullableAddition;
        }
        if active_generation {
            ContractDecision::InvalidateGeneration("ACTIVE_GENERATION_SCHEMA_DRIFT")
        } else {
            ContractDecision::BlockAndRequireReseed("RELATION_CONTRACT_CHANGED")
        }
    }
}

fn is_nullable_no_default_addition(old: &RelationContract, next: &RelationContract) -> bool {
    if old.key != next.key
        || old.partition != next.partition
        || old.publication_member != next.publication_member
    {
        return false;
    }
    if next.columns.len() != old.columns.len() + 1 {
        return false;
    }
    if next.columns[..old.columns.len()] != old.columns[..] {
        return false;
    }
    let c = next.columns.last().expect("one added column");
    c.nullable
        && !c.dropped
        && c.default_expression_hash.is_none()
        && c.generated_expression_hash.is_none()
        && c.identity_expression_hash.is_none()
        && next.publication_attnums
            == old
                .publication_attnums
                .union(&BTreeSet::from([c.attnum]))
                .copied()
                .collect()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContractDecision {
    Unchanged,
    AdmitNullableAddition,
    InvalidateGeneration(&'static str),
    BlockAndRequireReseed(&'static str),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum GuardPhase {
    BeforeExport,
    Exported,
    Copying,
    AwaitingDurableFence,
    Released,
    Invalid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GuardState {
    generation: u64,
    phase: GuardPhase,
    locked: Vec<LogicalRelationId>,
    feedback_gate_open: bool,
}

impl GuardState {
    pub fn acquire_before_export(
        generation: u64,
        mut relations: Vec<LogicalRelationId>,
    ) -> Result<Self, DdlFailure> {
        if generation == 0 || relations.is_empty() {
            return Err(DdlFailure::guard("DDL_GUARD_INPUT_INVALID"));
        }
        relations.sort();
        if relations.windows(2).any(|w| w[0] == w[1]) {
            return Err(DdlFailure::guard("DDL_GUARD_DUPLICATE_RELATION"));
        }
        Ok(Self {
            generation,
            phase: GuardPhase::BeforeExport,
            locked: relations,
            feedback_gate_open: false,
        })
    }
    pub fn locked_relations(&self) -> &[LogicalRelationId] {
        &self.locked
    }
    pub fn phase(&self) -> GuardPhase {
        self.phase
    }
    pub fn feedback_gate_open(&self) -> bool {
        self.feedback_gate_open
    }
    pub fn advance(&mut self, next: GuardPhase) -> Result<(), DdlFailure> {
        let valid = matches!(
            (self.phase, next),
            (GuardPhase::BeforeExport, GuardPhase::Exported)
                | (GuardPhase::Exported, GuardPhase::Copying)
                | (GuardPhase::Copying, GuardPhase::AwaitingDurableFence)
        );
        if !valid {
            return Err(DdlFailure::guard("DDL_GUARD_PHASE_ORDER"));
        }
        self.phase = next;
        Ok(())
    }
    pub fn durable_fence_observed(&mut self, generation: u64) -> Result<(), DdlFailure> {
        if generation != self.generation || self.phase != GuardPhase::AwaitingDurableFence {
            return Err(DdlFailure::guard("DDL_GUARD_FENCE_MISMATCH"));
        }
        self.phase = GuardPhase::Released;
        self.feedback_gate_open = true;
        Ok(())
    }
    pub fn observe_waiter(&mut self, waiter: &WaiterObservation) -> Result<(), DdlFailure> {
        if self.phase == GuardPhase::Released || self.phase == GuardPhase::Invalid {
            return Ok(());
        }
        if waiter.conflicts
            && self.locked.contains(&waiter.relation)
            && waiter.age_ms >= DDL_WAITER_BOUND_MS
        {
            self.phase = GuardPhase::Invalid;
            self.feedback_gate_open = true;
            return Err(DdlFailure::waiter());
        }
        Ok(())
    }
    pub fn guard_session_lost(&mut self) -> DdlFailure {
        self.phase = GuardPhase::Invalid;
        self.feedback_gate_open = true;
        DdlFailure::guard("DDL_GUARD_SESSION_LOST")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WaiterObservation {
    pub relation: LogicalRelationId,
    pub age_ms: u64,
    pub conflicts: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DdlFailure {
    pub class: &'static str,
    pub fingerprint: &'static str,
    pub failed_boundary: &'static str,
    pub allowed_actions: &'static [&'static str],
}
impl DdlFailure {
    const fn contract(code: &'static str) -> Self {
        Self {
            class: "deterministic",
            fingerprint: code,
            failed_boundary: "before_decode_or_feedback",
            allowed_actions: &["status", "recover reseed"],
        }
    }
    const fn guard(code: &'static str) -> Self {
        Self {
            class: "source_impact",
            fingerprint: code,
            failed_boundary: "active_generation",
            allowed_actions: &["status", "start new generation"],
        }
    }
    const fn waiter() -> Self {
        Self {
            class: "source_impact",
            fingerprint: "BACKFILL_DDL_WAITER",
            failed_boundary: "guard_waiter_bound",
            allowed_actions: &["status", "start new generation"],
        }
    }
}

#[derive(Default)]
pub struct RelationValidationGate {
    admitted: BTreeMap<u32, String>,
    pending: BTreeSet<u32>,
    blocked: bool,
}
impl RelationValidationGate {
    pub fn relation_message(&mut self, relation_id: u32, wire_changed: bool) {
        if wire_changed || !self.admitted.contains_key(&relation_id) {
            self.pending.insert(relation_id);
        }
    }
    pub fn admit_catalog(
        &mut self,
        relation_id: u32,
        contract: &RelationContract,
        expected: Option<&RelationContract>,
        active_generation: bool,
    ) -> Result<ContractDecision, DdlFailure> {
        if !self.pending.remove(&relation_id) {
            self.blocked = true;
            return Err(DdlFailure::contract("RELATION_VALIDATION_NOT_PENDING"));
        }
        let fp = contract.fingerprint()?;
        let decision = expected.map_or(ContractDecision::Unchanged, |old| {
            old.compare(contract, active_generation)
        });
        if matches!(
            decision,
            ContractDecision::BlockAndRequireReseed(_) | ContractDecision::InvalidateGeneration(_)
        ) {
            self.blocked = true;
        } else {
            self.admitted.insert(relation_id, fp);
        }
        Ok(decision)
    }
    pub fn require_dml(&self, relation_id: u32) -> Result<(), DdlFailure> {
        if self.blocked
            || self.pending.contains(&relation_id)
            || !self.admitted.contains_key(&relation_id)
        {
            Err(DdlFailure::contract("DML_BEFORE_RELATION_VALIDATION"))
        } else {
            Ok(())
        }
    }
    pub fn feedback_allowed(&self) -> bool {
        !self.blocked && self.pending.is_empty()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    fn relation(oid: u32) -> RelationContract {
        RelationContract {
            schema_version: 1,
            identity: LogicalRelationId {
                database_oid: 1,
                relation_oid: oid,
                logical_table_id: format!("table-{oid}"),
            },
            namespace: "public".into(),
            relation_name: format!("t{oid}"),
            columns: vec![ColumnContract {
                attnum: 1,
                logical_order: 0,
                physical_order: 0,
                name: "id".into(),
                dropped: false,
                type_oid: 20,
                typmod: -1,
                collation_oid: 0,
                nullable: false,
                default_expression_hash: None,
                generated_expression_hash: None,
                identity_expression_hash: None,
            }],
            key: KeyContract {
                primary_attnums: vec![1],
                unique_indexes: BTreeMap::new(),
                replica_identity_mode: "default".into(),
                replica_identity_index: None,
            },
            partition: PartitionContract {
                routing: "plain".into(),
                root_relation_oid: None,
                key_expression_hash: None,
                bounds_expression_hash: None,
            },
            publication_member: true,
            publication_attnums: BTreeSet::from([1]),
        }
    }
    fn nullable_add(old: &RelationContract) -> RelationContract {
        let mut n = old.clone();
        n.columns.push(ColumnContract {
            attnum: 2,
            logical_order: 1,
            physical_order: 1,
            name: "optional".into(),
            dropped: false,
            type_oid: 25,
            typmod: -1,
            collation_oid: 100,
            nullable: true,
            default_expression_hash: None,
            generated_expression_hash: None,
            identity_expression_hash: None,
        });
        n.publication_attnums.insert(2);
        n
    }

    // SCENARIO: SCN-IDLE-NO-ROW-DDL
    #[test]
    fn catalog_poll_fingerprint_detects_idle_ddl_and_only_safe_addition_is_admitted() {
        let old = relation(10);
        let add = nullable_add(&old);
        assert_ne!(old.fingerprint().unwrap(), add.fingerprint().unwrap());
        assert_eq!(
            old.compare(&add, false),
            ContractDecision::AdmitNullableAddition
        );
        let mut bad = add.clone();
        bad.columns[1].default_expression_hash = Some("sha256:default".into());
        assert!(matches!(
            old.compare(&bad, false),
            ContractDecision::BlockAndRequireReseed(_)
        ));
    }
    // SCENARIO: SCN-DDL-DURING-ACTIVE-BACKFILL
    #[test]
    fn all_contract_changes_invalidate_active_generation() {
        let old = relation(10);
        assert!(matches!(
            old.compare(&nullable_add(&old), true),
            ContractDecision::InvalidateGeneration(_)
        ));
    }
    // SCENARIO: SCN-DDL-IMMEDIATELY-BEFORE-AFTER-COPY-FENCE
    #[test]
    fn guard_spans_all_copy_boundaries_until_durable_fence() {
        let mut g =
            GuardState::acquire_before_export(7, vec![relation(2).identity, relation(1).identity])
                .unwrap();
        assert_eq!(g.locked[0].relation_oid, 1);
        g.advance(GuardPhase::Exported).unwrap();
        g.advance(GuardPhase::Copying).unwrap();
        g.advance(GuardPhase::AwaitingDurableFence).unwrap();
        assert!(!g.feedback_gate_open());
        g.durable_fence_observed(7).unwrap();
        assert_eq!(g.phase(), GuardPhase::Released);
    }
    // SCENARIO: SCN-IMPORTER-DDL-GUARD-LOSS
    #[test]
    fn guard_loss_invalidates_and_releases_feedback_gate() {
        let mut g = GuardState::acquire_before_export(1, vec![relation(1).identity]).unwrap();
        let e = g.guard_session_lost();
        assert_eq!(e.fingerprint, "DDL_GUARD_SESSION_LOST");
        assert_eq!(g.phase(), GuardPhase::Invalid);
        assert!(g.feedback_gate_open());
    }
    // SCENARIO: SCN-M1-DDL-WAITER-BOUND
    #[test]
    fn conflicting_waiter_at_bound_invalidates_and_releases() {
        let id = relation(1).identity;
        let mut g = GuardState::acquire_before_export(1, vec![id.clone()]).unwrap();
        assert!(
            g.observe_waiter(&WaiterObservation {
                relation: id.clone(),
                age_ms: DDL_WAITER_BOUND_MS - 1,
                conflicts: true
            })
            .is_ok()
        );
        let e = g
            .observe_waiter(&WaiterObservation {
                relation: id,
                age_ms: DDL_WAITER_BOUND_MS,
                conflicts: true,
            })
            .unwrap_err();
        assert_eq!(e.fingerprint, "BACKFILL_DDL_WAITER");
        assert!(g.feedback_gate_open());
    }
    // SCENARIO: SCN-M1-DDL-IMMEDIATE-RELATION-DML
    #[test]
    fn changed_relation_synchronously_blocks_following_dml_and_feedback() {
        let old = relation(1);
        let mut gate = RelationValidationGate::default();
        gate.relation_message(1, true);
        assert!(gate.require_dml(1).is_err());
        assert!(!gate.feedback_allowed());
        assert_eq!(
            gate.admit_catalog(1, &old, None, false).unwrap(),
            ContractDecision::Unchanged
        );
        assert!(gate.require_dml(1).is_ok());
        let changed = nullable_add(&old);
        gate.relation_message(1, true);
        assert_eq!(
            gate.admit_catalog(1, &changed, Some(&old), true).unwrap(),
            ContractDecision::InvalidateGeneration("ACTIVE_GENERATION_SCHEMA_DRIFT")
        );
        assert!(gate.require_dml(1).is_err());
        assert!(!gate.feedback_allowed());
    }
    #[test]
    fn noncanonical_contracts_and_guard_order_fail_closed() {
        let mut r = relation(1);
        r.columns.push(r.columns[0].clone());
        assert_eq!(
            r.fingerprint().unwrap_err().fingerprint,
            "RELATION_CONTRACT_NON_CANONICAL"
        );
        let id = relation(1).identity;
        assert_eq!(
            GuardState::acquire_before_export(1, vec![id.clone(), id])
                .unwrap_err()
                .fingerprint,
            "DDL_GUARD_DUPLICATE_RELATION"
        );
    }
    #[test]
    fn provisional_bounds_and_matrix_are_explicit() {
        assert_eq!(SUPPORTED_POSTGRES_MAJORS, [15, 16, 17]);
        assert_eq!(CATALOG_POLL_INTERVAL_MS, 5000);
        assert_eq!(DDL_WAITER_BOUND_MS, 5000);
    }
}
