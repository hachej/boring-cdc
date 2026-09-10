//! Relation-contract fingerprinting and fail-closed DDL guard fixture kernel.
//!
//! Database adapters provide catalog facts and lock-waiter observations. This module owns the
//! deterministic contract comparison and guard lifecycle, but deliberately performs no I/O.

use crate::m1_decoder::{
    CopyBothEvent, DecodeFailure, Decoder, PgoutputEvent, RelationContract as WireRelationContract,
    RowKind, WireLimits,
};
use crate::m1_transition_kernel::DurableSourceBoundary;
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
    if old.namespace != next.namespace
        || old.relation_name != next.relation_name
        || old.key != next.key
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
pub enum AdmissionDecision {
    Admit,
    BlockCapture(&'static str),
    BlockDestination(&'static str),
}

/// Fixture-level admission for the schema/delete-safety dimensions carried by the fingerprint.
pub fn validate_relation_admission(
    contract: &RelationContract,
    capture_type_oids: &BTreeSet<u32>,
    destination_type_oids: &BTreeSet<u32>,
    destination_supports_delete: bool,
) -> AdmissionDecision {
    if !contract.publication_member {
        return AdmissionDecision::BlockCapture("SELECTED_TABLE_NOT_PUBLISHED");
    }
    if contract
        .columns
        .iter()
        .filter(|c| !c.dropped)
        .any(|c| !capture_type_oids.contains(&c.type_oid))
    {
        return AdmissionDecision::BlockCapture("SOURCE_TYPE_UNSUPPORTED");
    }
    let effective_key = match contract.key.replica_identity_mode.as_str() {
        "default" => Some(&contract.key.primary_attnums),
        "index" => contract
            .key
            .replica_identity_index
            .as_ref()
            .and_then(|name| contract.key.unique_indexes.get(name)),
        _ => None,
    };
    let Some(key) = effective_key else {
        return AdmissionDecision::BlockCapture("REPLICA_IDENTITY_UNSUPPORTED");
    };
    if key.is_empty()
        || key.iter().any(|attnum| {
            contract
                .columns
                .iter()
                .find(|c| c.attnum == *attnum)
                .is_none_or(|c| c.nullable || c.dropped)
        })
    {
        return AdmissionDecision::BlockCapture("REPLICA_IDENTITY_INCOMPLETE");
    }
    if !destination_supports_delete {
        return AdmissionDecision::BlockDestination("DESTINATION_DELETE_UNSUPPORTED");
    }
    if contract
        .columns
        .iter()
        .filter(|c| !c.dropped)
        .any(|c| !destination_type_oids.contains(&c.type_oid))
    {
        return AdmissionDecision::BlockDestination("DESTINATION_TYPE_INCOMPATIBLE");
    }
    AdmissionDecision::Admit
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
pub struct DurableFenceProof {
    capture_epoch: u64,
    generation: u64,
    table_set_fingerprint: String,
    nonce: u64,
    _journal_boundary: DurableSourceBoundary,
}
impl DurableFenceProof {
    /// Only the journal transaction owner can supply the durable boundary capability.
    #[cfg(test)]
    fn from_journal_commit(
        capture_epoch: u64,
        generation: u64,
        table_set_fingerprint: String,
        nonce: u64,
        boundary: DurableSourceBoundary,
    ) -> Self {
        Self {
            capture_epoch,
            generation,
            table_set_fingerprint,
            nonce,
            _journal_boundary: boundary,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GuardState {
    capture_epoch: u64,
    generation: u64,
    table_set_fingerprint: String,
    intended_fence_nonce: u64,
    phase: GuardPhase,
    locked: Vec<LogicalRelationId>,
    feedback_gate_open: bool,
}

impl GuardState {
    pub fn acquire_before_export(
        capture_epoch: u64,
        generation: u64,
        table_set_fingerprint: String,
        intended_fence_nonce: u64,
        mut relations: Vec<LogicalRelationId>,
    ) -> Result<Self, DdlFailure> {
        if capture_epoch == 0
            || generation == 0
            || table_set_fingerprint.is_empty()
            || intended_fence_nonce == 0
            || relations.is_empty()
        {
            return Err(DdlFailure::guard("DDL_GUARD_INPUT_INVALID"));
        }
        relations.sort();
        if relations.windows(2).any(|w| w[0] == w[1]) {
            return Err(DdlFailure::guard("DDL_GUARD_DUPLICATE_RELATION"));
        }
        Ok(Self {
            capture_epoch,
            generation,
            table_set_fingerprint,
            intended_fence_nonce,
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
    pub fn durable_fence_observed(&mut self, proof: &DurableFenceProof) -> Result<(), DdlFailure> {
        if proof.capture_epoch != self.capture_epoch
            || proof.generation != self.generation
            || proof.table_set_fingerprint != self.table_set_fingerprint
            || proof.nonce != self.intended_fence_nonce
            || self.phase != GuardPhase::AwaitingDurableFence
        {
            return Err(DdlFailure::guard("DDL_GUARD_FENCE_MISMATCH"));
        }
        self.phase = GuardPhase::Released;
        self.feedback_gate_open = true;
        self.locked.clear();
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
            self.locked.clear();
            return Err(DdlFailure::waiter());
        }
        Ok(())
    }
    pub fn guard_session_lost(&mut self) -> DdlFailure {
        self.phase = GuardPhase::Invalid;
        self.feedback_gate_open = true;
        self.locked.clear();
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
        if !self.pending.contains(&relation_id) {
            self.blocked = true;
            return Err(DdlFailure::contract("RELATION_VALIDATION_NOT_PENDING"));
        }
        let fp = match contract.fingerprint() {
            Ok(value) => value,
            Err(failure) => {
                self.blocked = true;
                return Err(failure);
            }
        };
        self.pending.remove(&relation_id);
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
    /// Adapter boundary for actual decoder events: Relation arms synchronous catalog validation;
    /// a row can cross only after that full contract has been admitted.
    pub fn observe_decoder_event(&mut self, event: &PgoutputEvent) -> Result<(), DdlFailure> {
        match event {
            PgoutputEvent::RelationNeedsValidation(relation) => {
                self.relation_message(relation.id, true);
                Ok(())
            }
            PgoutputEvent::Row(change)
                if matches!(
                    change.kind,
                    RowKind::Insert | RowKind::Update | RowKind::Delete
                ) =>
            {
                self.require_dml(change.relation_id)
            }
            _ => Ok(()),
        }
    }
    pub fn feedback_allowed(&self) -> bool {
        !self.blocked && self.pending.is_empty()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GuardedDecodeFailure {
    Catalog(DdlFailure),
    Decoder(DecodeFailure),
}

/// Owns the decoder so no row or feedback API is reachable without full catalog admission.
pub struct GuardedDecoder {
    decoder: Decoder,
    catalog: RelationValidationGate,
    blocked: bool,
}
impl GuardedDecoder {
    pub fn new(limits: WireLimits) -> Self {
        Self {
            decoder: Decoder::new(limits),
            catalog: RelationValidationGate::default(),
            blocked: false,
        }
    }
    pub fn decode_copy_data(
        &mut self,
        frame: &[u8],
    ) -> Result<CopyBothEvent, GuardedDecodeFailure> {
        if self.blocked {
            return Err(GuardedDecodeFailure::Catalog(DdlFailure::contract(
                "GUARDED_DECODER_BLOCKED",
            )));
        }
        let event = self
            .decoder
            .decode_copy_data(frame)
            .map_err(GuardedDecodeFailure::Decoder)?;
        if let CopyBothEvent::XLogData {
            event: pgoutput, ..
        } = &event
        {
            if let Err(failure) = self.catalog.observe_decoder_event(pgoutput) {
                self.blocked = true;
                return Err(GuardedDecodeFailure::Catalog(failure));
            }
        }
        Ok(event)
    }
    pub fn admit_relation(
        &mut self,
        wire: WireRelationContract,
        catalog: &RelationContract,
        expected: Option<&RelationContract>,
        active_generation: bool,
    ) -> Result<ContractDecision, GuardedDecodeFailure> {
        if self.blocked {
            return Err(GuardedDecodeFailure::Catalog(DdlFailure::contract(
                "GUARDED_DECODER_BLOCKED",
            )));
        }
        let id = wire.relation.id;
        let decision = self
            .catalog
            .admit_catalog(id, catalog, expected, active_generation)
            .map_err(|failure| {
                self.blocked = true;
                GuardedDecodeFailure::Catalog(failure)
            })?;
        self.decoder.admit_relation(wire).map_err(|failure| {
            self.blocked = true;
            GuardedDecodeFailure::Decoder(failure)
        })?;
        Ok(decision)
    }
    pub fn standby_status(
        &self,
        boundary: DurableSourceBoundary,
        unix_time_micros: i64,
        reply_requested: bool,
    ) -> Result<Vec<u8>, GuardedDecodeFailure> {
        if self.blocked || !self.catalog.feedback_allowed() || self.decoder.is_feedback_blocked() {
            return Err(GuardedDecodeFailure::Catalog(DdlFailure::contract(
                "FEEDBACK_BEFORE_FULL_RELATION_VALIDATION",
            )));
        }
        Decoder::standby_status(boundary, unix_time_micros, reply_requested)
            .map_err(GuardedDecodeFailure::Decoder)
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
        let mut g = GuardState::acquire_before_export(
            1,
            7,
            "tables-v1".into(),
            99,
            vec![relation(2).identity, relation(1).identity],
        )
        .unwrap();
        assert_eq!(g.locked[0].relation_oid, 1);
        g.advance(GuardPhase::Exported).unwrap();
        g.advance(GuardPhase::Copying).unwrap();
        g.advance(GuardPhase::AwaitingDurableFence).unwrap();
        assert!(!g.feedback_gate_open());
        let proof = DurableFenceProof::from_journal_commit(
            1,
            7,
            "tables-v1".into(),
            99,
            crate::m1_transition_kernel::synthetic_durable_boundary(
                crate::m1_transition_kernel::ReceivedLsn::from_wire(8),
                crate::m1_transition_kernel::JournalCursor::from_store(9),
            ),
        );
        g.durable_fence_observed(&proof).unwrap();
        assert_eq!(g.phase(), GuardPhase::Released);
    }
    // SCENARIO: SCN-IMPORTER-DDL-GUARD-LOSS
    #[test]
    fn guard_loss_invalidates_and_releases_feedback_gate() {
        let mut g = GuardState::acquire_before_export(
            1,
            1,
            "tables-v1".into(),
            99,
            vec![relation(1).identity],
        )
        .unwrap();
        let e = g.guard_session_lost();
        assert_eq!(e.fingerprint, "DDL_GUARD_SESSION_LOST");
        assert_eq!(g.phase(), GuardPhase::Invalid);
        assert!(g.feedback_gate_open());
        assert!(g.locked_relations().is_empty());
    }
    // SCENARIO: SCN-M1-DDL-WAITER-BOUND
    #[test]
    fn conflicting_waiter_at_bound_invalidates_and_releases() {
        let id = relation(1).identity;
        let mut g =
            GuardState::acquire_before_export(1, 1, "tables-v1".into(), 99, vec![id.clone()])
                .unwrap();
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
        assert!(g.locked_relations().is_empty());
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
            GuardState::acquire_before_export(1, 1, "tables-v1".into(), 99, vec![id.clone(), id])
                .unwrap_err()
                .fingerprint,
            "DDL_GUARD_DUPLICATE_RELATION"
        );
    }
    // SCENARIO: SCN-M1-DDL-FULL-FINGERPRINT
    #[test]
    fn every_relation_contract_dimension_changes_the_fingerprint() {
        let old = relation(1);
        let base = old.fingerprint().unwrap();
        let mut changed = Vec::new();
        let mut x = old.clone();
        x.namespace = "other".into();
        changed.push(x);
        let mut x = old.clone();
        x.relation_name = "other".into();
        changed.push(x);
        let mut x = old.clone();
        x.columns[0].type_oid = 23;
        changed.push(x);
        let mut x = old.clone();
        x.columns[0].typmod = 8;
        changed.push(x);
        let mut x = old.clone();
        x.columns[0].collation_oid = 101;
        changed.push(x);
        let mut x = old.clone();
        x.columns[0].nullable = true;
        changed.push(x);
        let mut x = old.clone();
        x.columns[0].generated_expression_hash = Some("generated".into());
        changed.push(x);
        let mut x = old.clone();
        x.columns[0].identity_expression_hash = Some("identity".into());
        changed.push(x);
        let mut x = old.clone();
        x.key.replica_identity_mode = "full".into();
        changed.push(x);
        let mut x = old.clone();
        x.partition.routing = "root".into();
        changed.push(x);
        let mut x = old.clone();
        x.publication_member = false;
        changed.push(x);
        let mut x = old.clone();
        x.publication_attnums.clear();
        changed.push(x);
        assert!(changed.iter().all(|x| x.fingerprint().unwrap() != base
            && matches!(
                old.compare(x, false),
                ContractDecision::BlockAndRequireReseed(_)
            )));
    }
    // SCENARIO: SCN-M1-DDL-KEY-DELETE-SAFETY
    #[test]
    fn changed_type_or_removed_replica_identity_blocks_update_delete_safety() {
        let old = relation(1);
        let mut typed = old.clone();
        typed.columns[0].type_oid = 25;
        let mut no_key = old.clone();
        no_key.key.primary_attnums.clear();
        no_key.key.replica_identity_mode = "nothing".into();
        assert!(matches!(
            old.compare(&typed, false),
            ContractDecision::BlockAndRequireReseed(_)
        ));
        assert!(matches!(
            old.compare(&no_key, false),
            ContractDecision::BlockAndRequireReseed(_)
        ));
    }
    // SCENARIO: SCN-M1-DDL-STALE-FENCE
    #[test]
    fn stale_or_nonmatching_durable_fence_cannot_release_newer_guard() {
        let boundary = crate::m1_transition_kernel::synthetic_durable_boundary(
            crate::m1_transition_kernel::ReceivedLsn::from_wire(8),
            crate::m1_transition_kernel::JournalCursor::from_store(9),
        );
        let stale = DurableFenceProof::from_journal_commit(1, 6, "tables-v1".into(), 99, boundary);
        let mut g = GuardState::acquire_before_export(
            1,
            7,
            "tables-v1".into(),
            99,
            vec![relation(1).identity],
        )
        .unwrap();
        g.advance(GuardPhase::Exported).unwrap();
        g.advance(GuardPhase::Copying).unwrap();
        g.advance(GuardPhase::AwaitingDurableFence).unwrap();
        assert_eq!(
            g.durable_fence_observed(&stale).unwrap_err().fingerprint,
            "DDL_GUARD_FENCE_MISMATCH"
        );
        assert!(!g.feedback_gate_open());
        assert!(!g.locked_relations().is_empty());
    }
    // SCENARIO: SCN-M1-DDL-VALIDATION-ERROR
    #[test]
    fn malformed_catalog_validation_and_actual_decoder_row_remain_fail_closed() {
        let mut gate = RelationValidationGate::default();
        let mut malformed = relation(1);
        malformed.columns.push(malformed.columns[0].clone());
        gate.relation_message(1, true);
        assert!(gate.admit_catalog(1, &malformed, None, false).is_err());
        assert!(!gate.feedback_allowed());
        assert!(gate.require_dml(1).is_err());
        let decoder_relation = crate::m1_decoder::Relation {
            id: 2,
            namespace: "public".into(),
            name: "t2".into(),
            replica_identity: b'd',
            columns: vec![],
        };
        gate.observe_decoder_event(&PgoutputEvent::RelationNeedsValidation(decoder_relation))
            .unwrap();
        let row = crate::m1_decoder::RowChange {
            xid: 1,
            ordinal: 0,
            relation_id: 2,
            kind: RowKind::Delete,
            old_kind: None,
            old: None,
            new: None,
        };
        assert!(
            gate.observe_decoder_event(&PgoutputEvent::Row(row))
                .is_err()
        );
        assert!(!gate.feedback_allowed());
    }

    // SCENARIO: SCN-M1-DDL-ADMISSION-COMPATIBILITY
    #[test]
    fn selected_types_keys_delete_and_destination_compatibility_fail_independently() {
        let base = relation(1);
        let capture = BTreeSet::from([20]);
        let destination = BTreeSet::from([20]);
        assert_eq!(
            validate_relation_admission(&base, &capture, &destination, true),
            AdmissionDecision::Admit
        );
        assert_eq!(
            validate_relation_admission(&base, &BTreeSet::new(), &destination, true),
            AdmissionDecision::BlockCapture("SOURCE_TYPE_UNSUPPORTED")
        );
        let mut unique = base.clone();
        unique.key.primary_attnums.clear();
        unique.key.replica_identity_mode = "index".into();
        unique.key.replica_identity_index = Some("logical_key".into());
        unique
            .key
            .unique_indexes
            .insert("logical_key".into(), vec![1]);
        assert_eq!(
            validate_relation_admission(&unique, &capture, &destination, true),
            AdmissionDecision::Admit
        );
        unique.columns[0].nullable = true;
        assert_eq!(
            validate_relation_admission(&unique, &capture, &destination, true),
            AdmissionDecision::BlockCapture("REPLICA_IDENTITY_INCOMPLETE")
        );
        assert_eq!(
            validate_relation_admission(&base, &capture, &destination, false),
            AdmissionDecision::BlockDestination("DESTINATION_DELETE_UNSUPPORTED")
        );
        assert_eq!(
            validate_relation_admission(&base, &capture, &BTreeSet::new(), true),
            AdmissionDecision::BlockDestination("DESTINATION_TYPE_INCOMPATIBLE")
        );
        let mut absent = base.clone();
        absent.publication_member = false;
        assert_eq!(
            validate_relation_admission(&absent, &capture, &destination, true),
            AdmissionDecision::BlockCapture("SELECTED_TABLE_NOT_PUBLISHED")
        );
    }

    // SCENARIO: SCN-M1-DDL-DECODED-RELATION-DML
    #[test]
    fn decoded_wire_relation_must_pass_full_catalog_before_row_and_feedback() {
        fn xlog(payload: Vec<u8>) -> Vec<u8> {
            let mut out = vec![b'w'];
            out.extend(1u64.to_be_bytes());
            out.extend(2u64.to_be_bytes());
            out.extend(3i64.to_be_bytes());
            out.extend(payload);
            out
        }
        let mut rel = vec![b'R'];
        rel.extend(1u32.to_be_bytes());
        rel.extend(b"public\0t1\0");
        rel.push(b'd');
        rel.extend(1u16.to_be_bytes());
        rel.push(1);
        rel.extend(b"id\0");
        rel.extend(20u32.to_be_bytes());
        rel.extend((-1i32).to_be_bytes());
        let mut guarded = GuardedDecoder::new(WireLimits::default());
        assert!(guarded.decode_copy_data(&xlog(rel)).is_ok());
        let boundary = crate::m1_transition_kernel::synthetic_durable_boundary(
            crate::m1_transition_kernel::ReceivedLsn::from_wire(8),
            crate::m1_transition_kernel::JournalCursor::from_store(9),
        );
        assert!(
            guarded
                .standby_status(boundary, 946_684_800_000_000, false)
                .is_err()
        );
        let wire_relation = crate::m1_decoder::Relation {
            id: 1,
            namespace: "public".into(),
            name: "t1".into(),
            replica_identity: b'd',
            columns: vec![crate::m1_decoder::Column {
                key: true,
                name: "id".into(),
                type_oid: 20,
                type_modifier: -1,
            }],
        };
        guarded
            .admit_relation(
                WireRelationContract {
                    relation: wire_relation,
                    key_columns: vec![0],
                    control: None,
                },
                &relation(1),
                None,
                false,
            )
            .unwrap();
        let mut begin = vec![b'B'];
        begin.extend(8u64.to_be_bytes());
        begin.extend(0i64.to_be_bytes());
        begin.extend(1u32.to_be_bytes());
        guarded.decode_copy_data(&xlog(begin)).unwrap();
        let mut insert = vec![b'I'];
        insert.extend(1u32.to_be_bytes());
        insert.push(b'N');
        insert.extend(1u16.to_be_bytes());
        insert.push(b't');
        insert.extend(1u32.to_be_bytes());
        insert.extend(b"1");
        assert!(guarded.decode_copy_data(&xlog(insert)).is_ok());
        assert!(
            guarded
                .standby_status(boundary, 946_684_800_000_000, false)
                .is_ok()
        );
    }

    #[test]
    fn provisional_bounds_and_matrix_are_explicit() {
        assert_eq!(SUPPORTED_POSTGRES_MAJORS, [15, 16, 17]);
        assert_eq!(CATALOG_POLL_INTERVAL_MS, 5000);
        assert_eq!(DDL_WAITER_BOUND_MS, 5000);
    }
}
