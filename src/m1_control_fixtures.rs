//! Publication, control-row, and slot-creation protocol boundary.
//!
//! The kernel is deliberately side-effect free. SQL/pgoutput adapters must feed observed facts
//! through this one writer before feedback or an external mutation is allowed.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

pub const HEARTBEAT_RELATION: &str = "boring_cdc_control.heartbeat";
pub const FENCE_RELATION: &str = "boring_cdc_control.capture_fences";
pub const CONTROL_KEY: &str = "singleton";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum PublicationOperation {
    Insert,
    Update,
    Delete,
    Truncate,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublicationSpec {
    pub name: String,
    pub owner_role: String,
    pub relations: BTreeSet<String>,
    pub operations: BTreeSet<String>,
}

impl PublicationSpec {
    pub fn new(
        name: &str,
        owner_role: &str,
        user_relations: impl IntoIterator<Item = String>,
    ) -> Self {
        let mut relations: BTreeSet<_> = user_relations.into_iter().collect();
        relations.insert(HEARTBEAT_RELATION.into());
        relations.insert(FENCE_RELATION.into());
        Self {
            name: name.into(),
            owner_role: owner_role.into(),
            relations,
            operations: ["delete", "insert", "truncate", "update"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
        }
    }

    /// Exact, order-independent publication contract fingerprint.
    pub fn fingerprint(&self) -> String {
        let canonical =
            serde_json::to_vec(self).expect("serializing a typed publication spec cannot fail");
        format!("{:x}", Sha256::digest(canonical))
    }

    pub fn verify(&self, observed: &Self) -> Result<(), ProtocolFailure> {
        if self == observed {
            Ok(())
        } else {
            Err(ProtocolFailure::publication_drift())
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ControlKind {
    Heartbeat,
    CaptureFence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedControlUpdate {
    pub kind: ControlKind,
    pub operation: RowOperation,
    pub old_key: String,
    pub new_key: String,
    pub changed_columns: BTreeSet<String>,
    pub affected_rows: u64,
    pub nonce: u64,
}

impl ObservedControlUpdate {
    pub fn heartbeat(nonce: u64) -> Self {
        Self {
            kind: ControlKind::Heartbeat,
            operation: RowOperation::Update,
            old_key: CONTROL_KEY.into(),
            new_key: CONTROL_KEY.into(),
            changed_columns: ["nonce", "updated_at"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            affected_rows: 1,
            nonce,
        }
    }
    pub fn fence(nonce: u64) -> Self {
        Self {
            kind: ControlKind::CaptureFence,
            operation: RowOperation::Update,
            old_key: CONTROL_KEY.into(),
            new_key: CONTROL_KEY.into(),
            changed_columns: [
                "capture_epoch",
                "generation",
                "table_set_fingerprint",
                "unique_nonce",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            affected_rows: 1,
            nonce,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProtocolFailure {
    pub class: &'static str,
    pub fingerprint: &'static str,
    pub failed_boundary: &'static str,
    pub allowed_actions: &'static [&'static str],
}
impl ProtocolFailure {
    const fn blocked(fingerprint: &'static str, boundary: &'static str) -> Self {
        Self {
            class: "deterministic",
            fingerprint,
            failed_boundary: boundary,
            allowed_actions: &["status", "recover reseed"],
        }
    }
    const fn publication_drift() -> Self {
        Self::blocked("PUBLICATION_DRIFT", "before_stream_or_feedback")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalControlNoOp {
    pub kind: ControlKind,
    pub nonce: u64,
    pub writes_user_row: bool,
    pub writes_benchmark_mutation: bool,
    pub feedback_eligible: bool,
}

/// State changed only by the capture-priority writer. Adapters persist the returned no-op in the
/// same journal transaction as the complete source boundary before setting `durable=true`.
#[derive(Default)]
pub struct ControlWriterState {
    last_heartbeat_nonce: Option<u64>,
    intended_fence_nonces: BTreeSet<u64>,
    observed_fence_nonces: BTreeSet<u64>,
}
impl ControlWriterState {
    pub fn intend_fence(&mut self, nonce: u64) -> Result<(), ProtocolFailure> {
        if nonce == 0 || !self.intended_fence_nonces.insert(nonce) {
            return Err(ProtocolFailure::blocked(
                "FENCE_NONCE_NOT_UNIQUE",
                "before_control_dispatch",
            ));
        }
        Ok(())
    }

    pub fn observe(
        &mut self,
        update: &ObservedControlUpdate,
        durable: bool,
    ) -> Result<JournalControlNoOp, ProtocolFailure> {
        let expected_columns: BTreeSet<String> = match update.kind {
            ControlKind::Heartbeat => ["nonce", "updated_at"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            ControlKind::CaptureFence => [
                "capture_epoch",
                "generation",
                "table_set_fingerprint",
                "unique_nonce",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        };
        if update.operation != RowOperation::Update {
            return Err(ProtocolFailure::blocked(
                "CONTROL_OPERATION_FORBIDDEN",
                "before_feedback",
            ));
        }
        if update.affected_rows != 1 {
            return Err(ProtocolFailure::blocked(
                "CONTROL_CARDINALITY_INVALID",
                "before_feedback",
            ));
        }
        if update.old_key != CONTROL_KEY || update.new_key != CONTROL_KEY {
            return Err(ProtocolFailure::blocked(
                "CONTROL_KEY_CHANGED",
                "before_feedback",
            ));
        }
        if update.changed_columns != expected_columns {
            return Err(ProtocolFailure::blocked(
                "CONTROL_COLUMNS_INVALID",
                "before_feedback",
            ));
        }
        match update.kind {
            ControlKind::Heartbeat => {
                if self
                    .last_heartbeat_nonce
                    .is_some_and(|last| update.nonce <= last)
                {
                    return Err(ProtocolFailure::blocked(
                        "HEARTBEAT_NOT_MONOTONIC",
                        "before_feedback",
                    ));
                }
                if durable {
                    self.last_heartbeat_nonce = Some(update.nonce);
                }
            }
            ControlKind::CaptureFence => {
                if !self.intended_fence_nonces.contains(&update.nonce) {
                    return Err(ProtocolFailure::blocked(
                        "FENCE_NONCE_UNBOUND",
                        "before_feedback",
                    ));
                }
                // A retry remains an audit event but cannot create a second proof.
                if durable {
                    self.observed_fence_nonces.insert(update.nonce);
                }
            }
        }
        Ok(JournalControlNoOp {
            kind: update.kind,
            nonce: update.nonce,
            writes_user_row: false,
            writes_benchmark_mutation: false,
            feedback_eligible: durable,
        })
    }

    pub fn fence_proof_count(&self) -> usize {
        self.observed_fence_nonces.len()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotIntent {
    Bootstrap,
    FullReseed,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SlotCreationRequest<'a> {
    pub configured_slot: &'a str,
    pub requested_slot: &'a str,
    pub plugin: &'a str,
    pub export_snapshot: bool,
    pub state_lock_held: bool,
    pub source_lock_held: bool,
    pub persisted_intent: Option<SlotIntent>,
    pub administration_credential_loaded: bool,
}
pub fn authorize_slot_creation(request: &SlotCreationRequest<'_>) -> Result<(), ProtocolFailure> {
    if request.administration_credential_loaded {
        return Err(ProtocolFailure::blocked(
            "ADMIN_CREDENTIAL_PRESENT",
            "before_exporter_session",
        ));
    }
    if !request.state_lock_held || !request.source_lock_held {
        return Err(ProtocolFailure::blocked(
            "OWNERSHIP_LOCK_MISSING",
            "before_slot_creation",
        ));
    }
    if request.persisted_intent.is_none() {
        return Err(ProtocolFailure::blocked(
            "SLOT_INTENT_MISSING",
            "before_slot_creation",
        ));
    }
    if request.requested_slot != request.configured_slot {
        return Err(ProtocolFailure::blocked(
            "SLOT_NAME_MISMATCH",
            "before_slot_creation",
        ));
    }
    if request.plugin != "pgoutput" || !request.export_snapshot {
        return Err(ProtocolFailure::blocked(
            "SLOT_PROTOCOL_MISMATCH",
            "before_slot_creation",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableSetChange {
    Unchanged,
    AddOrRemove,
}
pub fn authorize_live_publication_change(change: TableSetChange) -> Result<(), ProtocolFailure> {
    match change {
        TableSetChange::Unchanged => Ok(()),
        TableSetChange::AddOrRemove => Err(ProtocolFailure::blocked(
            "FULL_RESEED_REQUIRED",
            "before_publication_change",
        )),
    }
}
pub fn observe_truncate(operation: PublicationOperation) -> Result<(), ProtocolFailure> {
    if operation == PublicationOperation::Truncate {
        Err(ProtocolFailure::blocked(
            "TRUNCATE_REQUIRES_RESEED",
            "before_feedback",
        ))
    } else {
        Ok(())
    }
}

pub fn verify_source_identity(
    expected: (&str, u32, &str),
    observed: (&str, u32, &str),
) -> Result<(), ProtocolFailure> {
    if expected == observed {
        Ok(())
    } else {
        Err(ProtocolFailure::blocked(
            "SOURCE_PUBLICATION_SLOT_MISMATCH",
            "before_streaming",
        ))
    }
}

/// Normative grant matrix. `REPLICATION` cannot be slot-scoped, so the connector guard above is
/// the operation/name boundary and deliberately exposes no slot-drop function.
pub const ROLE_GRANTS: &[(&str, &[&str], &[&str])] = &[
    (
        "application",
        &["selected_table_dml"],
        &["publication_admin", "control_dml", "replication"],
    ),
    (
        "capture_bootstrap",
        &["replication", "configured_slot_create_export_snapshot"],
        &[
            "publication_alter",
            "publication_drop",
            "slot_drop",
            "control_dml",
        ],
    ),
    (
        "control_writer",
        &[
            "update_control_value_columns",
            "select_control_immutable_key",
        ],
        &[
            "insert",
            "delete",
            "key_update",
            "excess_column_access",
            "publication_admin",
            "slot_admin",
        ],
    ),
    (
        "administration",
        &[
            "configured_old_slot_drop_during_confirmed_maintenance",
            "publication_lifecycle",
            "seed_fixed_control_rows",
        ],
        &["ordinary_run"],
    ),
];

#[cfg(test)]
pub mod tests {
    use super::*;
    fn slot<'a>() -> SlotCreationRequest<'a> {
        SlotCreationRequest {
            configured_slot: "boring_cdc",
            requested_slot: "boring_cdc",
            plugin: "pgoutput",
            export_snapshot: true,
            state_lock_held: true,
            source_lock_held: true,
            persisted_intent: Some(SlotIntent::Bootstrap),
            administration_credential_loaded: false,
        }
    }

    #[test]
    fn publication_fingerprint_is_exact_and_order_independent() {
        let a = PublicationSpec::new(
            "boring_cdc_pub",
            "boring_cdc_admin",
            ["public.b".into(), "public.a".into()],
        );
        let b = PublicationSpec::new(
            "boring_cdc_pub",
            "boring_cdc_admin",
            ["public.a".into(), "public.b".into()],
        );
        assert_eq!(a.fingerprint(), b.fingerprint());
        assert!(a.verify(&b).is_ok());
        let mut drift = b;
        drift.relations.remove("public.b");
        assert_eq!(
            a.verify(&drift).unwrap_err().fingerprint,
            "PUBLICATION_DRIFT"
        );
    }
    #[test]
    fn heartbeat_is_monotonic_durable_noop() {
        let mut s = ControlWriterState::default();
        let e = s
            .observe(&ObservedControlUpdate::heartbeat(1), true)
            .unwrap();
        assert!(e.feedback_eligible);
        assert!(!e.writes_user_row && !e.writes_benchmark_mutation);
        assert_eq!(
            s.observe(&ObservedControlUpdate::heartbeat(1), true)
                .unwrap_err()
                .fingerprint,
            "HEARTBEAT_NOT_MONOTONIC"
        );
    }
    #[test]
    fn heartbeat_cannot_feedback_before_durable_commit() {
        let mut s = ControlWriterState::default();
        assert!(
            !s.observe(&ObservedControlUpdate::heartbeat(1), false)
                .unwrap()
                .feedback_eligible
        );
        assert!(
            s.observe(&ObservedControlUpdate::heartbeat(1), true)
                .unwrap()
                .feedback_eligible
        );
    }
    #[test]
    fn repeated_fence_keeps_one_proof() {
        let mut s = ControlWriterState::default();
        s.intend_fence(7).unwrap();
        s.observe(&ObservedControlUpdate::fence(7), true).unwrap();
        s.observe(&ObservedControlUpdate::fence(7), true).unwrap();
        assert_eq!(s.fence_proof_count(), 1);
        assert_eq!(
            s.intend_fence(7).unwrap_err().fingerprint,
            "FENCE_NONCE_NOT_UNIQUE"
        );
    }
    #[test]
    fn unbound_fence_fails_closed() {
        let mut s = ControlWriterState::default();
        assert_eq!(
            s.observe(&ObservedControlUpdate::fence(8), true)
                .unwrap_err()
                .fingerprint,
            "FENCE_NONCE_UNBOUND"
        );
    }
    #[test]
    fn zero_or_multiple_control_rows_block() {
        for n in [0, 2] {
            let mut u = ObservedControlUpdate::heartbeat(1);
            u.affected_rows = n;
            assert_eq!(
                ControlWriterState::default()
                    .observe(&u, true)
                    .unwrap_err()
                    .fingerprint,
                "CONTROL_CARDINALITY_INVALID"
            );
        }
    }
    #[test]
    fn forbidden_control_shapes_block() {
        let mut u = ObservedControlUpdate::heartbeat(1);
        u.operation = RowOperation::Insert;
        assert_eq!(
            ControlWriterState::default()
                .observe(&u, true)
                .unwrap_err()
                .fingerprint,
            "CONTROL_OPERATION_FORBIDDEN"
        );
        u.operation = RowOperation::Update;
        u.new_key = "other".into();
        assert_eq!(
            ControlWriterState::default()
                .observe(&u, true)
                .unwrap_err()
                .fingerprint,
            "CONTROL_KEY_CHANGED"
        );
        u.new_key = CONTROL_KEY.into();
        u.changed_columns.insert("secret".into());
        assert_eq!(
            ControlWriterState::default()
                .observe(&u, true)
                .unwrap_err()
                .fingerprint,
            "CONTROL_COLUMNS_INVALID"
        );
    }
    #[test]
    fn truncate_is_detection_only() {
        assert_eq!(
            observe_truncate(PublicationOperation::Truncate)
                .unwrap_err()
                .fingerprint,
            "TRUNCATE_REQUIRES_RESEED"
        );
    }
    #[test]
    fn table_membership_change_has_no_live_path() {
        assert_eq!(
            authorize_live_publication_change(TableSetChange::AddOrRemove)
                .unwrap_err()
                .fingerprint,
            "FULL_RESEED_REQUIRED"
        );
    }
    #[test]
    fn slot_guard_accepts_only_bound_configured_export() {
        assert!(authorize_slot_creation(&slot()).is_ok());
        let mut x = slot();
        x.requested_slot = "other";
        assert_eq!(
            authorize_slot_creation(&x).unwrap_err().fingerprint,
            "SLOT_NAME_MISMATCH"
        );
        let mut x = slot();
        x.persisted_intent = None;
        assert_eq!(
            authorize_slot_creation(&x).unwrap_err().fingerprint,
            "SLOT_INTENT_MISSING"
        );
        let mut x = slot();
        x.source_lock_held = false;
        assert_eq!(
            authorize_slot_creation(&x).unwrap_err().fingerprint,
            "OWNERSHIP_LOCK_MISSING"
        );
    }
    #[test]
    fn administration_credential_is_gone_before_exporter() {
        let mut x = slot();
        x.administration_credential_loaded = true;
        assert_eq!(
            authorize_slot_creation(&x).unwrap_err().fingerprint,
            "ADMIN_CREDENTIAL_PRESENT"
        );
    }
    #[test]
    fn source_timeline_publication_slot_mismatch_blocks_startup() {
        assert_eq!(
            verify_source_identity(("source-a", 1, "pub/slot"), ("source-a", 2, "pub/slot"))
                .unwrap_err()
                .fingerprint,
            "SOURCE_PUBLICATION_SLOT_MISMATCH"
        );
    }
    #[test]
    fn role_matrix_has_no_capture_slot_drop() {
        let capture = ROLE_GRANTS
            .iter()
            .find(|r| r.0 == "capture_bootstrap")
            .unwrap();
        assert!(capture.2.contains(&"slot_drop"));
        assert!(!capture.1.iter().any(|x| x.contains("drop")));
    }
}
