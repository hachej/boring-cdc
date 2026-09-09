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
    const fn wire(fingerprint: &'static str) -> Self {
        Self::blocked(fingerprint, "before_feedback")
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

/// Capability emitted only by the journal transaction owner after atomically persisting the
/// control no-op and complete source boundary. There is intentionally no public constructor.
pub struct JournalCommitProof {
    _private: (),
}
#[cfg(test)]
fn committed_for_fixture() -> JournalCommitProof {
    JournalCommitProof { _private: () }
}

/// State changed only by the capture-priority writer. Persistent adapters reconstruct this state
/// from SQLite and can advance it only while presenting a [`JournalCommitProof`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ControlHistorySnapshot {
    pub last_heartbeat_nonce: Option<u64>,
    pub intended_fence_nonces: BTreeSet<u64>,
    pub observed_fence_nonces: BTreeSet<u64>,
}

#[derive(Default)]
pub struct ControlWriterState {
    last_heartbeat_nonce: Option<u64>,
    intended_fence_nonces: BTreeSet<u64>,
    observed_fence_nonces: BTreeSet<u64>,
}
impl ControlWriterState {
    /// Reconstructs writer state from the SQLite transaction owner's durable snapshot.
    pub fn from_persisted(snapshot: ControlHistorySnapshot) -> Result<Self, ProtocolFailure> {
        if snapshot.intended_fence_nonces.contains(&0)
            || !snapshot
                .observed_fence_nonces
                .is_subset(&snapshot.intended_fence_nonces)
        {
            return Err(ProtocolFailure::blocked(
                "CONTROL_HISTORY_INVALID",
                "before_control_dispatch",
            ));
        }
        Ok(Self {
            last_heartbeat_nonce: snapshot.last_heartbeat_nonce,
            intended_fence_nonces: snapshot.intended_fence_nonces,
            observed_fence_nonces: snapshot.observed_fence_nonces,
        })
    }

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
        commit_proof: Option<&JournalCommitProof>,
    ) -> Result<JournalControlNoOp, ProtocolFailure> {
        let durable = commit_proof.is_some();
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

/// Opaque capabilities supplied by the ownership and SQLite intent owners. These types have no
/// public constructors, so raw configuration or network input cannot mint slot authority.
pub struct OwnershipLocks {
    _private: (),
}
pub struct PersistedSlotIntent {
    intent: SlotIntent,
}
pub struct AdministrationSession {
    _private: (),
}
pub struct CaptureBootstrapSession {
    _private: (),
}
impl AdministrationSession {
    /// Consumes the administration session and drops its credential-bearing state.
    pub fn drop_credential(self) -> CaptureBootstrapSession {
        CaptureBootstrapSession { _private: () }
    }
}
#[cfg(test)]
fn fixture_slot_authority(
    intent: SlotIntent,
) -> (OwnershipLocks, PersistedSlotIntent, CaptureBootstrapSession) {
    (
        OwnershipLocks { _private: () },
        PersistedSlotIntent { intent },
        AdministrationSession { _private: () }.drop_credential(),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotMode {
    ExportSnapshot,
    NoExport,
}
pub struct SlotCreationRequest<'a> {
    pub configured_slot: &'a str,
    pub requested_slot: &'a str,
    pub plugin: &'a str,
    pub snapshot_mode: SnapshotMode,
    pub ownership: Option<&'a OwnershipLocks>,
    pub persisted_intent: Option<&'a PersistedSlotIntent>,
    pub capture_session: Option<&'a CaptureBootstrapSession>,
}
pub fn authorize_slot_creation(
    request: &SlotCreationRequest<'_>,
) -> Result<SlotIntent, ProtocolFailure> {
    if request.capture_session.is_none() {
        return Err(ProtocolFailure::blocked(
            "ADMIN_CREDENTIAL_PRESENT",
            "before_exporter_session",
        ));
    }
    if request.ownership.is_none() {
        return Err(ProtocolFailure::blocked(
            "OWNERSHIP_LOCK_MISSING",
            "before_slot_creation",
        ));
    }
    let intent = request
        .persisted_intent
        .ok_or_else(|| ProtocolFailure::blocked("SLOT_INTENT_MISSING", "before_slot_creation"))?;
    if request.requested_slot != request.configured_slot {
        return Err(ProtocolFailure::blocked(
            "SLOT_NAME_MISMATCH",
            "before_slot_creation",
        ));
    }
    if request.plugin != "pgoutput" || request.snapshot_mode != SnapshotMode::ExportSnapshot {
        return Err(ProtocolFailure::blocked(
            "SLOT_PROTOCOL_MISMATCH",
            "before_slot_creation",
        ));
    }
    Ok(intent.intent)
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeartbeatDegraded {
    pub condition: &'static str,
    pub failure_fingerprint: &'static str,
    pub wal_headroom_bytes: u64,
    pub feedback_advanced: bool,
}
pub fn heartbeat_outage(wal_headroom_bytes: u64) -> HeartbeatDegraded {
    HeartbeatDegraded {
        condition: "heartbeat_degraded",
        failure_fingerprint: "HEARTBEAT_WRITE_UNAVAILABLE",
        wal_headroom_bytes,
        feedback_advanced: false,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PgoutputRelation {
    namespace: String,
    name: String,
    columns: Vec<(String, u32)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PgoutputTupleValue {
    Null,
    UnchangedToast,
    Text(Vec<u8>),
    Binary(Vec<u8>),
}

struct WireCursor<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> WireCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }
    fn take(&mut self, count: usize) -> Result<&'a [u8], ProtocolFailure> {
        let end = self
            .at
            .checked_add(count)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| ProtocolFailure::wire("PGOUTPUT_FRAME_INVALID"))?;
        let value = &self.bytes[self.at..end];
        self.at = end;
        Ok(value)
    }
    fn u8(&mut self) -> Result<u8, ProtocolFailure> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, ProtocolFailure> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, ProtocolFailure> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn cstr(&mut self) -> Result<String, ProtocolFailure> {
        let tail = &self.bytes[self.at..];
        let length = tail
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(|| ProtocolFailure::wire("PGOUTPUT_FRAME_INVALID"))?;
        let value = std::str::from_utf8(self.take(length)?)
            .map_err(|_| ProtocolFailure::wire("PGOUTPUT_TEXT_INVALID"))?
            .to_owned();
        self.take(1)?;
        Ok(value)
    }
    fn finish(self) -> Result<(), ProtocolFailure> {
        if self.at == self.bytes.len() {
            Ok(())
        } else {
            Err(ProtocolFailure::wire("PGOUTPUT_FRAME_INVALID"))
        }
    }
}

fn decode_relation(bytes: &[u8]) -> Result<(u32, PgoutputRelation), ProtocolFailure> {
    let mut cursor = WireCursor::new(bytes);
    if cursor.u8()? != b'R' {
        return Err(ProtocolFailure::wire("PGOUTPUT_MESSAGE_TYPE_INVALID"));
    }
    let relation_id = cursor.u32()?;
    let namespace = cursor.cstr()?;
    let name = cursor.cstr()?;
    cursor.u8()?;
    let count = usize::from(cursor.u16()?);
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        cursor.u8()?;
        columns.push((cursor.cstr()?, cursor.u32()?));
        cursor.u32()?;
    }
    cursor.finish()?;
    Ok((
        relation_id,
        PgoutputRelation {
            namespace,
            name,
            columns,
        },
    ))
}

fn decode_tuple(cursor: &mut WireCursor<'_>) -> Result<Vec<PgoutputTupleValue>, ProtocolFailure> {
    let count = usize::from(cursor.u16()?);
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(match cursor.u8()? {
            b'n' => PgoutputTupleValue::Null,
            b'u' => PgoutputTupleValue::UnchangedToast,
            kind @ (b't' | b'b') => {
                let length = cursor.u32()? as usize;
                let value = cursor.take(length)?.to_vec();
                if kind == b't' {
                    PgoutputTupleValue::Text(value)
                } else {
                    PgoutputTupleValue::Binary(value)
                }
            }
            _ => return Err(ProtocolFailure::wire("PGOUTPUT_TUPLE_KIND_INVALID")),
        });
    }
    Ok(values)
}

fn tuple_text(values: &[PgoutputTupleValue], index: usize) -> Result<&str, ProtocolFailure> {
    match values.get(index) {
        Some(PgoutputTupleValue::Text(value)) => {
            std::str::from_utf8(value).map_err(|_| ProtocolFailure::wire("PGOUTPUT_TEXT_INVALID"))
        }
        _ => Err(ProtocolFailure::wire("CONTROL_TUPLE_SHAPE_INVALID")),
    }
}

fn expected_relation(kind: ControlKind) -> (&'static str, &'static [(&'static str, u32)]) {
    match kind {
        ControlKind::Heartbeat => (
            HEARTBEAT_RELATION,
            &[("id", 25), ("nonce", 20), ("updated_at", 1184)],
        ),
        ControlKind::CaptureFence => (
            FENCE_RELATION,
            &[
                ("id", 25),
                ("capture_epoch", 20),
                ("generation", 20),
                ("table_set_fingerprint", 25),
                ("unique_nonce", 20),
            ],
        ),
    }
}

/// Decodes live pgoutput Relation/Update frames into the typed control-kernel observation.
/// Exactly one matching update is admitted; zero or multiple frames fail before feedback.
pub fn decode_control_update(
    messages: &[Vec<u8>],
    expected_kind: ControlKind,
    expected_nonce: u64,
) -> Result<ObservedControlUpdate, ProtocolFailure> {
    let (expected_name, expected_columns) = expected_relation(expected_kind);
    let mut relations = std::collections::BTreeMap::new();
    let mut updates = Vec::new();
    for message in messages {
        match message.first().copied() {
            Some(b'R') => {
                let (id, relation) = decode_relation(message)?;
                relations.insert(id, relation);
            }
            Some(b'U') => {
                let mut cursor = WireCursor::new(message);
                cursor.u8()?;
                let relation_id = cursor.u32()?;
                let relation = relations
                    .get(&relation_id)
                    .ok_or_else(|| ProtocolFailure::wire("CONTROL_RELATION_UNKNOWN"))?;
                let qualified = format!("{}.{}", relation.namespace, relation.name);
                if qualified != expected_name
                    || relation.columns.len() != expected_columns.len()
                    || !relation
                        .columns
                        .iter()
                        .zip(expected_columns)
                        .all(|((name, oid), expected)| name == expected.0 && oid == &expected.1)
                {
                    return Err(ProtocolFailure::wire("CONTROL_RELATION_SHAPE_INVALID"));
                }
                let marker = cursor.u8()?;
                let old = match marker {
                    b'K' | b'O' => {
                        let old = decode_tuple(&mut cursor)?;
                        if cursor.u8()? != b'N' {
                            return Err(ProtocolFailure::wire("CONTROL_TUPLE_SHAPE_INVALID"));
                        }
                        old
                    }
                    b'N' => Vec::new(),
                    _ => return Err(ProtocolFailure::wire("CONTROL_TUPLE_SHAPE_INVALID")),
                };
                let new = decode_tuple(&mut cursor)?;
                cursor.finish()?;
                if new.len() != expected_columns.len() {
                    return Err(ProtocolFailure::wire("CONTROL_TUPLE_SHAPE_INVALID"));
                }
                let new_key = tuple_text(&new, 0)?.to_owned();
                let old_key = if old.is_empty() {
                    new_key.clone()
                } else {
                    tuple_text(&old, 0)?.to_owned()
                };
                let nonce_index = match expected_kind {
                    ControlKind::Heartbeat => 1,
                    ControlKind::CaptureFence => 4,
                };
                let nonce = tuple_text(&new, nonce_index)?
                    .parse::<u64>()
                    .map_err(|_| ProtocolFailure::wire("CONTROL_NONCE_INVALID"))?;
                if nonce != expected_nonce {
                    return Err(ProtocolFailure::wire("CONTROL_NONCE_MISMATCH"));
                }
                updates.push(ObservedControlUpdate {
                    kind: expected_kind,
                    operation: RowOperation::Update,
                    old_key,
                    new_key,
                    changed_columns: expected_columns[1..]
                        .iter()
                        .map(|(name, _)| (*name).to_owned())
                        .collect(),
                    affected_rows: 1,
                    nonce,
                });
            }
            Some(b'T') => return Err(ProtocolFailure::wire("TRUNCATE_REQUIRES_RESEED")),
            Some(_) => {}
            None => return Err(ProtocolFailure::wire("PGOUTPUT_FRAME_INVALID")),
        }
    }
    if updates.len() != 1 {
        return Err(ProtocolFailure::wire("CONTROL_CARDINALITY_INVALID"));
    }
    Ok(updates.pop().unwrap())
}

/// Decodes a live pgoutput Truncate and blocks it before feedback. Relation frames from the same
/// observation batch are required so relation identity cannot be reconstructed by the fixture.
pub fn decode_truncate(messages: &[Vec<u8>]) -> Result<(), ProtocolFailure> {
    let mut relations = std::collections::BTreeMap::new();
    for message in messages {
        match message.first().copied() {
            Some(b'R') => {
                let (id, relation) = decode_relation(message)?;
                relations.insert(id, relation);
            }
            Some(b'T') => {
                let mut cursor = WireCursor::new(message);
                cursor.u8()?;
                let count = cursor.u32()? as usize;
                cursor.u8()?;
                if count == 0 {
                    return Err(ProtocolFailure::wire("PGOUTPUT_TRUNCATE_INVALID"));
                }
                for _ in 0..count {
                    let id = cursor.u32()?;
                    let relation = relations
                        .get(&id)
                        .ok_or_else(|| ProtocolFailure::wire("CONTROL_RELATION_UNKNOWN"))?;
                    if relation.namespace == "boring_cdc_control" {
                        return Err(ProtocolFailure::wire("CONTROL_RELATION_SHAPE_INVALID"));
                    }
                }
                cursor.finish()?;
                return observe_truncate(PublicationOperation::Truncate);
            }
            Some(_) => {}
            None => return Err(ProtocolFailure::wire("PGOUTPUT_FRAME_INVALID")),
        }
    }
    Err(ProtocolFailure::wire("PGOUTPUT_TRUNCATE_MISSING"))
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
    fn slot<'a>(
        ownership: &'a OwnershipLocks,
        intent: &'a PersistedSlotIntent,
        session: &'a CaptureBootstrapSession,
    ) -> SlotCreationRequest<'a> {
        SlotCreationRequest {
            configured_slot: "boring_cdc",
            requested_slot: "boring_cdc",
            plugin: "pgoutput",
            snapshot_mode: SnapshotMode::ExportSnapshot,
            ownership: Some(ownership),
            persisted_intent: Some(intent),
            capture_session: Some(session),
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
            .observe(
                &ObservedControlUpdate::heartbeat(1),
                Some(&committed_for_fixture()),
            )
            .unwrap();
        assert!(e.feedback_eligible);
        assert!(!e.writes_user_row && !e.writes_benchmark_mutation);
        assert_eq!(
            s.observe(
                &ObservedControlUpdate::heartbeat(1),
                Some(&committed_for_fixture())
            )
            .unwrap_err()
            .fingerprint,
            "HEARTBEAT_NOT_MONOTONIC"
        );
    }
    #[test]
    fn heartbeat_cannot_feedback_before_durable_commit() {
        let mut s = ControlWriterState::default();
        assert!(
            !s.observe(&ObservedControlUpdate::heartbeat(1), None)
                .unwrap()
                .feedback_eligible
        );
        assert!(
            s.observe(
                &ObservedControlUpdate::heartbeat(1),
                Some(&committed_for_fixture())
            )
            .unwrap()
            .feedback_eligible
        );
    }
    #[test]
    fn repeated_fence_keeps_one_proof() {
        let mut s = ControlWriterState::default();
        s.intend_fence(7).unwrap();
        s.observe(
            &ObservedControlUpdate::fence(7),
            Some(&committed_for_fixture()),
        )
        .unwrap();
        s.observe(
            &ObservedControlUpdate::fence(7),
            Some(&committed_for_fixture()),
        )
        .unwrap();
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
            s.observe(
                &ObservedControlUpdate::fence(8),
                Some(&committed_for_fixture())
            )
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
                    .observe(&u, Some(&committed_for_fixture()))
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
                .observe(&u, Some(&committed_for_fixture()))
                .unwrap_err()
                .fingerprint,
            "CONTROL_OPERATION_FORBIDDEN"
        );
        u.operation = RowOperation::Update;
        u.new_key = "other".into();
        assert_eq!(
            ControlWriterState::default()
                .observe(&u, Some(&committed_for_fixture()))
                .unwrap_err()
                .fingerprint,
            "CONTROL_KEY_CHANGED"
        );
        u.new_key = CONTROL_KEY.into();
        u.changed_columns.insert("secret".into());
        assert_eq!(
            ControlWriterState::default()
                .observe(&u, Some(&committed_for_fixture()))
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
        let (locks, intent, session) = fixture_slot_authority(SlotIntent::Bootstrap);
        assert_eq!(
            authorize_slot_creation(&slot(&locks, &intent, &session)).unwrap(),
            SlotIntent::Bootstrap
        );
        let mut x = slot(&locks, &intent, &session);
        x.requested_slot = "other";
        assert_eq!(
            authorize_slot_creation(&x).unwrap_err().fingerprint,
            "SLOT_NAME_MISMATCH"
        );
        let mut x = slot(&locks, &intent, &session);
        x.persisted_intent = None;
        assert_eq!(
            authorize_slot_creation(&x).unwrap_err().fingerprint,
            "SLOT_INTENT_MISSING"
        );
        let mut x = slot(&locks, &intent, &session);
        x.ownership = None;
        assert_eq!(
            authorize_slot_creation(&x).unwrap_err().fingerprint,
            "OWNERSHIP_LOCK_MISSING"
        );
    }
    #[test]
    fn administration_credential_is_gone_before_exporter() {
        let (locks, intent, session) = fixture_slot_authority(SlotIntent::Bootstrap);
        let mut x = slot(&locks, &intent, &session);
        x.capture_session = None;
        assert_eq!(
            authorize_slot_creation(&x).unwrap_err().fingerprint,
            "ADMIN_CREDENTIAL_PRESENT"
        );
    }
    #[test]
    fn persisted_control_history_survives_restart() {
        let snapshot = ControlHistorySnapshot {
            last_heartbeat_nonce: Some(9),
            intended_fence_nonces: [7].into_iter().collect(),
            observed_fence_nonces: [7].into_iter().collect(),
        };
        let mut restored = ControlWriterState::from_persisted(snapshot).unwrap();
        assert_eq!(restored.fence_proof_count(), 1);
        assert_eq!(
            restored.intend_fence(7).unwrap_err().fingerprint,
            "FENCE_NONCE_NOT_UNIQUE"
        );
        let proof = committed_for_fixture();
        assert_eq!(
            restored
                .observe(&ObservedControlUpdate::heartbeat(9), Some(&proof))
                .unwrap_err()
                .fingerprint,
            "HEARTBEAT_NOT_MONOTONIC"
        );
    }
    #[test]
    fn heartbeat_outage_degrades_without_feedback() {
        let degraded = heartbeat_outage(1_048_576);
        assert_eq!(degraded.condition, "heartbeat_degraded");
        assert_eq!(degraded.wal_headroom_bytes, 1_048_576);
        assert!(!degraded.feedback_advanced);
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
