//! Canonical mutation ordering, positional identities, and payload conflict handling.
//!
//! Inputs are already-admitted decoder values and the relation fingerprint owned by
//! `m1_ddl_fixtures`. This module performs no journal or destination I/O.

use crate::m1_source_identity::{
    CanonicalKeyComponent, LogicalTableIdentity, PhysicalKeyHash, RelationSchemaVersion,
    SourceIdentity,
};
use crate::m1_transition_kernel::{
    CaptureEpoch, DestinationGeneration, ReceivedLsn, SourceVersion,
};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::fmt;

// M0-PROVISIONAL: boring-cdc-d-event-id (RECOMMENDED SHA-256 domain/version literals).
const WAL_EVENT_DOMAIN: &[u8] = b"boring-cdc/wal-event/v1";
// M0-PROVISIONAL: boring-cdc-d-event-id (RECOMMENDED source/slot identity domain).
const SOURCE_SLOT_DOMAIN: &[u8] = b"boring-cdc/source-slot/v1";
// M0-PROVISIONAL: boring-cdc-d-event-id (RECOMMENDED SHA-256 domain/version literals).
const SNAPSHOT_EVENT_DOMAIN: &[u8] = b"boring-cdc/snapshot-event/v1";
// M0-PROVISIONAL: boring-cdc-d-event-id (RECOMMENDED SHA-256 domain/version literals).
const VALUE_HASH_DOMAIN: &[u8] = b"boring-cdc/canonical-value/v1";
// M0-PROVISIONAL: boring-cdc-d-event-id (RECOMMENDED SHA-256 domain/version literals).
const PAYLOAD_HASH_DOMAIN: &[u8] = b"boring-cdc/mutation-payload/v1";
// M0-PROVISIONAL: boring-cdc-d-event-id (RECOMMENDED origin rank: snapshot then WAL).
pub const SNAPSHOT_ORIGIN_RANK: u8 = 0;
// M0-PROVISIONAL: boring-cdc-d-event-id (RECOMMENDED origin rank: snapshot then WAL).
pub const WAL_ORIGIN_RANK: u8 = 1;
// M0-PROVISIONAL: boring-cdc-d-event-id (RECOMMENDED column-state tags).
const COLUMN_ABSENT_TAG: u8 = 0;
const COLUMN_NULL_TAG: u8 = 1;
const COLUMN_UNCHANGED_TOAST_TAG: u8 = 2;
const COLUMN_VALUE_TAG: u8 = 3;
// M0-PROVISIONAL: boring-cdc-d-event-id (RECOMMENDED mutation-kind tags).
const MUTATION_DELETE_TAG: u8 = 0;
const MUTATION_UPSERT_TAG: u8 = 1;
// M0-PROVISIONAL: boring-cdc-d-keys (RECOMMENDED maximum canonical key arity).
const MAX_CANONICAL_KEY_COMPONENTS: usize = 32;

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Hash32([u8; 32]);

impl Hash32 {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
    #[must_use]
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
    #[must_use]
    pub fn hex(self) -> String {
        self.0.iter().map(|b| format!("{b:02x}")).collect()
    }
}
impl fmt::Debug for Hash32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash32({})", self.hex())
    }
}

fn hash_fields(domain: &[u8], fields: &[&[u8]]) -> Hash32 {
    let mut h = Sha256::new();
    put_field(&mut h, domain);
    for field in fields {
        put_field(&mut h, field);
    }
    Hash32(h.finalize().into())
}
// M0-PROVISIONAL: boring-cdc-d-event-id (RECOMMENDED canonical field framing: u64 big-endian byte length).
fn canonical_length_bytes(len: usize) -> [u8; 8] {
    (len as u64).to_be_bytes()
}
fn put_field(h: &mut Sha256, bytes: &[u8]) {
    h.update(canonical_length_bytes(bytes.len()));
    h.update(bytes);
}

/// A canonical scalar is the admitted PostgreSQL type identity plus its contract-defined bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalValue {
    pub type_oid: u32,
    pub type_modifier: i32,
    pub bytes: Vec<u8>,
}
impl CanonicalValue {
    #[must_use]
    pub fn hash(&self) -> Hash32 {
        hash_fields(
            VALUE_HASH_DOMAIN,
            &[
                &self.type_oid.to_be_bytes(),
                &self.type_modifier.to_be_bytes(),
                &self.bytes,
            ],
        )
    }
}

/// Four states remain distinct; no sentinel byte sequence can masquerade as another state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColumnState {
    Absent,
    Null,
    UnchangedToast,
    Value(CanonicalValue),
}
impl ColumnState {
    fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            Self::Absent => out.push(COLUMN_ABSENT_TAG),
            Self::Null => out.push(COLUMN_NULL_TAG),
            Self::UnchangedToast => out.push(COLUMN_UNCHANGED_TOAST_TAG),
            Self::Value(value) => {
                out.push(COLUMN_VALUE_TAG);
                out.extend(value.type_oid.to_be_bytes());
                out.extend(value.type_modifier.to_be_bytes());
                out.extend(canonical_length_bytes(value.bytes.len()));
                out.extend(&value.bytes);
            }
        }
    }
}

pub type CanonicalKey = Vec<CanonicalKeyComponent>;

fn validate_key(key: &CanonicalKey) -> Result<(), OrderingFailure> {
    if key.is_empty()
        || key.len() > MAX_CANONICAL_KEY_COMPONENTS
        || key.iter().any(|v| matches!(v, CanonicalKeyComponent::Null))
    {
        return Err(OrderingFailure::contract("CANONICAL_KEY_INVALID"));
    }
    Ok(())
}
fn canonical_key_hash(key: &CanonicalKey) -> Result<PhysicalKeyHash, OrderingFailure> {
    validate_key(key)?;
    Ok(PhysicalKeyHash::derive(key))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceSlotIdentity(Hash32);
impl SourceSlotIdentity {
    #[must_use]
    pub fn derive(source: &SourceIdentity) -> Self {
        Self(hash_fields(
            SOURCE_SLOT_DOMAIN,
            &[
                &source.system_identifier.to_be_bytes(),
                &source.database_identity.to_be_bytes(),
                source.slot_name.as_bytes(),
                source.plugin.as_bytes(),
            ],
        ))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalIdentityInput {
    pub capture_epoch: CaptureEpoch,
    /// Stable fingerprint of source system/database/slot, never a raw identifier.
    pub source_slot_identity: SourceSlotIdentity,
    pub transaction_end_lsn: ReceivedLsn,
    pub row_ordinal: u64,
    pub mutation_ordinal: u8,
}
#[must_use]
pub fn wal_connector_event_id(input: WalIdentityInput) -> Hash32 {
    hash_fields(
        WAL_EVENT_DOMAIN,
        &[
            &input.capture_epoch.get().to_be_bytes(),
            &input.source_slot_identity.0.bytes(),
            &input.transaction_end_lsn.get().to_be_bytes(),
            &input.row_ordinal.to_be_bytes(),
            &[input.mutation_ordinal],
        ],
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotIdentityInput {
    pub capture_epoch: CaptureEpoch,
    pub generation: DestinationGeneration,
    pub logical_table_id: LogicalTableIdentity,
    pub chunk_id: u64,
    pub key_hash: PhysicalKeyHash,
}
#[must_use]
pub fn snapshot_connector_event_id(input: SnapshotIdentityInput) -> Hash32 {
    hash_fields(
        SNAPSHOT_EVENT_DOMAIN,
        &[
            &input.capture_epoch.get().to_be_bytes(),
            &input.generation.get().to_be_bytes(),
            &input.logical_table_id.fingerprint().bytes(),
            &input.chunk_id.to_be_bytes(),
            &input.key_hash.bytes(),
        ],
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationKind {
    Delete,
    Upsert,
}
impl MutationKind {
    const fn tag(self) -> u8 {
        match self {
            Self::Delete => MUTATION_DELETE_TAG,
            Self::Upsert => MUTATION_UPSERT_TAG,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationPayload {
    /// Lowercase SHA-256 emitted by the admitted DDL relation contract.
    pub relation: RelationSchemaVersion,
    pub key: CanonicalKey,
    pub kind: MutationKind,
    pub columns: Vec<ColumnState>,
}
impl MutationPayload {
    pub fn hash(&self, version: MutationVersion) -> Result<Hash32, OrderingFailure> {
        let key_hash = canonical_key_hash(&self.key)?;
        if let Some(binding) = version.snapshot_binding {
            if binding.logical_table_id != self.relation.logical_table
                || binding.key_hash != key_hash
            {
                return Err(OrderingFailure::contract(
                    "SNAPSHOT_PAYLOAD_IDENTITY_MISMATCH",
                ));
            }
        }
        let mut columns = Vec::new();
        columns.extend(canonical_length_bytes(self.columns.len()));
        for state in &self.columns {
            state.encode_into(&mut columns);
        }
        Ok(hash_fields(
            PAYLOAD_HASH_DOMAIN,
            &[
                &version.source.capture_epoch().get().to_be_bytes(),
                &version.source.commit_lsn().get().to_be_bytes(),
                &[version.origin_rank],
                &version.source.transaction_id().to_be_bytes(),
                &version.source.ordinal().to_be_bytes(),
                &[version.mutation_ordinal],
                &version.connector_event_id.bytes(),
                &self.relation.fingerprint().bytes(),
                &key_hash.bytes(),
                &[self.kind.tag()],
                &columns,
            ],
        ))
    }
}

/// Checked bridge from the decoder's u64 row ordinal into the shared M1.1 representation.
pub fn source_version_for_row(
    capture_epoch: CaptureEpoch,
    commit_lsn: ReceivedLsn,
    transaction_id: u32,
    row_ordinal: u64,
) -> Result<SourceVersion, OrderingFailure> {
    let ordinal = u32::try_from(row_ordinal)
        .map_err(|_| OrderingFailure::contract("ROW_ORDINAL_OUT_OF_RANGE"))?;
    Ok(SourceVersion::from_decoded(
        capture_epoch,
        commit_lsn,
        transaction_id,
        ordinal,
    ))
}

/// Total version for expanded mutations. SourceVersion is reused rather than represented again.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SnapshotPayloadBinding {
    logical_table_id: LogicalTableIdentity,
    key_hash: PhysicalKeyHash,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MutationVersion {
    source: SourceVersion,
    origin_rank: u8,
    mutation_ordinal: u8,
    connector_event_id: Hash32,
    snapshot_binding: Option<SnapshotPayloadBinding>,
}
impl MutationVersion {
    pub fn from_wal(
        source: SourceVersion,
        position: WalIdentityInput,
    ) -> Result<Self, OrderingFailure> {
        if position.capture_epoch != source.capture_epoch() {
            return Err(OrderingFailure::contract("CAPTURE_EPOCH_MISMATCH"));
        }
        if position.row_ordinal != u64::from(source.ordinal()) {
            return Err(OrderingFailure::contract("ROW_ORDINAL_MISMATCH"));
        }
        if position.transaction_end_lsn.get() < source.commit_lsn().get() {
            return Err(OrderingFailure::contract("END_LSN_BEFORE_COMMIT_LSN"));
        }
        Ok(Self {
            source,
            origin_rank: WAL_ORIGIN_RANK,
            mutation_ordinal: position.mutation_ordinal,
            connector_event_id: wal_connector_event_id(position),
            snapshot_binding: None,
        })
    }
    pub fn from_snapshot(
        source: SourceVersion,
        position: SnapshotIdentityInput,
    ) -> Result<Self, OrderingFailure> {
        if position.capture_epoch != source.capture_epoch() {
            return Err(OrderingFailure::contract("CAPTURE_EPOCH_MISMATCH"));
        }
        if source.transaction_id() != 0 || source.ordinal() != 0 {
            return Err(OrderingFailure::contract("SNAPSHOT_ORDINAL_INVALID"));
        }
        Ok(Self {
            source,
            origin_rank: SNAPSHOT_ORIGIN_RANK,
            mutation_ordinal: 0,
            connector_event_id: snapshot_connector_event_id(position),
            snapshot_binding: Some(SnapshotPayloadBinding {
                logical_table_id: position.logical_table_id,
                key_hash: position.key_hash,
            }),
        })
    }
    #[must_use]
    pub const fn source(self) -> SourceVersion {
        self.source
    }
    #[must_use]
    pub const fn origin_rank(self) -> u8 {
        self.origin_rank
    }
    #[must_use]
    pub const fn mutation_ordinal(self) -> u8 {
        self.mutation_ordinal
    }
    #[must_use]
    pub const fn connector_event_id(self) -> Hash32 {
        self.connector_event_id
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VersionComparison {
    Less,
    Equal,
    Greater,
    DifferentCaptureEpoch,
}
#[must_use]
pub fn compare_versions(left: MutationVersion, right: MutationVersion) -> VersionComparison {
    if left.source.capture_epoch() != right.source.capture_epoch() {
        return VersionComparison::DifferentCaptureEpoch;
    }
    let tuple = |v: MutationVersion| {
        (
            v.source.commit_lsn().get(),
            v.origin_rank,
            v.source.ordinal(),
            v.mutation_ordinal,
            v.connector_event_id,
        )
    };
    match tuple(left).cmp(&tuple(right)) {
        Ordering::Less => VersionComparison::Less,
        Ordering::Equal => VersionComparison::Equal,
        Ordering::Greater => VersionComparison::Greater,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalMutation {
    connector_event_id: Hash32,
    version: MutationVersion,
    payload: MutationPayload,
    payload_hash: Hash32,
}
impl CanonicalMutation {
    #[must_use]
    pub const fn connector_event_id(&self) -> Hash32 {
        self.connector_event_id
    }
    #[must_use]
    pub const fn version(&self) -> MutationVersion {
        self.version
    }
    #[must_use]
    pub const fn payload(&self) -> &MutationPayload {
        &self.payload
    }
    #[must_use]
    pub const fn payload_hash(&self) -> Hash32 {
        self.payload_hash
    }
}

/// One logical key change is deterministically delete-old then upsert-new.
pub fn expand_key_change(
    positional: WalIdentityInput,
    source: SourceVersion,
    relation: RelationSchemaVersion,
    old_key: CanonicalKey,
    new_key: CanonicalKey,
    new_columns: Vec<ColumnState>,
) -> Result<[CanonicalMutation; 2], OrderingFailure> {
    if positional.capture_epoch != source.capture_epoch() {
        return Err(OrderingFailure::contract("CAPTURE_EPOCH_MISMATCH"));
    }
    if positional.row_ordinal != u64::from(source.ordinal()) {
        return Err(OrderingFailure::contract("ROW_ORDINAL_MISMATCH"));
    }
    validate_key(&old_key)?;
    validate_key(&new_key)?;
    if old_key == new_key {
        return Err(OrderingFailure::contract("KEY_CHANGE_IDENTITIES_EQUAL"));
    }
    if new_columns.is_empty()
        || new_columns
            .iter()
            .any(|state| matches!(state, ColumnState::Absent | ColumnState::UnchangedToast))
    {
        return Err(OrderingFailure::contract("KEY_CHANGE_NEW_TUPLE_INCOMPLETE"));
    }
    let build = |mutation_ordinal, key, kind, columns| {
        let input = WalIdentityInput {
            mutation_ordinal,
            ..positional
        };
        let version = MutationVersion::from_wal(source, input)?;
        let event_id = version.connector_event_id();
        let payload = MutationPayload {
            relation,
            key,
            kind,
            columns,
        };
        let payload_hash = payload.hash(version)?;
        Ok(CanonicalMutation {
            connector_event_id: event_id,
            version,
            payload,
            payload_hash,
        })
    };
    Ok([
        build(0, old_key, MutationKind::Delete, Vec::new())?,
        build(1, new_key, MutationKind::Upsert, new_columns)?,
    ])
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DuplicateDecision {
    DifferentIdentity,
    AcceptDuplicate,
    BlockConflict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ImmutableMutationRecord {
    event_id: Hash32,
    position: MutationVersion,
    payload_hash: Hash32,
}
impl ImmutableMutationRecord {
    #[must_use]
    pub const fn from_parts(position: MutationVersion, payload_hash: Hash32) -> Self {
        Self {
            event_id: position.connector_event_id(),
            position,
            payload_hash,
        }
    }
}
#[must_use]
pub fn classify_replay(
    existing: ImmutableMutationRecord,
    replay: ImmutableMutationRecord,
) -> DuplicateDecision {
    if existing.event_id != replay.event_id {
        DuplicateDecision::DifferentIdentity
    } else if existing.position != replay.position || existing.payload_hash != replay.payload_hash {
        DuplicateDecision::BlockConflict
    } else {
        DuplicateDecision::AcceptDuplicate
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderingFailure {
    pub class: &'static str,
    pub fingerprint: &'static str,
    pub failed_boundary: &'static str,
    pub allowed_actions: &'static str,
    pub recovery: &'static str,
}
impl OrderingFailure {
    fn contract(fingerprint: &'static str) -> Self {
        Self {
            class: "contract",
            fingerprint,
            failed_boundary: "before_feedback",
            allowed_actions: "inspect_or_reseed",
            recovery: "correct_contract_or_reseed",
        }
    }
    #[must_use]
    pub fn payload_conflict() -> Self {
        Self {
            class: "payload_conflict",
            fingerprint: "CONNECTOR_EVENT_PAYLOAD_CONFLICT",
            failed_boundary: "before_checkpoint_and_feedback",
            allowed_actions: "inspect_or_reseed",
            recovery: "do_not_retry_until_conflict_is_resolved",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const EPOCH: CaptureEpoch = CaptureEpoch::from_store(7);
    fn value(oid: u32, bytes: &[u8]) -> CanonicalValue {
        CanonicalValue {
            type_oid: oid,
            type_modifier: -1,
            bytes: bytes.to_vec(),
        }
    }
    fn key(bytes: &[u8]) -> CanonicalKey {
        vec![CanonicalKeyComponent::Bytes(bytes.to_vec())]
    }
    fn relation() -> RelationSchemaVersion {
        relation_for("accounts")
    }
    fn relation_for(table: &str) -> RelationSchemaVersion {
        RelationSchemaVersion::derive(
            LogicalTableIdentity::derive(&source_identity(), "public", table),
            42,
            b"id:bytea,value:text",
        )
    }
    fn source(lsn: u64, xid: u32, ordinal: u32) -> SourceVersion {
        source_version_for_row(EPOCH, ReceivedLsn::from_wire(lsn), xid, u64::from(ordinal)).unwrap()
    }
    fn source_identity() -> SourceIdentity {
        SourceIdentity {
            system_identifier: 42,
            timeline: 1,
            database_identity: 16_384,
            slot_name: "boring_cdc".into(),
            plugin: "pgoutput".into(),
            publication_fingerprint: crate::m1_source_identity::Fingerprint::digest(b"publication"),
            protocol_fingerprint: crate::m1_source_identity::supported_protocol_fingerprint(),
        }
    }
    fn version() -> MutationVersion {
        MutationVersion::from_wal(source(112, 8, 3), wal(0)).unwrap()
    }
    fn ordered_version(lsn: u64, xid: u32, ordinal: u32) -> MutationVersion {
        let position = WalIdentityInput {
            row_ordinal: u64::from(ordinal),
            ..wal(0)
        };
        MutationVersion::from_wal(source(lsn, xid, ordinal), position).unwrap()
    }
    fn wal(mutation_ordinal: u8) -> WalIdentityInput {
        WalIdentityInput {
            capture_epoch: EPOCH,
            source_slot_identity: SourceSlotIdentity::derive(&source_identity()),
            transaction_end_lsn: ReceivedLsn::from_wire(120),
            row_ordinal: 3,
            mutation_ordinal,
        }
    }

    #[test]
    fn four_column_states_have_distinct_payload_hashes() {
        let hashes = [
            ColumnState::Absent,
            ColumnState::Null,
            ColumnState::UnchangedToast,
            ColumnState::Value(value(25, b"")),
        ]
        .into_iter()
        .map(|state| {
            MutationPayload {
                relation: relation(),
                key: key(b"k"),
                kind: MutationKind::Upsert,
                columns: vec![state],
            }
            .hash(version())
            .unwrap()
        })
        .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(hashes.len(), 4);
    }

    #[test]
    fn positional_event_id_excludes_payload_and_volatile_fields() {
        let id = wal_connector_event_id(wal(0));
        let a = MutationPayload {
            relation: relation(),
            key: key(b"k"),
            kind: MutationKind::Upsert,
            columns: vec![ColumnState::Value(value(25, b"a"))],
        }
        .hash(version())
        .unwrap();
        let b = MutationPayload {
            relation: relation(),
            key: key(b"k"),
            kind: MutationKind::Upsert,
            columns: vec![ColumnState::Value(value(25, b"b"))],
        }
        .hash(version())
        .unwrap();
        assert_ne!(a, b);
        assert_eq!(id, wal_connector_event_id(wal(0)));
    }

    #[test]
    fn wal_and_snapshot_identities_are_namespaced_and_stable() {
        let wal_id = wal_connector_event_id(wal(0));
        let snapshot_id = snapshot_connector_event_id(SnapshotIdentityInput {
            capture_epoch: EPOCH,
            generation: DestinationGeneration::from_store(2),
            logical_table_id: LogicalTableIdentity::derive(
                &source_identity(),
                "public",
                "accounts",
            ),
            chunk_id: 4,
            key_hash: canonical_key_hash(&key(b"k")).unwrap(),
        });
        assert_ne!(wal_id, snapshot_id);
        assert_eq!(
            wal_id.hex(),
            "06004c4a0b18bedd87c4fabd9102bbaf009e50ffecfea740528edeb68485d548"
        );
        assert_eq!(
            snapshot_id.hex(),
            "e4a6350855eb0705318a27f6822976320cc869cff47753af9324600165c046a9"
        );
        let snapshot_source =
            source_version_for_row(EPOCH, ReceivedLsn::from_wire(80), 0, 0).unwrap();
        let snapshot_position = SnapshotIdentityInput {
            capture_epoch: EPOCH,
            generation: DestinationGeneration::from_store(2),
            logical_table_id: LogicalTableIdentity::derive(
                &source_identity(),
                "public",
                "accounts",
            ),
            chunk_id: 4,
            key_hash: canonical_key_hash(&key(b"k")).unwrap(),
        };
        assert_eq!(
            MutationVersion::from_snapshot(snapshot_source, snapshot_position)
                .unwrap()
                .connector_event_id(),
            snapshot_id
        );
    }

    #[test]
    fn snapshot_payload_requires_matching_table_and_key() {
        let payload = MutationPayload {
            relation: relation(),
            key: key(b"k"),
            kind: MutationKind::Upsert,
            columns: vec![ColumnState::Null],
        };
        let snapshot_source =
            source_version_for_row(EPOCH, ReceivedLsn::from_wire(80), 0, 0).unwrap();
        let snapshot_version = |logical_table_id, key_hash| {
            MutationVersion::from_snapshot(
                snapshot_source,
                SnapshotIdentityInput {
                    capture_epoch: EPOCH,
                    generation: DestinationGeneration::from_store(2),
                    logical_table_id,
                    chunk_id: 4,
                    key_hash,
                },
            )
            .unwrap()
        };
        assert!(
            payload
                .hash(snapshot_version(
                    relation().logical_table,
                    canonical_key_hash(&key(b"k")).unwrap()
                ))
                .is_ok()
        );
        for mismatched in [
            snapshot_version(
                relation_for("other").logical_table,
                canonical_key_hash(&key(b"k")).unwrap(),
            ),
            snapshot_version(
                relation().logical_table,
                canonical_key_hash(&key(b"other")).unwrap(),
            ),
        ] {
            assert_eq!(
                payload.hash(mismatched).unwrap_err().fingerprint,
                "SNAPSHOT_PAYLOAD_IDENTITY_MISMATCH"
            );
        }
    }

    #[test]
    fn relation_fingerprint_changes_payload_hash() {
        let payload_hash = |relation| {
            MutationPayload {
                relation,
                key: key(b"k"),
                kind: MutationKind::Upsert,
                columns: vec![ColumnState::Null],
            }
            .hash(version())
            .unwrap()
        };
        assert_ne!(
            payload_hash(relation()),
            payload_hash(relation_for("other"))
        );
    }

    #[test]
    fn key_change_expands_delete_before_upsert() {
        let mutations = expand_key_change(
            wal(9),
            source(112, 8, 3),
            relation(),
            key(b"old"),
            key(b"new"),
            vec![ColumnState::Value(value(25, b"row"))],
        )
        .unwrap();
        assert_eq!(mutations[0].payload().kind, MutationKind::Delete);
        assert_eq!(mutations[1].payload().kind, MutationKind::Upsert);
        assert_eq!(mutations[0].version().mutation_ordinal(), 0);
        assert_eq!(
            compare_versions(mutations[0].version(), mutations[1].version()),
            VersionComparison::Less
        );
        assert_ne!(
            mutations[0].connector_event_id(),
            mutations[1].connector_event_id()
        );
        assert_eq!(
            expand_key_change(
                wal(0),
                source(112, 8, 3),
                relation(),
                key(b"same"),
                key(b"same"),
                vec![ColumnState::Null],
            )
            .unwrap_err()
            .fingerprint,
            "KEY_CHANGE_IDENTITIES_EQUAL"
        );
        for incomplete in [
            vec![],
            vec![ColumnState::Absent],
            vec![ColumnState::UnchangedToast],
        ] {
            assert_eq!(
                expand_key_change(
                    wal(0),
                    source(112, 8, 3),
                    relation(),
                    key(b"old"),
                    key(b"new"),
                    incomplete
                )
                .unwrap_err()
                .fingerprint,
                "KEY_CHANGE_NEW_TUPLE_INCOMPLETE"
            );
        }
    }

    #[test]
    fn repeated_same_key_has_total_source_order() {
        let versions = [ordered_version(100, 9, 1), ordered_version(101, 1, 0)];
        assert_eq!(
            compare_versions(versions[0], versions[1]),
            VersionComparison::Less
        );
        let same_position_other_xid = ordered_version(100, 99, 1);
        assert_eq!(
            compare_versions(versions[0], same_position_other_xid),
            VersionComparison::Equal
        );
        let next_transaction_ordinal = ordered_version(100, 1, 2);
        assert_eq!(
            compare_versions(versions[0], next_transaction_ordinal),
            VersionComparison::Less
        );
        let other_source =
            source_version_for_row(CaptureEpoch::from_store(8), ReceivedLsn::from_wire(1), 1, 0)
                .unwrap();
        let other_position = WalIdentityInput {
            capture_epoch: CaptureEpoch::from_store(8),
            row_ordinal: 0,
            ..wal(0)
        };
        let other = MutationVersion::from_wal(other_source, other_position).unwrap();
        assert_eq!(
            compare_versions(versions[0], other),
            VersionComparison::DifferentCaptureEpoch
        );
    }

    #[test]
    fn same_id_same_hash_is_duplicate_and_different_hash_is_conflict() {
        let a = hash_fields(b"fixture", &[b"a"]);
        let b = hash_fields(b"fixture", &[b"b"]);
        let position = version();
        let same = ImmutableMutationRecord::from_parts(position, a);
        assert_eq!(
            classify_replay(same, same),
            DuplicateDecision::AcceptDuplicate
        );
        assert_eq!(
            classify_replay(same, ImmutableMutationRecord::from_parts(position, b)),
            DuplicateDecision::BlockConflict
        );
        let other_position = MutationVersion::from_wal(source(113, 8, 3), wal(1)).unwrap();
        assert_eq!(
            classify_replay(same, ImmutableMutationRecord::from_parts(other_position, b)),
            DuplicateDecision::DifferentIdentity
        );
        let corrupted_position = MutationVersion {
            source: source(999, 8, 3),
            ..position
        };
        let corrupted = ImmutableMutationRecord {
            event_id: same.event_id,
            position: corrupted_position,
            payload_hash: a,
        };
        assert_eq!(
            classify_replay(same, corrupted),
            DuplicateDecision::BlockConflict
        );
        for seed in 0_u64..64 {
            let id = hash_fields(b"property-id", &[&seed.to_be_bytes()]);
            let payload = hash_fields(b"property-payload", &[&seed.to_be_bytes()]);
            let changed = hash_fields(b"property-payload", &[&seed.wrapping_add(1).to_be_bytes()]);
            let position = MutationVersion {
                connector_event_id: id,
                ..version()
            };
            let record = ImmutableMutationRecord::from_parts(position, payload);
            assert_eq!(
                classify_replay(record, record),
                DuplicateDecision::AcceptDuplicate
            );
            assert_eq!(
                classify_replay(
                    record,
                    ImmutableMutationRecord::from_parts(position, changed)
                ),
                DuplicateDecision::BlockConflict
            );
        }
        let failure = OrderingFailure::payload_conflict();
        assert_eq!(failure.failed_boundary, "before_checkpoint_and_feedback");

        crate::m1_raw_demo::emit_asserted_case(
            "SCN-M1-RAW-IDENTITY-CONFLICT",
            "blocked",
            "unchanged",
            "identity_payload_conflict",
        );
    }

    #[test]
    fn canonical_hashes_are_unambiguous_for_component_boundaries_and_types() {
        assert_ne!(
            canonical_key_hash(&vec![
                CanonicalKeyComponent::Bytes(b"ab".to_vec()),
                CanonicalKeyComponent::Bytes(b"c".to_vec())
            ])
            .unwrap(),
            canonical_key_hash(&vec![
                CanonicalKeyComponent::Bytes(b"a".to_vec()),
                CanonicalKeyComponent::Bytes(b"bc".to_vec())
            ])
            .unwrap()
        );
        assert_ne!(
            canonical_key_hash(&key(b"1")).unwrap(),
            PhysicalKeyHash::derive(&[CanonicalKeyComponent::I64(1)])
        );
        assert_ne!(value(25, b"x").hash(), value(17, b"x").hash());
        assert_eq!(
            Hash32::from_bytes(canonical_key_hash(&key(b"golden")).unwrap().bytes()).hex(),
            "4317afc16b609fcbf9d0133dc604a3250d0b18425f37226a9dd16320e4bba187"
        );
        assert_eq!(
            value(25, b"golden").hash().hex(),
            "ddf09f7280c10ad15ffb79c78ce4678a5a99d27a890655098046394736f39adb"
        );
        let payload = MutationPayload {
            relation: relation(),
            key: key(b"golden"),
            kind: MutationKind::Upsert,
            columns: vec![ColumnState::Null],
        };
        assert_eq!(
            payload.hash(version()).unwrap().hex(),
            "fc98f5b0520965efe632181840282500e412a5bab5e9ca2c2a63645e6abf4f4d"
        );
    }

    #[test]
    fn epoch_and_decoder_ordinal_mismatches_fail_closed() {
        assert_eq!(
            source_version_for_row(EPOCH, ReceivedLsn::from_wire(1), 1, u64::from(u32::MAX) + 1)
                .unwrap_err()
                .fingerprint,
            "ROW_ORDINAL_OUT_OF_RANGE"
        );
        let mut position = wal(0);
        position.capture_epoch = CaptureEpoch::from_store(8);
        assert_eq!(
            expand_key_change(
                position,
                source(112, 8, 3),
                relation(),
                key(b"old"),
                key(b"new"),
                vec![]
            )
            .unwrap_err()
            .fingerprint,
            "CAPTURE_EPOCH_MISMATCH"
        );
        let invalid_end = WalIdentityInput {
            transaction_end_lsn: ReceivedLsn::from_wire(111),
            ..wal(0)
        };
        assert_eq!(
            MutationVersion::from_wal(source(112, 8, 3), invalid_end)
                .unwrap_err()
                .fingerprint,
            "END_LSN_BEFORE_COMMIT_LSN"
        );
        let position = WalIdentityInput {
            row_ordinal: 4,
            ..wal(0)
        };
        assert_eq!(
            expand_key_change(
                position,
                source(112, 8, 3),
                relation(),
                key(b"old"),
                key(b"new"),
                vec![]
            )
            .unwrap_err()
            .fingerprint,
            "ROW_ORDINAL_MISMATCH"
        );
        let snapshot_position = |capture_epoch| SnapshotIdentityInput {
            capture_epoch,
            generation: DestinationGeneration::from_store(2),
            logical_table_id: relation().logical_table,
            chunk_id: 4,
            key_hash: canonical_key_hash(&key(b"k")).unwrap(),
        };
        assert_eq!(
            MutationVersion::from_snapshot(
                source(80, 0, 0),
                snapshot_position(CaptureEpoch::from_store(8)),
            )
            .unwrap_err()
            .fingerprint,
            "CAPTURE_EPOCH_MISMATCH"
        );
        for invalid_source in [source(80, 1, 0), source(80, 0, 1)] {
            assert_eq!(
                MutationVersion::from_snapshot(invalid_source, snapshot_position(EPOCH))
                    .unwrap_err()
                    .fingerprint,
                "SNAPSHOT_ORDINAL_INVALID"
            );
        }
    }

    #[test]
    fn ordering_case_inventory_is_complete() {
        let inventory: serde_json::Value =
            serde_json::from_str(include_str!("../contracts/m1/ordering-cases.json")).unwrap();
        assert_eq!(inventory["owner_bead"], "boring-cdc-m1-ordering");
        assert_eq!(inventory["cases"].as_array().unwrap().len(), 10);
    }

    #[test]
    fn invalid_keys_fail_closed() {
        assert_eq!(
            canonical_key_hash(&Vec::new()).unwrap_err().fingerprint,
            "CANONICAL_KEY_INVALID"
        );
        assert_eq!(
            canonical_key_hash(&vec![CanonicalKeyComponent::Null])
                .unwrap_err()
                .fingerprint,
            "CANONICAL_KEY_INVALID"
        );
        let max_arity = vec![CanonicalKeyComponent::I64(1); MAX_CANONICAL_KEY_COMPONENTS];
        assert!(canonical_key_hash(&max_arity).is_ok());
        let over_max = vec![CanonicalKeyComponent::I64(1); MAX_CANONICAL_KEY_COMPONENTS + 1];
        assert_eq!(
            canonical_key_hash(&over_max).unwrap_err().fingerprint,
            "CANONICAL_KEY_INVALID"
        );
    }
}
