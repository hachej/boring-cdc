//! Canonical mutation ordering, positional identities, and payload conflict handling.
//!
//! Inputs are already-admitted decoder values and the relation fingerprint owned by
//! `m1_ddl_fixtures`. This module performs no journal or destination I/O.

use crate::m1_source_identity::{LogicalTableIdentity, SourceIdentity};
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
const KEY_HASH_DOMAIN: &[u8] = b"boring-cdc/canonical-key/v1";
// M0-PROVISIONAL: boring-cdc-d-event-id (RECOMMENDED SHA-256 domain/version literals).
const VALUE_HASH_DOMAIN: &[u8] = b"boring-cdc/canonical-value/v1";
// M0-PROVISIONAL: boring-cdc-d-event-id (RECOMMENDED SHA-256 domain/version literals).
const PAYLOAD_HASH_DOMAIN: &[u8] = b"boring-cdc/mutation-payload/v1";
// M0-PROVISIONAL: boring-cdc-d-event-id (RECOMMENDED origin rank: snapshot then WAL).
pub const SNAPSHOT_ORIGIN_RANK: u8 = 0;
// M0-PROVISIONAL: boring-cdc-d-event-id (RECOMMENDED origin rank: snapshot then WAL).
pub const WAL_ORIGIN_RANK: u8 = 1;

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
fn put_field(h: &mut Sha256, bytes: &[u8]) {
    h.update((bytes.len() as u64).to_be_bytes());
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
            Self::Absent => out.push(0),
            Self::Null => out.push(1),
            Self::UnchangedToast => out.push(2),
            Self::Value(value) => {
                out.push(3);
                out.extend(value.type_oid.to_be_bytes());
                out.extend(value.type_modifier.to_be_bytes());
                out.extend((value.bytes.len() as u64).to_be_bytes());
                out.extend(&value.bytes);
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalKey(pub Vec<CanonicalValue>);
impl CanonicalKey {
    pub fn validate(&self) -> Result<(), OrderingFailure> {
        // M0-PROVISIONAL: boring-cdc-d-keys (RECOMMENDED non-empty, max 32 components).
        if self.0.is_empty() || self.0.len() > 32 {
            return Err(OrderingFailure::contract("CANONICAL_KEY_ARITY_INVALID"));
        }
        Ok(())
    }
    pub fn hash(&self) -> Result<Hash32, OrderingFailure> {
        self.validate()?;
        let mut bytes = Vec::new();
        bytes.extend((self.0.len() as u64).to_be_bytes());
        for value in &self.0 {
            bytes.extend(value.type_oid.to_be_bytes());
            bytes.extend(value.type_modifier.to_be_bytes());
            bytes.extend((value.bytes.len() as u64).to_be_bytes());
            bytes.extend(&value.bytes);
        }
        Ok(hash_fields(KEY_HASH_DOMAIN, &[&bytes]))
    }
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
    pub key_hash: Hash32,
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
            Self::Delete => 0,
            Self::Upsert => 1,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationPayload {
    /// Lowercase SHA-256 emitted by the admitted DDL relation contract.
    pub relation_fingerprint: String,
    pub key: CanonicalKey,
    pub kind: MutationKind,
    pub columns: Vec<ColumnState>,
}
impl MutationPayload {
    pub fn hash(&self, version: MutationVersion) -> Result<Hash32, OrderingFailure> {
        if self.relation_fingerprint.len() != 64
            || !self
                .relation_fingerprint
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(OrderingFailure::contract("RELATION_FINGERPRINT_INVALID"));
        }
        let key_hash = self.key.hash()?;
        let mut columns = Vec::new();
        columns.extend((self.columns.len() as u64).to_be_bytes());
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
                self.relation_fingerprint.as_bytes(),
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
pub struct MutationVersion {
    pub source: SourceVersion,
    pub origin_rank: u8,
    pub mutation_ordinal: u8,
    pub connector_event_id: Hash32,
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
            v.source.transaction_id(),
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
    pub connector_event_id: Hash32,
    pub version: MutationVersion,
    pub payload: MutationPayload,
    pub payload_hash: Hash32,
}

/// One logical key change is deterministically delete-old then upsert-new.
pub fn expand_key_change(
    positional: WalIdentityInput,
    source: SourceVersion,
    relation_fingerprint: &str,
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
    old_key.validate()?;
    new_key.validate()?;
    if old_key == new_key {
        return Err(OrderingFailure::contract("KEY_CHANGE_IDENTITIES_EQUAL"));
    }
    let build = |mutation_ordinal, key, kind, columns| {
        let input = WalIdentityInput {
            mutation_ordinal,
            ..positional
        };
        let event_id = wal_connector_event_id(input);
        let payload = MutationPayload {
            relation_fingerprint: relation_fingerprint.to_owned(),
            key,
            kind,
            columns,
        };
        let version = MutationVersion {
            source,
            origin_rank: WAL_ORIGIN_RANK,
            mutation_ordinal,
            connector_event_id: event_id,
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
#[must_use]
pub fn classify_replay(
    existing_id: Hash32,
    existing_payload_hash: Hash32,
    replay_id: Hash32,
    replay_payload_hash: Hash32,
) -> DuplicateDecision {
    if existing_id != replay_id {
        DuplicateDecision::DifferentIdentity
    } else if existing_payload_hash == replay_payload_hash {
        DuplicateDecision::AcceptDuplicate
    } else {
        DuplicateDecision::BlockConflict
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
        CanonicalKey(vec![value(25, bytes)])
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
        MutationVersion {
            source: source(112, 8, 3),
            origin_rank: WAL_ORIGIN_RANK,
            mutation_ordinal: 0,
            connector_event_id: wal_connector_event_id(wal(0)),
        }
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
        let fp = "11".repeat(32);
        let hashes = [
            ColumnState::Absent,
            ColumnState::Null,
            ColumnState::UnchangedToast,
            ColumnState::Value(value(25, b"")),
        ]
        .into_iter()
        .map(|state| {
            MutationPayload {
                relation_fingerprint: fp.clone(),
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
            relation_fingerprint: "11".repeat(32),
            key: key(b"k"),
            kind: MutationKind::Upsert,
            columns: vec![ColumnState::Value(value(25, b"a"))],
        }
        .hash(version())
        .unwrap();
        let b = MutationPayload {
            relation_fingerprint: "11".repeat(32),
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
            key_hash: key(b"k").hash().unwrap(),
        });
        assert_ne!(wal_id, snapshot_id);
        assert_eq!(
            wal_id.hex(),
            "06004c4a0b18bedd87c4fabd9102bbaf009e50ffecfea740528edeb68485d548"
        );
        assert_eq!(
            snapshot_id.hex(),
            "5a0574e19de8c33923e12953a6c002e16c06dc13c46aa82b8e249f77ffb83fe3"
        );
    }

    #[test]
    fn key_change_expands_delete_before_upsert() {
        let mutations = expand_key_change(
            wal(9),
            source(112, 8, 3),
            &"11".repeat(32),
            key(b"old"),
            key(b"new"),
            vec![ColumnState::Value(value(25, b"row"))],
        )
        .unwrap();
        assert_eq!(mutations[0].payload.kind, MutationKind::Delete);
        assert_eq!(mutations[1].payload.kind, MutationKind::Upsert);
        assert_eq!(mutations[0].version.mutation_ordinal, 0);
        assert_eq!(
            compare_versions(mutations[0].version, mutations[1].version),
            VersionComparison::Less
        );
        assert_ne!(
            mutations[0].connector_event_id,
            mutations[1].connector_event_id
        );
    }

    #[test]
    fn repeated_same_key_has_total_source_order() {
        let id0 = wal_connector_event_id(wal(0));
        let versions = [
            MutationVersion {
                source: source(100, 9, 1),
                origin_rank: WAL_ORIGIN_RANK,
                mutation_ordinal: 0,
                connector_event_id: id0,
            },
            MutationVersion {
                source: source(101, 1, 0),
                origin_rank: WAL_ORIGIN_RANK,
                mutation_ordinal: 0,
                connector_event_id: id0,
            },
        ];
        assert_eq!(
            compare_versions(versions[0], versions[1]),
            VersionComparison::Less
        );
        let other = MutationVersion {
            source: SourceVersion::from_decoded(
                CaptureEpoch::from_store(8),
                ReceivedLsn::from_wire(1),
                1,
                0,
            ),
            ..versions[0]
        };
        assert_eq!(
            compare_versions(versions[0], other),
            VersionComparison::DifferentCaptureEpoch
        );
    }

    #[test]
    fn same_id_same_hash_is_duplicate_and_different_hash_is_conflict() {
        let a = hash_fields(b"fixture", &[b"a"]);
        let b = hash_fields(b"fixture", &[b"b"]);
        assert_eq!(
            classify_replay(a, a, a, a),
            DuplicateDecision::AcceptDuplicate
        );
        assert_eq!(
            classify_replay(a, a, a, b),
            DuplicateDecision::BlockConflict
        );
        assert_eq!(
            classify_replay(a, a, b, b),
            DuplicateDecision::DifferentIdentity
        );
        for seed in 0_u64..64 {
            let id = hash_fields(b"property-id", &[&seed.to_be_bytes()]);
            let payload = hash_fields(b"property-payload", &[&seed.to_be_bytes()]);
            let changed = hash_fields(b"property-payload", &[&seed.wrapping_add(1).to_be_bytes()]);
            assert_eq!(
                classify_replay(id, payload, id, payload),
                DuplicateDecision::AcceptDuplicate
            );
            assert_eq!(
                classify_replay(id, payload, id, changed),
                DuplicateDecision::BlockConflict
            );
        }
        let failure = OrderingFailure::payload_conflict();
        assert_eq!(failure.failed_boundary, "before_checkpoint_and_feedback");
    }

    #[test]
    fn canonical_hashes_are_unambiguous_for_component_boundaries_and_types() {
        assert_ne!(
            CanonicalKey(vec![value(25, b"ab"), value(25, b"c")])
                .hash()
                .unwrap(),
            CanonicalKey(vec![value(25, b"a"), value(25, b"bc")])
                .hash()
                .unwrap()
        );
        assert_ne!(
            key(b"1").hash().unwrap(),
            CanonicalKey(vec![value(20, b"1")]).hash().unwrap()
        );
        assert_ne!(value(25, b"x").hash(), value(17, b"x").hash());
        assert_eq!(
            key(b"golden").hash().unwrap().hex(),
            "000162f8dd1a1f04d4ec4e8f6357c5202e25db4f0993de3069591621cbc56be3"
        );
        assert_eq!(
            value(25, b"golden").hash().hex(),
            "ddf09f7280c10ad15ffb79c78ce4678a5a99d27a890655098046394736f39adb"
        );
        let payload = MutationPayload {
            relation_fingerprint: "11".repeat(32),
            key: key(b"golden"),
            kind: MutationKind::Upsert,
            columns: vec![ColumnState::Null],
        };
        assert_eq!(
            payload.hash(version()).unwrap().hex(),
            "69977ac3ca1be0868cf1f6dc35ecaef3cf68e8eace61967a1ad0e24eaf8e0b00"
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
                &"11".repeat(32),
                key(b"old"),
                key(b"new"),
                vec![]
            )
            .unwrap_err()
            .fingerprint,
            "CAPTURE_EPOCH_MISMATCH"
        );
        let position = WalIdentityInput {
            row_ordinal: 4,
            ..wal(0)
        };
        assert_eq!(
            expand_key_change(
                position,
                source(112, 8, 3),
                &"11".repeat(32),
                key(b"old"),
                key(b"new"),
                vec![]
            )
            .unwrap_err()
            .fingerprint,
            "ROW_ORDINAL_MISMATCH"
        );
    }

    #[test]
    fn ordering_case_inventory_is_complete() {
        let inventory: serde_json::Value =
            serde_json::from_str(include_str!("../contracts/m1/ordering-cases.json")).unwrap();
        assert_eq!(inventory["owner_bead"], "boring-cdc-m1-ordering");
        assert_eq!(inventory["cases"].as_array().unwrap().len(), 9);
    }

    #[test]
    fn invalid_key_and_relation_fingerprint_fail_closed() {
        assert_eq!(
            CanonicalKey(Vec::new()).hash().unwrap_err().fingerprint,
            "CANONICAL_KEY_ARITY_INVALID"
        );
        let payload = MutationPayload {
            relation_fingerprint: "raw-name".into(),
            key: key(b"k"),
            kind: MutationKind::Delete,
            columns: vec![],
        };
        assert_eq!(
            payload.hash(version()).unwrap_err().fingerprint,
            "RELATION_FINGERPRINT_INVALID"
        );
    }
}
