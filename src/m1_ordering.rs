//! Canonical mutation ordering, positional identities, and payload conflict handling.
//!
//! Inputs are already-admitted decoder values and the relation fingerprint owned by
//! `m1_ddl_fixtures`. This module performs no journal or destination I/O.

use crate::m1_transition_kernel::{
    CaptureEpoch, DestinationGeneration, ReceivedLsn, SourceVersion,
};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::fmt;

// M0-PROVISIONAL: boring-cdc-d-event-id (RECOMMENDED SHA-256 domain/version literals).
const WAL_EVENT_DOMAIN: &[u8] = b"boring-cdc/wal-event/v1";
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
pub struct WalIdentityInput {
    pub capture_epoch: CaptureEpoch,
    /// Stable fingerprint of source system/database/slot, never a raw identifier.
    pub source_slot_fingerprint: Hash32,
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
            &input.source_slot_fingerprint.bytes(),
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
    pub logical_table_id: Hash32,
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
            &input.logical_table_id.bytes(),
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
        SourceVersion::from_decoded(EPOCH, ReceivedLsn::from_wire(lsn), xid, ordinal)
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
            source_slot_fingerprint: hash_fields(b"fixture", &[b"source-slot"]),
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
            logical_table_id: hash_fields(b"fixture", &[b"table"]),
            chunk_id: 4,
            key_hash: key(b"k").hash().unwrap(),
        });
        assert_ne!(wal_id, snapshot_id);
        assert_eq!(
            wal_id.hex(),
            "2e61c53ad7f03342674ea78b9db8f0b008ee6b3363135b544a0e3c31d3fd16c7"
        );
        assert_eq!(
            snapshot_id.hex(),
            "4f76eeb2353aa1ff3a934ee5b83bba95b0676194f22699645c871b0e82d7c2d9"
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
