//! M0 mutation envelope rendering from admitted M1 identities and canonical values.
//!
//! The journal assigns `journal_seq` atomically. Use `render_mutation_template` before its
//! commit; the journal binds the sequence to the stored JSON. The sequence is excluded from
//! the stable M0 field hash.

use crate::m1_decoder::{OldTupleKind, RelationContract, RowChange, RowKind};
use crate::m1_ordering::{
    ColumnState, MutationKind, MutationPayload, MutationVersion, SnapshotIdentityInput,
    SourceOperation, SourceSlotIdentity, WalIdentityInput, expand_key_change,
    source_version_for_row,
};
use crate::m1_source_identity::{
    CanonicalKeyComponent, PhysicalKeyHash, RelationSchemaVersion, SourceIdentity,
};
use crate::m1_transition_kernel::{CaptureEpoch, DestinationGeneration, ReceivedLsn};
use crate::m2_pg_value::{canonical_columns, canonical_key};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const MAX_ROW_BYTES: usize = 4_194_304;
const MAX_SCALAR_BYTES: usize = 1_048_576;
const MAX_EVENT_BYTES: usize = 8_388_608;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnvelopeError {
    Invalid,
    Limit,
}

pub struct MutationEnvelope {
    pub json: Vec<u8>,
    pub canonical_preimage: Vec<u8>,
    pub canonical_payload_hash: String,
}

pub struct WalMutationTemplate {
    pub event_id: String,
    pub source_row_ordinal: u32,
    pub source_mutation_ordinal: u8,
    pub relation_fingerprint: String,
    pub envelope: MutationEnvelope,
}

/// Render one row copied from an admitted M3 snapshot. The caller must verify the complete
/// anchor, persisted chunk identity and relation schema before supplying this tuple.
pub fn render_snapshot_row(
    capture_epoch: CaptureEpoch,
    generation: DestinationGeneration,
    contract: &RelationContract,
    relation: RelationSchemaVersion,
    snapshot_boundary_lsn: ReceivedLsn,
    chunk_ordinal: u64,
    tuple: &[crate::m1_decoder::TupleValue],
) -> Result<(String, MutationEnvelope), EnvelopeError> {
    if contract.control.is_some() {
        return Err(EnvelopeError::Invalid);
    }
    let key = canonical_key(contract, tuple, false).map_err(|_| EnvelopeError::Invalid)?;
    let columns =
        canonical_columns(&contract.relation, tuple).map_err(|_| EnvelopeError::Invalid)?;
    if columns
        .iter()
        .any(|column| matches!(column, ColumnState::UnchangedToast))
    {
        return Err(EnvelopeError::Invalid);
    }
    let source_version = source_version_for_row(capture_epoch, snapshot_boundary_lsn, 0, 0)
        .map_err(|_| EnvelopeError::Invalid)?;
    let version = MutationVersion::from_snapshot(
        source_version,
        SnapshotIdentityInput {
            capture_epoch,
            generation,
            logical_table_id: relation.logical_table,
            chunk_id: chunk_ordinal,
            key: key.clone(),
        },
    )
    .map_err(|_| EnvelopeError::Invalid)?;
    let key_modifiers = contract
        .key_columns
        .iter()
        .map(|&index| {
            contract
                .relation
                .columns
                .get(index)
                .map(|column| column.type_modifier)
                .ok_or(EnvelopeError::Invalid)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let envelope = render_mutation_template(
        &version,
        &MutationPayload {
            relation,
            key,
            operation: SourceOperation::Snapshot,
            before_key: None,
            kind: MutationKind::Upsert,
            columns,
        },
        &key_modifiers,
    )?;
    Ok((version.connector_event_id().hex(), envelope))
}

pub struct ControlPosition<'a> {
    pub kind: &'a str,
    pub generation: Option<u64>,
    pub xid: u32,
    pub row_ordinal: u64,
    pub commit_lsn: ReceivedLsn,
    pub end_lsn: ReceivedLsn,
}

pub fn render_control_template(
    source: &SourceIdentity,
    capture_epoch: CaptureEpoch,
    position: ControlPosition<'_>,
) -> Result<(String, MutationEnvelope), EnvelopeError> {
    let ControlPosition {
        kind,
        generation,
        xid,
        row_ordinal,
        commit_lsn,
        end_lsn,
    } = position;
    if !matches!(
        (kind, generation),
        ("heartbeat", None) | ("capture_fence", Some(1..))
    ) {
        return Err(EnvelopeError::Invalid);
    }
    let source_version = source_version_for_row(capture_epoch, commit_lsn, xid, row_ordinal)
        .map_err(|_| EnvelopeError::Invalid)?;
    let version = MutationVersion::from_wal(
        source_version,
        WalIdentityInput {
            capture_epoch,
            source_slot_identity: SourceSlotIdentity::derive(source),
            transaction_end_lsn: end_lsn,
            row_ordinal,
            mutation_ordinal: 0,
        },
    )
    .map_err(|_| EnvelopeError::Invalid)?;
    let event_id = version.connector_event_id().hex();
    let generation_bytes = generation.map(u64::to_be_bytes);
    let mut canonical_preimage = Vec::new();
    for field in [
        b"boring-cdc/control-payload/v1".as_slice(),
        version.connector_event_id().bytes().as_slice(),
        kind.as_bytes(),
        &end_lsn.get().to_be_bytes(),
        generation_bytes
            .as_ref()
            .map_or(&[][..], |bytes| bytes.as_slice()),
    ] {
        canonical_preimage.extend((field.len() as u64).to_be_bytes());
        canonical_preimage.extend(field);
    }
    let canonical_payload_hash = format!("{:x}", Sha256::digest(&canonical_preimage));
    let event = json!({
        "schema_version":"boring-cdc/event/v1",
        "event_type":"control",
        "connector_event_id":event_id,
        "journal_seq":0,
        "capture_epoch":capture_epoch.get(),
        "source_version":{
            "lsn_u64":commit_lsn.get(),
            "origin_rank":1,
            "transaction_ordinal":source_version.ordinal(),
            "mutation_ordinal":0,
            "connector_event_id":event_id,
        },
        "routing":"control",
        "operation":"control",
        "control":{
            "kind":kind,
            "transaction_end_lsn":end_lsn.get(),
            "generation":generation,
        },
        "payload_hash":canonical_payload_hash,
    });
    let json = serde_json::to_vec(&event).map_err(|_| EnvelopeError::Invalid)?;
    if json.len() > MAX_EVENT_BYTES {
        return Err(EnvelopeError::Limit);
    }
    Ok((
        event_id,
        MutationEnvelope {
            json,
            canonical_preimage,
            canonical_payload_hash,
        },
    ))
}

/// Convert an admitted pgoutput row to one or two durable M0 mutation templates. The relation
/// version must come from the catalog, not from the pgoutput Relation message alone.
pub fn render_wal_row_templates(
    source: &SourceIdentity,
    capture_epoch: CaptureEpoch,
    contract: &RelationContract,
    relation: RelationSchemaVersion,
    row: &RowChange,
    commit_lsn: ReceivedLsn,
    end_lsn: ReceivedLsn,
) -> Result<Vec<WalMutationTemplate>, EnvelopeError> {
    if row.relation_id != contract.relation.id || contract.control.is_some() {
        return Err(EnvelopeError::Invalid);
    }
    let source_version = source_version_for_row(capture_epoch, commit_lsn, row.xid, row.ordinal)
        .map_err(|_| EnvelopeError::Invalid)?;
    let position = WalIdentityInput {
        capture_epoch,
        source_slot_identity: SourceSlotIdentity::derive(source),
        transaction_end_lsn: end_lsn,
        row_ordinal: row.ordinal,
        mutation_ordinal: 0,
    };
    let key_modifiers = contract
        .key_columns
        .iter()
        .map(|&index| {
            contract
                .relation
                .columns
                .get(index)
                .map(|column| column.type_modifier)
                .ok_or(EnvelopeError::Invalid)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let new = row.new.as_deref();
    let old = row.old.as_deref();
    let old_key = match (old, row.old_kind) {
        (Some(tuple), Some(kind)) => Some(
            canonical_key(contract, tuple, kind == OldTupleKind::Key)
                .map_err(|_| EnvelopeError::Invalid)?,
        ),
        (None, None) => None,
        _ => return Err(EnvelopeError::Invalid),
    };
    let mut mutations = Vec::new();
    match row.kind {
        RowKind::Insert if old.is_none() => {
            let tuple = new.ok_or(EnvelopeError::Invalid)?;
            let key = canonical_key(contract, tuple, false).map_err(|_| EnvelopeError::Invalid)?;
            let columns =
                canonical_columns(&contract.relation, tuple).map_err(|_| EnvelopeError::Invalid)?;
            mutations.push((
                MutationVersion::from_wal(source_version, position)
                    .map_err(|_| EnvelopeError::Invalid)?,
                MutationPayload {
                    relation,
                    key,
                    operation: SourceOperation::Insert,
                    before_key: None,
                    kind: MutationKind::Upsert,
                    columns,
                },
            ));
        }
        RowKind::Update => {
            let tuple = new.ok_or(EnvelopeError::Invalid)?;
            let key = canonical_key(contract, tuple, false).map_err(|_| EnvelopeError::Invalid)?;
            let columns =
                canonical_columns(&contract.relation, tuple).map_err(|_| EnvelopeError::Invalid)?;
            if let Some(previous) = old_key.filter(|previous| *previous != key) {
                for mutation in
                    expand_key_change(position, source_version, relation, previous, key, columns)
                        .map_err(|_| EnvelopeError::Invalid)?
                {
                    mutations.push((mutation.version().clone(), mutation.payload().clone()));
                }
            } else {
                mutations.push((
                    MutationVersion::from_wal(source_version, position)
                        .map_err(|_| EnvelopeError::Invalid)?,
                    MutationPayload {
                        relation,
                        key,
                        operation: SourceOperation::Update,
                        before_key: None,
                        kind: MutationKind::Upsert,
                        columns,
                    },
                ));
            }
        }
        RowKind::Delete if new.is_none() => {
            let key = old_key.ok_or(EnvelopeError::Invalid)?;
            mutations.push((
                MutationVersion::from_wal(source_version, position)
                    .map_err(|_| EnvelopeError::Invalid)?,
                MutationPayload {
                    relation,
                    key,
                    operation: SourceOperation::Delete,
                    before_key: None,
                    kind: MutationKind::Delete,
                    columns: Vec::new(),
                },
            ));
        }
        _ => return Err(EnvelopeError::Invalid),
    }
    let source_row_ordinal = u32::try_from(row.ordinal).map_err(|_| EnvelopeError::Limit)?;
    mutations
        .into_iter()
        .map(|(version, payload)| {
            let envelope = render_mutation_template(&version, &payload, &key_modifiers)?;
            Ok(WalMutationTemplate {
                event_id: version.connector_event_id().hex(),
                source_row_ordinal,
                source_mutation_ordinal: version.mutation_ordinal(),
                relation_fingerprint: hex(&relation.fingerprint().bytes()),
                envelope,
            })
        })
        .collect()
}

pub fn render_mutation(
    version: &MutationVersion,
    payload: &MutationPayload,
    key_type_modifiers: &[i32],
    journal_seq: u64,
) -> Result<MutationEnvelope, EnvelopeError> {
    if journal_seq == 0 {
        return Err(EnvelopeError::Invalid);
    }
    render_mutation_inner(version, payload, key_type_modifiers, journal_seq)
}

/// Internal journal input. Sequence zero is replaced inside the atomic journal commit and is
/// never a valid emitted M0 event.
pub fn render_mutation_template(
    version: &MutationVersion,
    payload: &MutationPayload,
    key_type_modifiers: &[i32],
) -> Result<MutationEnvelope, EnvelopeError> {
    render_mutation_inner(version, payload, key_type_modifiers, 0)
}

fn render_mutation_inner(
    version: &MutationVersion,
    payload: &MutationPayload,
    key_type_modifiers: &[i32],
    journal_seq: u64,
) -> Result<MutationEnvelope, EnvelopeError> {
    let key = key_json(&payload.key, key_type_modifiers)?;
    let before_key = payload
        .before_key
        .as_ref()
        .map(|components| key_json(components, key_type_modifiers))
        .transpose()?;
    let key_hash = PhysicalKeyHash::derive(&payload.key).map_err(|_| EnvelopeError::Invalid)?;
    let columns = columns_json(&payload.columns)?;
    let canonical_preimage = payload
        .canonical_preimage(version)
        .map_err(|_| EnvelopeError::Invalid)?;
    if canonical_preimage.len() > MAX_EVENT_BYTES {
        return Err(EnvelopeError::Limit);
    }
    let canonical_payload_hash = format!("{:x}", Sha256::digest(&canonical_preimage));
    let event_id = version.connector_event_id().hex();
    let source = version.source();
    let operation = match payload.operation {
        SourceOperation::Snapshot => "snapshot",
        SourceOperation::Insert => "insert",
        SourceOperation::Update => "update",
        SourceOperation::Delete => "delete",
    };
    let mutation_kind = match payload.kind {
        MutationKind::Delete => "delete",
        MutationKind::Upsert => "upsert",
    };
    let event = json!({
        "schema_version": "boring-cdc/event/v1",
        "event_type": "mutation",
        "connector_event_id": event_id,
        "journal_seq": journal_seq,
        "capture_epoch": source.capture_epoch().get(),
        "source_version": {
            "lsn_u64": source.commit_lsn().get(),
            "origin_rank": version.origin_rank(),
            "transaction_ordinal": source.ordinal(),
            "mutation_ordinal": version.mutation_ordinal(),
            "connector_event_id": event_id,
        },
        "routing": "business",
        "operation": operation,
        "logical_table_id": hex(&payload.relation.logical_table.fingerprint().bytes()),
        "relation_fingerprint": hex(&payload.relation.fingerprint().bytes()),
        "key_hash": hex(&key_hash.bytes()),
        "canonical_key": key,
        "before_key": before_key,
        "mutation_kind": mutation_kind,
        "columns": columns,
        "payload_hash": canonical_payload_hash,
    });
    let json = serde_json::to_vec(&event).map_err(|_| EnvelopeError::Invalid)?;
    if json.len() > MAX_EVENT_BYTES {
        return Err(EnvelopeError::Limit);
    }
    Ok(MutationEnvelope {
        json,
        canonical_preimage,
        canonical_payload_hash,
    })
}

fn columns_json(columns: &[ColumnState]) -> Result<Vec<Value>, EnvelopeError> {
    let mut row_bytes = 0_usize;
    let mut rendered = Vec::with_capacity(columns.len());
    for (index, state) in columns.iter().enumerate() {
        let column_id = u32::try_from(index).map_err(|_| EnvelopeError::Limit)?;
        rendered.push(match state {
            ColumnState::Absent => json!({"column_id":column_id,"state":"absent_for_schema"}),
            ColumnState::Null => json!({"column_id":column_id,"state":"explicit_null"}),
            ColumnState::UnchangedToast => json!({"column_id":column_id,"state":"unchanged_toast"}),
            ColumnState::Value(value) => {
                if value.bytes.len() > MAX_SCALAR_BYTES {
                    return Err(EnvelopeError::Limit);
                }
                row_bytes = row_bytes
                    .checked_add(value.bytes.len())
                    .ok_or(EnvelopeError::Limit)?;
                json!({
                    "column_id":column_id,
                    "state":"explicit_value",
                    "type_oid":value.type_oid,
                    "type_modifier":value.type_modifier,
                    "bytes":URL_SAFE_NO_PAD.encode(&value.bytes),
                })
            }
        });
    }
    if row_bytes > MAX_ROW_BYTES {
        return Err(EnvelopeError::Limit);
    }
    Ok(rendered)
}

fn key_json(
    key: &[CanonicalKeyComponent],
    type_modifiers: &[i32],
) -> Result<Vec<Value>, EnvelopeError> {
    if key.len() != type_modifiers.len() || !(1..=8).contains(&key.len()) {
        return Err(EnvelopeError::Invalid);
    }
    key.iter()
        .zip(type_modifiers)
        .map(|(component, type_modifier)| {
            let oid = component.type_oid();
            let bytes = component.canonical_bytes();
            let base = |kind: &str, value: Value| {
                json!({
                    "kind":kind,"type_oid":oid,"type_modifier":type_modifier,"value":value,
                })
            };
            Ok(match oid {
                21 | 23 | 20 => {
                    let mut raw = bytes.to_vec();
                    raw[0] ^= 0x80;
                    let value = match oid {
                        21 => i64::from(i16::from_be_bytes(
                            raw.try_into().map_err(|_| EnvelopeError::Invalid)?,
                        )),
                        23 => i64::from(i32::from_be_bytes(
                            raw.try_into().map_err(|_| EnvelopeError::Invalid)?,
                        )),
                        _ => {
                            i64::from_be_bytes(raw.try_into().map_err(|_| EnvelopeError::Invalid)?)
                        }
                    };
                    base("int64", json!(value))
                }
                25 | 1043 | 1042 => base(
                    "text",
                    json!(std::str::from_utf8(bytes).map_err(|_| EnvelopeError::Invalid)?),
                ),
                17 | 2950 | 1700 | 1082 | 1114 | 1184 => {
                    base("bytes", json!(URL_SAFE_NO_PAD.encode(bytes)))
                }
                _ => return Err(EnvelopeError::Invalid),
            })
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m1_decoder::{Column, Relation, TupleValue};
    use crate::m1_ordering::{
        CanonicalValue, SourceSlotIdentity, WalIdentityInput, source_version_for_row,
    };
    use crate::m1_source_identity::{
        CanonicalKeyComponent, Fingerprint, LogicalTableIdentity, RelationSchemaVersion,
        SourceIdentity, supported_protocol_fingerprint,
    };
    use crate::m1_transition_kernel::{CaptureEpoch, ReceivedLsn};

    #[test]
    fn rendered_mutation_matches_m0_golden_envelope_and_hash() {
        let vectors: Value = serde_json::from_str(include_str!(
            "../fixtures/m0/event-format/golden-vectors.json"
        ))
        .unwrap();
        let expected = &vectors["vectors"][0]["event"];
        let source = SourceIdentity {
            system_identifier: 72_623_859_790_382_856,
            timeline: 1,
            database_identity: 16_384,
            slot_name: "boring_cdc".into(),
            plugin: "pgoutput".into(),
            publication_fingerprint: Fingerprint::digest(b"publication"),
            protocol_fingerprint: supported_protocol_fingerprint(),
        };
        let table = LogicalTableIdentity::derive(&source, "public", "orders");
        let relation = RelationSchemaVersion::derive(
            table,
            24_576,
            include_bytes!("../tests/fixtures/m1_ordering/relation-contract.bin"),
        )
        .unwrap();
        let epoch = CaptureEpoch::from_store(7);
        let source_version =
            source_version_for_row(epoch, ReceivedLsn::from_wire(112), 0, 0).unwrap();
        let version = MutationVersion::from_wal(
            source_version,
            WalIdentityInput {
                capture_epoch: epoch,
                source_slot_identity: SourceSlotIdentity::derive(&source),
                transaction_end_lsn: ReceivedLsn::from_wire(120),
                row_ordinal: 0,
                mutation_ordinal: 0,
            },
        )
        .unwrap();
        let mut key_bytes = 42_i64.to_be_bytes();
        key_bytes[0] ^= 0x80;
        let payload = MutationPayload {
            relation,
            key: vec![CanonicalKeyComponent::from_canonical(20, key_bytes.to_vec()).unwrap()],
            operation: SourceOperation::Insert,
            before_key: None,
            kind: MutationKind::Upsert,
            columns: vec![ColumnState::Value(CanonicalValue {
                type_oid: 20,
                type_modifier: -1,
                bytes: 42_i64.to_be_bytes().to_vec(),
            })],
        };
        let rendered = render_mutation(&version, &payload, &[-1], 1).unwrap();
        let mut expected = expected.clone();
        expected.as_object_mut().unwrap().remove("captured_at");
        assert_eq!(
            serde_json::from_slice::<Value>(&rendered.json).unwrap(),
            expected
        );
        assert_eq!(rendered.canonical_payload_hash, expected["payload_hash"]);
        assert_eq!(
            format!("{:x}", sha2::Sha256::digest(&rendered.canonical_preimage)),
            rendered.canonical_payload_hash
        );
        let later = render_mutation(&version, &payload, &[-1], 2).unwrap();
        assert_eq!(
            later.canonical_payload_hash,
            rendered.canonical_payload_hash
        );
        assert_ne!(later.json, rendered.json);
        assert!(matches!(
            render_mutation(&version, &payload, &[], 1),
            Err(EnvelopeError::Invalid)
        ));

        let contract = RelationContract {
            relation: Relation {
                id: 24_576,
                namespace: "public".into(),
                name: "orders".into(),
                replica_identity: b'd',
                columns: vec![Column {
                    key: true,
                    name: "id".into(),
                    type_oid: 20,
                    type_modifier: -1,
                }],
            },
            key_columns: vec![0],
            control: None,
        };
        let row = RowChange {
            xid: 0,
            ordinal: 0,
            relation_id: 24_576,
            kind: RowKind::Insert,
            old_kind: None,
            old: None,
            new: Some(vec![TupleValue::Text(b"42".to_vec())]),
        };
        let templates = render_wal_row_templates(
            &source,
            epoch,
            &contract,
            relation,
            &row,
            ReceivedLsn::from_wire(112),
            ReceivedLsn::from_wire(120),
        )
        .unwrap();
        assert_eq!(templates.len(), 1);
        let mut from_row: Value = serde_json::from_slice(&templates[0].envelope.json).unwrap();
        assert_eq!(from_row["journal_seq"], 0);
        from_row["journal_seq"] = json!(1);
        assert_eq!(from_row, expected);

        let changed = RowChange {
            kind: RowKind::Update,
            old_kind: Some(OldTupleKind::Key),
            old: Some(vec![TupleValue::Text(b"42".to_vec())]),
            new: Some(vec![TupleValue::Text(b"43".to_vec())]),
            ..row
        };
        let pair = render_wal_row_templates(
            &source,
            epoch,
            &contract,
            relation,
            &changed,
            ReceivedLsn::from_wire(112),
            ReceivedLsn::from_wire(120),
        )
        .unwrap();
        assert_eq!(pair.len(), 2);
        assert_eq!(
            pair.iter()
                .map(|mutation| mutation.source_mutation_ordinal)
                .collect::<Vec<_>>(),
            [0, 1]
        );
        let old: Value = serde_json::from_slice(&pair[0].envelope.json).unwrap();
        let new: Value = serde_json::from_slice(&pair[1].envelope.json).unwrap();
        assert_eq!(old["mutation_kind"], "delete");
        assert_eq!(old["canonical_key"][0]["value"], 42);
        assert_eq!(new["mutation_kind"], "upsert");
        assert_eq!(new["canonical_key"][0]["value"], 43);
        assert_eq!(new["before_key"][0]["value"], 42);
        assert_ne!(pair[0].event_id, pair[1].event_id);

        for (index, kind, generation, row_ordinal) in [
            (12, "heartbeat", None, 5),
            (13, "capture_fence", Some(2), 6),
        ] {
            let (_, control) = render_control_template(
                &source,
                epoch,
                ControlPosition {
                    kind,
                    generation,
                    xid: 0,
                    row_ordinal,
                    commit_lsn: ReceivedLsn::from_wire(124),
                    end_lsn: ReceivedLsn::from_wire(128),
                },
            )
            .unwrap();
            let mut expected = vectors["vectors"][index]["event"].clone();
            expected.as_object_mut().unwrap().remove("captured_at");
            expected["journal_seq"] = json!(0);
            assert_eq!(
                serde_json::from_slice::<Value>(&control.json).unwrap(),
                expected
            );
            assert_eq!(control.canonical_payload_hash, expected["payload_hash"]);
        }
    }

    #[test]
    fn snapshot_row_uses_m1_identity_and_precedes_equal_lsn_wal() {
        let source = SourceIdentity {
            system_identifier: 72_623_859_790_382_856,
            timeline: 1,
            database_identity: 16_384,
            slot_name: "boring_cdc".into(),
            plugin: "pgoutput".into(),
            publication_fingerprint: Fingerprint::digest(b"publication"),
            protocol_fingerprint: supported_protocol_fingerprint(),
        };
        let table = LogicalTableIdentity::derive(&source, "public", "orders");
        let relation = RelationSchemaVersion::derive(
            table,
            24_576,
            include_bytes!("../tests/fixtures/m1_ordering/relation-contract.bin"),
        )
        .unwrap();
        let contract = RelationContract {
            relation: Relation {
                id: 24_576,
                namespace: "public".into(),
                name: "orders".into(),
                replica_identity: b'd',
                columns: vec![Column {
                    key: true,
                    name: "id".into(),
                    type_oid: 20,
                    type_modifier: -1,
                }],
            },
            key_columns: vec![0],
            control: None,
        };
        let tuple = [TupleValue::Text(b"42".to_vec())];
        let epoch = CaptureEpoch::from_store(7);
        let lsn = ReceivedLsn::from_wire(112);
        let (id, envelope) = render_snapshot_row(
            epoch,
            DestinationGeneration::from_store(3),
            &contract,
            relation,
            lsn,
            5,
            &tuple,
        )
        .unwrap();
        let event: Value = serde_json::from_slice(&envelope.json).unwrap();
        let key = canonical_key(&contract, &tuple, false).unwrap();
        let expected_id = crate::m1_ordering::snapshot_connector_event_id(&SnapshotIdentityInput {
            capture_epoch: epoch,
            generation: DestinationGeneration::from_store(3),
            logical_table_id: table,
            chunk_id: 5,
            key,
        })
        .unwrap()
        .hex();
        assert_eq!(id, expected_id);
        assert_eq!(event["operation"], "snapshot");
        assert_eq!(event["journal_seq"], 0);
        assert_eq!(event["source_version"]["lsn_u64"], 112);
        assert_eq!(event["source_version"]["origin_rank"], 0);
        assert_eq!(event["source_version"]["transaction_ordinal"], 0);
        assert_eq!(event["payload_hash"], envelope.canonical_payload_hash);
        let (other_id, _) = render_snapshot_row(
            epoch,
            DestinationGeneration::from_store(3),
            &contract,
            relation,
            lsn,
            6,
            &tuple,
        )
        .unwrap();
        assert_ne!(id, other_id);
    }
}
