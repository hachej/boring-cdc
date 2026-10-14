//! M0 mutation envelope rendering from admitted M1 identities and canonical values.
//!
//! The journal assigns `journal_seq` atomically. Use `render_mutation_template` before its
//! commit; the journal binds the sequence to the stored JSON. The sequence is excluded from
//! the stable M0 field hash.

use crate::m1_ordering::{
    ColumnState, MutationKind, MutationPayload, MutationVersion, SourceOperation,
};
use crate::m1_source_identity::{CanonicalKeyComponent, PhysicalKeyHash};
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
    }
}
