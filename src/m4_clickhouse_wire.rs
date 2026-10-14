//! Typed rows at the pinned ClickHouse RowBinary boundary.

use crate::m4_clickhouse_adapter::RedactedWireError;
use crate::m4_clickhouse_durability::{BatchMarker, DestinationEvent};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use clickhouse::Row;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Row, Serialize)]
pub struct EventHistoryRow {
    pub capture_epoch: u64,
    pub generation: u64,
    pub logical_table_id: String,
    pub relation_schema_fingerprint: String,
    pub canonical_key: String,
    pub key_hash: String,
    pub before_key: String,
    pub connector_event_id: String,
    pub payload_hash: String,
    pub operation: i8,
    pub mutation_kind: i8,
    pub lsn_u64: u64,
    pub origin_rank: u8,
    pub transaction_ordinal: u64,
    pub mutation_ordinal: u8,
    pub journal_seq: u64,
    pub batch_id: String,
    pub columns: Vec<(u32, i8, u32, i32, String)>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MutationEnvelope {
    schema_version: String,
    event_type: String,
    connector_event_id: String,
    journal_seq: u64,
    capture_epoch: u64,
    source_version: SourceVersion,
    routing: String,
    operation: String,
    logical_table_id: String,
    relation_fingerprint: String,
    key_hash: String,
    canonical_key: Vec<Value>,
    before_key: Option<Vec<Value>>,
    mutation_kind: String,
    columns: Vec<Column>,
    payload_hash: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceVersion {
    lsn_u64: u64,
    origin_rank: u8,
    transaction_ordinal: u64,
    mutation_ordinal: u8,
    connector_event_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Column {
    column_id: u32,
    state: String,
    type_oid: Option<u32>,
    type_modifier: Option<i32>,
    bytes: Option<String>,
}

impl EventHistoryRow {
    /// Reject an envelope whose persisted journal identity differs from its M0 fields before any
    /// remote insert. Control transactions have no history rows and never call this converter.
    pub fn from_destination_event(
        event: &DestinationEvent,
        marker: &BatchMarker,
    ) -> Result<Self, RedactedWireError> {
        let envelope: MutationEnvelope =
            serde_json::from_slice(&event.payload).map_err(|_| RedactedWireError)?;
        let version = envelope.source_version;
        if envelope.schema_version != "boring-cdc/event/v1"
            || envelope.event_type != "mutation"
            || envelope.routing != "business"
            || envelope.capture_epoch != marker.capture_epoch
            || envelope.journal_seq != event.journal_seq
            || envelope.connector_event_id != event.connector_event_id
            || version.connector_event_id != event.connector_event_id
            || version.transaction_ordinal != event.transaction_ordinal
            || u32::from(version.mutation_ordinal) != event.mutation_ordinal
            || envelope.relation_fingerprint != event.relation_schema_fingerprint
            || envelope.payload_hash != event.payload_hash
            || !hex64(&envelope.connector_event_id)
            || !hex64(&envelope.logical_table_id)
            || !hex64(&envelope.relation_fingerprint)
            || !hex64(&envelope.key_hash)
            || !hex64(&envelope.payload_hash)
            || !hex64(&marker.batch_id)
            || envelope.canonical_key.is_empty()
            || envelope.canonical_key.len() > 8
        {
            return Err(RedactedWireError);
        }
        let operation = match envelope.operation.as_str() {
            "snapshot" => 0,
            "insert" => 1,
            "update" => 2,
            "delete" => 3,
            _ => return Err(RedactedWireError),
        };
        if (operation == 0) != (version.origin_rank == 0) || version.origin_rank > 1 {
            return Err(RedactedWireError);
        }
        let mutation_kind = match envelope.mutation_kind.as_str() {
            "delete" => 0,
            "upsert" => 1,
            _ => return Err(RedactedWireError),
        };
        let canonical_key =
            serde_json::to_string(&envelope.canonical_key).map_err(|_| RedactedWireError)?;
        let before_key = envelope
            .before_key
            .map(|key| serde_json::to_string(&key).map_err(|_| RedactedWireError))
            .transpose()?
            .unwrap_or_default();
        let columns = envelope
            .columns
            .into_iter()
            .enumerate()
            .map(|(index, column)| {
                if usize::try_from(column.column_id).ok() != Some(index) {
                    return Err(RedactedWireError);
                }
                let (state, oid, modifier, bytes) = match column.state.as_str() {
                    "absent_for_schema" => (0, None, None, None),
                    "explicit_null" => (1, None, None, None),
                    "unchanged_toast" => (2, None, None, None),
                    "explicit_value" => (3, column.type_oid, column.type_modifier, column.bytes),
                    _ => return Err(RedactedWireError),
                };
                if state == 3 {
                    let (Some(oid), Some(modifier), Some(bytes)) = (oid, modifier, bytes) else {
                        return Err(RedactedWireError);
                    };
                    URL_SAFE_NO_PAD
                        .decode(&bytes)
                        .map_err(|_| RedactedWireError)?;
                    Ok((column.column_id, state, oid, modifier, bytes))
                } else if oid.is_none() && modifier.is_none() && bytes.is_none() {
                    Ok((column.column_id, state, 0, 0, String::new()))
                } else {
                    Err(RedactedWireError)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            capture_epoch: envelope.capture_epoch,
            generation: marker.generation,
            logical_table_id: envelope.logical_table_id,
            relation_schema_fingerprint: envelope.relation_fingerprint,
            canonical_key,
            key_hash: envelope.key_hash,
            before_key,
            connector_event_id: envelope.connector_event_id,
            payload_hash: envelope.payload_hash,
            operation,
            mutation_kind,
            lsn_u64: version.lsn_u64,
            origin_rank: version.origin_rank,
            transaction_ordinal: version.transaction_ordinal,
            mutation_ordinal: version.mutation_ordinal,
            journal_seq: event.journal_seq,
            batch_id: marker.batch_id.clone(),
            columns,
        })
    }
}

fn hex64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn golden_m0_mutation_maps_to_the_closed_history_columns() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../fixtures/m0/event-format/golden-vectors.json"
        ))
        .unwrap();
        let mut golden = fixture["vectors"][0]["event"].clone();
        golden.as_object_mut().unwrap().remove("captured_at");
        let marker = BatchMarker {
            capture_epoch: 7,
            generation: 2,
            batch_id: "a".repeat(64),
            first_journal_seq: 1,
            last_journal_seq: 1,
            event_count: 1,
            ordered_event_digest: "b".repeat(64),
            object_fingerprint: "c".repeat(64),
        };
        let event = DestinationEvent {
            journal_seq: 1,
            transaction_ordinal: golden["source_version"]["transaction_ordinal"]
                .as_u64()
                .unwrap(),
            mutation_ordinal: golden["source_version"]["mutation_ordinal"]
                .as_u64()
                .unwrap() as u32,
            connector_event_id: golden["connector_event_id"].as_str().unwrap().into(),
            relation_schema_fingerprint: golden["relation_fingerprint"].as_str().unwrap().into(),
            payload: serde_json::to_vec(&golden).unwrap(),
            payload_hash: golden["payload_hash"].as_str().unwrap().into(),
            canonical_payload: vec![],
        };
        let row = EventHistoryRow::from_destination_event(&event, &marker).unwrap();
        assert_eq!(row.capture_epoch, 7);
        assert_eq!(row.generation, 2);
        assert_eq!(row.operation, 1);
        assert_eq!(row.mutation_kind, 1);
        assert_eq!(row.columns.len(), 1);
        assert_eq!(row.columns[0].0, 0);
        assert_eq!(row.columns[0].1, 3);
        assert_eq!(row.columns[0].2, 20);
        assert_eq!(row.payload_hash, event.payload_hash);
        assert_eq!(row.batch_id, marker.batch_id);
        let malformed = DestinationEvent {
            payload: serde_json::to_vec(&json!({"event_type":"control"})).unwrap(),
            ..event
        };
        assert_eq!(
            EventHistoryRow::from_destination_event(&malformed, &marker),
            Err(RedactedWireError)
        );
    }
}
