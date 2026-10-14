//! Typed rows at the pinned ClickHouse RowBinary boundary.

use crate::m4_clickhouse_adapter::RedactedWireError;
use crate::m4_clickhouse_durability::{BatchMarker, DestinationEvent};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::runtime::{Builder, Runtime};

use crate::m4_clickhouse_adapter::SYNCHRONOUS_QUERY_SETTINGS;
use crate::m4_clickhouse_schema::EVENT_HISTORY;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Row, Serialize)]
pub struct EventHistoryRow {
    pub capture_epoch: u64,
    pub generation: u64,
    #[serde(with = "fixed64")]
    pub logical_table_id: [u8; 64],
    #[serde(with = "fixed64")]
    pub relation_schema_fingerprint: [u8; 64],
    pub canonical_key: String,
    #[serde(with = "fixed64")]
    pub key_hash: [u8; 64],
    pub before_key: String,
    #[serde(with = "fixed64")]
    pub connector_event_id: [u8; 64],
    #[serde(with = "fixed64")]
    pub payload_hash: [u8; 64],
    pub operation: i8,
    pub mutation_kind: i8,
    pub lsn_u64: u64,
    pub origin_rank: u8,
    pub transaction_ordinal: u64,
    pub mutation_ordinal: u8,
    pub journal_seq: u64,
    #[serde(with = "fixed64")]
    pub batch_id: [u8; 64],
    pub columns: Vec<(u32, i8, u32, i32, String)>,
}

mod fixed64 {
    use serde::de::{Error as _, SeqAccess, Visitor};
    use serde::ser::SerializeTuple;
    use serde::{Deserializer, Serializer};
    use std::fmt;

    pub fn serialize<S: Serializer>(value: &[u8; 64], serializer: S) -> Result<S::Ok, S::Error> {
        let mut tuple = serializer.serialize_tuple(64)?;
        for byte in value {
            tuple.serialize_element(byte)?;
        }
        tuple.end()
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<[u8; 64], D::Error> {
        struct Fixed64;
        impl<'de> Visitor<'de> for Fixed64 {
            type Value = [u8; 64];

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("exactly 64 fixed bytes")
            }

            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut value = [0; 64];
                for (index, byte) in value.iter_mut().enumerate() {
                    *byte = sequence
                        .next_element()?
                        .ok_or_else(|| A::Error::invalid_length(index, &self))?;
                }
                Ok(value)
            }
        }
        deserializer.deserialize_tuple(64, Fixed64)
    }
}

/// The pinned RowBinary HTTP client. The synchronous outer boundary returns only after
/// `insert.end().await` has completed, so the M4 coordinator can keep its existing lease and
/// checkpoint order around the remote call.
pub struct PinnedClickHouseWire {
    client: Client,
    runtime: Runtime,
}

impl PinnedClickHouseWire {
    pub fn new(client: Client) -> Result<Self, RedactedWireError> {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| RedactedWireError)?;
        Ok(Self { client, runtime })
    }

    pub fn insert_history_rows(
        &mut self,
        rows: &[EventHistoryRow],
    ) -> Result<(), RedactedWireError> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut client = self.client.clone();
        for (name, value) in SYNCHRONOUS_QUERY_SETTINGS {
            client = client.with_option(name, value.to_string());
        }
        self.runtime.block_on(async {
            let mut insert = client
                .insert(EVENT_HISTORY)
                .map_err(|_| RedactedWireError)?;
            for row in rows {
                insert.write(row).await.map_err(|_| RedactedWireError)?;
            }
            insert.end().await.map_err(|_| RedactedWireError)
        })
    }

    pub fn read_history_batch(
        &mut self,
        marker: &BatchMarker,
    ) -> Result<Vec<EventHistoryRow>, RedactedWireError> {
        self.runtime.block_on(async {
            self.client
                .query("SELECT ?fields FROM boring_cdc.event_history_v1 WHERE capture_epoch = ? AND generation = ? AND batch_id = ?")
                .bind(marker.capture_epoch)
                .bind(marker.generation)
                .bind(&marker.batch_id)
                .fetch_all::<EventHistoryRow>()
                .await
                .map_err(|_| RedactedWireError)
        })
    }
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
            logical_table_id: fixed_hex(&envelope.logical_table_id)?,
            relation_schema_fingerprint: fixed_hex(&envelope.relation_fingerprint)?,
            canonical_key,
            key_hash: fixed_hex(&envelope.key_hash)?,
            before_key,
            connector_event_id: fixed_hex(&envelope.connector_event_id)?,
            payload_hash: fixed_hex(&envelope.payload_hash)?,
            operation,
            mutation_kind,
            lsn_u64: version.lsn_u64,
            origin_rank: version.origin_rank,
            transaction_ordinal: version.transaction_ordinal,
            mutation_ordinal: version.mutation_ordinal,
            journal_seq: event.journal_seq,
            batch_id: fixed_hex(&marker.batch_id)?,
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

fn fixed_hex(value: &str) -> Result<[u8; 64], RedactedWireError> {
    if !hex64(value) {
        return Err(RedactedWireError);
    }
    value.as_bytes().try_into().map_err(|_| RedactedWireError)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn live_client() -> Client {
        let url = std::env::var("BCDC_M4_CLICKHOUSE_URL").unwrap();
        let user = std::env::var("BCDC_M4_CLICKHOUSE_USER").unwrap();
        let password = std::env::var("BCDC_M4_CLICKHOUSE_PASSWORD").unwrap();
        Client::default()
            .with_url(&url)
            .with_user(user)
            .with_password(password)
    }

    fn golden_event() -> (DestinationEvent, BatchMarker) {
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
        (event, marker)
    }

    #[test]
    fn golden_m0_mutation_maps_to_the_closed_history_columns() {
        let (event, marker) = golden_event();
        let row = EventHistoryRow::from_destination_event(&event, &marker).unwrap();
        assert_eq!(row.capture_epoch, 7);
        assert_eq!(row.generation, 2);
        assert_eq!(row.operation, 1);
        assert_eq!(row.mutation_kind, 1);
        assert_eq!(row.columns.len(), 1);
        assert_eq!(row.columns[0].0, 0);
        assert_eq!(row.columns[0].1, 3);
        assert_eq!(row.columns[0].2, 20);
        assert_eq!(row.payload_hash.as_slice(), event.payload_hash.as_bytes());
        assert_eq!(row.batch_id.as_slice(), marker.batch_id.as_bytes());
        let malformed = DestinationEvent {
            payload: serde_json::to_vec(&json!({"event_type":"control"})).unwrap(),
            ..event
        };
        assert_eq!(
            EventHistoryRow::from_destination_event(&malformed, &marker),
            Err(RedactedWireError)
        );
    }

    #[test]
    #[ignore = "requires a pinned ClickHouse 25.8.2.29 instance with the M4 DDL"]
    fn live_rowbinary_history_insert_and_fresh_readback() {
        let (event, marker) = golden_event();
        let row = EventHistoryRow::from_destination_event(&event, &marker).unwrap();
        let mut wire = PinnedClickHouseWire::new(live_client()).unwrap();
        wire.insert_history_rows(std::slice::from_ref(&row))
            .unwrap();
        let stored = wire.read_history_batch(&marker).unwrap();
        assert_eq!(stored, vec![row]);
    }

    #[test]
    #[ignore = "requires the preceding live insert and a restarted pinned ClickHouse server"]
    fn live_rowbinary_history_survives_server_restart() {
        let (event, marker) = golden_event();
        let row = EventHistoryRow::from_destination_event(&event, &marker).unwrap();
        let mut wire = PinnedClickHouseWire::new(live_client()).unwrap();
        assert_eq!(wire.read_history_batch(&marker).unwrap(), vec![row]);
    }
}
