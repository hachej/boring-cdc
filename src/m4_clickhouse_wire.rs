//! Typed rows at the pinned ClickHouse RowBinary boundary.

use crate::m1_source_identity::{CanonicalKeyComponent, PhysicalKeyHash, canonical_key_encoding};
use crate::m4_clickhouse_adapter::{ClickHouseWire, RedactedWireError, WireRequest, WireResponse};
use crate::m4_clickhouse_durability::RemoteBatch;
use crate::m4_clickhouse_durability::{BatchMarker, DestinationEvent, RemoteEventIdentity};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::runtime::{Builder, Runtime};

use crate::m4_clickhouse_adapter::SYNCHRONOUS_QUERY_SETTINGS;
use crate::m4_clickhouse_schema::{BATCH_MARKERS, EVENT_HISTORY};

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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredKeyComponent {
    kind: String,
    type_oid: u32,
    type_modifier: i32,
    value: Value,
}

impl EventHistoryRow {
    /// Derive the M0 payload bytes from server-returned columns. The stored hash is checked
    /// against this reconstruction; it is never used as a substitute for the actual fields.
    pub fn remote_identity(&self) -> Result<RemoteEventIdentity, RedactedWireError> {
        let canonical_payload = self.canonical_preimage()?;
        if self.payload_hash != hex64_bytes(&Sha256::digest(&canonical_payload)) {
            return Err(RedactedWireError);
        }
        Ok(RemoteEventIdentity {
            connector_event_id: ascii_hex(&self.connector_event_id)?,
            payload_hash: ascii_hex(&self.payload_hash)?,
            canonical_payload,
        })
    }

    fn canonical_preimage(&self) -> Result<Vec<u8>, RedactedWireError> {
        let event_id = decode_hex32(&self.connector_event_id)?;
        let relation = decode_hex32(&self.relation_schema_fingerprint)?;
        let stored_key = decode_key(&self.canonical_key)?;
        let key = canonical_key_encoding(&stored_key).map_err(|_| RedactedWireError)?;
        let key_hash = PhysicalKeyHash::derive(&stored_key)
            .map_err(|_| RedactedWireError)?
            .bytes();
        if self.key_hash != hex64_bytes(&key_hash) {
            return Err(RedactedWireError);
        }
        let before_key = if self.before_key.is_empty() {
            Vec::new()
        } else {
            let before = decode_key(&self.before_key)?;
            canonical_key_encoding(&before).map_err(|_| RedactedWireError)?
        };
        let operation = u8::try_from(self.operation).map_err(|_| RedactedWireError)?;
        let mutation_kind = u8::try_from(self.mutation_kind).map_err(|_| RedactedWireError)?;
        if operation > 3
            || mutation_kind > 1
            || self.origin_rank > 1
            || (operation == 0) != (self.origin_rank == 0)
        {
            return Err(RedactedWireError);
        }
        let transaction_ordinal =
            u32::try_from(self.transaction_ordinal).map_err(|_| RedactedWireError)?;
        let mut columns = Vec::new();
        columns.extend((self.columns.len() as u64).to_be_bytes());
        for (index, (column_id, state, oid, modifier, encoded)) in self.columns.iter().enumerate() {
            if usize::try_from(*column_id).ok() != Some(index) || !(0..=3).contains(state) {
                return Err(RedactedWireError);
            }
            columns.extend(column_id.to_be_bytes());
            columns.push(*state as u8);
            if *state == 3 {
                let bytes = URL_SAFE_NO_PAD
                    .decode(encoded)
                    .map_err(|_| RedactedWireError)?;
                if URL_SAFE_NO_PAD.encode(&bytes) != *encoded {
                    return Err(RedactedWireError);
                }
                columns.extend(oid.to_be_bytes());
                columns.extend(modifier.to_be_bytes());
                columns.extend((bytes.len() as u64).to_be_bytes());
                columns.extend(bytes);
            } else if *oid != 0 || *modifier != 0 || !encoded.is_empty() {
                return Err(RedactedWireError);
            }
        }
        let mut canonical_payload = Vec::new();
        for field in [
            b"boring-cdc/mutation-payload/v1".as_slice(),
            &self.capture_epoch.to_be_bytes(),
            &self.lsn_u64.to_be_bytes(),
            &[self.origin_rank],
            &transaction_ordinal.to_be_bytes(),
            &[self.mutation_ordinal],
            &event_id,
            &relation,
            &key,
            &key_hash,
            &[operation],
            &before_key,
            &[mutation_kind],
            &columns,
        ] {
            canonical_payload.extend((field.len() as u64).to_be_bytes());
            canonical_payload.extend(field);
        }
        Ok(canonical_payload)
    }
}

fn decode_key(encoded: &str) -> Result<Vec<CanonicalKeyComponent>, RedactedWireError> {
    let parts: Vec<StoredKeyComponent> =
        serde_json::from_str(encoded).map_err(|_| RedactedWireError)?;
    if !(1..=8).contains(&parts.len()) {
        return Err(RedactedWireError);
    }
    parts
        .into_iter()
        .map(|part| {
            if part.type_modifier < -1 {
                return Err(RedactedWireError);
            }
            let bytes = match (part.kind.as_str(), part.type_oid) {
                ("int64", 21 | 23 | 20) => {
                    let value = part.value.as_i64().ok_or(RedactedWireError)?;
                    let mut bytes = match part.type_oid {
                        21 => i16::try_from(value)
                            .map_err(|_| RedactedWireError)?
                            .to_be_bytes()
                            .to_vec(),
                        23 => i32::try_from(value)
                            .map_err(|_| RedactedWireError)?
                            .to_be_bytes()
                            .to_vec(),
                        _ => value.to_be_bytes().to_vec(),
                    };
                    bytes[0] ^= 0x80;
                    bytes
                }
                ("text", 25 | 1042 | 1043) => part
                    .value
                    .as_str()
                    .ok_or(RedactedWireError)?
                    .as_bytes()
                    .to_vec(),
                ("bytes", 17 | 2950 | 1700 | 1082 | 1114 | 1184) => {
                    let encoded = part.value.as_str().ok_or(RedactedWireError)?;
                    let bytes = URL_SAFE_NO_PAD
                        .decode(encoded)
                        .map_err(|_| RedactedWireError)?;
                    if URL_SAFE_NO_PAD.encode(&bytes) != encoded {
                        return Err(RedactedWireError);
                    }
                    bytes
                }
                _ => return Err(RedactedWireError),
            };
            CanonicalKeyComponent::from_canonical(part.type_oid, bytes)
                .map_err(|_| RedactedWireError)
        })
        .collect()
}

fn decode_hex32(ascii: &[u8; 64]) -> Result<[u8; 32], RedactedWireError> {
    let mut bytes = [0; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let pair =
            std::str::from_utf8(&ascii[index * 2..index * 2 + 2]).map_err(|_| RedactedWireError)?;
        *byte = u8::from_str_radix(pair, 16).map_err(|_| RedactedWireError)?;
    }
    if hex64_bytes(&bytes) != *ascii {
        return Err(RedactedWireError);
    }
    Ok(bytes)
}

fn hex64_bytes(bytes: &[u8]) -> [u8; 64] {
    let mut ascii = [0; 64];
    for (index, byte) in bytes.iter().enumerate() {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        ascii[index * 2] = DIGITS[usize::from(byte >> 4)];
        ascii[index * 2 + 1] = DIGITS[usize::from(byte & 15)];
    }
    ascii
}

fn ascii_hex(ascii: &[u8; 64]) -> Result<String, RedactedWireError> {
    std::str::from_utf8(ascii)
        .map(str::to_owned)
        .map_err(|_| RedactedWireError)
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Row, Serialize)]
pub struct BatchMarkerRow {
    pub capture_epoch: u64,
    pub generation: u64,
    #[serde(with = "fixed64")]
    pub batch_id: [u8; 64],
    pub first_journal_seq: u64,
    pub last_journal_seq: u64,
    pub event_count: u64,
    #[serde(with = "fixed64")]
    pub ordered_event_digest: [u8; 64],
    #[serde(with = "fixed64")]
    pub object_fingerprint: [u8; 64],
    pub finalized_at_unix_ms: u64,
}

impl BatchMarkerRow {
    pub fn from_marker(
        marker: &BatchMarker,
        finalized_at_unix_ms: u64,
    ) -> Result<Self, RedactedWireError> {
        if marker.first_journal_seq == 0
            || marker.first_journal_seq > marker.last_journal_seq
            || marker.capture_epoch == 0
            || marker.generation == 0
        {
            return Err(RedactedWireError);
        }
        Ok(Self {
            capture_epoch: marker.capture_epoch,
            generation: marker.generation,
            batch_id: fixed_hex(&marker.batch_id)?,
            first_journal_seq: marker.first_journal_seq,
            last_journal_seq: marker.last_journal_seq,
            event_count: marker.event_count,
            ordered_event_digest: fixed_hex(&marker.ordered_event_digest)?,
            object_fingerprint: fixed_hex(&marker.object_fingerprint)?,
            finalized_at_unix_ms,
        })
    }

    fn to_marker(&self) -> Result<BatchMarker, RedactedWireError> {
        Ok(BatchMarker {
            capture_epoch: self.capture_epoch,
            generation: self.generation,
            batch_id: ascii_hex(&self.batch_id)?,
            first_journal_seq: self.first_journal_seq,
            last_journal_seq: self.last_journal_seq,
            event_count: self.event_count,
            ordered_event_digest: ascii_hex(&self.ordered_event_digest)?,
            object_fingerprint: ascii_hex(&self.object_fingerprint)?,
        })
    }
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

    pub fn insert_marker_row(&mut self, row: &BatchMarkerRow) -> Result<(), RedactedWireError> {
        let mut client = self.client.clone();
        for (name, value) in SYNCHRONOUS_QUERY_SETTINGS {
            client = client.with_option(name, value.to_string());
        }
        self.runtime.block_on(async {
            let mut insert = client
                .insert(BATCH_MARKERS)
                .map_err(|_| RedactedWireError)?;
            insert.write(row).await.map_err(|_| RedactedWireError)?;
            insert.end().await.map_err(|_| RedactedWireError)
        })
    }

    pub fn read_marker_rows(
        &mut self,
        marker: &BatchMarker,
    ) -> Result<Vec<BatchMarkerRow>, RedactedWireError> {
        self.runtime.block_on(async {
            self.client
                .query("SELECT ?fields FROM boring_cdc.batch_markers_v1 WHERE capture_epoch = ? AND generation = ? AND batch_id = ?")
                .bind(marker.capture_epoch)
                .bind(marker.generation)
                .bind(&marker.batch_id)
                .fetch_all::<BatchMarkerRow>()
                .await
                .map_err(|_| RedactedWireError)
        })
    }
}

impl ClickHouseWire for PinnedClickHouseWire {
    fn execute(&mut self, request: WireRequest<'_>) -> Result<WireResponse, RedactedWireError> {
        match request {
            WireRequest::InsertEvents { batch, settings } => {
                if settings != SYNCHRONOUS_QUERY_SETTINGS {
                    return Err(RedactedWireError);
                }
                let rows = batch
                    .events
                    .iter()
                    .map(|event| EventHistoryRow::from_destination_event(event, &batch.marker))
                    .collect::<Result<Vec<_>, _>>()?;
                self.insert_history_rows(&rows)?;
                Ok(WireResponse::Acknowledged)
            }
            WireRequest::ReadEventsFresh { batch } => {
                let events = self
                    .read_history_batch(&batch.marker)?
                    .iter()
                    .map(EventHistoryRow::remote_identity)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(WireResponse::Events(events))
            }
            WireRequest::InsertMarker { marker, settings } => {
                if settings != SYNCHRONOUS_QUERY_SETTINGS {
                    return Err(RedactedWireError);
                }
                let elapsed = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|_| RedactedWireError)?;
                let finalized_at_unix_ms =
                    u64::try_from(elapsed.as_millis()).map_err(|_| RedactedWireError)?;
                self.insert_marker_row(&BatchMarkerRow::from_marker(
                    marker,
                    finalized_at_unix_ms,
                )?)?;
                Ok(WireResponse::Acknowledged)
            }
            WireRequest::ReadBatchFresh { marker } => {
                let rows = self.read_marker_rows(marker)?;
                let Some(first) = rows.first() else {
                    return Ok(WireResponse::Batch(None));
                };
                let stored = first.to_marker()?;
                if stored != *marker
                    || rows
                        .iter()
                        .skip(1)
                        .any(|row| row.to_marker().ok() != Some(stored.clone()))
                {
                    return Err(RedactedWireError);
                }
                let events = self
                    .read_history_batch(marker)?
                    .iter()
                    .map(EventHistoryRow::remote_identity)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(WireResponse::Batch(Some(RemoteBatch {
                    marker: stored,
                    events,
                })))
            }
            WireRequest::InspectContractFresh | WireRequest::ReadAuditRangeFresh { .. } => {
                Err(RedactedWireError)
            }
        }
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
    use crate::m1_ordering::{
        CanonicalValue, ColumnState, MutationKind, MutationPayload, MutationVersion,
        SourceOperation, SourceSlotIdentity, WalIdentityInput, source_version_for_row,
    };
    use crate::m1_source_identity::{
        Fingerprint, LogicalTableIdentity, RelationSchemaVersion, SourceIdentity,
        supported_protocol_fingerprint,
    };
    use crate::m1_transition_kernel::{CaptureEpoch, ReceivedLsn};
    use crate::m2_event_envelope::render_mutation;
    use crate::m4_clickhouse_durability::PreparedBatch;
    use serde_json::json;

    fn golden_preimage() -> Vec<u8> {
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
        render_mutation(&version, &payload, &[-1], 1)
            .unwrap()
            .canonical_preimage
    }

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
            canonical_payload: golden_preimage(),
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
        let key = decode_key(&row.canonical_key).unwrap();
        assert_eq!(
            hex64_bytes(&PhysicalKeyHash::derive(&key).unwrap().bytes()),
            row.key_hash
        );
        assert_eq!(row.canonical_preimage().unwrap(), golden_preimage());
        let remote = row.remote_identity().unwrap();
        assert_eq!(remote.payload_hash, event.payload_hash);
        assert_eq!(
            format!("{:x}", Sha256::digest(&remote.canonical_payload)),
            event.payload_hash
        );
        let mut corrupted = row.clone();
        corrupted.columns[0].4 = "AA".into();
        assert_eq!(corrupted.remote_identity(), Err(RedactedWireError));
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
        let batch = PreparedBatch {
            intent_id: marker.batch_id.clone(),
            destination_id: "clickhouse".into(),
            capture_epoch_text: "7".into(),
            configuration_fingerprint: "config".into(),
            lease_id: "lease".into(),
            run_id: "run".into(),
            complete_transaction_id: "tx".into(),
            events: vec![event.clone()],
            marker: marker.clone(),
        };
        assert_eq!(
            wire.execute(WireRequest::InsertEvents {
                batch: &batch,
                settings: &SYNCHRONOUS_QUERY_SETTINGS,
            })
            .unwrap(),
            WireResponse::Acknowledged
        );
        let stored = wire.read_history_batch(&marker).unwrap();
        assert_eq!(stored, vec![row]);
        assert_eq!(
            stored[0].remote_identity().unwrap().payload_hash,
            event.payload_hash
        );
        assert_eq!(
            wire.execute(WireRequest::InsertMarker {
                marker: &marker,
                settings: &SYNCHRONOUS_QUERY_SETTINGS,
            })
            .unwrap(),
            WireResponse::Acknowledged
        );
        let WireResponse::Batch(Some(remote)) = wire
            .execute(WireRequest::ReadBatchFresh { marker: &marker })
            .unwrap()
        else {
            panic!("missing marker readback")
        };
        assert_eq!(remote.marker, marker);
        assert_eq!(remote.events[0].canonical_payload, event.canonical_payload);
    }

    #[test]
    #[ignore = "requires the preceding live insert and a restarted pinned ClickHouse server"]
    fn live_rowbinary_history_survives_server_restart() {
        let (event, marker) = golden_event();
        let row = EventHistoryRow::from_destination_event(&event, &marker).unwrap();
        let mut wire = PinnedClickHouseWire::new(live_client()).unwrap();
        let stored = wire.read_history_batch(&marker).unwrap();
        assert_eq!(stored, vec![row]);
        assert_eq!(
            stored[0].remote_identity().unwrap().payload_hash,
            event.payload_hash
        );
        let markers = wire.read_marker_rows(&marker).unwrap();
        assert_eq!(markers.len(), 1);
        assert_eq!(markers[0].to_marker().unwrap(), marker);
        assert!(matches!(
            wire.execute(WireRequest::ReadBatchFresh { marker: &marker }),
            Ok(WireResponse::Batch(Some(_)))
        ));
    }
}
