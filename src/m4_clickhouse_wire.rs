//! Typed rows at the pinned ClickHouse RowBinary boundary.

use crate::m1_source_identity::{CanonicalKeyComponent, PhysicalKeyHash, canonical_key_encoding};
use crate::m4_clickhouse_adapter::{ClickHouseWire, RedactedWireError, WireRequest, WireResponse};
use crate::m4_clickhouse_audit::{
    AuditIdentity, DestinationContractObservation, StoredAuditBatch, StoredAuditEvent,
    marker_digest,
};
use crate::m4_clickhouse_durability::RemoteBatch;
use crate::m4_clickhouse_durability::{BatchMarker, DestinationEvent, RemoteEventIdentity};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tokio::runtime::{Builder, Runtime};

use crate::m4_clickhouse_adapter::SYNCHRONOUS_QUERY_SETTINGS;
use crate::m4_clickhouse_schema::{
    BATCH_MARKERS, EVENT_HISTORY, SERVER_VERSION, object_fingerprint,
};

const EXPECTED_OBJECTS: [(&str, &str, &str); 6] = [
    (
        "batch_markers_v1",
        "MergeTree",
        "7f95b63c458fd873cdfd178cc0c3eb93589cb599b18b67ea4b4cc16b284000a6",
    ),
    (
        "event_history_v1",
        "MergeTree",
        "e73c8f3d25cc9b492602bcc0c3f5bea8c5c31fe6e018f78450e50ee28b0e7857",
    ),
    (
        "event_identity_conflicts_v1",
        "View",
        "e16a288d2fc597d4258b4d7525c4e7d2f67afd1a63fb6f9b49b6af743fe286aa",
    ),
    (
        "generation_selectors_v1",
        "MergeTree",
        "51a51f357ca22db39280a8d0c6ca397aa78f63d145df85f85e9b6277792d86ea",
    ),
    (
        "live_generation_v1",
        "View",
        "efe75aec390cdb952546d59403a09bec91ee246c4e14b46bf0ed9f45b4fd5caa",
    ),
    (
        "selector_conflicts_v1",
        "View",
        "ddbc48206308d80c83e1e9ac8cbd34ea1a215ca436ca3e5d055550ddb124b81a",
    ),
];
const EXPECTED_COLUMNS_DIGEST: &str =
    "938f3680ed9451d6d24ca72c886cd62c9607dc66540bf7c860873754f55d52ed";
const EXPECTED_SETTINGS_DIGEST: &str =
    "8acec9cdf7de939b26e9882f3973a541686fc1ce80ec3e408ceef21e5f492225";

fn framed_hash(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
}

#[derive(Deserialize, Row)]
struct ServerVersionRow {
    version: String,
}

#[derive(Deserialize, Row)]
struct SystemTableRow {
    name: String,
    engine: String,
    create_table_query: String,
}

#[derive(Deserialize, Row)]
struct SystemColumnRow {
    table: String,
    name: String,
    r#type: String,
    position: u64,
    is_in_partition_key: u8,
    is_in_sorting_key: u8,
}

#[derive(Deserialize, Row)]
struct SystemSettingRow {
    name: String,
    value: String,
}

#[derive(Deserialize, Row)]
struct CountRow {
    count: u64,
}

#[derive(Deserialize, Row)]
struct SelectorRow {
    capture_epoch: u64,
    promotion_fence: u64,
    generation: u64,
    #[serde(with = "fixed64")]
    table_set_fingerprint: [u8; 64],
    #[serde(with = "fixed64")]
    compatible_anchor_id: [u8; 64],
    #[serde(with = "fixed64")]
    candidate_digest: [u8; 64],
}

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
            journal_seq: self.journal_seq,
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

    fn inspect_server_contract(&mut self) -> Result<(String, String), RedactedWireError> {
        let (version, tables, columns, settings) = self.runtime.block_on(async {
            let version = self
                .client
                .query("SELECT version() AS version")
                .fetch_one::<ServerVersionRow>()
                .await
                .map_err(|_| RedactedWireError)?;
            let tables = self
                .client
                .query("SELECT name,engine,create_table_query FROM system.tables WHERE database='boring_cdc' ORDER BY name")
                .fetch_all::<SystemTableRow>()
                .await
                .map_err(|_| RedactedWireError)?;
            let columns = self
                .client
                .query("SELECT table,name,type,position,is_in_partition_key,is_in_sorting_key FROM system.columns WHERE database='boring_cdc' ORDER BY table,position")
                .fetch_all::<SystemColumnRow>()
                .await
                .map_err(|_| RedactedWireError)?;
            let mut insert_client = self.client.clone();
            for (name, value) in SYNCHRONOUS_QUERY_SETTINGS {
                insert_client = insert_client.with_option(name, value.to_string());
            }
            let settings = insert_client
                .query("SELECT name,value FROM system.settings WHERE name IN ('async_insert','insert_deduplicate','insert_quorum','wait_for_async_insert') ORDER BY name")
                .fetch_all::<SystemSettingRow>()
                .await
                .map_err(|_| RedactedWireError)?;
            Ok::<_, RedactedWireError>((version, tables, columns, settings))
        })?;

        let tables_match = tables.len() == EXPECTED_OBJECTS.len()
            && tables.iter().zip(EXPECTED_OBJECTS).all(|(row, expected)| {
                row.name == expected.0
                    && row.engine == expected.1
                    && format!("{:x}", Sha256::digest(row.create_table_query.as_bytes()))
                        == expected.2
            });
        let mut columns_hash = Sha256::new();
        framed_hash(
            &mut columns_hash,
            b"boring-cdc/clickhouse-system-columns/v1",
        );
        for row in &columns {
            for field in [
                row.table.as_str(),
                row.name.as_str(),
                row.r#type.as_str(),
                &row.position.to_string(),
                &row.is_in_partition_key.to_string(),
                &row.is_in_sorting_key.to_string(),
            ] {
                framed_hash(&mut columns_hash, field.as_bytes());
            }
        }
        let objects_match = version.version == SERVER_VERSION
            && tables_match
            && columns.len() == 43
            && format!("{:x}", columns_hash.finalize()) == EXPECTED_COLUMNS_DIGEST;

        let expected_settings = [
            ("async_insert", "0"),
            ("insert_deduplicate", "0"),
            ("insert_quorum", "1"),
            ("wait_for_async_insert", "1"),
        ];
        let mut settings_hash = Sha256::new();
        framed_hash(&mut settings_hash, b"boring-cdc/clickhouse-settings/v1");
        framed_hash(&mut settings_hash, b"wait_end_of_query");
        framed_hash(&mut settings_hash, b"1");
        if SYNCHRONOUS_QUERY_SETTINGS
            != [
                ("async_insert", 0),
                ("wait_for_async_insert", 1),
                ("wait_end_of_query", 1),
                ("insert_quorum", 1),
                ("insert_deduplicate", 0),
            ]
            || settings.len() != expected_settings.len()
            || !settings
                .iter()
                .zip(expected_settings)
                .all(|(row, expected)| row.name == expected.0)
        {
            return Ok((String::new(), String::new()));
        }
        for row in &settings {
            framed_hash(&mut settings_hash, row.name.as_bytes());
            framed_hash(&mut settings_hash, row.value.as_bytes());
        }
        for table in [
            "batch_markers_v1",
            "event_history_v1",
            "generation_selectors_v1",
        ] {
            for field in [
                table,
                "fsync_after_insert",
                "1",
                "fsync_part_directory",
                "1",
            ] {
                framed_hash(&mut settings_hash, field.as_bytes());
            }
        }
        let observed_settings = format!("{:x}", settings_hash.finalize());
        Ok((
            if objects_match {
                object_fingerprint()
            } else {
                String::new()
            },
            observed_settings,
        ))
    }

    fn inspect_audit_contract(
        &mut self,
        identity: &AuditIdentity,
    ) -> Result<DestinationContractObservation, RedactedWireError> {
        let (object_fingerprint, settings_fingerprint) = self.inspect_server_contract()?;
        let epoch = identity
            .capture_epoch
            .parse::<u64>()
            .map_err(|_| RedactedWireError)?;
        let (selector_conflicts, selectors, event_conflicts, marker_conflicts) =
            self.runtime.block_on(async {
                let selector_conflicts = self.client.query("SELECT count() AS count FROM boring_cdc.selector_conflicts_v1 WHERE capture_epoch = ?")
                    .bind(epoch).fetch_one::<CountRow>().await.map_err(|_| RedactedWireError)?;
                let selectors = self.client.query("SELECT DISTINCT ?fields FROM boring_cdc.generation_selectors_v1 WHERE capture_epoch = ? AND promotion_fence = (SELECT max(promotion_fence) FROM boring_cdc.generation_selectors_v1 WHERE capture_epoch = ?) LIMIT 2")
                    .bind(epoch).bind(epoch).fetch_all::<SelectorRow>().await.map_err(|_| RedactedWireError)?;
                let event_conflicts = self.client.query("SELECT count() AS count FROM boring_cdc.event_identity_conflicts_v1 WHERE capture_epoch = ? AND generation = ?")
                    .bind(epoch).bind(identity.generation).fetch_one::<CountRow>().await.map_err(|_| RedactedWireError)?;
                let marker_conflicts = self.client.query("SELECT count() AS count FROM (SELECT batch_id FROM boring_cdc.batch_markers_v1 WHERE capture_epoch = ? AND generation = ? GROUP BY batch_id HAVING uniqExact(tuple(first_journal_seq,last_journal_seq,event_count,ordered_event_digest,object_fingerprint)) > 1)")
                    .bind(epoch).bind(identity.generation).fetch_one::<CountRow>().await.map_err(|_| RedactedWireError)?;
                Ok::<_, RedactedWireError>((selector_conflicts, selectors, event_conflicts, marker_conflicts))
            })?;
        let mut selector_fingerprint = String::new();
        let mut promotion_fence = 0;
        if selector_conflicts.count == 0 && selectors.len() == 1 {
            let row = &selectors[0];
            let fixed_fields = [
                row.table_set_fingerprint,
                row.compatible_anchor_id,
                row.candidate_digest,
            ];
            if fixed_fields.iter().all(|field| {
                std::str::from_utf8(field).is_ok_and(|value| value.is_ascii() && hex64(value))
            }) && row.capture_epoch == epoch
                && row.generation == identity.generation
                && row.promotion_fence > 0
            {
                let mut hash = Sha256::new();
                for field in [
                    b"boring-cdc/clickhouse-selector/v1".as_slice(),
                    epoch.to_string().as_bytes(),
                    row.promotion_fence.to_string().as_bytes(),
                    row.generation.to_string().as_bytes(),
                    &fixed_fields[0],
                    &fixed_fields[1],
                    &fixed_fields[2],
                ] {
                    framed_hash(&mut hash, field);
                }
                selector_fingerprint = format!("{:x}", hash.finalize());
                promotion_fence = row.promotion_fence;
            }
        }
        Ok(DestinationContractObservation {
            promotion_fence,
            selector_fingerprint,
            object_fingerprint,
            settings_fingerprint,
            expected_settings_fingerprint: EXPECTED_SETTINGS_DIGEST.into(),
            event_identity_conflicts: event_conflicts.count,
            marker_identity_conflicts: marker_conflicts.count,
        })
    }

    fn insert_history_rows(
        &mut self,
        batch_id: &str,
        rows: &[EventHistoryRow],
    ) -> Result<(), RedactedWireError> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut client = self.client.clone();
        for (name, value) in SYNCHRONOUS_QUERY_SETTINGS {
            client = client.with_option(name, value.to_string());
        }
        client = client.with_option("query_id", batch_id);
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

    fn read_audit_range(
        &mut self,
        identity: &AuditIdentity,
        first_seq: u64,
        last_seq: u64,
        max_events: usize,
    ) -> Result<StoredAuditBatch, RedactedWireError> {
        if first_seq > last_seq || max_events == 0 {
            return Err(RedactedWireError);
        }
        let capture_epoch = identity
            .capture_epoch
            .parse::<u64>()
            .map_err(|_| RedactedWireError)?;
        let limit = u64::try_from(max_events.checked_add(1).ok_or(RedactedWireError)?)
            .map_err(|_| RedactedWireError)?;
        let rows = self.runtime.block_on(async {
            self.client
                .query("SELECT ?fields FROM boring_cdc.event_history_v1 WHERE capture_epoch = ? AND generation = ? AND journal_seq >= ? AND journal_seq <= ? ORDER BY journal_seq LIMIT ?")
                .bind(capture_epoch)
                .bind(identity.generation)
                .bind(first_seq)
                .bind(last_seq)
                .bind(limit)
                .fetch_all::<EventHistoryRow>()
                .await
                .map_err(|_| RedactedWireError)
        })?;
        if rows.len() > max_events {
            return Err(RedactedWireError);
        }
        let mut events = Vec::with_capacity(rows.len());
        for row in rows {
            let remote = row.remote_identity()?;
            events.push(StoredAuditEvent {
                journal_seq: row.journal_seq,
                connector_event_id: remote.connector_event_id,
                stored_payload_hash: remote.payload_hash,
                reconstructed_payload: remote.canonical_payload,
            });
        }
        let indexed = events
            .iter()
            .map(|event| {
                (
                    event.journal_seq,
                    (
                        event.connector_event_id.as_str(),
                        event.stored_payload_hash.as_str(),
                        event.reconstructed_payload.as_slice(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        if indexed.len() != events.len() {
            return Err(RedactedWireError);
        }
        Ok(StoredAuditBatch {
            first_journal_seq: first_seq,
            last_journal_seq: last_seq,
            event_count: u64::try_from(events.len()).map_err(|_| RedactedWireError)?,
            marker_digest: marker_digest(first_seq, last_seq, &indexed),
            events,
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
                self.insert_history_rows(&batch.marker.batch_id, &rows)?;
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
            WireRequest::ReadAuditRangeFresh {
                identity,
                first_seq,
                last_seq,
                max_events,
            } => self
                .read_audit_range(identity, first_seq, last_seq, max_events)
                .map(WireResponse::AuditBatch),
            WireRequest::InspectObjectsFresh => {
                let (object, settings) = self.inspect_server_contract()?;
                if object.is_empty() || settings != EXPECTED_SETTINGS_DIGEST {
                    return Err(RedactedWireError);
                }
                Ok(WireResponse::ObjectFingerprint(object))
            }
            WireRequest::InspectAuditContractFresh { identity } => self
                .inspect_audit_contract(identity)
                .map(WireResponse::Contract),
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
    use crate::m2_journal::{CopiedEvent, CopiedRange};
    use crate::m2_schema::{WriterConnection, open_writer};
    use crate::m4_clickhouse_adapter::ProductionClickHouseAdapter;
    use crate::m4_clickhouse_durability::{
        BatchContext, ClickHouseBatchExecutor, PreparedBatch, Reconciliation,
        dispatch_and_readback, finalize_checkpoint, mark_insert_dispatched, prepare_batch,
        reconcile_remote,
    };
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

    fn coordinator_path() -> std::path::PathBuf {
        std::env::var("BCDC_M4_COORDINATOR_SQLITE").unwrap().into()
    }

    fn coordinator_context() -> BatchContext {
        BatchContext {
            destination_id: "clickhouse".into(),
            capture_epoch: "7".into(),
            generation: 3,
            configuration_fingerprint: "cfg".into(),
            lease_id: "lease".into(),
            run_id: "run".into(),
        }
    }

    fn coordinator_range() -> CopiedRange {
        let (event, _) = golden_event();
        let raw_hash = format!("{:x}", Sha256::digest(&event.payload));
        CopiedRange {
            events: vec![CopiedEvent {
                journal_seq: 1,
                transaction_id: "tx-live".into(),
                transaction_ordinal: 0,
                mutation_ordinal: 0,
                source_row_ordinal: 0,
                source_mutation_ordinal: 0,
                event_id: event.connector_event_id,
                relation_schema_fingerprint: Some(event.relation_schema_fingerprint),
                source_relation_id: Some("relation-live".into()),
                control_kind: None,
                payload: event.payload,
                payload_hash: raw_hash,
                canonical_payload: Some(event.canonical_payload),
                canonical_payload_hash: Some(event.payload_hash),
            }],
            first_seq: 1,
            last_seq: 1,
            copied_bytes: 512,
        }
    }

    fn coordinator_batch(writer: &mut WriterConnection) -> PreparedBatch {
        let transaction = writer.connection_mut().transaction().unwrap();
        let batch =
            prepare_batch(&transaction, &coordinator_context(), coordinator_range()).unwrap();
        transaction.commit().unwrap();
        batch
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

    fn live_audit_identity(marker: &BatchMarker) -> AuditIdentity {
        AuditIdentity {
            audit_id: "wire-live".into(),
            destination_id: "clickhouse".into(),
            configuration_fingerprint: "config".into(),
            capture_epoch: marker.capture_epoch.to_string(),
            generation: marker.generation,
            promotion_fence: 1,
            target_checkpoint: marker.last_journal_seq,
            selector_fingerprint: "selector".into(),
            object_fingerprint: marker.object_fingerprint.clone(),
            contract_digest: "contract".into(),
            freshness_started_at: "2026-10-14T00:00:00Z".into(),
            freshness_expires_at: "2026-10-15T00:00:00Z".into(),
            retained_history_start_seq: marker.first_journal_seq,
            expected_current_incarnation: None,
        }
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
    fn live_inspection_literals_match_the_m0_contract() {
        let model: Value =
            serde_json::from_str(include_str!("../contracts/clickhouse/model.json")).unwrap();
        let inspection = &model["live_inspection"];
        assert_eq!(inspection["server_version"].as_str(), Some(SERVER_VERSION));
        let expected = &inspection["object_metadata"]["expected"];
        assert_eq!(expected.as_object().unwrap().len(), EXPECTED_OBJECTS.len());
        for (name, engine, digest) in EXPECTED_OBJECTS {
            assert_eq!(expected[name]["engine"].as_str(), Some(engine));
            assert_eq!(expected[name]["create_query_sha256"].as_str(), Some(digest));
        }
        assert_eq!(
            inspection["object_metadata"]["columns_digest_sha256"].as_str(),
            Some(EXPECTED_COLUMNS_DIGEST)
        );
        assert_eq!(
            inspection["settings"]["expected_fingerprint_sha256"].as_str(),
            Some(EXPECTED_SETTINGS_DIGEST)
        );
        assert_eq!(
            model["audit"]["selector_fingerprint"]["domain"].as_str(),
            Some("boring-cdc/clickhouse-selector/v1")
        );
    }

    #[test]
    #[ignore = "requires pinned ClickHouse 25.8.2.29 with M4 DDL and the selector probe row"]
    fn live_contract_inspection_matches_pinned_objects_and_selector() {
        let mut wire = PinnedClickHouseWire::new(live_client()).unwrap();
        assert_eq!(
            wire.execute(WireRequest::InspectObjectsFresh).unwrap(),
            WireResponse::ObjectFingerprint(object_fingerprint())
        );
        let (_, marker) = golden_event();
        let mut identity = live_audit_identity(&marker);
        identity.capture_epoch = "1".into();
        identity.generation = 7;
        identity.promotion_fence = 9;
        identity.selector_fingerprint =
            "2d3882a0f2ad8eb3c95fc1d98de156a1093a4200a32275c46d926cecd1f8c86e".into();
        identity.object_fingerprint = object_fingerprint();
        let WireResponse::Contract(observed) = wire
            .execute(WireRequest::InspectAuditContractFresh {
                identity: &identity,
            })
            .unwrap()
        else {
            panic!("missing contract observation")
        };
        assert_eq!(observed.promotion_fence, identity.promotion_fence);
        assert_eq!(observed.selector_fingerprint, identity.selector_fingerprint);
        assert_eq!(observed.object_fingerprint, identity.object_fingerprint);
        assert_eq!(observed.settings_fingerprint, EXPECTED_SETTINGS_DIGEST);
        assert_eq!(observed.event_identity_conflicts, 0);
        assert_eq!(observed.marker_identity_conflicts, 0);
    }

    #[test]
    #[ignore = "requires pinned ClickHouse and a fresh coordinator SQLite path"]
    fn live_coordinator_persists_intent_and_marker_before_checkpoint() {
        let mut writer = open_writer(&coordinator_path(), "run", 1, 1_000).unwrap();
        writer.connection_mut().execute("INSERT INTO source_transactions(transaction_id,capture_epoch,source_system_id,database_id,slot_name,xid,end_lsn,first_seq,last_seq,event_count,payload_checksum,state) VALUES('tx-live','7','source','db','slot','1','0000000000000002',1,1,1,'sum','committed')", []).unwrap();
        writer.connection_mut().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('clickhouse','clickhouse','cfg','7',3)", []).unwrap();
        writer.connection_mut().execute("INSERT INTO destination_generation_leases(lease_id,destination_id,capture_epoch,anchor_id,generation,configuration_fingerprint,run_id,expires_mono_ms,state) VALUES('lease','clickhouse','7',NULL,3,'cfg','run',999999,'held')", []).unwrap();
        let batch = coordinator_batch(&mut writer);
        let transaction = writer.connection_mut().transaction().unwrap();
        mark_insert_dispatched(&transaction, &batch).unwrap();
        transaction.commit().unwrap();

        let wire = PinnedClickHouseWire::new(live_client()).unwrap();
        let mut adapter = ProductionClickHouseAdapter::new(wire);
        let remote = dispatch_and_readback(&mut adapter, &batch).unwrap();
        assert_eq!(remote.marker, batch.marker);
        assert_eq!(remote.events.len(), 1);
        assert_eq!(
            writer
                .connection()
                .query_row("SELECT state FROM clickhouse_batch_intents", [], |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "dispatched"
        );
        assert_eq!(
            writer
                .connection()
                .query_row("SELECT count() FROM destination_checkpoints", [], |row| row
                    .get::<_, u64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    #[ignore = "requires the preceding coordinator write and a restarted pinned ClickHouse server"]
    fn live_coordinator_adopts_marker_after_restart_and_checkpoints() {
        let mut writer = open_writer(&coordinator_path(), "run", 1, 1_000).unwrap();
        let batch = coordinator_batch(&mut writer);
        let wire = PinnedClickHouseWire::new(live_client()).unwrap();
        let mut adapter = ProductionClickHouseAdapter::new(wire);
        assert_eq!(
            reconcile_remote(&mut adapter, &batch),
            Ok(Reconciliation::Verified)
        );
        let remote = adapter.read_batch(&batch.marker).unwrap().unwrap();
        let transaction = writer.connection_mut().transaction().unwrap();
        finalize_checkpoint(&transaction, &coordinator_context(), &batch, &remote, None).unwrap();
        transaction.commit().unwrap();
        assert_eq!(
            writer
                .connection()
                .query_row(
                    "SELECT journal_seq FROM destination_checkpoints",
                    [],
                    |row| row.get::<_, u64>(0)
                )
                .unwrap(),
            1
        );
        assert_eq!(
            writer
                .connection()
                .query_row("SELECT state FROM clickhouse_batch_intents", [], |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "verified"
        );
    }

    #[test]
    #[ignore = "requires a conflicting selector inserted in the pinned ClickHouse probe"]
    fn live_contract_inspection_rejects_selector_conflict() {
        let mut wire = PinnedClickHouseWire::new(live_client()).unwrap();
        let (_, marker) = golden_event();
        let mut identity = live_audit_identity(&marker);
        identity.capture_epoch = "1".into();
        identity.generation = 7;
        identity.promotion_fence = 9;
        identity.selector_fingerprint =
            "2d3882a0f2ad8eb3c95fc1d98de156a1093a4200a32275c46d926cecd1f8c86e".into();
        let WireResponse::Contract(observed) = wire
            .execute(WireRequest::InspectAuditContractFresh {
                identity: &identity,
            })
            .unwrap()
        else {
            panic!("missing contract observation")
        };
        assert!(observed.selector_fingerprint.is_empty());
        assert_eq!(observed.promotion_fence, 0);
    }

    #[test]
    #[ignore = "requires one pinned ClickHouse view removed after positive readback"]
    fn live_contract_inspection_rejects_missing_view() {
        let mut wire = PinnedClickHouseWire::new(live_client()).unwrap();
        assert_eq!(
            wire.execute(WireRequest::InspectObjectsFresh),
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
        let WireResponse::AuditBatch(audit) = wire
            .execute(WireRequest::ReadAuditRangeFresh {
                identity: &live_audit_identity(&marker),
                first_seq: 1,
                last_seq: 1,
                max_events: 1,
            })
            .unwrap()
        else {
            panic!("missing audit range")
        };
        assert_eq!(audit.event_count, 1);
        assert_eq!(
            audit.events[0].reconstructed_payload,
            event.canonical_payload
        );
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
        let WireResponse::AuditBatch(audit) = wire
            .execute(WireRequest::ReadAuditRangeFresh {
                identity: &live_audit_identity(&marker),
                first_seq: 1,
                last_seq: 1,
                max_events: 1,
            })
            .unwrap()
        else {
            panic!("missing audit range")
        };
        assert_eq!(
            audit.events[0].reconstructed_payload,
            event.canonical_payload
        );
        assert!(matches!(
            wire.execute(WireRequest::ReadBatchFresh { marker: &marker }),
            Ok(WireResponse::Batch(Some(_)))
        ));
    }
}
