//! Initial-slot snapshot admission into the pinned ClickHouse history and marker protocol.

use crate::m1_decoder::{RelationContract, TupleValue};
use crate::m1_source_identity::RelationSchemaVersion;
use crate::m1_transition_kernel::{CaptureEpoch, DestinationGeneration, ReceivedLsn};
use crate::m2_event_envelope::render_snapshot_row;
use crate::m4_clickhouse_durability::{
    BatchContext, BatchMarker, DestinationEvent, DurabilityError, PreparedBatch, RemoteBatch,
    ordered_event_digest, verify_remote_batch,
};
use crate::m4_clickhouse_schema::object_fingerprint;
use rusqlite::{OptionalExtension, Transaction, params};
use serde_json::Value;
use sha2::{Digest, Sha256};

const INSTALL_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS m4_snapshot_batch_intents(
 intent_id TEXT PRIMARY KEY, destination_id TEXT NOT NULL REFERENCES destinations(destination_id),
 capture_epoch TEXT NOT NULL, generation INTEGER NOT NULL, anchor_id TEXT NOT NULL REFERENCES bootstrap_anchors(anchor_id),
 relation_schema_fingerprint TEXT NOT NULL REFERENCES relation_schemas(schema_fingerprint),
 first_snapshot_seq INTEGER NOT NULL CHECK(first_snapshot_seq>0),
 last_snapshot_seq INTEGER NOT NULL CHECK(last_snapshot_seq>=first_snapshot_seq),
 ordered_event_digest TEXT NOT NULL, configuration_fingerprint TEXT NOT NULL,
 object_fingerprint TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('prepared','dispatched','verified','failed')),
 UNIQUE(destination_id,capture_epoch,generation,first_snapshot_seq,last_snapshot_seq));
CREATE TRIGGER IF NOT EXISTS m4_snapshot_intent_immutable BEFORE UPDATE ON m4_snapshot_batch_intents
 WHEN NEW.intent_id!=OLD.intent_id OR NEW.destination_id!=OLD.destination_id OR NEW.capture_epoch!=OLD.capture_epoch
 OR NEW.generation!=OLD.generation OR NEW.anchor_id!=OLD.anchor_id
 OR NEW.relation_schema_fingerprint!=OLD.relation_schema_fingerprint
 OR NEW.first_snapshot_seq!=OLD.first_snapshot_seq OR NEW.last_snapshot_seq!=OLD.last_snapshot_seq
 OR NEW.ordered_event_digest!=OLD.ordered_event_digest OR NEW.configuration_fingerprint!=OLD.configuration_fingerprint
 OR NEW.object_fingerprint!=OLD.object_fingerprint
 BEGIN SELECT RAISE(ABORT,'snapshot intent is immutable'); END;
CREATE TRIGGER IF NOT EXISTS m4_snapshot_intent_transition BEFORE UPDATE OF state ON m4_snapshot_batch_intents
 WHEN NOT ((OLD.state='prepared' AND NEW.state IN ('dispatched','failed'))
 OR (OLD.state='dispatched' AND NEW.state IN ('verified','failed')) OR NEW.state=OLD.state)
 BEGIN SELECT RAISE(ABORT,'invalid snapshot intent transition'); END;
"#;

type SnapshotIntentRow = (
    String,
    String,
    u64,
    String,
    String,
    u64,
    u64,
    String,
    String,
    String,
    String,
);

pub fn install(connection: &rusqlite::Connection) -> Result<(), DurabilityError> {
    connection.execute_batch(INSTALL_SQL)?;
    Ok(())
}

#[derive(Clone, Debug)]
pub struct PreparedSnapshotBatch {
    pub batch: PreparedBatch,
    pub anchor_id: String,
    pub relation_schema_fingerprint: String,
    pub first_snapshot_seq: u64,
    pub last_snapshot_seq: u64,
}

/// Copy a bounded, contiguous M3 snapshot range while holding only the SQLite transaction. The
/// caller commits this intent before any external ClickHouse operation.
pub fn prepare_snapshot_batch(
    transaction: &Transaction<'_>,
    context: &BatchContext,
    anchor_id: &str,
    contract: &RelationContract,
    relation: RelationSchemaVersion,
    first_snapshot_seq: u64,
    last_snapshot_seq: u64,
) -> Result<PreparedSnapshotBatch, DurabilityError> {
    if first_snapshot_seq == 0 || last_snapshot_seq < first_snapshot_seq || anchor_id.is_empty() {
        return Err(DurabilityError::Invalid("invalid snapshot range"));
    }
    let capture_epoch: u64 = context
        .capture_epoch
        .parse()
        .map_err(|_| DurabilityError::Invalid("capture epoch is not ClickHouse UInt64"))?;
    let relation_fingerprint = hex(&relation.fingerprint().bytes());
    let anchor: Option<(String, u64, String, u64, String, String)> = transaction.query_row(
        "SELECT a.generation_id,a.snapshot_complete_seq,a.snapshot_boundary_lsn,a.start_seq,p.snapshot_schema_fingerprint,r.destination_id
         FROM bootstrap_anchors a JOIN m3_planner_runs p ON p.generation_id=a.generation_id
         JOIN backfill_runs r ON r.run_id=p.run_id
         WHERE a.anchor_id=?1 AND a.capture_epoch=?2 AND a.generation=?3 AND a.state='complete'",
        params![anchor_id, context.capture_epoch, context.generation],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?)),
    ).optional()?;
    let Some((
        generation_id,
        complete_seq,
        boundary_lsn,
        start_seq,
        schema_fingerprint,
        destination_id,
    )) = anchor
    else {
        return Err(DurabilityError::Conflict(
            "complete initial anchor unavailable",
        ));
    };
    if start_seq != 0
        || last_snapshot_seq > complete_seq
        || schema_fingerprint != relation_fingerprint
        || destination_id != context.destination_id
        || relation.relation_id != contract.relation.id
    {
        return Err(DurabilityError::Conflict("snapshot anchor binding differs"));
    }
    let imported_relation: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM m3_snapshot_schema_proofs p
         JOIN bootstrap_anchors a ON a.bootstrap_intent_id=p.intent_id
         JOIN relation_schemas s ON s.schema_fingerprint=p.schema_fingerprint
         WHERE a.anchor_id=?1 AND p.capture_epoch=?2 AND p.schema_fingerprint=?3
         AND p.relation_id=?4 AND p.namespace=?5 AND p.relation_name=?6
         AND p.canonical_schema=s.canonical_schema)",
        params![
            anchor_id,
            context.capture_epoch,
            relation_fingerprint,
            contract.relation.id.to_string(),
            contract.relation.namespace,
            contract.relation.name
        ],
        |row| row.get(0),
    )?;
    if !imported_relation {
        return Err(DurabilityError::Conflict(
            "snapshot imported relation proof differs",
        ));
    }
    let held: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM destination_generation_leases l JOIN destinations d USING(destination_id)
         WHERE l.lease_id=?1 AND l.destination_id=?2 AND l.capture_epoch=?3 AND l.generation=?4
         AND l.anchor_id=?5 AND l.configuration_fingerprint=?6 AND l.run_id=?7 AND l.state='held'
         AND d.kind='clickhouse' AND d.capture_epoch=l.capture_epoch AND d.generation=l.generation
         AND d.configuration_fingerprint=l.configuration_fingerprint AND d.current_failure_id IS NULL)",
        params![context.lease_id,context.destination_id,context.capture_epoch,context.generation,anchor_id,context.configuration_fingerprint,context.run_id],
        |row| row.get(0),
    )?;
    if !held {
        return Err(DurabilityError::OwnershipLost);
    }
    let boundary = u64::from_str_radix(&boundary_lsn, 16)
        .map_err(|_| DurabilityError::Conflict("invalid anchor boundary LSN"))?;
    let mut stmt = transaction.prepare(
        "SELECT e.snapshot_seq,e.chunk_id,e.canonical_key,e.payload,e.payload_hash
         FROM m3_snapshot_events e JOIN backfill_chunks c ON c.chunk_id=e.chunk_id AND c.generation_id=e.generation_id
         WHERE e.generation_id=?1 AND e.snapshot_seq BETWEEN ?2 AND ?3 AND c.state='complete'
         ORDER BY e.snapshot_seq",
    )?;
    let rows = stmt
        .query_map(
            params![generation_id, first_snapshot_seq, last_snapshot_seq],
            |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, String>(4)?,
                ))
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if rows.len() as u64 != last_snapshot_seq - first_snapshot_seq + 1 {
        return Err(DurabilityError::Conflict("snapshot range has a gap"));
    }
    let mut events = Vec::with_capacity(rows.len());
    for (offset, (snapshot_seq, chunk_id, key, payload, payload_hash)) in rows.iter().enumerate() {
        if *snapshot_seq != first_snapshot_seq + offset as u64
            || hex(&Sha256::digest(payload)) != *payload_hash
        {
            return Err(DurabilityError::Conflict("snapshot row content mismatch"));
        }
        let chunk_ordinal = chunk_id
            .strip_prefix(&format!("{generation_id}:"))
            .filter(|value| value.len() == 8 && value.bytes().all(|byte| byte.is_ascii_digit()))
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or(DurabilityError::Conflict(
                "snapshot chunk identity mismatch",
            ))?;
        let tuple = tuple_from_m3_json(contract, key, payload)?;
        let (event_id, envelope) = render_snapshot_row(
            CaptureEpoch::from_store(capture_epoch),
            DestinationGeneration::from_store(context.generation),
            contract,
            relation,
            ReceivedLsn::from_wire(boundary),
            chunk_ordinal,
            &tuple,
        )
        .map_err(|_| DurabilityError::Conflict("snapshot event render failed"))?;
        events.push(DestinationEvent {
            journal_seq: 0,
            transaction_ordinal: 0,
            mutation_ordinal: 0,
            connector_event_id: event_id,
            relation_schema_fingerprint: relation_fingerprint.clone(),
            payload: envelope.json,
            payload_hash: envelope.canonical_payload_hash,
            canonical_payload: envelope.canonical_preimage,
        });
    }
    let digest = ordered_event_digest(&events);
    let object = object_fingerprint();
    let intent_id = snapshot_batch_id(
        context,
        anchor_id,
        &relation_fingerprint,
        first_snapshot_seq,
        last_snapshot_seq,
        &digest,
        &object,
    );
    let marker = BatchMarker {
        capture_epoch,
        generation: context.generation,
        batch_id: intent_id.clone(),
        first_journal_seq: 0,
        last_journal_seq: 0,
        event_count: events.len() as u64,
        ordered_event_digest: digest.clone(),
        object_fingerprint: object.clone(),
    };
    transaction.execute(
        "INSERT INTO m4_snapshot_batch_intents VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,'prepared') ON CONFLICT(intent_id) DO NOTHING",
        params![intent_id,context.destination_id,context.capture_epoch,context.generation,anchor_id,relation_fingerprint,first_snapshot_seq,last_snapshot_seq,digest,context.configuration_fingerprint,object],
    )?;
    let prepared = PreparedSnapshotBatch {
        batch: PreparedBatch {
            intent_id: intent_id.clone(),
            destination_id: context.destination_id.clone(),
            capture_epoch_text: context.capture_epoch.clone(),
            configuration_fingerprint: context.configuration_fingerprint.clone(),
            lease_id: context.lease_id.clone(),
            run_id: context.run_id.clone(),
            complete_transaction_id: String::new(),
            events,
            marker,
        },
        anchor_id: anchor_id.to_owned(),
        relation_schema_fingerprint: relation_fingerprint,
        first_snapshot_seq,
        last_snapshot_seq,
    };
    validate_intent(transaction, context, &prepared)?;
    Ok(prepared)
}

pub fn mark_dispatched(
    transaction: &Transaction<'_>,
    context: &BatchContext,
    prepared: &PreparedSnapshotBatch,
) -> Result<(), DurabilityError> {
    validate_intent(transaction, context, prepared)?;
    let changed=transaction.execute("UPDATE m4_snapshot_batch_intents SET state='dispatched' WHERE intent_id=?1 AND state='prepared'",[&prepared.batch.intent_id])?;
    if changed == 1 || intent_state(transaction, &prepared.batch.intent_id)? == "dispatched" {
        Ok(())
    } else {
        Err(DurabilityError::Conflict(
            "snapshot batch cannot enter dispatched state",
        ))
    }
}

pub fn finalize_verified(
    transaction: &Transaction<'_>,
    context: &BatchContext,
    prepared: &PreparedSnapshotBatch,
    remote: &RemoteBatch,
) -> Result<(), DurabilityError> {
    validate_intent(transaction, context, prepared)?;
    verify_remote_batch(&prepared.batch, remote)?;
    let changed=transaction.execute("UPDATE m4_snapshot_batch_intents SET state='verified' WHERE intent_id=?1 AND state='dispatched'",[&prepared.batch.intent_id])?;
    if changed == 1 {
        Ok(())
    } else {
        Err(DurabilityError::Conflict(
            "snapshot intent finalization compare-and-swap failed",
        ))
    }
}

pub(crate) fn baseline_verified(
    transaction: &Transaction<'_>,
    context: &BatchContext,
    anchor_id: &str,
) -> Result<bool, DurabilityError> {
    let total: Option<u64>=transaction.query_row(
        "SELECT snapshot_complete_seq FROM bootstrap_anchors WHERE anchor_id=?1 AND capture_epoch=?2 AND generation=?3 AND state='complete' AND start_seq=0",
        params![anchor_id,context.capture_epoch,context.generation],|row|row.get(0),
    ).optional()?;
    let Some(total) = total else { return Ok(false) };
    let mut stmt=transaction.prepare(
        "SELECT first_snapshot_seq,last_snapshot_seq FROM m4_snapshot_batch_intents WHERE destination_id=?1 AND capture_epoch=?2 AND generation=?3 AND anchor_id=?4 AND configuration_fingerprint=?5 AND state='verified' ORDER BY first_snapshot_seq",
    )?;
    let ranges = stmt.query_map(
        params![
            context.destination_id,
            context.capture_epoch,
            context.generation,
            anchor_id,
            context.configuration_fingerprint
        ],
        |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?)),
    )?;
    let mut next = 1;
    for range in ranges {
        let (first, last) = range?;
        if first != next || last > total {
            return Ok(false);
        }
        next = last
            .checked_add(1)
            .ok_or(DurabilityError::Conflict("snapshot sequence overflow"))?;
    }
    Ok(next == total + 1)
}

fn validate_intent(
    transaction: &Transaction<'_>,
    context: &BatchContext,
    prepared: &PreparedSnapshotBatch,
) -> Result<(), DurabilityError> {
    let held: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM destination_generation_leases WHERE lease_id=?1 AND destination_id=?2 AND capture_epoch=?3 AND generation=?4 AND anchor_id=?5 AND configuration_fingerprint=?6 AND run_id=?7 AND state='held')",
        params![context.lease_id,context.destination_id,context.capture_epoch,context.generation,prepared.anchor_id,context.configuration_fingerprint,context.run_id],
        |row| row.get(0),
    )?;
    if !held {
        return Err(DurabilityError::OwnershipLost);
    }
    if prepared.batch.destination_id != context.destination_id
        || prepared.batch.capture_epoch_text != context.capture_epoch
        || prepared.batch.marker.generation != context.generation
        || prepared.batch.configuration_fingerprint != context.configuration_fingerprint
        || prepared.batch.lease_id != context.lease_id
        || prepared.batch.run_id != context.run_id
    {
        return Err(DurabilityError::Conflict("stale snapshot batch context"));
    }
    let persisted:Option<SnapshotIntentRow>=transaction.query_row(
        "SELECT destination_id,capture_epoch,generation,anchor_id,relation_schema_fingerprint,first_snapshot_seq,last_snapshot_seq,ordered_event_digest,configuration_fingerprint,object_fingerprint,state FROM m4_snapshot_batch_intents WHERE intent_id=?1",
        [&prepared.batch.intent_id],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?,row.get(8)?,row.get(9)?,row.get(10)?)),
    ).optional()?;
    let Some((dest, epoch, generation, anchor, schema, first, last, digest, config, object, state)) =
        persisted
    else {
        return Err(DurabilityError::Conflict("snapshot intent missing"));
    };
    if dest != context.destination_id
        || epoch != context.capture_epoch
        || generation != context.generation
        || anchor != prepared.anchor_id
        || schema != prepared.relation_schema_fingerprint
        || first != prepared.first_snapshot_seq
        || last != prepared.last_snapshot_seq
        || digest != prepared.batch.marker.ordered_event_digest
        || config != context.configuration_fingerprint
        || object != prepared.batch.marker.object_fingerprint
        || !matches!(state.as_str(), "prepared" | "dispatched" | "verified")
    {
        return Err(DurabilityError::Conflict(
            "snapshot intent differs from batch",
        ));
    }
    Ok(())
}

fn intent_state(transaction: &Transaction<'_>, intent_id: &str) -> Result<String, DurabilityError> {
    Ok(transaction.query_row(
        "SELECT state FROM m4_snapshot_batch_intents WHERE intent_id=?1",
        [intent_id],
        |row| row.get(0),
    )?)
}

fn tuple_from_m3_json(
    contract: &RelationContract,
    key: &[u8],
    payload: &[u8],
) -> Result<Vec<TupleValue>, DurabilityError> {
    let value: Value = serde_json::from_slice(payload)
        .map_err(|_| DurabilityError::Conflict("invalid M3 row JSON"))?;
    let object = value
        .as_object()
        .ok_or(DurabilityError::Conflict("invalid M3 row JSON"))?;
    if contract.key_columns.len() != 1 || object.len() != contract.relation.columns.len() {
        return Err(DurabilityError::Conflict(
            "unsupported snapshot relation shape",
        ));
    }
    let mut tuple = Vec::with_capacity(object.len());
    for column in &contract.relation.columns {
        let value = object
            .get(&column.name)
            .ok_or(DurabilityError::Conflict("snapshot column missing"))?;
        let wire = match (column.type_oid, value) {
            (_, Value::Null) => TupleValue::Null,
            (20, Value::Number(number)) => TupleValue::Text(
                number
                    .as_i64()
                    .ok_or(DurabilityError::Conflict("snapshot integer out of range"))?
                    .to_string()
                    .into_bytes(),
            ),
            (25, Value::String(text)) => TupleValue::Text(text.as_bytes().to_vec()),
            _ => return Err(DurabilityError::Conflict("unsupported snapshot value type")),
        };
        tuple.push(wire);
    }
    let key_index = contract.key_columns[0];
    let Some(TupleValue::Text(wire)) = tuple.get(key_index) else {
        return Err(DurabilityError::Conflict("snapshot key missing"));
    };
    if contract.relation.columns[key_index].type_oid != 20 {
        return Err(DurabilityError::Conflict("unsupported snapshot key type"));
    }
    let key_value = std::str::from_utf8(wire)
        .ok()
        .and_then(|text| text.parse::<i64>().ok())
        .ok_or(DurabilityError::Conflict("invalid snapshot key"))?;
    let mut expected = vec![1];
    expected.extend_from_slice(&((key_value as u64) ^ (1u64 << 63)).to_be_bytes());
    if key != expected {
        return Err(DurabilityError::Conflict(
            "M3 snapshot key differs from row",
        ));
    }
    Ok(tuple)
}

fn snapshot_batch_id(
    context: &BatchContext,
    anchor: &str,
    schema: &str,
    first: u64,
    last: u64,
    digest: &str,
    object: &str,
) -> String {
    let mut hash = Sha256::new();
    for value in [
        "boring-cdc/clickhouse-snapshot-batch/v1",
        &context.destination_id,
        &context.capture_epoch,
        &context.generation.to_string(),
        anchor,
        schema,
        &first.to_string(),
        &last.to_string(),
        digest,
        &context.configuration_fingerprint,
        object,
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hex(&hash.finalize())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m1_decoder::{Column, Relation};
    use crate::m1_source_identity::{
        Fingerprint, LogicalTableIdentity, SourceIdentity, supported_protocol_fingerprint,
    };
    use crate::m4_clickhouse_durability::RemoteEventIdentity;

    #[test]
    fn snapshot_intent_requires_exact_m3_row_and_remote_readback() {
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
        let schema = hex(&relation.fingerprint().bytes());
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
        let mut connection = rusqlite::Connection::open_in_memory().unwrap();
        connection.execute_batch("CREATE TABLE destinations(destination_id TEXT PRIMARY KEY,kind TEXT,capture_epoch TEXT,generation INTEGER,configuration_fingerprint TEXT,current_failure_id TEXT);
            CREATE TABLE destination_generation_leases(lease_id TEXT PRIMARY KEY,destination_id TEXT,capture_epoch TEXT,generation INTEGER,anchor_id TEXT,configuration_fingerprint TEXT,run_id TEXT,state TEXT);
            CREATE TABLE bootstrap_anchors(anchor_id TEXT PRIMARY KEY,capture_epoch TEXT,generation INTEGER,state TEXT,start_seq INTEGER,snapshot_boundary_lsn TEXT,snapshot_complete_seq INTEGER,generation_id TEXT,bootstrap_intent_id TEXT);
            CREATE TABLE backfill_runs(run_id TEXT PRIMARY KEY,destination_id TEXT);
            CREATE TABLE m3_planner_runs(run_id TEXT,generation_id TEXT PRIMARY KEY,snapshot_schema_fingerprint TEXT);
            CREATE TABLE backfill_chunks(chunk_id TEXT PRIMARY KEY,generation_id TEXT,state TEXT);
            CREATE TABLE m3_snapshot_events(generation_id TEXT,snapshot_seq INTEGER,chunk_id TEXT,canonical_key BLOB,payload BLOB,payload_hash TEXT);
            CREATE TABLE relation_schemas(schema_fingerprint TEXT PRIMARY KEY,canonical_schema BLOB);
            CREATE TABLE m3_snapshot_schema_proofs(intent_id TEXT,capture_epoch TEXT,schema_fingerprint TEXT,relation_id TEXT,namespace TEXT,relation_name TEXT,canonical_schema BLOB);").unwrap();
        install(&connection).unwrap();
        connection
            .execute(
                "INSERT INTO destinations VALUES('ch','clickhouse','7',3,'cfg',NULL)",
                [],
            )
            .unwrap();
        connection.execute("INSERT INTO destination_generation_leases VALUES('lease','ch','7',3,'anchor','cfg','run','held')",[]).unwrap();
        connection.execute("INSERT INTO bootstrap_anchors VALUES('anchor','7',3,'complete',0,'0000000000000070',1,'gen','boot')",[]).unwrap();
        connection
            .execute("INSERT INTO backfill_runs VALUES('run','ch')", [])
            .unwrap();
        connection
            .execute(
                "INSERT INTO m3_planner_runs VALUES('run','gen',?1)",
                [&schema],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO backfill_chunks VALUES('gen:00000005','gen','complete')",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO relation_schemas VALUES(?1,?2)",
                params![
                    schema,
                    include_bytes!("../tests/fixtures/m1_ordering/relation-contract.bin")
                ],
            )
            .unwrap();
        connection.execute("INSERT INTO m3_snapshot_schema_proofs VALUES('boot','7',?1,'24576','public','orders',?2)",params![schema,include_bytes!("../tests/fixtures/m1_ordering/relation-contract.bin")]).unwrap();
        let payload = br#"{"id":42}"#;
        let mut key = vec![1];
        key.extend_from_slice(&((42_u64) ^ (1_u64 << 63)).to_be_bytes());
        connection
            .execute(
                "INSERT INTO m3_snapshot_events VALUES('gen',1,'gen:00000005',?1,?2,?3)",
                params![key, payload.to_vec(), hex(&Sha256::digest(payload))],
            )
            .unwrap();
        let context = BatchContext {
            destination_id: "ch".into(),
            capture_epoch: "7".into(),
            generation: 3,
            configuration_fingerprint: "cfg".into(),
            lease_id: "lease".into(),
            run_id: "run".into(),
        };
        {
            let tx = connection.transaction().unwrap();
            assert!(!baseline_verified(&tx, &context, "anchor").unwrap());
            let prepared =
                prepare_snapshot_batch(&tx, &context, "anchor", &contract, relation, 1, 1).unwrap();
            assert_eq!(prepared.batch.events.len(), 1);
            assert_eq!(prepared.batch.marker.first_journal_seq, 0);
            assert_eq!(prepared.batch.marker.last_journal_seq, 0);
            assert_eq!(prepared.batch.events[0].journal_seq, 0);
            let remote = RemoteBatch {
                marker: prepared.batch.marker.clone(),
                events: prepared
                    .batch
                    .events
                    .iter()
                    .map(|event| RemoteEventIdentity {
                        journal_seq: event.journal_seq,
                        connector_event_id: event.connector_event_id.clone(),
                        payload_hash: event.payload_hash.clone(),
                        canonical_payload: event.canonical_payload.clone(),
                    })
                    .collect(),
            };
            assert!(finalize_verified(&tx, &context, &prepared, &remote).is_err());
            mark_dispatched(&tx, &context, &prepared).unwrap();
            let mut bad = remote.clone();
            bad.events[0].payload_hash = "different".into();
            assert!(finalize_verified(&tx, &context, &prepared, &bad).is_err());
            assert!(!baseline_verified(&tx, &context, "anchor").unwrap());
            finalize_verified(&tx, &context, &prepared, &remote).unwrap();
            assert!(baseline_verified(&tx, &context, "anchor").unwrap());
            tx.commit().unwrap();
        }
        connection
            .execute("UPDATE m3_snapshot_events SET canonical_key=X'00'", [])
            .unwrap();
        let tx = connection.transaction().unwrap();
        assert!(
            prepare_snapshot_batch(&tx, &context, "anchor", &contract, relation, 1, 1).is_err()
        );
    }
}
