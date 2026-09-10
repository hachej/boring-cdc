use boring_cdc::m2_journal::{
    CommitFault, CommitLimits, JournalEvent, JournalStore, RelationSchema, SourceCommit,
    SourceIdentity, read_complete_range, sha256, transaction_checksum,
};
use boring_cdc::m2_schema::open_writer;
use std::path::Path;
use std::time::Duration;

fn event(id: &str, ordinal: u32, payload: &[u8]) -> JournalEvent {
    JournalEvent {
        event_id: id.into(),
        transaction_ordinal: ordinal,
        relation_schema_fingerprint: Some("schema-v1".into()),
        control_kind: None,
        payload: payload.into(),
        payload_hash: sha256(payload),
    }
}
fn source_commit() -> SourceCommit {
    let events = vec![event("event-1", 0, b"alpha"), event("event-2", 1, b"beta")];
    SourceCommit {
        transaction_id: "transaction-1".into(),
        xid: "41".into(),
        end_lsn: "0000000000000042".into(),
        payload_checksum: transaction_checksum(&events),
        schemas: vec![RelationSchema {
            fingerprint: "schema-v1".into(),
            relation_id: "relation-1".into(),
            canonical_schema: b"canonical-schema-v1".into(),
            checksum: "schema-checksum-v1".into(),
        }],
        events,
    }
}
fn main() {
    let mut args = std::env::args().skip(1);
    let mode = args.next().expect("mode");
    let path = args.next().expect("sqlite path");
    let identity = SourceIdentity {
        capture_epoch: "capture-epoch-v1".into(),
        source_system_id: "source-system-v1".into(),
        timeline_id: "timeline-v1".into(),
        database_id: "database-v1".into(),
        slot_name: "slot-v1".into(),
        publication_fingerprint: "publication-v1".into(),
        protocol_fingerprint: "protocol-v1".into(),
    };
    let writer = open_writer(Path::new(&path), "journal-component-run", 1, 1000).unwrap();
    let mut store = JournalStore::new(
        writer,
        identity,
        CommitLimits {
            max_events: 16,
            max_copied_bytes: 4096,
            max_writer_hold: Duration::from_secs(5),
        },
    )
    .unwrap();
    let commit = source_commit();
    let (fault, duplicate) = if mode == "fault" {
        assert!(
            store
                .commit(&commit, CommitFault::AfterSqliteCommit)
                .is_err()
        );
        let d = store.commit(&commit, CommitFault::None).unwrap();
        ("after-sqlite-commit", d.was_duplicate())
    } else {
        let d = store.commit(&commit, CommitFault::None).unwrap();
        ("none", d.was_duplicate())
    };
    drop(store);
    let range = read_complete_range(Path::new(&path), 0, 16, 4096, Duration::from_secs(2))
        .unwrap()
        .unwrap();
    println!(
        "{{\"copied_bytes\":{},\"duplicate_reconciled\":{},\"event_count\":{},\"fault_hook\":\"{}\",\"first_seq\":{},\"last_seq\":{},\"durable_end_lsn\":\"0000000000000042\"}}",
        range.copied_bytes,
        duplicate,
        range.events.len(),
        fault,
        range.first_seq,
        range.last_seq
    );
}
