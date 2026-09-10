use boring_cdc::m2_journal::{
    CommitFault, CommitLimits, JournalEvent, JournalStore, JournalWriterService, RelationSchema,
    SourceCommit, SourceIdentity, WorkOutcome, read_complete_range, sha256, transaction_checksum,
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
    let store = JournalStore::new(
        writer,
        identity,
        CommitLimits {
            max_events: 16,
            max_copied_bytes: 4096,
            max_writer_hold: if mode == "slow-commit" {
                Duration::from_millis(1)
            } else {
                Duration::from_secs(5)
            },
        },
    )
    .unwrap();
    let mut service = JournalWriterService::new(store, [8, 4, 4, 4], 2).unwrap();
    let commit = source_commit();
    let injected = match mode.as_str() {
        "terminate-before" => CommitFault::TerminateBeforeSqliteCommit,
        "terminate-after" => CommitFault::TerminateAfterSqliteCommit,
        "slow-commit" => CommitFault::SlowSqliteCommit,
        _ => CommitFault::None,
    };
    service.enqueue_capture(commit.clone(), injected).unwrap();
    if mode == "slow-commit" {
        assert!(service.service_next().unwrap().is_err());
        service.enqueue_capture(commit, CommitFault::None).unwrap();
    }
    let durable = match service.service_next().unwrap().unwrap() {
        WorkOutcome::Durable(d) => d,
        WorkOutcome::Serviced(_) => unreachable!(),
    };
    let fault = if mode == "normal" {
        "none"
    } else {
        mode.as_str()
    };
    let duplicate = durable.was_duplicate();
    drop(service);
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
