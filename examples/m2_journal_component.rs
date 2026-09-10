use boring_cdc::m2_journal::{
    CommitFault, CommitLimits, EnqueueError, JournalEvent, JournalStore, JournalWriterService,
    RelationSchema, ServiceCommand, SourceCommit, SourceIdentity, WorkClass, WorkOutcome,
    journal_gc_dry_run, journal_inspect_event, journal_verify, read_complete_range, sha256,
    transaction_checksum,
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
            max_writer_hold: if mode == "slow-commit" || mode == "saturated-service" {
                Duration::from_millis(20)
            } else {
                Duration::from_secs(5)
            },
        },
    )
    .unwrap();
    let (queue_caps, capture_burst) = if mode == "saturated-service" {
        ([4, 1, 1, 1], 1)
    } else {
        ([8, 4, 4, 4], 2)
    };
    let mut service = JournalWriterService::new(store, queue_caps, capture_burst).unwrap();
    if mode == "saturated-service" {
        for i in 1..=4 {
            let mut commit = source_commit();
            commit.transaction_id = format!("transaction-{i}");
            commit.xid = format!("{i}");
            commit.end_lsn = format!("{i:016X}");
            for (ordinal, event) in commit.events.iter_mut().enumerate() {
                event.event_id = format!("event-{i}-{ordinal}");
            }
            service
                .enqueue_capture(
                    commit,
                    if i == 1 {
                        CommitFault::SlowSqliteCommit
                    } else {
                        CommitFault::None
                    },
                )
                .unwrap();
        }
        service
            .enqueue_service(
                WorkClass::FailureControl,
                ServiceCommand::PersistFailureControl {
                    failure_id: "saturated-failure".into(),
                    fingerprint: "saturated-fingerprint".into(),
                    max_writer_hold: Duration::from_secs(1),
                },
            )
            .unwrap();
        service
            .enqueue_service(
                WorkClass::Checkpoint,
                ServiceCommand::CheckpointWal {
                    max_writer_hold: Duration::from_secs(1),
                },
            )
            .unwrap();
        service
            .enqueue_service(
                WorkClass::Gc,
                ServiceCommand::GcDryRun {
                    retain_from_seq: 9,
                    max_transactions: 4,
                    max_writer_hold: Duration::from_secs(1),
                },
            )
            .unwrap();
        let overflow = service.enqueue_service(
            WorkClass::Gc,
            ServiceCommand::GcDryRun {
                retain_from_seq: 9,
                max_transactions: 1,
                max_writer_hold: Duration::from_secs(1),
            },
        );
        assert_eq!(overflow, Err(EnqueueError::Overloaded(WorkClass::Gc)));
        let mut max_wait = Duration::ZERO;
        let mut serviced = Vec::new();
        let mut service_turns = Vec::new();
        let mut turn = 0;
        let mut durable = 0;
        let mut ambiguous = 0;
        while let Some(outcome) = service.service_next() {
            turn += 1;
            match outcome {
                Ok(WorkOutcome::Durable(_)) => durable += 1,
                Ok(WorkOutcome::Serviced(report)) => {
                    max_wait = max_wait.max(report.queue_wait);
                    serviced.push(format!("{:?}", report.class));
                    service_turns.push(turn);
                }
                Err(boring_cdc::m2_journal::JournalError::BusyBoundExceededAfterCommit) => {
                    ambiguous += 1
                }
                Err(error) => panic!("unexpected service error: {error:?}"),
            }
        }
        assert!(max_wait < Duration::from_secs(1));
        println!(
            "{{\"ambiguous_commits\":{ambiguous},\"atomic_transactions\":{},\"durable_commits\":{durable},\"max_service_wait_lt_ms\":1000,\"overload\":\"Gc\",\"service_turns\":{:?},\"serviced\":\"{}\"}}",
            durable + ambiguous,
            service_turns,
            serviced.join(",")
        );
        return;
    }
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
    if mode == "commands" {
        let inspection = journal_inspect_event(Path::new(&path), "event-1", Duration::from_secs(2))
            .unwrap()
            .unwrap();
        let verification = journal_verify(Path::new(&path), Duration::from_secs(2)).unwrap();
        let gc = journal_gc_dry_run(Path::new(&path), 3, 16, Duration::from_secs(2)).unwrap();
        println!(
            "{{\"command_boundaries\":[\"CMD-JOURNAL-INSPECT-EVENT-ID-ID-EXPLAIN-JSON\",\"CMD-JOURNAL-VERIFY\",\"CMD-JOURNAL-GC-DRY-RUN\"],\"event_id\":\"{}\",\"verified_events\":{},\"gc_dry_run_events\":{}}}",
            inspection.event_id, verification.event_count, gc.event_count
        );
        return;
    }
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
