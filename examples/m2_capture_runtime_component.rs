use boring_cdc::article1_capture::CaptureConfig;
use boring_cdc::m2_capture_runtime::{
    CaptureRuntime, NoSnapshotGate, RuntimeError, RuntimeSpool, SLOT, capture_copyboth_until,
};
use boring_cdc::m2_journal::{CommitLimits, JournalStore, SourceIdentity};
use boring_cdc::m2_schema::open_writer;
use boring_cdc::m2_spool::{
    DiskAdmission, FilesystemAdmissionController, FilesystemLimit, MemoryBudget, MemoryLimits,
    PosixAllocation, SpoolLimits, StatvfsSpace, TxnBuffer,
};
use pg_walstream::CancellationToken;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::time::Duration;

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 4 {
        eprintln!("usage: m2_capture_runtime_component DSN JOURNAL SPOOL");
        std::process::exit(2);
    }
    let dsn = args[1].clone();
    let journal = PathBuf::from(&args[2]);
    let spool = PathBuf::from(&args[3]);
    std::fs::create_dir_all(journal.parent().unwrap()).unwrap();
    std::fs::create_dir_all(&spool).unwrap();
    let writer = open_writer(&journal, "component-run", 1, 1).unwrap();
    let store = JournalStore::new(
        writer,
        SourceIdentity {
            capture_epoch: "component-epoch".into(),
            source_system_id: "article1-system".into(),
            timeline_id: "1".into(),
            database_id: "article1".into(),
            slot_name: SLOT.into(),
            publication_fingerprint: "article1-publication-v1".into(),
            protocol_fingerprint: "pgoutput-v1".into(),
        },
        CommitLimits {
            max_events: 1024,
            max_copied_bytes: 64 * 1024 * 1024,
            max_writer_hold: Duration::from_secs(5),
        },
    )
    .unwrap();
    let dev = std::fs::metadata(&spool).unwrap().dev();
    let disk = FilesystemAdmissionController::new(DiskAdmission::default());
    disk.configure(
        dev,
        FilesystemLimit {
            total_budget: 128 * 1024 * 1024,
            emergency_reserve: 1024 * 1024,
        },
    )
    .unwrap();
    let make_disk = disk.clone();
    let make_spool = spool.clone();
    let factory = move |xid: u32| -> Result<Box<dyn RuntimeSpool>, RuntimeError> {
        let memory = MemoryBudget::new(MemoryLimits {
            process_limit: 32 * 1024 * 1024,
            runtime_fixed: 1024 * 1024,
            receive: 2 * 1024 * 1024,
            decoder: 4 * 1024 * 1024,
            staging: 20 * 1024 * 1024,
        })
        .map_err(|e| RuntimeError::Spool(e.to_string()))?;
        TxnBuffer::new(
            make_spool.clone(),
            "component-epoch".into(),
            xid.to_string(),
            "component-run".into(),
            SpoolLimits {
                max_frame_bytes: 1024 * 1024,
                max_event_bytes: 1024 * 1024,
                max_transaction_bytes: 16 * 1024 * 1024,
                max_transaction_events: 1024,
                memory_prefix_bytes: 64 * 1024,
            },
            memory,
            make_disk.clone(),
            Box::new(StatvfsSpace),
            Box::new(PosixAllocation),
        )
        .map(|v| Box::new(v) as Box<dyn RuntimeSpool>)
        .map_err(|e| RuntimeError::Spool(e.to_string()))
    };
    let mut runtime = CaptureRuntime::new(store, factory, NoSnapshotGate, Default::default(), None);
    let config = CaptureConfig::article1(dsn, 1).unwrap();
    let cancel = CancellationToken::new();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(capture_copyboth_until(&config, &cancel, &mut runtime, 1))
        .unwrap();
    println!(
        "{{\"status\":\"pass\",\"commits\":{},\"durable_end_lsn\":\"{:016X}\"}}",
        runtime.committed_transactions(),
        runtime.durable_end_lsn().unwrap()
    );
}
