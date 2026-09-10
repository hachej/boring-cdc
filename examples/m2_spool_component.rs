use boring_cdc::m2_ownership::StateLock;
use boring_cdc::m2_spool::*;
use serde_json::json;
use std::fs;
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

struct FixedSpace(u64);
impl FreeSpace for FixedSpace {
    fn available_bytes(&self, _: &Path) -> io::Result<u64> {
        Ok(self.0)
    }
}
fn buffer(
    root: &Path,
    xid: &str,
    prefix: usize,
    space: u64,
    max_bytes: u64,
    max_events: u64,
) -> TxnBuffer {
    fs::create_dir_all(root).unwrap();
    fs::set_permissions(root, fs::Permissions::from_mode(0o700)).unwrap();
    let dev = fs::metadata(root).unwrap().dev();
    let mut disk = DiskAdmission::default();
    disk.configure(
        dev,
        FilesystemLimit {
            total_budget: 4096,
            emergency_reserve: 512,
        },
    )
    .unwrap();
    let memory = MemoryBudget::new(MemoryLimits {
        process_limit: 2048,
        runtime_fixed: 256,
        receive: 512,
        decoder: 512,
        staging: 768,
    })
    .unwrap();
    TxnBuffer::new(
        root.to_path_buf(),
        "capture-epoch-v1".into(),
        xid.into(),
        "dead-run".into(),
        SpoolLimits {
            max_frame_bytes: 512,
            max_event_bytes: 512,
            max_transaction_bytes: max_bytes,
            max_transaction_events: max_events,
            memory_prefix_bytes: prefix,
        },
        memory,
        disk,
        Box::new(FixedSpace(space)),
    )
    .unwrap()
}
fn push(buffer: &mut TxnBuffer, bytes: &[u8]) -> Result<(), SpoolError> {
    let mut receive = buffer.admit_receive(bytes.len())?;
    receive.extend_from_slice(bytes)?;
    buffer.push_received(receive)
}
fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = &args[1];
    let root = PathBuf::from(&args[2]);
    fs::create_dir_all(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let output = match mode.as_str() {
        "near-limit" => {
            let mut b = buffer(&root, "xid-near", 4, 4096, 8, 2);
            push(&mut b, b"1234").unwrap();
            push(&mut b, b"5678").unwrap();
            let iterator_events = {
                let mut count = 0;
                for value in b.commit_iter().unwrap() {
                    value.unwrap();
                    count += 1;
                }
                count
            };
            let high = b.high_water();
            let observed = b.observed();
            b.finish().unwrap();
            json!({"outcome":"commit_ready","observed_bytes":observed.0,"observed_events":observed.1,"iterator_events":iterator_events,"spill_delta":8,"stream_delta":0,"feedback_permitted":false,"high_water":high})
        }
        "oversized" => {
            let mut b = buffer(&root, "xid-large", 8, 4096, 4, 2);
            push(&mut b, b"1234").unwrap();
            let error = push(&mut b, b"5").unwrap_err();
            let (_, out) = b.failure_observation(&error, Some("0000000000000042"), "limit-a");
            json!({"outcome":out,"error":format!("{error:?}")})
        }
        "enospc" => {
            let mut b = buffer(&root, "xid-enospc", 0, 520, 32, 2);
            let error = push(&mut b, b"1234").unwrap_err();
            let (_, out) = b.failure_observation(&error, None, "disk-a");
            json!({"outcome":out,"error":format!("{error:?}")})
        }
        "startup" => {
            let store = root.join("state.sqlite");
            let lock = StateLock::acquire(&store, "owner-run", "nonce").unwrap();
            let mut dead = buffer(&root, "xid-dead", 0, 4096, 32, 2);
            push(&mut dead, b"one").unwrap();
            drop(dead);
            let mut contradictory = buffer(&root, "xid-bad", 0, 4096, 32, 2);
            push(&mut contradictory, b"two").unwrap();
            drop(contradictory);
            fs::write(root.join("malformed.spool"), b"bad").unwrap();
            let actions = classify_startup_spools(
                &lock,
                &root,
                "capture-epoch-v1",
                "owner-run",
                512,
                32,
                2,
                |_, xid| {
                    if xid == "xid-dead" {
                        ExistingTransaction::Uncommitted
                    } else {
                        ExistingTransaction::Contradictory
                    }
                },
            )
            .unwrap();
            json!({"outcome":"startup_blocked_for_quarantine","removed":actions.iter().filter(|a|matches!(a,StartupAction::RemovedUncommitted(_))).count(),"quarantined":actions.iter().filter(|a|matches!(a,StartupAction::Quarantined(_))).count(),"feedback_permitted":false})
        }
        _ => panic!("unknown mode"),
    };
    println!("{}", serde_json::to_string(&output).unwrap());
}
