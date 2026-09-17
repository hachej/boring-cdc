use boring_cdc::m1_transition_kernel::{Randomness, TransitionContext, VirtualClock};
use boring_cdc::m2_ownership::StateLock;
use boring_cdc::m2_spool::*;
use serde_json::json;
use std::fs;
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

struct ZeroRandom;
impl Randomness for ZeroRandom {
    fn next_u64(&mut self) -> u64 {
        0
    }
}
struct EnospcAllocation;
impl PhysicalAllocation for EnospcAllocation {
    fn allocate(&self, _: &std::fs::File, _: u64, _: u64) -> io::Result<()> {
        Err(io::Error::from_raw_os_error(libc::ENOSPC))
    }
}
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
        process_limit: 65_536,
        runtime_fixed: 4_096,
        receive: 1_024,
        decoder: 32_768,
        staging: 16_384,
    })
    .unwrap();
    let allocator: Box<dyn PhysicalAllocation> = if xid == "xid-enospc" {
        Box::new(EnospcAllocation)
    } else {
        Box::new(PosixAllocation)
    };
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
        FilesystemAdmissionController::new(disk),
        Box::new(FixedSpace(space)),
        allocator,
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
            let spill_delta = b.spill_bytes();
            let stream_delta = b.stream_events();
            b.finish().unwrap();
            json!({"outcome":"commit_ready","observed_bytes":observed.0,"observed_events":observed.1,"iterator_events":iterator_events,"spill_delta":spill_delta,"stream_delta":stream_delta,"feedback_permitted":false,"high_water":high})
        }
        "filesystem-isolation" => {
            let first_fs = 41;
            let second_fs = 42;
            let shared = FilesystemAdmissionController::new(DiskAdmission::default());
            for filesystem in [first_fs, second_fs] {
                shared
                    .configure(
                        filesystem,
                        FilesystemLimit {
                            total_budget: 1_000,
                            emergency_reserve: 100,
                        },
                    )
                    .unwrap();
            }
            shared.account_existing(first_fs, 100).unwrap();
            let first_transaction = shared.clone();
            let second_transaction = shared.clone();
            first_transaction.admit(first_fs, 1_000, 400).unwrap();
            let same_filesystem_blocked = second_transaction.admit(first_fs, 1_000, 401).is_err();
            second_transaction.admit(second_fs, 1_000, 800).unwrap();
            first_transaction.release(first_fs, 400).unwrap();
            json!({"outcome":"isolated","same_filesystem_blocked":same_filesystem_blocked,"first_filesystem_reserved":shared.reserved(first_fs),"second_filesystem_reserved":shared.reserved(second_fs)})
        }
        "oversized" => {
            let mut b = buffer(&root, "xid-large", 8, 4096, 4, 2);
            push(&mut b, b"1234").unwrap();
            let error = push(&mut b, b"5").unwrap_err();
            let clock = VirtualClock::new(1000);
            let mut random = ZeroRandom;
            let mut context = TransitionContext {
                clock: &clock,
                randomness: &mut random,
            };
            let (out, action, prepared) = b.apply_failure_policy(
                &error,
                Some("0000000000000042"),
                "limit-a",
                None,
                &mut context,
            );
            json!({"outcome":out,"error":format!("{error:?}"),"policy_action":format!("{action:?}"),"prepared_persistence":prepared.is_some()})
        }
        "policy-vectors" => {
            let b = buffer(&root, "xid-policy", 8, 4096, 4, 2);
            let error = SpoolError::TransactionBytesLimit {
                limit: 4,
                observed: 5,
            };
            let clock = VirtualClock::new(1_000);
            let mut random = ZeroRandom;
            let mut context = TransitionContext {
                clock: &clock,
                randomness: &mut random,
            };
            let (_, initial, _) = b.apply_failure_policy(
                &error,
                Some("0000000000000042"),
                "limit-a",
                None,
                &mut context,
            );
            let boring_cdc::failure_policy::PolicyAction::Persist(record) = initial else {
                panic!("initial policy action")
            };
            let (_, suppressed, suppressed_write) = b.apply_failure_policy(
                &error,
                Some("0000000000000042"),
                "limit-a",
                Some(&record),
                &mut context,
            );
            let later = VirtualClock::new(2_000);
            let mut random = ZeroRandom;
            let mut context = TransitionContext {
                clock: &later,
                randomness: &mut random,
            };
            let (unchanged, unchanged_action, _) = b.rearm_failure_policy(
                &error,
                Some("0000000000000042"),
                "limit-a",
                "limit-a",
                &record,
                true,
                "same-limit",
                &mut context,
            );
            let (wal_missing, wal_missing_action, _) = b.rearm_failure_policy(
                &error,
                Some("0000000000000042"),
                "limit-a",
                "limit-b",
                &record,
                false,
                "wal-missing",
                &mut context,
            );
            let (changed, changed_action, changed_write) = b.rearm_failure_policy(
                &error,
                Some("0000000000000042"),
                "limit-a",
                "limit-b",
                &record,
                true,
                "changed-limit",
                &mut context,
            );
            b.finish().unwrap();
            json!({"outcome":"policy_vectors_proved","same_fingerprint":format!("{suppressed:?}"),"same_fingerprint_write":suppressed_write.is_some(),"unchanged_limit":unchanged,"unchanged_action":format!("{unchanged_action:?}"),"wal_missing":wal_missing,"wal_missing_action":format!("{wal_missing_action:?}"),"changed_limit":changed,"changed_action":format!("{changed_action:?}"),"changed_write":changed_write.is_some()})
        }
        "enospc" => {
            let mut b = buffer(&root, "xid-enospc", 0, 4096, 32, 2);
            let error = push(&mut b, b"1234").unwrap_err();
            let clock = VirtualClock::new(1000);
            let mut random = ZeroRandom;
            let mut context = TransitionContext {
                clock: &clock,
                randomness: &mut random,
            };
            let (out, action, prepared) =
                b.apply_failure_policy(&error, None, "disk-a", None, &mut context);
            json!({"outcome":out,"error":format!("{error:?}"),"policy_action":format!("{action:?}"),"prepared_persistence":prepared.is_some()})
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
            let mut startup_memory = MemoryBudget::new(MemoryLimits {
                process_limit: 1_048_576,
                runtime_fixed: 1,
                receive: 1,
                decoder: 65_536,
                staging: 65_536,
            })
            .unwrap();
            let blocked = classify_startup_spools(
                &lock,
                &store,
                &root,
                "capture-epoch-v1",
                "owner-run",
                512,
                32,
                2,
                8,
                &mut startup_memory,
                |_, xid| {
                    if xid == "xid-dead" {
                        ExistingTransaction::Uncommitted
                    } else {
                        ExistingTransaction::Contradictory
                    }
                },
            );
            assert!(matches!(blocked, Err(SpoolError::StartupBlocked)));
            let quarantined = fs::read_dir(root.join("quarantine")).unwrap().count();
            let remaining = fs::read_dir(&root)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|e| e.path().extension().and_then(|v| v.to_str()) == Some("spool"))
                .count();
            json!({"outcome":"startup_blocked_for_quarantine","removed":1,"quarantined":quarantined,"remaining_spools":remaining,"feedback_permitted":false})
        }
        _ => panic!("unknown mode"),
    };
    println!("{}", serde_json::to_string(&output).unwrap());
}
