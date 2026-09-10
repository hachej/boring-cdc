//! Bounded incremental transaction spool and capture admission controller.
//!
//! This module owns no replication loop and sends no feedback. Callers incrementally transfer
//! one CopyBoth frame at a time, then consume exactly one commit iterator. Every memory and disk
//! allocation is admitted before it is made.

use crate::failure_policy::{
    Component, FailedBoundary, FailureClass, FailureObservation, FailureRecord, FingerprintInput,
    PolicyAction, PolicyEvent, PreparedFailureOperation, SafeContextKey, SafeContextValue,
    StableErrorCode, build_fingerprint, transition,
};
use crate::m1_transition_kernel::TransitionContext;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"BCDCSP01";
const HEADER_WORK_BYTES: usize = 8 * 1024;
const READER_WORK_BYTES: usize = 16 * 1024;
// M0-PROVISIONAL: boring-cdc-d-admission must approve the spool format version.
pub const SPOOL_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum MemoryClass {
    Receive,
    Decoder,
    Staging,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryLimits {
    pub process_limit: usize,
    pub runtime_fixed: usize,
    pub receive: usize,
    pub decoder: usize,
    pub staging: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct MemoryHighWater {
    pub receive_bytes: usize,
    pub decoder_bytes: usize,
    pub staging_bytes: usize,
    pub aggregate_bytes: usize,
}

#[derive(Debug, Eq, PartialEq)]
pub enum SpoolError {
    Invalid(&'static str),
    MemoryLimit(MemoryClass),
    FrameLimit { limit: usize, observed: usize },
    EventLimit { limit: usize, observed: usize },
    TransactionBytesLimit { limit: u64, observed: u64 },
    TransactionEventsLimit { limit: u64, observed: u64 },
    DiskReserve { filesystem: u64, requested: u64 },
    Checksum,
    Io(io::ErrorKind),
    Enospc,
    StartupBlocked,
}
impl std::fmt::Display for SpoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for SpoolError {}
impl From<io::Error> for SpoolError {
    fn from(value: io::Error) -> Self {
        if value.raw_os_error() == Some(libc::ENOSPC) {
            Self::Enospc
        } else {
            Self::Io(value.kind())
        }
    }
}

pub struct MemoryBudget {
    limits: MemoryLimits,
    used: [usize; 3],
    high: MemoryHighWater,
}
impl MemoryBudget {
    pub fn new(limits: MemoryLimits) -> Result<Self, SpoolError> {
        let named = limits
            .receive
            .checked_add(limits.decoder)
            .and_then(|v| v.checked_add(limits.staging))
            .ok_or(SpoolError::Invalid("memory equation overflow"))?;
        let aggregate = limits
            .runtime_fixed
            .checked_add(named)
            .ok_or(SpoolError::Invalid("memory equation overflow"))?;
        if limits.process_limit == 0
            || limits.receive == 0
            || limits.decoder == 0
            || limits.staging == 0
            || aggregate > limits.process_limit
        {
            return Err(SpoolError::Invalid(
                "aggregate runtime memory equation exceeds process limit",
            ));
        }
        Ok(Self {
            limits,
            used: [0; 3],
            high: MemoryHighWater::default(),
        })
    }
    fn index(class: MemoryClass) -> usize {
        match class {
            MemoryClass::Receive => 0,
            MemoryClass::Decoder => 1,
            MemoryClass::Staging => 2,
        }
    }
    fn class_limit(&self, class: MemoryClass) -> usize {
        match class {
            MemoryClass::Receive => self.limits.receive,
            MemoryClass::Decoder => self.limits.decoder,
            MemoryClass::Staging => self.limits.staging,
        }
    }
    pub fn reserve(&mut self, class: MemoryClass, bytes: usize) -> Result<(), SpoolError> {
        let i = Self::index(class);
        let next = self.used[i]
            .checked_add(bytes)
            .ok_or(SpoolError::MemoryLimit(class))?;
        if next > self.class_limit(class) {
            return Err(SpoolError::MemoryLimit(class));
        }
        let aggregate = self
            .limits
            .runtime_fixed
            .checked_add(self.used.iter().sum::<usize>())
            .and_then(|v| v.checked_add(bytes))
            .ok_or(SpoolError::MemoryLimit(class))?;
        if aggregate > self.limits.process_limit {
            return Err(SpoolError::MemoryLimit(class));
        }
        self.used[i] = next;
        self.high.receive_bytes = self.high.receive_bytes.max(self.used[0]);
        self.high.decoder_bytes = self.high.decoder_bytes.max(self.used[1]);
        self.high.staging_bytes = self.high.staging_bytes.max(self.used[2]);
        self.high.aggregate_bytes = self.high.aggregate_bytes.max(aggregate);
        Ok(())
    }
    pub fn release(&mut self, class: MemoryClass, bytes: usize) {
        let i = Self::index(class);
        assert!(self.used[i] >= bytes, "memory reservation underflow");
        self.used[i] -= bytes;
    }
    pub fn high_water(&self) -> MemoryHighWater {
        self.high
    }
    pub fn used(&self, class: MemoryClass) -> usize {
        self.used[Self::index(class)]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FilesystemLimit {
    pub total_budget: u64,
    pub emergency_reserve: u64,
}
#[derive(Default)]
pub struct DiskAdmission {
    limits: HashMap<u64, FilesystemLimit>,
    reserved: HashMap<u64, u64>,
}
impl DiskAdmission {
    pub fn configure(&mut self, filesystem: u64, limit: FilesystemLimit) -> Result<(), SpoolError> {
        if limit.total_budget == 0 || limit.emergency_reserve >= limit.total_budget {
            return Err(SpoolError::Invalid("invalid filesystem reserve"));
        }
        self.limits.insert(filesystem, limit);
        Ok(())
    }
    pub fn admit(&mut self, filesystem: u64, available: u64, bytes: u64) -> Result<(), SpoolError> {
        let limit = *self
            .limits
            .get(&filesystem)
            .ok_or(SpoolError::Invalid("filesystem budget missing"))?;
        let current = *self.reserved.get(&filesystem).unwrap_or(&0);
        let next = current.checked_add(bytes).ok_or(SpoolError::DiskReserve {
            filesystem,
            requested: bytes,
        })?;
        if next > limit.total_budget - limit.emergency_reserve
            || available.saturating_sub(next) < limit.emergency_reserve
        {
            return Err(SpoolError::DiskReserve {
                filesystem,
                requested: bytes,
            });
        }
        self.reserved.insert(filesystem, next);
        Ok(())
    }
    /// Accounts bytes already held by SQLite or another approved user on this filesystem.
    pub fn account_existing(&mut self, filesystem: u64, bytes: u64) -> Result<(), SpoolError> {
        let limit = *self
            .limits
            .get(&filesystem)
            .ok_or(SpoolError::Invalid("filesystem budget missing"))?;
        if bytes > limit.total_budget.saturating_sub(limit.emergency_reserve) {
            return Err(SpoolError::DiskReserve {
                filesystem,
                requested: bytes,
            });
        }
        self.reserved.insert(filesystem, bytes);
        Ok(())
    }
    pub fn release(&mut self, filesystem: u64, bytes: u64) {
        let value = self.reserved.entry(filesystem).or_default();
        *value = value.saturating_sub(bytes);
    }
    pub fn reserved(&self, filesystem: u64) -> u64 {
        *self.reserved.get(&filesystem).unwrap_or(&0)
    }
}

#[derive(Clone)]
pub struct FilesystemAdmissionController(std::rc::Rc<RefCell<DiskAdmission>>);
impl FilesystemAdmissionController {
    pub fn new(admission: DiskAdmission) -> Self {
        Self(std::rc::Rc::new(RefCell::new(admission)))
    }
    pub fn reserved(&self, filesystem: u64) -> u64 {
        self.0.borrow().reserved(filesystem)
    }
}

pub trait FreeSpace {
    fn available_bytes(&self, path: &Path) -> io::Result<u64>;
}
pub struct StatvfsSpace;
impl FreeSpace for StatvfsSpace {
    fn available_bytes(&self, path: &Path) -> io::Result<u64> {
        let c = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        if unsafe { libc::statvfs(c.as_ptr(), stat.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let stat = unsafe { stat.assume_init() };
        Ok((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
    }
}

#[derive(Clone, Debug)]
pub struct SpoolLimits {
    pub max_frame_bytes: usize,
    pub max_event_bytes: usize,
    pub max_transaction_bytes: u64,
    pub max_transaction_events: u64,
    pub memory_prefix_bytes: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct Header {
    version: u32,
    capture_epoch: String,
    xid: String,
    creation_run: String,
}

pub struct ReceiveBuffer {
    expected: usize,
    bytes: Vec<u8>,
}
impl ReceiveBuffer {
    pub fn capacity(&self) -> usize {
        self.expected
    }
    pub fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), SpoolError> {
        if self.bytes.len().saturating_add(bytes.len()) > self.expected {
            return Err(SpoolError::Invalid("receive buffer overflow"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}

pub struct TxnBuffer {
    directory: PathBuf,
    filesystem: u64,
    header: Header,
    limits: SpoolLimits,
    memory: MemoryBudget,
    disk: FilesystemAdmissionController,
    space: Box<dyn FreeSpace>,
    memory_events: Vec<Vec<u8>>,
    metadata_reserved: usize,
    memory_payload: usize,
    spool: Option<(File, PathBuf, u64, u64)>,
    bytes: u64,
    events: u64,
    spill_bytes: u64,
    failed: bool,
    receive_admitted: Option<usize>,
}
impl TxnBuffer {
    pub fn new(
        directory: PathBuf,
        capture_epoch: String,
        xid: String,
        creation_run: String,
        limits: SpoolLimits,
        memory: MemoryBudget,
        disk: FilesystemAdmissionController,
        space: Box<dyn FreeSpace>,
    ) -> Result<Self, SpoolError> {
        if capture_epoch.is_empty()
            || xid.is_empty()
            || creation_run.is_empty()
            || capture_epoch.len() > 256
            || xid.len() > 256
            || creation_run.len() > 256
            || limits.max_frame_bytes == 0
            || limits.max_event_bytes == 0
            || limits.max_transaction_bytes == 0
            || limits.max_transaction_events == 0
        {
            return Err(SpoolError::Invalid("zero or empty transaction spool input"));
        }
        fs::create_dir_all(&directory)?;
        let dev = fs::metadata(&directory)?.dev();
        if !disk.0.borrow().limits.contains_key(&dev) {
            return Err(SpoolError::Invalid("spool filesystem budget missing"));
        }
        let event_capacity = usize::try_from(limits.max_transaction_events)
            .map_err(|_| SpoolError::Invalid("transaction event capacity overflow"))?;
        let metadata_reserved = event_capacity
            .checked_mul(std::mem::size_of::<Vec<u8>>())
            .ok_or(SpoolError::Invalid("transaction event metadata overflow"))?;
        let mut memory = memory;
        memory.reserve(MemoryClass::Staging, metadata_reserved)?;
        let memory_events = Vec::with_capacity(event_capacity);
        Ok(Self {
            directory,
            filesystem: dev,
            header: Header {
                version: SPOOL_FORMAT_VERSION,
                capture_epoch,
                xid,
                creation_run,
            },
            limits,
            memory,
            disk,
            space,
            memory_events,
            metadata_reserved,
            memory_payload: 0,
            spool: None,
            bytes: 0,
            events: 0,
            spill_bytes: 0,
            failed: false,
            receive_admitted: None,
        })
    }
    /// Reserves receive memory before the transport reads or allocates the frame.
    pub fn admit_receive(&mut self, frame_len: usize) -> Result<ReceiveBuffer, SpoolError> {
        if self.failed || self.receive_admitted.is_some() {
            return Err(SpoolError::Invalid(
                "transaction failed or receive already admitted",
            ));
        }
        if frame_len > self.limits.max_frame_bytes {
            self.failed = true;
            return Err(SpoolError::FrameLimit {
                limit: self.limits.max_frame_bytes,
                observed: frame_len,
            });
        }
        if frame_len > self.limits.max_event_bytes {
            self.failed = true;
            return Err(SpoolError::EventLimit {
                limit: self.limits.max_event_bytes,
                observed: frame_len,
            });
        }
        let observed_bytes =
            self.bytes
                .checked_add(frame_len as u64)
                .ok_or(SpoolError::TransactionBytesLimit {
                    limit: self.limits.max_transaction_bytes,
                    observed: u64::MAX,
                })?;
        if observed_bytes > self.limits.max_transaction_bytes {
            self.failed = true;
            return Err(SpoolError::TransactionBytesLimit {
                limit: self.limits.max_transaction_bytes,
                observed: observed_bytes,
            });
        }
        let observed_events = self.events.saturating_add(1);
        if observed_events > self.limits.max_transaction_events {
            self.failed = true;
            return Err(SpoolError::TransactionEventsLimit {
                limit: self.limits.max_transaction_events,
                observed: observed_events,
            });
        }
        self.memory.reserve(MemoryClass::Receive, frame_len)?;
        self.receive_admitted = Some(frame_len);
        Ok(ReceiveBuffer {
            expected: frame_len,
            bytes: Vec::with_capacity(frame_len),
        })
    }
    /// Transfers one pre-admitted frame into decoder then staging or the single overflow file.
    pub fn push_received(&mut self, frame: ReceiveBuffer) -> Result<(), SpoolError> {
        let frame_len = frame.expected;
        if self.receive_admitted.take() != Some(frame_len) || frame.bytes.len() != frame_len {
            self.failed = true;
            self.memory.release(MemoryClass::Receive, frame_len);
            return Err(SpoolError::Invalid("receive permit length mismatch"));
        }
        if let Err(error) = self.memory.reserve(MemoryClass::Decoder, frame_len) {
            self.memory.release(MemoryClass::Receive, frame_len);
            self.failed = true;
            return Err(error);
        }
        let result = self.stage_frame(frame.bytes);
        self.memory.release(MemoryClass::Decoder, frame_len);
        self.memory.release(MemoryClass::Receive, frame_len);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    pub fn cancel_receive(&mut self, frame: ReceiveBuffer) -> Result<(), SpoolError> {
        if self.receive_admitted.take() != Some(frame.expected) {
            return Err(SpoolError::Invalid("stale receive permit"));
        }
        self.memory.release(MemoryClass::Receive, frame.expected);
        Ok(())
    }
    fn stage_frame(&mut self, frame: Vec<u8>) -> Result<(), SpoolError> {
        let next_bytes = self.bytes + frame.len() as u64;
        let next_events = self.events + 1;
        if self.spool.is_none()
            && self
                .memory_payload
                .checked_add(frame.len())
                .is_some_and(|n| n <= self.limits.memory_prefix_bytes)
        {
            self.memory.reserve(MemoryClass::Staging, frame.len())?;
            self.memory_payload += frame.len();
            self.memory_events.push(frame);
        } else {
            self.ensure_spool()?;
            self.append_file(&frame)?;
        }
        self.bytes = next_bytes;
        self.events = next_events;
        Ok(())
    }
    fn ensure_spool(&mut self) -> Result<(), SpoolError> {
        if self.spool.is_some() {
            return Ok(());
        }
        self.memory
            .reserve(MemoryClass::Decoder, HEADER_WORK_BYTES)?;
        let name = spool_name(&self.header);
        let path = self.directory.join(name);
        let opened = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path);
        let mut file = match opened {
            Ok(file) => file,
            Err(error) => {
                self.memory.release(MemoryClass::Decoder, HEADER_WORK_BYTES);
                return Err(error.into());
            }
        };
        let header = match serde_json::to_vec(&self.header) {
            Ok(header) => header,
            Err(_) => {
                self.memory.release(MemoryClass::Decoder, HEADER_WORK_BYTES);
                fs::remove_file(&path).ok();
                return Err(SpoolError::Invalid("spool header encoding"));
            }
        };
        if header.len() > HEADER_WORK_BYTES / 2 {
            self.memory.release(MemoryClass::Decoder, HEADER_WORK_BYTES);
            fs::remove_file(&path).ok();
            return Err(SpoolError::MemoryLimit(MemoryClass::Decoder));
        }
        let header_len = (header.len() as u32).to_be_bytes();
        let header_sum = Sha256::digest(&header);
        let dev = self.filesystem;
        let prefix_result = self.preallocate_parts(
            &mut file,
            dev,
            0,
            &[MAGIC, &header_len, &header, &header_sum],
        );
        self.memory.release(MemoryClass::Decoder, HEADER_WORK_BYTES);
        let prefix_len = match prefix_result {
            Ok(value) => value,
            Err(error) => {
                drop(file);
                fs::remove_file(&path).ok();
                sync_dir(&self.directory).ok();
                return Err(error);
            }
        };
        self.spool = Some((file, path, prefix_len, dev));
        let prior = std::mem::take(&mut self.memory_events);
        let prior_payload: usize = prior.iter().map(Vec::len).sum();
        let flush_result = prior.iter().try_for_each(|event| self.append_file(event));
        drop(prior);
        self.memory.release(MemoryClass::Staging, prior_payload);
        self.memory_payload = 0;
        self.memory
            .release(MemoryClass::Staging, self.metadata_reserved);
        self.metadata_reserved = 0;
        flush_result
    }
    fn append_file(&mut self, event: &[u8]) -> Result<(), SpoolError> {
        let event_len = (event.len() as u32).to_be_bytes();
        let checksum = Sha256::digest(event);
        let (mut file, path, offset, dev) = self.spool.take().expect("spool exists");
        let result =
            self.preallocate_parts(&mut file, dev, offset, &[&event_len, event, &checksum]);
        let next = result.as_ref().map_or(offset, |written| offset + written);
        self.spool = Some((file, path, next, dev));
        if result.is_ok() {
            self.spill_bytes = self.spill_bytes.saturating_add(event.len() as u64);
        }
        result.map(|_| ())
    }
    fn preallocate_parts(
        &mut self,
        file: &mut File,
        dev: u64,
        offset: u64,
        parts: &[&[u8]],
    ) -> Result<u64, SpoolError> {
        let bytes = parts
            .iter()
            .try_fold(0_u64, |total, p| total.checked_add(p.len() as u64))
            .ok_or(SpoolError::DiskReserve {
                filesystem: dev,
                requested: u64::MAX,
            })?;
        let available = self.space.available_bytes(&self.directory)?;
        self.disk.0.borrow_mut().admit(dev, available, bytes)?;
        let result = (|| {
            let code = unsafe {
                libc::posix_fallocate(
                    file.as_raw_fd(),
                    offset as libc::off_t,
                    bytes as libc::off_t,
                )
            };
            if code != 0 {
                return Err(io::Error::from_raw_os_error(code));
            }
            file.seek(SeekFrom::Start(offset))?;
            for part in parts {
                file.write_all(part)?;
            }
            Ok::<_, io::Error>(())
        })();
        if let Err(error) = result {
            if file.set_len(offset).is_ok() {
                self.disk.0.borrow_mut().release(dev, bytes);
            }
            return Err(error.into());
        }
        Ok(bytes)
    }
    pub fn high_water(&self) -> MemoryHighWater {
        self.memory.high_water()
    }
    pub fn observed(&self) -> (u64, u64) {
        (self.bytes, self.events)
    }
    pub fn spill_bytes(&self) -> u64 {
        self.spill_bytes
    }
    pub fn stream_events(&self) -> u64 {
        0
    }
    pub fn commit_iter(&mut self) -> Result<CommitIter<'_>, SpoolError> {
        if self.failed || self.events == 0 {
            return Err(SpoolError::Invalid("failed or empty transaction"));
        }
        if self.spool.is_some() {
            let reservation = self.limits.max_event_bytes;
            self.memory
                .reserve(MemoryClass::Decoder, READER_WORK_BYTES)?;
            if let Err(error) = self.memory.reserve(MemoryClass::Staging, reservation) {
                self.memory.release(MemoryClass::Decoder, READER_WORK_BYTES);
                return Err(error);
            }
            let prepared = (|| {
                let (file, _, _, _) = self.spool.as_mut().expect("spool");
                file.sync_all()?;
                file.seek(SeekFrom::Start(0))?;
                let mut reader = BufReader::new(file.try_clone()?);
                read_header(&mut reader)?;
                Ok::<_, SpoolError>(reader)
            })();
            let reader = match prepared {
                Ok(reader) => reader,
                Err(error) => {
                    self.memory.release(MemoryClass::Staging, reservation);
                    self.memory.release(MemoryClass::Decoder, READER_WORK_BYTES);
                    return Err(error);
                }
            };
            return Ok(CommitIter::File {
                reader,
                remaining: self.events,
                max_event_bytes: self.limits.max_event_bytes,
                reservation,
                reader_reservation: READER_WORK_BYTES,
                owner: self,
            });
        }
        Ok(CommitIter::Memory {
            events: self.memory_events.iter(),
            _owner: self,
        })
    }
    pub fn finish(mut self) -> Result<(), SpoolError> {
        if let Some((file, path, len, dev)) = self.spool.take() {
            file.sync_all()?;
            drop(file);
            fs::remove_file(path)?;
            sync_dir(&self.directory)?;
            self.disk.0.borrow_mut().release(dev, len);
        }
        Ok(())
    }
    /// Runs the sole shared policy core and projects its exact CAS fences into the supplied
    /// capture-priority writer operation. Execution remains with the sole writer.
    pub fn apply_failure_policy(
        &self,
        error: &SpoolError,
        end_lsn: Option<&str>,
        config_fingerprint: &str,
        current: Option<&FailureRecord>,
        context: &mut TransitionContext<'_>,
    ) -> (
        CaptureAdmissionOutcome,
        PolicyAction,
        Option<PreparedFailureOperation>,
    ) {
        let (observation, outcome) = self.failure_observation(error, end_lsn, config_fingerprint);
        let action = transition(current, PolicyEvent::Observe(observation), context);
        let prepared = PreparedFailureOperation::from_policy_action(action.clone(), None);
        (outcome, action, prepared)
    }

    pub fn failure_observation(
        &self,
        error: &SpoolError,
        end_lsn: Option<&str>,
        config_fingerprint: &str,
    ) -> (FailureObservation, CaptureAdmissionOutcome) {
        let (class, code, limit_kind) = match error {
            SpoolError::Io(_) | SpoolError::Enospc | SpoolError::DiskReserve { .. } => (
                FailureClass::TransientIo,
                StableErrorCode::ResourceLimit,
                SafeContextValue::JournalBytes,
            ),
            SpoolError::Checksum => (
                FailureClass::Integrity,
                StableErrorCode::ChecksumMismatch,
                SafeContextValue::JournalBytes,
            ),
            SpoolError::Invalid(_) => (
                FailureClass::Unsupported,
                StableErrorCode::InvalidRecord,
                SafeContextValue::MemoryBytes,
            ),
            _ => (
                FailureClass::Configuration,
                StableErrorCode::ResourceLimit,
                SafeContextValue::MemoryBytes,
            ),
        };
        let observed_bytes = match error {
            SpoolError::FrameLimit { observed, .. } | SpoolError::EventLimit { observed, .. } => {
                self.bytes.max(*observed as u64)
            }
            SpoolError::TransactionBytesLimit { observed, .. } => *observed,
            _ => self.bytes,
        };
        let observed_events = match error {
            SpoolError::TransactionEventsLimit { observed, .. } => *observed,
            SpoolError::FrameLimit { .. } | SpoolError::EventLimit { .. } => {
                self.events.saturating_add(1)
            }
            _ => self.events,
        };
        let input = FingerprintInput {
            component: Component::Capture,
            class,
            code,
            boundary: FailedBoundary::Capture {
                capture_epoch: self.header.capture_epoch.clone(),
                end_lsn: end_lsn.unwrap_or("unknown").into(),
            },
            relevant_configuration_fingerprint: config_fingerprint.into(),
            context: BTreeMap::from([(SafeContextKey::LimitKind, limit_kind)]),
        };
        let fingerprint = build_fingerprint(&input);
        (
            FailureObservation {
                fingerprint: input,
                destination_id: None,
            },
            CaptureAdmissionOutcome::SafeStopped {
                fingerprint,
                xid: self.header.xid.clone(),
                end_lsn: end_lsn.map(str::to_owned),
                observed_bytes,
                observed_events,
                feedback_permitted: false,
            },
        )
    }
}

pub enum CommitEvent<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}
impl AsRef<[u8]> for CommitEvent<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Borrowed(v) => v,
            Self::Owned(v) => v,
        }
    }
}
pub enum CommitIter<'a> {
    Memory {
        events: std::slice::Iter<'a, Vec<u8>>,
        _owner: &'a TxnBuffer,
    },
    File {
        reader: BufReader<File>,
        remaining: u64,
        max_event_bytes: usize,
        reservation: usize,
        reader_reservation: usize,
        owner: &'a mut TxnBuffer,
    },
}
impl<'a> Iterator for CommitIter<'a> {
    type Item = Result<CommitEvent<'a>, SpoolError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Memory { events, .. } => events.next().map(|v| Ok(CommitEvent::Borrowed(v))),
            Self::File {
                reader,
                remaining,
                max_event_bytes,
                ..
            } => {
                if *remaining == 0 {
                    return None;
                }
                *remaining -= 1;
                Some(read_record(reader, *max_event_bytes).map(CommitEvent::Owned))
            }
        }
    }
}
impl Drop for CommitIter<'_> {
    fn drop(&mut self) {
        if let Self::File {
            reservation,
            reader_reservation,
            owner,
            ..
        } = self
        {
            owner.memory.release(MemoryClass::Staging, *reservation);
            owner
                .memory
                .release(MemoryClass::Decoder, *reader_reservation);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CaptureAdmissionOutcome {
    SafeStopped {
        fingerprint: String,
        xid: String,
        end_lsn: Option<String>,
        observed_bytes: u64,
        observed_events: u64,
        feedback_permitted: bool,
    },
}

fn spool_name(h: &Header) -> String {
    let raw = serde_json::to_vec(h).expect("header");
    format!("txn-{:x}.spool", Sha256::digest(raw))
}
fn read_header(reader: &mut impl Read) -> Result<Header, SpoolError> {
    let mut magic = [0; 8];
    reader.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(SpoolError::Checksum);
    }
    let mut n = [0; 4];
    reader.read_exact(&mut n)?;
    let len = u32::from_be_bytes(n) as usize;
    if len > 16 * 1024 {
        return Err(SpoolError::Invalid("spool header too large"));
    }
    let mut raw = vec![0; len];
    reader.read_exact(&mut raw)?;
    let mut checksum = [0_u8; 32];
    reader.read_exact(&mut checksum)?;
    if Sha256::digest(&raw).as_slice() != checksum {
        return Err(SpoolError::Checksum);
    }
    let h: Header = serde_json::from_slice(&raw).map_err(|_| SpoolError::Checksum)?;
    if h.version != SPOOL_FORMAT_VERSION
        || h.capture_epoch.is_empty()
        || h.xid.is_empty()
        || h.creation_run.is_empty()
    {
        return Err(SpoolError::Invalid("invalid spool header"));
    }
    Ok(h)
}
fn read_record(reader: &mut impl Read, max_event_bytes: usize) -> Result<Vec<u8>, SpoolError> {
    let mut n = [0; 4];
    reader.read_exact(&mut n)?;
    let len = u32::from_be_bytes(n) as usize;
    if len > max_event_bytes {
        return Err(SpoolError::EventLimit {
            limit: max_event_bytes,
            observed: len,
        });
    }
    let mut value = vec![0; len];
    reader.read_exact(&mut value)?;
    let mut sum = [0; 32];
    reader.read_exact(&mut sum)?;
    if Sha256::digest(&value).as_slice() != sum {
        return Err(SpoolError::Checksum);
    }
    Ok(value)
}
fn sync_dir(path: &Path) -> Result<(), SpoolError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExistingTransaction {
    CommittedSame,
    Uncommitted,
    Contradictory,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum StartupAction {
    RemovedCommitted(PathBuf),
    RemovedUncommitted(PathBuf),
    Quarantined(PathBuf),
}
/// Requires the caller to hold the repository's exclusive runtime state lock. Unknown entries are
/// never deleted. Every malformed, unowned, or contradictory spool is quarantined and blocks.
pub fn classify_startup_spools(
    lock: &crate::m2_ownership::StateLock,
    store: &Path,
    directory: &Path,
    current_epoch: &str,
    current_run: &str,
    max_event_bytes: usize,
    max_transaction_bytes: u64,
    max_transaction_events: u64,
    mut existing: impl FnMut(&str, &str) -> ExistingTransaction,
) -> Result<Vec<StartupAction>, SpoolError> {
    if !lock.protects_store(store) {
        return Err(SpoolError::Invalid(
            "startup lock does not protect state store",
        ));
    }
    if max_event_bytes == 0 || max_transaction_bytes == 0 || max_transaction_events == 0 {
        return Err(SpoolError::Invalid("zero startup scan bound"));
    }
    let quarantine = directory.join("quarantine");
    fs::create_dir_all(&quarantine)?;
    let mut actions = vec![];
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.extension().and_then(|x| x.to_str()) != Some("spool") {
            continue;
        }
        let parsed = (|| {
            let mut r = BufReader::new(File::open(&path)?);
            let h = read_header(&mut r)?;
            if path.file_name().and_then(|v| v.to_str()) != Some(spool_name(&h).as_str()) {
                return Err(SpoolError::Checksum);
            }
            let mut bytes = 0_u64;
            let mut events = 0_u64;
            loop {
                let mut peek = [0; 1];
                match r.read(&mut peek)? {
                    0 => break,
                    1 => {
                        r.seek_relative(-1)?;
                        if events >= max_transaction_events {
                            return Err(SpoolError::TransactionEventsLimit {
                                limit: max_transaction_events,
                                observed: events + 1,
                            });
                        }
                        let remaining = max_transaction_bytes.saturating_sub(bytes);
                        let allocation_limit =
                            max_event_bytes.min(usize::try_from(remaining).unwrap_or(usize::MAX));
                        let event = read_record(&mut r, allocation_limit)?;
                        bytes = bytes.checked_add(event.len() as u64).ok_or(
                            SpoolError::TransactionBytesLimit {
                                limit: max_transaction_bytes,
                                observed: u64::MAX,
                            },
                        )?;
                        events += 1;
                        if bytes > max_transaction_bytes {
                            return Err(SpoolError::TransactionBytesLimit {
                                limit: max_transaction_bytes,
                                observed: bytes,
                            });
                        }
                        if events > max_transaction_events {
                            return Err(SpoolError::TransactionEventsLimit {
                                limit: max_transaction_events,
                                observed: events,
                            });
                        }
                    }
                    _ => unreachable!(),
                }
            }
            Ok::<_, SpoolError>(h)
        })();
        let action = match parsed {
            Ok(h) if h.capture_epoch == current_epoch && h.creation_run != current_run => {
                match existing(&h.capture_epoch, &h.xid) {
                    ExistingTransaction::CommittedSame => {
                        fs::remove_file(&path)?;
                        StartupAction::RemovedCommitted(path.clone())
                    }
                    ExistingTransaction::Uncommitted => {
                        fs::remove_file(&path)?;
                        StartupAction::RemovedUncommitted(path.clone())
                    }
                    ExistingTransaction::Contradictory => quarantine_file(&path, &quarantine)?,
                }
            }
            _ => quarantine_file(&path, &quarantine)?,
        };
        actions.push(action);
    }
    sync_dir(directory)?;
    sync_dir(&quarantine)?;
    if actions
        .iter()
        .any(|a| matches!(a, StartupAction::Quarantined(_)))
    {
        return Err(SpoolError::StartupBlocked);
    }
    Ok(actions)
}
fn quarantine_file(path: &Path, dir: &Path) -> Result<StartupAction, SpoolError> {
    let name = path
        .file_name()
        .ok_or(SpoolError::Invalid("spool filename"))?;
    let target = dir.join(name);
    fs::rename(path, &target)?;
    Ok(StartupAction::Quarantined(target))
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::m1_transition_kernel::{Randomness, TransitionContext, VirtualClock};
    use crate::m2_ownership::StateLock;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    struct ZeroRandom;
    impl Randomness for ZeroRandom {
        fn next_u64(&mut self) -> u64 {
            0
        }
    }
    struct FixedSpace(u64);
    impl FreeSpace for FixedSpace {
        fn available_bytes(&self, _: &Path) -> io::Result<u64> {
            Ok(self.0)
        }
    }
    fn dir(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "m2-spool-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&p).unwrap();
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&p, fs::Permissions::from_mode(0o700)).unwrap();
        p
    }
    fn push(buffer: &mut TxnBuffer, bytes: &[u8]) -> Result<(), SpoolError> {
        let mut receive = buffer.admit_receive(bytes.len())?;
        receive.extend_from_slice(bytes)?;
        buffer.push_received(receive)
    }
    fn setup(name: &str, prefix: usize, space: u64) -> TxnBuffer {
        let d = dir(name);
        let dev = fs::metadata(&d).unwrap().dev();
        let mut disk = DiskAdmission::default();
        disk.configure(
            dev,
            FilesystemLimit {
                total_budget: 4096,
                emergency_reserve: 512,
            },
        )
        .unwrap();
        let mem = MemoryBudget::new(MemoryLimits {
            process_limit: 65_536,
            runtime_fixed: 4_096,
            receive: 1_024,
            decoder: 32_768,
            staging: 16_384,
        })
        .unwrap();
        TxnBuffer::new(
            d,
            "epoch".into(),
            "xid".into(),
            "dead-run".into(),
            SpoolLimits {
                max_frame_bytes: 512,
                max_event_bytes: 512,
                max_transaction_bytes: 1024,
                max_transaction_events: 4,
                memory_prefix_bytes: prefix,
            },
            mem,
            FilesystemAdmissionController::new(disk),
            Box::new(FixedSpace(space)),
        )
        .unwrap()
    }
    #[test]
    fn memory_equation_and_high_water_are_aggregate() {
        assert!(
            MemoryBudget::new(MemoryLimits {
                process_limit: 10,
                runtime_fixed: 4,
                receive: 3,
                decoder: 3,
                staging: 3
            })
            .is_err()
        );
        let mut b = setup("memory", 512, 4096);
        push(&mut b, &vec![1; 100]).unwrap();
        let h = b.high_water();
        assert_eq!(
            (h.receive_bytes, h.decoder_bytes, h.staging_bytes),
            (100, 100, 100 + 4 * std::mem::size_of::<Vec<u8>>())
        );
        assert!(h.aggregate_bytes <= 65_536);
        b.finish().unwrap();
    }
    #[test]
    fn incremental_overflow_has_one_iterator_and_no_whole_transaction_collection() {
        let mut b = setup("overflow", 3, 4096);
        push(&mut b, b"one").unwrap();
        push(&mut b, b"two").unwrap();
        assert!(b.spool.is_some());
        let values = b
            .commit_iter()
            .unwrap()
            .map(|value| value.map(|event| event.as_ref().to_vec()))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(values, vec![b"one".to_vec(), b"two".to_vec()]);
        drop(values);
        b.finish().unwrap();
    }
    #[test]
    fn frame_event_transaction_limits_poison_and_never_permit_feedback() {
        let mut b = setup("limits", 512, 4096);
        assert!(matches!(
            b.admit_receive(513),
            Err(SpoolError::FrameLimit { .. })
        ));
        assert!(b.commit_iter().is_err());
        let mut event = setup("event-limit", 512, 4096);
        event.limits.max_event_bytes = 2;
        assert!(matches!(
            event.admit_receive(3),
            Err(SpoolError::EventLimit { observed: 3, .. })
        ));
        let mut bytes = setup("byte-limit", 512, 4096);
        bytes.limits.max_transaction_bytes = 3;
        push(&mut bytes, b"123").unwrap();
        assert!(matches!(
            bytes.admit_receive(1),
            Err(SpoolError::TransactionBytesLimit { observed: 4, .. })
        ));
        let mut events = setup("event-count", 512, 4096);
        events.limits.max_transaction_events = 1;
        push(&mut events, b"1").unwrap();
        assert!(matches!(
            events.admit_receive(1),
            Err(SpoolError::TransactionEventsLimit { observed: 2, .. })
        ));
        let clock = VirtualClock::new(1000);
        let mut random = ZeroRandom;
        let mut context = TransitionContext {
            clock: &clock,
            randomness: &mut random,
        };
        let (out, action, prepared) = b.apply_failure_policy(
            &SpoolError::TransactionBytesLimit {
                limit: 1,
                observed: 2,
            },
            Some("0000000000000010"),
            "cfg",
            None,
            &mut context,
        );
        assert!(matches!(
            out,
            CaptureAdmissionOutcome::SafeStopped {
                feedback_permitted: false,
                observed_bytes: 2,
                ..
            }
        ));
        assert!(
            matches!(action, PolicyAction::Persist(_))
                && matches!(prepared, Some(PreparedFailureOperation::StoreAndArm { .. }))
        );
        b.finish().unwrap();
    }
    #[test]
    fn filesystem_reserve_rejects_before_write_and_preserves_emergency_space() {
        let mut b = setup("enospc", 0, 520);
        let e = push(&mut b, &vec![1; 32]).unwrap_err();
        assert!(matches!(e, SpoolError::DiskReserve { .. }));
        assert!(b.spool.as_ref().map(|x| x.2).unwrap_or(0) == 0);
        b.finish().unwrap();
        let shared_dir = dir("shared-disk");
        let dev = fs::metadata(&shared_dir).unwrap().dev();
        let mut admission = DiskAdmission::default();
        admission
            .configure(
                dev,
                FilesystemLimit {
                    total_budget: 1000,
                    emergency_reserve: 100,
                },
            )
            .unwrap();
        admission.account_existing(dev, 100).unwrap();
        let shared = FilesystemAdmissionController::new(admission);
        shared.0.borrow_mut().admit(dev, 600, 400).unwrap();
        assert_eq!(shared.clone().reserved(dev), 500);
        assert!(matches!(
            shared.0.borrow_mut().admit(dev, 600, 1),
            Err(SpoolError::DiskReserve { .. })
        ));
        fs::remove_dir_all(shared_dir).ok();
    }
    #[test]
    fn checksum_failure_is_detected_by_commit_iterator() {
        let mut b = setup("checksum", 0, 4096);
        push(&mut b, b"one").unwrap();
        let (_, path, offset, _) = b.spool.as_ref().unwrap();
        let mut f = OpenOptions::new().write(true).open(path).unwrap();
        f.seek(SeekFrom::Start(offset - 1)).unwrap();
        f.write_all(&[0]).unwrap();
        drop(f);
        assert!(matches!(
            b.commit_iter().unwrap().next().unwrap(),
            Err(SpoolError::Checksum)
        ));
        b.finish().unwrap();
    }
    #[test]
    fn startup_removes_owned_dead_spools_and_quarantines_malformed_or_contradictory() {
        let store_dir = dir("startup");
        let store = store_dir.join("state.sqlite");
        let lock = StateLock::acquire(&store, "owner", "nonce").unwrap();
        let dev = fs::metadata(&store_dir).unwrap().dev();
        let mk = |xid: &str| {
            let mut disk = DiskAdmission::default();
            disk.configure(
                dev,
                FilesystemLimit {
                    total_budget: 4096,
                    emergency_reserve: 512,
                },
            )
            .unwrap();
            let mem = MemoryBudget::new(MemoryLimits {
                process_limit: 65_536,
                runtime_fixed: 4_096,
                receive: 1_024,
                decoder: 32_768,
                staging: 16_384,
            })
            .unwrap();
            TxnBuffer::new(
                store_dir.clone(),
                "epoch".into(),
                xid.into(),
                "dead".into(),
                SpoolLimits {
                    max_frame_bytes: 512,
                    max_event_bytes: 512,
                    max_transaction_bytes: 1024,
                    max_transaction_events: 4,
                    memory_prefix_bytes: 0,
                },
                mem,
                FilesystemAdmissionController::new(disk),
                Box::new(FixedSpace(4096)),
            )
            .unwrap()
        };
        let mut good = mk("good");
        push(&mut good, b"x").unwrap();
        drop(good);
        let mut bad = mk("bad");
        push(&mut bad, b"y").unwrap();
        drop(bad);
        let malformed = store_dir.join("malformed.spool");
        fs::write(&malformed, b"bad").unwrap();
        let aliased_store = store_dir.join("state.db");
        assert!(matches!(
            classify_startup_spools(
                &lock,
                &aliased_store,
                &store_dir,
                "epoch",
                "owner",
                512,
                1024,
                4,
                |_, _| ExistingTransaction::Uncommitted
            ),
            Err(SpoolError::Invalid(
                "startup lock does not protect state store"
            ))
        ));
        let blocked = classify_startup_spools(
            &lock,
            &store,
            &store_dir,
            "epoch",
            "owner",
            512,
            1024,
            4,
            |_, xid| {
                if xid == "good" {
                    ExistingTransaction::Uncommitted
                } else {
                    ExistingTransaction::Contradictory
                }
            },
        );
        assert!(matches!(blocked, Err(SpoolError::StartupBlocked)));
        assert_eq!(
            fs::read_dir(store_dir.join("quarantine")).unwrap().count(),
            2
        );
        assert_eq!(
            fs::read_dir(&store_dir)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|e| e.path().extension().and_then(|v| v.to_str()) == Some("spool"))
                .count(),
            0
        );
        drop(lock);
        fs::remove_dir_all(store_dir).ok();
    }
}
