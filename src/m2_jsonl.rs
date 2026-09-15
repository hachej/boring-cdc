//! Deterministic, crash-reconciling JSONL candidate-segment publication.
//! Candidate directories are durability records only; M2 exposes no live selector.
use crate::failure_policy::{
    DestinationOutcome, DomainHookInput, DomainProjection, DomainRecoveryHook,
};
use crate::m2_journal::{CopiedRange, sha256};
use crate::m2_schema::WriterConnection;
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

const FILE_MODE: u32 = 0o600;
const DIR_MODE: u32 = 0o700;
const FORMAT_VERSION: &str = "boring-jsonl-v1";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SegmentIntent {
    pub intent_id: String,
    pub destination_id: String,
    pub generation_id: String,
    pub capture_epoch: String,
    pub generation: u64,
    pub anchor_id: Option<String>,
    pub start_seq: u64,
    pub end_seq: u64,
    pub writer_configuration_hash: String,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArchiveFault {
    None,
    AfterIntent,
    AfterWrite,
    AfterFileSync,
    AfterDirectorySync,
    AfterRename,
    AfterParentSync,
    BeforeMarker,
    AfterMarkerWrite,
    AfterMarkerSync,
    BeforeCheckpoint,
}
#[derive(Debug)]
pub enum ArchiveError {
    Invalid(&'static str),
    Blocked(&'static str),
    Fault(ArchiveFault),
    Io(io::Error),
    Sqlite(rusqlite::Error),
    Json(serde_json::Error),
}
impl std::fmt::Display for ArchiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ArchiveError {}
impl From<io::Error> for ArchiveError {
    fn from(v: io::Error) -> Self {
        Self::Io(v)
    }
}
impl From<rusqlite::Error> for ArchiveError {
    fn from(v: rusqlite::Error) -> Self {
        Self::Sqlite(v)
    }
}
impl From<serde_json::Error> for ArchiveError {
    fn from(v: serde_json::Error) -> Self {
        Self::Json(v)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedSegment {
    pub segment_dir: PathBuf,
    pub manifest_hash: String,
    pub checkpoint: u64,
    pub adopted: bool,
}
#[derive(Serialize)]
struct JsonEvent<'a> {
    journal_seq: u64,
    transaction_ordinal: u64,
    mutation_ordinal: u64,
    connector_event_id: &'a str,
    payload_sha256: &'a str,
    payload_hex: String,
}
#[derive(Serialize)]
struct Part {
    path: &'static str,
    sha256: String,
    size: u64,
}
#[derive(Serialize)]
struct Manifest<'a> {
    format_version: &'static str,
    intent: &'a SegmentIntent,
    enabled_formats: [&'static str; 1],
    event_count: usize,
    schema_fingerprints: Vec<String>,
    source_identifier_mappings: std::collections::BTreeMap<String, String>,
    parts: [Part; 1],
}

fn opaque(prefix: &str, value: &str) -> String {
    format!("{prefix}-{}", sha256(value.as_bytes()))
}
fn names(intent: &SegmentIntent) -> (String, String) {
    let final_name = opaque("segment", &intent.intent_id);
    (format!(".tmp-{final_name}"), final_name)
}
fn check_component(v: &str) -> Result<(), ArchiveError> {
    if v.is_empty() || !v.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
        Err(ArchiveError::Invalid("non-opaque path identity"))
    } else {
        Ok(())
    }
}
struct OpenedDir {
    file: File,
}
impl OpenedDir {
    fn path(&self, name: &str) -> PathBuf {
        PathBuf::from(format!("/proc/self/fd/{}/{}", self.file.as_raw_fd(), name))
    }
    fn sync(&self) -> Result<(), ArchiveError> {
        self.file.sync_all()?;
        Ok(())
    }
}
struct RootDir {
    file: File,
}
impl RootDir {
    fn open(root: &Path) -> Result<Self, ArchiveError> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(root)?;
        let m = file.metadata()?;
        if !m.is_dir()
            || m.nlink() < 2
            || m.uid() != unsafe { libc::geteuid() }
            || m.permissions().mode() & 0o077 != 0
        {
            return Err(ArchiveError::Blocked("unsafe archive root"));
        }
        Ok(Self { file })
    }
    fn child(&self, name: &str) -> Result<Option<OpenedDir>, ArchiveError> {
        let n = std::ffi::CString::new(name).map_err(|_| ArchiveError::Invalid("path NUL"))?;
        let fd = unsafe {
            libc::openat(
                self.file.as_raw_fd(),
                n.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::NotFound {
                return Ok(None);
            }
            return Err(e.into());
        }
        let file = unsafe { File::from_raw_fd(fd) };
        let m = file.metadata()?;
        if !m.is_dir()
            || m.uid() != unsafe { libc::geteuid() }
            || m.permissions().mode() & 0o777 != DIR_MODE
        {
            return Err(ArchiveError::Blocked("unsafe archive directory"));
        }
        Ok(Some(OpenedDir { file }))
    }
    fn sync(&self) -> Result<(), ArchiveError> {
        self.file.sync_all()?;
        Ok(())
    }
    fn mkdir(&self, name: &str) -> Result<(), ArchiveError> {
        let n = std::ffi::CString::new(name).map_err(|_| ArchiveError::Invalid("path NUL"))?;
        if unsafe { libc::mkdirat(self.file.as_raw_fd(), n.as_ptr(), DIR_MODE) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        Ok(())
    }
    fn cleanup_temp(&self, name: &str, expected_inode: u64) -> Result<(), ArchiveError> {
        let n = std::ffi::CString::new(name).unwrap();
        let fd = unsafe {
            libc::openat(
                self.file.as_raw_fd(),
                n.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error().into());
        }
        let dir = unsafe { File::from_raw_fd(fd) };
        let m = dir.metadata()?;
        if !m.is_dir()
            || m.ino() != expected_inode
            || m.uid() != unsafe { libc::geteuid() }
            || m.permissions().mode() & 0o777 != DIR_MODE
        {
            return Err(ArchiveError::Blocked("unsafe temporary directory"));
        }
        for child in ["events.jsonl", "manifest.pending.json", "SEGMENT_READY"] {
            let c = std::ffi::CString::new(child).unwrap();
            let rc = unsafe { libc::unlinkat(dir.as_raw_fd(), c.as_ptr(), 0) };
            if rc != 0 && io::Error::last_os_error().kind() != io::ErrorKind::NotFound {
                return Err(io::Error::last_os_error().into());
            }
        }
        drop(dir);
        if unsafe { libc::unlinkat(self.file.as_raw_fd(), n.as_ptr(), libc::AT_REMOVEDIR) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        Ok(())
    }
    fn rename(&self, from: &str, to: &str) -> Result<(), ArchiveError> {
        let a = std::ffi::CString::new(from).unwrap();
        let b = std::ffi::CString::new(to).unwrap();
        #[cfg(target_os = "linux")]
        let result = unsafe {
            libc::syscall(
                libc::SYS_renameat2,
                self.file.as_raw_fd(),
                a.as_ptr(),
                self.file.as_raw_fd(),
                b.as_ptr(),
                libc::RENAME_NOREPLACE,
            ) as i32
        };
        #[cfg(not(target_os = "linux"))]
        let result = -1;
        if result != 0 {
            return Err(io::Error::last_os_error().into());
        }
        Ok(())
    }
}
fn checked_file(path: &Path) -> Result<File, ArchiveError> {
    let f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(FILE_MODE)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let m = f.metadata()?;
    if !m.is_file()
        || m.nlink() != 1
        || m.uid() != unsafe { libc::geteuid() }
        || m.permissions().mode() & 0o777 != FILE_MODE
    {
        return Err(ArchiveError::Blocked("unsafe archive file"));
    }
    Ok(f)
}
fn checked_read(path: &Path) -> Result<Vec<u8>, ArchiveError> {
    let mut f = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let m = f.metadata()?;
    if !m.is_file()
        || m.nlink() != 1
        || m.uid() != unsafe { libc::geteuid() }
        || m.permissions().mode() & 0o777 != FILE_MODE
    {
        return Err(ArchiveError::Blocked("unsafe archive file"));
    }
    let mut v = Vec::new();
    f.read_to_end(&mut v)?;
    Ok(v)
}
fn write_sync(path: &Path, bytes: &[u8]) -> Result<(), ArchiveError> {
    let mut f = checked_file(path)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    Ok(())
}
fn canonical_line<T: Serialize>(v: &T) -> Result<Vec<u8>, ArchiveError> {
    let mut b = serde_json::to_vec(v)?;
    b.push(b'\n');
    Ok(b)
}
fn staged_bytes(
    intent: &SegmentIntent,
    range: &CopiedRange,
) -> Result<(Vec<u8>, Vec<u8>, String), ArchiveError> {
    let mut rows = range.events.clone();
    rows.sort_by_key(|e| e.journal_seq);
    if range.first_seq != intent.start_seq
        || range.last_seq != intent.end_seq
        || rows.first().map(|e| e.journal_seq) != Some(intent.start_seq)
        || rows.last().map(|e| e.journal_seq) != Some(intent.end_seq)
    {
        return Err(ArchiveError::Invalid("range differs from immutable intent"));
    }
    if rows
        .windows(2)
        .any(|w| w[1].journal_seq != w[0].journal_seq + 1)
        || rows.len() as u64 != intent.end_seq - intent.start_seq + 1
    {
        return Err(ArchiveError::Blocked("non-contiguous intent range"));
    }
    let mut part = Vec::new();
    let mut schema_fingerprints = std::collections::BTreeSet::new();
    let mut source_identifier_mappings = std::collections::BTreeMap::new();
    for e in &rows {
        if sha256(&e.payload) != e.payload_hash {
            return Err(ArchiveError::Blocked("journal payload hash mismatch"));
        }
        if let Some(fingerprint) = &e.relation_schema_fingerprint {
            schema_fingerprints.insert(fingerprint.clone());
        }
        if let Some(relation) = &e.source_relation_id {
            source_identifier_mappings.insert(opaque("source", relation), relation.clone());
        }
        part.extend(canonical_line(&JsonEvent {
            journal_seq: e.journal_seq,
            transaction_ordinal: e.transaction_ordinal,
            mutation_ordinal: u64::from(e.mutation_ordinal),
            connector_event_id: &e.event_id,
            payload_sha256: &e.payload_hash,
            payload_hex: e.payload.iter().map(|b| format!("{b:02x}")).collect(),
        })?);
    }
    let p = Part {
        path: "events.jsonl",
        sha256: sha256(&part),
        size: part.len() as u64,
    };
    let manifest = canonical_line(&Manifest {
        format_version: FORMAT_VERSION,
        intent,
        enabled_formats: ["jsonl"],
        event_count: rows.len(),
        schema_fingerprints: schema_fingerprints.into_iter().collect(),
        source_identifier_mappings,
        parts: [p],
    })?;
    let hash = sha256(&manifest);
    Ok((part, manifest, hash))
}
fn fail(point: ArchiveFault, wanted: ArchiveFault) -> Result<(), ArchiveError> {
    if point == wanted {
        Err(ArchiveError::Fault(point))
    } else {
        Ok(())
    }
}

pub fn commit_jsonl_segment(
    writer: &mut WriterConnection,
    root: &Path,
    intent: &SegmentIntent,
    range: &CopiedRange,
    fault: ArchiveFault,
) -> Result<PublishedSegment, ArchiveError> {
    check_component(&intent.intent_id)?;
    if intent.start_seq == 0
        || intent.end_seq < intent.start_seq
        || intent.generation == 0
        || intent.writer_configuration_hash.len() != 64
    {
        return Err(ArchiveError::Invalid("invalid intent"));
    }
    let root_dir = RootDir::open(root)?;
    let (part, manifest, manifest_hash) = staged_bytes(intent, range)?;
    let selection = sha256(&serde_json::to_vec(intent)?);
    {
        let tx = writer
            .connection_mut()
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let generation_row:Option<(String,String,i64,String,Option<String>)>=tx.query_row("SELECT destination_id,capture_epoch,generation,state,anchor_id FROM archive_generations WHERE generation_id=?1",[&intent.generation_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
        if generation_row
            != Some((
                intent.destination_id.clone(),
                intent.capture_epoch.clone(),
                intent.generation as i64,
                "candidate".into(),
                intent.anchor_id.clone(),
            ))
        {
            return Err(ArchiveError::Blocked("candidate generation mismatch"));
        }
        tx.execute("INSERT INTO archive_segment_intents(intent_id,generation_id,first_seq,last_seq,selection_digest,state) VALUES(?1,?2,?3,?4,?5,'selected') ON CONFLICT(intent_id) DO NOTHING",params![intent.intent_id,intent.generation_id,intent.start_seq,intent.end_seq,selection])?;
        let got:(String,i64,i64,String)=tx.query_row("SELECT generation_id,first_seq,last_seq,selection_digest FROM archive_segment_intents WHERE intent_id=?1",[&intent.intent_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?;
        if got
            != (
                intent.generation_id.clone(),
                intent.start_seq as i64,
                intent.end_seq as i64,
                selection,
            )
        {
            return Err(ArchiveError::Blocked("immutable intent mismatch"));
        }
        tx.commit()?;
    }
    fail(ArchiveFault::AfterIntent, fault)?;
    let (temp_name, final_name) = names(intent);
    let mut adopted = false;
    if let Some(existing_temp) = root_dir.child(&temp_name)? {
        let inode = existing_temp.file.metadata()?.ino();
        root_dir.cleanup_temp(&temp_name, inode)?;
    }
    let final_handle = if let Some(dir) = root_dir.child(&final_name)? {
        validate_final(&dir, intent, &manifest_hash, &part, &manifest)?;
        adopted = true;
        dir
    } else {
        root_dir.mkdir(&temp_name)?;
        let temp_handle = root_dir
            .child(&temp_name)?
            .ok_or(ArchiveError::Blocked("temporary directory disappeared"))?;
        let mut part_file = checked_file(&temp_handle.path("events.jsonl"))?;
        part_file.write_all(&part)?;
        fail(ArchiveFault::AfterWrite, fault)?;
        part_file.sync_all()?;
        fail(ArchiveFault::AfterFileSync, fault)?;
        write_sync(&temp_handle.path("manifest.pending.json"), &manifest)?;
        temp_handle.sync()?;
        fail(ArchiveFault::AfterDirectorySync, fault)?;
        root_dir.rename(&temp_name, &final_name)?;
        fail(ArchiveFault::AfterRename, fault)?;
        root_dir.sync()?;
        fail(ArchiveFault::AfterParentSync, fault)?;
        let final_handle = root_dir
            .child(&final_name)?
            .ok_or(ArchiveError::Blocked("renamed directory disappeared"))?;
        if final_handle.file.metadata()?.ino() != temp_handle.file.metadata()?.ino() {
            return Err(ArchiveError::Blocked("renamed directory identity changed"));
        }
        final_handle
    };
    fail(ArchiveFault::BeforeMarker, fault)?;
    let marker = canonical_line(
        &serde_json::json!({"format_version":FORMAT_VERSION,"intent_id":intent.intent_id,"manifest_sha256":manifest_hash}),
    )?;
    let marker_path = final_handle.path("SEGMENT_READY");
    if marker_path.exists() {
        if checked_read(&marker_path)? != marker {
            return Err(ArchiveError::Blocked("ready marker mismatch"));
        }
    } else {
        write_sync(&marker_path, &marker)?;
    }
    fail(ArchiveFault::AfterMarkerWrite, fault)?;
    final_handle.sync()?;
    fail(ArchiveFault::AfterMarkerSync, fault)?;
    root_dir.sync()?;
    fail(ArchiveFault::BeforeCheckpoint, fault)?;
    let marker_hash = sha256(&marker);
    let tx = writer
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let intent_state: String = tx.query_row(
        "SELECT state FROM archive_segment_intents WHERE intent_id=?1",
        [&intent.intent_id],
        |r| r.get(0),
    )?;
    if !matches!(intent_state.as_str(), "selected" | "writing" | "published") {
        return Err(ArchiveError::Blocked("intent is not publishable"));
    }
    if intent_state == "selected" {
        let changed=tx.execute("UPDATE archive_segment_intents SET state='writing' WHERE intent_id=?1 AND state='selected'",[&intent.intent_id])?;
        if changed != 1 {
            return Err(ArchiveError::Blocked("intent state CAS rejected"));
        }
    }
    tx.execute("INSERT INTO archive_segments(segment_id,intent_id,manifest_digest,ready_marker_digest,first_seq,last_seq) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(intent_id) DO NOTHING",params![opaque("record",&intent.intent_id),intent.intent_id,manifest_hash,marker_hash,intent.start_seq,intent.end_seq])?;
    let existing: (String, String) = tx.query_row(
        "SELECT manifest_digest,ready_marker_digest FROM archive_segments WHERE intent_id=?1",
        [&intent.intent_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if existing != (manifest_hash.clone(), marker_hash) {
        return Err(ArchiveError::Blocked("durable segment hash mismatch"));
    }
    if intent_state != "published" {
        let changed=tx.execute("UPDATE archive_segment_intents SET state='published' WHERE intent_id=?1 AND state='writing'",[&intent.intent_id])?;
        if changed != 1 {
            return Err(ArchiveError::Blocked("intent publish CAS rejected"));
        }
    }
    let complete:String=tx.query_row("SELECT transaction_id FROM source_transactions WHERE capture_epoch=?1 AND last_seq=?2 AND state='committed'",params![intent.capture_epoch,intent.end_seq],|r|r.get(0)).map_err(|_|ArchiveError::Blocked("checkpoint is not complete transaction boundary"))?;
    let changed=tx.execute("INSERT INTO destination_checkpoints(destination_id,capture_epoch,anchor_id,configuration_fingerprint,generation,complete_transaction_id,journal_seq,current_failure_id,revision) VALUES(?1,?2,?3,?4,?5,?6,?7,NULL,0) ON CONFLICT(destination_id) DO UPDATE SET complete_transaction_id=excluded.complete_transaction_id,journal_seq=excluded.journal_seq,revision=destination_checkpoints.revision+1 WHERE destination_checkpoints.capture_epoch=excluded.capture_epoch AND destination_checkpoints.generation=excluded.generation AND destination_checkpoints.configuration_fingerprint=excluded.configuration_fingerprint AND destination_checkpoints.anchor_id IS excluded.anchor_id AND destination_checkpoints.journal_seq<=excluded.journal_seq",params![intent.destination_id,intent.capture_epoch,intent.anchor_id,intent.writer_configuration_hash,intent.generation,complete,intent.end_seq])?;
    if changed != 1 {
        return Err(ArchiveError::Blocked("checkpoint CAS rejected"));
    }
    let durable:(String,i64,i64,String,Option<String>)=tx.query_row("SELECT capture_epoch,generation,journal_seq,configuration_fingerprint,anchor_id FROM destination_checkpoints WHERE destination_id=?1",[&intent.destination_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?;
    if durable
        != (
            intent.capture_epoch.clone(),
            intent.generation as i64,
            intent.end_seq as i64,
            intent.writer_configuration_hash.clone(),
            intent.anchor_id.clone(),
        )
    {
        return Err(ArchiveError::Blocked("checkpoint readback mismatch"));
    }
    tx.commit()?;
    Ok(PublishedSegment {
        segment_dir: PathBuf::from(final_name),
        manifest_hash,
        checkpoint: intent.end_seq,
        adopted,
    })
}
fn validate_final(
    dir: &OpenedDir,
    _intent: &SegmentIntent,
    manifest_hash: &str,
    part: &[u8],
    manifest: &[u8],
) -> Result<(), ArchiveError> {
    let m = dir.file.metadata()?;
    if !m.is_dir()
        || m.file_type().is_symlink()
        || m.uid() != unsafe { libc::geteuid() }
        || m.permissions().mode() & 0o077 != 0
    {
        return Err(ArchiveError::Blocked("unsafe final directory"));
    }
    for (name, expected) in [("events.jsonl", part), ("manifest.pending.json", manifest)] {
        let p = dir.path(name);
        let md = fs::symlink_metadata(&p)?;
        if !md.is_file()
            || md.file_type().is_symlink()
            || md.nlink() != 1
            || md.uid() != unsafe { libc::geteuid() }
            || checked_read(&p)? != expected
        {
            return Err(ArchiveError::Blocked("published artifact mismatch"));
        }
    }
    if sha256(manifest) != manifest_hash {
        return Err(ArchiveError::Blocked("manifest mismatch"));
    }
    dir.sync()?;
    Ok(())
}

/// Production-facing commit wrapper: every archive error is projected through the sole shared
/// FailurePolicy and persisted by the supplied capture-priority writer before return.
pub fn commit_jsonl_segment_with_policy(
    writer: &mut WriterConnection,
    root: &Path,
    intent: &SegmentIntent,
    range: &CopiedRange,
    fault: ArchiveFault,
    context: &mut crate::m1_transition_kernel::TransitionContext<'_>,
) -> Result<PublishedSegment, ArchiveError> {
    match commit_jsonl_segment(writer, root, intent, range, fault) {
        Ok(v) => Ok(v),
        Err(error) => {
            use crate::failure_policy::{FailedBoundary, PreparedFailureOperation, load_failure};
            let current_id: Option<String> = writer.connection().query_row(
                "SELECT current_failure_id FROM destinations WHERE destination_id=?1",
                [&intent.destination_id],
                |r| r.get(0),
            )?;
            let boundary = FailedBoundary::Destination {
                capture_epoch: intent.capture_epoch.clone(),
                generation: intent.generation,
                first_seq: intent.start_seq,
                last_seq: intent.end_seq,
            };
            let current = match &current_id {
                Some(id) => load_failure(writer.connection(), id, boundary)?,
                None => None,
            };
            let outcome = match error {
                ArchiveError::Io(_) | ArchiveError::Sqlite(_) | ArchiveError::Fault(_) => {
                    DestinationOutcome::RetryEligible
                }
                _ => DestinationOutcome::IntegrityRecoveryRequired,
            };
            let action = ArchiveFailureAdapter::prepare_failure(
                current.as_ref(),
                outcome,
                &intent.destination_id,
                &intent.capture_epoch,
                intent.generation,
                intent.start_seq,
                intent.end_seq,
                &intent.writer_configuration_hash,
                context,
            );
            if let Some(op) = PreparedFailureOperation::from_policy_action(action, current_id) {
                let tx = writer.connection_mut().transaction()?;
                op.execute(&tx)?;
                tx.commit()?;
            }
            Err(error)
        }
    }
}

/// Reconstructs the immutable logical range pin after restart. Physical journal readers are
/// deliberately absent here; the caller re-copies this exact retained range with the shared
/// bounded reader before any file/hash await.
pub fn load_segment_intent(
    writer: &WriterConnection,
    intent_id: &str,
) -> Result<Option<SegmentIntent>, ArchiveError> {
    let row=writer.connection().query_row("SELECT i.intent_id,g.destination_id,i.generation_id,g.capture_epoch,g.generation,g.anchor_id,i.first_seq,i.last_seq,d.configuration_fingerprint,i.selection_digest FROM archive_segment_intents i JOIN archive_generations g ON g.generation_id=i.generation_id JOIN destinations d ON d.destination_id=g.destination_id WHERE i.intent_id=?1 AND i.state IN ('selected','writing')",[intent_id],|r|Ok(SegmentIntent{intent_id:r.get(0)?,destination_id:r.get(1)?,generation_id:r.get(2)?,capture_epoch:r.get(3)?,generation:r.get::<_,i64>(4)? as u64,anchor_id:r.get(5)?,start_seq:r.get::<_,i64>(6)? as u64,end_seq:r.get::<_,i64>(7)? as u64,writer_configuration_hash:r.get(8)?})).optional()?;
    if let Some(value) = &row {
        let stored: String = writer.connection().query_row(
            "SELECT selection_digest FROM archive_segment_intents WHERE intent_id=?1",
            [intent_id],
            |r| r.get(0),
        )?;
        if stored != sha256(&serde_json::to_vec(value)?) {
            return Err(ArchiveError::Blocked("persisted intent digest mismatch"));
        }
    }
    Ok(row)
}

/// The real archive adapter consumes the shared typed hook; it does not define another policy.
pub struct ArchiveFailureAdapter;
impl ArchiveFailureAdapter {
    pub fn prepare_failure(
        current: Option<&crate::failure_policy::FailureRecord>,
        outcome: DestinationOutcome,
        destination_id: &str,
        capture_epoch: &str,
        generation: u64,
        start_seq: u64,
        end_seq: u64,
        configuration_fingerprint: &str,
        context: &mut crate::m1_transition_kernel::TransitionContext<'_>,
    ) -> crate::failure_policy::PolicyAction {
        use crate::failure_policy::{
            Component, FailedBoundary, FailureClass, FailureObservation, FingerprintInput,
            PolicyEvent, StableErrorCode,
        };
        let (class, code) = match outcome {
            DestinationOutcome::RetryEligible => (
                FailureClass::TransientIo,
                StableErrorCode::TransportUnavailable,
            ),
            DestinationOutcome::Blocked | DestinationOutcome::IntegrityRecoveryRequired => {
                (FailureClass::Integrity, StableErrorCode::ChecksumMismatch)
            }
            DestinationOutcome::ContinuityRecoveryRequired => (
                FailureClass::OwnershipLost,
                StableErrorCode::HistoryUnavailable,
            ),
        };
        crate::failure_policy::transition(
            current,
            PolicyEvent::Observe(FailureObservation {
                destination_id: Some(destination_id.into()),
                fingerprint: FingerprintInput {
                    component: Component::Archive,
                    class,
                    code,
                    boundary: FailedBoundary::Destination {
                        capture_epoch: capture_epoch.into(),
                        generation,
                        first_seq: start_seq,
                        last_seq: end_seq,
                    },
                    relevant_configuration_fingerprint: configuration_fingerprint.into(),
                    context: Default::default(),
                },
            }),
            context,
        )
    }
}
impl DomainRecoveryHook for ArchiveFailureAdapter {
    fn project(&self, input: &DomainHookInput) -> DomainProjection {
        match input {
            DomainHookInput::Archive {
                outcome: DestinationOutcome::Blocked,
                ..
            } => DomainProjection::Blocked,
            DomainHookInput::Archive {
                outcome: DestinationOutcome::RetryEligible,
                ..
            } => DomainProjection::RetryEligible,
            DomainHookInput::Archive {
                outcome:
                    DestinationOutcome::IntegrityRecoveryRequired
                    | DestinationOutcome::ContinuityRecoveryRequired,
                ..
            } => DomainProjection::RecoveryRequired,
            _ => DomainProjection::Blocked,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::failure_policy::{PolicyAction, PreparedFailureOperation};
    use crate::m1_transition_kernel::{SplitMix64, TransitionContext, VirtualClock};
    use crate::m2_journal::{
        CommitLimits, JournalEvent, JournalStore, SourceCommit, SourceIdentity,
        read_complete_range, transaction_checksum,
    };
    use crate::m2_schema::open_writer;
    use std::time::Duration;
    fn setup(
        tag: &str,
    ) -> (
        PathBuf,
        PathBuf,
        WriterConnection,
        SegmentIntent,
        CopiedRange,
    ) {
        let base = std::env::temp_dir().join(format!("m2-jsonl-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&base);
        fs::create_dir(&base).unwrap();
        fs::set_permissions(&base, fs::Permissions::from_mode(0o700)).unwrap();
        let root = base.join("archive");
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let db = base.join("state.sqlite");
        let mut w = open_writer(&db, "run", 1, 1).unwrap();
        let events = vec![
            JournalEvent {
                event_id: "event-z".into(),
                transaction_ordinal: 0,
                relation_schema_fingerprint: None,
                control_kind: None,
                payload: b"{\"z\":1}".to_vec(),
                payload_hash: sha256(b"{\"z\":1}"),
            },
            JournalEvent {
                event_id: "event-a".into(),
                transaction_ordinal: 1,
                relation_schema_fingerprint: None,
                control_kind: None,
                payload: b"beta".to_vec(),
                payload_hash: sha256(b"beta"),
            },
        ];
        let c = SourceCommit {
            transaction_id: "tx-1".into(),
            xid: "1".into(),
            end_lsn: "0000000000000001".into(),
            payload_checksum: transaction_checksum(&events),
            schemas: vec![],
            events,
        };
        let mut js = JournalStore::new(
            w,
            SourceIdentity {
                capture_epoch: "epoch".into(),
                source_system_id: "sys".into(),
                timeline_id: "timeline".into(),
                database_id: "db".into(),
                slot_name: "slot".into(),
                publication_fingerprint: "pub".into(),
                protocol_fingerprint: "proto".into(),
            },
            CommitLimits {
                max_events: 10,
                max_copied_bytes: 4096,
                max_writer_hold: Duration::from_secs(2),
            },
        )
        .unwrap();
        js.commit_atomic(&c, crate::m2_journal::CommitFault::None)
            .unwrap();
        drop(js);
        w = open_writer(&db, "run", 2, 2).unwrap();
        w.connection().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('archive','archive',?1,'epoch',1)",["a".repeat(64)]).unwrap();
        w.connection().execute("INSERT INTO archive_generations VALUES('gen','archive','epoch',1,NULL,'candidate')",[]).unwrap();
        let range = read_complete_range(&db, 0, 10, 4096, Duration::from_secs(2))
            .unwrap()
            .unwrap();
        let i = SegmentIntent {
            intent_id: "intent-a".into(),
            destination_id: "archive".into(),
            generation_id: "gen".into(),
            capture_epoch: "epoch".into(),
            generation: 1,
            anchor_id: None,
            start_seq: 1,
            end_seq: 2,
            writer_configuration_hash: "a".repeat(64),
        };
        (base, root, w, i, range)
    }
    #[test]
    fn deterministic_commit_and_exact_range_retry_are_byte_identical() {
        let (b, r, mut w, i, x) = setup("retry");
        let a = commit_jsonl_segment(&mut w, &r, &i, &x, ArchiveFault::None).unwrap();
        let before = fs::read(r.join(&a.segment_dir).join("events.jsonl")).unwrap();
        let b2 = commit_jsonl_segment(&mut w, &r, &i, &x, ArchiveFault::None).unwrap();
        assert!(b2.adopted);
        assert_eq!(
            before,
            fs::read(r.join(b2.segment_dir).join("events.jsonl")).unwrap()
        );
        assert_eq!(
            w.connection()
                .query_row("select count(*) from archive_segments", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        fs::remove_dir_all(b).unwrap()
    }
    #[test]
    fn every_crash_boundary_is_absent_adopted_or_blocks_without_checkpoint_skip() {
        for (n, f) in [
            ArchiveFault::AfterIntent,
            ArchiveFault::AfterWrite,
            ArchiveFault::AfterFileSync,
            ArchiveFault::AfterDirectorySync,
            ArchiveFault::AfterRename,
            ArchiveFault::AfterParentSync,
            ArchiveFault::BeforeMarker,
            ArchiveFault::AfterMarkerWrite,
            ArchiveFault::AfterMarkerSync,
            ArchiveFault::BeforeCheckpoint,
        ]
        .into_iter()
        .enumerate()
        {
            let (b, r, mut w, i, x) = setup(&format!("fault-{n}"));
            assert!(commit_jsonl_segment(&mut w, &r, &i, &x, f).is_err());
            let resumed = load_segment_intent(&w, &i.intent_id).unwrap().unwrap();
            assert_eq!(resumed, i);
            let exact = crate::m2_journal::read_exact_complete_range(
                &b.join("state.sqlite"),
                resumed.start_seq,
                resumed.end_seq,
                10,
                4096,
                Duration::from_secs(2),
            )
            .unwrap();
            let _ = fs::remove_dir_all(r.join(names(&i).0));
            let out =
                commit_jsonl_segment(&mut w, &r, &resumed, &exact, ArchiveFault::None).unwrap();
            assert_eq!(out.checkpoint, 2);
            fs::remove_dir_all(b).unwrap()
        }
    }
    #[test]
    fn incomplete_caller_range_never_publishes_or_checkpoints() {
        let (b, r, mut w, i, mut x) = setup("gap");
        x.events.remove(0);
        x.first_seq = 1;
        assert!(commit_jsonl_segment(&mut w, &r, &i, &x, ArchiveFault::None).is_err());
        assert_eq!(
            w.connection()
                .query_row("SELECT count(*) FROM destination_checkpoints", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        fs::remove_dir_all(b).unwrap()
    }
    #[test]
    fn candidate_is_not_live_and_mismatch_blocks() {
        let (b, r, mut w, i, x) = setup("candidate");
        let out = commit_jsonl_segment(&mut w, &r, &i, &x, ArchiveFault::BeforeMarker);
        assert!(out.is_err());
        let final_dir = r.join(names(&i).1);
        if final_dir.exists() {
            fs::write(final_dir.join("events.jsonl"), b"poison").unwrap();
            assert!(matches!(
                commit_jsonl_segment(&mut w, &r, &i, &x, ArchiveFault::None),
                Err(ArchiveError::Blocked(_))
            ))
        }
        assert_eq!(
            w.connection()
                .query_row("select state from archive_generations", [], |r| r
                    .get::<_, String>(0))
                .unwrap(),
            "candidate"
        );
        fs::remove_dir_all(b).unwrap()
    }
    #[test]
    fn hostile_paths_symlinks_hardlinks_modes_and_type_swaps_fail_closed() {
        let (b, r, mut w, mut i, x) = setup("paths");
        for bad in ["../x", "a/b", ".", "é", "a_b"] {
            i.intent_id = bad.into();
            assert!(commit_jsonl_segment(&mut w, &r, &i, &x, ArchiveFault::None).is_err())
        }
        i.intent_id = "intent-safe".into();
        fs::set_permissions(&r, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(matches!(
            commit_jsonl_segment(&mut w, &r, &i, &x, ArchiveFault::None),
            Err(ArchiveError::Blocked(_))
        ));
        fs::set_permissions(&r, fs::Permissions::from_mode(0o700)).unwrap();
        let link = b.join("root-link");
        std::os::unix::fs::symlink(&r, &link).unwrap();
        assert!(commit_jsonl_segment(&mut w, &link, &i, &x, ArchiveFault::None).is_err());
        commit_jsonl_segment(&mut w, &r, &i, &x, ArchiveFault::None).unwrap();
        let part = r.join(names(&i).1).join("events.jsonl");
        let extra = b.join("hardlink");
        fs::hard_link(&part, &extra).unwrap();
        assert!(matches!(
            commit_jsonl_segment(&mut w, &r, &i, &x, ArchiveFault::None),
            Err(ArchiveError::Blocked(_))
        ));
        fs::remove_dir_all(b).unwrap()
    }
    #[test]
    fn checkpoint_cas_mismatch_never_reports_success() {
        let (b, r, mut w, i, x) = setup("checkpoint-cas");
        w.connection().execute("INSERT INTO destination_checkpoints(destination_id,capture_epoch,anchor_id,configuration_fingerprint,generation,complete_transaction_id,journal_seq,current_failure_id,revision) VALUES('archive','epoch',NULL,?1,1,'tx-1',2,NULL,0)",["b".repeat(64)]).unwrap();
        assert!(matches!(
            commit_jsonl_segment(&mut w, &r, &i, &x, ArchiveFault::None),
            Err(ArchiveError::Blocked("checkpoint CAS rejected"))
        ));
        fs::remove_dir_all(b).unwrap();
    }
    #[test]
    fn shared_policy_real_archive_adapter_matches_golden_outcomes() {
        let a = ArchiveFailureAdapter;
        for (out, expected) in [
            (DestinationOutcome::Blocked, DomainProjection::Blocked),
            (
                DestinationOutcome::RetryEligible,
                DomainProjection::RetryEligible,
            ),
            (
                DestinationOutcome::IntegrityRecoveryRequired,
                DomainProjection::RecoveryRequired,
            ),
            (
                DestinationOutcome::ContinuityRecoveryRequired,
                DomainProjection::RecoveryRequired,
            ),
        ] {
            assert_eq!(
                a.project(&DomainHookInput::Archive {
                    outcome: out,
                    capture_epoch: "epoch".into(),
                    generation: 1
                }),
                expected
            )
        }
        let corpus: serde_json::Value =
            serde_json::from_str(include_str!("../contracts/m2/failure-policy-cases.json"))
                .unwrap();
        assert_eq!(corpus["owner_bead"], "boring-cdc-m2.1");
        assert!(corpus["cases"].as_array().unwrap().len() >= 14);
        let (b, r, mut w, i, x) = setup("policy-persist");
        let clock = VirtualClock::new(1000);
        let mut rng = SplitMix64::new(7);
        let mut cx = TransitionContext {
            clock: &clock,
            randomness: &mut rng,
        };
        assert!(
            commit_jsonl_segment_with_policy(
                &mut w,
                &r,
                &i,
                &x,
                ArchiveFault::BeforeCheckpoint,
                &mut cx
            )
            .is_err()
        );
        assert_eq!(w.connection().query_row("SELECT journal_seq FROM destination_checkpoints WHERE destination_id='archive'",[],|r|r.get::<_,i64>(0)).optional().unwrap(),None);
        let persisted_id: String = w
            .connection()
            .query_row("SELECT current_failure_id FROM destinations", [], |r| {
                r.get(0)
            })
            .unwrap();
        let persisted_attempt: i64 = w
            .connection()
            .query_row(
                "SELECT attempt FROM processing_failures WHERE failure_id=?1",
                [persisted_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(persisted_attempt, 1);
        // Separate unaffected destination state remains independent.
        w.connection().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('other','archive',?1,'epoch',1)",["a".repeat(64)]).unwrap();
        assert!(
            w.connection()
                .query_row(
                    "SELECT current_failure_id FROM destinations WHERE destination_id='other'",
                    [],
                    |r| r.get::<_, Option<String>>(0)
                )
                .unwrap()
                .is_none()
        );
        fs::remove_dir_all(&r).unwrap();
        fs::create_dir(&r).unwrap();
        fs::set_permissions(&r, fs::Permissions::from_mode(0o700)).unwrap();
        let (b2, _r, mut w, _i, _x) = setup("policy-core");
        let clock = VirtualClock::new(1000);
        let mut rng = SplitMix64::new(7);
        let mut cx = TransitionContext {
            clock: &clock,
            randomness: &mut rng,
        };
        let action = ArchiveFailureAdapter::prepare_failure(
            None,
            DestinationOutcome::IntegrityRecoveryRequired,
            "archive",
            "epoch",
            1,
            1,
            2,
            &"a".repeat(64),
            &mut cx,
        );
        let record = match &action {
            PolicyAction::Persist(r) => r.clone(),
            _ => panic!("shared policy did not persist"),
        };
        let op = PreparedFailureOperation::from_policy_action(action, None).unwrap();
        let tx = w.connection_mut().transaction().unwrap();
        op.execute(&tx).unwrap();
        tx.commit().unwrap();
        assert_eq!(
            w.connection()
                .query_row(
                    "SELECT attempt FROM processing_failures WHERE failure_id=?1",
                    [&record.failure_id],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        let suppressed = ArchiveFailureAdapter::prepare_failure(
            Some(&record),
            DestinationOutcome::IntegrityRecoveryRequired,
            "archive",
            "epoch",
            1,
            1,
            2,
            &"a".repeat(64),
            &mut cx,
        );
        assert_eq!(suppressed, PolicyAction::Suppressed);
        assert!(w.connection().query_row("SELECT journal_seq FROM destination_checkpoints WHERE destination_id='archive'",[],|r|r.get::<_,i64>(0)).optional().unwrap().is_none());
        drop(w);
        let reopened = open_writer(&b2.join("state.sqlite"), "run-reopen", 3, 3).unwrap();
        assert_eq!(
            reopened
                .connection()
                .query_row("SELECT attempt FROM processing_failures", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        fs::remove_dir_all(b).unwrap();
        fs::remove_dir_all(b2).unwrap();
    }
}
