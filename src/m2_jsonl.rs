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
use std::os::fd::AsRawFd;
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
    schema_fingerprints: [String; 0],
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
    fn path(&self, name: &str) -> PathBuf {
        PathBuf::from(format!("/proc/self/fd/{}/{}", self.file.as_raw_fd(), name))
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
    fn rename(&self, from: &str, to: &str) -> Result<(), ArchiveError> {
        let a = std::ffi::CString::new(from).unwrap();
        let b = std::ffi::CString::new(to).unwrap();
        if unsafe {
            libc::renameat(
                self.file.as_raw_fd(),
                a.as_ptr(),
                self.file.as_raw_fd(),
                b.as_ptr(),
            )
        } != 0
        {
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
fn sync_dir(path: &Path) -> Result<(), ArchiveError> {
    let f = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    f.sync_all()?;
    Ok(())
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
    let mut part = Vec::new();
    let mut ords = std::collections::BTreeMap::<String, u64>::new();
    for e in &rows {
        if sha256(&e.payload) != e.payload_hash {
            return Err(ArchiveError::Blocked("journal payload hash mismatch"));
        }
        let o = ords.entry(e.transaction_id.clone()).or_default();
        part.extend(canonical_line(&JsonEvent {
            journal_seq: e.journal_seq,
            transaction_ordinal: *o,
            mutation_ordinal: *o,
            connector_event_id: &e.event_id,
            payload_sha256: &e.payload_hash,
            payload_hex: e.payload.iter().map(|b| format!("{b:02x}")).collect(),
        })?);
        *o += 1;
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
        schema_fingerprints: [],
        source_identifier_mappings: Default::default(),
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
        let generation_row:Option<(String,String,i64,String)>=tx.query_row("SELECT destination_id,capture_epoch,generation,state FROM archive_generations WHERE generation_id=?1",[&intent.generation_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
        if generation_row
            != Some((
                intent.destination_id.clone(),
                intent.capture_epoch.clone(),
                intent.generation as i64,
                "candidate".into(),
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
    let temp = root_dir.path(&temp_name);
    let final_dir = root_dir.path(&final_name);
    let mut adopted = false;
    if temp.exists() {
        fs::remove_dir_all(&temp)?
    }
    if final_dir.exists() {
        validate_final(&final_dir, intent, &manifest_hash, &part, &manifest)?;
        adopted = true;
    } else {
        root_dir.mkdir(&temp_name)?;
        write_sync(&temp.join("events.jsonl"), &part)?;
        fail(ArchiveFault::AfterWrite, fault)?;
        fail(ArchiveFault::AfterFileSync, fault)?;
        write_sync(&temp.join("manifest.pending.json"), &manifest)?;
        sync_dir(&temp)?;
        fail(ArchiveFault::AfterDirectorySync, fault)?;
        root_dir.rename(&temp_name, &final_name)?;
        fail(ArchiveFault::AfterRename, fault)?;
        root_dir.sync()?;
        fail(ArchiveFault::AfterParentSync, fault)?;
    }
    fail(ArchiveFault::BeforeMarker, fault)?;
    let marker = canonical_line(
        &serde_json::json!({"format_version":FORMAT_VERSION,"intent_id":intent.intent_id,"manifest_sha256":manifest_hash}),
    )?;
    let marker_path = final_dir.join("SEGMENT_READY");
    if marker_path.exists() {
        if checked_read(&marker_path)? != marker {
            return Err(ArchiveError::Blocked("ready marker mismatch"));
        }
    } else {
        write_sync(&marker_path, &marker)?;
    }
    fail(ArchiveFault::AfterMarkerWrite, fault)?;
    sync_dir(&final_dir)?;
    fail(ArchiveFault::AfterMarkerSync, fault)?;
    root_dir.sync()?;
    fail(ArchiveFault::BeforeCheckpoint, fault)?;
    let marker_hash = sha256(&marker);
    let tx = writer
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute("UPDATE archive_segment_intents SET state='writing' WHERE intent_id=?1 AND state='selected'",[&intent.intent_id])?;
    tx.execute("INSERT INTO archive_segments(segment_id,intent_id,manifest_digest,ready_marker_digest,first_seq,last_seq) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(intent_id) DO NOTHING",params![opaque("record",&intent.intent_id),intent.intent_id,manifest_hash,marker_hash,intent.start_seq,intent.end_seq])?;
    let existing: (String, String) = tx.query_row(
        "SELECT manifest_digest,ready_marker_digest FROM archive_segments WHERE intent_id=?1",
        [&intent.intent_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if existing != (manifest_hash.clone(), marker_hash) {
        return Err(ArchiveError::Blocked("durable segment hash mismatch"));
    }
    tx.execute("UPDATE archive_segment_intents SET state='published' WHERE intent_id=?1 AND state='writing'",[&intent.intent_id])?;
    let complete:String=tx.query_row("SELECT transaction_id FROM source_transactions WHERE capture_epoch=?1 AND last_seq=?2 AND state='committed'",params![intent.capture_epoch,intent.end_seq],|r|r.get(0)).map_err(|_|ArchiveError::Blocked("checkpoint is not complete transaction boundary"))?;
    tx.execute("INSERT INTO destination_checkpoints(destination_id,capture_epoch,anchor_id,configuration_fingerprint,generation,complete_transaction_id,journal_seq,current_failure_id,revision) VALUES(?1,?2,?3,?4,?5,?6,?7,NULL,0) ON CONFLICT(destination_id) DO UPDATE SET complete_transaction_id=excluded.complete_transaction_id,journal_seq=excluded.journal_seq,revision=destination_checkpoints.revision+1 WHERE destination_checkpoints.capture_epoch=excluded.capture_epoch AND destination_checkpoints.generation=excluded.generation AND destination_checkpoints.configuration_fingerprint=excluded.configuration_fingerprint AND destination_checkpoints.journal_seq<=excluded.journal_seq",params![intent.destination_id,intent.capture_epoch,intent.anchor_id,intent.writer_configuration_hash,intent.generation,complete,intent.end_seq])?;
    tx.commit()?;
    Ok(PublishedSegment {
        segment_dir: PathBuf::from(final_name),
        manifest_hash,
        checkpoint: intent.end_seq,
        adopted,
    })
}
fn validate_final(
    dir: &Path,
    _intent: &SegmentIntent,
    manifest_hash: &str,
    part: &[u8],
    manifest: &[u8],
) -> Result<(), ArchiveError> {
    let m = fs::symlink_metadata(dir)?;
    if !m.is_dir()
        || m.file_type().is_symlink()
        || m.uid() != unsafe { libc::geteuid() }
        || m.permissions().mode() & 0o077 != 0
    {
        return Err(ArchiveError::Blocked("unsafe final directory"));
    }
    for (name, expected) in [("events.jsonl", part), ("manifest.pending.json", manifest)] {
        let p = dir.join(name);
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
    sync_dir(dir)?;
    Ok(())
}

/// The real archive adapter consumes the shared typed hook; it does not define another policy.
pub struct ArchiveFailureAdapter;
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
            let _ = fs::remove_dir_all(r.join(names(&i).0));
            let out = commit_jsonl_segment(&mut w, &r, &i, &x, ArchiveFault::None).unwrap();
            assert_eq!(out.checkpoint, 2);
            fs::remove_dir_all(b).unwrap()
        }
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
    }
}
