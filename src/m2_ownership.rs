//! Exclusive runtime/maintenance ownership and bounded command authorization.
//!
//! This module is the reusable ownership boundary. Workflows receive an `OwnershipGuard`;
//! they do not open a second SQLite writer or load administration credentials themselves.

use sha2::{Digest, Sha256};
use std::ffi::CString;
use std::fs::{File, TryLockError};
use std::io::{Read, Seek, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::UnixStream;
use std::path::{Component, Path};
use std::time::{Duration, Instant};

pub const SOCKET_DIR_MODE: u32 = 0o700;
pub const SOCKET_MODE: u32 = 0o600;
pub const MAX_COMMAND_BYTES: usize = 1024 * 1024;
pub const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
pub const COMMAND_READ_TIMEOUT: Duration = Duration::from_secs(10);
pub const COMMAND_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
// v0.1 supports named Linux filesystems only; libc flags are used descriptor-relative.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerKind {
    Runtime,
    Maintenance,
    OfflineDryRun,
    OfflineConfirm,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipError {
    StateAlreadyOwned,
    SourceAlreadyOwned,
    SourceSessionLost,
    DeadlineCannotFit,
    UnexpectedTransportLoss,
    ReconciliationRequired,
    Io(String),
}

impl From<std::io::Error> for OwnershipError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.kind().to_string())
    }
}

/// Dedicated, non-pooled PostgreSQL advisory-lock session. Implementations must not reconnect.
pub trait SourceLockSession {
    fn backend_pid(&self) -> i32;
    fn connection_nonce(&self) -> &str;
    /// Fixed key derived by the adapter from the validated `SourceIdentity`.
    fn advisory_lock_key(&self) -> i64;
    fn try_lock(&mut self) -> Result<bool, OwnershipError>;
    fn healthy(&mut self) -> bool;
    fn unlock(&mut self) -> Result<(), OwnershipError>;
}

/// Exclusive local-store lock. A sidecar is used instead of SQLite's transient write lock so
/// ownership covers reads, reconciliation and remote effects between transactions.
pub struct StateLock {
    file: File,
    lock_dev: u64,
    lock_ino: u64,
    requires_reconciliation: bool,
}
impl StateLock {
    pub fn acquire(store: &Path, run_id: &str, nonce: &str) -> Result<Self, OwnershipError> {
        let path = store.with_extension("ownership.lock");
        let parent = path
            .parent()
            .ok_or_else(|| OwnershipError::Io("lock parent".into()))?;
        let process_uid = std::fs::metadata("/proc/self")?.uid();
        let parent_fd = open_directory_components_nofollow(parent)?;
        let parent_metadata = File::from(parent_fd.try_clone()?).metadata()?;
        if !parent_metadata.is_dir()
            || parent_metadata.uid() != process_uid
            || parent_metadata.permissions().mode() & 0o077 != 0
        {
            return Err(OwnershipError::Io("unsafe lock parent".into()));
        }
        let name = CString::new(
            path.file_name()
                .ok_or_else(|| OwnershipError::Io("lock file".into()))?
                .as_bytes(),
        )
        .map_err(|_| OwnershipError::Io("lock file".into()))?;
        // The validated parent descriptor cannot be swapped out between validation and create.
        let raw = unsafe {
            libc::openat(
                parent_fd.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDWR | libc::O_CREAT | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                SOCKET_MODE,
            )
        };
        if raw < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let mut file = unsafe { File::from_raw_fd(raw) };
        file.try_lock().map_err(|e| match e {
            TryLockError::WouldBlock => OwnershipError::StateAlreadyOwned,
            TryLockError::Error(error) => error.into(),
        })?;
        let metadata = file.metadata()?;
        if !metadata.file_type().is_file()
            || metadata.permissions().mode() & 0o777 != SOCKET_MODE
            || metadata.uid() != process_uid
            || metadata.nlink() != 1
        {
            return Err(OwnershipError::Io("unsafe lock file".into()));
        }
        let mut previous = String::new();
        file.rewind()?;
        file.read_to_string(&mut previous)?;
        let requires_reconciliation = previous.contains("clean_release=pending");
        file.set_len(0)?;
        file.rewind()?;
        file.write_all(
            format!(
                "pid={} run={} nonce={} clean_release=pending\n",
                std::process::id(),
                run_id,
                nonce
            )
            .as_bytes(),
        )?;
        file.sync_all()?;
        Ok(Self {
            lock_dev: metadata.dev(),
            lock_ino: metadata.ino(),
            file,
            requires_reconciliation,
        })
    }

    /// Proves this guard owns the sidecar derived from the supplied SQLite store.
    pub fn protects_store(&self, store: &Path) -> bool {
        store
            .with_extension("ownership.lock")
            .metadata()
            .is_ok_and(|m| m.dev() == self.lock_dev && m.ino() == self.lock_ino)
    }

    fn mark_clean_release(&mut self) -> Result<(), OwnershipError> {
        self.file.set_len(0)?;
        self.file.rewind()?;
        self.file.write_all(b"clean_release=proved\n")?;
        self.file.sync_all()?;
        Ok(())
    }
}

fn open_directory_components_nofollow(path: &Path) -> Result<OwnedFd, OwnershipError> {
    let start = if path.is_absolute() { c"/" } else { c"." };
    let raw = unsafe {
        libc::open(
            start.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if raw < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let mut current = unsafe { OwnedFd::from_raw_fd(raw) };
    for component in path.components() {
        let Component::Normal(value) = component else {
            if matches!(component, Component::ParentDir) {
                return Err(OwnershipError::Io("unsafe lock parent".into()));
            }
            continue;
        };
        let name = CString::new(value.as_bytes())
            .map_err(|_| OwnershipError::Io("unsafe lock parent".into()))?;
        let next = unsafe {
            libc::openat(
                current.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if next < 0 {
            return Err(OwnershipError::Io("unsafe lock parent".into()));
        }
        current = unsafe { OwnedFd::from_raw_fd(next) };
    }
    Ok(current)
}

impl Drop for StateLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

pub struct OwnershipGuard<S: SourceLockSession> {
    source: S,
    state: StateLock,
    pub run_id: String,
    pub kind: OwnerKind,
    deadline: Instant,
    backend_pid: i32,
    connection_nonce: String,
    advisory_lock_key: i64,
    fenced: bool,
    reconciled: bool,
}
impl<S: SourceLockSession> OwnershipGuard<S> {
    /// Canonical acquisition order is local store, then source advisory lock.
    pub fn acquire(
        store: &Path,
        run_id: String,
        kind: OwnerKind,
        deadline: Duration,
        mut source: S,
    ) -> Result<Self, OwnershipError> {
        let connection_nonce = source.connection_nonce().to_owned();
        let backend_pid = source.backend_pid();
        let advisory_lock_key = source.advisory_lock_key();
        let state = StateLock::acquire(store, &run_id, &connection_nonce)?;
        if !source.try_lock()? {
            return Err(OwnershipError::SourceAlreadyOwned);
        }
        if !source.healthy()
            || source.backend_pid() != backend_pid
            || source.connection_nonce() != connection_nonce
            || source.advisory_lock_key() != advisory_lock_key
        {
            let _ = source.unlock();
            return Err(OwnershipError::SourceSessionLost);
        }
        let reconciled = !state.requires_reconciliation;
        Ok(Self {
            source,
            state,
            run_id,
            kind,
            deadline: Instant::now() + deadline,
            backend_pid,
            connection_nonce,
            advisory_lock_key,
            fenced: false,
            reconciled,
        })
    }
    fn source_identity_is_live(&mut self) -> bool {
        self.source.healthy()
            && self.source.backend_pid() == self.backend_pid
            && self.source.connection_nonce() == self.connection_nonce
            && self.source.advisory_lock_key() == self.advisory_lock_key
    }

    pub fn reconcile_after_unclean_release(
        &mut self,
        reconcile_source_and_external_state: impl FnOnce() -> bool,
    ) -> Result<(), OwnershipError> {
        if self.fenced || !self.source_identity_is_live() {
            self.fenced = true;
            return Err(OwnershipError::SourceSessionLost);
        }
        if !reconcile_source_and_external_state() {
            return Err(OwnershipError::ReconciliationRequired);
        }
        self.reconciled = true;
        Ok(())
    }
    pub fn admit_source_mutation(&mut self, worst_case: Duration) -> Result<(), OwnershipError> {
        if !self.reconciled {
            return Err(OwnershipError::ReconciliationRequired);
        }
        if self.fenced || !self.source_identity_is_live() {
            self.fenced = true;
            return Err(OwnershipError::SourceSessionLost);
        }
        if Instant::now()
            .checked_add(worst_case)
            .is_none_or(|end| end > self.deadline)
        {
            return Err(OwnershipError::DeadlineCannotFit);
        }
        Ok(())
    }
    pub fn probe(&mut self, extension: Duration) -> Result<(), OwnershipError> {
        if self.fenced || !self.source_identity_is_live() {
            self.fenced = true;
            return Err(OwnershipError::SourceSessionLost);
        }
        self.deadline = Instant::now() + extension;
        Ok(())
    }
    /// Any unexpected CopyBoth EOF is process ownership loss even if advisory health remains.
    pub fn unexpected_transport_loss(&mut self) -> OwnershipError {
        self.fenced = true;
        OwnershipError::UnexpectedTransportLoss
    }
    pub fn dispatch_allowed(&mut self) -> bool {
        if !self.fenced && !self.source_identity_is_live() {
            self.fenced = true;
        }
        !self.fenced && self.reconciled
    }
    pub fn backend_pid(&self) -> i32 {
        self.backend_pid
    }
    pub fn advisory_lock_key(&self) -> i64 {
        self.advisory_lock_key
    }
}
impl<S: SourceLockSession> Drop for OwnershipGuard<S> {
    fn drop(&mut self) {
        if self.reconciled
            && !self.fenced
            && self.source_identity_is_live()
            && self.source.unlock().is_ok()
        {
            let _ = self.state.mark_clean_release();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReseedPhase {
    None,
    Prepared,
    SourceRecreatedAmbiguous,
    SourceRecreatedBound,
    SnapshotImported,
    Promoting,
    Complete,
    AmbiguousRequiresRestart,
    FailedRequiresFreshReseed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupDecision {
    Start,
    ResumeWithReseedIncomplete,
    BlockMaintenance,
}
pub fn startup_decision(phase: ReseedPhase, bound_live_snapshot: bool) -> StartupDecision {
    match phase {
        ReseedPhase::Prepared
        | ReseedPhase::SourceRecreatedAmbiguous
        | ReseedPhase::AmbiguousRequiresRestart
        | ReseedPhase::FailedRequiresFreshReseed => StartupDecision::BlockMaintenance,
        ReseedPhase::SourceRecreatedBound if !bound_live_snapshot => {
            StartupDecision::BlockMaintenance
        }
        ReseedPhase::SourceRecreatedBound
        | ReseedPhase::SnapshotImported
        | ReseedPhase::Promoting => StartupDecision::ResumeWithReseedIncomplete,
        ReseedPhase::None | ReseedPhase::Complete => StartupDecision::Start,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionSnapshot {
    pub expected_run_id: Option<String>,
    pub source_fingerprint: String,
    pub config_fingerprint: String,
    pub table_set_fingerprint: String,
    pub anchor_fingerprint: String,
    pub generation: u64,
    pub control_revisions: Vec<(String, u64)>,
    pub history_available: bool,
    pub reserve_sufficient: bool,
}
// Deliberately not `Debug`: the nonce and confirmation token must never enter logs.
#[derive(Clone, PartialEq, Eq)]
pub struct DryRun {
    nonce: Vec<u8>,
    pub request_id: String,
    pub confirm_token: String,
    pub canonical_argv: Vec<String>,
    pub snapshot: ActionSnapshot,
    pub expires_mono_ms: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfirmError {
    Expired,
    Replay,
    StaleActionPlan,
    OwnershipChanged,
    InadequateReserve,
    InvalidToken,
    PayloadChanged,
}

pub trait RequestWriter {
    fn issue(&mut self, plan: &DryRun, payload: &[u8]) -> Result<(), ConfirmError>;
    /// Implementations atomically compare predicates/revisions and consume the nonce with intent/pins.
    fn consume(
        &mut self,
        request_id: &str,
        payload: &[u8],
        current: &ActionSnapshot,
    ) -> Result<(), ConfirmError>;
    fn abort_prior_run_nonterminal(&mut self, current_run: &str) -> Result<usize, ConfirmError>;
}

pub fn issue_dry_run<W: RequestWriter>(
    writer: &mut W,
    secret: &[u8],
    nonce: &[u8],
    argv: Vec<String>,
    payload: &[u8],
    snapshot: ActionSnapshot,
    expires_mono_ms: u64,
) -> Result<DryRun, ConfirmError> {
    let payload_digest = hash_parts(&[b"payload-v1", payload]);
    let argv_digest = hash_argv(&argv);
    let id = hash_parts(&[
        b"request-v1",
        nonce,
        payload_digest.as_bytes(),
        argv_digest.as_bytes(),
    ]);
    let expiry = expires_mono_ms.to_be_bytes();
    let token = hash_parts(&[
        b"confirm-v1",
        secret,
        nonce,
        id.as_bytes(),
        payload_digest.as_bytes(),
        argv_digest.as_bytes(),
        &expiry,
    ]);
    let p = DryRun {
        nonce: nonce.to_vec(),
        request_id: id,
        confirm_token: token,
        canonical_argv: argv,
        snapshot,
        expires_mono_ms,
    };
    writer.issue(&p, payload)?;
    Ok(p)
}
pub fn confirm<W: RequestWriter>(
    writer: &mut W,
    plan: &DryRun,
    supplied: &str,
    secret: &[u8],
    payload: &[u8],
    current: &ActionSnapshot,
    now_mono_ms: u64,
) -> Result<(), ConfirmError> {
    if now_mono_ms > plan.expires_mono_ms {
        return Err(ConfirmError::Expired);
    }
    let payload_digest = hash_parts(&[b"payload-v1", payload]);
    let argv_digest = hash_argv(&plan.canonical_argv);
    let expiry = plan.expires_mono_ms.to_be_bytes();
    let expected = hash_parts(&[
        b"confirm-v1",
        secret,
        &plan.nonce,
        plan.request_id.as_bytes(),
        payload_digest.as_bytes(),
        argv_digest.as_bytes(),
        &expiry,
    ]);
    if !constant_time_eq(expected.as_bytes(), plan.confirm_token.as_bytes())
        || !constant_time_eq(supplied.as_bytes(), plan.confirm_token.as_bytes())
    {
        return Err(ConfirmError::InvalidToken);
    }
    writer.consume(&plan.request_id, payload, current)
}
fn hash_argv(argv: &[String]) -> String {
    let parts = argv.iter().map(String::as_bytes).collect::<Vec<_>>();
    hash_parts(&parts)
}
fn hash_parts(parts: &[&[u8]]) -> String {
    let mut h = Sha256::new();
    for p in parts {
        h.update((*p).len().to_be_bytes());
        h.update(p);
    }
    format!("{:x}", h.finalize())
}
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut x = 0;
    for (i, j) in a.iter().zip(b) {
        x |= i ^ j;
    }
    x == 0
}

/// POSIX-shell rendering for the human endpoint. JSON consumers use `canonical_argv` directly.
pub fn shell_command(argv: &[String]) -> String {
    argv.iter()
        .map(|s| format!("'{}'", s.replace('\'', "'\\''")))
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn validate_socket(
    path: &Path,
    accepted: &UnixStream,
    bytes: usize,
    elapsed: Duration,
) -> Result<(), &'static str> {
    let dir = path.parent().ok_or("socket_parent_missing")?;
    let process_uid = std::fs::metadata("/proc/self")
        .map_err(|_| "socket_metadata")?
        .uid();
    let dir_metadata = dir.symlink_metadata().map_err(|_| "socket_metadata")?;
    if !dir_metadata.is_dir()
        || dir_metadata.uid() != process_uid
        || dir_metadata.permissions().mode() & 0o777 != SOCKET_DIR_MODE
    {
        return Err("socket_dir_mode");
    }
    let socket_metadata = path.symlink_metadata().map_err(|_| "socket_metadata")?;
    if !socket_metadata.file_type().is_socket()
        || socket_metadata.uid() != process_uid
        || socket_metadata.permissions().mode() & 0o777 != SOCKET_MODE
    {
        return Err("socket_mode");
    }
    let mut credentials = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut length = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            accepted.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut credentials as *mut libc::ucred).cast(),
            &mut length,
        )
    };
    if rc != 0 || length as usize != std::mem::size_of::<libc::ucred>() {
        return Err("peer_credentials");
    }
    if credentials.uid != process_uid || socket_metadata.uid() != credentials.uid {
        return Err("peer_rejected");
    }
    if bytes > MAX_COMMAND_BYTES {
        return Err("message_too_large");
    }
    if elapsed > COMMAND_READ_TIMEOUT {
        return Err("request_timeout");
    }
    Ok(())
}

/// Enforces the independently approved response-size and write-duration limits.
pub fn validate_response(bytes: usize, elapsed: Duration) -> Result<(), &'static str> {
    if bytes > MAX_RESPONSE_BYTES {
        return Err("response_too_large");
    }
    if elapsed > COMMAND_WRITE_TIMEOUT {
        return Err("response_timeout");
    }
    Ok(())
}

/// Administration credentials exist only in this value and are overwritten on drop.
pub struct AdminCredential(Vec<u8>);
impl AdminCredential {
    pub fn load(path: &Path) -> Result<Self, OwnershipError> {
        let mode = path.metadata()?.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(OwnershipError::Io("admin credential permissions".into()));
        }
        let mut f = File::open(path)?;
        let mut b = Vec::new();
        f.read_to_end(&mut b)?;
        if b.is_empty() {
            return Err(OwnershipError::Io("empty admin credential".into()));
        }
        Ok(Self(b))
    }
    pub fn one_shot_admin_call<T>(mut self, f: impl FnOnce(&[u8]) -> T) -> T {
        let result = f(&self.0);
        self.0.fill(0);
        result
    }
}
impl Drop for AdminCredential {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    struct Session {
        nonce: String,
        healthy: Arc<AtomicBool>,
        source_lock: Arc<AtomicBool>,
    }
    impl SourceLockSession for Session {
        fn backend_pid(&self) -> i32 {
            7
        }
        fn connection_nonce(&self) -> &str {
            &self.nonce
        }
        fn advisory_lock_key(&self) -> i64 {
            99
        }
        fn try_lock(&mut self) -> Result<bool, OwnershipError> {
            Ok(self
                .source_lock
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok())
        }
        fn healthy(&mut self) -> bool {
            self.healthy.load(Ordering::SeqCst)
        }
        fn unlock(&mut self) -> Result<(), OwnershipError> {
            self.source_lock.store(false, Ordering::SeqCst);
            Ok(())
        }
    }
    fn path(tag: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("boring-cdc-own-{}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        root.join(tag)
    }
    fn session(n: &str) -> Session {
        Session {
            nonce: n.into(),
            healthy: Arc::new(AtomicBool::new(true)),
            source_lock: Arc::new(AtomicBool::new(false)),
        }
    }
    #[test]
    fn symlinked_lock_parent_is_rejected() {
        let root = path("symlink-parent-root");
        fs::create_dir_all(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let real = root.join("real");
        let linked = root.join("linked");
        fs::create_dir(&real).unwrap();
        fs::set_permissions(&real, fs::Permissions::from_mode(0o700)).unwrap();
        fs::create_dir(real.join("nested")).unwrap();
        fs::set_permissions(real.join("nested"), fs::Permissions::from_mode(0o700)).unwrap();
        std::os::unix::fs::symlink(&real, &linked).unwrap();
        assert!(matches!(
            StateLock::acquire(&linked.join("nested/state.db"), "run", "nonce"),
            Err(OwnershipError::Io(message)) if message == "unsafe lock parent"
        ));
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn two_processes_same_store_fail_closed() {
        let child_path = std::env::var_os("BORING_CDC_OWNERSHIP_CHILD");
        let p = child_path
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| path("same-process"));
        let ready = p.with_extension("ready");
        let release = p.with_extension("release");
        if child_path.is_some() {
            let _guard = OwnershipGuard::acquire(
                &p,
                "child".into(),
                OwnerKind::Runtime,
                Duration::from_secs(10),
                session("child-nonce"),
            )
            .unwrap();
            fs::write(&ready, b"ready").unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            while !release.exists() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
            assert!(release.exists(), "parent never released child");
            return;
        }
        for stale in [&ready, &release, &p.with_extension("ownership.lock")] {
            let _ = fs::remove_file(stale);
        }
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "m2_ownership::tests::two_processes_same_store_fail_closed",
                "--nocapture",
            ])
            .env("BORING_CDC_OWNERSHIP_CHILD", &p)
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !ready.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(ready.exists(), "child owner did not become ready");
        assert!(matches!(
            OwnershipGuard::acquire(
                &p,
                "parent".into(),
                OwnerKind::Runtime,
                Duration::from_secs(1),
                session("parent-nonce")
            ),
            Err(OwnershipError::StateAlreadyOwned)
        ));
        fs::write(&release, b"release").unwrap();
        assert!(child.wait().unwrap().success());
        assert_eq!(
            fs::read_to_string(p.with_extension("ownership.lock")).unwrap(),
            "clean_release=proved\n"
        );
        let g2 = OwnershipGuard::acquire(
            &p,
            "successor".into(),
            OwnerKind::Runtime,
            Duration::from_secs(1),
            session("successor-nonce"),
        )
        .unwrap();
        assert_eq!(g2.advisory_lock_key(), 99);
        let _ = fs::remove_file(ready);
        let _ = fs::remove_file(release);
    }
    #[test]
    fn two_state_paths_same_source_fail_closed() {
        let p1 = path("source-a");
        let p2 = path("source-b");
        let shared = Arc::new(AtomicBool::new(false));
        let make = |nonce: &str| Session {
            nonce: nonce.into(),
            healthy: Arc::new(AtomicBool::new(true)),
            source_lock: shared.clone(),
        };
        let g = OwnershipGuard::acquire(
            &p1,
            "r1".into(),
            OwnerKind::Runtime,
            Duration::from_secs(1),
            make("n1"),
        )
        .unwrap();
        assert!(matches!(
            OwnershipGuard::acquire(
                &p2,
                "r2".into(),
                OwnerKind::Runtime,
                Duration::from_secs(1),
                make("n2")
            ),
            Err(OwnershipError::SourceAlreadyOwned)
        ));
        drop(g);
        assert!(
            OwnershipGuard::acquire(
                &p2,
                "r3".into(),
                OwnerKind::Maintenance,
                Duration::from_secs(1),
                make("n3")
            )
            .is_ok()
        );
    }

    #[test]
    fn deadline_and_session_loss_fence_without_reacquire() {
        let p = path("loss");
        let health = Arc::new(AtomicBool::new(true));
        let s = Session {
            nonce: "n".into(),
            healthy: health.clone(),
            source_lock: Arc::new(AtomicBool::new(false)),
        };
        let mut g = OwnershipGuard::acquire(
            &p,
            "r".into(),
            OwnerKind::Runtime,
            Duration::from_millis(10),
            s,
        )
        .unwrap();
        assert_eq!(
            g.admit_source_mutation(Duration::from_secs(1)),
            Err(OwnershipError::DeadlineCannotFit)
        );
        health.store(false, Ordering::SeqCst);
        assert!(!g.dispatch_allowed());
        assert_eq!(
            g.probe(Duration::from_secs(1)),
            Err(OwnershipError::SourceSessionLost)
        );
        assert!(!g.dispatch_allowed())
    }
    #[test]
    fn unexpected_copyboth_loss_always_fences() {
        let p = path("eof");
        let mut g = OwnershipGuard::acquire(
            &p,
            "r".into(),
            OwnerKind::Runtime,
            Duration::from_secs(1),
            session("n"),
        )
        .unwrap();
        assert_eq!(
            g.unexpected_transport_loss(),
            OwnershipError::UnexpectedTransportLoss
        );
        assert!(!g.dispatch_allowed());
        drop(g);
        assert!(
            fs::read_to_string(p.with_extension("ownership.lock"))
                .unwrap()
                .contains("clean_release=pending")
        )
    }
    #[test]
    fn startup_phase_machine_blocks_only_ambiguous_maintenance() {
        assert_eq!(
            startup_decision(ReseedPhase::Prepared, true),
            StartupDecision::BlockMaintenance
        );
        assert_eq!(
            startup_decision(ReseedPhase::SourceRecreatedAmbiguous, true),
            StartupDecision::BlockMaintenance
        );
        assert_eq!(
            startup_decision(ReseedPhase::SourceRecreatedBound, false),
            StartupDecision::BlockMaintenance
        );
        assert_eq!(
            startup_decision(ReseedPhase::SourceRecreatedBound, true),
            StartupDecision::ResumeWithReseedIncomplete
        );
        assert_eq!(
            startup_decision(ReseedPhase::SnapshotImported, false),
            StartupDecision::ResumeWithReseedIncomplete
        );
        assert_eq!(
            startup_decision(ReseedPhase::Promoting, true),
            StartupDecision::ResumeWithReseedIncomplete
        );
        assert_eq!(
            startup_decision(ReseedPhase::Complete, true),
            StartupDecision::Start
        )
    }
    #[derive(Default)]
    struct Writer {
        plans: HashMap<String, (Vec<u8>, ActionSnapshot, bool)>,
        prior: usize,
    }
    impl RequestWriter for Writer {
        fn issue(&mut self, p: &DryRun, b: &[u8]) -> Result<(), ConfirmError> {
            if self.plans.contains_key(&p.request_id) {
                return Err(ConfirmError::Replay);
            }
            self.plans.insert(
                p.request_id.clone(),
                (b.to_vec(), p.snapshot.clone(), false),
            );
            Ok(())
        }
        fn consume(
            &mut self,
            id: &str,
            b: &[u8],
            cur: &ActionSnapshot,
        ) -> Result<(), ConfirmError> {
            let (old, s, used) = self.plans.get_mut(id).ok_or(ConfirmError::InvalidToken)?;
            if *used {
                return Err(ConfirmError::Replay);
            }
            if old != b {
                return Err(ConfirmError::PayloadChanged);
            }
            if s.expected_run_id != cur.expected_run_id {
                return Err(ConfirmError::OwnershipChanged);
            }
            if !cur.reserve_sufficient {
                return Err(ConfirmError::InadequateReserve);
            }
            if s != cur {
                return Err(ConfirmError::StaleActionPlan);
            }
            *used = true;
            Ok(())
        }
        fn abort_prior_run_nonterminal(&mut self, _: &str) -> Result<usize, ConfirmError> {
            let n = self.prior;
            self.prior = 0;
            Ok(n)
        }
    }
    fn snap(run: Option<&str>) -> ActionSnapshot {
        ActionSnapshot {
            expected_run_id: run.map(str::to_string),
            source_fingerprint: "s".into(),
            config_fingerprint: "c".into(),
            table_set_fingerprint: "t".into(),
            anchor_fingerprint: "a".into(),
            generation: 1,
            control_revisions: vec![("destination".into(), 2)],
            history_available: true,
            reserve_sufficient: true,
        }
    }
    #[test]
    fn nonce_identity_lost_response_replay_and_later_repeat() {
        let mut w = Writer::default();
        let p1 = issue_dry_run(
            &mut w,
            b"secret",
            b"nonce1",
            vec!["pause".into()],
            b"x",
            snap(Some("run")),
            20,
        )
        .unwrap();
        assert!(matches!(w.issue(&p1, b"x"), Err(ConfirmError::Replay)));
        let p2 = issue_dry_run(
            &mut w,
            b"secret",
            b"nonce2",
            vec!["pause".into()],
            b"x",
            snap(Some("run")),
            20,
        )
        .unwrap();
        assert_ne!(p1.request_id, p2.request_id);
        let mut altered_display = p1.clone();
        altered_display.canonical_argv.push("different".into());
        assert_eq!(
            confirm(
                &mut w,
                &altered_display,
                &p1.confirm_token,
                b"secret",
                b"x",
                &snap(Some("run")),
                10,
            ),
            Err(ConfirmError::InvalidToken)
        );
        confirm(
            &mut w,
            &p1,
            &p1.confirm_token,
            b"secret",
            b"x",
            &snap(Some("run")),
            10,
        )
        .unwrap();
        assert_eq!(
            confirm(
                &mut w,
                &p1,
                &p1.confirm_token,
                b"secret",
                b"x",
                &snap(Some("run")),
                10,
            ),
            Err(ConfirmError::Replay)
        )
    }
    #[test]
    fn offline_plan_and_races_revalidate_every_bound_predicate() {
        let mut w = Writer::default();
        let p = issue_dry_run(
            &mut w,
            b"k",
            b"n",
            vec!["reseed".into()],
            b"x",
            snap(None),
            20,
        )
        .unwrap();
        let mut changed = snap(Some("new-runtime"));
        assert_eq!(
            confirm(&mut w, &p, &p.confirm_token, b"k", b"x", &changed, 10),
            Err(ConfirmError::OwnershipChanged)
        );
        changed = snap(None);
        changed.control_revisions[0].1 = 3;
        assert_eq!(
            confirm(&mut w, &p, &p.confirm_token, b"k", b"x", &changed, 10),
            Err(ConfirmError::StaleActionPlan)
        );
        assert_eq!(
            confirm(
                &mut w,
                &p,
                &p.confirm_token,
                b"wrong-secret",
                b"x",
                &snap(None),
                10,
            ),
            Err(ConfirmError::InvalidToken)
        );
        assert_eq!(
            confirm(
                &mut w,
                &p,
                &p.confirm_token,
                b"k",
                b"changed",
                &snap(None),
                10,
            ),
            Err(ConfirmError::InvalidToken)
        );
        assert_eq!(
            confirm(&mut w, &p, &p.confirm_token, b"k", b"x", &snap(None), 21),
            Err(ConfirmError::Expired)
        )
    }
    #[test]
    fn shell_renderer_handles_hostile_arguments() {
        let a = vec![
            "boring-cdc".into(),
            "--".into(),
            "a b'$(touch nope)☃".into(),
            "-leading".into(),
        ];
        let s = shell_command(&a);
        assert_eq!(s, "'boring-cdc' '--' 'a b'\\''$(touch nope)☃' '-leading'")
    }
    #[test]
    fn credential_permissions_are_strict_and_value_is_not_debuggable() {
        let p = path("secret");
        fs::write(&p, b"postgres://admin:seeded-secret@host/db").unwrap();
        fs::set_permissions(&p, fs::Permissions::from_mode(0o600)).unwrap();
        let c = AdminCredential::load(&p).unwrap();
        assert_eq!(c.one_shot_admin_call(|x| x.len()), 38);
        fs::set_permissions(&p, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(AdminCredential::load(&p).is_err());
        fs::remove_file(p).unwrap()
    }
    #[test]
    fn command_endpoint_rejects_peer_mode_message_and_time_bounds() {
        let d = path("sockdir");
        let p = d.join("command.sock");
        fs::create_dir_all(&d).unwrap();
        fs::set_permissions(&d, fs::Permissions::from_mode(0o700)).unwrap();
        let listener = std::os::unix::net::UnixListener::bind(&p).unwrap();
        fs::set_permissions(&p, fs::Permissions::from_mode(0o600)).unwrap();
        let client = UnixStream::connect(&p).unwrap();
        let (accepted, _) = listener.accept().unwrap();
        assert_eq!(validate_socket(&p, &accepted, 1, Duration::ZERO), Ok(()));
        assert_eq!(
            validate_socket(&p, &accepted, MAX_COMMAND_BYTES + 1, Duration::ZERO),
            Err("message_too_large")
        );
        assert_eq!(
            validate_socket(
                &p,
                &accepted,
                1,
                COMMAND_READ_TIMEOUT + Duration::from_millis(1)
            ),
            Err("request_timeout")
        );
        assert_eq!(validate_response(1, Duration::ZERO), Ok(()));
        assert_eq!(
            validate_response(MAX_RESPONSE_BYTES + 1, Duration::ZERO),
            Err("response_too_large")
        );
        assert_eq!(
            validate_response(1, COMMAND_WRITE_TIMEOUT + Duration::from_millis(1)),
            Err("response_timeout")
        );
        drop(client);
        drop(accepted);
        drop(listener);
        fs::remove_dir_all(d).unwrap()
    }
    #[test]
    fn restart_reconciles_then_aborts_nonterminal_requests() {
        let mut w = Writer {
            prior: 2,
            ..Default::default()
        };
        assert_eq!(w.abort_prior_run_nonterminal("new").unwrap(), 2);
        assert_eq!(w.abort_prior_run_nonterminal("new").unwrap(), 0)
    }
}
