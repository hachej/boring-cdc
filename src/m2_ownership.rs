//! Exclusive runtime/maintenance ownership and bounded command authorization.
//!
//! This module is the reusable ownership boundary. Workflows receive an `OwnershipGuard`;
//! they do not open a second SQLite writer or load administration credentials themselves.

use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub const SOCKET_DIR_MODE: u32 = 0o700;
pub const SOCKET_MODE: u32 = 0o600;
// M0-PROVISIONAL: boring-cdc-d-security
pub const MAX_COMMAND_BYTES: usize = 64 * 1024;
// M0-PROVISIONAL: boring-cdc-d-security
pub const COMMAND_TIMEOUT: Duration = Duration::from_secs(5);

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
    fn try_lock(&mut self, key: i64) -> Result<bool, OwnershipError>;
    fn healthy(&mut self) -> bool;
    fn unlock(&mut self, key: i64) -> Result<(), OwnershipError>;
}

/// Exclusive local-store lock. A sidecar is used instead of SQLite's transient write lock so
/// ownership covers reads, reconciliation and remote effects between transactions.
pub struct StateLock {
    path: PathBuf,
    file: File,
}
impl StateLock {
    pub fn acquire(store: &Path, run_id: &str, nonce: &str) -> Result<Self, OwnershipError> {
        let path = store.with_extension("ownership.lock");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(SOCKET_MODE)
            .open(&path)
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    OwnershipError::StateAlreadyOwned
                } else {
                    e.into()
                }
            })?;
        file.write_all(
            format!(
                "pid={} run={} nonce={}\n",
                std::process::id(),
                run_id,
                nonce
            )
            .as_bytes(),
        )?;
        file.sync_all()?;
        let mode = file.metadata()?.permissions().mode() & 0o777;
        if mode != SOCKET_MODE {
            let _ = fs::remove_file(&path);
            return Err(OwnershipError::Io("unsafe lock permissions".into()));
        }
        Ok(Self { path, file })
    }
}
impl Drop for StateLock {
    fn drop(&mut self) {
        let _ = self.file.sync_all();
        let _ = fs::remove_file(&self.path);
    }
}

pub struct OwnershipGuard<S: SourceLockSession> {
    _state: StateLock,
    source: S,
    key: i64,
    pub run_id: String,
    pub kind: OwnerKind,
    deadline: Instant,
    fenced: bool,
}
impl<S: SourceLockSession> OwnershipGuard<S> {
    /// Canonical acquisition order is local store, then source advisory lock.
    pub fn acquire(
        store: &Path,
        run_id: String,
        kind: OwnerKind,
        key: i64,
        deadline: Duration,
        mut source: S,
    ) -> Result<Self, OwnershipError> {
        let nonce = source.connection_nonce().to_owned();
        let state = StateLock::acquire(store, &run_id, &nonce)?;
        if !source.try_lock(key)? {
            return Err(OwnershipError::SourceAlreadyOwned);
        }
        Ok(Self {
            _state: state,
            source,
            key,
            run_id,
            kind,
            deadline: Instant::now() + deadline,
            fenced: false,
        })
    }
    pub fn admit_source_mutation(&mut self, worst_case: Duration) -> Result<(), OwnershipError> {
        if self.fenced || !self.source.healthy() {
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
        if self.fenced || !self.source.healthy() {
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
    pub fn dispatch_allowed(&self) -> bool {
        !self.fenced
    }
    pub fn backend_pid(&self) -> i32 {
        self.source.backend_pid()
    }
}
impl<S: SourceLockSession> Drop for OwnershipGuard<S> {
    fn drop(&mut self) {
        if !self.fenced {
            let _ = self.source.unlock(self.key);
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
pub fn startup_decision(phase: ReseedPhase) -> StartupDecision {
    match phase {
        ReseedPhase::Prepared
        | ReseedPhase::SourceRecreatedAmbiguous
        | ReseedPhase::AmbiguousRequiresRestart
        | ReseedPhase::FailedRequiresFreshReseed => StartupDecision::BlockMaintenance,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DryRun {
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
    let id = hash_parts(&[b"request-v1", nonce, payload_digest.as_bytes()]);
    let token = hash_parts(&[
        b"confirm-v1",
        secret,
        nonce,
        id.as_bytes(),
        payload_digest.as_bytes(),
    ]);
    let p = DryRun {
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
    payload: &[u8],
    current: &ActionSnapshot,
    now_mono_ms: u64,
) -> Result<(), ConfirmError> {
    if now_mono_ms > plan.expires_mono_ms {
        return Err(ConfirmError::Expired);
    }
    if !constant_time_eq(supplied.as_bytes(), plan.confirm_token.as_bytes()) {
        return Err(ConfirmError::InvalidToken);
    }
    writer.consume(&plan.request_id, payload, current)
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
    peer_uid: u32,
    owner_uid: u32,
    bytes: usize,
    elapsed: Duration,
) -> Result<(), &'static str> {
    let dir = path.parent().ok_or("socket_parent_missing")?;
    if dir
        .metadata()
        .map_err(|_| "socket_metadata")?
        .permissions()
        .mode()
        & 0o777
        != SOCKET_DIR_MODE
    {
        return Err("socket_dir_mode");
    }
    if path
        .metadata()
        .map_err(|_| "socket_metadata")?
        .permissions()
        .mode()
        & 0o777
        != SOCKET_MODE
    {
        return Err("socket_mode");
    }
    if peer_uid != owner_uid {
        return Err("peer_rejected");
    }
    if bytes > MAX_COMMAND_BYTES {
        return Err("message_too_large");
    }
    if elapsed > COMMAND_TIMEOUT {
        return Err("request_timeout");
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
    pub fn expose_for_admin_call<T>(&self, f: impl FnOnce(&[u8]) -> T) -> T {
        f(&self.0)
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
        fn try_lock(&mut self, _: i64) -> Result<bool, OwnershipError> {
            Ok(self
                .source_lock
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok())
        }
        fn healthy(&mut self) -> bool {
            self.healthy.load(Ordering::SeqCst)
        }
        fn unlock(&mut self, _: i64) -> Result<(), OwnershipError> {
            self.source_lock.store(false, Ordering::SeqCst);
            Ok(())
        }
    }
    fn path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("boring-cdc-own-{}-{}", std::process::id(), tag))
    }
    fn session(n: &str) -> Session {
        Session {
            nonce: n.into(),
            healthy: Arc::new(AtomicBool::new(true)),
            source_lock: Arc::new(AtomicBool::new(false)),
        }
    }
    #[test]
    fn two_processes_same_store_fail_closed() {
        let p = path("same");
        let g = OwnershipGuard::acquire(
            &p,
            "r1".into(),
            OwnerKind::Runtime,
            1,
            Duration::from_secs(1),
            session("n1"),
        )
        .unwrap();
        assert!(matches!(
            OwnershipGuard::acquire(
                &p,
                "r2".into(),
                OwnerKind::Runtime,
                1,
                Duration::from_secs(1),
                session("n2")
            ),
            Err(OwnershipError::StateAlreadyOwned)
        ));
        drop(g)
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
            99,
            Duration::from_secs(1),
            make("n1"),
        )
        .unwrap();
        assert!(matches!(
            OwnershipGuard::acquire(
                &p2,
                "r2".into(),
                OwnerKind::Runtime,
                99,
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
                99,
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
            1,
            Duration::from_millis(10),
            s,
        )
        .unwrap();
        assert_eq!(
            g.admit_source_mutation(Duration::from_secs(1)),
            Err(OwnershipError::DeadlineCannotFit)
        );
        health.store(false, Ordering::SeqCst);
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
            1,
            Duration::from_secs(1),
            session("n"),
        )
        .unwrap();
        assert_eq!(
            g.unexpected_transport_loss(),
            OwnershipError::UnexpectedTransportLoss
        );
        assert!(!g.dispatch_allowed())
    }
    #[test]
    fn startup_phase_machine_blocks_only_ambiguous_maintenance() {
        assert_eq!(
            startup_decision(ReseedPhase::Prepared),
            StartupDecision::BlockMaintenance
        );
        assert_eq!(
            startup_decision(ReseedPhase::SourceRecreatedAmbiguous),
            StartupDecision::BlockMaintenance
        );
        assert_eq!(
            startup_decision(ReseedPhase::SnapshotImported),
            StartupDecision::ResumeWithReseedIncomplete
        );
        assert_eq!(
            startup_decision(ReseedPhase::Promoting),
            StartupDecision::ResumeWithReseedIncomplete
        );
        assert_eq!(
            startup_decision(ReseedPhase::Complete),
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
        confirm(&mut w, &p1, &p1.confirm_token, b"x", &snap(Some("run")), 10).unwrap();
        assert_eq!(
            confirm(&mut w, &p1, &p1.confirm_token, b"x", &snap(Some("run")), 10),
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
            confirm(&mut w, &p, &p.confirm_token, b"x", &changed, 10),
            Err(ConfirmError::OwnershipChanged)
        );
        changed = snap(None);
        changed.control_revisions[0].1 = 3;
        assert_eq!(
            confirm(&mut w, &p, &p.confirm_token, b"x", &changed, 10),
            Err(ConfirmError::StaleActionPlan)
        );
        assert_eq!(
            confirm(&mut w, &p, &p.confirm_token, b"changed", &snap(None), 10),
            Err(ConfirmError::PayloadChanged)
        );
        assert_eq!(
            confirm(&mut w, &p, &p.confirm_token, b"x", &snap(None), 21),
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
        assert_eq!(c.expose_for_admin_call(|x| x.len()), 38);
        drop(c);
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
        fs::write(&p, []).unwrap();
        fs::set_permissions(&p, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(validate_socket(&p, 100, 100, 1, Duration::ZERO), Ok(()));
        assert_eq!(
            validate_socket(&p, 101, 100, 1, Duration::ZERO),
            Err("peer_rejected")
        );
        assert_eq!(
            validate_socket(&p, 100, 100, MAX_COMMAND_BYTES + 1, Duration::ZERO),
            Err("message_too_large")
        );
        assert_eq!(
            validate_socket(&p, 100, 100, 1, COMMAND_TIMEOUT + Duration::from_millis(1)),
            Err("request_timeout")
        );
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
