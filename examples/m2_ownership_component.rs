use boring_cdc::m2_ownership::{
    OwnerKind, OwnershipError, OwnershipGuard, SourceLockSession, validate_socket,
};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::Duration;

struct FileSession {
    file: File,
    nonce: String,
}
impl FileSession {
    fn open(path: &Path, nonce: &str) -> Self {
        Self {
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
                .unwrap(),
            nonce: nonce.into(),
        }
    }
}
impl SourceLockSession for FileSession {
    fn backend_pid(&self) -> i32 {
        std::process::id() as i32
    }
    fn connection_nonce(&self) -> &str {
        &self.nonce
    }
    fn advisory_lock_key(&self) -> i64 {
        72420260910
    }
    fn try_lock(&mut self) -> Result<bool, OwnershipError> {
        self.file.try_lock().map(|_| true).or_else(|e| match e {
            TryLockError::WouldBlock => Ok(false),
            TryLockError::Error(x) => Err(x.into()),
        })
    }
    fn healthy(&mut self) -> bool {
        true
    }
    fn unlock(&mut self) -> Result<(), OwnershipError> {
        self.file.unlock().map_err(Into::into)
    }
}
struct DockerPgSession {
    container: String,
    nonce: String,
    pid: i32,
    child: Child,
}
impl DockerPgSession {
    fn open(container: &str, nonce: &str) -> Self {
        let app = format!("m2-guard-{nonce}");
        let child = Command::new("docker")
            .args([
                "exec",
                "-e",
                &format!("PGAPPNAME={app}"),
                container,
                "psql",
                "-XAt",
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-c",
                "SELECT pg_advisory_lock(72420260910); SELECT pg_sleep(120)",
            ])
            .spawn()
            .unwrap();
        let mut pid = 0;
        for _ in 0..300 {
            let out=Command::new("docker").args(["exec",container,"psql","-XAt","-U","postgres","-d","postgres","-c",&format!("SELECT pid FROM pg_stat_activity WHERE application_name='{app}' ORDER BY pid LIMIT 1")]).output().unwrap();
            pid = String::from_utf8_lossy(&out.stdout)
                .trim()
                .parse()
                .unwrap_or(0);
            if pid > 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(pid > 0, "PostgreSQL ownership backend missing");
        Self {
            container: container.into(),
            nonce: nonce.into(),
            pid,
            child,
        }
    }
    fn sql(&self, sql: &str) -> String {
        let out = Command::new("docker")
            .args([
                "exec",
                &self.container,
                "psql",
                "-XAt",
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-c",
                sql,
            ])
            .output()
            .unwrap();
        assert!(out.status.success());
        String::from_utf8_lossy(&out.stdout).trim().into()
    }
}
impl SourceLockSession for DockerPgSession {
    fn backend_pid(&self) -> i32 {
        self.pid
    }
    fn connection_nonce(&self) -> &str {
        &self.nonce
    }
    fn advisory_lock_key(&self) -> i64 {
        72420260910
    }
    fn try_lock(&mut self) -> Result<bool, OwnershipError> {
        Ok(self.sql(&format!(
            "SELECT count(*) FROM pg_locks WHERE pid={} AND locktype='advisory' AND granted",
            self.pid
        )) == "1")
    }
    fn healthy(&mut self) -> bool {
        self.sql(&format!(
            "SELECT count(*) FROM pg_stat_activity WHERE pid={} AND application_name='m2-guard-{}'",
            self.pid, self.nonce
        )) == "1"
    }
    fn unlock(&mut self) -> Result<(), OwnershipError> {
        let _ = self.sql(&format!("SELECT pg_terminate_backend({})", self.pid));
        let _ = self.child.wait();
        Ok(())
    }
}
impl Drop for DockerPgSession {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args([
                "exec",
                &self.container,
                "psql",
                "-XAt",
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-c",
                &format!("SELECT pg_terminate_backend({})", self.pid),
            ])
            .output();
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn postgres_guard_probe(root: &Path, container: &str) {
    let store = root.join("pg-state.db");
    let mut owner = OwnershipGuard::acquire(
        &store,
        "pg-owner".into(),
        OwnerKind::Runtime,
        Duration::from_secs(5),
        DockerPgSession::open(container, "owner"),
    )
    .unwrap();
    let contender_store = root.join("pg-contender.db");
    assert!(matches!(
        OwnershipGuard::acquire(
            &contender_store,
            "pg-contender".into(),
            OwnerKind::Runtime,
            Duration::from_secs(5),
            DockerPgSession::open(container, "contender")
        ),
        Err(OwnershipError::SourceAlreadyOwned)
    ));
    let pid = owner.backend_pid();
    let out = Command::new("docker")
        .args([
            "exec",
            container,
            "psql",
            "-XAt",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-c",
            &format!("SELECT pg_terminate_backend({pid})"),
        ])
        .output()
        .unwrap();
    assert!(out.status.success() && String::from_utf8_lossy(&out.stdout).trim() == "t");
    assert!(!owner.dispatch_allowed());
    drop(owner);
    assert!(
        fs::read_to_string(store.with_extension("ownership.lock"))
            .unwrap()
            .contains("clean_release=pending")
    );
    let mut successor = OwnershipGuard::acquire(
        &store,
        "pg-successor".into(),
        OwnerKind::Runtime,
        Duration::from_secs(5),
        DockerPgSession::open(container, "successor"),
    )
    .unwrap();
    assert_eq!(
        successor.admit_source_mutation(Duration::from_millis(1)),
        Err(OwnershipError::ReconciliationRequired)
    );
    successor
        .reconcile_after_unclean_release(|| {
            let out = Command::new("docker")
                .args([
                    "exec",
                    container,
                    "psql",
                    "-XAt",
                    "-U",
                    "postgres",
                    "-d",
                    "postgres",
                    "-c",
                    "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND granted",
                ])
                .output()
                .unwrap();
            out.status.success() && String::from_utf8_lossy(&out.stdout).trim() == "1"
        })
        .unwrap();
    successor
        .admit_source_mutation(Duration::from_millis(1))
        .unwrap();
    drop(successor);
    assert_eq!(
        fs::read_to_string(store.with_extension("ownership.lock")).unwrap(),
        "clean_release=proved\n"
    );
}

fn socket_probe(root: &Path) {
    let dir = root.join("socket");
    fs::create_dir_all(&dir).unwrap();
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
    let path = dir.join("command.sock");
    let listener = UnixListener::bind(&path).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let client = UnixStream::connect(&path).unwrap();
    let (accepted, _) = listener.accept().unwrap();
    validate_socket(&path, &accepted, 8, Duration::from_millis(1)).unwrap();
    drop(client);
    drop(accepted);
    drop(listener);
    fs::remove_dir_all(dir).unwrap();
}
fn crash_probe(root: &Path) {
    let store = root.join("state.db");
    let source = root.join("source.lock");
    let ready = root.join("ready");
    let exe = std::env::current_exe().unwrap();
    let mut child = std::process::Command::new(exe)
        .args([
            "hold",
            store.to_str().unwrap(),
            source.to_str().unwrap(),
            ready.to_str().unwrap(),
        ])
        .spawn()
        .unwrap();
    for _ in 0..300 {
        if ready.exists() {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(ready.exists());
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    assert!(
        fs::read_to_string(store.with_extension("ownership.lock"))
            .unwrap()
            .contains("clean_release=pending")
    );
    let mut successor = OwnershipGuard::acquire(
        &store,
        "successor".into(),
        OwnerKind::Runtime,
        Duration::from_secs(2),
        FileSession::open(&source, "successor"),
    )
    .unwrap();
    assert!(successor.backend_pid() > 0);
    assert_eq!(
        successor.admit_source_mutation(Duration::ZERO),
        Err(OwnershipError::ReconciliationRequired)
    );
    successor.reconcile_after_unclean_release(|| true).unwrap();
    successor.admit_source_mutation(Duration::ZERO).unwrap();
    drop(successor);
    assert_eq!(
        fs::read_to_string(store.with_extension("ownership.lock")).unwrap(),
        "clean_release=proved\n"
    );
}
fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).map(String::as_str) == Some("hold") {
        let store = PathBuf::from(&args[2]);
        let source = PathBuf::from(&args[3]);
        let ready = PathBuf::from(&args[4]);
        let _guard = OwnershipGuard::acquire(
            &store,
            "crashed-owner".into(),
            OwnerKind::Runtime,
            Duration::from_secs(60),
            FileSession::open(&source, "crashed"),
        )
        .unwrap();
        fs::write(ready, b"ready").unwrap();
        loop {
            std::thread::park();
        }
    }
    let root = PathBuf::from(args.get(1).expect("probe root"));
    fs::create_dir_all(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    socket_probe(&root);
    crash_probe(&root);
    if let Some(container) = args.get(2) {
        postgres_guard_probe(&root, container);
    }
    println!("production_ownership_component=pass");
}
