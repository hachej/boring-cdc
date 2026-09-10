use boring_cdc::m2_ownership::{
    OwnerKind, OwnershipError, OwnershipGuard, SourceLockSession, validate_socket,
};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
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
    let successor = OwnershipGuard::acquire(
        &store,
        "successor".into(),
        OwnerKind::Runtime,
        Duration::from_secs(2),
        FileSession::open(&source, "successor"),
    )
    .unwrap();
    assert!(successor.backend_pid() > 0);
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
    println!("production_ownership_component=pass");
}
