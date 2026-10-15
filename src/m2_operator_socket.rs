//! Bounded local transport for the capture owner's operator commands.

use crate::m1_cli_contract::{
    OperatorWireRequest, OperatorWireResponse, encode_operator_response_frame,
    parse_operator_wire_request,
};
use crate::m1_config::OperatorEndpoint;
use std::fs;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const ACCEPT_POLL: Duration = Duration::from_millis(25);

pub struct OperatorSocket {
    path: PathBuf,
    device: u64,
    inode: u64,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl OperatorSocket {
    pub fn start(
        config: &OperatorEndpoint,
        dispatch: impl Fn(OperatorWireRequest) -> OperatorWireResponse + Send + Sync + 'static,
    ) -> Result<Self, &'static str> {
        let root = std::env::current_dir().map_err(|_| "M2_OPERATOR_PATH_UNSAFE")?;
        Self::start_at(&root, config, dispatch)
    }

    pub fn start_at(
        root: &Path,
        config: &OperatorEndpoint,
        dispatch: impl Fn(OperatorWireRequest) -> OperatorWireResponse + Send + Sync + 'static,
    ) -> Result<Self, &'static str> {
        if config.directory_mode != 0o700
            || config.socket_mode != 0o600
            || !config.peer_credentials
            || config.max_request_bytes.0 == 0
            || config.max_response_bytes.0 == 0
            || config.read_timeout_ms.0 == 0
            || config.write_timeout_ms.0 == 0
        {
            return Err("M2_OPERATOR_CONFIG_INVALID");
        }
        let response_limit = usize::try_from(config.max_response_bytes.0)
            .map_err(|_| "M2_OPERATOR_CONFIG_INVALID")?;
        let request_limit = usize::try_from(config.max_request_bytes.0)
            .map_err(|_| "M2_OPERATOR_CONFIG_INVALID")?;
        for (code, message) in [
            ("CLI_OPERATOR_FRAME_INVALID", "operator frame is invalid"),
            (
                "CLI_OPERATOR_FRAME_TOO_LARGE",
                "operator response is too large",
            ),
            ("CLI_OPERATOR_PEER_DENIED", "operator peer is denied"),
        ] {
            encode_operator_response_frame(&response_error(code, message), response_limit)
                .map_err(|_| "M2_OPERATOR_CONFIG_INVALID")?;
        }
        let path = prepare_path(root, Path::new(&config.socket_path))?;
        let listener = UnixListener::bind(&path).map_err(|_| "M2_OPERATOR_BIND_FAILED")?;
        if fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).is_err() {
            let _ = fs::remove_file(&path);
            return Err("M2_OPERATOR_BIND_FAILED");
        }
        let metadata = fs::symlink_metadata(&path).map_err(|_| "M2_OPERATOR_BIND_FAILED")?;
        if !metadata.file_type().is_socket()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o777 != 0o600
        {
            let _ = fs::remove_file(&path);
            return Err("M2_OPERATOR_BIND_FAILED");
        }
        if listener.set_nonblocking(true).is_err() {
            let _ = fs::remove_file(&path);
            return Err("M2_OPERATOR_BIND_FAILED");
        }
        let stop = Arc::new(AtomicBool::new(false));
        let requested_stop = Arc::clone(&stop);
        let dispatch = Arc::new(dispatch);
        let read_timeout = Duration::from_millis(config.read_timeout_ms.0);
        let write_timeout = Duration::from_millis(config.write_timeout_ms.0);
        let worker = thread::Builder::new()
            .name("m2-operator-socket".into())
            .spawn(move || {
                while !requested_stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((mut stream, _)) => handle(
                            &mut stream,
                            request_limit,
                            response_limit,
                            read_timeout,
                            write_timeout,
                            dispatch.as_ref(),
                        ),
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(ACCEPT_POLL);
                        }
                        Err(_) => break,
                    }
                }
            });
        let worker = match worker {
            Ok(worker) => worker,
            Err(_) => {
                let _ = fs::remove_file(&path);
                return Err("M2_OPERATOR_LISTENER_START_FAILED");
            }
        };
        Ok(Self {
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
            stop,
            thread: Some(worker),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for OperatorSocket {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.thread.take() {
            let _ = worker.join();
        }
        if let Ok(metadata) = fs::symlink_metadata(&self.path)
            && metadata.file_type().is_socket()
            && metadata.dev() == self.device
            && metadata.ino() == self.inode
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn prepare_path(root: &Path, path: &Path) -> Result<PathBuf, &'static str> {
    let parts = path.components().collect::<Vec<_>>();
    if path.is_absolute()
        || parts.len() < 3
        || parts
            .iter()
            .any(|part| !matches!(part, Component::Normal(_)))
        || path.extension().and_then(|part| part.to_str()) != Some("sock")
    {
        return Err("M2_OPERATOR_PATH_UNSAFE");
    }
    let owner = unsafe { libc::geteuid() };
    if !root.is_absolute() {
        return Err("M2_OPERATOR_PATH_UNSAFE");
    }
    for ancestor in root.ancestors() {
        let metadata = fs::symlink_metadata(ancestor).map_err(|_| "M2_OPERATOR_PATH_UNSAFE")?;
        let writable = metadata.mode() & 0o022 != 0;
        let protected_sticky = metadata.uid() == 0 && metadata.mode() & 0o1000 != 0;
        if !metadata.is_dir()
            || metadata.file_type().is_symlink()
            || (metadata.uid() != 0 && metadata.uid() != owner)
            || (writable && !protected_sticky)
            || (ancestor == root && metadata.uid() != owner)
        {
            return Err("M2_OPERATOR_PATH_UNSAFE");
        }
    }
    let mut dir = root.to_path_buf();
    for (index, part) in parts[..parts.len() - 1].iter().enumerate() {
        dir.push(part.as_os_str());
        if !dir.exists() {
            fs::create_dir(&dir).map_err(|_| "M2_OPERATOR_PATH_UNSAFE")?;
            fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))
                .map_err(|_| "M2_OPERATOR_PATH_UNSAFE")?;
        }
        let metadata = fs::symlink_metadata(&dir).map_err(|_| "M2_OPERATOR_PATH_UNSAFE")?;
        let is_parent = index == parts.len() - 2;
        if !metadata.is_dir()
            || metadata.file_type().is_symlink()
            || metadata.uid() != owner
            || metadata.mode() & 0o022 != 0
            || (is_parent && metadata.mode() & 0o777 != 0o700)
        {
            return Err("M2_OPERATOR_PATH_UNSAFE");
        }
    }
    let full = root.join(path);
    if let Ok(metadata) = fs::symlink_metadata(&full) {
        if !metadata.file_type().is_socket()
            || metadata.uid() != owner
            || metadata.mode() & 0o777 != 0o600
        {
            return Err("M2_OPERATOR_PATH_UNSAFE");
        }
        match UnixStream::connect(&full) {
            Ok(_) => return Err("M2_OPERATOR_ALREADY_ACTIVE"),
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                fs::remove_file(&full).map_err(|_| "M2_OPERATOR_PATH_UNSAFE")?;
            }
            Err(_) => return Err("M2_OPERATOR_PATH_UNSAFE"),
        }
    }
    Ok(full)
}

fn response_error(code: &str, message: &str) -> OperatorWireResponse {
    OperatorWireResponse::Error {
        schema_version: 1,
        code: code.into(),
        message: message.into(),
    }
}

fn handle(
    stream: &mut UnixStream,
    request_limit: usize,
    response_limit: usize,
    read_timeout: Duration,
    write_timeout: Duration,
    dispatch: &impl Fn(OperatorWireRequest) -> OperatorWireResponse,
) {
    let response = if peer_uid(stream) != Some(unsafe { libc::geteuid() }) {
        response_error("CLI_OPERATOR_PEER_DENIED", "operator peer is denied")
    } else {
        match read_request(stream, request_limit, read_timeout) {
            Ok(request) => dispatch(request),
            Err((code, message)) => response_error(code, message),
        }
    };
    let framed = encode_operator_response_frame(&response, response_limit).or_else(|_| {
        encode_operator_response_frame(
            &response_error(
                "CLI_OPERATOR_FRAME_TOO_LARGE",
                "operator response is too large",
            ),
            response_limit,
        )
    });
    if let Ok(frame) = framed {
        if let Some(deadline) = Instant::now().checked_add(write_timeout) {
            let _ = write_before(stream, &frame, deadline);
        }
        let _ = stream.flush();
    }
}

#[cfg(target_os = "linux")]
fn peer_uid(stream: &UnixStream) -> Option<u32> {
    let mut credentials = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut length = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    let status = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&raw mut credentials).cast(),
            &raw mut length,
        )
    };
    (status == 0 && length as usize == std::mem::size_of::<libc::ucred>())
        .then_some(credentials.uid)
}

#[cfg(not(target_os = "linux"))]
fn peer_uid(_: &UnixStream) -> Option<u32> {
    None
}

fn read_request(
    stream: &mut UnixStream,
    limit: usize,
    timeout: Duration,
) -> Result<OperatorWireRequest, (&'static str, &'static str)> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(("CLI_OPERATOR_FRAME_INVALID", "operator frame is invalid"))?;
    let mut prefix = [0u8; 4];
    read_exact_before(stream, &mut prefix, deadline)?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length > limit {
        return Err((
            "CLI_OPERATOR_FRAME_TOO_LARGE",
            "operator request is too large",
        ));
    }
    if length == 0 {
        return Err(("CLI_OPERATOR_FRAME_INVALID", "operator frame is invalid"));
    }
    let mut body = vec![0u8; length];
    read_exact_before(stream, &mut body, deadline)?;
    let mut extra = [0u8; 1];
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(("CLI_OPERATOR_FRAME_INVALID", "operator frame is invalid"))?;
        stream
            .set_read_timeout(Some(remaining))
            .map_err(|_| ("CLI_OPERATOR_FRAME_INVALID", "operator frame is invalid"))?;
        match stream.read(&mut extra) {
            Ok(0) => break,
            Ok(_) => return Err(("CLI_OPERATOR_FRAME_INVALID", "operator frame is invalid")),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(("CLI_OPERATOR_FRAME_INVALID", "operator frame is invalid")),
        }
    }
    parse_operator_wire_request(&body, limit).map_err(|error| (error.code, error.message))
}

fn read_exact_before(
    stream: &mut UnixStream,
    mut bytes: &mut [u8],
    deadline: Instant,
) -> Result<(), (&'static str, &'static str)> {
    while !bytes.is_empty() {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(("CLI_OPERATOR_FRAME_INVALID", "operator frame is invalid"))?;
        stream
            .set_read_timeout(Some(remaining))
            .map_err(|_| ("CLI_OPERATOR_FRAME_INVALID", "operator frame is invalid"))?;
        match stream.read(bytes) {
            Ok(0) => return Err(("CLI_OPERATOR_FRAME_INVALID", "operator frame is invalid")),
            Ok(count) => bytes = &mut bytes[count..],
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(("CLI_OPERATOR_FRAME_INVALID", "operator frame is invalid")),
        }
    }
    Ok(())
}

fn write_before(
    stream: &mut UnixStream,
    mut bytes: &[u8],
    deadline: Instant,
) -> std::io::Result<()> {
    while !bytes.is_empty() {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(std::io::ErrorKind::TimedOut)?;
        stream.set_write_timeout(Some(remaining))?;
        match stream.write(bytes) {
            Ok(0) => return Err(std::io::ErrorKind::WriteZero.into()),
            Ok(count) => bytes = &bytes[count..],
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m1_cli_contract::encode_operator_request_frame;
    use crate::m1_config::{Bytes, Milliseconds};
    use std::net::Shutdown;
    use std::sync::atomic::AtomicUsize;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestPath {
        root: PathBuf,
        relative: PathBuf,
    }
    impl TestPath {
        fn new() -> Self {
            let suffix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let root = std::env::temp_dir()
                .join(format!("m2-operator-tests-{}-{suffix}", std::process::id()));
            fs::create_dir(&root).unwrap();
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
            Self {
                root,
                relative: PathBuf::from("run/owner/operator.sock"),
            }
        }
        fn config(&self) -> OperatorEndpoint {
            OperatorEndpoint {
                socket_path: self.relative.to_str().unwrap().into(),
                max_request_bytes: Bytes(512),
                max_response_bytes: Bytes(512),
                read_timeout_ms: Milliseconds(200),
                write_timeout_ms: Milliseconds(200),
                ..Default::default()
            }
        }
    }
    impl Drop for TestPath {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn read_response(stream: &mut UnixStream) -> serde_json::Value {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut prefix = [0u8; 4];
        stream.read_exact(&mut prefix).unwrap();
        let mut bytes = vec![0u8; u32::from_be_bytes(prefix) as usize];
        stream.read_exact(&mut bytes).unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn complete_eof_frame_dispatches_once_and_trailing_bytes_never_dispatch() {
        let path = TestPath::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let server = OperatorSocket::start_at(&path.root, &path.config(), move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            response_error("TEST_ROUTED", "test request routed")
        })
        .unwrap();
        let request = OperatorWireRequest::Result {
            schema_version: 1,
            expected_run_id: "run".into(),
            request_id: "request".into(),
            canonical_payload_digest: "payload".into(),
        };
        let frame = encode_operator_request_frame(&request, 512).unwrap();
        let mut valid = UnixStream::connect(server.path()).unwrap();
        valid.write_all(&frame).unwrap();
        valid.shutdown(Shutdown::Write).unwrap();
        assert_eq!(read_response(&mut valid)["code"], "TEST_ROUTED");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let mut extra = UnixStream::connect(server.path()).unwrap();
        extra.write_all(&frame).unwrap();
        extra.write_all(b"late").unwrap();
        extra.shutdown(Shutdown::Write).unwrap();
        assert_eq!(
            read_response(&mut extra)["code"],
            "CLI_OPERATOR_FRAME_INVALID"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let mut no_eof = UnixStream::connect(server.path()).unwrap();
        no_eof.write_all(&frame).unwrap();
        assert_eq!(
            read_response(&mut no_eof)["code"],
            "CLI_OPERATOR_FRAME_INVALID"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(server);
        assert!(!path.root.join(&path.relative).exists());
    }

    #[test]
    fn oversized_prefix_and_unsafe_path_fail_before_dispatch() {
        let path = TestPath::new();
        let unsafe_parent = path.root.join("unsafe");
        let nested_root = unsafe_parent.join("safe");
        fs::create_dir_all(&nested_root).unwrap();
        fs::set_permissions(&unsafe_parent, fs::Permissions::from_mode(0o777)).unwrap();
        fs::set_permissions(&nested_root, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(matches!(
            OperatorSocket::start_at(&nested_root, &path.config(), |_| response_error("X", "Y")),
            Err("M2_OPERATOR_PATH_UNSAFE")
        ));
        let mut short_response = path.config();
        short_response.max_response_bytes = Bytes(1);
        assert!(matches!(
            OperatorSocket::start_at(&path.root, &short_response, |_| response_error("X", "Y")),
            Err("M2_OPERATOR_CONFIG_INVALID")
        ));
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let server = OperatorSocket::start_at(&path.root, &path.config(), move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            response_error("TEST_ROUTED", "test request routed")
        })
        .unwrap();
        let metadata = fs::symlink_metadata(server.path()).unwrap();
        assert_eq!(metadata.mode() & 0o777, 0o600);
        assert_eq!(
            fs::symlink_metadata(server.path().parent().unwrap())
                .unwrap()
                .mode()
                & 0o777,
            0o700
        );
        let mut oversized = UnixStream::connect(server.path()).unwrap();
        oversized.write_all(&513u32.to_be_bytes()).unwrap();
        oversized.shutdown(Shutdown::Write).unwrap();
        assert_eq!(
            read_response(&mut oversized)["code"],
            "CLI_OPERATOR_FRAME_TOO_LARGE"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(matches!(
            OperatorSocket::start_at(&path.root, &path.config(), |_| response_error("X", "Y")),
            Err("M2_OPERATOR_ALREADY_ACTIVE")
        ));
        drop(server);

        let full = path.root.join(&path.relative);
        let stale = UnixListener::bind(&full).unwrap();
        fs::set_permissions(&full, fs::Permissions::from_mode(0o600)).unwrap();
        drop(stale);
        let restarted = OperatorSocket::start_at(&path.root, &path.config(), |_| {
            response_error("TEST_ROUTED", "test request routed")
        })
        .unwrap();
        drop(restarted);
        let parent = full.parent().unwrap();
        fs::create_dir_all(parent).unwrap();
        std::os::unix::fs::symlink("/dev/null", &full).unwrap();
        assert!(matches!(
            OperatorSocket::start_at(&path.root, &path.config(), |_| response_error("X", "Y")),
            Err("M2_OPERATOR_PATH_UNSAFE")
        ));
    }
}
