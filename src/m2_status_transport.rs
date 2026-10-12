//! Bounded read-only status listeners. TLS material is parsed before either socket binds.

use crate::m1_config::LoadedConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig, ServerConnection, StreamOwned};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const MAX_REQUEST_BYTES: usize = 1_048_576;
const MAX_RESPONSE_BYTES: usize = 4_194_304;
const MAX_CONNECTIONS: usize = 16;
const IO_TIMEOUT: Duration = Duration::from_secs(10);

pub type StatusHandler = Arc<dyn Fn() -> Result<Vec<u8>, ()> + Send + Sync>;

pub struct StatusListeners {
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
}

impl Drop for StatusListeners {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

pub fn start(
    config: &LoadedConfig,
    status: StatusHandler,
) -> Result<StatusListeners, &'static str> {
    let observability = &config.public().observability;
    let tls = if observability.tls {
        let (cert, key, ca) = config.status_tls_material().ok_or(TLS_MATERIAL_INVALID)?;
        Some(server_config(cert, key, ca)?)
    } else {
        None
    };
    let status_addr: SocketAddr = observability
        .status_listen_addr
        .parse()
        .map_err(|_| "M2_STATUS_BIND_INVALID")?;
    let metrics_addr: SocketAddr = observability
        .prometheus_listen_addr
        .parse()
        .map_err(|_| "M2_STATUS_BIND_INVALID")?;
    if tls.is_none() && (!status_addr.ip().is_loopback() || !metrics_addr.ip().is_loopback()) {
        return Err("M2_STATUS_PUBLIC_PLAINTEXT_FORBIDDEN");
    }
    let status_listener = TcpListener::bind(status_addr).map_err(|_| "M2_STATUS_BIND_FAILED")?;
    let metrics_listener = TcpListener::bind(metrics_addr).map_err(|_| "M2_STATUS_BIND_FAILED")?;
    status_listener
        .set_nonblocking(true)
        .map_err(|_| "M2_STATUS_BIND_FAILED")?;
    metrics_listener
        .set_nonblocking(true)
        .map_err(|_| "M2_STATUS_BIND_FAILED")?;
    let stop = Arc::new(AtomicBool::new(false));
    let active = Arc::new(AtomicUsize::new(0));
    let threads = [(status_listener, "/status"), (metrics_listener, "/metrics")]
        .into_iter()
        .map(|(listener, route)| {
            let stop = stop.clone();
            let active = active.clone();
            let tls = tls.clone();
            let status = status.clone();
            thread::spawn(move || accept_loop(listener, route, tls, status, stop, active))
        })
        .collect();
    Ok(StatusListeners { stop, threads })
}

fn accept_loop(
    listener: TcpListener,
    route: &'static str,
    tls: Option<Arc<ServerConfig>>,
    status: StatusHandler,
    stop: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
) {
    while !stop.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _)) => {
                if active.fetch_add(1, Ordering::AcqRel) >= MAX_CONNECTIONS {
                    active.fetch_sub(1, Ordering::AcqRel);
                    continue;
                }
                let active = active.clone();
                let tls = tls.clone();
                let status = status.clone();
                thread::spawn(move || {
                    let _ = handle(stream, route, tls, status);
                    active.fetch_sub(1, Ordering::AcqRel);
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(20));
            }
            Err(_) => break,
        }
    }
}

fn handle(
    stream: TcpStream,
    route: &str,
    tls: Option<Arc<ServerConfig>>,
    status: StatusHandler,
) -> Result<(), ()> {
    stream.set_read_timeout(Some(IO_TIMEOUT)).map_err(|_| ())?;
    stream.set_write_timeout(Some(IO_TIMEOUT)).map_err(|_| ())?;
    match tls {
        Some(config) => {
            let connection = ServerConnection::new(config).map_err(|_| ())?;
            let mut secure = StreamOwned::new(connection, stream);
            respond(&mut secure, route, status)
        }
        None => {
            let mut plain = stream;
            respond(&mut plain, route, status)
        }
    }
}

fn respond(stream: &mut impl ReadWrite, route: &str, status: StatusHandler) -> Result<(), ()> {
    let mut request = Vec::new();
    let mut block = [0u8; 4096];
    while !request.ends_with(b"\r\n\r\n") {
        let count = stream.read(&mut block).map_err(|_| ())?;
        if count == 0 || request.len() + count > MAX_REQUEST_BYTES {
            return Err(());
        }
        request.extend_from_slice(&block[..count]);
        if request.windows(4).any(|part| part == b"\r\n\r\n") {
            break;
        }
    }
    let first = request.split(|byte| *byte == b'\r').next().ok_or(())?;
    let allowed = first == format!("GET {route} HTTP/1.1").as_bytes()
        || first == format!("GET {route} HTTP/1.0").as_bytes();
    let (code, kind, body) = if allowed && route == "/status" {
        match status() {
            Ok(bytes) if bytes.len() <= MAX_RESPONSE_BYTES => ("200 OK", "application/json", bytes),
            _ => (
                "503 Service Unavailable",
                "text/plain",
                b"status unavailable\n".to_vec(),
            ),
        }
    } else if allowed && route == "/metrics" {
        (
            "200 OK",
            "text/plain; version=0.0.4",
            b"boring_cdc_status_listener_up 1\n".to_vec(),
        )
    } else {
        ("404 Not Found", "text/plain", b"not found\n".to_vec())
    };
    let header = format!(
        "HTTP/1.1 {code}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).map_err(|_| ())?;
    stream.write_all(&body).map_err(|_| ())?;
    stream.flush().map_err(|_| ())
}

trait ReadWrite: Read + Write {}
impl<T: Read + Write> ReadWrite for T {}

/// Fixed, redacted failure code for all malformed status TLS material.
pub const TLS_MATERIAL_INVALID: &str = "M2_STATUS_TLS_MATERIAL_INVALID";

pub fn server_config(
    cert_pem: &str,
    key_pem: &str,
    ca_pem: &str,
) -> Result<Arc<ServerConfig>, &'static str> {
    let certs: Vec<_> = CertificateDer::pem_slice_iter(cert_pem.as_bytes())
        .collect::<Result<_, _>>()
        .map_err(|_| TLS_MATERIAL_INVALID)?;
    if certs.is_empty() {
        return Err(TLS_MATERIAL_INVALID);
    }
    let key =
        PrivateKeyDer::from_pem_slice(key_pem.as_bytes()).map_err(|_| TLS_MATERIAL_INVALID)?;
    let roots: Vec<_> = CertificateDer::pem_slice_iter(ca_pem.as_bytes())
        .collect::<Result<_, _>>()
        .map_err(|_| TLS_MATERIAL_INVALID)?;
    if roots.is_empty() {
        return Err(TLS_MATERIAL_INVALID);
    }
    let mut trust = RootCertStore::empty();
    for root in roots {
        trust.add(root).map_err(|_| TLS_MATERIAL_INVALID)?;
    }
    let verifier = WebPkiClientVerifier::builder(Arc::new(trust))
        .build()
        .map_err(|_| TLS_MATERIAL_INVALID)?;
    let config = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .map_err(|_| TLS_MATERIAL_INVALID)?;
    Ok(Arc::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m1_config::{Environment, LoadPurpose, load_str_for};
    use rustls::pki_types::ServerName;
    use rustls::{ClientConfig, ClientConnection};
    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::process::Command;
    use std::sync::atomic::AtomicU64;

    static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

    struct TestTlsMaterial {
        cert: String,
        key: String,
        ca: String,
        client_cert: String,
        client_key: String,
        other_key: String,
        dir: PathBuf,
    }

    impl Drop for TestTlsMaterial {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn test_tls_material() -> TestTlsMaterial {
        use std::os::unix::fs::DirBuilderExt;
        let dir = std::env::temp_dir().join(format!(
            "boring-cdc-m2-tls-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
        fn openssl(dir: &PathBuf, args: &[&str]) {
            let result = Command::new("openssl")
                .args(args)
                .current_dir(dir)
                .output()
                .expect("OpenSSL is required for the TLS boundary test");
            assert!(
                result.status.success(),
                "OpenSSL test fixture generation failed"
            );
        }
        openssl(
            &dir,
            &[
                "ecparam",
                "-name",
                "prime256v1",
                "-genkey",
                "-noout",
                "-out",
                "ca.key",
            ],
        );
        openssl(
            &dir,
            &[
                "req",
                "-new",
                "-x509",
                "-key",
                "ca.key",
                "-out",
                "ca.crt",
                "-days",
                "2",
                "-subj",
                "/CN=Boring CDC test CA",
            ],
        );
        openssl(
            &dir,
            &[
                "ecparam",
                "-name",
                "prime256v1",
                "-genkey",
                "-noout",
                "-out",
                "server-ec.key",
            ],
        );
        openssl(
            &dir,
            &["pkey", "-in", "server-ec.key", "-out", "server.key"],
        );
        openssl(
            &dir,
            &[
                "req",
                "-new",
                "-key",
                "server.key",
                "-out",
                "server.csr",
                "-subj",
                "/CN=localhost",
            ],
        );
        std::fs::write(
            dir.join("server.ext"),
            "subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n",
        )
        .unwrap();
        openssl(
            &dir,
            &[
                "x509",
                "-req",
                "-in",
                "server.csr",
                "-CA",
                "ca.crt",
                "-CAkey",
                "ca.key",
                "-CAcreateserial",
                "-out",
                "server.crt",
                "-days",
                "2",
                "-extfile",
                "server.ext",
            ],
        );
        openssl(
            &dir,
            &[
                "ecparam",
                "-name",
                "prime256v1",
                "-genkey",
                "-noout",
                "-out",
                "client.key",
            ],
        );
        openssl(
            &dir,
            &[
                "req",
                "-new",
                "-key",
                "client.key",
                "-out",
                "client.csr",
                "-subj",
                "/CN=Boring CDC test client",
            ],
        );
        std::fs::write(dir.join("client.ext"), "extendedKeyUsage=clientAuth\n").unwrap();
        openssl(
            &dir,
            &[
                "x509",
                "-req",
                "-in",
                "client.csr",
                "-CA",
                "ca.crt",
                "-CAkey",
                "ca.key",
                "-CAcreateserial",
                "-out",
                "client.crt",
                "-days",
                "2",
                "-extfile",
                "client.ext",
            ],
        );
        openssl(
            &dir,
            &[
                "ecparam",
                "-name",
                "prime256v1",
                "-genkey",
                "-noout",
                "-out",
                "other-ec.key",
            ],
        );
        openssl(&dir, &["pkey", "-in", "other-ec.key", "-out", "other.key"]);
        let read = |name: &str| std::fs::read_to_string(dir.join(name)).unwrap();
        TestTlsMaterial {
            cert: read("server.crt"),
            key: read("server.key"),
            ca: read("ca.crt"),
            client_cert: read("client.crt"),
            client_key: read("client.key"),
            other_key: read("other.key"),
            dir,
        }
    }

    #[test]
    fn malformed_material_is_rejected_with_one_redacted_code() {
        for (cert, key, ca) in [
            ("", "", ""),
            (
                "private certificate text",
                "private key text",
                "private ca text",
            ),
            (
                "-----BEGIN CERTIFICATE-----\nnot-base64\n-----END CERTIFICATE-----",
                "",
                "",
            ),
        ] {
            assert_eq!(
                server_config(cert, key, ca).err(),
                Some(TLS_MATERIAL_INVALID)
            );
        }
        let material = test_tls_material();
        assert!(server_config(&material.cert, &material.key, &material.ca).is_ok());
        assert_eq!(
            server_config(&material.cert, &material.other_key, &material.ca).err(),
            Some(TLS_MATERIAL_INVALID)
        );

        struct Env(BTreeMap<String, String>);
        impl Environment for Env {
            fn get(&self, name: &str) -> Option<String> {
                self.0.get(name).cloned()
            }
            fn names(&self) -> Vec<String> {
                self.0.keys().cloned().collect()
            }
        }
        let fixture = include_str!("../tests/fixtures/m1_config/representative.toml")
            .replace("authentication = false", "authentication = true")
            .replace("tls = false", "tls = true");
        let env = Env(BTreeMap::from([
            (
                "PG_RUNTIME".into(),
                concat!("postgres://runtime:", "secret@source/db").into(),
            ),
            (
                "PG_CONTROL".into(),
                concat!("postgres://control:", "secret@source/db").into(),
            ),
            (
                "CH_RUNTIME".into(),
                "https://runtime:secret@clickhouse".into(),
            ),
            ("STATUS_TLS_CERT".into(), material.cert.clone()),
            ("STATUS_TLS_KEY".into(), material.key.clone()),
            ("STATUS_TLS_CLIENT_CA".into(), material.ca.clone()),
        ]));
        let loaded = load_str_for(&fixture, &env, LoadPurpose::Run).unwrap();
        let (cert, key, ca) = loaded.status_tls_material().unwrap();
        assert!(server_config(cert, key, ca).is_ok());
    }

    #[test]
    fn only_read_only_routes_return_data() {
        struct Memory {
            input: Cursor<Vec<u8>>,
            output: Vec<u8>,
        }
        impl Read for Memory {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
                self.input.read(out)
            }
        }
        impl Write for Memory {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.output.extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        fn request(method: &str, path: &str, route: &str) -> String {
            let input = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\n\r\n");
            let mut memory = Memory {
                input: Cursor::new(input.into_bytes()),
                output: Vec::new(),
            };
            respond(
                &mut memory,
                route,
                Arc::new(|| Ok(b"{\"ok\":true}".to_vec())),
            )
            .unwrap();
            String::from_utf8(memory.output).unwrap()
        }
        assert!(request("GET", "/status", "/status").contains("{\"ok\":true}"));
        assert!(request("GET", "/metrics", "/metrics").contains("boring_cdc_status_listener_up 1"));
        assert!(request("POST", "/status", "/status").starts_with("HTTP/1.1 404"));
        assert!(request("GET", "/status", "/metrics").starts_with("HTTP/1.1 404"));
    }

    #[test]
    fn tls_requires_a_trusted_client_certificate() {
        let material = test_tls_material();
        let server_config = server_config(&material.cert, &material.key, &material.ca).unwrap();
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(material.ca.as_bytes()).unwrap())
            .unwrap();
        let roots = Arc::new(roots);
        let authorized = ClientConfig::builder()
            .with_root_certificates(roots.clone())
            .with_client_auth_cert(
                vec![CertificateDer::from_pem_slice(material.client_cert.as_bytes()).unwrap()],
                PrivateKeyDer::from_pem_slice(material.client_key.as_bytes()).unwrap(),
            )
            .unwrap();
        let anonymous = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();

        fn handshake(server_config: Arc<ServerConfig>, client_config: ClientConfig) -> bool {
            let mut server = ServerConnection::new(server_config).unwrap();
            let mut client = ClientConnection::new(
                Arc::new(client_config),
                ServerName::try_from("localhost").unwrap(),
            )
            .unwrap();
            for _ in 0..8 {
                let mut outbound = Vec::new();
                client.write_tls(&mut outbound).unwrap();
                server.read_tls(&mut Cursor::new(outbound)).unwrap();
                if server.process_new_packets().is_err() {
                    return false;
                }
                let mut outbound = Vec::new();
                server.write_tls(&mut outbound).unwrap();
                client.read_tls(&mut Cursor::new(outbound)).unwrap();
                if client.process_new_packets().is_err() {
                    return false;
                }
                if !server.is_handshaking() && !client.is_handshaking() {
                    return true;
                }
            }
            false
        }
        assert!(handshake(server_config.clone(), authorized));
        assert!(!handshake(server_config, anonymous));
    }

    #[test]
    fn loopback_listeners_serve_read_only_routes_and_release_ports() {
        struct EmptyEnv;
        impl Environment for EmptyEnv {
            fn get(&self, _: &str) -> Option<String> {
                None
            }
        }
        fn get(port: u16, path: &str, method: &str) -> String {
            let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            write!(
                stream,
                "{method} {path} HTTP/1.1\r\nHost: localhost\r\n\r\n"
            )
            .unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            response
        }
        let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
        let status_port = reservation.local_addr().unwrap().port();
        let metrics_port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        drop(reservation);
        let fixture = include_str!("../tests/fixtures/m1_config/representative.toml")
            .replace("127.0.0.1:8787", &format!("127.0.0.1:{status_port}"))
            .replace("127.0.0.1:8788", &format!("127.0.0.1:{metrics_port}"));
        let config = load_str_for(&fixture, &EmptyEnv, LoadPurpose::Status).unwrap();
        let handler: StatusHandler = Arc::new(|| Ok(b"{\"ok\":true}".to_vec()));
        for _ in 0..2 {
            let listeners = start(&config, handler.clone()).unwrap();
            assert!(get(status_port, "/status", "GET").contains("{\"ok\":true}"));
            assert!(
                get(metrics_port, "/metrics", "GET").contains("boring_cdc_status_listener_up 1")
            );
            assert!(get(status_port, "/status", "POST").starts_with("HTTP/1.1 404"));
            assert!(get(metrics_port, "/status", "GET").starts_with("HTTP/1.1 404"));
            drop(listeners);
        }
    }

    #[test]
    fn live_tls_listener_requires_a_client_certificate() {
        struct Env(BTreeMap<String, String>);
        impl Environment for Env {
            fn get(&self, name: &str) -> Option<String> {
                self.0.get(name).cloned()
            }
            fn names(&self) -> Vec<String> {
                self.0.keys().cloned().collect()
            }
        }
        let material = test_tls_material();
        let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
        let status_port = reservation.local_addr().unwrap().port();
        let metrics_port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        drop(reservation);
        let fixture = include_str!("../tests/fixtures/m1_config/representative.toml")
            .replace("127.0.0.1:8787", &format!("127.0.0.1:{status_port}"))
            .replace("127.0.0.1:8788", &format!("127.0.0.1:{metrics_port}"))
            .replace("authentication = false", "authentication = true")
            .replace("tls = false", "tls = true");
        let env = Env(BTreeMap::from([
            (
                "PG_RUNTIME".into(),
                concat!("postgres://runtime:", "secret@source/db").into(),
            ),
            (
                "PG_CONTROL".into(),
                concat!("postgres://control:", "secret@source/db").into(),
            ),
            (
                "CH_RUNTIME".into(),
                "https://runtime:secret@clickhouse".into(),
            ),
            ("STATUS_TLS_CERT".into(), material.cert.clone()),
            ("STATUS_TLS_KEY".into(), material.key.clone()),
            ("STATUS_TLS_CLIENT_CA".into(), material.ca.clone()),
        ]));
        let config = load_str_for(&fixture, &env, LoadPurpose::Run).unwrap();
        let _listeners = start(&config, Arc::new(|| Ok(b"{\"ok\":true}".to_vec()))).unwrap();
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(material.ca.as_bytes()).unwrap())
            .unwrap();
        let roots = Arc::new(roots);
        let authorized = ClientConfig::builder()
            .with_root_certificates(roots.clone())
            .with_client_auth_cert(
                vec![CertificateDer::from_pem_slice(material.client_cert.as_bytes()).unwrap()],
                PrivateKeyDer::from_pem_slice(material.client_key.as_bytes()).unwrap(),
            )
            .unwrap();
        let anonymous = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        fn fetch(port: u16, config: ClientConfig) -> std::io::Result<String> {
            let stream = TcpStream::connect(("127.0.0.1", port))?;
            stream.set_read_timeout(Some(Duration::from_secs(2)))?;
            stream.set_write_timeout(Some(Duration::from_secs(2)))?;
            let connection =
                ClientConnection::new(Arc::new(config), ServerName::try_from("localhost").unwrap())
                    .unwrap();
            let mut stream = StreamOwned::new(connection, stream);
            stream.write_all(b"GET /status HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
            let mut response = String::new();
            let _ = stream.read_to_string(&mut response);
            Ok(response)
        }
        assert!(
            fetch(status_port, authorized)
                .unwrap()
                .contains("{\"ok\":true}")
        );
        assert!(
            !fetch(status_port, anonymous)
                .unwrap_or_default()
                .contains("{\"ok\":true}")
        );
    }
}
