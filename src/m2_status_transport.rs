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
    use rustls::pki_types::ServerName;
    use rustls::{ClientConfig, ClientConnection};
    use std::io::Cursor;

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
        let cert = include_str!("../tests/fixtures/m2_status/server.crt");
        let key = include_str!("../tests/fixtures/m2_status/server.key");
        let ca = include_str!("../tests/fixtures/m2_status/ca.crt");
        assert!(server_config(cert, key, ca).is_ok());
        assert_eq!(
            server_config(
                cert,
                include_str!("../tests/fixtures/m2_status/other.key"),
                ca
            )
            .err(),
            Some(TLS_MATERIAL_INVALID)
        );
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
        let cert = include_str!("../tests/fixtures/m2_status/server.crt");
        let key = include_str!("../tests/fixtures/m2_status/server.key");
        let ca = include_str!("../tests/fixtures/m2_status/ca.crt");
        let server_config = server_config(cert, key, ca).unwrap();
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(ca.as_bytes()).unwrap())
            .unwrap();
        let roots = Arc::new(roots);
        let authorized = ClientConfig::builder()
            .with_root_certificates(roots.clone())
            .with_client_auth_cert(
                vec![
                    CertificateDer::from_pem_slice(
                        include_str!("../tests/fixtures/m2_status/client.crt").as_bytes(),
                    )
                    .unwrap(),
                ],
                PrivateKeyDer::from_pem_slice(
                    include_str!("../tests/fixtures/m2_status/client.key").as_bytes(),
                )
                .unwrap(),
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
}
