//! Read-only local status transport for the capture-owning process.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig, ServerConnection, StreamOwned};

const MAX_REQUEST_BYTES: usize = 8 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_PEM_BYTES: usize = 1024 * 1024;

fn pem_blocks(text: &str, label: &str) -> Result<Vec<Vec<u8>>, &'static str> {
    if text.len() > MAX_PEM_BYTES {
        return Err("M2_STATUS_TLS_MATERIAL_INVALID");
    }
    let begin = format!("-----BEGIN {label}-----");
    let end = format!("-----END {label}-----");
    let mut lines = text.lines().map(str::trim).filter(|line| !line.is_empty());
    let mut blocks = Vec::new();
    while let Some(line) = lines.next() {
        if line != begin {
            return Err("M2_STATUS_TLS_MATERIAL_INVALID");
        }
        let mut encoded = String::new();
        loop {
            let line = lines.next().ok_or("M2_STATUS_TLS_MATERIAL_INVALID")?;
            if line == end {
                break;
            }
            if line.starts_with("-----") {
                return Err("M2_STATUS_TLS_MATERIAL_INVALID");
            }
            encoded.push_str(line);
        }
        let der = STANDARD
            .decode(encoded)
            .map_err(|_| "M2_STATUS_TLS_MATERIAL_INVALID")?;
        if der.is_empty() {
            return Err("M2_STATUS_TLS_MATERIAL_INVALID");
        }
        blocks.push(der);
    }
    if blocks.is_empty() {
        return Err("M2_STATUS_TLS_MATERIAL_INVALID");
    }
    Ok(blocks)
}

/// Parse and cryptographically validate the M1 process-start mTLS material before binding.
pub fn server_tls_config(
    cert_pem: &str,
    key_pem: &str,
    client_ca_pem: &str,
) -> Result<Arc<ServerConfig>, &'static str> {
    let certs = pem_blocks(cert_pem, "CERTIFICATE")?
        .into_iter()
        .map(CertificateDer::from)
        .collect::<Vec<_>>();
    let mut keys = pem_blocks(key_pem, "PRIVATE KEY")?;
    if keys.len() != 1 {
        return Err("M2_STATUS_TLS_MATERIAL_INVALID");
    }
    let key: PrivateKeyDer<'static> = PrivatePkcs8KeyDer::from(keys.remove(0)).into();
    let mut roots = RootCertStore::empty();
    for ca in pem_blocks(client_ca_pem, "CERTIFICATE")? {
        roots
            .add(CertificateDer::from(ca))
            .map_err(|_| "M2_STATUS_TLS_MATERIAL_INVALID")?;
    }
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|_| "M2_STATUS_TLS_MATERIAL_INVALID")?;
    let config = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .map_err(|_| "M2_STATUS_TLS_MATERIAL_INVALID")?;
    Ok(Arc::new(config))
}

pub struct StatusListener {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl StatusListener {
    pub fn start(
        address: SocketAddr,
        tls: Option<Arc<ServerConfig>>,
        snapshot: impl Fn() -> Result<Vec<u8>, ()> + Send + 'static,
    ) -> Result<Self, &'static str> {
        if tls.is_none() && !address.ip().is_loopback() {
            return Err("M2_STATUS_TLS_LISTENER_UNAVAILABLE");
        }
        let listener = TcpListener::bind(address).map_err(|_| "M2_STATUS_BIND_FAILED")?;
        let address = listener.local_addr().map_err(|_| "M2_STATUS_BIND_FAILED")?;
        listener
            .set_nonblocking(true)
            .map_err(|_| "M2_STATUS_BIND_FAILED")?;
        let stop = Arc::new(AtomicBool::new(false));
        let requested_stop = Arc::clone(&stop);
        let thread = thread::Builder::new()
            .name("m2-status-listener".into())
            .spawn(move || {
                while !requested_stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((mut stream, peer)) => {
                            if tls.is_none() && !peer.ip().is_loopback() {
                                continue;
                            }
                            let _ = stream.set_read_timeout(Some(IO_TIMEOUT));
                            let _ = stream.set_write_timeout(Some(IO_TIMEOUT));
                            if let Some(config) = &tls {
                                if let Ok(connection) = ServerConnection::new(Arc::clone(config)) {
                                    let mut secured = StreamOwned::new(connection, stream);
                                    handle(&mut secured, &snapshot);
                                    secured.conn.send_close_notify();
                                    let _ = secured.flush();
                                }
                            } else {
                                handle(&mut stream, &snapshot);
                            }
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(25));
                        }
                        Err(_) => break,
                    }
                }
            })
            .map_err(|_| "M2_STATUS_LISTENER_START_FAILED")?;
        Ok(Self {
            address,
            stop,
            thread: Some(thread),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }
}

impl Drop for StatusListener {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn handle(stream: &mut (impl Read + Write), snapshot: &impl Fn() -> Result<Vec<u8>, ()>) {
    let mut request = Vec::new();
    let mut chunk = [0_u8; 1024];
    while request.len() < MAX_REQUEST_BYTES {
        let count = match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(count) => count,
        };
        request.extend_from_slice(&chunk[..count]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    let response = if request.len() >= MAX_REQUEST_BYTES {
        (413, "Payload Too Large", None)
    } else {
        let line = request.split(|byte| *byte == b'\n').next().unwrap_or(&[]);
        match line {
            b"GET /status HTTP/1.1\r" | b"GET /status HTTP/1.0\r" => match snapshot() {
                Ok(bytes) => (200, "OK", Some(bytes)),
                Err(()) => (503, "Service Unavailable", None),
            },
            line if line.starts_with(b"POST ") || line.starts_with(b"PUT ") => {
                (405, "Method Not Allowed", None)
            }
            _ => (404, "Not Found", None),
        }
    };
    let body = response.2.unwrap_or_else(|| b"{}".to_vec());
    let header = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        response.0,
        response.1,
        body.len()
    );
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(&body);
    let _ = stream.flush();
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::pki_types::ServerName;
    use rustls::{ClientConfig, ClientConnection};
    use std::net::TcpStream;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn local_status_is_read_only_and_bounded() {
        let calls = Arc::new(AtomicUsize::new(0));
        let snapshot_calls = Arc::clone(&calls);
        let server = StatusListener::start("127.0.0.1:0".parse().unwrap(), None, move || {
            snapshot_calls.fetch_add(1, Ordering::Relaxed);
            Ok(br#"{"healthy":true}"#.to_vec())
        })
        .unwrap();
        let address = server.local_addr();
        for (request, expected) in [
            ("GET /status HTTP/1.1\r\nHost: localhost\r\n\r\n", "200 OK"),
            (
                "POST /status HTTP/1.1\r\nHost: localhost\r\n\r\n",
                "405 Method Not Allowed",
            ),
            (
                "GET /mutate HTTP/1.1\r\nHost: localhost\r\n\r\n",
                "404 Not Found",
            ),
        ] {
            let mut stream = TcpStream::connect(address).unwrap();
            stream.write_all(request.as_bytes()).unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            assert!(response.contains(expected), "{response}");
            if expected == "200 OK" {
                assert!(response.ends_with(r#"{"healthy":true}"#));
            }
        }
        let mut stream = TcpStream::connect(address).unwrap();
        stream
            .write_all(
                format!(
                    "GET /status HTTP/1.1\r\nHost: localhost\r\nX-Fill: {}\r\n\r\n",
                    "x".repeat(MAX_REQUEST_BYTES)
                )
                .as_bytes(),
            )
            .unwrap();
        let mut response = String::new();
        let read = stream.read_to_string(&mut response);
        assert!(read.is_err() || response.contains("413 Payload Too Large"));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let mut stream = TcpStream::connect(address).unwrap();
        stream
            .write_all(b"GET /status HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        response.clear();
        stream.read_to_string(&mut response).unwrap();
        assert!(response.contains("200 OK"));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        drop(server);
        assert!(
            StatusListener::start("0.0.0.0:0".parse().unwrap(), None, || Ok(Vec::new())).is_err()
        );
        assert_eq!(
            server_tls_config("broken", "broken", "broken").err(),
            Some("M2_STATUS_TLS_MATERIAL_INVALID")
        );
    }

    #[test]
    #[ignore = "requires ephemeral certificate material from scripts/e2e/m2_status_mtls.sh"]
    fn mtls_rejects_anonymous_client_and_serves_authenticated_status() {
        let material = |name| std::fs::read_to_string(std::env::var(name).unwrap()).unwrap();
        let server_cert = material("BCDC_TEST_STATUS_SERVER_CERT_FILE");
        let server_key = material("BCDC_TEST_STATUS_SERVER_KEY_FILE");
        let client_ca = material("BCDC_TEST_STATUS_CA_FILE");
        let client_cert = material("BCDC_TEST_STATUS_CLIENT_CERT_FILE");
        let client_key = material("BCDC_TEST_STATUS_CLIENT_KEY_FILE");
        let untrusted_cert = material("BCDC_TEST_STATUS_UNTRUSTED_CERT_FILE");
        let untrusted_key = material("BCDC_TEST_STATUS_UNTRUSTED_KEY_FILE");
        assert_eq!(
            server_tls_config(&server_cert, &client_key, &client_ca).err(),
            Some("M2_STATUS_TLS_MATERIAL_INVALID")
        );
        let config = server_tls_config(&server_cert, &server_key, &client_ca).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let snapshot_calls = Arc::clone(&calls);
        let server =
            StatusListener::start("127.0.0.1:0".parse().unwrap(), Some(config), move || {
                snapshot_calls.fetch_add(1, Ordering::Relaxed);
                Ok(br#"{"healthy":true}"#.to_vec())
            })
            .unwrap();
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from(
                pem_blocks(&client_ca, "CERTIFICATE").unwrap().remove(0),
            ))
            .unwrap();
        let client_cert = pem_blocks(&client_cert, "CERTIFICATE")
            .unwrap()
            .into_iter()
            .map(CertificateDer::from)
            .collect();
        let client_key: PrivateKeyDer<'static> =
            PrivatePkcs8KeyDer::from(pem_blocks(&client_key, "PRIVATE KEY").unwrap().remove(0))
                .into();
        let authenticated = ClientConfig::builder()
            .with_root_certificates(roots.clone())
            .with_client_auth_cert(client_cert, client_key)
            .unwrap();
        let connection = ClientConnection::new(
            Arc::new(authenticated),
            ServerName::try_from("localhost").unwrap(),
        )
        .unwrap();
        let mut secured =
            StreamOwned::new(connection, TcpStream::connect(server.local_addr()).unwrap());
        secured
            .write_all(b"GET /status HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        let mut response = String::new();
        secured.read_to_string(&mut response).unwrap();
        assert!(response.contains("200 OK") && response.ends_with(r#"{"healthy":true}"#));
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        let untrusted_cert = pem_blocks(&untrusted_cert, "CERTIFICATE")
            .unwrap()
            .into_iter()
            .map(CertificateDer::from)
            .collect();
        let untrusted_key: PrivateKeyDer<'static> =
            PrivatePkcs8KeyDer::from(pem_blocks(&untrusted_key, "PRIVATE KEY").unwrap().remove(0))
                .into();
        let untrusted = ClientConfig::builder()
            .with_root_certificates(roots.clone())
            .with_client_auth_cert(untrusted_cert, untrusted_key)
            .unwrap();
        let connection = ClientConnection::new(
            Arc::new(untrusted),
            ServerName::try_from("localhost").unwrap(),
        )
        .unwrap();
        let mut secured =
            StreamOwned::new(connection, TcpStream::connect(server.local_addr()).unwrap());
        let write = secured.write_all(b"GET /status HTTP/1.1\r\nHost: localhost\r\n\r\n");
        response.clear();
        let read = secured.read_to_string(&mut response);
        assert!(write.is_err() || read.is_err() || !response.contains("200 OK"));
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        let anonymous = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connection = ClientConnection::new(
            Arc::new(anonymous),
            ServerName::try_from("localhost").unwrap(),
        )
        .unwrap();
        let mut secured =
            StreamOwned::new(connection, TcpStream::connect(server.local_addr()).unwrap());
        let write = secured.write_all(b"GET /status HTTP/1.1\r\nHost: localhost\r\n\r\n");
        response.clear();
        let read = secured.read_to_string(&mut response);
        assert!(write.is_err() || read.is_err() || !response.contains("200 OK"));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }
}
