//! Read-only local status transport for the capture-owning process.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const MAX_REQUEST_BYTES: usize = 8 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(10);

pub struct StatusListener {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl StatusListener {
    pub fn start(
        address: SocketAddr,
        tls: bool,
        snapshot: impl Fn() -> Result<Vec<u8>, ()> + Send + 'static,
    ) -> Result<Self, &'static str> {
        if tls || !address.ip().is_loopback() {
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
                        Ok((stream, peer)) if peer.ip().is_loopback() => {
                            handle(stream, &snapshot);
                        }
                        Ok(_) => {}
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

fn handle(mut stream: TcpStream, snapshot: &impl Fn() -> Result<Vec<u8>, ()>) {
    let _ = stream.set_read_timeout(Some(IO_TIMEOUT));
    let _ = stream.set_write_timeout(Some(IO_TIMEOUT));
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn local_status_is_read_only_and_bounded() {
        let calls = Arc::new(AtomicUsize::new(0));
        let snapshot_calls = Arc::clone(&calls);
        let server = StatusListener::start("127.0.0.1:0".parse().unwrap(), false, move || {
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
            StatusListener::start("0.0.0.0:0".parse().unwrap(), false, || Ok(Vec::new())).is_err()
        );
        assert!(StatusListener::start(address, true, || Ok(Vec::new())).is_err());
    }
}
