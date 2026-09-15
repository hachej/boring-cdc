use boring_cdc::article1_capture::{CaptureConfig, CaptureFailure, capture_jsonl};
use boring_cdc::m1_cli_contract::{
    ExitCode, command_help, error_envelope, parse, root_help, unavailable,
};
use boring_cdc::m1_config::{LoadPurpose, ProcessEnvironment, load_str_for};
use boring_cdc::m1_preflight::{
    CheckStatus, PreflightObservation, envelope, evaluate_untrusted, input_failure,
};
use pg_walstream::CancellationToken;
use serde::Deserialize;
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

const REQUEST_LIMIT_BYTES: u64 = 1_048_576;
const RESPONSE_LIMIT_BYTES: u64 = 4_194_304;
const READ_TIMEOUT_SECONDS: u64 = 10;
const WRITE_TIMEOUT_SECONDS: u64 = 30;
const CONFIRMATION_EXPIRY_SECONDS: u64 = 300;
const RETRY_BASE_MILLISECONDS: u64 = 250;
const RETRY_CAP_SECONDS: u64 = 30;
const RETRY_MAX_ATTEMPTS: u16 = 10;
const SQLITE_BUSY_TIMEOUT_MILLISECONDS: u64 = 5_000;
const SQLITE_MAX_READERS: u16 = 16;
const SLOT_WAL_CAP_BYTES: u64 = 68_719_476_736;
const WAL_REACTION_RESERVE_SECONDS: u64 = 120;
const READINESS_TIMEOUT_SECONDS: u64 = 120;
const OWNERSHIP_TAKEOVER_SECONDS: u64 = 90;
const ARTICLE1_CONFIG_PATH: &str = "config/article1-reader.toml";
const ARTICLE1_CONFIG_LIMIT_BYTES: u64 = 16_384;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Article1ReaderConfig {
    schema_version: u32,
    dsn_env: String,
    stop_after_commits: usize,
}

#[derive(Clone, Copy)]
struct ReaderFailure {
    code: &'static str,
    message: &'static str,
    exit: ExitCode,
}

impl ReaderFailure {
    fn unavailable(code: &'static str, message: &'static str) -> Self {
        Self {
            code,
            message,
            exit: ExitCode::Unavailable,
        }
    }
}

fn load_article1_config() -> Result<CaptureConfig, ReaderFailure> {
    let file = std::fs::File::open(ARTICLE1_CONFIG_PATH).map_err(|_| {
        ReaderFailure::unavailable(
            "ARTICLE1_CONFIG_UNAVAILABLE",
            "reader configuration is unavailable",
        )
    })?;
    let metadata = file.metadata().map_err(|_| {
        ReaderFailure::unavailable(
            "ARTICLE1_CONFIG_UNAVAILABLE",
            "reader configuration is unavailable",
        )
    })?;
    if !metadata.is_file() || metadata.len() > ARTICLE1_CONFIG_LIMIT_BYTES {
        return Err(ReaderFailure::unavailable(
            "ARTICLE1_CONFIG_INVALID",
            "reader configuration is invalid",
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(ARTICLE1_CONFIG_LIMIT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| {
            ReaderFailure::unavailable(
                "ARTICLE1_CONFIG_UNAVAILABLE",
                "reader configuration is unavailable",
            )
        })?;
    if bytes.len() as u64 > ARTICLE1_CONFIG_LIMIT_BYTES {
        return Err(ReaderFailure::unavailable(
            "ARTICLE1_CONFIG_INVALID",
            "reader configuration is invalid",
        ));
    }
    let text = String::from_utf8(bytes).map_err(|_| {
        ReaderFailure::unavailable("ARTICLE1_CONFIG_INVALID", "reader configuration is invalid")
    })?;
    let reader: Article1ReaderConfig = toml::from_str(&text).map_err(|_| {
        ReaderFailure::unavailable("ARTICLE1_CONFIG_INVALID", "reader configuration is invalid")
    })?;
    if reader.schema_version != 1
        || reader.dsn_env != "BORING_CDC_ARTICLE1_DSN"
        || reader.stop_after_commits != 1
    {
        return Err(ReaderFailure::unavailable(
            "ARTICLE1_CONFIG_INVALID",
            "reader configuration is invalid",
        ));
    }
    let dsn = std::env::var(&reader.dsn_env).map_err(|_| {
        ReaderFailure::unavailable(
            "ARTICLE1_DSN_UNAVAILABLE",
            "reader source credential is unavailable",
        )
    })?;
    CaptureConfig::article1(dsn, reader.stop_after_commits)
        .map_err(|error| ReaderFailure::unavailable(error.code, "reader configuration is invalid"))
}

#[cfg(unix)]
static ARTICLE1_SIGNAL_RECEIVED: AtomicBool = AtomicBool::new(false);

#[cfg(unix)]
type SignalHandler = extern "C" fn(i32);

#[cfg(unix)]
unsafe extern "C" {
    fn signal(number: i32, handler: SignalHandler) -> SignalHandler;
}

#[cfg(unix)]
extern "C" fn article1_signal_handler(_: i32) {
    ARTICLE1_SIGNAL_RECEIVED.store(true, Ordering::Relaxed);
}

fn cancellation_for_signals() -> CancellationToken {
    #[cfg(unix)]
    {
        ARTICLE1_SIGNAL_RECEIVED.store(false, Ordering::Relaxed);
        // `signal` installs a handler that performs only an async-signal-safe atomic store.
        unsafe {
            signal(2, article1_signal_handler);
            signal(15, article1_signal_handler);
        }
    }
    CancellationToken::new()
}

#[cfg(unix)]
fn signal_received() -> bool {
    ARTICLE1_SIGNAL_RECEIVED.load(Ordering::Relaxed)
}

#[cfg(not(unix))]
fn signal_received() -> bool {
    false
}

fn capture_article1(
    config: CaptureConfig,
    cancellation: CancellationToken,
) -> Result<(), ReaderFailure> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|_| {
            ReaderFailure::unavailable(
                "ARTICLE1_RUNTIME_UNAVAILABLE",
                "reader runtime is unavailable",
            )
        })?;
    let mut stdout = io::stdout().lock();
    let mut broken_pipe = false;
    let result = runtime.block_on(capture_jsonl(&config, &cancellation, |line| {
        if let Err(error) = writeln!(stdout, "{line}").and_then(|_| stdout.flush()) {
            broken_pipe = error.kind() == io::ErrorKind::BrokenPipe;
            return Err(CaptureFailure {
                boundary: "output",
                code: "ARTICLE1_STDOUT_FAILED",
            });
        }
        Ok(())
    }));
    match result {
        Ok(_) => Ok(()),
        Err(_) if broken_pipe || cancellation.is_cancelled() => Ok(()),
        Err(error) => Err(ReaderFailure {
            code: error.code,
            message: "reader capture failed",
            exit: if error.boundary == "protocol" {
                ExitCode::Integrity
            } else {
                ExitCode::Unavailable
            },
        }),
    }
}

fn run_article1() -> Result<(), ReaderFailure> {
    let config = load_article1_config()?;
    let cancellation = cancellation_for_signals();
    let worker_cancellation = cancellation.clone();
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::Builder::new()
        .name("article1-capture".into())
        .spawn(move || {
            let _ = sender.send(capture_article1(config, worker_cancellation));
        })
        .map_err(|_| {
            ReaderFailure::unavailable(
                "ARTICLE1_RUNTIME_UNAVAILABLE",
                "reader runtime is unavailable",
            )
        })?;
    loop {
        if signal_received() {
            cancellation.cancel();
            // Do not join potentially blocked synchronous connection setup. Process exit
            // terminates this stdout-only provisional reader without publishing feedback.
            return Ok(());
        }
        match receiver.recv_timeout(Duration::from_millis(25)) {
            Ok(result) => return result,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(ReaderFailure::unavailable(
                    "ARTICLE1_RUNTIME_UNAVAILABLE",
                    "reader runtime is unavailable",
                ));
            }
        }
    }
}

fn scaffold_check() -> io::Result<()> {
    let mut out = io::stdout().lock();
    writeln!(
        out,
        "{{\"schema_version\":\"scaffold-check/v1\",\"status\":\"ready\",\"topology\":\"one-binary\",\"request_limit_bytes\":{REQUEST_LIMIT_BYTES},\"response_limit_bytes\":{RESPONSE_LIMIT_BYTES},\"read_timeout_seconds\":{READ_TIMEOUT_SECONDS},\"write_timeout_seconds\":{WRITE_TIMEOUT_SECONDS},\"confirmation_expiry_seconds\":{CONFIRMATION_EXPIRY_SECONDS},\"retry_base_milliseconds\":{RETRY_BASE_MILLISECONDS},\"retry_cap_seconds\":{RETRY_CAP_SECONDS},\"retry_max_attempts\":{RETRY_MAX_ATTEMPTS},\"sqlite_busy_timeout_milliseconds\":{SQLITE_BUSY_TIMEOUT_MILLISECONDS},\"sqlite_max_readers\":{SQLITE_MAX_READERS},\"slot_wal_cap_bytes\":{SLOT_WAL_CAP_BYTES},\"wal_reaction_reserve_seconds\":{WAL_REACTION_RESERVE_SECONDS},\"readiness_timeout_seconds\":{READINESS_TIMEOUT_SECONDS},\"ownership_takeover_seconds\":{OWNERSHIP_TAKEOVER_SECONDS}}}"
    )
}

fn serve() -> ! {
    loop {
        thread::sleep(Duration::from_secs(60));
    }
}

fn write_stdout(bytes: &[u8]) -> Result<(), ()> {
    let mut out = io::stdout().lock();
    out.write_all(bytes)
        .and_then(|_| out.flush())
        .map_err(|_| ())
}
fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    match argv.as_slice() {
        [command] if command == "scaffold-check" => {
            if scaffold_check().is_err() {
                std::process::exit(75);
            }
            return;
        }
        [command] if command == "serve" => serve(),
        _ => {}
    }
    if argv.is_empty() || matches!(argv.first().map(String::as_str), Some("-h" | "--help")) {
        let _ = write_stdout(root_help().as_bytes());
        return;
    }
    match parse(&argv) {
        Ok(parsed) if parsed.help => {
            let _ = write_stdout(command_help(parsed.spec).as_bytes());
        }
        Ok(parsed) => {
            if parsed.spec.id == "CMD-RUN" {
                match run_article1() {
                    Ok(()) => return,
                    Err(error) => {
                        eprintln!("{}: {}", error.code, error.message);
                        std::process::exit(error.exit as i32);
                    }
                }
            }
            let check_result = if parsed.spec.id == "CMD-CHECK" {
                Some(match std::fs::read_to_string("boring-cdc.toml") {
                    Err(_) => input_failure(
                        "SCN-M1-PREFLIGHT-CONFIG-INPUT",
                        "PREFLIGHT_CONFIG_UNAVAILABLE",
                    ),
                    Ok(config_text) => {
                        match load_str_for(&config_text, &ProcessEnvironment, LoadPurpose::Check) {
                            Err(error) => {
                                input_failure("SCN-M1-PREFLIGHT-CONFIG-INPUT", error.code)
                            }
                            Ok(config) => {
                                match std::fs::read_to_string("preflight-observation.json") {
                                    Err(_) => input_failure(
                                        "SCN-M1-PREFLIGHT-OBSERVATION-INPUT",
                                        "PREFLIGHT_OBSERVATION_UNAVAILABLE",
                                    ),
                                    Ok(observation_text) => {
                                        match serde_json::from_str::<PreflightObservation>(
                                            &observation_text,
                                        ) {
                                            Err(_) => input_failure(
                                                "SCN-M1-PREFLIGHT-SCHEMA",
                                                "PREFLIGHT_OBSERVATION_SCHEMA_UNSUPPORTED",
                                            ),
                                            Ok(observation) => evaluate_untrusted(
                                                config.public(),
                                                config.fingerprints().runtime.as_str(),
                                                &observation,
                                            ),
                                        }
                                    }
                                }
                            }
                        }
                    }
                })
            } else {
                None
            };
            let result = check_result
                .as_ref()
                .map(envelope)
                .unwrap_or_else(|| unavailable(parsed.spec));
            let exit = check_result
                .as_ref()
                .map_or(ExitCode::Unavailable, |report| match report.outcome {
                    CheckStatus::Healthy => ExitCode::Success,
                    CheckStatus::Degraded | CheckStatus::Unverified => ExitCode::Unavailable,
                    CheckStatus::Blocked => ExitCode::SafetyBlocked,
                });
            if parsed.json {
                let mut bytes = serde_json::to_vec(&result).expect("envelope");
                bytes.push(b'\n');
                if write_stdout(&bytes).is_err() {
                    std::process::exit(0)
                }
            } else if parsed.spec.id == "CMD-CHECK" {
                let mut text = format!("{}: {}\n", result.code, result.message);
                if let Some(report) = &check_result {
                    for check in report
                        .checks
                        .iter()
                        .filter(|check| check.reason != "PREFLIGHT_OK")
                    {
                        text.push_str(&format!("{}: {}\n", check.scenario_id, check.reason));
                    }
                }
                if write_stdout(text.as_bytes()).is_err() {
                    std::process::exit(0);
                }
            } else {
                eprintln!("{}: {}", result.code, result.message);
            }
            std::process::exit(exit as i32);
        }
        Err(error) => {
            if error.code == "CLI_ROOT_HELP" {
                let _ = write_stdout(root_help().as_bytes());
                return;
            }
            if argv.iter().any(|arg| arg == "--json") {
                let mut bytes = serde_json::to_vec(&error_envelope(&error)).expect("envelope");
                bytes.push(b'\n');
                if write_stdout(&bytes).is_err() {
                    std::process::exit(0);
                }
            } else {
                eprintln!("{}: {}", error.code, error.message);
            }
            std::process::exit(error.exit as i32);
        }
    }
}

#[cfg(test)]
mod m0_scaffold {
    mod tests {
        use super::super::*;

        #[test]
        fn article1_config_is_secret_indirect_and_bounded() {
            let text = include_str!("../config/article1-reader.toml");
            let config: Article1ReaderConfig = toml::from_str(text).unwrap();
            assert_eq!(config.schema_version, 1);
            assert_eq!(config.dsn_env, "BORING_CDC_ARTICLE1_DSN");
            assert_eq!(config.stop_after_commits, 1);
            assert!(!text.contains("postgresql://"));
            assert!((text.len() as u64) < ARTICLE1_CONFIG_LIMIT_BYTES);
        }

        #[test]
        fn constants_are_bounded_and_takeover_exceeds_reap_bound() {
            assert!(std::hint::black_box(REQUEST_LIMIT_BYTES) < RESPONSE_LIMIT_BYTES);
            assert!(std::hint::black_box(RETRY_BASE_MILLISECONDS) < RETRY_CAP_SECONDS * 1_000);
            assert_eq!(std::hint::black_box(RETRY_MAX_ATTEMPTS), 10);
            assert!(std::hint::black_box(SQLITE_MAX_READERS) <= 16);
            assert!(std::hint::black_box(WAL_REACTION_RESERVE_SECONDS) < 3_600);
            assert!(std::hint::black_box(OWNERSHIP_TAKEOVER_SECONDS) >= 70);
            assert!(std::hint::black_box(READINESS_TIMEOUT_SECONDS) >= OWNERSHIP_TAKEOVER_SECONDS);
        }

        #[test]
        fn package_license_is_apache_2() {
            assert_eq!(env!("CARGO_PKG_LICENSE"), "Apache-2.0");
        }
    }
}
