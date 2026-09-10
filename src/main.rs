use boring_cdc::m1_cli_contract::{
    ExitCode, command_help, error_envelope, parse, root_help, unavailable,
};
use boring_cdc::m1_config::{LoadPurpose, ProcessEnvironment, load_str_for};
use boring_cdc::m1_preflight::{
    CheckStatus, PreflightObservation, envelope, evaluate_untrusted, input_failure,
};
use std::io::{self, Write};
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
