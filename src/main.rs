//! Repository scaffold only. Capture and destination behavior starts after M0.

use std::env;
use std::io::{self, Write};
use std::thread;
use std::time::Duration;

// M0-PROVISIONAL: boring-cdc-d-security
const REQUEST_LIMIT_BYTES: u64 = 1_048_576;
// M0-PROVISIONAL: boring-cdc-d-security
const RESPONSE_LIMIT_BYTES: u64 = 4_194_304;
// M0-PROVISIONAL: boring-cdc-d-security
const READ_TIMEOUT_SECONDS: u64 = 10;
// M0-PROVISIONAL: boring-cdc-d-security
const WRITE_TIMEOUT_SECONDS: u64 = 30;
// M0-PROVISIONAL: boring-cdc-d-security
const CONFIRMATION_EXPIRY_SECONDS: u64 = 300;
// M0-PROVISIONAL: boring-cdc-d-failure-policy
const RETRY_BASE_MILLISECONDS: u64 = 250;
// M0-PROVISIONAL: boring-cdc-d-failure-policy
const RETRY_CAP_SECONDS: u64 = 30;
// M0-PROVISIONAL: boring-cdc-d-failure-policy
const RETRY_MAX_ATTEMPTS: u16 = 10;
// M0-PROVISIONAL: boring-cdc-d-sqlite
const SQLITE_BUSY_TIMEOUT_MILLISECONDS: u64 = 5_000;
// M0-PROVISIONAL: boring-cdc-d-sqlite
const SQLITE_MAX_READERS: u16 = 16;
// M0-PROVISIONAL: boring-cdc-d-wal-cap
const SLOT_WAL_CAP_BYTES: u64 = 68_719_476_736;
// M0-PROVISIONAL: boring-cdc-d-wal-cap
const WAL_REACTION_RESERVE_SECONDS: u64 = 120;
// M0-PROVISIONAL: boring-cdc-d-compose
const READINESS_TIMEOUT_SECONDS: u64 = 120;
// M0-PROVISIONAL: boring-cdc-d-compose
const OWNERSHIP_TAKEOVER_SECONDS: u64 = 90;

fn check() -> io::Result<()> {
    let mut out = io::stdout().lock();
    writeln!(
        out,
        "{{\"schema_version\":\"scaffold-check/v1\",\"status\":\"ready\",\"topology\":\"one-binary\",\"request_limit_bytes\":{REQUEST_LIMIT_BYTES},\"response_limit_bytes\":{RESPONSE_LIMIT_BYTES},\"read_timeout_seconds\":{READ_TIMEOUT_SECONDS},\"write_timeout_seconds\":{WRITE_TIMEOUT_SECONDS},\"confirmation_expiry_seconds\":{CONFIRMATION_EXPIRY_SECONDS},\"retry_base_milliseconds\":{RETRY_BASE_MILLISECONDS},\"retry_cap_seconds\":{RETRY_CAP_SECONDS},\"retry_max_attempts\":{RETRY_MAX_ATTEMPTS},\"sqlite_busy_timeout_milliseconds\":{SQLITE_BUSY_TIMEOUT_MILLISECONDS},\"sqlite_max_readers\":{SQLITE_MAX_READERS},\"slot_wal_cap_bytes\":{SLOT_WAL_CAP_BYTES},\"wal_reaction_reserve_seconds\":{WAL_REACTION_RESERVE_SECONDS},\"readiness_timeout_seconds\":{READINESS_TIMEOUT_SECONDS},\"ownership_takeover_seconds\":{OWNERSHIP_TAKEOVER_SECONDS}}}"
    )
}

fn serve() -> ! {
    // A no-op process makes Compose health/restart wiring executable without
    // implementing capture, journal, or destination behavior in M0.
    loop {
        thread::sleep(Duration::from_secs(60));
    }
}

fn usage() {
    eprintln!("usage: boring-cdc <check|serve>");
}

fn main() {
    match env::args().nth(1).as_deref() {
        Some("check") => {
            if check().is_err() {
                std::process::exit(75);
            }
        }
        Some("serve") => serve(),
        _ => {
            usage();
            std::process::exit(64);
        }
    }
}

#[cfg(test)]
mod m0_scaffold {
    mod tests {
        use super::super::*;

        #[test]
        fn constants_are_bounded_and_takeover_exceeds_reap_bound() {
            let request = std::hint::black_box(REQUEST_LIMIT_BYTES);
            let response = std::hint::black_box(RESPONSE_LIMIT_BYTES);
            let retry_base = std::hint::black_box(RETRY_BASE_MILLISECONDS);
            let retry_cap = std::hint::black_box(RETRY_CAP_SECONDS);
            let max_attempts = std::hint::black_box(RETRY_MAX_ATTEMPTS);
            let max_readers = std::hint::black_box(SQLITE_MAX_READERS);
            let reserve = std::hint::black_box(WAL_REACTION_RESERVE_SECONDS);
            let takeover = std::hint::black_box(OWNERSHIP_TAKEOVER_SECONDS);
            let readiness = std::hint::black_box(READINESS_TIMEOUT_SECONDS);
            assert!(request < response);
            assert!(retry_base < retry_cap * 1_000);
            assert_eq!(max_attempts, 10);
            assert!(max_readers <= 16);
            assert!(reserve < 3_600);
            assert!(takeover >= 70);
            assert!(readiness >= takeover);
        }

        #[test]
        fn package_license_is_apache_2() {
            assert_eq!(env!("CARGO_PKG_LICENSE"), "Apache-2.0");
        }
    }
}
