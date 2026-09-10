use boring_cdc::m1_cli_contract::{
    ExitCode, command_help, error_envelope, parse, root_help, unavailable,
};
use boring_cdc::m1_config::{LoadPurpose, ProcessEnvironment, load_str_for};
use boring_cdc::m1_preflight::{CheckStatus, PreflightObservation, envelope, evaluate};
use std::io::{self, Write};

fn write_stdout(bytes: &[u8]) -> Result<(), ()> {
    let mut out = io::stdout().lock();
    out.write_all(bytes)
        .and_then(|_| out.flush())
        .map_err(|_| ())
}
fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
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
                let config_text = std::fs::read_to_string("boring-cdc.toml");
                let observation_text = std::fs::read_to_string("preflight-observation.json");
                match (config_text, observation_text) {
                    (Ok(config_text), Ok(observation_text)) => {
                        match (
                            load_str_for(&config_text, &ProcessEnvironment, LoadPurpose::Check),
                            serde_json::from_str::<PreflightObservation>(&observation_text),
                        ) {
                            (Ok(config), Ok(observation)) => {
                                Some(evaluate(config.public(), &observation))
                            }
                            _ => None,
                        }
                    }
                    _ => None,
                }
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
                let text = format!("{}: {}\n", result.code, result.message);
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
