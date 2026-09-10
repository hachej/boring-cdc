use boring_cdc::m1_cli_contract::{
    ExitCode, command_help, error_envelope, parse, root_help, unavailable,
};
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
            let result = unavailable(parsed.spec);
            if parsed.json {
                let mut bytes = serde_json::to_vec(&result).expect("envelope");
                bytes.push(b'\n');
                if write_stdout(&bytes).is_err() {
                    std::process::exit(0)
                }
            } else {
                eprintln!("{}: {}", result.code, result.message);
            }
            std::process::exit(ExitCode::Unavailable as i32);
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
