use boring_cdc::m1_cli_contract::{COMMANDS, compatibility_fixture};
fn main() {
    if std::env::args().nth(1).as_deref() == Some("fixture") {
        println!(
            "{}",
            serde_json::to_string_pretty(&compatibility_fixture()).expect("contract")
        );
    } else {
        println!(
            "{}",
            serde_json::to_string_pretty(COMMANDS).expect("registry")
        );
    }
}
