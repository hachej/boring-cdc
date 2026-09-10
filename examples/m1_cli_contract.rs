use boring_cdc::m1_cli_contract::COMMANDS;
fn main() {
    println!(
        "{}",
        serde_json::to_string_pretty(COMMANDS).expect("registry")
    );
}
