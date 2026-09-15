use boring_cdc::m2_heartbeat::publish_once;
fn main() {
    let dsn = std::env::var("M2_HEARTBEAT_DSN").expect("M2_HEARTBEAT_DSN");
    let nonce: u64 = std::env::args()
        .nth(1)
        .expect("nonce")
        .parse()
        .expect("u64 nonce");
    let (affected_rows, selected_keys) = publish_once(&dsn, nonce).expect("published heartbeat");
    println!(
        "{{\"affected_rows\":{affected_rows},\"selected_keys\":{selected_keys},\"runtime_rust_writer\":true}}"
    );
}
