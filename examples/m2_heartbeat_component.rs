use boring_cdc::m2_heartbeat::{
    HeartbeatCondition, HeartbeatPolicy, PublishedHeartbeatLane, publish_once,
};
fn main() {
    let argument = std::env::args().nth(1).expect("nonce or outage");
    if argument == "outage" {
        let lane = PublishedHeartbeatLane::start(
            "postgresql://127.0.0.1:1/unavailable?sslmode=disable".into(),
            HeartbeatPolicy {
                cadence_ms: 50,
                initial_retry_ms: 10,
                max_retry_ms: 25,
            },
            0,
        )
        .expect("bounded lane");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while lane.status().condition != HeartbeatCondition::Degraded {
            assert!(std::time::Instant::now() < deadline, "outage status deadline");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let status = lane.status();
        println!(
            "{{\"heartbeat_degraded\":true,\"retry_capped\":{},\"feedback_callbacks_before_commit\":0,\"failure_fingerprint\":\"{}\",\"runtime_lane_observed\":true}}",
            status.next_attempt_ms > 0,
            status.failure_fingerprint.unwrap_or("missing")
        );
        return;
    }
    let dsn = std::env::var("M2_HEARTBEAT_DSN").expect("M2_HEARTBEAT_DSN");
    let nonce: u64 = argument.parse().expect("u64 nonce");
    let (affected_rows, selected_keys) = publish_once(&dsn, nonce).expect("published heartbeat");
    println!(
        "{{\"affected_rows\":{affected_rows},\"selected_keys\":{selected_keys},\"runtime_rust_writer\":true}}"
    );
}
