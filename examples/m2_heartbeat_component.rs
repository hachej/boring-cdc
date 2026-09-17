use boring_cdc::m2_heartbeat::{
    HeartbeatCondition, HeartbeatLogContext, HeartbeatPolicy, PublishedHeartbeatLane, publish_once,
};
use std::sync::Arc;

fn log_context() -> HeartbeatLogContext {
    HeartbeatLogContext {
        scenario_id: "SCN-M2-HEARTBEAT-MAXIMUM-OPERATION".into(),
        correlation_id: "heartbeat-timeout:component-v1".into(),
        run_id: "heartbeat-timeout-run-v1".into(),
        capture_epoch: "heartbeat-epoch-v1".into(),
        config_fingerprint: "heartbeat-config-v1".into(),
    }
}

fn main() {
    let argument = std::env::args().nth(1).expect("nonce, outage, or timeout");
    if argument == "outage" || argument == "timeout" {
        let timeout = argument == "timeout";
        let started = std::time::Instant::now();
        let lane = if timeout {
            PublishedHeartbeatLane::start_with_publisher(
                "redacted-test-dsn".into(),
                HeartbeatPolicy {
                    cadence_ms: 20,
                    initial_retry_ms: 5,
                    max_retry_ms: 10,
                },
                0,
                std::time::Duration::from_millis(25),
                log_context(),
                Arc::new(|_, _| {
                    std::thread::sleep(std::time::Duration::from_millis(500));
                    Err(boring_cdc::m2_heartbeat::HeartbeatError::SourceUnavailable)
                }),
            )
        } else {
            PublishedHeartbeatLane::start(
                "postgresql://127.0.0.1:1/unavailable?sslmode=disable".into(),
                HeartbeatPolicy {
                    cadence_ms: 50,
                    initial_retry_ms: 10,
                    max_retry_ms: 25,
                },
                0,
                std::time::Duration::from_millis(100),
                log_context(),
            )
        }
        .expect("bounded lane");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while lane.status().condition != HeartbeatCondition::Degraded
            || (timeout && !lane.diagnostics().lane_fenced)
        {
            assert!(
                std::time::Instant::now() < deadline,
                "outage status deadline"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let status = lane.status();
        let diagnostics = lane.diagnostics();
        let before_drop = std::time::Instant::now();
        drop(lane);
        let drop_ms = before_drop.elapsed().as_millis();
        println!(
            "{{\"heartbeat_degraded\":true,\"retry_capped\":{},\"feedback_callbacks_before_commit\":0,\"failure_fingerprint\":\"{}\",\"runtime_lane_observed\":true,\"maximum_operation_timed_out\":{},\"attempts_started\":{},\"attempts_finished_at_fence\":{},\"lane_fenced\":{},\"drop_ms\":{},\"elapsed_ms\":{}}}",
            status.next_attempt_ms > 0,
            status.failure_fingerprint.unwrap_or("missing"),
            diagnostics.operation_timed_out,
            diagnostics.attempts_started,
            diagnostics.attempts_finished,
            diagnostics.lane_fenced,
            drop_ms,
            started.elapsed().as_millis(),
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
