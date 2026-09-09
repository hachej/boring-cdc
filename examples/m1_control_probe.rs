//! Executable adapter used only by the M1 PostgreSQL component fixture.
use boring_cdc::m1_control_fixtures::{
    ControlKind, ControlWriterState, PublicationSpec, decode_control_update, decode_truncate,
};

fn decode_hex_messages(value: &str) -> Vec<Vec<u8>> {
    value
        .split(',')
        .filter(|item| !item.is_empty())
        .map(|item| {
            assert_eq!(item.len() % 2, 0, "odd hex message");
            item.as_bytes()
                .chunks_exact(2)
                .map(|pair| {
                    u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).expect("hex byte")
                })
                .collect()
        })
        .collect()
}

fn expected_publication() -> PublicationSpec {
    PublicationSpec::new(
        "boring_cdc_publication",
        "boring_cdc_admin",
        ["public.accounts".into()],
    )
}

fn main() {
    let mode = std::env::args().nth(1).expect("mode");
    match mode.as_str() {
        "update" | "fence" => {
            let messages = decode_hex_messages(&std::env::args().nth(2).expect("wire hex"));
            let nonce = std::env::args()
                .nth(3)
                .expect("nonce")
                .parse::<u64>()
                .expect("numeric nonce");
            let kind = if mode == "update" {
                ControlKind::Heartbeat
            } else {
                ControlKind::CaptureFence
            };
            let update = decode_control_update(&messages, kind, nonce).unwrap();
            let mut writer = ControlWriterState::default();
            if kind == ControlKind::CaptureFence {
                writer.intend_fence(nonce).unwrap();
            }
            let event = writer.observe(&update, None).unwrap();
            assert!(!event.writes_user_row && !event.writes_benchmark_mutation);
            assert!(!event.feedback_eligible);
            println!("PASS pgoutput_update=decoded_typed_noop feedback=awaits_durable_commit");
        }
        "cardinality" => {
            let messages = decode_hex_messages(&std::env::args().nth(2).expect("wire hex"));
            let nonce = std::env::args()
                .nth(3)
                .map(|value| value.parse::<u64>().expect("numeric nonce"))
                .unwrap_or(1);
            let failure =
                decode_control_update(&messages, ControlKind::Heartbeat, nonce).unwrap_err();
            assert_eq!(failure.fingerprint, "CONTROL_CARDINALITY_INVALID");
            println!("PASS live_cardinality=block_before_feedback");
        }
        "nonce" | "shape" => {
            let messages = decode_hex_messages(&std::env::args().nth(2).expect("wire hex"));
            let expected_nonce = if mode == "nonce" { 999 } else { 4 };
            let failure = decode_control_update(&messages, ControlKind::Heartbeat, expected_nonce)
                .unwrap_err();
            let expected = if mode == "nonce" {
                "CONTROL_NONCE_MISMATCH"
            } else {
                "CONTROL_RELATION_SHAPE_INVALID"
            };
            assert_eq!(failure.fingerprint, expected);
            println!("PASS live_{mode}_mismatch=block_before_feedback");
        }
        "truncate" => {
            let messages = decode_hex_messages(&std::env::args().nth(2).expect("wire hex"));
            let failure = decode_truncate(&messages).unwrap_err();
            assert_eq!(failure.fingerprint, "TRUNCATE_REQUIRES_RESEED");
            println!("PASS pgoutput_truncate=decoded_block_before_feedback");
        }
        "catalog" | "catalog-drift" => {
            let observed: PublicationSpec =
                serde_json::from_str(&std::env::args().nth(2).expect("catalog json")).unwrap();
            let result = expected_publication().verify(&observed);
            if mode == "catalog" {
                result.unwrap();
                println!(
                    "PASS live_catalog=exact fingerprint={}",
                    observed.fingerprint()
                );
            } else {
                let failure = result.unwrap_err();
                assert_eq!(failure.fingerprint, "PUBLICATION_DRIFT");
                println!("PASS live_catalog_drift=publication_drift_requires_reseed");
            }
        }
        _ => panic!("unknown mode"),
    }
}
