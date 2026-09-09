//! Executable adapter used only by the M1 PostgreSQL component fixture.
use boring_cdc::m1_control_fixtures::{
    ControlWriterState, ObservedControlUpdate, PublicationOperation, PublicationSpec,
    observe_truncate,
};
fn main() {
    let mode = std::env::args().nth(1).expect("mode");
    match mode.as_str() {
        "update" => {
            let wire_types = std::env::args().nth(2).expect("wire types");
            assert!(wire_types.split(',').any(|x| x == "85"));
            let event = ControlWriterState::default()
                .observe(&ObservedControlUpdate::heartbeat(1), None)
                .unwrap();
            assert!(
                !event.writes_user_row
                    && !event.writes_benchmark_mutation
                    && !event.feedback_eligible
            );
            println!("PASS pgoutput_update=journal_noop feedback=awaits_durable_commit");
        }
        "zero" => {
            let mut update = ObservedControlUpdate::heartbeat(1);
            update.affected_rows = 0;
            let failure = ControlWriterState::default()
                .observe(&update, None)
                .unwrap_err();
            assert_eq!(failure.fingerprint, "CONTROL_CARDINALITY_INVALID");
            println!("PASS zero_rows=block_before_feedback");
        }
        "truncate" => {
            let wire_types = std::env::args().nth(2).expect("wire types");
            assert!(wire_types.split(',').any(|x| x == "84"));
            let failure = observe_truncate(PublicationOperation::Truncate).unwrap_err();
            assert_eq!(failure.fingerprint, "TRUNCATE_REQUIRES_RESEED");
            println!("PASS pgoutput_truncate=block_before_feedback");
        }
        "drift" => {
            let expected = PublicationSpec::new(
                "boring_cdc_publication",
                "boring_cdc_admin",
                ["public.accounts".into()],
            );
            let observed = PublicationSpec::new(
                "boring_cdc_publication",
                "boring_cdc_admin",
                ["public.accounts".into(), "public.forced_drift".into()],
            );
            let failure = expected.verify(&observed).unwrap_err();
            assert_eq!(failure.fingerprint, "PUBLICATION_DRIFT");
            println!("PASS publication_drift=requires_reseed");
        }
        _ => panic!("unknown mode"),
    }
}
