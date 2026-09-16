use boring_cdc::m1_transition_kernel::DurableSourceBoundary;

fn main() {
    let _: DurableSourceBoundary = serde_json::from_str(
        r#"{"commit_lsn":42,"journal_cursor":7}"#,
    ).unwrap();
}
