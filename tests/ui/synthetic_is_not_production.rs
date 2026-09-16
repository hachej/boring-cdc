use boring_cdc::m1_transition_kernel::{synthetic, DurableSourceBoundary, JournalCursor, ReceivedLsn};

fn main() {
    let evidence = synthetic::commit_evidence(ReceivedLsn::from_wire(42), JournalCursor::from_store(7));
    let _ = DurableSourceBoundary::from_commit(evidence);
}
