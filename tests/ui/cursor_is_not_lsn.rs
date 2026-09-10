use boring_cdc::m1_transition_kernel::{JournalCursor, ReceivedLsn};

fn requires_lsn(_: ReceivedLsn) {}

fn main() {
    requires_lsn(JournalCursor::from_store(42));
}
