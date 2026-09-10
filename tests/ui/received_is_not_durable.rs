use boring_cdc::m1_transition_kernel::{DurableSourceBoundary, ReceivedLsn};

fn requires_durable(_: DurableSourceBoundary) {}

fn main() {
    requires_durable(ReceivedLsn::from_wire(42));
}
