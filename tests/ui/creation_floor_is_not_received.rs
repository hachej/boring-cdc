use boring_cdc::m1_transition_kernel::{ReceivedLsn, SlotCreationFloor};

fn requires_received(_: ReceivedLsn) {}

fn main() {
    requires_received(SlotCreationFloor::from_server(42));
}
