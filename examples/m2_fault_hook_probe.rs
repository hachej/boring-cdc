use boring_cdc::m2_fault_status::{FaultHook, fault_hook};
fn main() {
    let name = std::env::args().nth(1).expect("hook name");
    let hook = FaultHook::ALL
        .into_iter()
        .find(|h| h.name() == name)
        .expect("known hook");
    fault_hook(hook);
    panic!("fault hook was not armed")
}
