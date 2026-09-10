#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
python3 scripts/validate/m2_ownership.py
cargo test --locked m2_ownership::tests::two_processes_same_store_fail_closed
cargo test --locked m2_ownership::tests::two_state_paths_same_source_fail_closed
cargo test --locked m2_ownership::tests::offline_plan_and_races_revalidate_every_bound_predicate
cargo test --locked m2_ownership::tests::command_endpoint_rejects_peer_mode_message_and_time_bounds
