#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
export TMPDIR="${TMPDIR:-/var/tmp}"
[ "$TMPDIR" != /tmp ] || { echo "TMPDIR=/tmp is forbidden" >&2; exit 2; }
python3 scripts/validate/m2_ownership.py
cargo test --locked m2_ownership::tests::symlinked_lock_parent_is_rejected
cargo test --locked m2_ownership::tests::two_processes_same_store_fail_closed
cargo test --locked m2_ownership::tests::two_state_paths_same_source_fail_closed
cargo test --locked m2_ownership::tests::offline_plan_and_races_revalidate_every_bound_predicate
cargo test --locked m2_ownership::tests::command_endpoint_rejects_peer_mode_message_and_time_bounds
python3 scripts/lib/m2_ownership_component.py e2e
