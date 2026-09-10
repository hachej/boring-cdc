#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
export TMPDIR="${TMPDIR:-/var/tmp}"
[ "$TMPDIR" != /tmp ] || { echo "TMPDIR=/tmp is forbidden" >&2; exit 2; }
cargo test --locked m2_ownership::tests::deadline_and_session_loss_fence_without_reacquire
cargo test --locked m2_ownership::tests::unexpected_copyboth_loss_always_fences
cargo test --locked m2_ownership::tests::nonce_identity_lost_response_replay_and_later_repeat
cargo test --locked m2_ownership::tests::restart_reconciles_then_aborts_nonterminal_requests
python3 scripts/lib/m2_ownership_component.py fault
