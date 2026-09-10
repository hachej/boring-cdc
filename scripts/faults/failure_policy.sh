#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."
cargo test --locked failure_policy::tests::deterministic_schedule_caps_jitters_and_ignores_clock_rollback
cargo test --locked failure_policy::tests::stale_epoch_generation_attempt_and_fingerprint_completions_cannot_clear
cargo test --locked failure_policy::tests::prepared_adapter_persists_reopens_and_clears_via_supplied_transaction
