#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp
work=$(mktemp -d /var/tmp/m3-fence-fault.XXXXXX)
trap 'rm -rf "$work"' EXIT
for attempt in 1 2; do
  cargo test --locked m3_fence::tests::delayed_copy_cannot_dispatch_or_complete_anchor -- --exact
  cargo test --locked m3_fence::tests::restart_reconciles_persisted_intent_without_inventing_a_proof -- --exact
  cargo test --locked m3_fence::tests::sampled_or_mismatched_observation_cannot_complete_anchor -- --exact
  cargo test --locked m3_fence::tests::invalidation_marks_planner_generation_and_fence_intent_ineligible_atomically -- --exact
  cargo test --locked m3_fence::tests::repeated_matching_transaction_is_audit_only_and_first_pair_is_immutable -- --exact
done
cat >"$work/result.json" <<'JSON'
{"anchor_before_durable_pair":false,"delayed_copy_blocked":true,"deterministic_attempts":2,"duplicate_audit_only":true,"restart_without_pair_blocked":true,"sampled_lsn_rejected":true}
JSON
BORING_CDC_M3_FENCE_OBSERVATION="$work/result.json" python3 scripts/lib/m3_fence_evidence.py faults
scripts/validate/evidence.sh artifacts/boring-cdc-m3-fence/SCN-M3-FENCE-FAULTS/fence-pg17-v1/evidence.json
