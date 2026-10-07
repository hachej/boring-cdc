#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp

work=$(mktemp -d /var/tmp/m2-status-freshness.XXXXXX)
trap 'rm -rf "$work"' EXIT
for attempt in 1 2; do
  M2_STATUS_FRESHNESS_PROOF_OUT="$work/live-$attempt.json" \
    scripts/acceptance/durable_simple_case.sh >"$work/live-$attempt.log" 2>&1
  M2_STATUS_FRESHNESS_FAULT_PROOF_OUT="$work/fault-$attempt.json" \
    cargo test --locked m2_journal::tests::status_freshness_requires_a_heartbeat_committed_by_the_current_run -- --exact \
    >"$work/fault-$attempt.log" 2>&1
done
python3 scripts/lib/m2_status_freshness_evidence.py \
  "$work/live-1.json" "$work/live-2.json" "$work/fault-1.json" "$work/fault-2.json"
scripts/validate/evidence.sh \
  "artifacts/boring-cdc-m2-fault-status/SCN-M2-STATUS-FRESHNESS-COMPONENT/${M2_STATUS_FRESHNESS_EVIDENCE_SEED:-status-freshness-v1}/evidence.json"
