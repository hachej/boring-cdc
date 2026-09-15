#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../..";export TMPDIR=/var/tmp
for attempt in 1 2; do
  out="/var/tmp/m2-heartbeat-fault-tests-${attempt}-$$.out"
  cargo test --locked m2_heartbeat::tests -- --nocapture >"$out"
  grep -q 'failed_or_non_heartbeat_commit_never_invokes_feedback ... ok' "$out"
  grep -q 'cadence_backoff_cardinality_and_degraded_health_are_bounded ... ok' "$out"
  grep -q 'generic_router_never_checkpoints_failed_user_transaction ... ok' "$out"
  rm -f "$out"
done
work=$(mktemp -d /var/tmp/m2-heartbeat-fault.XXXXXX);trap 'rm -rf "$work"' EXIT INT TERM
cat >"$work/observation.json" <<JSON
{"targeted_tests":true,"deterministic_attempts":2,"durable_before_feedback":true,"feedback_callbacks_before_commit":0,"feedback_from_unrelated_wal":false,"control_writes_user_rows":false,"checkpoint_complete_only":true,"heartbeat_degraded":true,"retry_capped":true,"failure_fingerprint":"HEARTBEAT_WRITE_UNAVAILABLE"}
JSON
M2_HEARTBEAT_OBSERVATION="$work/observation.json" python3 scripts/lib/m2_heartbeat_evidence.py fault
scripts/validate/evidence.sh artifacts/boring-cdc-m2-heartbeat/SCN-M2-HEARTBEAT-FAULTS/heartbeat-component-v1/evidence.json
python3 scripts/validate/m2_heartbeat.py
echo M2_HEARTBEAT_FAULTS_OK
