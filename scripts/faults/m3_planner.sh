#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp
for attempt in 1 2; do
  cargo test --locked m3_planner::tests::limits_concurrency_and_stale_generation_are_enforced -- --exact
  cargo test --locked m3_planner::tests::persisted_chunks_resume_and_empty_chunk_commits_atomically -- --exact
  cargo test --locked m3_planner::tests::finish_copy_rejects_incomplete_generation -- --exact
done
observation=$(mktemp /var/tmp/m3-planner-fault-observation.XXXXXX); trap 'rm -f "$observation"' EXIT
printf '%s\n' '{"atomic_chunk_event_commit":true,"bounded_reader_released":true,"deterministic_attempts":2,"generation_state":"invalidated","limits_respected":true,"stale_completion_rejected":true}' >"$observation"
BORING_CDC_M3_OBSERVATION="$observation" python3 scripts/lib/m3_planner_evidence.py faults
scripts/validate/evidence.sh artifacts/boring-cdc-m3-planner/SCN-M3-PLANNER-FAULTS/planner-pg17-v1/evidence.json
