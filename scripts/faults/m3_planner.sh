#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp
for attempt in 1 2; do
  cargo test --locked m3_planner::tests::limits_concurrency_and_stale_generation_are_enforced -- --exact
  cargo test --locked m3_planner::tests::persisted_chunks_resume_and_empty_chunk_commits_atomically -- --exact
  cargo test --locked m3_planner::tests::finish_copy_rejects_incomplete_generation -- --exact
done
python3 scripts/lib/m3_planner_evidence.py faults
scripts/validate/evidence.sh artifacts/boring-cdc-m3-planner/SCN-M3-PLANNER-FAULTS/planner-pg17-v1/evidence.json
