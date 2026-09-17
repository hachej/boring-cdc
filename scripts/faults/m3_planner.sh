#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp
cargo test --locked --workspace --all-targets
work=$(mktemp -d /var/tmp/m3-planner-fault.XXXXXX); trap 'rm -rf "$work"' EXIT
for attempt in 1 2; do
  BORING_CDC_M3_TEST_OBSERVATION="$work/observation-$attempt.json" cargo test --locked m3_planner::tests::limits_concurrency_and_stale_generation_are_enforced -- --exact
  BORING_CDC_M3_ATOMIC_OBSERVATION="$work/atomic-$attempt.json" cargo test --locked m3_planner::tests::persisted_chunks_resume_and_event_commit_is_atomic -- --exact
  cargo test --locked m3_planner::tests::finish_copy_rejects_incomplete_generation -- --exact
done
python3 - "$work/observation-1.json" "$work/observation-2.json" "$work/atomic-1.json" "$work/atomic-2.json" >"$work/observation.json" <<'PY'
import json,pathlib,sys
runs=[json.loads(pathlib.Path(p).read_text()) for p in sys.argv[1:3]];atomic=[json.loads(pathlib.Path(p).read_text()) for p in sys.argv[3:]];assert runs[0]==runs[1] and atomic[0]==atomic[1]
o=runs[0];a=atomic[0];assert o['generation_state']=='invalidated' and o['remaining_claims']==0 and o['stale_completion_rejected'];assert a=={'chunk_commits':1,'pending_after_fault':1,'snapshot_events':1,'wal_checkpoint_busy':0};o.update(a,deterministic_attempts=len(runs),atomic_chunk_event_commit=a['snapshot_events']==a['chunk_commits'] and a['pending_after_fault']==1,bounded_reader_released=a['wal_checkpoint_busy']==0)
print(json.dumps(o,sort_keys=True))
PY
BORING_CDC_M3_OBSERVATION="$work/observation.json" python3 scripts/lib/m3_planner_evidence.py faults
scripts/validate/evidence.sh artifacts/boring-cdc-m3-planner/SCN-M3-PLANNER-FAULTS/planner-pg17-v1/evidence.json
