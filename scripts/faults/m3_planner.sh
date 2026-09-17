#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp
work=$(mktemp -d /var/tmp/m3-planner-fault.XXXXXX); trap 'rm -rf "$work"' EXIT
for attempt in 1 2; do
  BORING_CDC_M3_TEST_OBSERVATION="$work/observation-$attempt.json" cargo test --locked m3_planner::tests::limits_concurrency_and_stale_generation_are_enforced -- --exact
  cargo test --locked m3_planner::tests::persisted_chunks_resume_and_empty_chunk_commits_atomically -- --exact
  cargo test --locked m3_planner::tests::finish_copy_rejects_incomplete_generation -- --exact
done
python3 - "$work/observation-1.json" "$work/observation-2.json" >"$work/observation.json" <<'PY'
import json,pathlib,sys
runs=[json.loads(pathlib.Path(p).read_text()) for p in sys.argv[1:]];assert runs[0]==runs[1]
o=runs[0];assert o['generation_state']=='invalidated' and o['remaining_claims']==0 and o['stale_completion_rejected'];o['deterministic_attempts']=len(runs)
print(json.dumps(o,sort_keys=True))
PY
BORING_CDC_M3_OBSERVATION="$work/observation.json" python3 scripts/lib/m3_planner_evidence.py faults
scripts/validate/evidence.sh artifacts/boring-cdc-m3-planner/SCN-M3-PLANNER-FAULTS/planner-pg17-v1/evidence.json
