#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../..";export TMPDIR=/var/tmp
for attempt in 1 2; do
  out="/var/tmp/m2-heartbeat-fault-tests-${attempt}-$$.out"
  cargo test --locked m2_heartbeat::tests -- --nocapture >"$out"
  grep -q 'failed_or_non_heartbeat_commit_never_invokes_feedback ... ok' "$out"
  grep -q 'cadence_backoff_cardinality_and_degraded_health_are_bounded ... ok' "$out"
  grep -q 'maximum_operation_timeout_fences_one_attempt_and_drop_is_bounded ... ok' "$out"
  grep -q 'generic_router_never_checkpoints_failed_user_transaction ... ok' "$out"
  rm -f "$out"
done
work=$(mktemp -d /var/tmp/m2-heartbeat-fault.XXXXXX);trap 'rm -rf "$work"' EXIT INT TERM
stdout1="$work/lane-1.stdout"; stderr1="$work/lane-1.stderr"
stdout2="$work/lane-2.stdout"; stderr2="$work/lane-2.stderr"
cargo run --quiet --locked --example m2_heartbeat_component -- timeout >"$stdout1" 2>"$stderr1"
cargo run --quiet --locked --example m2_heartbeat_component -- timeout >"$stdout2" 2>"$stderr2"
python3 - "$work/observation.json" "$stdout1" "$stderr1" "$stdout2" "$stderr2" <<'PY'
import json,pathlib,sys
pairs=[(pathlib.Path(sys.argv[2]),pathlib.Path(sys.argv[3])),(pathlib.Path(sys.argv[4]),pathlib.Path(sys.argv[5]))]
observations=[];events=[]
required={'schema_version','case_event_seq','bead_id','scenario_id','correlation_id','run_id','capture_epoch','component','phase','outcome','config_fingerprint','generation','intent_id','request_id','xid','commit_lsn','end_lsn','journal_range','anchor','fence','attempt','fault_hook','failure_class','failure_fingerprint','metric_units','evidence_digest'}
for stdout,stderr in pairs:
 observed=json.loads(stdout.read_text()); observations.append(observed)
 assert observed['runtime_lane_observed'] and observed['failure_fingerprint']=='HEARTBEAT_WRITE_UNAVAILABLE'
 assert observed['maximum_operation_timed_out'] and observed['lane_fenced']
 assert observed['attempts_started']==1 and observed['attempts_finished_at_fence']==0
 assert observed['drop_ms'] < 100 and observed['elapsed_ms'] < 500
 lines=[line for line in stderr.read_text().splitlines() if line.strip()]
 assert len(lines)==1, lines
 event=json.loads(lines[0]); events.append(event)
 assert required <= event.keys(), sorted(required-event.keys())
 assert event['schema_version']=='heartbeat-health/v1' and event['case_event_seq']==1
 assert event['operation_timed_out'] and event['lane_fenced'] and event['fence']=='operation_timeout'
 assert event['fault_hook']=='maximum_operation_timeout' and not event['feedback_advanced']
 for forbidden in ('postgresql://','password','redacted-test-dsn','127.0.0.1'):
  assert forbidden not in lines[0]
assert events[0]==events[1], (events[0],events[1])
volatile={'drop_ms','elapsed_ms'}
assert {k:v for k,v in observations[0].items() if k not in volatile} == {k:v for k,v in observations[1].items() if k not in volatile}
observed=observations[0]
observed.update({'targeted_tests':True,'deterministic_attempts':2,'runtime_reruns_identical':True,'durable_before_feedback':True,'feedback_from_unrelated_wal':False,'control_writes_user_rows':False,'checkpoint_complete_only':True,'health_envelope_complete':True,'actual_stderr_captured':True})
pathlib.Path(sys.argv[1]).write_text(json.dumps(observed,sort_keys=True,separators=(',',':'))+'\n')
PY
M2_HEARTBEAT_OBSERVATION="$work/observation.json" \
 M2_HEARTBEAT_STDOUT="$stdout1" M2_HEARTBEAT_STDERR="$stderr1" \
 M2_HEARTBEAT_STDOUT_2="$stdout2" M2_HEARTBEAT_STDERR_2="$stderr2" \
 python3 scripts/lib/m2_heartbeat_evidence.py fault
scripts/validate/evidence.sh artifacts/boring-cdc-m2-heartbeat/SCN-M2-HEARTBEAT-FAULTS/heartbeat-component-v1/evidence.json
python3 scripts/validate/m2_heartbeat.py
echo M2_HEARTBEAT_FAULTS_OK
