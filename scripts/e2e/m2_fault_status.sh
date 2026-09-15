#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR=/var/tmp
work=$(mktemp -d /var/tmp/m2-fault-status.XXXXXX); trap 'rm -rf "$work"' EXIT INT TERM
# Reuse the just-hardened same-code live adapter proof: PostgreSQL 17.6, exact PID SIGKILL,
# restart reconciliation, and distinct missing/unreserved/lost slot states. Retain the direct
# process/boundary receipt when an evidence caller requests it.
M2_RECONCILE_PROOF_OUT="${M2_FAULT_STATUS_PROOF_OUT:-$work/reconcile-crash-proof.json}" scripts/e2e/m2_reconcile.sh
[[ -s "${M2_FAULT_STATUS_PROOF_OUT:-$work/reconcile-crash-proof.json}" ]]
mkdir -p "$work/run/state/spool"; chmod 700 "$work/run/state" "$work/run/state/spool"
cp tests/fixtures/m1_config/representative.toml "$work/run/boring-cdc.toml"
TMPDIR=/var/tmp cargo run --quiet --locked --example m2_fault_status_fixture -- "$work/run/state/boring.db"
TMPDIR=/var/tmp cargo build --quiet --locked --bin boring-cdc
export PG_RUNTIME='postgresql://redacted.invalid/db' PG_CONTROL='postgresql://redacted.invalid/db' PG_ADMIN='postgresql://redacted.invalid/db' CH_RUNTIME='https://redacted.invalid' CH_MAINT='https://redacted.invalid'
(cd "$work/run"; "$OLDPWD/target/debug/boring-cdc" status --json >status-1.json; "$OLDPWD/target/debug/boring-cdc" status >status.txt; "$OLDPWD/target/debug/boring-cdc" status --json >status-2.json)
python3 - "$work/run/status-1.json" "$work/run/status-2.json" "$work/run/status.txt" <<'PY'
import json,sys
one=json.load(open(sys.argv[1])); two=json.load(open(sys.argv[2])); text=open(sys.argv[3]).read(); a=one['data']; b=two['data']
assert one['command']=='CMD-STATUS' and one['outcome']=='success'
assert a['snapshot_id']==b['snapshot_id'] and a['state_revision']==b['state_revision']
assert a['schema_version']==1 and a['overall_health']=='blocked' and len(a['conditions'])>=2
assert all(c['procedure_status']=='pending_m6' and c['runbook_id'] for c in a['condition_details'])
assert 'source' in a['control_revisions'] and 'action_causality' in a
assert a['snapshot_id'] in text and 'facts_json:' in text
blob=json.dumps(one)+text
for secret in ('source-secret','database-secret','slot-secret','postgresql://'): assert secret not in blob
assert a['durability_boundaries']['feedback_lsn'] is None
PY
python3 scripts/validate/m2_fault_status.py
scripts/validate/runbook_registry.sh contracts/runbooks/index.json >/dev/null
printf 'M2_FAULT_STATUS_E2E_OK postgres=17.6 crash=exact-pid status=stable redaction=pass\n'
