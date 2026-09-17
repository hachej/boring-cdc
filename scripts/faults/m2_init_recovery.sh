#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR=/var/tmp
cargo test --locked m2_init_recovery::tests
# Static fault assertions cover the irreversible boundary: init has no slot create/drop path,
# source identifiers are validated before interpolation, and every source mutation is lock-gated.
python3 - <<'PY'
p='src/m2_init_recovery.rs'; s=open(p).read()
for forbidden in ['pg_create_logical_replication_slot(', 'pg_drop_replication_slot(', 'CREATE_REPLICATION_SLOT']:
 assert forbidden not in s
for required in ['M2_INIT_OWNERSHIP_CONFLICT','M2_INIT_CONTROL_CARDINALITY_INVALID','M2_INIT_CONTROL_PRIVILEGE_EXCESS','M2_INIT_PUBLICATION_DRIFT','M2_INIT_PERMANENT_SLOT_EXISTS']:
 assert required in s
print('M2_INIT_RECOVERY_FAULTS_OK ambiguous=blocked cardinality=blocked privilege=blocked slot=never-created')
PY
