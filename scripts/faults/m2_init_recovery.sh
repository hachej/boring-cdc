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
for required in [
 'M2_INIT_OWNERSHIP_CONFLICT','M2_INIT_CONTROL_CARDINALITY_INVALID',
 'M2_INIT_CONTROL_PRIVILEGE_EXCESS','M2_INIT_CONTROL_PRIVILEGE_MISSING',
 'M2_INIT_CONTROL_ROLE_MEMBERSHIP_EXCESS',
 'M2_INIT_PUBLICATION_RELATION_SET_MISMATCH','M2_INIT_PUBLICATION_OWNER_MISMATCH',
 'M2_INIT_PUBLICATION_PUBLISH_INSERT_MISMATCH','M2_INIT_PUBLICATION_PUBLISH_UPDATE_MISMATCH',
 'M2_INIT_PUBLICATION_PUBLISH_DELETE_MISMATCH','M2_INIT_PUBLICATION_PUBLISH_TRUNCATE_MISMATCH',
 'M2_INIT_PERMANENT_SLOT_EXISTS']:
 assert required in s
sql=open('scripts/setup/durable_simple_prerequisites.sql').read()
for role in ['boring_cdc_admin','boring_cdc_runtime','boring_cdc_control_writer','boring_cdc_app']:
 assert role in sql
runbook=open('docs/durable-simple-operator.md').read().split('Run these commands in this order',1)[1]
order=['init --dry-run','init --confirm','run --bootstrap','\nboring-cdc run\n']
assert [runbook.index(command) for command in order] == sorted(runbook.index(command) for command in order)
print('M2_INIT_RECOVERY_FAULTS_OK ambiguous=blocked cardinality=blocked publication_checks=specific privilege=blocked prerequisites=executable slot=never-created')
PY
