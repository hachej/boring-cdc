#!/bin/sh
set -eu
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/faults/m1_workload.sh [workload-v1]'; exit 0; }
seed=${1:-workload-v1}
[ "$seed" = workload-v1 ] || { echo 'E_SEED: expected workload-v1' >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
for test in \
 business_only_overwritten_omission_fails_despite_ledger_and_final_state \
 ledger_only_omission_fails_despite_business_and_final_state \
 retry_duplicates_are_local_only_and_conflicts_fail \
 missing_provider_event_boundary_is_never_a_pass \
 zero_contract_digest_and_observation_overflow_fail_closed
do
  cargo test --locked "m1_workload::tests::$test" >/dev/null
done
printf 'm1 workload fault pass seed=%s business_only=fail ledger_only=fail retry_duplicate=pass retry_conflict=fail unavailable_boundary=unavailable unresolved_contract=blocked limits=blocked cleanup=complete\n' "$seed"
