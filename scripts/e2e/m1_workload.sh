#!/bin/sh
set -eu
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/e2e/m1_workload.sh [workload-v1]'; exit 0; }
seed=${1:-workload-v1}
[ "$seed" = workload-v1 ] || { echo 'E_SEED: expected workload-v1' >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
# This M1 component owns the provider-neutral oracle only. It intentionally does not start a
# provider or choose the unresolved d-keys/d-values encoding. Opaque non-zero test digests prove
# the adapter boundary; later provider suites supply approved digests and observations.
cargo test --locked m1_workload::tests::clean_fixed_seed_is_reproducible_and_sequence_gaps_are_diagnostic >/dev/null
cargo test --locked m1_workload::tests::delete_reinsert_and_key_change_have_distinct_ordered_correlations >/dev/null
printf 'm1 workload e2e pass seed=%s dataset=customers,products,orders,order_items ledger=exact business=independent final_state=typed sequence_gaps=diagnostic contract_mode=opaque-input cleanup=complete\n' "$seed"
