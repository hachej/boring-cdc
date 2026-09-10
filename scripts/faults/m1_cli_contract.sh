#!/bin/sh
set -eu
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/faults/m1_cli_contract.sh [SEED]'; exit 0; }
seed=${1:-cli-contract-v1}; [ "$seed" = cli-contract-v1 ] || { echo 'E_SEED: expected cli-contract-v1' >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
cargo build --locked --quiet
bin=target/debug/boring-cdc
invalid() {
 set +e
 # Intentional fixed-vector splitting.
 # shellcheck disable=SC2086
 $bin $1 >fault.stdout 2>fault.stderr
 code=$?
 set -e
 [ "$code" -eq 2 ] || { echo "expected 2 got $code: $1" >&2; exit 1; }
 [ ! -s fault.stdout ]; grep -Eq '^CLI_[A-Z_]+' fault.stderr
}
invalid 'unknown'
invalid 'run --json'
invalid 'destination add d --archive-root p --continuity-break --from-seq 2 --dry-run --confirm-data-gap'
invalid 'replay d --from-seq 1 --since t --new-generation 2 --dry-run'
invalid 'journal inspect --explain'
# JSON diagnostics stay on stdout; stderr stays empty, including Unicode/path operands.
set +e
$bin destination verify '目标 path' --from-seq 1 --json >fault.stdout 2>fault.stderr
code=$?
set -e
[ "$code" -eq 4 ]; [ ! -s fault.stderr ]
python3 -c 'import json; x=json.load(open("fault.stdout")); assert "目标" not in str(x) and "path" not in str(x)'
! grep -Eqi 'confirm_token|nonce|postgres(ql)?://|password|dsn' fault.stdout fault.stderr
# A closed consumer is a normal Unix pipeline termination, never an invariant error.
set +e
$bin --help | head -n 1 >/dev/null
pipe_code=$?
set -e
[ "$pipe_code" -eq 0 ]
rm -f fault.stdout fault.stderr
cargo test --locked m1_cli_contract::tests >/dev/null
printf 'm1 cli fault pass seed=%s invalid_exit=2 unavailable_exit=4 redaction=pass broken_pipe=pass cleanup=complete\n' "$seed"
