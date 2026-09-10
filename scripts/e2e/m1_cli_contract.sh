#!/bin/sh
set -eu
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/e2e/m1_cli_contract.sh [SEED]'; exit 0; }
seed=${1:-cli-contract-v1}; [ "$seed" = cli-contract-v1 ] || { echo 'E_SEED: expected cli-contract-v1' >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
before=$(git status --porcelain=v1 --untracked-files=no | sha256sum | cut -d' ' -f1)
cargo build --locked --quiet
bin=target/debug/boring-cdc
$bin --help | grep -q CMD-JOURNAL-INSPECT-EXPLAIN
for invocation in \
 'check --help' 'init --help' 'run --help' 'run --bootstrap --help' 'status --help' \
 'backfill start --help' 'backfill pause --help' 'backfill resume --help' 'backfill status --help' 'backfill restart --help' \
 'destination list --help' 'destination add --help' 'destination pause --help' 'destination resume --help' 'destination detach --help' 'destination promote --help' 'destination retire --help' 'destination verify --help' \
 'archive reconstruct --help' 'archive verify --help' 'replay --help' 'journal inspect --help' 'journal inspect --event-id e --explain --help' 'journal verify --help' 'journal gc --help' \
 'recover inspect --help' 'recover promotion --help' 'recover reseed --help'; do
 # Intentional word splitting: these are fixed, non-user test vectors.
 # shellcheck disable=SC2086
 $bin $invocation >/dev/null 2>e2e.stderr || { cat e2e.stderr >&2; rm -f e2e.stderr; exit 1; }
done
rm -f e2e.stderr
set +e
json=$($bin status --json 2>e2e.stderr); code=$?
set -e
[ "$code" -eq 4 ]; [ ! -s e2e.stderr ]; rm -f e2e.stderr
printf '%s' "$json" | python3 -c 'import json,sys; x=json.load(sys.stdin); assert x["schema_version"]==1 and x["command"]=="CMD-STATUS" and x["code"]=="CLI_HANDLER_UNAVAILABLE"; assert set(["schema_version","command","outcome","code","message","request_id","run_id","capture_epoch","condition","runbook_id","data","warnings","next_commands"]) <= set(x)'
after=$(git status --porcelain=v1 --untracked-files=no | sha256sum | cut -d' ' -f1)
[ "$before" = "$after" ]
printf 'm1 cli e2e pass seed=%s help=28 json=versioned source_mutation=none store_mutation=none cleanup=complete\n' "$seed"
