#!/bin/sh
set -eu
[ "${1:-}" != "--help" ] || { echo 'Usage: scripts/e2e/m1_cli_contract.sh [SEED]'; exit 0; }
seed=${1:-cli-contract-v1}; [ "$seed" = cli-contract-v1 ] || { echo 'E_SEED: expected cli-contract-v1' >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd); cd "$root"
out=artifacts/boring-cdc-m1-cli-contract/SCN-M1-CLI-CONTRACT/cli-contract-v1
mkdir -p "$out"
snapshot() {
  { git diff --no-ext-diff --binary HEAD -- . ':!artifacts';
    for path in state archive run; do [ ! -e "$path" ] || find "$path" -type f -print0 | sort -z | xargs -0r sha256sum; done;
  } | sha256sum | cut -d' ' -f1
}
before=$(snapshot)
printf '%s\n' "$before" > "$out/source-before.sha256"
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
# Execute one canonical parse-success form for every operation variant. Domain
# handlers are intentionally unavailable, so parse success is stable exit 4.
while IFS= read -r invocation; do
  [ -n "$invocation" ] || continue
  set +e
  eval "$bin $invocation" >e2e.stdout 2>e2e.stderr
  code=$?
  set -e
  [ "$code" -eq 4 ] || { echo "parse failure ($code): $invocation" >&2; exit 1; }
  [ ! -s e2e.stdout ]; grep -q '^CLI_HANDLER_UNAVAILABLE:' e2e.stderr
  case "$invocation" in 'run'|'run --bootstrap') continue;; esac
  set +e
  eval "$bin $invocation --json" >e2e.stdout 2>e2e.stderr
  code=$?
  set -e
  [ "$code" -eq 4 ]; [ ! -s e2e.stderr ]
  python3 -c 'import json; x=json.load(open("e2e.stdout")); assert x["schema_version"]==1 and x["code"]=="CLI_HANDLER_UNAVAILABLE"'
done <<'CASES'
check
init --dry-run
run
run --bootstrap
status
backfill start --dry-run
backfill pause --dry-run
backfill resume --dry-run
backfill status
backfill restart --confirm --confirm-token tok
destination list
destination add dest --archive-root archive/root --continuity-break --from-seq 1 --dry-run
destination pause dest --dry-run
destination resume dest --dry-run
destination detach dest --confirm --confirm-token tok
destination promote dest --generation 2 --confirm --confirm-token tok
destination retire dest --generation 1 --confirm --confirm-token tok
destination verify dest --from-seq 1
archive reconstruct dest --selector-fence 1 --output output/path
archive verify dest --selector-fence 1 --oracle-manifest manifest/path
replay dest --from-seq 1 --new-generation 2 --dry-run
journal inspect --event-id event
journal inspect --event-id event --explain
journal verify
journal gc --dry-run
recover inspect
recover promotion dest --adopt-external-fence --confirm --confirm-token tok
recover reseed --resume reseed-1 --confirm --confirm-token tok
CASES
rm -f e2e.stdout e2e.stderr
set +e
json=$($bin status --json 2>e2e.stderr); code=$?
set -e
[ "$code" -eq 4 ]; [ ! -s e2e.stderr ]; rm -f e2e.stderr
printf '%s' "$json" | python3 -c 'import json,sys; x=json.load(sys.stdin); assert x["schema_version"]==1 and x["command"]=="CMD-STATUS" and x["code"]=="CLI_HANDLER_UNAVAILABLE"; assert set(["schema_version","command","outcome","code","message","request_id","run_id","capture_epoch","condition","runbook_id","data","warnings","next_commands"]) <= set(x)'
after=$(snapshot)
printf '%s\n' "$after" > "$out/source-after.sha256"
[ "$before" = "$after" ]
printf 'm1 cli e2e pass seed=%s help=28 parse_paths=28 text=golden json=versioned source_mutation=none store_mutation=none cleanup=complete\n' "$seed" | tee "$out/e2e.stdout"
: > "$out/e2e.stderr"
