#!/bin/sh
set -eu
cd "$(dirname "$0")/../.."; export TMPDIR=/var/tmp
scratch=$(mktemp -d /var/tmp/m2-jsonl-e2e.XXXXXX); trap 'rm -rf "$scratch"' EXIT INT TERM
export BORING_CDC_WORKSPACE_TEST_STDOUT="$scratch/workspace.out" BORING_CDC_WORKSPACE_TEST_STDERR="$scratch/workspace.err"
if cargo test --locked --workspace --all-targets >"$BORING_CDC_WORKSPACE_TEST_STDOUT" 2>"$BORING_CDC_WORKSPACE_TEST_STDERR";then export BORING_CDC_WORKSPACE_TEST_EXIT_CODE=0;else cat "$BORING_CDC_WORKSPACE_TEST_STDERR" >&2;exit 1;fi
python3 scripts/lib/m2_jsonl_component.py e2e;mkdir "$scratch/expected"; cp artifacts/boring-cdc-m2-jsonl/SCN-M2-JSONL-COMPONENT/jsonl-component-v1/config.json artifacts/boring-cdc-m2-jsonl/SCN-M2-JSONL-COMPONENT/jsonl-component-v1/fault-timeline.json "$scratch/expected"; cp -a artifacts/boring-cdc-m2-jsonl/SCN-M2-JSONL-COMPONENT/jsonl-component-v1/logs artifacts/boring-cdc-m2-jsonl/SCN-M2-JSONL-COMPONENT/jsonl-component-v1/state "$scratch/expected"; python3 scripts/lib/m2_jsonl_component.py e2e; diff -ru "$scratch/expected/config.json" artifacts/boring-cdc-m2-jsonl/SCN-M2-JSONL-COMPONENT/jsonl-component-v1/config.json; diff -ru "$scratch/expected/fault-timeline.json" artifacts/boring-cdc-m2-jsonl/SCN-M2-JSONL-COMPONENT/jsonl-component-v1/fault-timeline.json; diff -ru "$scratch/expected/logs" artifacts/boring-cdc-m2-jsonl/SCN-M2-JSONL-COMPONENT/jsonl-component-v1/logs; diff -ru "$scratch/expected/state" artifacts/boring-cdc-m2-jsonl/SCN-M2-JSONL-COMPONENT/jsonl-component-v1/state
python3 scripts/validate/m2_jsonl.py e2e
