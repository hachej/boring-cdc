#!/usr/bin/env bash
set -euo pipefail
[[ ${1:-m0-context-v1} == m0-context-v1 ]] || exit 64
ROOT=$(cd "$(dirname "$0")/../.." && pwd); cd "$ROOT"
export BORING_AGENT_NOW=2026-01-01T00:00:00Z
for cmd in 'doctor' 'next' 'context boring-cdc-m0.2' 'impact docs/PLAN.md'; do
  read -ra argv <<< "$cmd"; "scripts/agent/${argv[0]}" "${argv[@]:1}" | python3 -m json.tool >/dev/null
done
