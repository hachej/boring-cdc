#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$ROOT"
python3 scripts/lib/m0_scaffold.py validate >/dev/null
cargo run --locked --quiet -- check | grep '"status":"ready"' >/dev/null
first=$(mktemp -d "${TMPDIR:-/var/tmp}/m0-scaffold-first.XXXXXX")
second=$(mktemp -d "${TMPDIR:-/var/tmp}/m0-scaffold-second.XXXXXX")
trap 'rm -rf "$first" "$second"' EXIT HUP INT TERM
python3 scripts/lib/m0_scaffold.py generate --out "${first#$ROOT/}"
python3 scripts/lib/m0_scaffold.py generate --out "${second#$ROOT/}"
diff -ru "$first" "$second" >/dev/null
rm -rf artifacts/boring-cdc-m0-scaffold
python3 scripts/lib/m0_scaffold.py generate
scripts/validate/evidence.sh artifacts/boring-cdc-m0-scaffold >/dev/null
if command -v docker >/dev/null 2>&1; then
  mkdir -p .secrets; trap 'docker compose -f compose.yaml down --volumes --remove-orphans >/dev/null 2>&1 || true; rm -rf "$first" "$second" .secrets' EXIT HUP INT TERM
  printf 'scaffold-only-not-a-production-secret\n' > .secrets/postgres_password
  chmod 600 .secrets/postgres_password
  docker compose -f compose.yaml config --quiet
  if [ "${1:-}" != "--static" ]; then
    attempt=1
    until docker compose -f compose.yaml pull postgres clickhouse \
      && docker pull docker.io/library/rust:1.89.0-bookworm@sha256:948f9b08a66e7fe01b03a98ef1c7568292e07ec2e4fe90d88c07bb14563c84ff \
      && docker pull docker.io/library/debian:bookworm-20250811-slim@sha256:b1a741487078b369e78119849663d7f1a5341ef2768798f7b7406c4240f86aef \
      && DOCKER_BUILDKIT=0 docker compose -f compose.yaml build connector; do
      [ "$attempt" -lt 3 ] || exit 75
      attempt=$((attempt + 1))
      sleep 2
    done
    docker compose -f compose.yaml up -d --wait --wait-timeout 120
    docker compose -f compose.yaml exec -T connector boring-cdc check | grep '"status":"ready"' >/dev/null
  fi
fi
printf '%s\n' '{"status":"pass","scenario":"SCN-M0-SCAFFOLD-STATIC","deterministic_rerun":true}'
