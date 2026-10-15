#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export TMPDIR=/var/tmp

work=$(mktemp -d /var/tmp/m3-guard-waiter.XXXXXX)
project="m3-guard-waiter-$RANDOM-$$"
port=$((54000+$$%1000))
cleanup() {
  docker compose -p "$project" -f compose.yaml -f "$work/override.yml" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT INT TERM
python3 - <<'PY' >"$work/postgres_password"
import secrets
print(secrets.token_hex(24))
PY
chmod 600 "$work/postgres_password"
export BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password"
export PGPASSWORD
PGPASSWORD=$(cat "$BORING_CDC_POSTGRES_PASSWORD_FILE")
cat >"$work/override.yml" <<YAML
services:
  postgres:
    ports: ["127.0.0.1:${port}:5432"]
YAML
docker compose -p "$project" -f compose.yaml -f "$work/override.yml" up -d --wait postgres >/dev/null
version=$(docker compose -p "$project" -f compose.yaml -f "$work/override.yml" exec -T postgres psql -At -U boring_cdc -d boring_cdc -c 'show server_version')
[[ "$version" == 17.6* ]]
export BORING_CDC_M3_DSN="postgresql://boring_cdc:${PGPASSWORD}@127.0.0.1:${port}/boring_cdc?sslmode=disable"
for attempt in 1 2; do
  CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}" cargo test --locked --lib m3_bootstrap::live_tests::bounded_ddl_waiter_releases_only_guarded_relation_lock -- --ignored --exact
done
printf 'M3_GUARD_WAITER_OK postgres=%s attempts=2\n' "$version"
