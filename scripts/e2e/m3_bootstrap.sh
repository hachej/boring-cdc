#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."; export TMPDIR=/var/tmp
work=$(mktemp -d /var/tmp/m3-bootstrap-e2e.XXXXXX);project="m3-bootstrap-$RANDOM-$$";port=$((54000+$$%1000))
cleanup(){ docker compose -p "$project" -f compose.yaml -f "$work/override.yml" down -v --remove-orphans >/dev/null 2>&1||true;rm -rf "$work";};trap cleanup EXIT INT TERM
python3 - <<'PY' >"$work/postgres_password"
import secrets;print(secrets.token_hex(24))
PY
chmod 600 "$work/postgres_password";export BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres_password"; export PGPASSWORD; PGPASSWORD=$(cat "$BORING_CDC_POSTGRES_PASSWORD_FILE")
cat >"$work/override.yml" <<YAML
services:
  postgres:
    ports: ["127.0.0.1:${port}:5432"]
YAML
docker compose -p "$project" -f compose.yaml -f "$work/override.yml" up -d --wait postgres >/dev/null
export BORING_CDC_M3_DSN; BORING_CDC_M3_DSN=$(printf 'postgresql://%s@127.0.0.1:%s/boring_cdc?sslmode=disable' "boring_cdc:${PGPASSWORD}" "$port")
for attempt in 1 2;do BORING_CDC_M3_SLOT="boring_cdc_m3_bootstrap_${attempt}" cargo test --locked m3_bootstrap::live_tests::exported_snapshot_uses_distinct_command_idle_sessions -- --ignored --exact;done
[[ "$(docker compose -p "$project" -f compose.yaml -f "$work/override.yml" exec -T postgres psql -At -U boring_cdc -d boring_cdc -c 'show server_version')" == 17.6* ]]
python3 scripts/lib/m3_bootstrap_evidence.py e2e
scripts/validate/evidence.sh artifacts/boring-cdc-m3-bootstrap/SCN-M3-BOOTSTRAP-LIVE-SESSIONS/bootstrap-pg17-v1/evidence.json
