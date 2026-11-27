#!/usr/bin/env bash
set -euo pipefail
umask 077
repo=$(cd "$(dirname "$0")/../.." && pwd)
cd "$repo"
packet=${BCDC_ARTICLE3_PACKET_DIR:-$(mktemp -d /var/tmp/article3-impact-packet.XXXXXX)}
mkdir -p "$packet"
packet=$(cd "$packet" && pwd)
work=$(mktemp -d /var/tmp/article3-impact.XXXXXX)
rows=${BCDC_ARTICLE3_IMPACT_ROWS:-200000}
[[ "$rows" =~ ^[1-9][0-9]{0,5}$ ]]
stage=build
project=""
finish() {
  local rc=$?
  trap - EXIT
  if [[ -n "$project" ]]; then
    docker compose -p "$project" -f "$repo/compose.yaml" -f "$work/override.yml" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  cd "$repo"
  python3 scripts/e2e/article3_source_impact.py finalize "$packet" "$stage" "$rc" "$rows"
  rm -rf "$work"
  echo "Article 3 source impact completed stage $stage with exit $rc. Packet: $packet"
  exit "$rc"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

export BORING_CDC_POSTGRES_PASSWORD_FILE="$work/postgres-password"
openssl rand -hex 24 >"$BORING_CDC_POSTGRES_PASSWORD_FILE"
export PGPASSWORD
PGPASSWORD=$(cat "$BORING_CDC_POSTGRES_PASSWORD_FILE")
admin_secret=$(openssl rand -hex 24)
runtime_secret=$(openssl rand -hex 24)
control_secret=$(openssl rand -hex 24)
export IMPACT_APP_SECRET
IMPACT_APP_SECRET=$(openssl rand -hex 24)
export CH_RUNTIME='https://unused.invalid'
cargo build --locked --bin boring-cdc
binary="$(realpath "${CARGO_TARGET_DIR:-target}/debug/boring-cdc")"
python_probe="$repo/scripts/e2e/article3_source_impact.py"

cat >"$work/override.yml" <<'YAML'
services:
  postgres:
    ports: ["127.0.0.1::5432"]
YAML

measurement_failed=0
for arm in backfill naive ddl; do
  stage="$arm-setup"
  project="article3-impact-$arm-$RANDOM-$$"
  compose=(docker compose -p "$project" -f "$repo/compose.yaml" -f "$work/override.yml")
  "${compose[@]}" up -d --wait postgres
  pg_container=$("${compose[@]}" ps -q postgres)
  address=$("${compose[@]}" port postgres 5432)
  export PGPORT=${address##*:} PGHOST=127.0.0.1 PGDATABASE=boring_cdc PGUSER=boring_cdc
  psql -X -v ON_ERROR_STOP=1 -v "admin_password=$admin_secret" -v "runtime_password=$runtime_secret" \
    -v "control_password=$control_secret" -v "application_password=$IMPACT_APP_SECRET" \
    < scripts/setup/durable_simple_prerequisites.sql >/dev/null
  psql -X -v ON_ERROR_STOP=1 -v "row_count=$rows" <<'SQL' >/dev/null
ALTER TABLE public.orders RENAME COLUMN total TO payload;
ALTER TABLE public.orders ALTER COLUMN payload TYPE text;
INSERT INTO public.orders SELECT id, repeat(md5(id::text),16) FROM generate_series(1, :row_count) id;
ANALYZE public.orders;
SQL
  [[ "$(psql -X -Atqc 'SHOW server_version')" == 17.6* ]]
  profile="$work/$arm"
  mkdir -p "$profile/state/tmp" "$profile/state/spool" "$profile/archive/root"
  sed -e 's/total = "numeric:nullable"/payload = "text:nullable"/' \
    -e 's/verify_tls = true/verify_tls = false/' \
    -e 's/unsafe_local_experiment = false/unsafe_local_experiment = true/' \
    -e 's/chunk_rows = 1000/chunk_rows = 50/' \
    -e "s/127.0.0.1:8787/127.0.0.1:$((62000 + $$ % 1000))/" \
    -e "s/127.0.0.1:8788/127.0.0.1:$((63000 + $$ % 1000))/" \
    tests/fixtures/m1_config/representative.toml >"$profile/boring-cdc.toml"
  export PG_ADMIN=postgresql:"//boring_cdc_admin:$admin_secret@127.0.0.1:$PGPORT/boring_cdc?sslmode=disable"
  export PG_RUNTIME=postgresql:"//boring_cdc_runtime:$runtime_secret@127.0.0.1:$PGPORT/boring_cdc?sslmode=disable"
  export PG_CONTROL=postgresql:"//boring_cdc_control_writer:$control_secret@127.0.0.1:$PGPORT/boring_cdc?sslmode=disable"
  cd "$profile"
  confirmation=$(env -u BORING_CDC_POSTGRES_PASSWORD_FILE -u PG_ADMIN -u PG_CONTROL -u PG_RUNTIME -u CH_MAINT -u CH_RUNTIME "$binary" init --dry-run --json)
  token=$(jq -er .data.confirm_token <<<"$confirmation")
  CH_MAINT=$CH_RUNTIME env -u BORING_CDC_POSTGRES_PASSWORD_FILE -u PG_CONTROL -u PG_RUNTIME -u CH_RUNTIME \
    "$binary" init --confirm --confirm-token "$token" --json >"$profile/init.json"
  unset PG_ADMIN CH_MAINT
  stage="$arm-measure"
  env -u BORING_CDC_POSTGRES_PASSWORD_FILE python3 "$python_probe" measure "$packet" "$arm" "$binary" "$pg_container" "$rows" || measurement_failed=1
  cd "$repo"
  "${compose[@]}" down -v --remove-orphans
  project=""
done
if (( measurement_failed )); then
  stage=measurement_failed
  exit 1
fi
stage=complete
