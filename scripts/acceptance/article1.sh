#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
export TMPDIR="${TMPDIR:-/var/tmp}"
PORT="${ARTICLE1_PG_PORT:-55696}"
PROJECT="${ARTICLE1_PROJECT:-boring-cdc-article1-evidence}"
COMPOSE=(docker compose -p "$PROJECT" -f fixtures/article1/compose.yml)
PASSWORD_FILE="${BORING_CDC_POSTGRES_PASSWORD_FILE:-}"
if [[ -z "${PGPASSWORD:-}" ]]; then
  if [[ -n "$PASSWORD_FILE" ]]; then
    [[ -r "$PASSWORD_FILE" ]] || { echo "ARTICLE1_ACCEPTANCE_FAILED: unreadable PostgreSQL password file: $PASSWORD_FILE" >&2; exit 1; }
    PGPASSWORD=$(cat "$PASSWORD_FILE")
  else
    # The article-1 fixture initializes its own throwaway PostgreSQL in
    # fixtures/article1/compose.yml. Read the value from the fixture itself so the
    # two cannot drift, and so a clean clone can run this with no local setup.
    PGPASSWORD=$(sed -n 's/^[[:space:]]*POSTGRES_PASSWORD:[[:space:]]*//p' fixtures/article1/compose.yml | head -n 1 | tr -d '\r')
  fi
fi
if [[ -z "${PGPASSWORD:-}" ]]; then
  echo "ARTICLE1_ACCEPTANCE_FAILED: could not determine the article-1 fixture PostgreSQL password" >&2
  exit 1
fi
export PGPASSWORD
DSN="postgresql://postgres@127.0.0.1:${PORT}/article1?sslmode=disable"
WORK="$(mktemp -d "$TMPDIR/article1-evidence.XXXXXX")"

cleanup() {
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT
export ARTICLE1_PG_PORT="$PORT"

capture() {
  local scenario="$1" output="$2"
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  "${COMPOSE[@]}" up -d --wait >/dev/null
  if [[ "$scenario" == full ]]; then
    "${COMPOSE[@]}" exec -T postgres psql -v ON_ERROR_STOP=1 -U postgres -d article1 \
      -c 'ALTER TABLE customers REPLICA IDENTITY FULL;' >/dev/null
  fi

  BORING_CDC_ARTICLE1_DSN="$DSN" target/debug/boring-cdc run >"$output" 2>"$output.stderr" &
  local reader=$!
  sleep 1
  if [[ "$scenario" == default ]]; then
    sql="BEGIN; INSERT INTO customers VALUES (9101, 'Article Default', 1); UPDATE customers SET name='Article Default Updated', tier=2 WHERE id=9101; DELETE FROM customers WHERE id=9101; COMMIT;"
  else
    sql="BEGIN; INSERT INTO customers VALUES (9201, 'Article Full', 3); UPDATE customers SET name='Article Full Updated', tier=4 WHERE id=9201; DELETE FROM customers WHERE id=9201; COMMIT;"
  fi
  "${COMPOSE[@]}" exec -T postgres psql -v ON_ERROR_STOP=1 -U postgres -d article1 -c "$sql" >/dev/null
  wait "$reader"
  if [[ -s "$output.stderr" ]]; then
    cat "$output.stderr" >&2
    echo "ARTICLE1_ACCEPTANCE_FAILED: reader wrote stderr" >&2
    return 1
  fi
}

for round in 1 2; do
  capture default "$WORK/default-$round.raw.jsonl"
  capture full "$WORK/full-$round.raw.jsonl"
  python3 scripts/validate/article1_transcript.py \
    --default "$WORK/default-$round.raw.jsonl" \
    --full "$WORK/full-$round.raw.jsonl" \
    --output "$WORK/normalized-$round.jsonl" \
    --skip-manifest >/dev/null
done

cmp "$WORK/normalized-1.jsonl" "$WORK/normalized-2.jsonl"
cmp "$WORK/normalized-1.jsonl" evidence/article1/reader.normalized.jsonl
python3 scripts/validate/article1_transcript.py

cp -f evidence/article1/reader-default.raw.jsonl "$WORK/fabricated.raw.jsonl"
python3 - "$WORK/fabricated.raw.jsonl" <<'PY'
import json, pathlib, sys
path = pathlib.Path(sys.argv[1])
rows = [json.loads(line) for line in path.read_text().splitlines()]
rows[1]["new"] = None
rows[2]["new"] = ["fabricated"]
rows[3]["relation_id"] = 999999
rows[3]["unexpected"] = True
path.write_text("\n".join(json.dumps(row, separators=(",", ":")) for row in rows) + "\n")
PY
if python3 scripts/validate/article1_transcript.py --default "$WORK/fabricated.raw.jsonl" --skip-manifest >/dev/null 2>&1; then
  echo "ARTICLE1_ACCEPTANCE_FAILED: fabricated payload was accepted" >&2
  exit 1
fi
cp -f evidence/article1/manifest.json "$WORK/drifted-manifest.json"
python3 - "$WORK/drifted-manifest.json" <<'PY'
import json, pathlib, sys
path = pathlib.Path(sys.argv[1])
manifest = json.loads(path.read_text())
manifest["capture_code_sha"] = "fabricated"
path.write_text(json.dumps(manifest))
PY
if python3 scripts/validate/article1_transcript.py --manifest "$WORK/drifted-manifest.json" >/dev/null 2>&1; then
  echo "ARTICLE1_ACCEPTANCE_FAILED: provenance drift was accepted" >&2
  exit 1
fi

echo "ARTICLE1_ACCEPTANCE_OK postgres=17.6 clean_resets=2 normalized=byte-identical raw_stdout=preserved fabrication_drift=rejected"
