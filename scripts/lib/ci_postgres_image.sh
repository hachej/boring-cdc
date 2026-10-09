#!/usr/bin/env bash
set -euo pipefail

mirror=${BORING_CDC_POSTGRES_IMAGE_MIRROR:-}
[[ -n "$mirror" ]] || exit 0

canonical=$(docker compose -f compose.yaml config --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["services"]["postgres"]["image"])')
expected="public.ecr.aws/docker/${canonical#docker.io/}"
if [[ "$canonical" != docker.io/library/postgres:*@sha256:* || "$mirror" != "$expected" ]]; then
  echo 'PostgreSQL mirror must have the canonical tag and manifest digest' >&2
  exit 2
fi
printf '    image: %s\n' "$mirror"
