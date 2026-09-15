#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
TMP_ROOT=${TMPDIR:-/var/tmp}
LIST=$(mktemp "$TMP_ROOT/boring-cdc-package.XXXXXX")
trap 'rm -f "$LIST"' EXIT HUP INT TERM

cd "$ROOT"
cargo package --locked --allow-dirty --list >"$LIST"

python3 - "$LIST" <<'PY'
import json
import sys
from pathlib import PurePosixPath

paths = [line.strip() for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
required = {"Cargo.toml", "Cargo.lock", "LICENSE", "README.md", "rust-toolchain.toml", "src/lib.rs", "src/main.rs"}
missing = sorted(required - set(paths))
allowed_roots = {"src"}
allowed_files = required | {"Cargo.toml.orig", ".cargo_vcs_info.json"}
forbidden_roots = {".beads", ".github", ".handoff", "artifacts", "contracts", "evidence", "fixtures", "scripts", "tests"}
forbidden_files = {".env", ".env.example", "compose.yaml", "Dockerfile", "SECURITY.md", "AGENTS.md", "CLAUDE.md"}
unexpected = []
for raw in paths:
    path = PurePosixPath(raw)
    if raw in allowed_files or (path.parts and path.parts[0] in allowed_roots):
        continue
    unexpected.append(raw)
forbidden = sorted(
    raw for raw in paths
    if PurePosixPath(raw).parts
    and (PurePosixPath(raw).parts[0] in forbidden_roots or raw in forbidden_files)
)
if missing or unexpected or forbidden:
    print(json.dumps({"status": "fail", "missing": missing, "unexpected": sorted(unexpected), "forbidden": forbidden}, sort_keys=True, separators=(",", ":")))
    raise SystemExit(1)
print(f'{{"status":"pass","package_files":{len(paths)},"sensitive_repository_metadata":false}}')
PY
