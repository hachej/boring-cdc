#!/bin/sh
set -eu
python3 - <<'PY'
import pathlib,re,subprocess
paths=['Cargo.toml','Cargo.lock','rust-toolchain.toml','src/main.rs','config/boring-cdc.schema.json','config/boring-cdc.example.json','compose.yaml','Dockerfile','.dockerignore','.env.example','.github/workflows/ci.yml','README.md','AGENTS.md','CONTRIBUTING.md','SECURITY.md']
pat=re.compile(r'(?i)(postgres(?:ql)?://[^\s:@]+:[^\s@]+@|-----BEGIN [A-Z ]*PRIVATE KEY-----|(?:password|api[_-]?key|secret|token)\s*[=:]\s*["\x27][^"\x27]{4,})')
hits=[]
for name in paths:
 p=pathlib.Path(name)
 if not p.is_file() or p.parts[:1] in [('.beads',),('.handoff',)]: continue
 try: text=p.read_text()
 except UnicodeDecodeError: continue
 for n,line in enumerate(text.splitlines(),1):
  if pat.search(line) and 'POSTGRES_PASSWORD_FILE' not in line: hits.append(f'{name}:{n}')
assert not hits, hits
print('{"status":"pass","secrets_found":0}')
PY
