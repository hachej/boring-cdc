#!/bin/sh
set -eu
python3 - <<'PY'
import pathlib,re,subprocess
paths=subprocess.check_output(['git','ls-files'],text=True).splitlines()
pat=re.compile(r'(?i)(postgres(?:ql)?://[^\s:@]+:[^\s@]+@|-----BEGIN [A-Z ]*PRIVATE KEY-----|(?:password|api[_-]?key|secret|token)\s*[=:]\s*["\x27][^"\x27]{4,})')
hits=[]
for name in paths:
 p=pathlib.Path(name)
 if not p.is_file() or name.startswith('.beads/') or name.startswith('artifacts/'): continue
 try:text=p.read_text()
 except UnicodeDecodeError:continue
 for n,line in enumerate(text.splitlines(),1):
  if not pat.search(line):continue
  if 'example.invalid' in line or 'SECRET = re.compile' in line or 'pat=re.compile' in line or 'POSTGRES_PASSWORD_FILE' in line or 'E_SECRET' in line or 'user:pw@host/db' in line:continue
  hits.append(f'{name}:{n}')
assert not hits,hits
print('{"status":"pass","scope":"all tracked non-evidence files","secrets_found":0}')
PY
