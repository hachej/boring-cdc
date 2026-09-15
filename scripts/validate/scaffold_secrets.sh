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
  synthetic = ((name == 'tests/test_context.py' and 'postgres://user:supersecret@example.invalid/db' in line) or (name == 'tests/validate_knowledge.py' and 'postgresql://user:pw@host/db' in line) or (name in ('scripts/lib/core_validator.py','scripts/lib/knowledge_validator.py','scripts/validate/scaffold_secrets.sh') and ('SECRET = re.compile' in line or 'pat=re.compile' in line)))
  approved_file_reference = (name,line.strip()) in {
   ('compose.yaml','POSTGRES_PASSWORD_FILE: /run/secrets/postgres_password'),
   ('.env.example','BORING_CDC_POSTGRES_PASSWORD_FILE=.secrets/postgres_password'),
  }
  if synthetic or approved_file_reference:continue
  hits.append(f'{name}:{n}')
assert not hits,hits
print('{"status":"pass","scope":"all tracked non-evidence files","secrets_found":0}')
PY
