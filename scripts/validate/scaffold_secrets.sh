#!/bin/sh
set -eu
python3 - <<'PY'
import hashlib,pathlib,re,subprocess
paths=subprocess.check_output(['git','ls-files'],text=True).splitlines()
pat=re.compile(r'(?i)(postgres(?:ql)?://[^\s:@]+:[^\s@]+@|-----BEGIN [A-Z ]*PRIVATE KEY-----|(?:password|api[_-]?key|secret|token)\s*[=:]\s*["\x27][^"\x27]{4,})')
synthetic_line_sha256 = {
 ('tests/test_context.py','5883f5c0337185c232e27e463d28012fab24d4ffd6d361b0117d8ac86b31ce69'),
 ('tests/validate_knowledge.py','3017c62f5055319a24264fab01d9ac94de78c8354d3c4c079db09e334c936353'),
 ('scripts/validate/failure_policy.py','cdf586d916094a050143f668573a622d0ef7b37b114dc7e4d72377c7546f7541'),
 ('src/failure_policy.rs','265977cf786da300c32cbd0fb0db99c25b2390f0dd24a2a75137ac37ab917df8'),
 ('src/m2_ownership.rs','7a7210fa8294ef38edf962dca6fc617ae3a3c17e1cc939becdb65348c4482940'),
}
hits=[]
for name in paths:
 p=pathlib.Path(name)
 if not p.is_file() or name.startswith('.beads/') or name.startswith('artifacts/'): continue
 try:text=p.read_text()
 except UnicodeDecodeError:continue
 for n,line in enumerate(text.splitlines(),1):
  if not pat.search(line):continue
  synthetic = (name,hashlib.sha256(line.encode()).hexdigest()) in synthetic_line_sha256
  approved_file_reference = (name,line.strip()) in {
   ('compose.yaml','POSTGRES_PASSWORD_FILE: /run/secrets/postgres_password'),
   ('.env.example','BORING_CDC_POSTGRES_PASSWORD_FILE=.secrets/postgres_password'),
  }
  if synthetic or approved_file_reference:continue
  hits.append(f'{name}:{n}')
assert not hits,hits
print('{"status":"pass","scope":"all tracked non-evidence files","secrets_found":0}')
PY
