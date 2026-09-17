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
 ('vendor/pg_walstream/src/lib.rs','3ff4e303a30ae16c6e07ebc2db31714edbd94ae946e92eceb4f656cf280ed324'),
}
reviewed_vendor_paths = {
 'vendor/pg_walstream/.baseline-sha256',
 'vendor/pg_walstream/Cargo.toml',
 'vendor/pg_walstream/src/lib.rs',
 'vendor/pg_walstream/src/connection/mod.rs',
 'vendor/pg_walstream/src/connection/native/mod.rs',
 'vendor/pg_walstream/src/connection/native/connection.rs',
 'vendor/pg_walstream/src/connection/native/query.rs',
}
vendor_root='vendor/pg_walstream/'
baseline={}
for line in pathlib.Path(vendor_root+'.baseline-sha256').read_text().splitlines():
 digest,rel=line.split('  ',1);baseline[vendor_root+rel]=digest
tracked_vendor={name for name in paths if name.startswith(vendor_root) and name not in reviewed_vendor_paths}
assert tracked_vendor==set(baseline),('vendor baseline inventory drift',sorted(tracked_vendor^set(baseline)))
for name,digest in baseline.items():
 assert hashlib.sha256(pathlib.Path(name).read_bytes()).hexdigest()==digest,f'vendor baseline changed: {name}'
hits=[]
for name in paths:
 p=pathlib.Path(name)
 if not p.is_file() or name.startswith(('.beads/','artifacts/')): continue
 # Published dependency baseline contains synthetic credential fixtures. Its complete pinned
 # inventory is hash-verified above; every locally modified vendor surface is scanned here.
 if name.startswith('vendor/') and name not in reviewed_vendor_paths: continue
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
print('{"status":"pass","scope":"project files and locally modified vendor surfaces","secrets_found":0}')
PY
