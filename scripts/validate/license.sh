#!/bin/sh
set -eu
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
license_path=${1:-"$ROOT/LICENSE"}
exec python3 - "$ROOT" "$license_path" <<'PY'
import hashlib,json,re,subprocess,sys,tomllib
from pathlib import Path
root=Path(sys.argv[1]); license_path=Path(sys.argv[2])
OWNER='boring-cdc-d-license'; EXECUTOR='boring-cdc-m0-scaffold'; FIXTURE='fixtures/m0/decisions/boring-cdc-d-license.json'
SPDX='Apache-2.0'; LICENSE_SHA='cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30'
PROPOSED='Boring CDC is licensed under Apache-2.0 using the canonical Apache License, Version 2.0 text whose SHA-256 is cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30.'
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def fail(code='LICENSE_FIXTURE_INVALID'):
 print(json.dumps({'code':code,'outcome':'fail','phase':'validate_spec'},separators=(',',':'))); raise SystemExit(1)
try:
 spec=json.loads((root/FIXTURE).read_text())
 decisions=json.loads((root/'contracts/m0/decisions.json').read_text())
 artifacts=json.loads((root/'contracts/m0/artifacts.json').read_text())
 registry=json.loads((root/'contracts/agent/stable-ids.json').read_text())
 coverage=json.loads((root/'contracts/coverage/plan-to-beads.json').read_text())
 graph=[json.loads(x) for x in (root/'.beads/issues.jsonl').read_text().splitlines()]
 required=('inputs','preconditions','supported_matrix','deterministic_phase','expected','expected_failure','result_contract','redaction_assertions','later_executors')
 if any(not spec.get(x) for x in required): fail()
 if spec['fixture_id']!='DEC-LICENSE' or spec['owner_bead']!=OWNER or spec['later_executors']!=[EXECUTOR]: fail()
 if spec['approved_boundary']!={'license_file':'LICENSE','license_sha256':LICENSE_SHA,'license_version':'2.0','spdx_id':SPDX}: fail()
 if sha(license_path)!=LICENSE_SHA: fail('LICENSE_TEXT_MISMATCH')
 if spec['script']['path']!='scripts/validate/license.sh' or sha(root/spec['script']['path'])!=spec['script']['sha256']: fail()
 if EXECUTOR not in {x['id'] for x in graph}: fail()
 stable=next(x for x in registry['entries'] if x['id']=='DEC-LICENSE')
 covered=next(x for x in coverage['assignments'] if x['id']=='DEC-LICENSE')
 if stable['owner_bead']!=OWNER or covered!={'evidence_status':'pending','id':'DEC-LICENSE','owner_bead':OWNER,'source':'docs/PLAN.md','source_digest':stable['source_digest']}: fail()
 if subprocess.run([str(root/'scripts/validate/plan_coverage.sh')],cwd=root,capture_output=True).returncode: fail()
 probe_path=root/spec['execution_probe']['path']; probe=[json.loads(x) for x in probe_path.read_text().splitlines()]
 expected_probe=[{'code':'LICENSE_FIXTURE_VALID','metadata_state':'deferred_to_boring-cdc-m0-scaffold','outcome':'pass','phase':'validate_spec'}]
 if probe!=expected_probe or spec['execution_probe']['expected_lines']!=expected_probe or sha(probe_path)!=spec['execution_probe']['sha256']: fail()
 readme=(root/'README.md').read_text()
 if '## License\n\nLicensed under the [Apache License, Version 2.0](LICENSE) (`Apache-2.0`).' not in readme: fail('LICENSE_METADATA_MISMATCH')
 cargo=root/'Cargo.toml'
 metadata_state='deferred_to_boring-cdc-m0-scaffold'
 if cargo.exists():
  package=tomllib.loads(cargo.read_text()).get('package',{})
  if package.get('license')!=SPDX: fail('LICENSE_METADATA_MISMATCH')
  metadata_state='cargo_package_license_confirmed'
 row=next(x for x in decisions['decisions'] if x['id']=='DEC-LICENSE')
 approval={'approved_at':'2026-09-09T08:58:19Z','approved_by':'Julien Hurault (repository owner), intention d3e8abc3-d2f4-4bc0-8aec-d6ffd7bf2e36','value_digest':hashlib.sha256(PROPOSED.encode()).hexdigest()}
 expected={'approval':approval,'executor_beads':[EXECUTOR],'fixture_sha256':sha(root/FIXTURE),'fixture_spec':FIXTURE,'id':'DEC-LICENSE','owner_bead':OWNER,'proposed_value':PROPOSED,'status':'approved'}
 if row!=expected: fail()
 needed={'ART-M0-LICENSE-TEXT':'LICENSE','ART-M0-LICENSE-FIXTURE':FIXTURE,'ART-M0-LICENSE-PROBE':'artifacts/m0/decisions/boring-cdc-d-license/fixture-run.jsonl'}
 owned={x['id']:x for x in artifacts['artifacts'] if x.get('owner_bead')==OWNER}
 for ident,path in needed.items():
  item=owned.get(ident)
  if item!={'id':ident,'owner_bead':OWNER,'path':path,'sha256':sha(root/path),'status':'complete'}: fail()
except (OSError,KeyError,ValueError,TypeError,StopIteration,json.JSONDecodeError,tomllib.TOMLDecodeError): fail()
print(json.dumps({'code':'LICENSE_FIXTURE_VALID','metadata_state':metadata_state,'outcome':'pass','phase':'validate_spec'},separators=(',',':')))
PY
