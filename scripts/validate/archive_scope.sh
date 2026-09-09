#!/bin/sh
set -eu
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
exec python3 - "$ROOT" <<'PY'
import hashlib,json,re,subprocess,sys
from pathlib import Path
root=Path(sys.argv[1]); owner='boring-cdc-d-archive-scope'; fixture_id='DEC-ARCHIVE-SCOPE'
fixture_rel='fixtures/m0/decisions/boring-cdc-d-archive-scope.json'
executors=['boring-cdc-m2-jsonl','boring-cdc-m5-parquet']
proposed='One scheduled archive materializer supports both JSONL and Parquet in v0.1 acceptance scope.'
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def fail():
 print('{"code":"ARCHIVE_SCOPE_FIXTURE_INVALID","outcome":"fail","phase":"validate_spec"}'); raise SystemExit(1)
try:
 spec=json.loads((root/fixture_rel).read_text()); decisions=json.loads((root/'contracts/m0/decisions.json').read_text())
 artifacts=json.loads((root/'contracts/m0/artifacts.json').read_text()); registry=json.loads((root/'contracts/agent/stable-ids.json').read_text())
 coverage=json.loads((root/'contracts/coverage/plan-to-beads.json').read_text()); graph=[json.loads(x) for x in (root/'.beads/issues.jsonl').read_text().splitlines()]
 required=('inputs','preconditions','supported_matrix','deterministic_phase','expected','expected_failure','result_contract','redaction_assertions','later_executors')
 if any(not spec.get(x) for x in required): fail()
 if spec['fixture_id']!=fixture_id or spec['decision_id']!=fixture_id or spec['owner_bead']!=owner or spec['later_executors']!=executors: fail()
 if spec['approved_boundary']!={'acceptance_scope':'v0.1','archive_materializer_count':1,'scheduled_formats':['jsonl','parquet']}: fail()
 expected_matrix=[{'case_id':'jsonl-only','enabled_formats':['jsonl']},{'case_id':'parquet-only','enabled_formats':['parquet']},{'case_id':'jsonl-and-parquet','enabled_formats':['jsonl','parquet']}]
 if spec['supported_matrix']!=expected_matrix: fail()
 if spec['schedule_contract']!={'checkpoint_unit':'journal_seq','range_end':'complete_committed_transaction','source_extraction_count':1}: fail()
 if spec['generation_contract']!={'checkpoint_count_per_archive_generation':1,'format_or_writer_configuration_change':'new_archive_generation','shared_checkpoint_requires':'all enabled formats durable at the same complete transaction boundary'}: fail()
 if spec['script']['path']!='scripts/validate/archive_scope.sh' or sha(root/spec['script']['path'])!=spec['script']['sha256']: fail()
 if set(executors)-{x['id'] for x in graph}: fail()
 stable=next(x for x in registry['entries'] if x['id']==fixture_id); covered=next(x for x in coverage['assignments'] if x['id']==fixture_id)
 if stable['owner_bead']!=owner or covered!={'evidence_status':'pending','id':fixture_id,'owner_bead':owner,'source':'docs/PLAN.md','source_digest':stable['source_digest']}: fail()
 if subprocess.run([str(root/'scripts/validate/plan_coverage.sh')],cwd=root,capture_output=True).returncode: fail()
 probe_rel='artifacts/m0/decisions/boring-cdc-d-archive-scope/fixture-run.jsonl'; probe=[json.loads(x) for x in (root/probe_rel).read_text().splitlines()]
 expected_probe=[{'code':'ARCHIVE_SCOPE_FIXTURE_VALID','outcome':'pass','phase':'validate_spec'}]
 if probe!=expected_probe or spec['execution_probe']!={'expected_lines':expected_probe,'path':probe_rel,'sha256':sha(root/probe_rel)}: fail()
 decision=next(x for x in decisions['decisions'] if x['id']==fixture_id)
 approval={'approved_at':'2026-09-09T08:58:19Z','approved_by':'Julien Hurault (repository owner), intention d3e8abc3-d2f4-4bc0-8aec-d6ffd7bf2e36','value_digest':hashlib.sha256(proposed.encode()).hexdigest()}
 if decision!={'approval':approval,'executor_beads':executors,'fixture_sha256':sha(root/fixture_rel),'fixture_spec':fixture_rel,'id':fixture_id,'owner_bead':owner,'proposed_value':proposed,'status':'approved'}: fail()
 needed={'ART-M0-ARCHIVE-SCOPE-FIXTURE':fixture_rel,'ART-M0-ARCHIVE-SCOPE-PROBE':probe_rel,'ART-M0-ARCHIVE-SCOPE-VALIDATION':'artifacts/m0/decisions/boring-cdc-d-archive-scope/evidence.json'}
 owned={x['id']:x for x in artifacts['artifacts'] if x.get('owner_bead')==owner}
 if set(owned)!=set(needed): fail()
 for ident,path in needed.items():
  if owned[ident]!={'id':ident,'owner_bead':owner,'path':path,'sha256':sha(root/path),'status':'complete'}: fail()
 evidence=json.loads((root/needed['ART-M0-ARCHIVE-SCOPE-VALIDATION']).read_text())
 if evidence.get('schema_version')!='validation-result/v1' or evidence.get('validator_version')!='core-validators/1.0.0' or evidence.get('owner_bead')!='boring-cdc-m0.1' or evidence.get('status')!='pass' or evidence.get('findings')!=[] or evidence.get('input_sha256')!=sha(root/'contracts/m0/decisions.json') or not re.fullmatch(r'[0-9a-f]{40}',evidence.get('git_commit','')): fail()
 evidence_sha=evidence['git_commit']
 if subprocess.run(['git','cat-file','-e',evidence_sha+'^{commit}'],cwd=root,capture_output=True).returncode or subprocess.run(['git','merge-base','--is-ancestor',evidence_sha,'HEAD'],cwd=root,capture_output=True).returncode or subprocess.run(['git','diff','--quiet',evidence_sha+'..HEAD','--',fixture_rel,'scripts/validate/archive_scope.sh','contracts/m0/decisions.json'],cwd=root).returncode: fail()
except (OSError,KeyError,ValueError,TypeError,StopIteration,json.JSONDecodeError): fail()
print('{"code":"ARCHIVE_SCOPE_FIXTURE_VALID","outcome":"pass","phase":"validate_spec"}')
PY
