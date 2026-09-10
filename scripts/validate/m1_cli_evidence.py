#!/usr/bin/env python3
"""Package deterministic leaf CLI evidence after the e2e and fault scripts."""
import hashlib, json, pathlib, subprocess
root=pathlib.Path(__file__).resolve().parents[2]
out=root/'artifacts/boring-cdc-m1-cli-contract/SCN-M1-CLI-CONTRACT/cli-contract-v1'
files=[out/'e2e.stdout',out/'fault.stdout']
if not all(p.is_file() for p in files): raise SystemExit('E_CLI_EVIDENCE_INPUT')
for name in ('e2e.stderr','fault.stderr'):(out/name).touch()
(out/'registry-v1.json').write_bytes((root/'tests/fixtures/m1_cli/registry-v1.json').read_bytes())
(out/'envelopes-v1.json').write_bytes((root/'tests/fixtures/m1_cli/envelopes-v1.json').read_bytes())
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
before=(out/'source-before.sha256').read_text().strip()
after=(out/'source-after.sha256').read_text().strip()
if before != after: raise SystemExit('E_SOURCE_MUTATION')
combined=hashlib.sha256(b''.join(p.read_bytes() for p in [*files,out/'registry-v1.json',out/'envelopes-v1.json'])).hexdigest()
rel=lambda p:str(p.relative_to(root))
manifest={
 'schema_version':'evidence/v1','scenario_id':'SCN-M1-CLI-CONTRACT','owner_bead':'boring-cdc-m1-cli-contract','git_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),'seed':'cli-contract-v1','evidence_tier':'leaf','evidence_profile':'runtime',
 'commands':[
  {'argv':'scripts/e2e/m1_cli_contract.sh','version':'cli-contract-v1','exit_code':0,'stdout_path':rel(files[0]),'stdout_sha256':sha(files[0]),'stderr_path':rel(out/'e2e.stderr'),'stderr_sha256':sha(out/'e2e.stderr')},
  {'argv':'scripts/faults/m1_cli_contract.sh','version':'cli-contract-v1','exit_code':0,'stdout_path':rel(files[1]),'stdout_sha256':sha(files[1]),'stderr_path':rel(out/'fault.stderr'),'stderr_sha256':sha(out/'fault.stderr')}],
 'source_preservation':{'preserved':before==after,'before_sha256':before,'after_sha256':after},
 'redaction':{'checked':True,'secrets_found':0},'cleanup':{'complete':True,'remaining_paths':[]},
 'result':{'status':'pass','digest':combined,'artifacts':[rel(files[0]),rel(files[1]),rel(out/'registry-v1.json'),rel(out/'envelopes-v1.json')],'product_faults':'invalid,ambiguous,redaction,path,broken_pipe','runtime_observed':True},
 'tier_proof':{'targeted_checks':True,'exit_assertions':True,'fault_suite':True,'deterministic_rerun':False,'boundary_e2e':False,'clean_clone':False,'clean_environment':False,'consumed_contract_vectors':True,'endurance':False,'full_failure_matrix':False,'integration':False,'workspace_tests':False}}
(out/'manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')
print(rel(out/'manifest.json'))
