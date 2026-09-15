#!/usr/bin/env python3
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))
from m1_control_evidence import VERSION, validate

if len(sys.argv) == 2 and sys.argv[1] == "--version":
    print(VERSION)
    raise SystemExit(0)
artifact = Path(sys.argv[1]) if len(sys.argv) == 2 else Path("artifacts/boring-cdc-m1-control-fixtures/SCN-M1-CONTROL-COMPONENT/m1-control-v1")
findings = validate(artifact)
print(json.dumps({"schema_version": "m1-control-command-validation/v1", "validator_version": VERSION, "status": "fail" if findings else "pass", "findings": findings}, sort_keys=True, separators=(",", ":")))
raise SystemExit(1 if findings else 0)
