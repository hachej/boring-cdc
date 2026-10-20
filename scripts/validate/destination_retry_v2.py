#!/usr/bin/env python3
"""Check the approved destination-only successor to the shared v1 retry policy."""

import hashlib
import json
from pathlib import Path


root = Path(__file__).resolve().parents[2]
contract = json.loads((root / "contracts/m2/destination-retry-v2.json").read_text())
old = root / contract["supersedes"]["path"]
source = (root / "src/failure_policy.rs").read_text()
operator = (root / "src/m2_destination_rearm.rs").read_text()
cli = (root / "src/m1_cli_contract.rs").read_text()
errors = []

if contract["schema_version"] != "destination-retry-policy/v2":
    errors.append("version")
if hashlib.sha256(old.read_bytes()).hexdigest() != contract["supersedes"]["sha256"]:
    errors.append("superseded contract digest")
if contract["supersedes"]["scope"] != [
    "transient_destination retry limit",
    "transient_destination operator re-arm",
]:
    errors.append("supersession scope")
if contract["unchanged"]["other_classes_maximum_attempts"] != 10:
    errors.append("other class budget")
if contract["unchanged"]["maximum_delay_ms"] != 30000:
    errors.append("delay cap")
if contract["transient_destination"]["maximum_attempts"] is not None:
    errors.append("destination retry limit")
if "class != FailureClass::TransientDestination" not in source or "retry_exhausted(class, attempt)" not in source:
    errors.append("runtime rule")
if contract["operator_rearm"]["command"] != ["destination", "rearm"]:
    errors.append("operator command")
if "CMD-DESTINATION-REARM" not in cli or "owner_is_current" not in operator:
    errors.append("owner command routing")

print(json.dumps({"schema_version": "validation-result/v1", "validator": "destination-retry-v2", "valid": not errors, "findings": errors}, sort_keys=True))
raise SystemExit(bool(errors))
