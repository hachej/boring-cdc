#!/usr/bin/env python3
"""Static contract checks for the M1-local opaque workload/oracle adapter."""
from pathlib import Path
import json
import sys

source = Path("src/m1_workload.rs").read_text()
lower_source = source.lower()
required = {
    "ledger fields": ["run_id", "mutation_seq", "mutation_id", "transaction_group_id", "entity_table", "key", "operation", "expected_after_hash", "committed_at_micros", "record_kind"],
    "proof dimensions": ["ledger_delivery", "business_event_delivery", "final_state_convergence"],
    "negative tests": ["business_only_overwritten_omission", "ledger_only_omission", "missing_provider_event_boundary"],
    "boundedness": ["max_records", "WORKLOAD_OBSERVATION_LIMIT", "WORKLOAD_SORT_RECORD_LIMIT", "external_sorted_digest", "BinaryHeap"],
    "opaque contracts": ["ContractDigests", "WORKLOAD_CONTRACT_DIGEST_UNRESOLVED", "opaque canonical-key digest", "opaque typed-row digest"],
}
fixture = json.loads(Path("fixtures/m1/workload-v1.json").read_text())
assert fixture["owner_bead"] == "boring-cdc-m1-workload"
assert len({case["id"] for case in fixture["cases"]}) == len(fixture["cases"]) == 8
assert fixture["contract_inputs"]["normalization_owned_here"] is False
missing = [(group, token) for group, tokens in required.items() for token in tokens if token.lower() not in lower_source]
for forbidden in ("postgresql://", "tokio_postgres", "sqlx", "unicode_normalization"):
    if forbidden in lower_source:
        missing.append(("forbidden normalization/provider coupling", forbidden))
if missing:
    for group, token in missing:
        print(f"FAIL {group}: {token}", file=sys.stderr)
    raise SystemExit(1)
print("PASS m1 workload schema=opaque-provider-neutral proofs=3 negatives=2 gaps=diagnostic bounded=1")
