#!/usr/bin/env python3
import json
import re
from pathlib import Path

root = Path(__file__).resolve().parents[2]
contract = json.loads((root / "contracts/m2/m0-provisional-reconciliation.json").read_text())
errors: list[str] = []

if contract.get("owner_bead") != "boring-cdc-7fz":
    errors.append("wrong owner")
if [card.get("question_id") for card in contract.get("answered_cards", [])] != [
    "5a994cfd-e4e2-46a7-b512-5dae280acae0",
    "765bd3b2-4b68-4102-a9ec-43ca93357390",
]:
    errors.append("answered card inventory mismatch")

sources = {
    name: (root / name).read_text()
    for name in (
        "src/m1_config.rs",
        "src/m1_ordering.rs",
        "src/m2_ownership.rs",
        "src/m2_schema.rs",
        "src/failure_policy.rs",
    )
}
joined = "\n".join(sources.values())
for governed in ("boring-cdc-d-security", "boring-cdc-d-keys", "boring-cdc-m2.1"):
    if f"M0-PROVISIONAL: {governed}" in joined:
        errors.append(f"governed marker remains: {governed}")

required_literals = {
    "src/m1_config.rs": (
        'status_listen_addr: "127.0.0.1:8787"',
        'prometheus_listen_addr: "127.0.0.1:8788"',
        "directory_mode: 0o700",
        "socket_mode: 0o600",
        "max_request_bytes: Bytes(1_048_576)",
        "max_response_bytes: Bytes(4_194_304)",
        "timeout_ms: Milliseconds(10_000)",
        "confirmation_expiry_ms: Milliseconds(300_000)",
    ),
    "src/m1_ordering.rs": ("MAX_CANONICAL_KEY_COMPONENTS: usize = 8",),
    "src/m2_ownership.rs": (
        "MAX_COMMAND_BYTES: usize = 1024 * 1024",
        "MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024",
        "COMMAND_READ_TIMEOUT: Duration = Duration::from_secs(10)",
        "COMMAND_WRITE_TIMEOUT: Duration = Duration::from_secs(30)",
    ),
    "src/m2_schema.rs": ("WRITER_BUSY_TIMEOUT: Duration = Duration::from_secs(5)",),
    "src/failure_policy.rs": (
        'POLICY_VERSION: &str = "failure-policy-v1"',
        'JITTER_TEST_SEED: &str = "0x424344435f52455452595f563031"',
        "BASE_DELAY_MS: u64 = 250",
        "MAX_DELAY_MS: u64 = 30_000",
        "MAX_ATTEMPTS: u32 = 10",
        "sample % nominal.saturating_add(1)",
        "TransientIo",
        "TransientSource",
        "TransientDestination",
        "RateLimited",
        "OwnershipLost",
        "BCDC_SHARED_TRANSPORT_UNAVAILABLE",
        "expected_close_run_id",
    ),
}
for path, literals in required_literals.items():
    for literal in literals:
        if literal not in sources[path]:
            errors.append(f"missing confirmed literal: {path}: {literal}")

actual_owners: set[str] = set()
for subtree in ("src", "contracts/m1", "scripts/e2e"):
    for path in (root / subtree).rglob("*"):
        if not path.is_file():
            continue
        try:
            text = path.read_text()
        except UnicodeDecodeError:
            continue
        actual_owners.update(re.findall(r"M0-PROVISIONAL:\s*([A-Za-z0-9_.-]+)", text))
expected_owners = {
    "boring-cdc-d-archive-durability",
    "boring-cdc-d-backfill",
    "boring-cdc-d-ch-accept",
    "boring-cdc-d-ddl",
    "boring-cdc-d-event-id",
    "boring-cdc-d-pg-protocol",
    "boring-cdc-d-publication",
    "boring-cdc-m1.6",
    "boring-cdc-m2-schema",
}
if actual_owners != expected_owners:
    errors.append(f"untouched owner inventory mismatch: {sorted(actual_owners)}")

print(json.dumps({
    "schema_version": "validation-result/v1",
    "validator": "m0-provisional-reconciliation/v1",
    "valid": not errors,
    "findings": errors,
}, sort_keys=True, separators=(",", ":")))
raise SystemExit(bool(errors))
