# Boring CDC M0 Completion — what changed, visually

Reviewed implementation: `c4e68e84e123c5558882c965500c0ad812d882ff`  
Actual commit range: `d1eb211e369551fc23f7cda2fa4d65be7af15330..c4e68e84e123c5558882c965500c0ad812d882ff`  
Target: `master`

## Contract and proof shape

```diff
 repository/
+├── contracts/m0/                  # exact artifact and decision inventories
+├── fixtures/m0/                   # deterministic positive and hostile vectors
+├── scripts/validate/              # six contract + nine decision-domain gates
+├── scripts/acceptance/
+│   └── m0_complete.sh             # 20-check aggregate verification
+├── artifacts/boring-cdc-m0-*/     # source/validator/input-bound receipts
+└── artifacts/boring-cdc-m0-gate/
+    └── gate/close-guard.json       # immutable terminal result: fail, 30 open nodes
```

## Shipped flow

```mermaid
sequenceDiagram
    participant PG as PostgreSQL pgoutput
    participant EV as Event ABI
    participant TV as Process-local teaching view
    participant ST as SQLite specification
    participant AR as Archive specification
    participant CH as ClickHouse specification (Article 4)
    participant G as M0 aggregate gate
    PG->>EV: raw relation and row messages
    EV->>TV: same-stream current-state projection
    Note over TV: non-durable; teaching only
    EV->>ST: canonical event contract
    ST->>AR: bounded generation contract
    AR->>CH: replay/convergence contract
    Note over CH: runtime evidence remains Article 4
    G->>G: verify 6 artifacts + 9 decisions + inventories
    G-->>G: preserve 7 provisional owner decisions
```

## Authority and closure

```diff
 M0 completion
- accepted work depended on prose and per-domain receipts alone
+ exact 78-artifact inventory and 37 required-owner inventory
+ six primary artifacts and nine decision-domain validators
+ immutable, source-bound deterministic completion receipt

 Owner authority
- risk: recommended literals could be mistaken for approvals
+ seven exact M0-PROVISIONAL decision markers remain open
+ d-pg-protocol / ARTICLE1-PROVISIONAL remains unresolved
+ no owner decision or legacy M0 hierarchy node was self-closed

 Terminal closure
- no durable final observation
+ close_guard JSON is byte-reproducible from the committed ledger
+ expected exit 1 / status fail / 30 reachable open nodes
! merge remains blocked until the owner/Orchestrator resolves closure authority
```

## Review focus

1. Run `TMPDIR=/var/tmp scripts/acceptance/m0_complete.sh --verify`; expect 20 green checks.
2. Run `scripts/validate/close_guard.sh boring-cdc-m0`; expect exit 1 and output byte-identical to `artifacts/boring-cdc-m0-gate/gate/close-guard.json`.
3. Confirm the seven `provisional_decisions` in the completion summary remain open and marked.
4. Confirm Article 1 is raw pgoutput plus only a process-local non-durable teaching view; ClickHouse remains Article 4.
