# [Article 1 Capture Demo] Plan, visually

## Structure

```text
Cargo.toml / Cargo.lock              # pinned PostgreSQL CopyBoth client
src/
├── article1_capture.rs              # reusable live transport + JSONL rendering
├── article1_row_view.rs             # process-local TEACHING VIEW from same decoded objects
├── m1_decoder.rs                    # existing decoder; consumed unchanged
├── m1_cli_contract.rs               # existing `run` command contract
└── main.rs                          # dispatches `run` to live capture
fixtures/article1/
├── compose.yml                      # PostgreSQL 17.6 by accepted digest
├── schema-and-seed.sql              # fixed commerce dataset/publication/slot
└── mutate.sql                       # insert → update → delete
scripts/acceptance/article1.sh       # clean, real, repeatable reader flow
evidence/article1/                   # committed transcript + provenance
docs/issues/post1-capture-demo/      # reader command, limits, proof receipt
```

## Behavior

```mermaid
sequenceDiagram
    participant Reader
    participant CLI as boring-cdc run
    participant PG as PostgreSQL 17.6
    participant Decoder as existing m1_decoder
    Reader->>CLI: exact documented command
    CLI->>PG: validate version, publication, slot
    CLI->>PG: START_REPLICATION ... (pgoutput)
    PG-->>CLI: CopyBoth / CopyData
    CLI->>Decoder: decode_copy_data(frame)
    Decoder-->>CLI: BEGIN / row / COMMIT + LSNs
    CLI->>CLI: same RowChange updates article1_row_view
    CLI-->>Reader: raw event + teaching current row/removal JSONL
    Note over CLI,Reader: TEACHING VIEW only; no ClickHouse, durability, exactly-once, checkpoint, materializer, production state, or M4
```

## Change shape

```diff
 boring-cdc run
-  parse existing CLI contract
-  return CLI_HANDLER_UNAVAILABLE
+  validate one PostgreSQL 17.6 source/publication/slot
+  open reusable CopyBoth capture transport
+  feed frames to the existing m1_decoder
+  print BEGIN / INSERT / UPDATE / DELETE / COMMIT JSONL
+  expose absent or key-only old tuple state honestly
+  apply the same live RowChange to article1_row_view
+  show insert current row / update overwrite / delete removal

 article evidence
-  unavailable from this repository
+  real clean-run transcript + deterministic provenance
+  consumer-row shape
+  hard label: TEACHING VIEW; NOT ClickHouse/durable/exactly-once/checkpointed/materializer/production/M4
+  ClickHouse and destination guarantees deferred to Article 4/M4
```
