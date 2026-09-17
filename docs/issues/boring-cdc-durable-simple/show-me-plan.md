# [Boring CDC Durable Stream Simple Case] Plan, visually

## Structure

```text
src/
├── article1_capture.rs          # demo fixture remains article-1 specific
├── m2_capture_runtime.rs        # configured publication/slot → CopyBoth → journal → feedback
└── m2_init_recovery.rs          # exact prerequisite checks and specific diagnostics
scripts/acceptance/
└── durable_simple_case.sh       # real PostgreSQL 17.6 crash/restart proof; no SQLite writes
docs/
└── operator happy path          # prerequisites, init, bootstrap, run
```

## Behavior

```mermaid
sequenceDiagram
    participant O as Operator/test
    participant P as PostgreSQL 17.6
    participant C as boring-cdc
    participant J as SQLite journal
    O->>P: create roles/schema/tables/publication
    O->>C: init --dry-run, then init --confirm
    O->>C: run --bootstrap
    O->>C: run
    P-->>C: pgoutput transactions
    C->>J: atomically journal transaction + end LSN
    J-->>C: durable commit
    C->>P: flush feedback ≤ durable LSN
    O-xC: kill -9; write more rows; restart
    P-->>C: resume from slot and catch up
    O->>J: read-only verification of exact-once contents and gap-free sequence
```

## Planned delta

```diff
 production capture
-  require article1_publication + article1_slot constants
+  validate and use configured publication + slot
   commit complete transaction to SQLite
   send feedback only after durable commit

 operator path
-  undocumented privilege/publication shape
-  fixture test mutates bootstrap_intents/source_state in SQLite
+  exact exercised PostgreSQL prerequisites with specific drift failures
+  documented init → run --bootstrap → run sequence
+  real kill -9 acceptance through product interfaces only
+  assert no loss, duplicates, sequence gaps, or feedback beyond durability
```
