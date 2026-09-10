# Boring CDC M0 Contracts — plan, visually

## Structure

```text
M0 contract lane (epic/boring-cdc-m0-contracts)
├── repository scaffold
├── PostgreSQL capture/backfill contract
├── SQLite + disk-budget contract
├── event-format contract
│   ├── archive format + commit protocol
│   └── executable ClickHouse model
└── PR + exact-SHA proof + provisional-value inventory
```

## Behavior

```mermaid
sequenceDiagram
    participant O as Orchestrator
    participant W as Worker
    participant R as Fresh review
    participant P as Epic branch / PR
    O->>W: dispatch exact labeled Bead
    W->>W: claim; apply recommended owner-card values
    W->>W: reconcile values after owner confirmation
    W->>W: build + typecheck + affected tests
    W->>W: commit and exact-SHA sandbox proof
    W->>R: adversarial review exact SHA
    R-->>W: approve or bounded findings
    W->>P: push verified commit; record handoff
    O->>O: poll status; dispatch next Bead
    O->>P: freeze green head; open one PR
```

## Diff-shaped scope

```diff
+ repository scaffold and validation wiring
+ exact event-format contract, schemas, validators, and vectors
+ PostgreSQL capture/backfill contract fixtures
+ SQLite/disk-budget contract fixtures
+ archive layout/commit/audit contract fixtures
+ executable ClickHouse DDL/query/model fixtures
+ exact-SHA evidence and confirmed owner-value inventory
+ zero remaining M0-PROVISIONAL markers after reconciliation

  boring-cdc-d-*                    # referenced, never changed or closed
  boring-cdc-m0                    # untouched
  boring-cdc-m0-{decisions,complete,gate}  # untouched
  M1/M2 branches and worktrees      # untouched
```
