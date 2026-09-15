# Boring CDC M0 Completion — plan, visually

## Structure

```text
Boring CDC M0 Completion
├── finish existing contracts
│   ├── PostgreSQL capture/backfill
│   ├── SQLite + disk budget
│   ├── event ABI ──┬── archive model
│   │               └── ClickHouse model
│   └── repository scaffold
├── correct Article 1 sequencing policy
├── wait for canonical owner decisions
├── prove M0 completion
└── store terminal close-guard JSON → PR → merge gate
```

## Behavior

```mermaid
sequenceDiagram
    participant O as Orchestrator
    participant W as Worker
    participant C as Owner card 59a63169
    participant P as Epic PR
    O->>W: dispatch one ready finish slice
    W->>W: inspect integrated artifact + prior handoff
    W->>W: local clippy/tests/validators
    W->>W: exact-SHA sandbox + fresh review
    W->>P: push verified SHA + durable handoff
    O->>W: correct Article 1 row after six slices
    C-->>O: canonical literal decisions
    O->>W: decisions → completion → close guard
    O->>P: freeze green head; request merge approval
```

## Diff-shaped scope

```diff
  six integrated M0 contract families
+ accept or narrowly repair only demonstrated remainder
+ provisional owner-card literals retain explicit M0-PROVISIONAL markers

- Article 1 requires M4 ClickHouse destination-row evidence
+ Article 1 proves raw pgoutput + same-stream non-durable teaching view
+ Article 4 retains ClickHouse, durability, checkpoint, and exactly-once bars

+ decisions barrier waits for existing card 59a63169
+ terminal scripts/validate/close_guard.sh boring-cdc-m0 JSON
  M2 durable-journal worktree/branch                     # untouched
```
