# Boring CDC M3 Chunked Backfill — plan, visually

## Structure — what M3 touches

```text
PostgreSQL 17.6
├── permanent pgoutput slot + exported snapshot
├── selected tables read by bounded keyset workers
└── published capture_fences row
         │
Rust binary
├── bootstrap / planner / schema guard
├── concurrent WAL capture (extends M2)
├── fence + restart + controls + reseed
└── exact oracle + fault runner
         │
SQLite journal
├── M2 durable WAL transactions
├── snapshot generations/chunks
└── complete reconstruction anchors
         │
JSONL archive only             ClickHouse/M4 untouched
```

## Behavior — how the exact stitch is proved

```mermaid
sequenceDiagram
    participant C as Coordinator
    participant P as PostgreSQL
    participant J as SQLite journal
    C->>J: persist bootstrap intent
    C->>P: create permanent slot + export snapshot
    C->>J: persist consistent point and snapshot token
    C->>P: start WAL capture on separate connection
    C->>P: import snapshot; read bounded keyset chunks
    C->>J: commit generation-fenced snapshot rows
    C->>P: update fixed capture_fences row with unique nonce
    P-->>C: emit fence transaction through pgoutput
    C->>J: atomically journal fence LSN + sequence
    C->>J: complete anchor only when lower stitch and fence pair agree
```

## Diff-shaped execution boundary

```diff
 M2 durable live capture
 ├── pgoutput -> bounded spool -> SQLite transaction
 ├── durable boundary -> PostgreSQL feedback
 └── JSONL materializer
+
+M3 existing-row startup
+├── permanent-slot exported-snapshot bootstrap
+├── persisted keyset plan + bounded workers
+├── schema guard held through durable fence observation
+├── exact lower stitch + published post-copy fence
+├── generation-fenced restart / pause / reseed
+└── PostgreSQL 17.6 fault matrix + exact mutation oracle

-Missing: reconstruction baseline for pre-existing rows
+Result: complete retained anchor; no sampled-LSN shortcut
```
