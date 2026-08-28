# Boring CDC v0.1 implementation plan

This plan was produced from a `/plan`-style architecture pass over the article series and market-research requirements. It is planning only; implementation has not started.

## 1. Objective

Build the smallest inspectable PostgreSQL CDC system that proves:

```text
PostgreSQL
  → durable local journal
    → independent ClickHouse materialization
    → independent scheduled JSONL/Parquet archive
```

The system captures once, acknowledges PostgreSQL only after durable local storage, replays destinations independently, performs production-aware backfills, and converges after expected crashes and outages.

It is a public experimental/educational project and benchmark artifact—not a production SLA or universal connector platform.

## 2. Core design decisions

1. Use `pgoutput` with exactly one publication and one logical replication slot.
2. Run as one Tokio-based Rust binary plus Docker Compose.
3. Store the append-only event journal and all checkpoints in SQLite.
4. Buffer and durably commit a complete Postgres transaction before acknowledging its end LSN.
5. Keep **journal order** (`journal_seq`) separate from **source version order** (snapshot boundary or WAL LSN plus rank). This prevents a resumed snapshot row written later to SQLite from overwriting a newer WAL update.
6. Give ClickHouse and archive destinations independent checkpoints, retries, and lag.
7. Promise at-least-once capture and idempotent convergence only.
8. Bound journal retention and Postgres WAL retention; fail loudly and require re-seeding when history is genuinely unavailable.

## 3. Runtime architecture

```text
cli/config
postgres/preflight
postgres/capture
postgres/backfill
journal/sqlite
materialize/clickhouse
materialize/archive
retention
safety
status/metrics
benchmark
```

Long-running tasks:

1. Postgres capture loop.
2. Backfill coordinator/workers.
3. ClickHouse materializer loop.
4. Archive scheduler/materializer loop.
5. Safety/retention monitor.
6. Status and metrics server.

A task failure must transition the affected component into persisted, visible state; it must not silently exit while the process appears healthy.

## 4. Required capabilities

### R1 — Postgres capture

- Connect to one Postgres database.
- Use one configured publication and one logical replication slot.
- Decode `pgoutput` transaction, relation, insert, update, and delete messages.
- Preserve transaction identity and event order.
- Record the source system identifier and reject an unexpected source replacement.
- Require a usable primary key or replica identity where update/delete correctness is required.
- Preflight supported types and table compatibility.
- Detect relation/schema changes and block incompatible tables rather than silently corrupting them.

### R2 — Durable acknowledgement

- Buffer one complete source transaction.
- Commit its events, transaction metadata, relation schema, and durable source LSN in one SQLite transaction.
- Use durability settings appropriate to the promise, including `synchronous=FULL`.
- Acknowledge the transaction end LSN to Postgres only after the SQLite commit succeeds.
- Never couple Postgres acknowledgement to destination progress.
- Resume from the last durable source LSN.
- Deduplicate replayed transactions/events using deterministic identities.

### R3 — Backfill and WAL stitching

- Create/attach the logical slot and obtain an exported-snapshot boundary.
- Persist boundary and run metadata before reading history.
- Copy by stable primary-key chunks with configurable size, rate, and concurrency.
- Keep WAL capture active during backfill.
- Persist completed chunks, row counts, progress, rate, and ETA.
- Support pause/resume/cancellation.
- On process/session loss, retain completed chunks and resume remaining ranges under a new exported snapshot generation and boundary.
- Version snapshot rows by their generation boundary so newer WAL always wins.
- Verify keyed counts and deterministic checksums after capture catches up.
- Provide a full re-backfill path for incompatible schema changes, new tables, slot loss, or expired replay history.

A restarted backfill guarantees eventual convergence, not one globally atomic point-in-time snapshot across multiple snapshot generations.

### R4 — ClickHouse materialization

- Consume the journal independently from capture.
- Use explicit primary/order key, source version, event ID, and tombstone columns.
- Make retries and replay idempotent.
- Preserve TOAST values marked unchanged; never replace them with null/placeholders.
- Represent hard deletes with versioned tombstones.
- Provide a canonical current-state query/view that does not require users to guess merge timing.
- Document raw-table, background-merge, and `FINAL` behavior/cost.
- Support compatible column additions.
- Block and require re-backfill for incompatible type/key/replica-identity changes.
- Advance checkpoints only after destination batches are durably accepted.

No cross-table or cross-destination atomicity is promised.

### R5 — Archive materialization

- Support scheduled JSONL and Parquet through one archive materializer.
- Use deterministic segment names based on destination and journal range.
- Write to a temporary file, flush/fsync where supported, and atomically rename.
- Store segment hash, journal range, event count, schema/version, and timestamp in a manifest.
- Advance the checkpoint only after durable publication.
- After a crash between rename and checkpoint, validate/adopt the deterministic existing segment instead of duplicating it.
- Archive the CDC changelog and document reconstruction using source version and tombstones.

### R6 — Independent replay and retention

- Maintain separate checkpoint, retry, state, and lag for each destination.
- Allow one destination to stop while capture and the other destination continue.
- Bootstrap a newly added destination from the earliest retained event.
- Reject requests outside retained history and direct the operator to re-backfill/re-seed.
- Never garbage-collect events required by an active destination.
- Retain consumed events for the configured replay window while the disk budget permits.
- At hard pressure, stop backfill first and surface required operator action; never silently discard unconsumed events.

### R7 — Source safety

Monitor:

- retained WAL bytes and slot lag;
- `wal_status`, `safe_wal_size`, slot activity, and invalidation;
- source free disk where available;
- journal bytes and local free disk;
- last received, durable, and acknowledged source positions;
- capture and destination lag;
- backfill throughput, source connections, progress, and ETA.

Controls:

- warning, action, and critical thresholds;
- throttle/stop backfills before stopping capture;
- require a finite Postgres slot-WAL safety boundary unless explicitly overridden for local experiments;
- never automatically delete a slot or fake acknowledgement;
- expose critical states with recovery instructions;
- provide explicit destination detach, replay, re-seed, and slot recreation workflows.

“Safe stop” means the connector stops accepting work without lying about durability. It cannot make Postgres retain WAL forever. A finite Postgres WAL cap may invalidate the slot and force a re-seed.

## 5. Event envelope

A versioned logical event should contain:

```text
Event {
  envelope_version
  event_id
  journal_seq

  source {
    system_identifier
    database
    schema
    table
    relation_id
    slot
  }

  transaction {
    xid
    commit_lsn
    end_lsn
    ordinal
  }

  operation       // snapshot | insert | update | delete
  key
  before_key
  after_or_patch
  unchanged_toast_columns

  source_version {
    lsn
    rank           // snapshot < WAL at an equal boundary
  }

  schema {
    fingerprint
    columns_and_postgres_types
  }

  snapshot {
    run_id
    generation
    chunk_id
    boundary_lsn
  }

  captured_at
}
```

Rules:

- WAL `event_id` is deterministic from source identity, slot, commit/end LSN, and transaction ordinal.
- Snapshot IDs are deterministic from run, generation, table, chunk, and primary key.
- `journal_seq` controls local consumption, never conflict resolution.
- `source_version` controls destination convergence.
- `before` is only as complete as replica identity permits.
- Values preserve precision; unsupported values stay explicitly typed/opaque or fail preflight—never silently coerce.
- Begin/commit metadata is persisted, but destination-wide transactional application is not guaranteed.

## 6. SQLite model

Minimum logical tables:

- `journal_events`
- `source_transactions`
- `source_state`
- `relation_schemas`
- `destinations`
- `destination_checkpoints`
- `backfill_runs`
- `backfill_generations`
- `backfill_chunks`
- `archive_segments`
- `safety_state`
- `alerts`
- `schema_migrations`

Important constraints:

- unique source/slot WAL transaction end LSN;
- unique event ID;
- monotonic journal sequence;
- destination checkpoint references a committed journal sequence;
- source durable LSN changes in the same transaction as its events;
- backfill chunk completion and snapshot event insertion are atomic;
- migrations are versioned and covered by upgrade tests.

Represent LSNs as ordered eight-byte values or canonical hexadecimal strings; do not assume every value safely fits signed SQLite arithmetic.

## 7. Correctness invariants

1. **No acknowledgement before durability:** Postgres `flush_lsn` never exceeds SQLite’s durable source LSN.
2. **Whole source transaction:** a committed Postgres transaction is absent or fully present in the durable journal.
3. **Deterministic replay:** replay near an acknowledgement boundary can attempt duplicates but cannot create distinct logical event IDs.
4. **Destination independence:** destination checkpoints never change source acknowledgement.
5. **Checkpoint after side effect:** a destination checkpoint advances only after a durable or deterministically discoverable side effect.
6. **No GC past consumers:** retention never removes events above an active destination checkpoint.
7. **Bounded bootstrap:** a destination avoids Postgres rereads only when its start lies inside retained history.
8. **Snapshot/WAL ordering:** a snapshot row cannot overwrite a newer WAL mutation.
9. **Explicit loss boundary:** slot invalidation, explicit destination detachment, or expired replay transitions to `requires_reseed`, never fake health.

## 8. CLI

Proposed commands:

```text
boring-cdc check
boring-cdc init
boring-cdc run
boring-cdc status [--json]

boring-cdc backfill start|pause|resume|status
boring-cdc backfill restart --confirm

boring-cdc destination list|add|pause|resume
boring-cdc destination detach --confirm

boring-cdc replay DESTINATION --from-seq/--since
boring-cdc journal inspect
boring-cdc journal gc --dry-run

boring-cdc recover inspect
boring-cdc recover recreate-slot --confirm-data-gap
boring-cdc recover reseed --confirm
```

`check` is read-only and reports Postgres configuration/privileges, publication and slot state, keys/replica identities, type/ClickHouse compatibility, finite WAL safety boundary, local budget/replay settings, and schema drift.

Mutating commands print intended changes and require confirmation whenever they can introduce a data gap.

## 9. Configuration and observability

Use one documented TOML file with environment-variable secret overrides for source, slot/publication, table allowlist, journal budget/replay window, WAL thresholds, backfill controls, ClickHouse, archive, and metrics listener.

Never put passwords in example configuration or logs.

Expose human-readable status, `--json`, structured logs, and preferably Prometheus-compatible metrics. Payload values are excluded from logs by default.

Required states:

```text
healthy
degraded
backfill_throttled
destination_blocked
capture_safe_stopped
slot_invalid_requires_reseed
schema_blocked
```

## 10. Benchmark harness

Check in:

- deterministic e-commerce generator;
- customers, products, orders, and order-items schema;
- fixed random seed and named scale profiles;
- concurrent transactional writer and application reader;
- mutation ledger with expected event IDs;
- keyed count/checksum oracle;
- fault-injection scripts;
- standard result JSON schema;
- Boring CDC, Debezium, and Estuary runbooks.

Common measurements:

- source CPU, I/O, connections, free disk, retained WAL;
- application p50/p95/p99 latency;
- capture/destination lag;
- journal/local storage;
- loss and duplicate attempts;
- convergence/recovery time and operator steps;
- backfill duration and lock impact;
- ClickHouse query cost, merge backlog, and storage amplification;
- deployment/resource cost with assumptions.

Provider comparisons use externally visible behavior and the common oracle, not LSN equality.

Do not claim 10,000-table or terabyte-scale support without running and publishing those profiles.

## 11. Required test matrix

| Scenario | Required assertion |
|---|---|
| Insert/update/delete | Event order and final state match source |
| Multi-table transaction | Journal is all-or-nothing; no destination-atomicity claim |
| Crash before SQLite commit | Transaction replays; journal has no partial transaction |
| Crash after SQLite commit, before PG ack | Duplicate attempt resolves to one logical event |
| Crash after destination write, before checkpoint | Retry converges without logical duplication |
| Destination offline | Capture continues within bounds; other destination progresses |
| Network loss | Reconnect resumes from durable position without loss |
| Journal pressure | Backfill throttles/stops first; remediation is visible |
| Slot invalidation | Connector blocks and requires explicit re-seed |
| Backfill with concurrent writes | Final keyed checksum matches source |
| Crash during backfill | Completed chunks remain; new generation resumes remaining work |
| Update/delete during chunk copy | Newer WAL version wins |
| Compatible added column | Materialization continues after schema update |
| Incompatible type change | Table blocks with recovery instructions |
| Removed replica identity | Unsafe update/delete handling blocks |
| Added table | Explicit publication update and backfill are required |
| Unchanged TOAST column | Existing downstream value is preserved |
| ClickHouse update/delete load | Canonical current-state query remains correct |
| Query before/after merge and `FINAL` | Behavior and cost are documented |
| Add second destination | Bootstrap uses retained journal, not Postgres |
| Destination beyond retention | Replay rejects and points to re-seed |
| Archive crash around rename/checkpoint | Existing segment is adopted, not duplicated |
| Process/journal restart | Migrations and persisted states recover cleanly |

## 12. Milestones

### M0 — Repository and contract

- Rust single-binary scaffold.
- Owner-approved public license.
- README, requirements, architecture decisions, security/contribution guidance.
- Docker Compose for Postgres, ClickHouse, and connector.
- Configuration schema and CI for format, lint, tests, and dependency audit.
- Requirements traceability.

**Exit:** clean clone builds; Compose validates; no secrets; license decision is recorded.

### M1 — Workload and `pgoutput` foundation

- E-commerce workload and mutation ledger.
- Postgres preflight.
- Publication/slot initialization.
- Relation/transaction decoding.
- Raw event inspection demo.

**Exit:** fixed-seed inserts, updates, deletes, and transaction boundaries reproduce; unsupported tables fail clearly.

### M2 — Durable journal and first sink

- SQLite schema/migrations.
- Durable source transaction/ack invariant.
- Deterministic event IDs.
- JSONL archive as first materializer.
- Crash injection at commit/ack/checkpoint boundaries.
- Status CLI.

**Exit:** no missing logical events; duplicates converge; acknowledgement never outruns SQLite durability.

### M3 — Backfill and stitching

- Exported snapshot/boundary.
- Chunk planner/throttling.
- Persisted progress/restart generations.
- Source-version rules.
- Backfill CLI and checksums.
- Naive full-read benchmark control.

**Exit:** interrupted backfill with concurrent mutations converges; completed chunks are retained; source-impact metrics publish.

### M4 — ClickHouse correctness

- Versioned/tombstoned model and current-state view.
- Idempotent replay.
- Schema checks and re-backfill path.
- TOAST preservation.
- Merge/query/storage benchmark.

**Exit:** update/delete-heavy scenarios converge; documented queries are correct before and after merges.

### M5 — Parquet and fan-out

- Parquet serializer/schema metadata.
- Deterministic archive segments.
- Independent checkpoints and schedules.
- Late destination bootstrap.
- Retention/replay commands.

**Exit:** either destination can stop independently; bootstrap succeeds within retention; expired starts require re-seed.

### M6 — Source-safety and recovery hardening

- Safety state machine and thresholds.
- WAL/free-disk monitoring.
- Journal budget controls.
- Slot invalidation/recovery.
- Schema drift/source identity checks.
- Full failure matrix.

**Exit:** every unsafe state is visible with a tested runbook; no command silently creates a gap.

### M7 — Public v0.1 release

- Linux artifact/container image.
- Reproducibility instructions and benchmark data.
- Known limitations and security policy.
- Debezium and Estuary comparison runbooks/results.
- `v0.1.0` tag.

**Exit:** a new user can clone, run Compose, generate workload, capture, backfill, materialize both destinations, inject failures, and verify checksums.

## 13. v0.1 acceptance criteria

- All required fault tests pass repeatedly from a clean environment.
- No expected committed event is missing after recoverable crashes.
- Duplicate attempts are visible but logical identities stay stable.
- ClickHouse state and archive reconstruction converge after quiescence.
- Tests prove Postgres acknowledgement never outruns the journal.
- Interrupted backfills with concurrent mutations converge.
- Destination outages do not couple destination progress or source acknowledgement.
- Bounded local/WAL limits produce explicit degraded, safe-stop, or re-seed states.
- Schema/delete behavior and measured limits are documented.
- Benchmark profiles, seed, workload, oracle, raw results, and assumptions are public.
- At-least-once, single-source, non-HA limitations are prominent.

## 14. Out of scope

- Non-Postgres sources.
- More than one publication/slot/source per process.
- Generic source/sink plugin APIs.
- Kafka, Kafka Connect, or schema registry integration.
- Kubernetes/operator/control plane.
- Distributed HA or automatic source failover.
- Universal DDL migration.
- Unsafe update/delete replication without usable identity.
- Unlimited replay history.
- Global exactly-once semantics.
- Cross-destination or cross-table atomicity.
- Unmeasured 10,000-table/TB-scale claims.

## 15. Decisions to close before implementation

| Decision | Proposed default | Status |
|---|---|---|
| Public owner | `hachej/boring-cdc` | **Resolved** |
| License | Apache-2.0 | Owner approval needed |
| Archive scope | One archive materializer; ship/test JSONL and Parquet | Confirm |
| WAL cap | Require finite `max_slot_wal_keep_size`; explicit local-only unsafe override | Confirm |
| Restarted backfill | Eventual convergence across snapshot generations | Confirm |
| Supported Postgres types | Freeze a concrete v0.1 list before M1 exits | Open |
| TOAST | Patch-aware preservation; reject unsupported cases, never silent placeholders | Confirm |
| Scale profiles | Fix small/medium/large rows and rates before benchmark claims | Open |

## 16. Main risks

- SQLite may bottleneck before decoding; measure rather than claim scale.
- One very large Postgres transaction can exceed local memory/disk budget.
- TOAST reconstruction and ClickHouse pre-merge correctness may expand M4.
- WAL/source free-disk metrics may need additional privileges or collectors.
- Parquet schema evolution is materially harder than JSONL.
- Slot invalidation is necessarily a re-seed boundary.
- Managed Estuary comparisons require account access and manual setup.
