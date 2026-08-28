# Boring CDC v0.1 requirements

These requirements were extracted from the Boring CDC article series and its Postgres CDC market-research brief. Where the notes differ, the narrower series specification wins.

## 1. Product contract

Boring CDC must capture changes from one Postgres source into a durable local journal, then materialize that journal independently to two destinations. It must prioritize source safety, reproducible recovery, and destination correctness over feature breadth.

The delivery contract is:

- **At-least-once capture:** crashes may replay events.
- **Durable-before-ack:** Postgres progress is acknowledged only after the corresponding event and source checkpoint are committed locally.
- **Independent replay:** each destination has its own checkpoint, retry state, and lag.
- **Idempotent convergence:** replayed events must not prevent a destination from converging to the expected source state.
- **No global exactly-once claim.**

## 2. Required topology and deployment

- One Postgres source using logical replication and `pgoutput`.
- One publication and one replication slot.
- One Rust binary containing capture, journal, materializers, and operational CLI commands.
- Docker Compose for a reproducible local source, connector, and ClickHouse environment.
- No required Kafka, Kafka Connect, Kubernetes, schema registry, or generic connector/plugin framework.
- An append-only event journal for payloads and SQLite for metadata/checkpoints.
- Two destinations:
  1. ClickHouse for mutable serving/analytics state.
  2. A scheduled JSONL/Parquet archive.

## 3. Capture and event semantics

The capture path must:

- Decode `pgoutput` inserts, updates, and deletes.
- Preserve transaction boundaries and source ordering information sufficient for deterministic replay.
- Record source relation/schema metadata required to interpret row events.
- Track LSN progress durably.
- Acknowledge Postgres only after the journal append and source checkpoint commit atomically or recoverably complete.
- Resume after process or network failure without silent loss.
- Tolerate duplicate delivery after ambiguous crashes.
- Surface unsupported payload/schema cases rather than silently corrupting data.

The exact public event-envelope format and transaction batching policy remain design decisions for the implementation plan.

## 4. Durable journal and retention

The local middle layer must:

- Be append-only for captured events.
- Commit events and source checkpoint consistently.
- Support ordered replay from a stable journal position.
- Store independent destination checkpoints.
- Detect journal corruption and unavailable replay positions.
- Enforce an explicit disk budget and replay-retention window.
- Expose local bytes used, oldest/newest replay position, and per-destination retained backlog.
- Enter a documented safe-stop/fail-loud state before unbounded buffering can endanger Postgres.
- Define what happens when a destination falls behind the retained replay window.

## 5. Initial snapshot/backfill

The backfill must:

- Create an exported snapshot and record its boundary LSN before historical copying.
- Copy tables in chunks with configurable throttling.
- Continue or coordinate WAL capture so snapshot rows and concurrent changes can be stitched without gaps.
- Persist table/chunk progress.
- Define safe pause, interruption, and restart behavior.
- Handle updates and deletes to rows while their snapshot chunks are copied.
- Produce final keyed counts/checksums against the correctness oracle.
- Expose progress, ETA, source CPU/I/O/connections, WAL growth, and replication lag.

A naive full-table read must exist only as a benchmark control, not the production backfill strategy.

## 6. ClickHouse materializer

The ClickHouse destination must provide:

- Explicit versioning and a documented ordering/sort key.
- A documented tombstone or hard-delete strategy.
- Idempotent replay/upsert behavior.
- Correctness rules for queries before, during, and after background merges.
- A documented distinction between normal queries and `FINAL` where relevant.
- Basic schema-compatibility checks.
- A tested re-backfill path for incompatible schema changes or newly selected tables.
- Independent checkpointing, retries, lag reporting, and restart recovery.

It must be tested under update-heavy and delete-heavy workloads and report query cost, merge backlog, and storage amplification.

## 7. JSONL/Parquet archive materializer

The archive destination must:

- Read from the same journal, never create a second Postgres extraction path.
- Run on a declared schedule.
- Write deterministic, replay-safe archive output with a documented partition/file-commit policy.
- Keep its own checkpoint, retries, and lag.
- Bootstrap from retained journal history when added after capture starts.
- Fail with an explicit recovery path when the requested bootstrap position is outside the replay window.

Whether v0.1 ships both JSONL and Parquet modes or JSONL first followed by Parquet is an implementation sequencing decision; the completed v0.1 scope requires the documented JSONL/Parquet archive capability from the series.

## 8. Schema and delete safety

Before capture/materialization, the system must preflight:

- primary/unique keys and Postgres replica identity;
- selected table and column types;
- source-to-destination schema compatibility;
- delete support and destination-specific delete semantics.

It must detect and report at least these exercised changes:

- add a column;
- change a column type;
- remove usable replica identity;
- add a table.

Universal online DDL migration is not required. Unsupported changes must stop or quarantine affected work with a documented re-backfill/recovery path rather than silently diverge.

Large/TOASTed values and reconstruction of partial update records are identified edge cases. Their exact supported-size boundary and reconstruction strategy must be specified before v0.1 is called complete.

## 9. Source-safety and operations

The CLI/status surface must provide at least:

- status;
- capture/run;
- backfill start/status/pause/resume;
- destination replay/bootstrap;
- recovery/re-seed guidance or commands.

Operational telemetry must include:

- retained WAL and slot lag;
- `wal_status` and `safe_wal_size` when available;
- source free disk/headroom;
- connector/journal disk use;
- capture and destination lag;
- destination retry/failure state;
- backfill progress and ETA;
- source CPU/I/O/connections and application-latency measurements in the benchmark harness.

Alerts or failing health states must represent source risk, not merely process liveness. The system must publish explicit procedures for pause, slot loss/invalidation, re-seed, journal-window exhaustion, and network/destination outages.

## 10. Benchmark and correctness harness

The repository must include a reproducible e-commerce workload with customers, products, orders, and order items:

- fixed seed;
- published scale profiles;
- enough history to exercise backfills;
- continuous inserts, updates, and deletes;
- transactional writes and concurrent application reads during tests.

The correctness oracle must use provider-neutral event IDs plus keyed row counts/checksums; it must not compare provider-specific LSNs.

Every relevant scenario must measure:

- source CPU and I/O;
- application latency;
- WAL growth and slot lag;
- connector/destination lag;
- event loss and duplicates;
- recovery time and required operator steps;
- final destination correctness;
- relevant storage/cost metrics.

Required failure scenarios:

1. crash before and after journal commit;
2. crash before and after Postgres LSN acknowledgement;
3. crash after destination write but before destination checkpoint;
4. network interruption;
5. destination outage while Postgres continues changing;
6. interrupted snapshot;
7. updates/deletes during snapshot copying;
8. update-heavy and delete-heavy ClickHouse workloads;
9. queries around ClickHouse merges;
10. supported and unsupported schema changes;
11. one destination stopped while the other continues;
12. second destination added after capture starts;
13. destination falling outside journal retention;
14. WAL/slot risk and slot invalidation recovery.

The same externally visible outage/recovery scenarios should be run against Estuary where practical. Debezium is a capture/offset design reference only.

## 11. Capacity boundaries

v0.1 must declare and test bounded scale profiles rather than imply unlimited scale. The research notes explicitly raise 10,000 tables and terabyte-scale backfills as questions; they are benchmark questions, not accepted v0.1 guarantees unless later quantified and adopted.

The release documentation must state tested limits for:

- selected tables;
- transactions/events per second;
- row/event size, including large TOASTed values;
- backfill size;
- journal disk budget and replay duration;
- supported outage duration under the tested workload.

## 12. Out of scope

- Sources other than Postgres.
- Multiple source slots/publications or distributed capture HA.
- Arbitrary sink plugins or a generalized connector framework.
- A distributed control plane.
- Kafka/Kafka Connect as a required dependency.
- Universal DDL migration.
- Unlimited replay history.
- Global exactly-once semantics.
- A broad vendor leaderboard.

## 13. v0.1 release acceptance

v0.1 is complete only when:

- the documented Docker Compose environment starts reproducibly;
- a backfill plus concurrent WAL stream converges correctly in both destinations;
- all required crash/outage scenarios produce no silent loss and end in either verified convergence or an explicit safe failure/recovery state;
- Postgres is never acknowledged ahead of durable local state;
- destinations recover independently without re-reading Postgres while history remains in the journal;
- source-risk and bounded-storage controls are observable and exercised;
- schema/delete limitations and all tested capacity boundaries are published;
- benchmark commands, fixed seeds, raw results, and correctness checks are reproducible.
