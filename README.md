# Boring CDC

A deliberately small Postgres change-data-capture system built to test one claim:

> Can one Rust binary capture Postgres once, keep a durable local record, and replay independently to different destinations without turning analytics into a production risk?

Boring CDC is an educational, benchmark-driven project. It is **not** “Debezium in Rust” and does not claim global exactly-once delivery.

## v0.1 target

```text
Postgres (pgoutput)
        |
        v
append-only local journal + SQLite checkpoints
        |                         |
        v                         v
ClickHouse materializer     JSONL/Parquet archive
```

The intended guarantee is **at-least-once capture with idempotent destination convergence**. A crash may replay events; after recovery, each destination must converge to source-equivalent state.

## Project documents

- [Exact v0.1 requirements](docs/REQUIREMENTS.md)
- [Implementation plan](docs/PLAN.md)

## Status

Planning. No production-ready connector exists yet.

## Scope guardrails

- Postgres only
- one publication and one logical replication slot
- one Rust binary plus Docker Compose
- no Kafka, Kubernetes, schema registry, or generic connector framework
- two destinations: ClickHouse and scheduled JSONL/Parquet
- bounded local durability and replay, not unlimited retention

## Comparisons

The accompanying experiment compares externally visible behavior with Estuary as a managed reference implementation. Debezium may be used to explain capture and offset design. This repository is not a vendor leaderboard.
