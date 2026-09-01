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
- [Architecture and implementation roadmap](docs/PLAN.md)
- [Agent-native control and knowledge architecture](docs/AGENT_SYSTEM.md)

## For implementation agents

Start with [`AGENTS.md`](AGENTS.md), then inspect the current Git/Beads state and claim one ready Bead. Routine work should begin from the complete claimed Bead and a bounded set of owned contracts—not by loading the entire plan or tracker snapshot. Generated context, impact, evidence, and handoff views are projections with source digests; they never become independent authority.

Until the planned `scripts/agent/` interface exists, the first commands are:

```bash
git status --short --branch
br sync --status && br doctor
br ready
```

## Status

Planning and M0 contract work. No production-ready connector exists yet. `br ready` is the authoritative current-work view; this summary must not be used as a readiness gate.

## Scope guardrails

- Postgres only
- one publication and one logical replication slot
- one Rust binary plus Docker Compose
- no Kafka, Kubernetes, schema registry, or generic connector framework
- two destinations: ClickHouse and scheduled JSONL/Parquet
- bounded local durability and replay, not unlimited retention

## Comparisons

The accompanying experiment compares externally visible behavior with Estuary as a managed reference implementation. Debezium may be used to explain capture and offset design. This repository is not a vendor leaderboard.
