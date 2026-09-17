# Boring CDC M3 Chunked Backfill — execution plan

## Objective

Extend the merged M2 durable journal and JSONL engine so an existing PostgreSQL table can be captured from an exported snapshot in bounded keyset chunks, stitched to concurrent WAL at an exact transactional fence, and retained behind a complete reconstruction anchor.

The correctness core is the published capture-fence row observed through `pgoutput` and durably paired with its journal sequence. Sampled LSNs, timers, and informational `wal_end` are not fence proofs.

## Fixed scope and constraints

- Consume M2 on `master`; do not fork journal, spool, ownership, schema, or failure-policy code.
- Preserve one PostgreSQL source/publication/pgoutput slot, one Rust binary, one SQLite journal, durable-before-feedback, destination-independent capture, at-least-once/idempotent convergence, bounded failure, and no cross-epoch source-version comparisons.
- Stay out of `contracts/clickhouse/`, `docs/CLICKHOUSE_MODEL.md`, and every `m4_*` module.
- Real fault and stitching evidence uses the pinned PostgreSQL 17.6 Compose image; never a simulated transcript.
- Before every push: merge `origin/master`, flush Beads, prove zero duplicate IDs, and pass the six owner-mandated local gates.
- A green pushed head is frozen: post the receipt and stop changing it.

## Dependency-ordered slices

1. `boring-cdc-m3-bootstrap` — permanent-slot exported-snapshot bootstrap.
2. `boring-cdc-m3-planner` — persisted keyset ranges and bounded workers.
3. `boring-cdc-m3-fence` — transactional fence proof and complete anchors.
4. `boring-cdc-m3-restart` — existing-slot/restart-generation stitching.
5. `boring-cdc-m3-schema-guard` — snapshot schema binding and DDL guard lifecycle.
6. `boring-cdc-m3-oracle` — exact mutation oracle, watermark, and naive control.
7. `boring-cdc-m3-controls` — pause/resume and source-impact controls.
8. `boring-cdc-m3-reseed` — complete expanded-table reseed anchor.
9. `boring-cdc-m3-faults` — real interruption and stitching matrix.
10. `boring-cdc-m3-complete` — terminal completion receipt.

The graph is deliberately serial in this owner-specified order so each worker consumes one stable predecessor and the fence core cannot be bypassed.

## Proof and delivery

Each slice records an exact commit SHA, local proof, exact-SHA sandbox proof, adversarial review provenance, pushed branch, and complete Bead handoff. The terminal slice verifies all handoffs and all six push gates, opens the M3 PR to `master`, and posts `factory: MERGE-READY <40-char SHA>`. Merge remains owner-only.

## Plan review

The owner directed immediate use of the already-authored Bead contracts and `docs/PLAN.md`, with no replanning round. No independent fresh-eyes plan-review mechanism is available to this Orchestrator; Gate 1 is therefore raised with the existing canonical contracts and this bounded execution projection.
