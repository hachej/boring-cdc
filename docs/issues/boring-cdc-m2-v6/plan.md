# [Boring CDC M2 Durable Journal] Relaunch-v6 execution plan

## Objective
Finish M2 on `epic/boring-cdc-m2` without redoing retained implementation. Preserve durable-before-feedback: every complete source transaction and end LSN commits atomically to SQLite before PostgreSQL feedback, so crashes replay rather than lose data.

## Admitted durable state
The branch head `d6625e0` retains the journal, ownership, schema, spool, CopyBoth runtime, deterministic JSONL engine, heartbeat, leases, pressure controls, reconciliation, init/recovery, and PostgreSQL 17.6 fault/status evidence. The v6 graph records every named slice as an admission-only closed node linked to its historical Bead and immutable certification `d033cd2`; no retained source work is reclaimed or reimplemented.

## Remaining slice
`boring-cdc-u0w4.10` — merge current `origin/master` (`af4c285a2`) into the epic branch, resolve only integration fallout, materialize the owner-accepted M0 literals exactly, remove matching provisional markers, and recertify M2 at the resulting head. It is the only ready unassigned v6 Bead.

## Dependency graph
Epic `boring-cdc-u0w4` contains all named slices. The admitted dependency chain is capture runtime → JSONL → heartbeat/leases/pressure/reconcile/init-recovery → fault/status → completion. Current-base integration depends on completion and is the terminal epic blocker. `br dep cycles` reports no active cycles; `bv --robot-insights` was run.

## Worker contract and proof
The Worker verifies and claims exact Bead `boring-cdc-u0w4.10` under `epic:boring-cdc-m2-v6`, reads retained code/handoffs, and does not redo closed work. Before every push: `TMPDIR=/var/tmp`, format check, `cargo clippy --locked --all-targets`, locked workspace/all-target tests, affected validators including secrets validation, duplicate Bead IDs = 0, and `br sync --flush-only`. Then exact-SHA sandbox proof, adversarial fresh review, push, and a complete handoff with SHA/proof/review provenance. Real crash evidence remains from pinned PostgreSQL 17.6 Compose only; invalidated evidence is rerun, never synthesized.

## Constraints, risk, rollback
One source, publication, pgoutput slot, Rust binary, and SQLite journal; destination-independent capture; at-least-once/idempotent convergence; explicit bounded failure; no cross-epoch source-version comparison. No M3 backfill or destination beyond JSONL. Primary risk is base-integration drift invalidating durability or proof assumptions. Fail closed and rerun affected proof. Rollback is reverting the integration commit before owner approval. Once a pushed head is green, freeze code, open/update the PR to `epic/boring-cdc-m0`, and raise Gate 2 at the exact SHA; the agent never merges.

## Review record
This is an admission delta over the previously reviewed plan and immutable historical handoffs, not a redesign. No host-provided independent plan-review command is available to this Orchestrator; that limitation is disclosed at Gate 1. Structural checks are the retained review record, immutable certification, `br dep cycles`, and `bv --robot-insights`.
