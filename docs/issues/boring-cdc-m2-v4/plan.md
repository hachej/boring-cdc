# [Boring CDC M2 Durable Journal] Relaunch-v4 execution plan

## Objective
Finish M2 on `epic/boring-cdc-m2` without redoing the retained implementation. The branch already contains the durable journal, ownership, schema, spool, CopyBoth runtime, JSONL engine, heartbeat, leases, pressure, reconciliation, init/recovery, and real PostgreSQL 17.6 fault/status evidence. The invariant remains durable-before-feedback: SQLite commits each complete source transaction and end LSN before PostgreSQL acknowledgement.

## Durable state admitted from the prior lane
The prior replacement graph and its follow-up chains are retained under their historical `epic:boring-cdc-m2-v2` label because their claimed Worker sessions are durable provenance: additive relabeling caused the host to reject the graph with an ownership-conflict error. Their immutable handoffs are content-addressed by the completion certificate at `d033cd2`; the current pushed head is `d6625e0`. Closed foundation Beads (`m2.1`, schema, journal, ownership and successors) remain complete. The v4 graph contains the replacement epic and one terminal integration slice, both labeled `epic:boring-cdc-m2-v4`; every named implementation slice is linked through the immutable certificate rather than relabeled or reimplemented.

## Remaining execution slice
`boring-cdc-boring-cdc-m2-v2-jsq.2` — merge current `origin/master` (`af4c285a2` or newer approved base) into the epic branch, resolve only integration fallout, materialize accepted M0 literals/remove matching provisional markers, and regenerate M2 certification at the merged head. This slice blocks the epic and is the only unassigned ready Bead.

## Dependency shape
The existing dependency-correct historical graph is preserved: retained foundations → spool/capture → JSONL → heartbeat/leases/pressure/reconcile/init-recovery → completion/fault evidence → immutable certification. The v4 base-integration slice is a terminal blocker of the replacement epic and consumes that certificate. `br dep cycles` reports no active cycle; `bv --robot-insights` was run.

## Worker contract and proof
The Worker must verify/claim the exact ready Bead under `epic:boring-cdc-m2-v4`, preserve the prior host context, merge the current base, and never redo closed work. Before every push it must locally run locked all-target Clippy, workspace tests, affected validators, secrets validation, duplicate-ID check, and `br sync --flush-only`; then exact-SHA sandbox-test, obtain adversarial fresh review, push, and post a complete handoff with SHA/proof/review provenance. Real crash evidence must remain derived from PostgreSQL 17.6 Compose; invalidated evidence is rerun, never synthesized. `TMPDIR=/var/tmp`; no credentials in commands or artifacts.

## Risk and rollback
Primary risk is integration drift invalidating durability, literal, or evidence assumptions. Fail closed, rerun invalidated proof, and do not broaden into M3 or non-JSONL destinations. Rollback is reverting the integration commit before owner merge approval. On a green pushed head, freeze code, open/update the PR to `epic/boring-cdc-m0`, start the exact-SHA demo, and raise Gate 2; the agent never merges.

## Review record
This is a relaunch/admission delta over the prior reviewed plan and immutable handoffs, not a redesign. No separate tier-1 independent plan-review mechanism is exposed to this Orchestrator before Gate 1. Structural review consists of the retained reviewed plan, content-addressed completion certificate, `br dep cycles`, and `bv --robot-insights`; this limitation is disclosed for the owner's Gate 1 decision.
