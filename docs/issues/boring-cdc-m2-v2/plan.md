# [Boring CDC M2 Durable Journal] Relaunch plan

## Objective
Ship M2 from `epic/boring-cdc-m2` to `epic/boring-cdc-m0` without redoing the retained journal, ownership, schema, failure-policy, or partial spool work already merged at `180845a`. The invariant is durable-before-feedback: a complete source transaction and end LSN commit atomically to SQLite before PostgreSQL receives acknowledgement.

## Authority and boundaries
- Owner relaunch direction dated 2026-09-15 and repository authorities in `AGENTS.md`, `docs/PLAN.md`, `docs/REQUIREMENTS.md`, and approved `contracts/`.
- Consume `src/article1_capture.rs` and `pg_walstream` 0.8.1; keep `ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol` literals consistent and do not self-approve them.
- Use recommended unresolved SQLite, WAL-cap, and archive-durability literals with `// M0-PROVISIONAL: <bead id>` and inventory them at handoff.
- No M3 backfill and no destination beyond the M2 JSONL archive engine.
- Real crash proof uses the pinned PostgreSQL 17.6 Compose image, `PGPASSWORD` from the Compose secret file, and `TMPDIR=/var/tmp`.

## Retained work
Closed Beads `m2.1`, `m2.1.1`, `m2-schema`, `m2-schema.1`, `m2-journal`, `m2-journal.1`, `m2-ownership`, `m2-ownership.1`, and `m2-ownership.1.1` remain historical prerequisites and are not relabeled or reimplemented. The capped spool parent remains untouched; its one existing remainder child carries the unfinished work.

## Replacement graph
Epic: `boring-cdc-boring-cdc-m2-v2-jsq` (`epic:boring-cdc-m2-v2`). Active slices:

1. `boring-cdc-m2-spool.1` — finish the capped spool remainder.
2. `boring-cdc-m2-capture-runtime` — Article-1 CopyBoth receive path through spool, journal, feedback, keepalives, reconciliation, restart/takeover, and shutdown.
3. `boring-cdc-m2-jsonl` — deterministic crash-safe JSONL directory commit engine.
4. Parallel after JSONL: `boring-cdc-m2-heartbeat`, `boring-cdc-m2-leases`, `boring-cdc-m2-pressure`, `boring-cdc-m2-reconcile`, `boring-cdc-m2-init-recovery`.
5. `boring-cdc-m2-fault-status` — crash hooks, status JSON, and real milestone crash proof.
6. `boring-cdc-m2-complete` — completion receipt; then the replacement epic.

The graph is acyclic. `bv --robot-insights` identifies `m2-spool.1` as the immediate critical-path unlock and JSONL as the later parallelization cut.

## Worker contract
Every Worker verifies the exact ready Bead under `epic:boring-cdc-m2-v2`, claims with its session id, reads retained code and handoffs first, stages only intended files, commits on `epic/boring-cdc-m2`, and before each push locally runs `cargo clippy --locked --all-targets`, relevant tests and validators, duplicate-ID check, and Beads flush. It then exact-SHA sandbox-tests, obtains adversarial fresh review, pushes, and records a complete handoff with SHA, proof, review provenance, and provisional-literal inventory. Workers never merge or close their own Bead.

## Proof and terminal behavior
- Per slice: targeted tests plus `cargo clippy --locked --all-targets`, affected validators, exact-SHA sandbox receipt, adversarial review, and pushed handoff.
- Graph/ledger: duplicate Bead IDs = 0, `br dep cycles`, `bv --robot-insights`, and `br sync --flush-only`.
- Terminal: real abrupt and graceful restart/takeover scenarios against PostgreSQL 17.6; durable SQLite position never trails feedback; JSONL promotion is deterministic and crash-safe; status output is redacted; bounded pressure/retry/lease failures are explicit.
- On a green terminal head: code freeze, open/update one PR to `epic/boring-cdc-m0`, start an exact-SHA live demo, and raise one owner merge gate. The orchestrator never merges.

## Risks and rollback
Primary risks are acknowledgement outrunning durability, replay gaps, stale ownership, reserve exhaustion, and premature JSONL visibility. Fail closed, persist bounded state transitions, fence side effects, and prove transaction boundaries under real crashes. Rollback is reverting the M2 commits before owner merge approval.

## Plan review record
No independent fresh-eyes/adversarial plan-review mechanism is exposed to this Orchestrator before Gate 1; `dispatch_worker` is implementation dispatch and is prohibited before approval. This limitation is disclosed for the owner’s decision at Gate 1. `br dep cycles` and `bv --robot-insights` were run as structural review; both report an acyclic graph.
