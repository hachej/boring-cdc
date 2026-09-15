# Boring CDC M0 Completion — execution plan

## Outcome

Finish the relaunched M0 lane on `epic/boring-cdc-m0` without reimplementing accepted work: accept or narrowly finish the six existing contract artifacts and handoffs, correct the Article 1 sequencing row, then wait for the existing owner decision card before running the decision, completion, and terminal gate slices. Open one PR to `master`, prove its exact SHA, freeze the green head, and ask the owner to merge.

## Method and authority

Use a dependency-aware Beads graph under `epic:boring-cdc-m0-v2`. Workers first inspect the existing artifacts and durable handoffs on the integrated base `d1eb211`; they repair only demonstrated remainder. The seven literals blocked on owner card `59a63169` are never self-approved. Where a contract needs a blocked literal, use that card's recommended value with `// M0-PROVISIONAL: <bead id>` and record it. `d-pg-protocol` and its `ARTICLE1-PROVISIONAL` candidates remain open and consistent.

No independent fresh-eyes planning mechanism is available to this Orchestrator before Gate 1. The owner can judge this limitation at the gate. Every implementation SHA still requires adversarial exact-SHA review.

## Slices and dependencies

1. Six contract-finish slices inspect the integrated code plus prior handoffs for PostgreSQL, storage, archive, ClickHouse, event format, and scaffold. Event format blocks archive and ClickHouse because both consume its ABI. The other artifacts are graph-independent, but dispatch remains serial because the repository permits one writer in the shared worktree.
2. The Article 1 sequencing correction starts after all six contract slices. It changes only `docs/SERIES_EXECUTION.md`: Article 1 evidence is raw `pgoutput` plus the same-stream non-durable current-state teaching view already shipped in PR #3/evidence/article1. ClickHouse, durability, checkpointing, and exactly-once remain Article 4 evidence.
3. The M0 decisions barrier follows that correction and remains blocked on `d-values`, `d-values.1`, `d-keys`, `d-wal-cap`, `d-sqlite`, `d-archive-durability`, `d-compose`, and separately in-progress `d-pg-protocol`.
4. M0 completion follows the contracts, documentation correction, and decision barrier.
5. The terminal gate follows completion, runs `scripts/validate/close_guard.sh boring-cdc-m0`, and stores its JSON.

## Worker protocol

For the exact dispatched Bead, verify readiness under `br ready --label epic:boring-cdc-m0-v2 --unassigned`, claim with the Worker session ID, inspect prior code/handoffs before editing, stage only intended files, and commit on `epic/boring-cdc-m0`. Before every push run locally with `TMPDIR=/var/tmp`: `cargo clippy --locked --all-targets`, required tests, affected validators, `br sync --flush-only`, and duplicate-ID validation. Then exact-SHA sandbox-test, obtain adversarial fresh review, push, and record a complete Bead handoff with SHA/proof/review provenance. Workers never merge or close their own Bead.

At a 2-dispatch or 4-review cap, create one child scoped only to the remainder; never retry or raise a cap card. No credentials may enter DSNs, code, docs, or evidence. Never touch the M2 worktree or branch.

## Proof, risk, rollback

- Per slice: required local locked Rust checks, targeted tests and affected validators; clean intended diff; duplicate Bead IDs = 0; exact-SHA sandbox and fresh review; pushed handoff.
- Aggregate: all six contract validators, Article 1 evidence/sequence consistency, decisions and M0 completion validation, then stored close-guard JSON.
- Final: epic PR head equals the reviewed and demoed SHA (apart from explicitly cited docs-only presentation commits), with a live demo or exact sandbox fallback error.
- Main risk is accidentally converting provisional recommendations into owner-approved contracts. Guards are explicit markers, untouched decision states, and the existing card as sole authority.
- Rollback is commit reversion on the epic branch. Once the head is green and receipted, code freeze applies.
