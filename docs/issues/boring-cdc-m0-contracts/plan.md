# Boring CDC M0 Contracts — execution plan

## Outcome

Produce the six decision-independent M0 engineering artifacts on `epic/boring-cdc-m0-contracts`, based on `origin/epic/boring-cdc-m0`, and open one PR titled **[Boring CDC] M0 contract artifacts**. This lane does not decide or close any `boring-cdc-d-*` item and does not touch `boring-cdc-m0`, `boring-cdc-m0-decisions`, `boring-cdc-m0-complete`, or `boring-cdc-m0-gate`.

## Authority and confirmed values

Workers follow `AGENTS.md`, `docs/AGENT_SYSTEM.md`, `docs/REQUIREMENTS.md`, and `docs/PLAN.md` §§16 and 19. Decision dependencies were execution-waived for this lane. On 2026-09-10, the owner accepted every pre-filled RECOMMENDED value in cards `5a994cfd` and `765bd3b2`; the archive and final ClickHouse passes reconciled those literals, removed their provisional markers, and reran aggregate proof. No decision or milestone Bead was modified by this lane.

## Tracked graph

The six existing Beads were labeled additively with `epic:boring-cdc-m0-contracts` because the host initially could not see them. Existing labels, parentage, and dependency edges were preserved. The instruction not to create/delete/relabel or touch the existing M0 epic takes precedence over creating a duplicate lane epic Bead.

| Order | Bead | Deliverable | Proof |
|---|---|---|---|
| 1 | `boring-cdc-m0-scaffold` | planning-approved repository scaffold | Bead acceptance; local build/typecheck/affected tests; exact-SHA sandbox and review |
| 2 | `boring-cdc-m0-event-format` | exact event ABI, schema, validators, vectors | `scripts/validate/m0_artifact.sh boring-cdc-m0-event-format` plus affected suites |
| 3 | `boring-cdc-m0-pg-contract` | PostgreSQL capture/backfill contract | `scripts/validate/m0_artifact.sh boring-cdc-m0-pg-contract` plus affected suites |
| 4 | `boring-cdc-m0-storage-model` | SQLite and disk-budget contract | `scripts/validate/m0_artifact.sh boring-cdc-m0-storage-model` plus affected suites |
| 5 | `boring-cdc-m0-archive-model` | archive format and commit protocol; consumes event format | `scripts/validate/m0_artifact.sh boring-cdc-m0-archive-model` plus affected suites |
| 6 | `boring-cdc-m0-ch-model` | executable ClickHouse model; consumes event format | `scripts/validate/m0_artifact.sh boring-cdc-m0-ch-model` plus affected suites |

`event-format -> {archive-model, ch-model}` is the in-lane dependency. All six consume the already-closed validation-tooling prerequisite. Decision-Bead blockers remain unchanged in the canonical graph and are treated as provisionally satisfied only under the owner's explicit waiver.

## Execution protocol

1. Gate 1 approval.
2. Arm durable supervision at 120 seconds.
3. Dispatch one exact Bead at a time. Although the host permits two Workers, the repository requires one writer per shared worktree and these Beads overlap in `.beads/issues.jsonl`, `contracts/m0/manifest.json`, validators, and evidence indexes. Serial dispatch avoids invalid world states and lost manifest updates.
4. Each Worker verifies the exact target under `--label epic:boring-cdc-m0-contracts`, claims with its session ID (using the owner-authorized blocker waiver only where necessary), changes only its scope, runs build/typecheck/affected tests locally before push, commits, exact-SHA sandbox-tests, obtains adversarial review, pushes only the epic branch, and records a full handoff without closing the Bead.
5. After all six handoffs, run aggregate local proof, validate zero duplicate Bead IDs, flush and commit Beads state before the final verified push, open the epic PR against `epic/boring-cdc-m0`, start an exact-SHA demo if the artifact is runnable, and raise Gate 2. Freeze the green head.

## Risk and rollback

- **Owner-confirmed values:** accepted recommendations from cards `5a994cfd` and `765bd3b2` were reconciled in-lane; aggregate proof checks that no `M0-PROVISIONAL` markers remain.
- **Shared-file contention:** serialize Workers in this one worktree.
- **Cross-lane drift:** never rebase onto or write to M1/M2/parked-M0 worktrees or branches; target only the named base and epic branch.
- **Process defect:** no push until local build, typecheck, and affected tests pass. A green pushed head is frozen.
- **Rollback:** revert the epic's commits on `epic/boring-cdc-m0-contracts`; no milestone/decision Bead is closed by this lane.

## Owner confirmation — 2026-09-10

Owner cards `5a994cfd` and `765bd3b2` were answered with every pre-filled RECOMMENDED value accepted exactly. Their literals are now confirmed, not provisional. The next sole-writer Worker must compare every `// M0-PROVISIONAL` occurrence and each prior handoff inventory to those recommended values, correct any mismatch, remove matching provisional markers, rerun affected proof, and record the confirmation in its handoff. This is an owner-authorized reconciliation pass within this epic, not a new milestone or decision change.

## Planning checks

- `br sync --import-only`: completed.
- `br doctor`: data integrity healthy; warnings are pre-existing open/dead-edge and WAL-sidecar diagnostics.
- `br dep cycles`: no cycles.
- `bv --robot-insights`: completed; confirms the canonical graph is a DAG.
- Fresh-eyes mechanism: not available to this Orchestrator before Gate 1; this limitation is disclosed for owner judgment. Each implementation SHA still requires adversarial fresh review.
