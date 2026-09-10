# Boring CDC — epic boring-cdc-m0-contracts (M0 contract artifacts, decision-independent)

## Authority
- Owner (Julien) asked on 2026-09-10 08:53 whether more CDC threads could run. Machine has headroom, so this lane runs in parallel with M1 and M2.
- The existing M0 Orchestrator (session eacfe493, epic `boring-cdc-m0`) is parked because the 25 `boring-cdc-d-*` decision Beads are gated on two unanswered owner cards. It reported "no durable state changed" correctly. YOU DO NOT TOUCH those decision Beads, that worktree, or those cards.
- Repository authorities: `AGENTS.md`, `docs/AGENT_SYSTEM.md`, `docs/PLAN.md` §16 (M0) and §19, `docs/REQUIREMENTS.md`.
- Gate 1 is recorded by the supervising session on the owner's behalf; raise the card as usual and it is approved within minutes.

## Scope — exactly these Beads, nothing else
`boring-cdc-m0-scaffold` (planning-approved repository scaffold), `boring-cdc-m0-pg-contract` (PostgreSQL capture/backfill contract), `boring-cdc-m0-storage-model` (SQLite and disk-budget contract), `boring-cdc-m0-archive-model` (archive format and commit protocol), `boring-cdc-m0-ch-model` (executable ClickHouse model), `boring-cdc-m0-event-format` (exact event format contract).
- These are ENGINEERING artifacts — schemas, contracts, validators, fixtures — not owner decisions. The owner decisions they reference are the `boring-cdc-d-*` Beads, which stay with the other lane.
- Do NOT touch `boring-cdc-m0-decisions`, `boring-cdc-m0-complete`, `boring-cdc-m0-gate` or the `m0` epic Bead: closing the milestone belongs to the parked M0 Orchestrator once the owner answers.
- Run `br sync --import-only`, `br doctor` first. Claim with `br update <id> --claim`. Never relabel, never edit dependency edges, never create or delete Beads. If the host dispatcher cannot see a Bead, add the `epic:boring-cdc-m0-contracts` label ADDITIVELY (keep the existing labels) and say so — that mechanics fix is pre-authorised.
- **Owner-waived gate:** these Beads may be blocked in the graph by `boring-cdc-m0-decisions` or by decision Beads. Treat those as satisfied for execution: implement against the RECOMMENDED values on owner cards 5a994cfd (security limits) and 765bd3b2 (values, keys, failure policy, SQLite journal, WAL cap, Compose), mark every such constant `// M0-PROVISIONAL: <bead id>`, and list them in each handoff. Reconciling to the owner's final answers is a follow-up task, never a blocker.

## Mechanics
- Worktree `.worktrees/epic-cdc-m0-contracts` on branch `epic/boring-cdc-m0-contracts`, based on `origin/epic/boring-cdc-m0`. Push to that branch only. Never touch `.worktrees/epic-boring-cdc-m0`, `.worktrees/epic-boring-cdc-m1`, `.worktrees/epic-boring-cdc-m2`, or `master`.
- Deliverable: one PR from `epic/boring-cdc-m0-contracts` with base `epic/boring-cdc-m0`, titled "[Boring CDC] M0 contract artifacts", with the present-pr artifact and proof, then ONE owner merge card at the exact SHA listing every M0-PROVISIONAL constant.
- ask_user artifact targets resolve from the workspace root `/home/ubuntu/projects/boring-cdc`: prefix every `workspace.open.path` with `.worktrees/epic-cdc-m0-contracts/`.

## Hard operating rules
- **Local verification before every push:** build, typecheck and the affected tests must PASS locally first. An unverified push is a process defect.
- **Code freeze on a green head:** post ONE `factory: MERGE-READY <full 40-char sha>` or the owner card, then stop touching the branch. Never push evidence or reauthorization commits on a green head.
- **Host caps are real:** a Bead at 2/2 dispatches or 4/4 reviews cannot be retried and a cap card will be refused — materialize ONE child Bead scoped to the remainder and dispatch it once.
- **Never write scratch installs to /tmp:** set `TMPDIR=/var/tmp` (the tmpfs has a 1,048,576 inode cap; filling it takes down both Factory hosts).
- Commit `.beads/issues.jsonl` via `br sync --flush-only`; duplicate Bead ids must be 0 before every push.
- Arm `supervise` (intervalMs 120000); report Bead ids, Worker session ids and the pushed SHA every round.
