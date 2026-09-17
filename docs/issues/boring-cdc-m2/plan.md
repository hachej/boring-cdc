# [Boring CDC M2] Bounded durable journal and JSONL commit engine

## Objective
Ship M2 on `epic/boring-cdc-m2`, stacked on `epic/boring-cdc-m1`, proving durable-before-feedback capture, bounded recovery/admission/retention, exclusive ownership, and generation-invisible JSONL commits until promotion.

## Authority and execution policy
- Canonical scope: `AGENTS.md`, `docs/AGENT_SYSTEM.md`, `docs/PLAN.md` M2, `docs/REQUIREMENTS.md`, and approved `contracts/`.
- Existing Bead graph is retained; no new graph is created and dependencies are not edited.
- Owner's 2026-09-10 order waives external M0/M1 gate blockers for M2 execution only. It does not waive M2-internal dependencies.
- Pending M0 literals use the recommendation on owner cards `5a994cfd` / `765bd3b2`, marked `// M0-PROVISIONAL: <bead id>`, and are listed at handoff and Gate 2.
- Workers verify locally before push, commit/flush the Beads ledger, test the exact SHA, obtain adversarial review, push, and leave a complete Bead handoff. The orchestrator never implements, closes, merges, or self-approves.

## Existing slices and dependency flow
1. Foundations: `boring-cdc-m2-schema` → `boring-cdc-m2.1` (shared failure policy), plus ownership.
2. Durable capture: journal → spool + heartbeat → reconciliation → pressure; integrate through `m2-capture-runtime`.
3. Maintenance and fan-out: init/recovery; leases → JSONL commit engine.
4. Convergence: `m2-complete` → terminal `m2-fault-status` proof.

The owner-requested first dispatch is `boring-cdc-m2.1`; however the existing graph records it blocked by `boring-cdc-m2-schema`. The graph also has no `epic:boring-cdc-m2` labels (it uses `m2`, and `.1` uses `kickoff-plan,policy`), so host `factory_status` currently sees zero Beads. Because the same owner request says never relabel, never re-materialize, and never edit dependencies, these are explicit execution blockers to resolve without silent graph mutation.

## Proof path
- Per-Bead targeted build/test/typecheck and exact-SHA sandbox evidence.
- `br lint`, duplicate IDs = 0, `br dep cycles`, `bv --robot-insights`.
- `scripts/validate/plan_coverage.sh` and `scripts/validate/close_guard.sh` at owning gates.
- Terminal fault/status evidence covers crash hooks, status JSON, corruption/pressure/ownership/bootstrap ambiguity, deterministic-vs-transient retry behavior, and JSONL directory-commit visibility.
- One stacked PR: `[Boring CDC] M2 — bounded durable journal and JSONL commit engine`, base `epic/boring-cdc-m1`; Gate 2 at exact green SHA with live demo and provisional-constant inventory.

## Risks and rollback
Highest risks are acknowledgement outrunning durable SQLite, restart storms, reserve exhaustion, ownership ambiguity, and premature archive visibility. Contain them with typed state, transaction-boundary commits, persisted failure schedules, bounded budgets, leases/fences, fault injection, and fail-closed startup. Rollback is revert of the M2 commits; no merge occurs without owner approval.

## Review record
No host-provided independent plan-review mechanism is available in this session. Per Gate 1 policy, this limitation is disclosed for the owner to decide.
