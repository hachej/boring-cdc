# Boring CDC M0 — execution plan

## Authority and protocol override

Julien's 2026-09-08 order authorizes Factory execution of the already-materialized M0 plan and Bead graph. Repository authority is `AGENTS.md`, `docs/AGENT_SYSTEM.md`, `docs/PLAN.md` §§16 and 19, approved contracts, and the existing `br` graph.

The kickoff explicitly overrides the generic graph-materialization and `epic:boring-cdc-m0` labeling defaults: **do not recreate, relabel, or duplicate the graph**. M0 selection uses the existing `m0` label and dependency graph. The graph is a DAG (`br dep cycles`: no cycles); `bv --robot-insights` identifies `boring-cdc-m0.1` as the first critical unlock.

## Method

Execute the approved dependency-aware Bead graph in the shared epic worktree, one writer at a time. Workers claim exact ready Beads, implement only their bounded scope, produce the Bead-required evidence, obtain fresh review of the exact SHA, push the epic branch, and leave a complete durable handoff. The Orchestrator reads only Bead end states and never implements, closes, merges, or self-approves.

No extra adversarial plan review was run: the repository plan and Beads are already the owner-approved specification under the kickoff order, and no independent-review mechanism is available to this Orchestrator. Every implementation handoff still requires the specified exact-SHA independent fresh review.

## Execution slices

1. `boring-cdc-m0.1` — bootstrap validators and pinned graph primitives.
2. As dependencies unlock: `boring-cdc-m0.2` and `boring-cdc-m0.3`, then `boring-cdc-m0-validation-tooling`.
3. In parallel after Gate 1, collect all 25 owner decisions in one non-blocking decision-register card. Accepted defaults are materialized by Workers after validator prerequisites; rejected items receive focused follow-up cards with concrete alternatives.
4. Complete the decision and artifact owners required by the existing graph, including the event, PostgreSQL, storage, archive, ClickHouse, and scaffold contracts.
5. Run `boring-cdc-m0-decisions`, then `boring-cdc-m0-complete`, then terminal proof `boring-cdc-m0-gate`; capture `scripts/validate/close_guard.sh boring-cdc-m0` JSON.
6. Open one PR to `master`, start an exact-SHA live demo (or record the exact fallback error), and raise one blocking merge-approval card. The owner merges.

## Proof path

- Per Bead: required evidence tier and scripts from its embedded acceptance contract; exact command/version/result/digests; clean tree; pushed SHA; exact-SHA fresh review with explicit abstraction pass; durable handoff.
- Graph: `br doctor`, `br lint`, `br dep cycles`, pinned `.beads/issues.jsonl`, snapshot/round-trip validators once implemented.
- M0 aggregate: decision and artifact validators, complete-validation tooling, `scripts/acceptance/m0_complete.sh`, `scripts/acceptance/m0_gate.sh`, and closure-guard JSON.
- Final: PR head SHA equals reviewed/demo SHA except separately cited docs-only presentation commits.

## Risks and rollback

M0 freezes license, public identity, security defaults, storage/durability, protocol, and capacity boundaries. Unresolved or rejected decisions remain blocking. No M1/product-runtime implementation may start before terminal M0 proof. Source rollback is commit reversion; contract changes invalidate dependent evidence and must be reprojected rather than silently edited.

## Next action after approval

Arm durable 120-second supervision, dispatch `boring-cdc-m0.1`, and immediately raise the non-blocking 25-item decision-register card.
