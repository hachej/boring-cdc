# Boring CDC M1 — execution plan

## Outcome

Implement M1 from the already-materialized `br` graph: typed protocol boundaries, CLI/config contracts, PostgreSQL identity/decoder/control/DDL fixtures, bootstrap state model, read-only preflight, deterministic workload/oracle, and the terminal raw-event demo/fault proof.

Authority: `docs/PLAN.md` M1 and decision register, `docs/REQUIREMENTS.md`, approved contracts, and the embedded acceptance/evidence contract on each Bead.

## Execution graph

The existing 14-node epic graph is retained; no dependencies are edited. Owner order waives only the open `boring-cdc-m0-gate` edge for M1 scheduling. A Bead is dispatchable when every other blocking dependency is closed.

1. First wave: `boring-cdc-m1.1`, `boring-cdc-m1-config`, `boring-cdc-m1-control-fixtures`, `boring-cdc-m1-workload`.
2. Then: `boring-cdc-m1-source-identity` and `boring-cdc-m1-cli-contract`.
3. Then: `boring-cdc-m1-decoder` and `boring-cdc-m1-preflight`.
4. Then: `boring-cdc-m1-ddl-fixtures` and `boring-cdc-m1-ordering`.
5. Then: `boring-cdc-m1-bootstrap-sm`.
6. Barrier: `boring-cdc-m1-complete`.
7. Terminal milestone proof: `boring-cdc-m1-raw-demo`.

Worker concurrency is capped at two. Each Worker claims one exact Bead, uses only the shared M1 worktree, commits and pushes the epic branch, obtains an adversarial `fresh_review` at its exact SHA, and records a complete Bead handoff. The Orchestrator never implements or closes a Worker Bead.

## Provisional M0 values

Where M1 consumes owner-pending M0 literals, use the recommendation already shown on the M0 cards. Mark each source constant `// M0-PROVISIONAL: <bead id>` and enumerate all such constants in the Bead handoff and final merge card. Reconciliation is follow-up work, not an M1 blocker.

## Proof and delivery

- Leaf/component/milestone evidence follows each Bead's normative tier override.
- Every handoff SHA receives standards/spec review against the Bead contract and `docs/PLAN.md`, thermo, and an explicit abstraction PASS.
- Before each push: duplicate Bead IDs = 0; `br lint`; required tests/evidence validation; `br sync --flush-only` with `.beads/issues.jsonl` committed.
- Closure uses `scripts/validate/close_guard.sh`; hierarchy never closes from prose.
- Deliver one stacked PR from `epic/boring-cdc-m1` to `epic/boring-cdc-m0`, title `[Boring CDC] M1 — protocol, workload, and bootstrap design`.
- Gate 2 names the exact SHA, live demo, present-PR artifact, proof, and complete provisional-constant list. The owner merges.

## Risk and rollback

Highest risks are provisional M0 literals, protocol fail-open behavior, cross-epoch comparison, feedback beyond a durable transaction boundary, and accidental mutation in preflight. Mitigation is typed boundaries, fail-closed fixtures, exact-SHA reviews, dependency sequencing, and tiered evidence. Rollback is commit-level revert on the stacked epic branch; no M0 branch or Bead is touched.

## Review note

The pre-existing Bead contracts already contain accepted adversarial review corrections. No separate tier-1 independent-review command is available to this Orchestrator; per Factory policy this is disclosed at Gate 1 and the owner decides.
