# Agent-native control and knowledge architecture

This document defines how implementation agents understand and change Boring CDC with the least safe amount of context and compute. It does **not** add a product control plane, daemon, scheduler, remote mutation API, or generic agent framework. It is a repository-local design over Git, `br`, versioned contracts, generated views, and content-addressed evidence.

## 1. North star

At any point, an agent should be able to:

1. obtain the smallest sufficient, cryptographically identified world model;
2. know the authority, owner, freshness, and provenance of every relevant fact;
3. see legal next actions, preconditions, consequences, cost, and recovery boundaries;
4. execute against the exact snapshot it inspected;
5. verify postconditions at the cheapest valid evidence tier; and
6. leave reusable evidence and a deterministic handoff.

No generated summary or handoff becomes a new source of truth. The system accretes verified knowledge while keeping observations, hypotheses, decisions, contracts, and evidence distinct.

## 2. Tower of abstractions

| Layer | Purpose | Canonical authority |
|---|---|---|
| L0 — Charter | Purpose, fixed topology, guarantees, non-goals, current phase | `README.md` |
| L1 — Agent protocol | How an agent or contributor navigates, acts, verifies, and hands off | `AGENTS.md` and this document |
| L2 — Product obligations | What v0.1 must do, independent of sequencing | `docs/REQUIREMENTS.md` with stable `REQ-*` IDs |
| L3 — Architecture | Why the system is shaped this way, invariants, boundaries, and roadmap | `docs/PLAN.md` with stable architectural IDs |
| L4 — Approved contracts | Exact versioned literals, schemas, transitions, registries, and compatibility rules | Approved files under `contracts/` |
| L5 — Executable work | Live readiness, dependencies, one canonical executing owner, task-specific acceptance | local `br` SQLite state; Git-pinned interchange in `.beads/issues.jsonl` |
| L6 — Generated views | Bounded context, impact, readiness, effective contracts, and status | deterministic projections from L2–L5; never hand-edited authority |
| L7 — Proof and memory | Immutable claims, evidence, decisions, findings, and handoffs | content-addressed manifests bound to world-state and contract digests |

Authority is domain-specific, not “last file edited wins.” Product obligations belong to requirements. Architectural rationale belongs to the plan. Exact approved values belong to contracts. Work state belongs to `br`. Test results belong to evidence. If two layers conflict, work stops and the canonical owner resolves the conflict; an agent must not silently choose the convenient version.

### 2.1 PLAN-to-Bead freeze lifecycle

The plan is the canonical architectural synthesis. A Bead is a frozen executable projection for a particular set of stable source IDs and contract digests. Each effective work contract records:

- owning Bead and graph-snapshot digest;
- owned and consumed stable IDs;
- source contract IDs and digests;
- task-specific inputs, outputs, exclusions, scenarios, and evidence tier; and
- the projection schema version.

Changing a canonical requirement, architecture row, decision, or contract produces an impact set. Affected open or in-progress Beads become stale until regenerated or explicitly re-approved. Closed evidence remains valid history but cannot prove the changed contract. Repeated generic policy may move into a versioned common contract only after clean-checkout effective-contract materialization exists; task-specific safety boundaries must remain visible in the Bead.

## 3. Stable identity and canonical ownership

Identity never depends on prose wording, file line number, or tracker order. Registries use stable semantic IDs:

- `REQ-*` — product requirement;
- `INV-*` — architecture invariant;
- `DEC-*` — approved decision;
- `CMD-*` — public command;
- `COND-*` — observable condition or reason;
- `TRANS-*` — state transition;
- `SCN-*` — executable scenario;
- `REL-*` — release criterion;
- `RISK-*` — risk and mitigation;
- `RUNBOOK-*` — operator procedure;
- `CLAIM-*` — verified evidence claim;
- `FINDING-*` — discovered constraint or failed approach.

Every ID has exactly one canonical owner. Consumers cite owner ID and digest; they do not restate or re-own its semantics. Generated indexes may render the same graph for humans, but a generated file must identify its sources and fail validation when stale.

The ownership graph links:

```text
requirement -> invariant/decision/contract -> owner Bead
            -> command/condition/transition -> scenario
            -> evidence claim -> milestone/release gate
```

## 4. World-state identity

Every context, plan, mutation, verification, and handoff binds to one `world_state`:

```text
WorldState {
  schema_version
  git_commit
  working_tree_digest
  beads_snapshot_digest
  selected_bead
  dependency_closure_digest
  contract_digests[]
  generated_view_versions[]
  evidence_index_digest
  observed_at
}
```

A dirty tree is allowed only when its digest and changed paths are explicit. One operation uses one captured Beads snapshot; later filesystem or tracker changes cannot alter its meaning mid-run. Any action whose preconditions no longer match the inspected world state must be replanned.

## 5. Progressive disclosure and context packs

Routine work should not require reading all of `docs/PLAN.md` or `.beads/issues.jsonl`. Deterministic repository-local tooling will provide bounded, expandable packs:

| Profile | Contents |
|---|---|
| `orient` | Charter, agent rules, Git/Beads health, current milestone, blockers, and candidate work |
| `implement` | Full selected Bead, direct dependency outputs, affected canonical rows, likely code/doc seams, hazards, and required leaf evidence |
| `review` | Implementation pack plus diff, impacted consumers, invalidated claims, and relevant invariant/scenario rows |
| `handoff` | World-state digest, changes, decisions, checks, evidence, risks, unsafe repeats, and exact next safe action |
| `expand` | A named omitted section or dependency, added explicitly rather than by silent truncation |

A pack declares byte/token estimate, included IDs, omitted references, and source digests. The default target is 16 KiB. If completeness cannot fit, generation fails with an explicit expansion plan; it never silently truncates normative content.

Planned transparent interfaces under `scripts/agent/` are:

```text
scripts/agent/doctor
scripts/agent/next [--json]
scripts/agent/context BEAD --profile orient|implement|review|handoff
scripts/agent/impact PATH_OR_ID
scripts/agent/verify BEAD --tier leaf|component|milestone|release
scripts/agent/handoff BEAD
scripts/agent/recover BEAD_OR_MANIFEST
scripts/agent/finish BEAD
```

These wrappers are noninteractive, inspectable, and repository-local. They never claim work, mutate a product runtime, commit, push, conceal `br`/Git changes, or rank work without explaining the score. Until they exist, agents use the explicit commands in `AGENTS.md`.

## 6. Agent control loop

The canonical loop is:

```text
orient -> synchronize -> select -> inspect -> snapshot -> plan
       -> simulate/preflight -> execute -> verify -> record -> hand off/finish
```

### 6.1 Before mutation

- Import a changed Git-pinned Beads snapshot and verify sync/doctor health.
- Select ready work; read and claim the complete Bead.
- Capture `world_state` and determine owned IDs, dependencies, exclusions, likely impact, and evidence tier.
- Separate verified facts, observations, hypotheses, and proposed decisions.
- Write down intended files, external effects, preconditions, postconditions, rollback boundary, and resource budget.
- Refuse work whose canonical contract is unresolved, stale, multiply owned, or outside the Bead.

### 6.2 During execution

- Prefer the smallest reversible action that tests the highest-risk assumption.
- Recheck the bound snapshot before destructive or externally visible effects.
- Preserve immutable intents and evidence; never edit old results into apparent success.
- Update a canonical source once, then regenerate projections.
- Run targeted checks during iteration; do not spend milestone-scale resources on an unrelated leaf.

### 6.3 After execution

- Verify task postconditions and all impacted canonical IDs at the required evidence tier.
- Record exact commands, versions, outcomes, digests, and residual risks.
- Promote only verified reusable facts through their canonical owner.
- Close the Bead only with schema-valid evidence, synchronized JSONL, successful push, and a clean tree.
- If interrupted, emit a handoff instead of leaving free-form state.

## 7. Runtime control symmetry

The product gives its operator the same model. `boring-cdc status --json` projects one canonical runtime `SystemSnapshot`:

```text
SystemSnapshot {
  schema_version
  snapshot_id
  state_revision
  observed_at
  fresh_until
  source_identity
  capture_epoch
  run_id
  ownership
  configuration_fingerprint
  table_set_fingerprint
  durable_and_feedback_boundaries
  journal_and_filesystem_budgets
  destinations[]
  active_conditions[]
  blocked_by[]
  allowed_actions[]
  next_commands[]
  evidence_digest
}
```

Every mutating dry-run returns an `ActionPlan` bound to `snapshot_id` and `state_revision`, with asserted preconditions, intended transitions, external effects, resource/continuity consequences, confirmation policy, rollback boundary, expected postconditions, and plan digest. Confirmation and terminal evidence cite that digest. If state revision or a bound fingerprint changes, confirmation fails and a new plan is required. Domain Beads still own domain transitions; the shared CLI owns only envelopes and compatibility.

## 8. Evidence economy and accretive knowledge

Evidence is an immutable claim, not a directory that “looks recent.” A claim records:

- claim, owner Bead, and scenario IDs;
- Git, Beads, effective-contract, binary, fixture, image, configuration, profile, seed, and environment digests;
- command, result, artifact URI, result digest, and redaction result;
- applicability predicate and freshness or timeless declaration;
- superseded claim IDs and rerun command.

Reuse is permitted only when every declared compatibility input matches, or when a canonical contract explicitly owns a compatible range. A mismatch reports the exact invalidating input and the rerun owner. “Similar environment” is not compatibility.

Verification tiers keep cost proportional:

- **leaf:** formatting/lint plus targeted unit/property/contract tests for owned branches;
- **component:** external-boundary e2e/fault suite and consumed contract vectors;
- **milestone:** workspace tests, milestone integration, clean-environment reproducibility, and exit assertions;
- **release:** endurance/full failure matrix and clean-clone release reproduction.

A leaf does not rerun unrelated Compose/fault matrices. A gate cannot cite a cheaper tier where a stronger tier is required.

Negative knowledge is first-class. A failed approach or discovered limit gets a stable finding ID linked to the affected requirement/invariant/scenario, observations and environment, resolution or open owner, and supersession evidence. Handoff hypotheses never become contracts automatically.

## 9. Handoff contract

A handoff is a small validated manifest, not normative design prose. It contains:

- Bead ID and bound world-state digest;
- base/head SHA, dirty-tree summary, and changed paths;
- stable IDs touched and decisions made;
- completed, failed, and stale checks with evidence digests;
- verified facts, observations, and hypotheses as separate fields;
- active or ambiguous external intents;
- known risks and blockers;
- exact next safe command; and
- commands unsafe to repeat plus redaction validation.

Large logs stay in the artifact store and are referenced by digest. A successor verifies the world-state compatibility before following the next command.

## 10. Parallel work and resource control

Parallelize only graph-independent work with disjoint canonical owners, files, external resources, and artifact roots. One writer owns a working tree. Shared contract changes serialize before dependent implementation. A parallel plan declares merge order and invalidation rules.

Every agent plan states expected context bytes/tokens, tool calls, external services, test tier, runtime, disk, and artifact growth. Prefer static/schema/targeted checks first, then component tests, then gates. Resource savings never justify weaker evidence, silent truncation, cached-result guessing, or unsafe concurrency.

## 11. Canonical ownership and rollout

- `boring-cdc-m0-validation-tooling`: schemas and validation for stable IDs, world state, effective contracts, generated views, evidence claims, handoffs, graph snapshots, impact, and stale-contract detection.
- `boring-cdc-m0-scaffold`: repository-local `scripts/agent/` entry points, CI wiring, generated-view installation, and onboarding.
- `boring-cdc-m1-cli-contract`: runtime `SystemSnapshot`/`ActionPlan` envelopes and command compatibility.
- `boring-cdc-m2-fault-status`: fresh runtime snapshot projection and persisted postcondition evidence.
- Domain Beads: their facts, preconditions, transitions, allowed actions, and scenarios.
- `boring-cdc-m6-runbooks`: complete procedures consuming condition/action IDs.
- `boring-cdc-m7-docs`: public human projections checked against canonical registries and measured evidence.

Rollout order:

1. freeze authority and ID namespaces;
2. define schemas and exact ownership;
3. implement doctor/context/impact over the existing graph;
4. add evidence tiers, claims, stale detection, and handoff;
5. generate duplicated tables/views from registries;
6. only then compact repeated Bead boilerplate via effective contracts;
7. prove a fresh-agent task and a contract-change invalidation scenario; and
8. publish public projections at release.

## 12. Non-goals

- no generic agent platform, vector database, remote coordinator, autonomous scheduler, or cross-repository memory service;
- no replacement for `br`, Git, or the existing one-binary product model;
- no automatic claim, destructive runtime action, commit, or push;
- no editable generated view as authority;
- no evidence reuse across unmatched inputs;
- no weakening or deletion of v0.1 correctness, durability, fault, security, or release assertions; and
- no conversion of unverified observations or handoffs into contracts.
