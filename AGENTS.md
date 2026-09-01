# Agent Instructions

Read [`docs/AGENT_SYSTEM.md`](docs/AGENT_SYSTEM.md) for the repository's agent-native control and knowledge architecture. This file is the sole human-authored operating procedure for implementation agents; adapter files such as `CLAUDE.md` only point here.

## Authority and tools

This project uses **br** (`beads_rust`) for all issue tracking. The local SQLite database is live working state and the Git-tracked `.beads/issues.jsonl` is the pinned interchange snapshot. **Do not use `bd`, Dolt, or `bd dolt push`.**

Authority is domain-specific:

1. `docs/REQUIREMENTS.md` — product obligations and scope;
2. `docs/PLAN.md` — architecture, rationale, and roadmap;
3. approved `contracts/` artifacts — exact literals, schemas, registries, and compatibility rules;
4. `br` — live work state, ownership, dependencies, and acceptance;
5. evidence manifests — test results and reusable claims; and
6. generated views/context/handoffs — disposable projections that never override their sources.

If authorities conflict, stop and resolve the conflict through the canonical owner. Do not silently choose one copy or turn a handoff observation into a contract.

## Current bootstrap commands

The planned `scripts/agent/` interface is specified in `docs/AGENT_SYSTEM.md` but does not exist until its owning Beads implement it. Until then use these explicit commands:

```bash
br sync --import-only              # after tracked JSONL changes from Git
br sync --status                   # compare SQLite and JSONL
br doctor                          # diagnose workspace state
br ready                           # find unblocked work
br show <id>                       # read the complete Bead
br update <id> --claim             # claim atomically
br blocked                         # inspect blocked work
br lint                            # validate Bead templates
br sync --flush-only               # export SQLite changes to tracked JSONL
```

Use non-interactive commands. In particular, use `cp -f`, `mv -f`, `rm -f`, and `rm -rf`; never invoke an editor-based command.

## Deterministic work loop

### 1. Orient and synchronize

- Check `git status`, current commit, `br sync --status`, `br doctor`, and `br ready`.
- After a pull/rebase or any JSONL change, run `br sync --import-only` before relying on readiness.
- Select one ready Bead based on priority, dependency impact, and fit—not convenience alone.

### 2. Inspect and claim

- Read the complete Bead, its direct dependency outputs, and only the relevant requirement/plan/contract fragments.
- Claim with `br update <id> --claim` before implementation.
- Record the world state you inspected: Git SHA/dirty paths, Beads snapshot/sync state, selected Bead, dependency state, and relevant contract digests.
- Separate verified facts, observations, hypotheses, and proposed decisions.

### 3. Plan the smallest safe change

Before mutation, identify:

- canonical IDs and owner affected;
- intended files and external effects;
- preconditions, expected postconditions, and rollback/recovery boundary;
- likely consumers and invalidated evidence;
- required evidence tier (`leaf`, `component`, `milestone`, or `release`); and
- context, tool, service, time, disk, and artifact budget.

Do not read the entire plan by default when a bounded work packet suffices. Do not silently truncate normative context. If an exact contract is unresolved, stale, multiply owned, or outside the Bead, stop.

### 4. Execute coherently

- Change the canonical source once; regenerate projections instead of editing duplicate views.
- Preserve the narrow v0.1 topology and all durable-before-feedback, capture-epoch, transaction-boundary, anchor, failure, retention, promotion, and security invariants.
- Prefer the smallest reversible probe that resolves the highest-risk assumption.
- Never rewrite old evidence into apparent success or infer compatibility from similarity.
- Use one writer per working tree. Parallelize only graph-independent work with disjoint owners, files, external resources, and artifact roots.

### 5. Verify economically

- During iteration run targeted static/unit/contract checks.
- Run component e2e/fault suites only for owned external boundaries.
- Reserve workspace-wide, clean-environment, repeated, endurance, and release matrices for their owning gates unless the Bead explicitly requires them.
- Reuse evidence only when every declared Git/contract/code/fixture/image/configuration/profile/environment digest is compatible; otherwise rerun and report the invalidating input.
- Record exact commands, versions, outcomes, artifact paths/digests, and residual risks.

### 6. Record and hand off

- Use `br` for task tracking; do not create markdown TODO lists or another issue tracker.
- Promote reusable knowledge only through its canonical owner: contract/decision, fixture, runbook, or immutable evidence claim.
- If interrupted, leave a redacted handoff containing world-state digest, changed paths, canonical IDs touched, completed/failed/stale checks, verified facts, observations, hypotheses, active/ambiguous external intents, risks, unsafe repeats, and the exact next safe command.
- A handoff is not normative design and large logs are referenced by digest rather than copied.

## Beads synchronization rules

- Treat `.beads/issues.jsonl` as the Git-pinned graph snapshot for the checked-out commit.
- Use `.beads/beads.db` for commands; it is ignored by Git.
- Before committing Bead changes, run `br sync --flush-only`, then stage `.beads/issues.jsonl`.
- Never use Dolt or any remote database push. Collaborators synchronize Beads through Git-tracked JSONL.
- Generated projections must identify source IDs/digests and must not be edited as independent authority.

## Session completion

Work is not complete until Git is pushed successfully:

```bash
git status
br lint
br sync --flush-only
git add <changed-files> .beads/issues.jsonl
git commit -m "..."
git pull --rebase
git push
git status  # must be clean and synchronized with origin
```

If a rebase changes `.beads/issues.jsonl`, run `br sync --import-only` and re-check `br doctor`, `br lint`, and `br ready` before continuing.
