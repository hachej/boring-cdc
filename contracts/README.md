# Bootstrap contracts

These strict JSON contracts are repository tooling, not product-runtime evidence or decision approval.

- `m0/decisions.json` and `m0/artifacts.json` begin as schema-valid skeletons and accrue rows only from each owning decision/artifact Bead. Aggregate `--complete` validation rejects empty or incomplete inventories.
- `coverage/plan-to-beads.json` is assignment-only and intentionally empty until `boring-cdc-m0.2` populates it.
- `runbooks/index.json` permits declared ownership before M6; `--release` requires complete procedures.
- `graph/pinned.schema.json` describes normalized output from `scripts/validate/beads_snapshot.sh`, whose witness is obtained with `br sync --witness --json`.
- `common/` defines redacted deterministic validator result and structured-log envelopes.

All validators emit one compact JSON object. Findings are ordered by JSON pointer and stable code. Inputs are read-only; relative paths reject traversal and symlinks.

## Read-only agent context (M0 stage 2)

`agent/stable-ids.json` is the canonical initial stable-ID registry. Its rows
assign requirements, invariants, decisions, commands, conditions, transitions,
scenarios, release criteria, and risks to one executing Bead with only
`evidence_status=pending`; the schema recognizes future RUNBOOK/CLAIM/FINDING
identities without synthesizing rows. `coverage/plan-to-beads.json` is its
assignment-only generated projection, guarded by an immutable-source digest in
`plan-to-beads.provenance.json`.

The remaining `agent/*.schema.json` contracts describe captured world state,
fully materialized effective contracts, bounded context, impact/staleness, and
generated-view provenance. `scripts/agent/{doctor,next,context,impact}` are
read-only: they neither claim work nor mutate Git, Beads, or product state.
Until the M0.3 claim index exists, they explicitly report evidence as
pending/unavailable and disable reuse.
