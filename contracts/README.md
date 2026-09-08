# Bootstrap contracts

These strict JSON contracts are repository tooling, not product-runtime evidence or decision approval.

- `m0/decisions.json` and `m0/artifacts.json` are intentionally empty, schema-valid skeletons. Aggregate `--complete` validation rejects them.
- `coverage/plan-to-beads.json` is assignment-only and intentionally empty until `boring-cdc-m0.2` populates it.
- `runbooks/index.json` permits declared ownership before M6; `--release` requires complete procedures.
- `graph/pinned.schema.json` describes normalized output from `scripts/validate/beads_snapshot.sh`, whose witness is obtained with `br sync --witness --json`.
- `common/` defines redacted deterministic validator result and structured-log envelopes.

All validators emit one compact JSON object. Findings are ordered by JSON pointer and stable code. Inputs are read-only; relative paths reject traversal and symlinks.
