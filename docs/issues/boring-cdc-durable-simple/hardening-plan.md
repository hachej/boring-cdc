# [Boring CDC Durable Stream Simple Case] Bounded hardening plan

## Objective and boundary
Harden the already merged simple durable stream at `master` `8782ae5` against two correctness risks only: relation-schema changes while streaming, and real volume crossing configured spool/journal bounds. Backfill, chunking, anchors, ClickHouse, Parquet, and all M3/M4 work remain parked priority 2.

Every claim remains external-oracle based: PostgreSQL 17.6 owns the committed id/key set; SQLite is opened read-only; the journal projection must converge exactly with no duplicates or gaps; PostgreSQL feedback must never pass the durable journal boundary. No fixture may write `bootstrap_intents` or `source_state`.

## Risk judgment
The volume/boundary scenario is more likely to expose a real product defect. The repository has bounded spool and journal component tests, but no production-path proof that an over-bound transaction refuses safely and then has an operable recovery path that reaches the PostgreSQL oracle. A safe stop without a way to resume is not sufficient durability.

The decoder already detects changed `Relation` metadata, removes the old relation contract, requires revalidation, and blocks rows while validation is pending. Therefore incompatible DDL is expected to fail closed rather than silently mis-decode. The schema slice must determine and pin whether adding a nullable column is a supported live evolution or a named fail-closed boundary; either behavior is acceptable only when feedback and recovery semantics are explicit.

No different hardening item currently outranks these two. Restart continuity is already covered by the merged simple case; schema invalidation and admission/recovery are the highest unproved source-side correctness boundaries.

## Bead graph

1. `boring-cdc-ckoi.5` — **Harden schema changes mid-stream.** Against real pinned PostgreSQL 17.6, add a nullable column while capture is live, write through it, then execute genuinely incompatible DDL. Assert correct convergence or a stable named fail-closed result, never stale decoding. Use PostgreSQL ids/keys as the oracle and verify feedback remains bounded.
2. `boring-cdc-ckoi.6` — **Harden configured volume bounds.** Depends on `.5` to keep one shared-worktree writer and avoid overlapping runtime fixes. Drive large rows/transactions across a deliberately configured bound, observe bounded memory/refusal, then prove an operable recovery reaches exact PostgreSQL-oracle equality with no loss or duplication. Keep this as a standalone script and measure runtime before deciding CI placement.
3. `boring-cdc-ckoi.7` — **Certify bounded stream hardening.** Depends on `.6`. Merge `origin/master` forward, run the full mandatory matrix plus both hardening scenarios, durable simple case, corrected M2 runtime harness, and article-1. Perform the only repo-wide evidence reseal, push, open/update the PR to `master`, and post `factory: MERGE-READY <full SHA>`.

All nodes are children of `boring-cdc-ckoi` and carry `epic:boring-cdc-durable-simple` at priority 1.

## Proof details

### Schema change
- Begin from the documented empty-database operator path on PostgreSQL 17.6.
- Capture baseline transactions and PostgreSQL oracle ids.
- `ALTER TABLE ... ADD COLUMN <name> ... NULL`, then commit rows exercising old/default and new values.
- Apply an incompatible change chosen to invalidate the configured relation contract (for example key/type/column shape, selected only after inspecting PostgreSQL's emitted relation metadata).
- Pin the actual designed behavior: supported evolution converges exactly; unsupported evolution emits a stable named diagnostic, safe-stops before feedback, and documents the recovery action.
- Never accept an event decoded under stale metadata.

### Configured volume bound
- Configure a small but production-valid bound so the test crosses it with deterministic large rows without wasting CI time.
- Observe process RSS/high-water and spool/journal allocation around the boundary.
- Assert a stable bounded-refusal/back-pressure outcome and `confirmed_flush_lsn <= durable_lsn` at every sample.
- Exercise the supported recovery path and continue with post-bound transactions.
- Compare the complete PostgreSQL committed id/key set, including the over-bound interval, to read-only journal contents; assert set equality, unique transaction IDs, and gap-free sequence.
- Record elapsed time. The scenario joins per-push CI only if its measured runtime is sane; otherwise the standalone script remains mandatory terminal evidence and the handoff states why.

## Operating rules
Before every push, merge `origin/master` forward and locally run `cargo fmt --all -- --check`; `cargo clippy --locked --workspace --all-targets -- -D warnings`; `cargo test --locked --workspace --all-targets`; all four scaffold validators; and `python3 -m unittest discover -s tests`. Run `scripts/acceptance/article1.sh` after every change. Never delete or weaken tests. Use `TMPDIR=/var/tmp`, clean scratch data, and never reseal evidence before `.7`.

## Review record
No host-provided independent plan-review mechanism is available to this Orchestrator. Gate 1 discloses that limitation rather than self-certifying. The plan is bounded to the two owner-selected source correctness risks, with one terminal integration/reseal slice.
