# [Article 1 Capture Demo] Plan

## Outcome

Make Article 1’s hands-on beat runnable from a clean checkout: PostgreSQL 17.6 emits live `pgoutput`, `boring-cdc run` receives CopyBoth frames, the existing `m1_decoder` decodes them, and stable JSONL shows `BEGIN`, `INSERT`, `UPDATE`, `DELETE`, and `COMMIT` beside the consumer-row shape.

This is deliberately **not** M4 evidence. It proves a raw source event and the row shape a consumer can derive. It does not connect that event to a tested canonical destination row, and the article must narrow its claim accordingly.

## Authority and open risk

- Compose uses the owner-accepted PostgreSQL image: `docker.io/library/postgres:17.6@sha256:00bc86618629af00d2937fdc5a5d63db3ff8450acf52f0636ec813c7f4902929` on `linux/amd64`.
- Existing `src/m1_decoder.rs`, `src/m1_cli_contract.rs`, preflight/config, and `m1-workload` contracts are consumed rather than forked.
- The kickoff says the transport is frozen by `boring-cdc-d-pg-protocol`, but that Bead is still unresolved and explicitly lacks an approved CopyBoth crate/version/checksum. The transport slice must not silently guess this value. If no newer canonical authority exists, it hands back the exact selection blocker for owner disposition.
- “Byte-identical” evidence must define and test deterministic normalization for inherently run-varying server values while retaining meaningful transaction envelopes and LSN information; it must not hand-write or fabricate output.

## Bead graph

| Bead | Slice | Depends on | Proof |
|---|---|---|---|
| `boring-cdc-pci.1` | Reusable CopyBoth transport → existing decoder → stable JSONL | — | locked Rust checks, decoder integration, fail-closed negatives |
| `boring-cdc-pci.2` | Pinned PostgreSQL 17.6 commerce fixture and deterministic mutations | — | Compose clean start/reset, publication/slot/version assertions |
| `boring-cdc-pci.3` | Wire existing `boring-cdc run` contract to capture | `.1` | CLI contract, exit/redaction, live invocation |
| `boring-cdc-pci.4` | Capture and validate a real insert/update/delete transcript | `.1`, `.2`, `.3` | two clean runs byte-identical under documented normalization; evidence digests |
| `boring-cdc-pci.5` | Final integration, scope audit, and local green-head receipt | `.4` | full locked local suite, affected validators, duplicate-ID check, clean head |

The epic Bead is `boring-cdc-pci`. Initial parallelism is limited to `.1` and `.2`; later work is dependency-serial.

## Scope boundaries

No spool, SQLite journal, atomic commit, PostgreSQL feedback/acknowledgement, checkpoint, destination, backfill, or claim that M2/M3/M4 behavior exists. The live transport is a reusable library boundary that `boring-cdc-m2-capture-runtime` can consume later.

Wrong PostgreSQL version, publication, slot, or continuity is named and exits non-zero. Replica-identity output distinguishes absent, key-only, and full old tuples without treating an expected absence as corruption.

## Required release proof

At the exact pushed head, locally run `cargo build --locked`, `cargo check --locked`, `cargo test --locked`, all affected validators, Compose clean-reset replay, transcript validation, `br lint`, duplicate Bead-ID scan, and `git diff --check`. Flush `.beads/issues.jsonl` with `br sync --flush-only` before every push. Once the owner merge card names a green SHA, code freeze applies.

## Plan review

No host-provided independent plan-review mechanism is available before Gate 1: `dispatch_worker` is implementation dispatch and is prohibited before approval, while no `/skill:fresh-eyes` or codex review tool is exposed. This is disclosed rather than self-certifying the plan. Each implementation Bead still requires exact-SHA adversarial fresh review in its Worker handoff.
