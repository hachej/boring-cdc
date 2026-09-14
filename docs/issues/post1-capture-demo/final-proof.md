# [Article 1 Capture Demo] Final proof

## PR-ready identity

- Required base: `epic/boring-cdc-m0`
- Required title: `[Boring CDC] Article-1 live pgoutput capture demo`
- Preserved M0 base: `55078fec99836623b2edd7ca7a98819c8048e3db`
- Preserved integration closure: `107ad68547fb4e172d8b289846ba639ee73a58f6`
- Certification target: the commit containing this receipt; its exact pushed SHA is recorded in the `boring-cdc-pci.5` handoff.
- Supervisor override: SHIP NOW under standing full-merge authority, with Gate 2 owner-card waiver. The Worker did not open, approve, or merge the PR.

## Exact reader proof

Run from the repository root with `TMPDIR=/var/tmp` after starting the pinned PostgreSQL fixture on port 55696:

```sh
export PGPASSWORD
PGPASSWORD=$(cat "${BORING_CDC_POSTGRES_PASSWORD_FILE:-.secrets/postgres_password}")
BORING_CDC_ARTICLE1_DSN='postgresql://postgres@127.0.0.1:55696/article1?sslmode=disable' target/debug/boring-cdc run
```

Committed REAL PostgreSQL 17.6 evidence:

- `evidence/article1/reader-default.raw.jsonl`
- `evidence/article1/reader-full.raw.jsonl`
- `evidence/article1/reader.normalized.jsonl`
- server banner: `PostgreSQL 17.6 (Debian 17.6-2.pgdg13+1) ...`; `server_version_num=170006`
- pinned image: `docker.io/library/postgres:17.6@sha256:00bc86618629af00d2937fdc5a5d63db3ff8450acf52f0636ec813c7f4902929`

The two raw files contain the raw `BEGIN`/`INSERT`/`UPDATE`/`DELETE`/`COMMIT` capture, transaction envelopes and LSNs together with the resulting teaching-view current row, overwritten row, and explicit removal. Default identity retains absent UPDATE old values and key-only DELETE identity; FULL identity retains full UPDATE and DELETE old tuples. Raw capture AND resulting row/removal therefore coexist in committed REAL PostgreSQL 17.6 evidence.

SHA-256:

```text
e65fe9a3ce34d74715026d6354668aa832034fe46964f65d44144df621137f13  evidence/article1/reader-default.raw.jsonl
0c7d5b6d06997c6328e9aea084e3906c234fb991fa30670eef3c09f9cb9feeee  evidence/article1/reader-full.raw.jsonl
38254d4e414c12c7a5b3d8e59921a65c83111e64bb2bf208d1fe74f1b09592ff  evidence/article1/reader.normalized.jsonl
```

No M4 canonical destination row was produced or tested.

`article1_row_view` is a TEACHING VIEW; NOT ClickHouse, NOT durable, NOT exactly-once, NOT checkpointed, NOT a materializer, NOT production state, and NOT M4; ClickHouse/destination guarantees deferred to Article 4/M4.

## Scope audit

The audit compared `55078fec9..107ad685` and inspected the Article-1 runtime, fixture, validators, evidence, documentation, and dependency diff. The delivered runtime is a finite CopyBoth reader into the existing decoder, stdout raw-event rendering, and the process-local teaching view. It adds no spool, SQLite journal, feedback/acknowledgement packet, checkpoint, destination, backfill, ClickHouse path, durable state, retry loop, or other M2/M4 behavior. Existing M1 contract/model types that mention future journal, feedback, checkpoint, destination, or backfill boundaries predate this Article-1 slice and are not an Article-1 runtime implementation.

No cancellation polish was added by this integration pass. The previously accepted CLI slice retains only bounded SIGINT/SIGTERM/broken-pipe termination and a caller cancellation token needed to stop this finite stdout reader; it does not add M2 cancellation/recovery policy, retries, persistence, feedback, or another transport loop.

The dependency and protocol selection remains explicitly provisional: `pg_walstream = 0.8.1` and `tokio = 1.53.1` retain `ARTICLE1-PROVISIONAL: boring-cdc-d-pg-protocol` markers in `Cargo.toml` and protocol literals in `src/article1_capture.rs`. `boring-cdc-d-pg-protocol` remains open/in progress and was not amended, closed, or self-approved.

## Local proof

All commands used `TMPDIR=/var/tmp`.

```text
cargo fmt --all -- --check                                      PASS
cargo build --locked                                            PASS
cargo check --locked                                            PASS
cargo test --locked                                             PASS (178 passed, 5 pinned-PG ignored; 3 bin + trybuild pass)
python3 scripts/validate/article1_fixture.py all ...             PASS (REAL PostgreSQL 17.6)
python3 scripts/validate/article1_capture.py ...                 PASS (CopyBoth absent/key/FULL + fail-closed boundaries)
python3 scripts/validate/article1_cli.py ...                     PASS (exact reader path + row view)
scripts/acceptance/article1.sh                                  PASS (2 clean resets, byte-identical normalization)
python3 scripts/validate/article1_transcript.py                  PASS
python3 scripts/validate/m1_decoder.py                           PASS (10 cases)
scripts/validate/m1_cli_contract.sh                              PASS (28 commands)
scripts/e2e/m1_cli_contract.sh                                  PASS (28 parse/help paths; historical evidence restored after probe)
```

A broad `scripts/acceptance/m1_complete.sh --verify` probe was not claimed green: running the affected M1 CLI e2e script rewrites historical M1 evidence, so the completion validator correctly reported that transient output as stale against its committed inventory. Those probe-generated files were restored byte-for-byte from `HEAD`; no historical M1 evidence was changed. The non-mutating affected M1 validators above are green.

Final tracker and repository gates, exact-SHA sandbox proof, and fresh adversarial review are recorded in the Bead handoff. After that handoff: code freeze; the Orchestrator may open the PR with the exact base/title above, but this Worker must not merge or self-approve.
