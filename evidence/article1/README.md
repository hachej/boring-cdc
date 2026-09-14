# Article 1 real PostgreSQL reader evidence

> **`article1_row_view` is a TEACHING VIEW: NOT ClickHouse, NOT durable, NOT exactly-once, NOT checkpointed, NOT a materializer, NOT production state, and NOT M4.** ClickHouse and destination guarantees are deferred to Article 4/M4. This real PostgreSQL `pgoutput` reader stdout proves only a process-local explanation of current rows; it does not prove feedback, retry, spool/journal, checkpoint, destination, or backfill behavior.

## What these files are

- `reader-default.raw.jsonl` is unmodified stdout from the exact reader command against a clean default-replica-identity fixture. Each decoded event carries raw fields and the `article1_row_view` result produced from that same live in-process `PgoutputEvent`/`RowChange`; no SQL, transcript, or rendered JSONL is re-parsed. INSERT creates a current row, UPDATE with `old_state: "absent"` overwrites that known row, and key-only DELETE removes it.
- `reader-full.raw.jsonl` is unmodified stdout from a second clean fixture after `ALTER TABLE customers REPLICA IDENTITY FULL`. UPDATE and DELETE retain `old_state: "full"` while producing the same overwrite/removal teaching result.
- `reader.normalized.jsonl` is mechanically derived from those raw files by `scripts/validate/article1_transcript.py`.
- `manifest.json` records the capture identity and SHA-256 digests.

The default and FULL captures are separate clean database resets. FULL is an explicit teaching contrast, not the fixture default.

## Exact capture environment

Capture source SHA: `fbd139e0d9f09d1e4141f3d84b4ce7eb55992149` (includes preserved closure commits `e852813` and `30e9fe4`, merged M0 base `55078fec9`, the provisional transport/CLI markers, and the same-stream teaching-view implementation). The exact `target/debug/boring-cdc` executable used for both committed raw captures had SHA-256 `2a37d6efebe34d7efb1866bc62b74c58663ba3f3da5701e4e932c7391170f800`. This is capture identity, not a cross-checkout reproducible-build claim; Rust debug artifacts can encode their absolute build path.

Server banner:

```text
PostgreSQL 17.6 (Debian 17.6-2.pgdg13+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit
server_version_num=170006
```

Image: `docker.io/library/postgres:17.6@sha256:00bc86618629af00d2937fdc5a5d63db3ff8450acf52f0636ec813c7f4902929`, local image ID `sha256:50903ccdcab597707a1f61c7ae016a06b0b548da53a6f7ad716d56b072bedba0`, platform `linux/amd64`.

From the repository root with `TMPDIR=/var/tmp`, the clean stack and mounted seed were applied with:

```sh
export TMPDIR=/var/tmp ARTICLE1_PG_PORT=55696
project=article1-pci4-capture
docker compose -p "$project" -f fixtures/article1/compose.yml down -v --remove-orphans
docker compose -p "$project" -f fixtures/article1/compose.yml up -d --wait
# The new volume runs the read-only-mounted fixtures/article1/schema-and-seed.sql
# as /docker-entrypoint-initdb.d/10-schema-and-seed.sql.
docker compose -p "$project" -f fixtures/article1/compose.yml exec -T postgres \
  psql -U postgres -d article1 -Atqc 'select version(); show server_version_num;'
```

The exact reader command was:

```sh
BORING_CDC_ARTICLE1_DSN='postgresql://postgres:article1_fixture_only@127.0.0.1:55696/article1?sslmode=disable' target/debug/boring-cdc run
```

While that process was reading, the default-identity transaction was executed with:

```sh
docker compose -p article1-pci4-capture -f fixtures/article1/compose.yml exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U postgres -d article1 -c \
  "BEGIN; INSERT INTO customers VALUES (9101, 'Article Default', 1); UPDATE customers SET name='Article Default Updated', tier=2 WHERE id=9101; DELETE FROM customers WHERE id=9101; COMMIT;"
```

The stack was then removed with `down -v`, recreated, and changed to FULL identity before starting the same reader command:

```sh
docker compose -p article1-pci4-capture -f fixtures/article1/compose.yml exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U postgres -d article1 -c \
  'ALTER TABLE customers REPLICA IDENTITY FULL;'
docker compose -p article1-pci4-capture -f fixtures/article1/compose.yml exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U postgres -d article1 -c \
  "BEGIN; INSERT INTO customers VALUES (9201, 'Article Full', 3); UPDATE customers SET name='Article Full Updated', tier=4 WHERE id=9201; DELETE FROM customers WHERE id=9201; COMMIT;"
```

## Deterministic normalization contract

Raw real stdout is preserved because PostgreSQL commit timestamps vary by run. The normalizer parses every JSONL event, first validates transaction envelopes, old-tuple semantics, row ordinals, and LSN relationships, then performs exactly these deterministic transformations:

1. replace each numeric `transaction.commit_time` with numeric `0`, after proving the BEGIN and COMMIT values match within that transaction;
2. serialize each JSON object with lexicographically sorted keys and compact separators;
3. concatenate default then FULL events, retaining every xid, relation ID, ordinal, row value, WAL LSN, final LSN, commit LSN, and end LSN unchanged.

The retained LSN contract requires BEGIN and its first row to share the start LSN, later row/event LSNs to increase, BEGIN `final_lsn` to equal COMMIT `commit_lsn`, and COMMIT `wal_start`/`wal_end` to equal `end_lsn`. `scripts/acceptance/article1.sh` performs default and FULL capture across two independent clean volume resets per round, normalizes both rounds, and requires byte equality with each other and with the committed normalized transcript.

Run:

```sh
TMPDIR=/var/tmp ARTICLE1_PG_PORT=55696 ARTICLE1_PROJECT=article1-owner-evidence \
  scripts/acceptance/article1.sh
python3 scripts/validate/article1_transcript.py
```

## Raw event plus teaching row/removal

Each line is one raw source event object with an `article1_row_view` field. BEGIN and COMMIT retain their transaction envelope and LSNs and mark a transaction boundary. Row events retain `event`, `relation_id`, `new`, `old`, `old_state`, transaction identity/ordinal, and LSNs beside the result derived directly from the same decoded object:

- INSERT: `action: "current_row"` with the inserted row;
- UPDATE: `action: "current_row"` with the overwritten row;
- DELETE: `action: "removed"`, the removed prior row, and `row: null`.

Tuple arrays follow the published `customers(id, name, tier)` relation order; JSON `null` placeholders in a key-only old tuple are not full old values. Missing current state, keys, new UPDATE values, or DELETE identity fail with a visible `ARTICLE1_ROW_VIEW_*` error rather than guessing.

Again, **`article1_row_view` is a TEACHING VIEW: NOT ClickHouse, NOT durable, NOT exactly-once, NOT checkpointed, NOT a materializer, NOT production state, and NOT M4.** ClickHouse and destination guarantees are deferred to Article 4/M4.
