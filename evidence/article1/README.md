# Article 1 real PostgreSQL reader evidence

> **M4 boundary: no M4 canonical destination row was produced or tested.** This is real PostgreSQL `pgoutput` reader stdout and a documented consumer-facing event shape only. It does not prove feedback, retry, spool/journal, checkpoint, destination, or backfill behavior.

## What these files are

- `reader-default.raw.jsonl` is unmodified stdout from the exact reader command against a clean default-replica-identity fixture. Its UPDATE has no old tuple (`old_state: "absent"`); its DELETE has only the key (`["9101", null, null]`, `old_state: "key"`).
- `reader-full.raw.jsonl` is unmodified stdout from a second clean fixture after `ALTER TABLE customers REPLICA IDENTITY FULL`. UPDATE and DELETE contain complete old tuples (`old_state: "full"`).
- `reader.normalized.jsonl` is mechanically derived from those raw files by `scripts/validate/article1_transcript.py`.
- `manifest.json` records the capture identity and SHA-256 digests.

The default and FULL captures are separate clean database resets. FULL is an explicit teaching contrast, not the fixture default.

## Exact capture environment

Capture source SHA: `30e9fe4ed6f0796b2aca8497b31d30f517f141c5` (includes preserved closure commit `30e9fe4`, merged M0 base `55078fec9`, and the provisional transport/CLI implementation).

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

## Consumer-row shape

Each line is one source event object. BEGIN exposes `event`, `final_lsn`, `wal_start`, `wal_end`, and transaction `{xid, commit_time}`. Row events expose `event`, `relation_id`, `new`, `old`, `old_state`, `wal_start`, `wal_end`, and transaction `{xid, ordinal}`. COMMIT exposes `event`, `commit_lsn`, `end_lsn`, `wal_start`, `wal_end`, `row_count`, and transaction `{commit_time}`. Tuple arrays follow the published `customers(id, name, tier)` relation order; JSON `null` placeholders in a key-only old tuple are not full old values.
