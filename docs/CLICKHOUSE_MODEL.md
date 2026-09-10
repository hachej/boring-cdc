# ClickHouse model v1

**Canonical artifact:** `contracts/clickhouse/model.json` (`ART-M0-CLICKHOUSE-MODEL`). Owner confirmation on 2026-09-10 accepted every recommended value in cards `5a994cfd` and `765bd3b2`. This document is explanatory; JSON, SQL, schemas, fixtures, and their registered hashes are executable authority.

## Boundary and objects

The destination is one pinned ClickHouse `25.8.2.29` server on `linux/amd64`; it is explicitly non-HA. Maintenance pre-creates `event_history_v1`, `batch_markers_v1`, `generation_selectors_v1`, `selector_conflicts_v1`, `live_generation_v1`, and the event-conflict view using `contracts/clickhouse/ddl.sql`. Ordinary `run` issues no DDL. History and selectors are append-only `MergeTree` data. There are deliberately no materialized views: independently failing derived writes cannot enter the acceptance boundary. External dictionaries, PostgreSQL/source joins, and source repair queries are forbidden; correctness joins use only pinned local objects in one epoch/generation. Replacing/Collapsing engines, native narrow versions, mutation commands, unsafe TTL, and DDL-based promotion are forbidden.

## Dimensions, identity, order, and deduplication

Rows retain `capture_epoch`, `generation`, `logical_table_id`, `relation_schema_fingerprint`, and the complete canonical key. Event identity is `(connector_event_id, payload_hash)`. Identical repetitions converge logically; one event ID with multiple payload hashes is an integrity failure, not “last wins.” Source comparison uses exact `(lsn_u64, origin_rank, transaction_ordinal, mutation_ordinal, connector_event_id)` and never compares capture epochs. `journal_seq` is delivery/audit position, not source order.

`contracts/clickhouse/canonical-query.sql` resolves the unique greatest promotion fence, rejects selector and event-ID conflicts before serving, selects the latest operation per key, and reconstructs each column from its latest explicit value/null. `unchanged_toast` carries the nearest earlier explicit state; no source lookup is permitted. `absent_for_schema` projects null for the sole compatible nullable/no-default addition. A missing predecessor blocks. A latest delete hides the row while preserving its tombstone and predecessors. A key change is old-key delete plus new-key upsert; unchanged TOAST anywhere in such a change blocks.

Normal queries use the canonical view/query without `FINAL`. Correctness is invariant before, during, and after background merges. `FINAL` is neither required nor sufficient and has no special correctness authority.

## Durable batch acceptance and recovery

For an exact complete-transaction journal range, the worker persists an immutable SQLite intent, inserts history with synchronous settings, drives the Rust insert body/future through successful end-of-stream finalization, and reads back exact count, identity/payload digest, and zero conflicts. Only then does it insert and read back an identical batch marker and atomically advance the ClickHouse checkpoint/finalized intent in SQLite. No marker replays the same intent; a valid marker is adopted; conflicting history/marker blocks. Checkpoints never skip a failed transaction.

The fixed settings are `async_insert=0`, `wait_for_async_insert=1`, `insert_quorum=1`, `fsync_after_insert=1`, `fsync_directories=1`, and `insert_deduplicate=0`. They and every correctness-bearing object/query form the object fingerprint. Drift blocks only ClickHouse.

## Promotion and schema evolution

A candidate is promotable only with a compatible complete anchor, post-copy fence, every selected logical table, exact table-set fingerprint and watermark, and no selector conflict. Selectors are append-only. Greatest fence wins; lower fences are no-ops; an identical same-fence retry is harmless; a distinct candidate at the same fence is corruption. External state above restored SQLite blocks until maintenance verifies/adopts the unique high fence or rejects it. No DDL switches live generations.

Only a nullable, non-generated, no-default addition outside active backfill is compatible. Active-backfill change invalidates the candidate. Type/key/default/generated/non-null/removal changes block the whole destination at the failed transaction and require a complete fresh generation/re-seed. Migration creates versioned objects, dual-reads and verifies them, then adopts their fingerprint; it never mutates published history.

## Audit, quota, and retirement

Each read-only audit freezes epoch, generation, selector fence, destination checkpoint, object fingerprint, and event-contract digest. A pass is capped at 64 MiB, 100,000 events, and 5 seconds, runs every 300 seconds, and expires after 900 seconds. The cursor is `(logical_table_id, journal_seq, connector_event_id, column_offset)`. Oversized admitted events resume at a bounded column offset with versioned hash state. Identity change discards the partial round. Gaps, insufficient service, or expiry are explicit partial/unknown coverage.

The audit reads actual key and key hash, before key, operation and mutation kind, exact source-version tuple, schema identity, and positional tagged values including type OID and typmod, recomputes the canonical payload hash, and compares it with both stored and retained-journal hashes. Thus payload-only corruption with unchanged IDs, stored hashes, and markers blocks ClickHouse with evidence. It does not move the live checkpoint or read PostgreSQL.

History warns at 64 GiB and blocks ClickHouse before a new insert at 80 GiB or below a 10 GiB free-space reserve. It never self-deletes required history. Telemetry includes free/history/temporary-part bytes, active parts, merge queue/bytes/amplification, retired bytes, and quota state. Only maintenance may execute `retire-generation.sql`, after 86,400 seconds and proof that the generation is non-live and unpinned by readers, replay, anchors, intents, or audits. Selector history is retained.

## Failures, fixtures, and downstream execution

ClickHouse references the shared persisted `FailurePolicy` (v1, 250 ms base, 30,000 ms cap, 10 attempts); it does not duplicate scheduling. Transient due times survive restart. Deterministic, contract, configuration, integrity, and exhausted failures block without hot-loop or checkpoint skip. Resume revalidates the corrected cause and unchanged boundary. Capture and archive continue within their own limits.

`fixtures/m0/clickhouse/scenarios.json` specifies deterministic merge/TOAST/delete/order/dedup, durability, audit, corruption, promotion, quota, retirement, schema, retry, and recovery cases. `boring-cdc-m4-ddl`, `boring-cdc-m4-durability`, and `boring-cdc-m4-promotion` execute them later. M0 claims specification validation only, never runtime ClickHouse results.
