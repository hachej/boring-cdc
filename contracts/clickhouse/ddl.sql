-- boring-cdc ClickHouse contract v1; maintenance-owned, never issued by ordinary run.
CREATE DATABASE IF NOT EXISTS boring_cdc;

CREATE TABLE IF NOT EXISTS boring_cdc.event_history_v1
(
  capture_epoch FixedString(64), generation UInt64, logical_table_id FixedString(64),
  relation_schema_fingerprint FixedString(64), canonical_key String,
  connector_event_id FixedString(64), payload_hash FixedString(64), operation Enum8('snapshot'=0,'insert'=1,'update'=2,'delete'=3),
  origin_rank UInt8, commit_lsn_u64 UInt64, transaction_ordinal UInt32, mutation_ordinal UInt32,
  snapshot_anchor_seq UInt64, snapshot_chunk_ordinal UInt32, snapshot_row_ordinal UInt64,
  journal_seq UInt64, batch_id FixedString(64),
  columns Array(Tuple(column_id UInt32, state Enum8('absent_for_schema'=0,'explicit_null'=1,'unchanged_toast'=2,'explicit_value'=3), value_base64 String))
)
ENGINE = MergeTree
PARTITION BY (capture_epoch, generation, logical_table_id)
ORDER BY (capture_epoch, generation, logical_table_id, canonical_key, origin_rank, commit_lsn_u64, transaction_ordinal, mutation_ordinal, snapshot_anchor_seq, snapshot_chunk_ordinal, snapshot_row_ordinal, connector_event_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS boring_cdc.batch_markers_v1
(
  capture_epoch FixedString(64), generation UInt64, batch_id FixedString(64),
  first_journal_seq UInt64, last_journal_seq UInt64, event_count UInt64,
  ordered_event_digest FixedString(64), object_fingerprint FixedString(64), finalized_at_unix_ms UInt64
)
ENGINE = MergeTree
PARTITION BY (capture_epoch, generation)
ORDER BY (capture_epoch, generation, batch_id);

CREATE TABLE IF NOT EXISTS boring_cdc.generation_selectors_v1
(
  capture_epoch FixedString(64), promotion_fence UInt64, generation UInt64,
  table_set_fingerprint FixedString(64), compatible_anchor_id FixedString(64), candidate_digest FixedString(64)
)
ENGINE = MergeTree
PARTITION BY capture_epoch
ORDER BY (capture_epoch, promotion_fence, generation, candidate_digest);

CREATE VIEW IF NOT EXISTS boring_cdc.selector_conflicts_v1 AS
SELECT capture_epoch, promotion_fence, uniqExact(tuple(generation, table_set_fingerprint, compatible_anchor_id, candidate_digest)) AS candidates
FROM boring_cdc.generation_selectors_v1 GROUP BY capture_epoch, promotion_fence HAVING candidates > 1;

CREATE VIEW IF NOT EXISTS boring_cdc.live_generation_v1 AS
SELECT s.capture_epoch, max(s.promotion_fence) AS promotion_fence, any(s.generation) AS generation
FROM boring_cdc.generation_selectors_v1 AS s
INNER JOIN (SELECT capture_epoch, max(promotion_fence) AS promotion_fence FROM boring_cdc.generation_selectors_v1 GROUP BY capture_epoch) AS greatest
USING (capture_epoch, promotion_fence)
GROUP BY s.capture_epoch
HAVING uniqExact(tuple(s.generation,s.table_set_fingerprint,s.compatible_anchor_id,s.candidate_digest)) = 1;

CREATE VIEW IF NOT EXISTS boring_cdc.event_identity_conflicts_v1 AS
SELECT capture_epoch,generation,connector_event_id,uniqExact(payload_hash) AS payload_variants
FROM boring_cdc.event_history_v1 GROUP BY capture_epoch,generation,connector_event_id HAVING payload_variants > 1;
