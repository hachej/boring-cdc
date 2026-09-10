/* Canonical current state. Application predicates replace the parameters. FINAL is intentionally absent. */
WITH
selected AS (SELECT generation,promotion_fence FROM boring_cdc.live_generation_v1 WHERE capture_epoch={capture_epoch:FixedString(64)}),
guard AS (SELECT throwIf((SELECT count() FROM boring_cdc.selector_conflicts_v1 WHERE capture_epoch={capture_epoch:FixedString(64)})>0 OR (SELECT count() FROM boring_cdc.event_identity_conflicts_v1 WHERE capture_epoch={capture_epoch:FixedString(64)} AND generation=(SELECT generation FROM selected))>0, 'BCDC_CH_INTEGRITY_CONFLICT') AS ok,
dedup AS
(
 SELECT *, uniqExact(payload_hash) OVER (PARTITION BY capture_epoch,generation,connector_event_id) AS payload_variants,
           row_number() OVER (PARTITION BY capture_epoch,generation,connector_event_id,payload_hash ORDER BY connector_event_id) AS replay_number
 FROM boring_cdc.event_history_v1
 WHERE capture_epoch={capture_epoch:FixedString(64)} AND generation=(SELECT generation FROM selected)
   AND logical_table_id={logical_table_id:FixedString(64)}
), valid AS (SELECT * FROM dedup WHERE payload_variants=1 AND replay_number=1),
latest_operation AS
(
 SELECT canonical_key, argMax(operation, tuple(origin_rank,commit_lsn_u64,transaction_ordinal,mutation_ordinal,snapshot_anchor_seq,snapshot_chunk_ordinal,snapshot_row_ordinal,connector_event_id)) AS operation
 FROM valid GROUP BY canonical_key
), exploded AS
(
 SELECT canonical_key, cell.1 AS column_id, cell.2 AS state, cell.3 AS value_base64,
        tuple(origin_rank,commit_lsn_u64,transaction_ordinal,mutation_ordinal,snapshot_anchor_seq,snapshot_chunk_ordinal,snapshot_row_ordinal,connector_event_id) AS source_version
 FROM valid ARRAY JOIN columns AS cell
), cells AS
(
 SELECT canonical_key, column_id, argMaxIf(tuple(state,value_base64), source_version, state IN ('explicit_null','explicit_value')) AS value
 FROM exploded GROUP BY canonical_key,column_id
), rows AS
(
 SELECT o.canonical_key,o.operation,arraySort(x->x.1,groupArray((c.column_id,c.value.1,c.value.2))) AS explicit_cells
 FROM latest_operation o LEFT JOIN cells c USING canonical_key CROSS JOIN guard GROUP BY o.canonical_key,o.operation
)
SELECT canonical_key,explicit_cells FROM rows WHERE operation != 'delete' ORDER BY canonical_key;
