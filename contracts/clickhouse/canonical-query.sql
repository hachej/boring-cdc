/* Canonical current state; FINAL is intentionally absent. Run the two conflict views as a mandatory preflight. */
WITH selected AS (SELECT generation,promotion_fence FROM boring_cdc.live_generation_v1 WHERE capture_epoch={capture_epoch:FixedString(64)}),
dedup AS
(SELECT *,uniqExact(payload_hash) OVER (PARTITION BY capture_epoch,generation,connector_event_id) payload_variants,row_number() OVER (PARTITION BY capture_epoch,generation,connector_event_id,payload_hash ORDER BY connector_event_id) replay_number FROM boring_cdc.event_history_v1 WHERE capture_epoch={capture_epoch:FixedString(64)} AND generation=(SELECT generation FROM selected) AND logical_table_id={logical_table_id:FixedString(64)}),
valid AS (SELECT * FROM dedup WHERE payload_variants=1 AND replay_number=1),
exploded0 AS
(SELECT canonical_key,cell.1 column_id,cell.2 state,cell.3 type_oid,cell.4 typmod,cell.5 value_base64,tuple(lsn_u64,origin_rank,transaction_ordinal,mutation_ordinal,connector_event_id) source_version FROM valid ARRAY JOIN columns cell),
exploded AS
(SELECT *,countIf(state IN ('explicit_null','explicit_value')) OVER (PARTITION BY canonical_key,column_id ORDER BY source_version ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) predecessor_count FROM exploded0),
invalid_toast AS (SELECT count() invalid_count FROM exploded WHERE state='unchanged_toast' AND predecessor_count=0),
guard AS (SELECT throwIf((SELECT count() FROM boring_cdc.selector_conflicts_v1 WHERE capture_epoch={capture_epoch:FixedString(64)})>0 OR (SELECT count() FROM boring_cdc.event_identity_conflicts_v1 WHERE capture_epoch={capture_epoch:FixedString(64)} AND generation=(SELECT generation FROM selected))>0 OR (SELECT invalid_count FROM invalid_toast)>0,'BCDC_CH_INTEGRITY_CONFLICT') ok),
latest_operation AS (SELECT canonical_key,argMax(operation,tuple(lsn_u64,origin_rank,transaction_ordinal,mutation_ordinal,connector_event_id)) operation FROM valid GROUP BY canonical_key),
cells AS (SELECT canonical_key,column_id,argMaxIf(tuple(state,type_oid,typmod,value_base64),source_version,state IN ('explicit_null','explicit_value')) value FROM exploded GROUP BY canonical_key,column_id),
rows AS (SELECT o.canonical_key,o.operation,arraySort(x->x.1,groupArray((c.column_id,c.value.1,c.value.2,c.value.3,c.value.4))) explicit_cells FROM latest_operation o LEFT JOIN cells c USING canonical_key CROSS JOIN guard GROUP BY o.canonical_key,o.operation)
SELECT canonical_key,explicit_cells FROM rows WHERE operation!='delete' ORDER BY canonical_key;
