-- Maintenance only. Caller must revalidate: not live; grace >= 86400s; no audit/anchor/replay pins; candidate complete.
ALTER TABLE boring_cdc.event_history_v1 DROP PARTITION tuple({capture_epoch:FixedString(64)},{generation:UInt64},{logical_table_id:FixedString(64)});
ALTER TABLE boring_cdc.batch_markers_v1 DROP PARTITION tuple({capture_epoch:FixedString(64)},{generation:UInt64});
-- Selector history is never mutated or deleted by this script.
