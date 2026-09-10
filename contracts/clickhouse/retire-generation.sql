-- Maintenance only after revalidating non-live, grace >= 86400s, and no reader/replay/audit/anchor/intent pins.
ALTER TABLE boring_cdc.event_history_v1 DROP PARTITION tuple({capture_epoch:FixedString(64)},{generation:UInt64});
ALTER TABLE boring_cdc.batch_markers_v1 DROP PARTITION tuple({capture_epoch:FixedString(64)},{generation:UInt64});
-- generation_selectors_v1 is append-only and never retired.
