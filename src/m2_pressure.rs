//! Pin-safe retention, per-filesystem admission and bounded SQLite maintenance.

use crate::m2_schema::WriterConnection;
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use std::collections::BTreeMap;
use std::fmt;
use std::time::{Duration, Instant};

pub const GC_MAX_TRANSACTIONS: usize = 1_000;
pub const GC_MAX_HOLD: Duration = Duration::from_millis(50);
pub const INCREMENTAL_VACUUM_MAX_PAGES: u32 = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum PressureState {
    Normal,
    Warning,
    Action,
    Critical,
    Hard,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PressureThresholds {
    pub warning: u64,
    pub action: u64,
    pub critical: u64,
    pub hard: u64,
    pub reserve: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FilesystemBudget {
    pub filesystem_id: u64,
    pub total_bytes: u64,
    pub reserved_free_bytes: u64,
    /// True when this budget contains the journal, SQLite temp, or capture spool.
    pub capture_critical: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PhysicalFilesystemThresholds {
    pub filesystem_id: u64,
    pub thresholds: PressureThresholds,
    pub capture_critical: bool,
}

/// Convert configured used-capacity percentages into free-byte thresholds after grouping every
/// configured budget that resolves to the same physical filesystem. Grouping prevents state and
/// archive roots on one device from each assuming that they own the device's emergency space.
pub fn derive_physical_filesystem_thresholds(
    budgets: &[FilesystemBudget],
    percentages: [u64; 4],
) -> Result<Vec<PhysicalFilesystemThresholds>, PressureError> {
    let [warning, action, critical, hard] = percentages;
    if budgets.is_empty()
        || !(warning < action && action < critical && critical < hard && hard <= 100)
    {
        return Err(PressureError::Invalid(
            "invalid filesystem budgets or pressure percentages",
        ));
    }
    let mut grouped = BTreeMap::<u64, (u64, u64, bool)>::new();
    for budget in budgets {
        if budget.total_bytes == 0
            || budget.reserved_free_bytes == 0
            || budget.reserved_free_bytes > budget.total_bytes
        {
            return Err(PressureError::Invalid("invalid filesystem budget"));
        }
        let entry = grouped.entry(budget.filesystem_id).or_default();
        entry.0 = entry
            .0
            .checked_add(budget.total_bytes)
            .ok_or(PressureError::Invalid("filesystem budget overflow"))?;
        entry.1 = entry
            .1
            .checked_add(budget.reserved_free_bytes)
            .ok_or(PressureError::Invalid("filesystem reserve overflow"))?;
        entry.2 |= budget.capture_critical;
    }
    grouped
        .into_iter()
        .map(|(filesystem_id, (total, reserve, capture_critical))| {
            let usable = total
                .checked_sub(reserve)
                .ok_or(PressureError::Invalid("filesystem reserve exceeds budget"))?;
            let free_at = |used_percent: u64| -> Result<u64, PressureError> {
                let remaining = (usable as u128)
                    .checked_mul((100 - used_percent) as u128)
                    .ok_or(PressureError::Invalid("pressure threshold overflow"))?
                    / 100;
                reserve
                    .checked_add(remaining as u64)
                    .ok_or(PressureError::Invalid("pressure threshold overflow"))
            };
            let thresholds = PressureThresholds {
                warning: free_at(warning)?,
                action: free_at(action)?,
                critical: free_at(critical)?,
                hard: free_at(hard)?,
                reserve,
            };
            // Small or badly proportioned budgets can collapse distinct percentage boundaries.
            // Reject rather than silently changing transition ordering.
            decide_pressure(thresholds.warning, thresholds)?;
            Ok(PhysicalFilesystemThresholds {
                filesystem_id,
                thresholds,
                capture_critical,
            })
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PressureActions {
    pub throttle_backfill: bool,
    pub finish_snapshot_generations: bool,
    pub automatic_gc: bool,
    pub stop_new_archive_backfill: bool,
    pub drain_materializers: bool,
    pub safe_stop_capture: bool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PressureDecision {
    pub state: PressureState,
    pub actions: PressureActions,
    pub free_bytes: u64,
    pub required_free_bytes: u64,
}

#[derive(Debug, Eq, PartialEq)]
pub enum PressureError {
    Invalid(&'static str),
    ReserveExceeded,
    StalePin,
    Busy,
    Sqlite(String),
}
impl fmt::Display for PressureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for PressureError {}
impl From<rusqlite::Error> for PressureError {
    fn from(v: rusqlite::Error) -> Self {
        Self::Sqlite(v.to_string())
    }
}

pub fn decide_pressure(
    free_bytes: u64,
    t: PressureThresholds,
) -> Result<PressureDecision, PressureError> {
    if !(t.warning > t.action
        && t.action > t.critical
        && t.critical > t.hard
        && t.hard >= t.reserve
        && t.reserve > 0)
    {
        return Err(PressureError::Invalid(
            "pressure thresholds must strictly descend to reserve",
        ));
    }
    let state = if free_bytes <= t.hard {
        PressureState::Hard
    } else if free_bytes <= t.critical {
        PressureState::Critical
    } else if free_bytes <= t.action {
        PressureState::Action
    } else if free_bytes <= t.warning {
        PressureState::Warning
    } else {
        PressureState::Normal
    };
    Ok(PressureDecision {
        state,
        free_bytes,
        required_free_bytes: t.reserve,
        actions: PressureActions {
            throttle_backfill: state >= PressureState::Warning,
            finish_snapshot_generations: state >= PressureState::Action,
            automatic_gc: state >= PressureState::Action,
            stop_new_archive_backfill: state >= PressureState::Critical,
            drain_materializers: state >= PressureState::Critical,
            safe_stop_capture: state >= PressureState::Hard,
        },
    })
}

/// Evaluate every distinct physical filesystem and return the decision requiring the most
/// conservative system action. A separate archive device can therefore drive pressure even while
/// the state device remains normal.
pub fn decide_pressure_filesystems(
    observations: &[(u64, PressureThresholds)],
) -> Result<(usize, PressureDecision), PressureError> {
    let mut worst = None;
    for (index, &(free, thresholds)) in observations.iter().enumerate() {
        let decision = decide_pressure(free, thresholds)?;
        if worst
            .as_ref()
            .is_none_or(|(_, current): &(usize, PressureDecision)| decision.state > current.state)
        {
            worst = Some((index, decision));
        }
    }
    worst.ok_or(PressureError::Invalid(
        "no pressure filesystem observations",
    ))
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FilesystemReservations {
    pub spool: u64,
    pub sqlite_growth: u64,
    pub sqlite_temp: u64,
    pub backfill: u64,
    pub archive_segment: u64,
    pub publish_directory: u64,
    pub metadata: u64,
}
pub fn admit_filesystem(
    free: u64,
    reserve: u64,
    r: FilesystemReservations,
) -> Result<u64, PressureError> {
    if reserve == 0 {
        return Err(PressureError::Invalid("zero emergency reserve"));
    }
    let required = [
        r.spool,
        r.sqlite_growth,
        r.sqlite_temp,
        r.backfill,
        r.archive_segment,
        r.publish_directory,
        r.metadata,
    ]
    .into_iter()
    .try_fold(reserve, u64::checked_add)
    .ok_or(PressureError::Invalid("reservation overflow"))?;
    if free < required {
        Err(PressureError::ReserveExceeded)
    } else {
        Ok(required)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PinSpec<'a> {
    pub pin_id: &'a str,
    pub owner_kind: &'a str,
    pub owner_id: &'a str,
    pub capture_epoch: &'a str,
    pub start_seq: u64,
    pub end_seq: Option<u64>,
    pub expires_at_unix_ms: Option<i64>,
}
pub fn create_pin(writer: &mut WriterConnection, pin: PinSpec<'_>) -> Result<(), PressureError> {
    if [pin.pin_id, pin.owner_kind, pin.owner_id, pin.capture_epoch]
        .iter()
        .any(|v| v.is_empty())
        || pin.start_seq == 0
        || pin.end_seq.is_some_and(|v| v < pin.start_seq)
    {
        return Err(PressureError::Invalid("invalid logical pin"));
    }
    writer.connection().execute("INSERT INTO logical_range_pins(pin_id,owner_kind,owner_id,capture_epoch,start_seq,end_seq,expires_at_unix_ms,state) VALUES(?1,?2,?3,?4,?5,?6,?7,'active')",params![pin.pin_id,pin.owner_kind,pin.owner_id,pin.capture_epoch,pin.start_seq as i64,pin.end_seq.map(|v|v as i64),pin.expires_at_unix_ms])?;
    Ok(())
}
pub fn request_pin_release(
    writer: &mut WriterConnection,
    pin_id: &str,
    revision: u64,
) -> Result<(), PressureError> {
    let n=writer.connection().execute("UPDATE logical_range_pins SET state='release_pending',revision=revision+1 WHERE pin_id=?1 AND state='active' AND revision=?2",params![pin_id,revision as i64])?;
    if n == 1 {
        Ok(())
    } else {
        Err(PressureError::StalePin)
    }
}
pub fn confirm_pin_release(
    writer: &mut WriterConnection,
    pin_id: &str,
    revision: u64,
) -> Result<(), PressureError> {
    let tx = writer
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let owner:Option<(String,String)>=tx.query_row("SELECT owner_kind,owner_id FROM logical_range_pins WHERE pin_id=?1 AND state='release_pending' AND revision=?2",params![pin_id,revision as i64],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    let Some((kind, id)) = owner else {
        return Err(PressureError::StalePin);
    };
    let terminal=match kind.as_str(){
      "destination"=>!tx.query_row("SELECT EXISTS(SELECT 1 FROM destinations WHERE destination_id=?1)",[&id],|r|r.get::<_,bool>(0))?,
      "backfill_generation"=>tx.query_row("SELECT state IN ('complete','invalidated') FROM backfill_generations WHERE generation_id=?1",[&id],|r|r.get::<_,bool>(0)).optional()?.unwrap_or(false),
      "bootstrap_intent"=>tx.query_row("SELECT state IN ('complete','invalidated','aborted') FROM bootstrap_intents WHERE intent_id=?1",[&id],|r|r.get::<_,bool>(0)).optional()?.unwrap_or(false),
      "reseed_intent"=>tx.query_row("SELECT state IN ('complete','aborted') FROM reseed_intents WHERE intent_id=?1",[&id],|r|r.get::<_,bool>(0)).optional()?.unwrap_or(false),
      "lease"=>tx.query_row("SELECT state IN ('fenced','expired','released') FROM destination_generation_leases WHERE lease_id=?1",[&id],|r|r.get::<_,bool>(0)).optional()?.unwrap_or(false),
      "promotion_intent"=>tx.query_row("SELECT state='retired' FROM destination_promotion_intents WHERE intent_id=?1",[&id],|r|r.get::<_,bool>(0)).optional()?.unwrap_or(false),
      "clickhouse_intent"=>tx.query_row("SELECT state IN ('verified','failed') FROM clickhouse_batch_intents WHERE intent_id=?1",[&id],|r|r.get::<_,bool>(0)).optional()?.unwrap_or(false),
      "archive_intent"=>tx.query_row("SELECT state IN ('published','failed') FROM archive_segment_intents WHERE intent_id=?1",[&id],|r|r.get::<_,bool>(0)).optional()?.unwrap_or(false),
      "audit"=>tx.query_row("SELECT journal_cursor_seq>=round_target_seq AND self_cursor_seq>=round_target_seq FROM destination_audits WHERE audit_id=?1",[&id],|r|r.get::<_,bool>(0)).optional()?.unwrap_or(false),
      _=>false,
    };
    if !terminal {
        return Err(PressureError::StalePin);
    }
    tx.execute("UPDATE logical_range_pins SET state='released',revision=revision+1 WHERE pin_id=?1 AND state='release_pending' AND revision=?2",params![pin_id,revision as i64])?;
    tx.commit()?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PressureObservation<'a> {
    pub free_bytes: u64,
    pub capture_epoch: &'a str,
    pub replay_from_seq: u64,
    pub replay_cutoff_unix_ms: Option<i64>,
    pub now_unix_ms: i64,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PressureServiceResult {
    pub decision: PressureDecision,
    pub gc: Option<GcResult>,
    pub checkpoint: Option<CheckpointProgress>,
}
/// The run-owned maintenance tick: evaluate actions and execute automatic GC/checkpoint work.
/// Callers must invoke this from the journal writer's reserved essential-service turn.
pub fn service_pressure_tick(
    writer: &mut WriterConnection,
    thresholds: PressureThresholds,
    observation: PressureObservation<'_>,
) -> Result<PressureServiceResult, PressureError> {
    let decision = decide_pressure(observation.free_bytes, thresholds)?;
    let replay_from_seq = if let Some(cutoff) = observation.replay_cutoff_unix_ms {
        replay_floor_for_cutoff(writer, observation.capture_epoch, cutoff)?
    } else {
        observation.replay_from_seq
    };
    let gc = if decision.actions.automatic_gc {
        Some(automatic_gc(
            writer,
            observation.capture_epoch,
            replay_from_seq,
            observation.now_unix_ms,
            GC_MAX_TRANSACTIONS,
            GC_MAX_HOLD,
        )?)
    } else {
        None
    };
    let checkpoint = if decision.state >= PressureState::Action {
        Some(checkpoint_restart(writer)?)
    } else {
        None
    };
    if decision.state >= PressureState::Action {
        incremental_vacuum(writer, INCREMENTAL_VACUUM_MAX_PAGES)?;
    }
    Ok(PressureServiceResult {
        decision,
        gc,
        checkpoint,
    })
}

pub fn replay_floor_for_cutoff(
    writer: &WriterConnection,
    epoch: &str,
    cutoff_unix_ms: i64,
) -> Result<u64, PressureError> {
    let missing:bool=writer.connection().query_row("SELECT EXISTS(SELECT 1 FROM source_transactions t LEFT JOIN journal_retention_clock c USING(transaction_id) WHERE t.capture_epoch=?1 AND t.state='committed' AND c.transaction_id IS NULL)",[epoch],|r|r.get(0))?;
    if missing {
        return Ok(1);
    }
    let floor:Option<i64>=writer.connection().query_row("SELECT min(t.first_seq) FROM source_transactions t JOIN journal_retention_clock c USING(transaction_id) WHERE t.capture_epoch=?1 AND t.state='committed' AND c.committed_at_unix_ms>=?2",params![epoch,cutoff_unix_ms],|r|r.get(0))?;
    if let Some(v) = floor {
        return Ok(v as u64);
    }
    let newest:i64=writer.connection().query_row("SELECT coalesce(max(last_seq),0)+1 FROM source_transactions WHERE capture_epoch=?1 AND state='committed'",[epoch],|r|r.get(0))?;
    Ok(newest.max(1) as u64)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcResult {
    pub transactions: u64,
    pub events: u64,
    pub first_seq: Option<u64>,
    pub last_seq: Option<u64>,
    pub retain_from_seq: u64,
    pub blockers: Vec<String>,
}
/// Automatic runtime GC. Eligibility is recomputed inside the same IMMEDIATE transaction as deletion.
pub fn automatic_gc(
    writer: &mut WriterConnection,
    epoch: &str,
    replay_from_seq: u64,
    now_ms: i64,
    max_transactions: usize,
    max_hold: Duration,
) -> Result<GcResult, PressureError> {
    if epoch.is_empty()
        || replay_from_seq == 0
        || max_transactions == 0
        || max_transactions > GC_MAX_TRANSACTIONS
        || max_hold.is_zero()
        || max_hold > GC_MAX_HOLD
    {
        return Err(PressureError::Invalid("invalid GC bound"));
    }
    let started = Instant::now();
    writer.connection().busy_timeout(max_hold)?;
    let tx = writer
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    // Expiry is intentionally not release: owner reconciliation must complete the two-step lifecycle.
    let mut floors = vec![replay_from_seq];
    let mut blockers = Vec::new();
    {
        let mut s=tx.prepare("SELECT d.destination_id,c.journal_seq FROM destinations d JOIN destination_checkpoints c USING(destination_id) WHERE d.capture_epoch=?1")?;
        for row in s.query_map([epoch], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })? {
            let (id, seq) = row?;
            floors.push((seq as u64).saturating_add(1));
            blockers.push(format!("destination:{id}"));
        }
    }
    {
        let mut s=tx.prepare("SELECT pin_id,start_seq,expires_at_unix_ms FROM logical_range_pins WHERE capture_epoch=?1 AND state!='released'")?;
        for row in s.query_map([epoch], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, Option<i64>>(2)?,
            ))
        })? {
            let (id, seq, expires) = row?;
            floors.push(seq as u64);
            blockers.push(if expires.is_some_and(|v| v <= now_ms) {
                format!("expired_pin_requires_reconciliation:{id}")
            } else {
                format!("pin:{id}")
            });
        }
    }
    {
        let mut s=tx.prepare("SELECT anchor_id,start_seq,state FROM bootstrap_anchors WHERE capture_epoch=?1 AND state IN ('building','complete')")?;
        for row in s.query_map([epoch], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
            ))
        })? {
            let (id, seq, state) = row?;
            floors.push((seq as u64).max(1));
            blockers.push(format!("anchor:{state}:{id}"));
        }
    }
    // Nonterminal intents are implicit pins even if a worker died before creating its logical pin.
    {
        let mut s=tx.prepare("SELECT intent_id,first_seq FROM clickhouse_batch_intents WHERE capture_epoch=?1 AND state IN ('prepared','dispatched')")?;
        for row in s.query_map([epoch], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })? {
            let (id, seq) = row?;
            floors.push(seq as u64);
            blockers.push(format!("clickhouse_intent:{id}"));
        }
    }
    {
        let mut s=tx.prepare("SELECT i.intent_id,i.first_seq FROM archive_segment_intents i JOIN archive_generations g ON g.generation_id=i.generation_id WHERE g.capture_epoch=?1 AND i.state IN ('selected','writing')")?;
        for row in s.query_map([epoch], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })? {
            let (id, seq) = row?;
            floors.push(seq as u64);
            blockers.push(format!("archive_intent:{id}"));
        }
    }
    {
        let mut s=tx.prepare("SELECT audit_id,coalesce(retained_history_start_seq,journal_cursor_seq) FROM destination_audits WHERE capture_epoch=?1 AND journal_cursor_seq<round_target_seq")?;
        for row in s.query_map([epoch], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })? {
            let (id, seq) = row?;
            floors.push((seq as u64).max(1));
            blockers.push(format!("audit:{id}"));
        }
    }
    let retain = *floors.iter().min().unwrap();
    let mut selected = Vec::new();
    {
        let mut s=tx.prepare("SELECT transaction_id,first_seq,last_seq,event_count FROM source_transactions WHERE capture_epoch=?1 AND state='committed' AND last_seq<?2 ORDER BY first_seq LIMIT ?3")?;
        let rows = s.query_map(
            params![epoch, retain as i64, max_transactions as i64],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                ))
            },
        )?;
        for row in rows {
            selected.push(row?);
            if started.elapsed() > max_hold {
                return Err(PressureError::Busy);
            }
        }
    }
    if selected.is_empty() {
        tx.commit()?;
        return Ok(GcResult {
            transactions: 0,
            events: 0,
            first_seq: None,
            last_seq: None,
            retain_from_seq: retain,
            blockers,
        });
    }
    let first = selected[0].1;
    let last = selected.last().unwrap().2;
    let events: i64 = selected.iter().map(|v| v.3).sum();
    // Range is contiguous by construction; transaction metadata remains as bounded proof while payload rows go.
    for (id, _, _, _) in &selected {
        tx.execute("DELETE FROM journal_events WHERE transaction_id=?1", [id])?;
        tx.execute(
            "UPDATE source_transactions SET state='gc_removed' WHERE transaction_id=?1",
            [id],
        )?;
    }
    tx.execute("INSERT INTO journal_gc_audits(capture_epoch,first_seq,last_seq,transaction_count,reason,created_at_unix_ms) VALUES(?1,?2,?3,?4,'automatic_pin_safe',?5)",params![epoch,first,last,selected.len() as i64,now_ms])?;
    if started.elapsed() > max_hold {
        return Err(PressureError::Busy);
    }
    tx.commit()?;
    Ok(GcResult {
        transactions: selected.len() as u64,
        events: events as u64,
        first_seq: Some(first as u64),
        last_seq: Some(last as u64),
        retain_from_seq: retain,
        blockers,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataRetention<'a> {
    pub alerts_rows: u64,
    pub audit_rows: u64,
    pub completed_command_cutoff: &'a str,
    pub invalid_generation_cutoff_ms: i64,
    pub retired_generation_cutoff_ms: i64,
    pub orphan_cutoff_ms: i64,
    pub batch_max: usize,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MetadataGcResult {
    pub alerts: u64,
    pub audits: u64,
    pub commands: u64,
    pub invalid_generations: u64,
    pub retired_generations: u64,
    pub orphan_diagnostics: u64,
}
pub fn bounded_metadata_gc(
    writer: &mut WriterConnection,
    limits: MetadataRetention<'_>,
) -> Result<MetadataGcResult, PressureError> {
    if limits.batch_max == 0
        || limits.batch_max > GC_MAX_TRANSACTIONS
        || limits.alerts_rows == 0
        || limits.audit_rows == 0
        || limits.completed_command_cutoff.is_empty()
    {
        return Err(PressureError::Invalid("invalid metadata retention"));
    }
    let tx = writer
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let n = limits.batch_max as i64;
    let alerts=tx.execute("DELETE FROM alerts WHERE rowid IN (SELECT rowid FROM alerts WHERE state='cleared' ORDER BY rowid LIMIT ?1) AND (SELECT count(*) FROM alerts)>?2",params![n,limits.alerts_rows as i64])? as u64;
    let audit_count = tx.query_row("SELECT count(*) FROM destination_audits", [], |row| {
        row.get::<_, u64>(0)
    })?;
    let audit_limit = audit_count
        .saturating_sub(limits.audit_rows)
        .min(limits.batch_max as u64) as i64;
    let audit_ids = {
        let mut statement = tx.prepare(
            "SELECT a.audit_id FROM destination_audits a
             JOIN terminal_metadata_retention m
               ON m.category='destination_audit' AND m.object_id=a.audit_id
             WHERE a.journal_cursor_seq>=a.round_target_seq
               AND a.self_cursor_seq>=a.round_target_seq
             ORDER BY m.terminal_at_unix_ms,a.audit_id LIMIT ?1",
        )?;
        statement
            .query_map([audit_limit], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut audits = 0;
    let mut remaining_pin_rows = limits.batch_max;
    for audit_id in audit_ids {
        // The owner index and shared row budget keep reconciliation bounded even when corrupt or
        // legacy state contains many pins for one audit. The audit remains until a later tick has
        // released every pin, and each tick is one IMMEDIATE transaction.
        let pin_ids = {
            let mut statement = tx.prepare(
                "SELECT pin_id FROM logical_range_pins
                 WHERE owner_kind='audit' AND owner_id=?1 AND state!='released'
                 ORDER BY pin_id LIMIT ?2",
            )?;
            statement
                .query_map(params![audit_id, remaining_pin_rows as i64], |row| {
                    row.get::<_, String>(0)
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        remaining_pin_rows -= pin_ids.len();
        for pin_id in pin_ids {
            // Advancing active pins through release_pending preserves the lifecycle trigger and
            // also reconciles a release already requested by another maintenance turn.
            tx.execute(
                "UPDATE logical_range_pins SET state='release_pending',revision=revision+1
                 WHERE pin_id=?1 AND state='active'",
                [&pin_id],
            )?;
            tx.execute(
                "UPDATE logical_range_pins SET state='released',revision=revision+1
                 WHERE pin_id=?1 AND state='release_pending'",
                [&pin_id],
            )?;
        }
        let has_unreleased = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM logical_range_pins
             WHERE owner_kind='audit' AND owner_id=?1 AND state!='released')",
            [&audit_id],
            |row| row.get::<_, bool>(0),
        )?;
        if has_unreleased {
            continue;
        }
        let deleted = tx.execute(
            "DELETE FROM destination_audits WHERE audit_id=?1
             AND journal_cursor_seq>=round_target_seq AND self_cursor_seq>=round_target_seq",
            [&audit_id],
        )?;
        if deleted == 1 {
            tx.execute(
                "DELETE FROM terminal_metadata_retention
                 WHERE category='destination_audit' AND object_id=?1",
                [&audit_id],
            )?;
            audits += 1;
        }
    }
    let commands=tx.execute("DELETE FROM operator_command_requests WHERE request_id IN (SELECT request_id FROM operator_command_requests WHERE state IN ('completed','failed','aborted_by_restart') AND expires_at<?1 ORDER BY expires_at LIMIT ?2)",params![limits.completed_command_cutoff,n])? as u64;
    let invalid_generations=tx.execute("DELETE FROM backfill_generations WHERE generation_id IN (SELECT g.generation_id FROM backfill_generations g JOIN terminal_metadata_retention m ON m.category='invalid_generation' AND m.object_id=g.generation_id WHERE g.state='invalidated' AND m.terminal_at_unix_ms<?1 ORDER BY m.terminal_at_unix_ms LIMIT ?2)",params![limits.invalid_generation_cutoff_ms,n])? as u64;
    let retired_generations=tx.execute("DELETE FROM archive_generations WHERE generation_id IN (SELECT g.generation_id FROM archive_generations g JOIN terminal_metadata_retention m ON m.category='retired_generation' AND m.object_id=g.generation_id WHERE g.state IN ('retired','invalidated') AND m.terminal_at_unix_ms<?1 ORDER BY m.terminal_at_unix_ms LIMIT ?2)",params![limits.retired_generation_cutoff_ms,n])? as u64;
    let orphan_diagnostics=tx.execute("DELETE FROM orphan_diagnostics WHERE diagnostic_id IN (SELECT diagnostic_id FROM orphan_diagnostics WHERE state='resolved' AND resolved_at_unix_ms<?1 ORDER BY resolved_at_unix_ms LIMIT ?2)",params![limits.orphan_cutoff_ms,n])? as u64;
    tx.commit()?;
    Ok(MetadataGcResult {
        alerts,
        audits,
        commands,
        invalid_generations,
        retired_generations,
        orphan_diagnostics,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointProgress {
    pub busy: u32,
    pub wal_pages: u32,
    pub checkpointed_pages: u32,
}
pub fn checkpoint_restart(
    writer: &mut WriterConnection,
) -> Result<CheckpointProgress, PressureError> {
    // M0-PROVISIONAL: boring-cdc-m2-pressure -- frozen storage contract literal.
    writer
        .connection()
        .busy_timeout(Duration::from_millis(50))?;
    let result = writer
        .connection()
        .query_row("PRAGMA wal_checkpoint(RESTART)", [], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
            ))
        });
    writer.connection().busy_timeout(Duration::from_secs(5))?;
    let (busy, wal, done) = result?;
    Ok(CheckpointProgress {
        busy: busy as u32,
        wal_pages: wal.max(0) as u32,
        checkpointed_pages: done.max(0) as u32,
    })
}
pub fn incremental_vacuum(writer: &mut WriterConnection, pages: u32) -> Result<(), PressureError> {
    if pages == 0 || pages > INCREMENTAL_VACUUM_MAX_PAGES {
        return Err(PressureError::Invalid("invalid incremental vacuum bound"));
    }
    writer
        .connection()
        .execute_batch(&format!("PRAGMA incremental_vacuum({pages})"))?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalRisk {
    pub seconds_to_cap: Option<u64>,
    pub requires_reseed: bool,
    pub metrics_available: bool,
}
pub fn forecast_wal_risk(
    retained: u64,
    cap: u64,
    rate_per_sec: Option<u64>,
    monitor_delay_sec: u64,
    reaction_reserve_sec: u64,
) -> WalRisk {
    let Some(rate) = rate_per_sec.filter(|v| *v > 0) else {
        return WalRisk {
            seconds_to_cap: None,
            requires_reseed: retained >= cap,
            metrics_available: false,
        };
    };
    let remaining = cap.saturating_sub(retained);
    let secs = remaining / rate;
    WalRisk {
        seconds_to_cap: Some(secs),
        requires_reseed: retained >= cap
            || secs <= monitor_delay_sec.saturating_add(reaction_reserve_sec),
        metrics_available: true,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::m2_schema::open_writer;
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(1);
    fn writer() -> (WriterConnection, std::path::PathBuf) {
        let p = std::env::temp_dir().join(format!(
            "m2-pressure-{}-{}.db",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        (open_writer(&p, "run", 1, 0).unwrap(), p)
    }
    fn seed(w: &WriterConnection) {
        for i in 1..=5 {
            w.connection().execute("INSERT INTO source_transactions VALUES(?1,'epoch','sys','db','slot',?2,?3,?4,?4,1,?5,'committed')",params![format!("tx{i}"),format!("x{i}"),format!("{i:016X}"),i,format!("sum{i}")]).unwrap();
            w.connection().execute("INSERT INTO journal_events(journal_seq,event_id,transaction_id,transaction_ordinal,capture_epoch,control_kind,payload,payload_hash) VALUES(?1,?2,?3,0,'epoch','heartbeat',x'01','hash')",params![i,format!("e{i}"),format!("tx{i}")]).unwrap();
        }
    }
    #[test]
    fn transition_order_and_actions_are_exact() {
        let t = PressureThresholds {
            warning: 100,
            action: 80,
            critical: 60,
            hard: 40,
            reserve: 40,
        };
        let states = [101, 100, 80, 60, 40].map(|f| decide_pressure(f, t).unwrap().state);
        assert_eq!(
            states,
            [
                PressureState::Normal,
                PressureState::Warning,
                PressureState::Action,
                PressureState::Critical,
                PressureState::Hard
            ]
        );
        assert!(decide_pressure(40, t).unwrap().actions.safe_stop_capture);
        assert!(decide_pressure(60, t).unwrap().actions.drain_materializers);
    }
    #[test]
    fn configured_thresholds_group_shared_devices_and_preserve_boundaries() {
        let budgets = [
            FilesystemBudget {
                filesystem_id: 7,
                total_bytes: 1_000,
                reserved_free_bytes: 100,
                capture_critical: true,
            },
            FilesystemBudget {
                filesystem_id: 7,
                total_bytes: 2_000,
                reserved_free_bytes: 200,
                capture_critical: false,
            },
            FilesystemBudget {
                filesystem_id: 9,
                total_bytes: 2_000,
                reserved_free_bytes: 200,
                capture_critical: false,
            },
        ];
        let grouped = derive_physical_filesystem_thresholds(&budgets, [60, 75, 90, 100]).unwrap();
        assert_eq!(grouped.len(), 2);
        assert_eq!(
            grouped[0],
            PhysicalFilesystemThresholds {
                filesystem_id: 7,
                thresholds: PressureThresholds {
                    warning: 1_380,
                    action: 975,
                    critical: 570,
                    hard: 300,
                    reserve: 300,
                },
                capture_critical: true,
            }
        );
        let separate = grouped[1].thresholds;
        assert_eq!((separate.warning, separate.hard), (920, 200));
        assert!(!grouped[1].capture_critical);
        let (index, worst) = decide_pressure_filesystems(&[
            (grouped[0].thresholds.warning + 1, grouped[0].thresholds),
            (separate.hard, separate),
        ])
        .unwrap();
        assert_eq!(index, 1);
        assert_eq!(worst.state, PressureState::Hard);
        assert_eq!(worst.required_free_bytes, 200);
        assert_eq!(
            decide_pressure(grouped[0].thresholds.warning, grouped[0].thresholds)
                .unwrap()
                .state,
            PressureState::Warning
        );
        assert_eq!(
            decide_pressure(grouped[0].thresholds.warning + 1, grouped[0].thresholds)
                .unwrap()
                .state,
            PressureState::Normal
        );
        assert!(derive_physical_filesystem_thresholds(&budgets, [60, 75, 90, 101]).is_err());
    }

    #[test]
    fn per_filesystem_reserve_equal_admits_and_one_over_rejects() {
        let r = FilesystemReservations {
            spool: 2,
            sqlite_growth: 3,
            ..Default::default()
        };
        assert_eq!(admit_filesystem(15, 10, r), Ok(15));
        assert_eq!(
            admit_filesystem(14, 10, r),
            Err(PressureError::ReserveExceeded)
        );
    }
    #[test]
    fn pin_lifecycle_and_gc_preserve_whole_transactions() {
        let (mut w, p) = writer();
        seed(&w);
        create_pin(
            &mut w,
            PinSpec {
                pin_id: "p",
                owner_kind: "clickhouse_intent",
                owner_id: "intent",
                capture_epoch: "epoch",
                start_seq: 4,
                end_seq: None,
                expires_at_unix_ms: Some(1),
            },
        )
        .unwrap();
        let g = automatic_gc(&mut w, "epoch", 6, 2, 10, GC_MAX_HOLD).unwrap();
        assert_eq!(
            (g.transactions, g.last_seq, g.retain_from_seq),
            (3, Some(3), 4)
        );
        assert!(
            g.blockers
                .contains(&"expired_pin_requires_reconciliation:p".into())
        );
        assert_eq!(
            w.connection()
                .query_row("SELECT count(*) FROM journal_events", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
        request_pin_release(&mut w, "p", 0).unwrap();
        assert_eq!(
            confirm_pin_release(&mut w, "p", 1),
            Err(PressureError::StalePin)
        );
        w.connection().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('d','clickhouse','cfg','epoch',1)",[]).unwrap();
        w.connection().execute("INSERT INTO clickhouse_batch_intents VALUES('intent','d','epoch',1,4,4,'sum','verified')",[]).unwrap();
        confirm_pin_release(&mut w, "p", 1).unwrap();
        let g = automatic_gc(&mut w, "epoch", 6, 2, 10, GC_MAX_HOLD).unwrap();
        assert_eq!(g.transactions, 2);
        drop(w);
        let _ = std::fs::remove_file(p);
    }
    #[test]
    fn paused_destination_is_a_visible_retention_pin() {
        let (mut w, p) = writer();
        seed(&w);
        w.connection().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('paused','archive','cfg','epoch',1)",[]).unwrap();
        w.connection().execute("INSERT INTO destination_checkpoints(destination_id,capture_epoch,configuration_fingerprint,generation,complete_transaction_id,journal_seq) VALUES('paused','epoch','cfg',1,'tx2',2)",[]).unwrap();
        let g = automatic_gc(&mut w, "epoch", 6, 0, 10, GC_MAX_HOLD).unwrap();
        assert_eq!(g.last_seq, Some(2));
        assert!(g.blockers.contains(&"destination:paused".into()));
        drop(w);
        let _ = std::fs::remove_file(p);
    }
    #[test]
    fn maintenance_is_bounded_and_never_full_vacuum() {
        let (mut w, p) = writer();
        let expired =
            crate::m2_schema::open_reader_with_limits(&p, Duration::from_millis(1), 10).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        assert!(
            expired
                .query_one_bounded("SELECT count(*) FROM schema_migrations", |r| r
                    .get::<_, i64>(0))
                .is_err()
        );
        drop(expired);
        let reader = rusqlite::Connection::open(&p).unwrap();
        reader
            .execute_batch("BEGIN; SELECT count(*) FROM schema_migrations;")
            .unwrap();
        w.connection()
            .execute("CREATE TABLE checkpoint_probe(value INTEGER)", [])
            .unwrap();
        let blocked = checkpoint_restart(&mut w).unwrap();
        assert_eq!(blocked.busy, 1);
        assert!(blocked.wal_pages >= blocked.checkpointed_pages);
        reader.execute_batch("ROLLBACK").unwrap();
        drop(reader);
        assert_eq!(checkpoint_restart(&mut w).unwrap().busy, 0);
        incremental_vacuum(&mut w, INCREMENTAL_VACUUM_MAX_PAGES).unwrap();
        assert!(matches!(
            incremental_vacuum(&mut w, 1001),
            Err(PressureError::Invalid(_))
        ));
        let sql: String = w
            .connection()
            .query_row(
                "SELECT group_concat(sql,' ') FROM sqlite_schema WHERE sql IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(!sql.contains("VACUUM"));
        drop(w);
        let _ = std::fs::remove_file(p);
    }
    #[test]
    fn metadata_gc_is_terminal_bounded_and_preserves_active_rows() {
        let (mut w, p) = writer();
        for i in 0..4 {
            w.connection().execute("INSERT INTO alerts(alert_id,condition_id,state,opened_at) VALUES(?1,'pressure',?2,'2020')",params![format!("a{i}"),if i==0{"active"}else{"cleared"}]).unwrap();
        }
        let limits = MetadataRetention {
            alerts_rows: 2,
            audit_rows: 2,
            completed_command_cutoff: "2025",
            invalid_generation_cutoff_ms: 1,
            retired_generation_cutoff_ms: 1,
            orphan_cutoff_ms: 1,
            batch_max: 1,
        };
        let result = bounded_metadata_gc(&mut w, limits).unwrap();
        assert_eq!(result.alerts, 1);
        assert_eq!(
            w.connection()
                .query_row(
                    "SELECT count(*) FROM alerts WHERE state='active'",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        assert_eq!(
            w.connection()
                .query_row("SELECT count(*) FROM alerts", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            3
        );
        drop(w);
        let _ = std::fs::remove_file(p);
    }
    #[test]
    fn audit_metadata_gc_atomically_releases_active_and_pending_pins() {
        let (mut w, p) = writer();
        w.connection().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('d','clickhouse','cfg','epoch',1)",[]).unwrap();
        for (audit_id, terminal_at, terminal) in [
            ("old", 1_i64, true),
            ("next", 2, true),
            ("active", 3, false),
        ] {
            let cursor = if terminal { 10 } else { 9 };
            w.connection().execute("INSERT INTO destination_audits(audit_id,destination_id,configuration_fingerprint,capture_epoch,generation,round_target_seq,round_identity_digest,journal_cursor_seq,self_cursor_seq,budget_bytes_used,budget_events_used,budget_ms_used,freshness_window_started_at,freshness_expires_at,contract_digest) VALUES(?1,'d','cfg','epoch',1,10,'round',?2,?2,0,0,0,'2026','2027','contract')",params![audit_id,cursor]).unwrap();
            w.connection().execute("INSERT INTO terminal_metadata_retention(category,object_id,terminal_at_unix_ms) VALUES('destination_audit',?1,?2)",params![audit_id,terminal_at]).unwrap();
            create_pin(
                &mut w,
                PinSpec {
                    pin_id: audit_id,
                    owner_kind: "audit",
                    owner_id: audit_id,
                    capture_epoch: "epoch",
                    start_seq: 1,
                    end_seq: Some(10),
                    expires_at_unix_ms: None,
                },
            )
            .unwrap();
        }
        request_pin_release(&mut w, "next", 0).unwrap();
        let limits = MetadataRetention {
            alerts_rows: 1,
            audit_rows: 1,
            completed_command_cutoff: "2025",
            invalid_generation_cutoff_ms: 1,
            retired_generation_cutoff_ms: 1,
            orphan_cutoff_ms: 1,
            batch_max: 1,
        };

        assert_eq!(
            bounded_metadata_gc(&mut w, limits.clone()).unwrap().audits,
            1
        );
        assert_eq!(
            w.connection()
                .query_row(
                    "SELECT state FROM logical_range_pins WHERE pin_id='old'",
                    [],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
            "released"
        );
        assert_eq!(bounded_metadata_gc(&mut w, limits).unwrap().audits, 1);
        assert_eq!(
            w.connection()
                .query_row(
                    "SELECT state FROM logical_range_pins WHERE pin_id='next'",
                    [],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
            "released"
        );
        assert_eq!(
            w.connection()
                .query_row(
                    "SELECT state FROM logical_range_pins WHERE pin_id='active'",
                    [],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
            "active"
        );
        assert_eq!(
            w.connection()
                .query_row(
                    "SELECT group_concat(audit_id,',') FROM destination_audits",
                    [],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
            "active"
        );
        assert_eq!(
            w.connection()
                .query_row("SELECT count(*) FROM terminal_metadata_retention WHERE category='destination_audit'", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        drop(w);
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn audit_pin_reconciliation_bounds_many_owner_pins_per_tick() {
        let (mut w, p) = writer();
        w.connection().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('d','clickhouse','cfg','epoch',1)",[]).unwrap();
        for (audit_id, terminal_at) in [("many", 1_i64), ("keeper", 2)] {
            w.connection().execute("INSERT INTO destination_audits(audit_id,destination_id,configuration_fingerprint,capture_epoch,generation,round_target_seq,round_identity_digest,journal_cursor_seq,self_cursor_seq,budget_bytes_used,budget_events_used,budget_ms_used,freshness_window_started_at,freshness_expires_at,contract_digest) VALUES(?1,'d','cfg','epoch',1,10,'round',10,10,0,0,0,'2026','2027','contract')",[audit_id]).unwrap();
            w.connection().execute("INSERT INTO terminal_metadata_retention(category,object_id,terminal_at_unix_ms) VALUES('destination_audit',?1,?2)",params![audit_id,terminal_at]).unwrap();
        }
        for pin_id in ["pin-1", "pin-2", "pin-3"] {
            create_pin(
                &mut w,
                PinSpec {
                    pin_id,
                    owner_kind: "audit",
                    owner_id: "many",
                    capture_epoch: "epoch",
                    start_seq: 1,
                    end_seq: Some(10),
                    expires_at_unix_ms: None,
                },
            )
            .unwrap();
        }
        let limits = MetadataRetention {
            alerts_rows: 1,
            audit_rows: 1,
            completed_command_cutoff: "2025",
            invalid_generation_cutoff_ms: 1,
            retired_generation_cutoff_ms: 1,
            orphan_cutoff_ms: 1,
            batch_max: 1,
        };
        for released in 1..=2 {
            assert_eq!(
                bounded_metadata_gc(&mut w, limits.clone()).unwrap().audits,
                0
            );
            assert_eq!(
                w.connection()
                    .query_row("SELECT count(*) FROM logical_range_pins WHERE owner_id='many' AND state='released'", [], |r| r.get::<_, i64>(0))
                    .unwrap(),
                released
            );
        }
        assert_eq!(bounded_metadata_gc(&mut w, limits).unwrap().audits, 1);
        assert_eq!(
            w.connection()
                .query_row("SELECT count(*) FROM logical_range_pins WHERE owner_id='many' AND state='released'", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            3
        );
        assert_eq!(
            w.connection()
                .query_row(
                    "SELECT count(*) FROM destination_audits WHERE audit_id='many'",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        drop(w);
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn audit_pin_release_rolls_back_when_metadata_delete_fails() {
        let (mut w, p) = writer();
        w.connection().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('d','clickhouse','cfg','epoch',1)",[]).unwrap();
        for audit_id in ["blocked", "keeper"] {
            w.connection().execute("INSERT INTO destination_audits(audit_id,destination_id,configuration_fingerprint,capture_epoch,generation,round_target_seq,round_identity_digest,journal_cursor_seq,self_cursor_seq,budget_bytes_used,budget_events_used,budget_ms_used,freshness_window_started_at,freshness_expires_at,contract_digest) VALUES(?1,'d','cfg','epoch',1,10,'round',10,10,0,0,0,'2026','2027','contract')",[audit_id]).unwrap();
            w.connection().execute("INSERT INTO terminal_metadata_retention(category,object_id,terminal_at_unix_ms) VALUES('destination_audit',?1,1)",[audit_id]).unwrap();
        }
        create_pin(
            &mut w,
            PinSpec {
                pin_id: "blocked-pin",
                owner_kind: "audit",
                owner_id: "blocked",
                capture_epoch: "epoch",
                start_seq: 1,
                end_seq: Some(10),
                expires_at_unix_ms: None,
            },
        )
        .unwrap();
        w.connection().execute_batch("CREATE TRIGGER reject_blocked_audit BEFORE DELETE ON destination_audits WHEN OLD.audit_id='blocked' BEGIN SELECT RAISE(ABORT,'injected delete failure'); END;").unwrap();
        let result = bounded_metadata_gc(
            &mut w,
            MetadataRetention {
                alerts_rows: 1,
                audit_rows: 1,
                completed_command_cutoff: "2025",
                invalid_generation_cutoff_ms: 1,
                retired_generation_cutoff_ms: 1,
                orphan_cutoff_ms: 1,
                batch_max: 1,
            },
        );
        assert!(result.is_err());
        assert_eq!(
            w.connection()
                .query_row(
                    "SELECT state FROM logical_range_pins WHERE pin_id='blocked-pin'",
                    [],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
            "active"
        );
        assert_eq!(
            w.connection()
                .query_row(
                    "SELECT count(*) FROM destination_audits WHERE audit_id='blocked'",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        drop(w);
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn runtime_service_tick_executes_action_gc_and_maintenance() {
        let (mut w, p) = writer();
        seed(&w);
        let thresholds = PressureThresholds {
            warning: 100,
            action: 80,
            critical: 60,
            hard: 40,
            reserve: 40,
        };
        let result = service_pressure_tick(
            &mut w,
            thresholds,
            PressureObservation {
                free_bytes: 80,
                capture_epoch: "epoch",
                replay_from_seq: 6,
                replay_cutoff_unix_ms: None,
                now_unix_ms: 1,
            },
        )
        .unwrap();
        assert_eq!(result.decision.state, PressureState::Action);
        assert_eq!(result.gc.unwrap().transactions, 5);
        assert!(result.checkpoint.is_some());
        drop(w);
        let _ = std::fs::remove_file(p);
    }
    #[test]
    fn wal_forecast_is_honest() {
        assert!(forecast_wal_risk(900, 1000, Some(1), 10, 120).requires_reseed);
        assert!(!forecast_wal_risk(1, 1000, None, 10, 120).metrics_available);
    }
}
