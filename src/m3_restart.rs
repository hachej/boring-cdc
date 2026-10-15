//! Existing-slot generation preparation owned by `boring-cdc-m3-restart`.

use crate::m2_schema::WriterConnection;
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use std::fmt;

const INSTALL_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS m3_restart_lower_stitches(
 restart_id TEXT PRIMARY KEY,
 owner_run_id TEXT NOT NULL,
 backfill_run_id TEXT NOT NULL REFERENCES backfill_runs(run_id),
 generation_id TEXT NOT NULL UNIQUE REFERENCES backfill_generations(generation_id),
 destination_id TEXT NOT NULL REFERENCES destinations(destination_id),
 capture_epoch TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK(generation>1),
 slot_name TEXT NOT NULL,
 configuration_fingerprint TEXT NOT NULL,
 table_set_fingerprint TEXT NOT NULL,
 source_transaction_id TEXT NOT NULL,
 lower_stitch_lsn TEXT NOT NULL CHECK(length(lower_stitch_lsn)=16 AND lower_stitch_lsn NOT GLOB '*[^0-9A-F]*'),
 lower_stitch_seq INTEGER NOT NULL CHECK(lower_stitch_seq>0),
 pin_id TEXT NOT NULL UNIQUE REFERENCES logical_range_pins(pin_id),
 FOREIGN KEY(source_transaction_id,capture_epoch,lower_stitch_seq,lower_stitch_lsn)
 REFERENCES source_transactions(transaction_id,capture_epoch,last_seq,end_lsn)
) STRICT;
CREATE TRIGGER IF NOT EXISTS m3_restart_lower_stitch_immutable
 BEFORE UPDATE ON m3_restart_lower_stitches BEGIN SELECT RAISE(ABORT,'lower stitch is immutable'); END;
CREATE TRIGGER IF NOT EXISTS m3_restart_lower_stitch_no_delete
 BEFORE DELETE ON m3_restart_lower_stitches BEGIN SELECT RAISE(ABORT,'lower stitch is immutable'); END;
"#;

#[derive(Clone, Copy)]
pub struct LowerStitchRequest<'a> {
    pub restart_id: &'a str,
    pub owner_run_id: &'a str,
    pub backfill_run_id: &'a str,
    pub generation_id: &'a str,
    pub destination_id: &'a str,
    pub capture_epoch: &'a str,
    pub generation: u64,
    pub slot_name: &'a str,
    pub configuration_fingerprint: &'a str,
    pub table_set_fingerprint: &'a str,
    pub now_mono_ms: u64,
}

#[derive(Debug, Eq, PartialEq)]
pub struct LowerStitch {
    pub restart_id: String,
    pub generation_id: String,
    pub source_transaction_id: String,
    pub lower_stitch_lsn: String,
    pub lower_stitch_seq: u64,
    pub pin_id: String,
    pub newly_pinned: bool,
}

struct StoredLower {
    owner: String,
    run: String,
    generation_id: String,
    destination_id: String,
    epoch: String,
    generation: i64,
    slot: String,
    configuration: String,
    table_set: String,
    transaction_id: String,
    sequence: i64,
    lsn: String,
}

pub enum RestartError {
    Stale,
    Store(rusqlite::Error),
}

impl From<rusqlite::Error> for RestartError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Store(value)
    }
}
impl fmt::Display for RestartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Stale => "M3_RESTART_LOWER_STALE",
            Self::Store(_) => "M3_RESTART_LOWER_STORE_FAILED",
        })
    }
}
impl fmt::Debug for RestartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

/// Pins the last complete durable transaction before an existing-slot snapshot is exported.
/// The caller must prove retained WAL has been drained before opening the snapshot.
pub fn prepare_lower_stitch(
    writer: &mut WriterConnection,
    request: LowerStitchRequest<'_>,
) -> Result<LowerStitch, RestartError> {
    if writer.attestation().run_id != request.owner_run_id
        || request.generation <= 1
        || [
            request.restart_id,
            request.owner_run_id,
            request.backfill_run_id,
            request.generation_id,
            request.destination_id,
            request.capture_epoch,
            request.slot_name,
            request.configuration_fingerprint,
            request.table_set_fingerprint,
        ]
        .iter()
        .any(|value| value.is_empty() || value.len() > 256 || value.chars().any(char::is_control))
    {
        return Err(RestartError::Stale);
    }
    let generation = i64::try_from(request.generation).map_err(|_| RestartError::Stale)?;
    let owner_connection_generation = i64::try_from(writer.attestation().connection_generation)
        .map_err(|_| RestartError::Stale)?;
    let now_mono_ms = i64::try_from(request.now_mono_ms).map_err(|_| RestartError::Stale)?;
    writer.connection().execute_batch(INSTALL_SQL)?;
    let tx = writer
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let live_owner: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM runtime_ownership WHERE run_id=?1 AND state='held'
         AND connection_generation=?2 AND ownership_deadline_mono_ms>=?3)",
        params![
            request.owner_run_id,
            owner_connection_generation,
            now_mono_ms
        ],
        |r| r.get(0),
    )?;
    if !live_owner {
        return Err(RestartError::Stale);
    }
    let existing: Option<StoredLower> = tx
        .query_row(
            "SELECT owner_run_id,backfill_run_id,generation_id,destination_id,capture_epoch,
             generation,slot_name,configuration_fingerprint,table_set_fingerprint,
             source_transaction_id,lower_stitch_seq,lower_stitch_lsn
             FROM m3_restart_lower_stitches WHERE restart_id=?1",
            [request.restart_id],
            |r| {
                Ok(StoredLower {
                    owner: r.get(0)?,
                    run: r.get(1)?,
                    generation_id: r.get(2)?,
                    destination_id: r.get(3)?,
                    epoch: r.get(4)?,
                    generation: r.get(5)?,
                    slot: r.get(6)?,
                    configuration: r.get(7)?,
                    table_set: r.get(8)?,
                    transaction_id: r.get(9)?,
                    sequence: r.get(10)?,
                    lsn: r.get(11)?,
                })
            },
        )
        .optional()?;
    if let Some(stored) = existing {
        let pin_id = pin_id(request.restart_id);
        let active_pin: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM logical_range_pins WHERE pin_id=?1
             AND owner_kind='backfill_generation' AND owner_id=?2 AND capture_epoch=?3
             AND start_seq=?4 AND state='active')",
            params![pin_id, stored.generation_id, stored.epoch, stored.sequence],
            |r| r.get(0),
        )?;
        let prepared_generation: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM backfill_generations WHERE generation_id=?1
             AND run_id=?2 AND generation=?3 AND state='prepared')",
            params![stored.generation_id, stored.run, stored.generation],
            |r| r.get(0),
        )?;
        if stored.owner != request.owner_run_id
            || stored.run != request.backfill_run_id
            || stored.generation_id != request.generation_id
            || stored.destination_id != request.destination_id
            || stored.epoch != request.capture_epoch
            || stored.generation != generation
            || stored.slot != request.slot_name
            || stored.configuration != request.configuration_fingerprint
            || stored.table_set != request.table_set_fingerprint
            || !prepared_generation
            || !active_pin
        {
            return Err(RestartError::Stale);
        }
        return Ok(LowerStitch {
            restart_id: request.restart_id.into(),
            generation_id: stored.generation_id,
            source_transaction_id: stored.transaction_id,
            lower_stitch_lsn: stored.lsn,
            lower_stitch_seq: stored.sequence as u64,
            pin_id,
            newly_pinned: false,
        });
    }
    let predecessor: Option<(i64, String)> = tx
        .query_row(
            "SELECT g.generation,g.state FROM backfill_generations g WHERE g.run_id=?1
         ORDER BY g.generation DESC LIMIT 1",
            [request.backfill_run_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if !matches!(predecessor, Some((previous, ref state)) if previous.checked_add(1) == Some(generation) && state == "invalidated")
    {
        return Err(RestartError::Stale);
    }
    let eligible_run: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM backfill_runs r JOIN destinations d
         ON d.destination_id=r.destination_id AND d.capture_epoch=r.capture_epoch
         WHERE r.run_id=?1 AND r.destination_id=?2 AND r.capture_epoch=?3
         AND r.state IN ('running','paused') AND d.configuration_fingerprint=?4)",
        params![
            request.backfill_run_id,
            request.destination_id,
            request.capture_epoch,
            request.configuration_fingerprint
        ],
        |r| r.get(0),
    )?;
    if !eligible_run {
        return Err(RestartError::Stale);
    }
    let durable: Option<(String, String, i64)> = tx
        .query_row(
            "SELECT s.durable_transaction_id,s.durable_transaction_end_lsn,s.durable_journal_seq
         FROM source_state s JOIN source_transactions t
         ON t.transaction_id=s.durable_transaction_id AND t.capture_epoch=s.capture_epoch
         AND t.last_seq=s.durable_journal_seq AND t.end_lsn=s.durable_transaction_end_lsn
         WHERE s.singleton=1 AND s.capture_epoch=?1 AND s.slot_name=?2 AND t.state='committed'
         AND s.durable_journal_seq>0",
            params![request.capture_epoch, request.slot_name],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let Some((transaction_id, lsn, seq)) = durable else {
        return Err(RestartError::Stale);
    };
    tx.execute(
        "INSERT INTO backfill_generations(generation_id,run_id,generation,state)
         VALUES(?1,?2,?3,'prepared')",
        params![request.generation_id, request.backfill_run_id, generation],
    )?;
    let pin_id = pin_id(request.restart_id);
    tx.execute(
        "INSERT INTO logical_range_pins(pin_id,owner_kind,owner_id,capture_epoch,start_seq,state)
         VALUES(?1,'backfill_generation',?2,?3,?4,'active')",
        params![pin_id, request.generation_id, request.capture_epoch, seq],
    )?;
    tx.execute(
        "INSERT INTO m3_restart_lower_stitches(restart_id,owner_run_id,backfill_run_id,
         generation_id,destination_id,capture_epoch,generation,slot_name,configuration_fingerprint,
         table_set_fingerprint,source_transaction_id,lower_stitch_lsn,lower_stitch_seq,pin_id)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
        params![
            request.restart_id,
            request.owner_run_id,
            request.backfill_run_id,
            request.generation_id,
            request.destination_id,
            request.capture_epoch,
            generation,
            request.slot_name,
            request.configuration_fingerprint,
            request.table_set_fingerprint,
            transaction_id,
            lsn,
            seq,
            pin_id
        ],
    )?;
    tx.commit()?;
    Ok(LowerStitch {
        restart_id: request.restart_id.into(),
        generation_id: request.generation_id.into(),
        source_transaction_id: transaction_id,
        lower_stitch_lsn: lsn,
        lower_stitch_seq: seq as u64,
        pin_id,
        newly_pinned: true,
    })
}

fn pin_id(restart_id: &str) -> String {
    format!("m3-lower:{:x}", Sha256::digest(restart_id.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m2_pressure::{GC_MAX_HOLD, automatic_gc};
    use crate::m2_schema::open_writer;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn fixture() -> WriterConnection {
        let path = std::env::temp_dir().join(format!(
            "m3-lower-{}-{}.db",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let writer = open_writer(&path, "owner", 1, 1).unwrap();
        writer.connection().execute_batch(
            "INSERT INTO source_transactions VALUES('tx1','epoch','sys','db','slot','1','0000000000000010',1,1,1,'sum1','committed');
             INSERT INTO source_transactions VALUES('tx2','epoch','sys','db','slot','2','0000000000000020',2,2,1,'sum2','committed');
             INSERT INTO journal_events(journal_seq,event_id,transaction_id,transaction_ordinal,capture_epoch,control_kind,payload,payload_hash)
               VALUES(1,'e1','tx1',0,'epoch','heartbeat',X'01','hash1');
             INSERT INTO journal_events(journal_seq,event_id,transaction_id,transaction_ordinal,capture_epoch,control_kind,payload,payload_hash)
               VALUES(2,'e2','tx2',0,'epoch','heartbeat',X'02','hash2');
             INSERT INTO source_state(singleton,capture_epoch,source_system_id,timeline_id,database_id,slot_name,plugin,publication_fingerprint,protocol_fingerprint,durable_transaction_end_lsn,durable_transaction_id,durable_journal_seq)
               VALUES(1,'epoch','sys','1','db','slot','pgoutput','pub','proto','0000000000000020','tx2',2);
             INSERT INTO runtime_ownership(run_id,backend_pid,connection_nonce,ownership_deadline_mono_ms,state,connection_generation)
               VALUES('owner',1,'nonce',1000,'held',1);
             INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation)
               VALUES('dest','archive','cfg','epoch',1);
             INSERT INTO backfill_runs(run_id,destination_id,capture_epoch,state) VALUES('backfill','dest','epoch','paused');
             INSERT INTO backfill_generations(generation_id,run_id,generation,state)
               VALUES('old','backfill',1,'invalidated');",
        ).unwrap();
        writer
    }

    fn request() -> LowerStitchRequest<'static> {
        LowerStitchRequest {
            restart_id: "restart",
            owner_run_id: "owner",
            backfill_run_id: "backfill",
            generation_id: "new",
            destination_id: "dest",
            capture_epoch: "epoch",
            generation: 2,
            slot_name: "slot",
            configuration_fingerprint: "cfg",
            table_set_fingerprint: "tables",
            now_mono_ms: 1,
        }
    }

    #[test]
    fn lower_pair_and_pin_precede_export_and_survive_gc() {
        let mut writer = fixture();
        let lower = prepare_lower_stitch(&mut writer, request()).unwrap();
        assert_eq!(lower.source_transaction_id, "tx2");
        assert_eq!(lower.lower_stitch_lsn, "0000000000000020");
        assert_eq!(lower.lower_stitch_seq, 2);
        assert!(lower.newly_pinned);
        let state: (String, i64, String) = writer
            .connection()
            .query_row(
                "SELECT g.state,p.start_seq,p.state FROM backfill_generations g
             JOIN logical_range_pins p ON p.owner_id=g.generation_id
             WHERE g.generation_id='new'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(state, ("prepared".into(), 2, "active".into()));
        let gc = automatic_gc(&mut writer, "epoch", 3, 10_000, 8, GC_MAX_HOLD).unwrap();
        assert_eq!(gc.retain_from_seq, 2);
        assert!(
            gc.blockers
                .iter()
                .any(|item| item == &format!("pin:{}", lower.pin_id))
        );
        let retained: String = writer
            .connection()
            .query_row(
                "SELECT state FROM source_transactions WHERE transaction_id='tx2'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(retained, "committed");
    }

    #[test]
    fn identical_retry_keeps_original_pair_after_capture_advances() {
        let mut writer = fixture();
        let first = prepare_lower_stitch(&mut writer, request()).unwrap();
        writer.connection().execute_batch(
            "INSERT INTO source_transactions VALUES('tx3','epoch','sys','db','slot','3','0000000000000030',3,3,1,'sum3','committed');
             INSERT INTO journal_events(journal_seq,event_id,transaction_id,transaction_ordinal,capture_epoch,control_kind,payload,payload_hash)
               VALUES(3,'e3','tx3',0,'epoch','heartbeat',X'03','hash3');
             UPDATE source_state SET durable_transaction_end_lsn='0000000000000030',
               durable_transaction_id='tx3',durable_journal_seq=3,control_revision=control_revision+1
               WHERE singleton=1;",
        ).unwrap();
        let retry = prepare_lower_stitch(&mut writer, request()).unwrap();
        assert_eq!(retry.lower_stitch_lsn, first.lower_stitch_lsn);
        assert_eq!(retry.lower_stitch_seq, first.lower_stitch_seq);
        assert!(!retry.newly_pinned);
        let mut changed = request();
        changed.table_set_fingerprint = "changed";
        assert!(matches!(
            prepare_lower_stitch(&mut writer, changed),
            Err(RestartError::Stale)
        ));
    }

    #[test]
    fn stale_owner_epoch_generation_or_missing_boundary_has_no_pin() {
        let mut writer = fixture();
        let mut stale = request();
        stale.owner_run_id = "former-owner";
        assert!(matches!(
            prepare_lower_stitch(&mut writer, stale),
            Err(RestartError::Stale)
        ));
        stale = request();
        stale.capture_epoch = "other";
        assert!(matches!(
            prepare_lower_stitch(&mut writer, stale),
            Err(RestartError::Stale)
        ));
        stale = request();
        stale.generation = 3;
        assert!(matches!(
            prepare_lower_stitch(&mut writer, stale),
            Err(RestartError::Stale)
        ));
        stale = request();
        stale.now_mono_ms = 1001;
        assert!(matches!(
            prepare_lower_stitch(&mut writer, stale),
            Err(RestartError::Stale)
        ));
        stale = request();
        stale.slot_name = "different-slot";
        assert!(matches!(
            prepare_lower_stitch(&mut writer, stale),
            Err(RestartError::Stale)
        ));
        let count: i64 = writer
            .connection()
            .query_row("SELECT count(*) FROM logical_range_pins", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn pin_failure_rolls_back_fenced_generation() {
        let mut writer = fixture();
        writer
            .connection()
            .execute_batch(
                "CREATE TRIGGER fail_pin BEFORE INSERT ON logical_range_pins
             BEGIN SELECT RAISE(ABORT,'injected pin failure'); END;",
            )
            .unwrap();
        assert!(matches!(
            prepare_lower_stitch(&mut writer, request()),
            Err(RestartError::Store(_))
        ));
        let generations: i64 = writer
            .connection()
            .query_row(
                "SELECT count(*) FROM backfill_generations WHERE generation_id='new'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let intents: i64 = writer
            .connection()
            .query_row("SELECT count(*) FROM m3_restart_lower_stitches", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!((generations, intents), (0, 0));
    }
}
