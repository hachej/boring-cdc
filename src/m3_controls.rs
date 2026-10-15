//! Backfill control transitions owned by `boring-cdc-m3-controls`.

use crate::m2_schema::WriterConnection;
use crate::m3_fence::invalidate_and_release_in_transaction;
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use std::fmt;

#[derive(Clone, Copy)]
pub struct PauseRequest<'a> {
    pub owner_run_id: &'a str,
    pub backfill_run_id: &'a str,
    pub generation_id: &'a str,
    pub capture_epoch: &'a str,
    pub generation: u64,
    pub configuration_fingerprint: &'a str,
    pub table_set_fingerprint: &'a str,
    pub run_revision: u64,
    pub bootstrap_revision: u64,
    pub now_mono_ms: u64,
}

#[derive(Debug, Eq, PartialEq)]
pub struct PauseReceipt {
    pub bootstrap_intent_id: String,
    pub generation_id: String,
    pub generation: u64,
    pub newly_paused: bool,
}

pub enum PauseError {
    Stale,
    Store(rusqlite::Error),
}

impl fmt::Display for PauseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Stale => "M3_PAUSE_STALE",
            Self::Store(_) => "M3_PAUSE_STORE_FAILED",
        })
    }
}

impl fmt::Debug for PauseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

struct PauseState {
    run_state: String,
    generation_state: String,
    run_revision: i64,
    generation: i64,
    intent_id: String,
    bootstrap_state: String,
    bootstrap_revision: i64,
    configuration: String,
    table_set: String,
    promotable: i64,
    guard_liveness: String,
    gate_state: String,
}

impl From<rusqlite::Error> for PauseError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Store(value)
    }
}

/// The owner commits pause and gate release together before closing its matching source sessions.
/// A caller must never act on a receipt whose intent and generation do not match its live sessions.
pub fn pause_generation(
    writer: &mut WriterConnection,
    request: PauseRequest<'_>,
) -> Result<PauseReceipt, PauseError> {
    if writer.attestation().run_id != request.owner_run_id
        || request.generation == 0
        || [
            request.owner_run_id,
            request.backfill_run_id,
            request.generation_id,
            request.capture_epoch,
            request.configuration_fingerprint,
            request.table_set_fingerprint,
        ]
        .iter()
        .any(|value| value.is_empty())
    {
        return Err(PauseError::Stale);
    }
    let expected_generation = i64::try_from(request.generation).map_err(|_| PauseError::Stale)?;
    let expected_run_revision =
        i64::try_from(request.run_revision).map_err(|_| PauseError::Stale)?;
    let expected_bootstrap_revision =
        i64::try_from(request.bootstrap_revision).map_err(|_| PauseError::Stale)?;
    let next_run_revision = expected_run_revision
        .checked_add(1)
        .ok_or(PauseError::Stale)?;
    let next_bootstrap_revision = expected_bootstrap_revision
        .checked_add(1)
        .ok_or(PauseError::Stale)?;
    let now_mono_ms = i64::try_from(request.now_mono_ms).map_err(|_| PauseError::Stale)?;
    let owner_connection_generation =
        i64::try_from(writer.attestation().connection_generation).map_err(|_| PauseError::Stale)?;
    let tx = writer
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let row: Option<PauseState> = tx
        .query_row(
            "SELECT r.state,g.state,r.revision,g.generation,p.bootstrap_intent_id,x.state,x.revision,x.configuration_fingerprint,x.table_set_fingerprint,x.snapshot_promotable,x.guard_liveness,f.state \
             FROM backfill_runs r JOIN backfill_generations g ON g.run_id=r.run_id \
             JOIN m3_planner_runs p ON p.run_id=r.run_id AND p.generation_id=g.generation_id \
             JOIN m3_bootstrap_runtime x ON x.intent_id=p.bootstrap_intent_id AND x.generation=g.generation \
             JOIN m3_feedback_gates f ON f.intent_id=x.intent_id AND f.generation=x.generation \
             JOIN destinations d ON d.destination_id=r.destination_id AND d.capture_epoch=r.capture_epoch AND d.generation=g.generation \
             JOIN source_state s ON s.singleton=1 AND s.capture_epoch=r.capture_epoch \
             JOIN runtime_ownership o ON o.run_id=?1 AND o.state='held' AND o.ownership_deadline_mono_ms>=?7 AND o.connection_generation=?8 \
             WHERE r.run_id=?2 AND g.generation_id=?3 AND r.capture_epoch=?4 \
             AND g.generation=?5 AND d.configuration_fingerprint=?6 \
             AND NOT EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.generation_id=g.generation_id AND a.state='complete')",
            params![request.owner_run_id, request.backfill_run_id, request.generation_id,
                request.capture_epoch, expected_generation, request.configuration_fingerprint,
                now_mono_ms, owner_connection_generation],
            |r| Ok(PauseState {
                run_state: r.get(0)?,
                generation_state: r.get(1)?,
                run_revision: r.get(2)?,
                generation: r.get(3)?,
                intent_id: r.get(4)?,
                bootstrap_state: r.get(5)?,
                bootstrap_revision: r.get(6)?,
                configuration: r.get(7)?,
                table_set: r.get(8)?,
                promotable: r.get(9)?,
                guard_liveness: r.get(10)?,
                gate_state: r.get(11)?,
            }),
        )
        .optional()?;
    let Some(PauseState {
        run_state,
        generation_state,
        run_revision,
        generation,
        intent_id,
        bootstrap_state,
        bootstrap_revision,
        configuration,
        table_set,
        promotable,
        guard_liveness,
        gate_state,
    }) = row
    else {
        return Err(PauseError::Stale);
    };
    if configuration != request.configuration_fingerprint
        || table_set != request.table_set_fingerprint
        || generation != expected_generation
    {
        return Err(PauseError::Stale);
    }
    if run_state == "paused"
        && generation_state == "invalidated"
        && bootstrap_state == "snapshot_unusable"
        && gate_state == "invalidated"
        && run_revision == next_run_revision
        && bootstrap_revision == next_bootstrap_revision
    {
        return Ok(PauseReceipt {
            bootstrap_intent_id: intent_id,
            generation_id: request.generation_id.into(),
            generation: request.generation,
            newly_paused: false,
        });
    }
    if run_state != "running"
        || !matches!(generation_state.as_str(), "copying" | "fencing")
        || bootstrap_state != "exporter_released"
        || run_revision != expected_run_revision
        || bootstrap_revision != expected_bootstrap_revision
        || promotable != 1
        || guard_liveness != "held"
        || !matches!(gate_state.as_str(), "hold" | "open")
    {
        return Err(PauseError::Stale);
    }
    invalidate_and_release_in_transaction(&tx, &intent_id, request.generation, "operator_pause")
        .map_err(|_| PauseError::Stale)?;
    let changed = tx.execute(
        "UPDATE backfill_runs SET state='paused',revision=revision+1 WHERE run_id=?1 AND state='running' AND revision=?2",
        params![request.backfill_run_id, expected_run_revision],
    )?;
    if changed != 1 {
        return Err(PauseError::Stale);
    }
    tx.commit()?;
    Ok(PauseReceipt {
        bootstrap_intent_id: intent_id,
        generation_id: request.generation_id.into(),
        generation: request.generation,
        newly_paused: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m2_schema::{WriterConnection, open_writer};
    use crate::m3_bootstrap::BootstrapStore;
    use crate::m3_planner::{PlannerLimits, PlannerStore};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn fixture() -> WriterConnection {
        let path = std::env::temp_dir().join(format!(
            "m3-pause-{}-{}.db",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let writer = open_writer(&path, "owner", 1, 1).unwrap();
        drop(BootstrapStore::open(writer).unwrap());
        let writer = open_writer(&path, "owner", 1, 1).unwrap();
        drop(
            PlannerStore::open(
                writer,
                PlannerLimits {
                    chunk_rows: 1,
                    chunk_bytes: 1024,
                    chunk_duration: Duration::from_secs(1),
                    writer_hold: Duration::from_secs(1),
                    concurrency: 1,
                    source_impact_bytes: 1024,
                    max_rows_per_second: 1,
                },
            )
            .unwrap(),
        );
        let writer = open_writer(&path, "owner", 1, 1).unwrap();
        writer.connection().execute_batch(
            "INSERT INTO source_state(singleton,capture_epoch,source_system_id,timeline_id,database_id,slot_name,plugin,publication_fingerprint,protocol_fingerprint) VALUES(1,'epoch','sys','1','db','slot','pgoutput','pub','proto');
             INSERT INTO runtime_ownership(run_id,backend_pid,connection_nonce,ownership_deadline_mono_ms,state,connection_generation) VALUES('owner',1,'nonce',1000,'held',1);
             INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('dest','archive','cfg','epoch',1);
             INSERT INTO bootstrap_intents VALUES('boot','epoch','sys','db','slot','0000000000000001','slot_created',0,'now');
             INSERT INTO m3_bootstrap_runtime(intent_id,generation,table_set_fingerprint,configuration_fingerprint,snapshot_token,consistent_lsn,start_seq,exporter_liveness,guard_liveness,snapshot_promotable,feedback_gate_open,state,revision)
               VALUES('boot',1,'tables','cfg','token','0000000000000001',0,'released','held',1,1,'exporter_released',0);
             INSERT INTO m3_bootstrap_importers(intent_id,importer_id,assigned_ranges_digest,snapshot_schema_fingerprint,state) VALUES('boot','worker','ranges','schema','acknowledged');
             INSERT INTO m3_feedback_gates(intent_id,generation,state) VALUES('boot',1,'open');
             INSERT INTO backfill_runs(run_id,destination_id,capture_epoch,state) VALUES('backfill','dest','epoch','running');
             INSERT INTO backfill_generations(generation_id,run_id,generation,state) VALUES('gen','backfill',1,'copying');
             INSERT INTO backfill_chunks(chunk_id,generation_id,range_start,range_end,state,completed_seq,checksum) VALUES('done','gen',X'00',X'01','complete',1,'sum');
             INSERT INTO backfill_chunks(chunk_id,generation_id,range_start,range_end,state) VALUES('pending','gen',X'01',X'02','pending');
             INSERT INTO m3_planner_runs(run_id,generation_id,bootstrap_intent_id,importer_id,snapshot_schema_fingerprint,key_schema,key_schema_digest,estimated_rows,started_mono_ms,next_snapshot_seq,max_concurrency)
               VALUES('backfill','gen','boot','worker','schema',X'01','key',2,1,2,1);",
        ).unwrap();
        writer
    }

    fn request() -> PauseRequest<'static> {
        PauseRequest {
            owner_run_id: "owner",
            backfill_run_id: "backfill",
            generation_id: "gen",
            capture_epoch: "epoch",
            generation: 1,
            configuration_fingerprint: "cfg",
            table_set_fingerprint: "tables",
            run_revision: 0,
            bootstrap_revision: 0,
            now_mono_ms: 1,
        }
    }

    #[test]
    fn pause_atomically_invalidates_generation_and_releases_gate() {
        let mut writer = fixture();
        let receipt = pause_generation(&mut writer, request()).unwrap();
        assert_eq!(receipt.bootstrap_intent_id, "boot");
        assert!(receipt.newly_paused);
        let state: (String, String, String, String, String, String) = writer
            .connection()
            .query_row(
                "SELECT r.state,g.state,x.state,f.state,
                 (SELECT state FROM backfill_chunks WHERE chunk_id='done'),
                 (SELECT state FROM backfill_chunks WHERE chunk_id='pending')
                 FROM backfill_runs r JOIN backfill_generations g ON g.run_id=r.run_id
                 JOIN m3_planner_runs p ON p.generation_id=g.generation_id
                 JOIN m3_bootstrap_runtime x ON x.intent_id=p.bootstrap_intent_id
                 JOIN m3_feedback_gates f ON f.intent_id=x.intent_id WHERE r.run_id='backfill'",
                [],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            state,
            (
                "paused".into(),
                "invalidated".into(),
                "snapshot_unusable".into(),
                "invalidated".into(),
                "complete".into(),
                "invalidated".into()
            )
        );
        let repeat = pause_generation(&mut writer, request()).unwrap();
        assert!(!repeat.newly_paused);
    }

    #[test]
    fn stale_action_or_missing_gate_has_no_partial_pause() {
        let mut writer = fixture();
        let mut stale = request();
        stale.capture_epoch = "another";
        assert!(matches!(
            pause_generation(&mut writer, stale),
            Err(PauseError::Stale)
        ));
        stale = request();
        stale.table_set_fingerprint = "changed";
        assert!(matches!(
            pause_generation(&mut writer, stale),
            Err(PauseError::Stale)
        ));
        stale = request();
        stale.owner_run_id = "former-owner";
        assert!(matches!(
            pause_generation(&mut writer, stale),
            Err(PauseError::Stale)
        ));
        stale = request();
        stale.configuration_fingerprint = "changed";
        assert!(matches!(
            pause_generation(&mut writer, stale),
            Err(PauseError::Stale)
        ));
        stale = request();
        stale.generation = 2;
        assert!(matches!(
            pause_generation(&mut writer, stale),
            Err(PauseError::Stale)
        ));
        stale = request();
        stale.run_revision = 1;
        assert!(matches!(
            pause_generation(&mut writer, stale),
            Err(PauseError::Stale)
        ));
        stale = request();
        stale.bootstrap_revision = 1;
        assert!(matches!(
            pause_generation(&mut writer, stale),
            Err(PauseError::Stale)
        ));
        stale = request();
        stale.now_mono_ms = 1001;
        assert!(matches!(
            pause_generation(&mut writer, stale),
            Err(PauseError::Stale)
        ));
        writer
            .connection()
            .execute("DELETE FROM m3_feedback_gates WHERE intent_id='boot'", [])
            .unwrap();
        assert!(matches!(
            pause_generation(&mut writer, request()),
            Err(PauseError::Stale)
        ));
        let state: (String, String) = writer.connection().query_row(
            "SELECT r.state,g.state FROM backfill_runs r JOIN backfill_generations g ON g.run_id=r.run_id WHERE r.run_id='backfill'",
            [], |r| Ok((r.get(0)?,r.get(1)?)),
        ).unwrap();
        assert_eq!(state, ("running".into(), "copying".into()));
    }

    #[test]
    fn failed_pause_write_rolls_back_gate_and_generation() {
        let mut writer = fixture();
        writer.connection().execute_batch(
            "CREATE TRIGGER fail_pause BEFORE UPDATE OF state ON backfill_runs
             WHEN NEW.state='paused' BEGIN SELECT RAISE(ABORT,'injected pause write failure'); END;",
        ).unwrap();
        assert!(matches!(
            pause_generation(&mut writer, request()),
            Err(PauseError::Store(_))
        ));
        let states: (String, String, String, String) = writer
            .connection()
            .query_row(
                "SELECT r.state,g.state,x.state,f.state FROM backfill_runs r
             JOIN backfill_generations g ON g.run_id=r.run_id
             JOIN m3_planner_runs p ON p.generation_id=g.generation_id
             JOIN m3_bootstrap_runtime x ON x.intent_id=p.bootstrap_intent_id
             JOIN m3_feedback_gates f ON f.intent_id=x.intent_id WHERE r.run_id='backfill'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            states,
            (
                "running".into(),
                "copying".into(),
                "exporter_released".into(),
                "open".into()
            )
        );
    }

    #[test]
    fn completed_anchor_blocks_pause_even_if_generation_row_is_stale() {
        let mut writer = fixture();
        // This crafted drift exercises the defensive anchor check; normal schema proof gates
        // cannot complete an anchor while the generation is still copying.
        writer.connection().execute_batch(
            "INSERT INTO bootstrap_anchors(anchor_id,capture_epoch,generation,start_seq,snapshot_boundary_lsn,
             snapshot_complete_seq,post_copy_fence_nonce,post_copy_fence_lsn,post_copy_fence_seq,
             table_set_fingerprint,snapshot_schema_fingerprints,state,expires_at,generation_id,bootstrap_intent_id)
             VALUES('anchor','epoch',1,0,'0000000000000001',1,'nonce','0000000000000002',2,
             'tables','[\"schema\"]','building','never','gen','boot');
             DROP TRIGGER complete_anchor_update_requires_fence;
             DROP TRIGGER anchor_v2_update;
             UPDATE bootstrap_anchors SET state='complete' WHERE anchor_id='anchor';",
        ).unwrap();
        assert!(matches!(
            pause_generation(&mut writer, request()),
            Err(PauseError::Stale)
        ));
    }

    #[test]
    fn store_errors_do_not_log_driver_details() {
        let error = PauseError::Store(rusqlite::Error::InvalidParameterName("sensitive".into()));
        assert_eq!(format!("{error:?}"), "M3_PAUSE_STORE_FAILED");
    }
}
