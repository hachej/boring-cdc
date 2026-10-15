//! Source versions for existing-slot snapshot rows, bound to the durable lower stitch.

use crate::m1_ordering::{MutationVersion, SnapshotIdentityInput};
use crate::m1_transition_kernel::{ReceivedLsn, SourceVersion};
use crate::m2_schema::WriterConnection;
use rusqlite::{OptionalExtension, params};
use std::fmt;

pub enum SnapshotVersionError {
    Stale,
    Store(rusqlite::Error),
}

impl fmt::Display for SnapshotVersionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Stale => "M3_RESTART_SNAPSHOT_VERSION_STALE",
            Self::Store(_) => "M3_RESTART_SNAPSHOT_VERSION_STORE_FAILED",
        })
    }
}
impl fmt::Debug for SnapshotVersionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

/// The importer capability must remain live at the caller. This function checks the durable
/// lower boundary, pin, generation, acknowledged assignment, and current writer ownership.
/// It never asks PostgreSQL for a new LSN.
pub fn version_for_snapshot_row(
    writer: &WriterConnection,
    restart_id: &str,
    importer_id: &str,
    position: SnapshotIdentityInput,
    now_mono_ms: u64,
) -> Result<MutationVersion, SnapshotVersionError> {
    if [restart_id, importer_id]
        .iter()
        .any(|value| value.is_empty() || value.len() > 256 || value.chars().any(char::is_control))
    {
        return Err(SnapshotVersionError::Stale);
    }
    let now = i64::try_from(now_mono_ms).map_err(|_| SnapshotVersionError::Stale)?;
    let connection_generation = i64::try_from(writer.attestation().connection_generation)
        .map_err(|_| SnapshotVersionError::Stale)?;
    let stored: Option<(String, i64, String)> = writer
        .connection()
        .query_row(
            "SELECT l.capture_epoch,l.generation,l.lower_stitch_lsn
             FROM m3_restart_lower_stitches l
             JOIN m3_restart_exports e ON e.restart_id=l.restart_id
               AND e.generation_id=l.generation_id AND e.owner_run_id=l.owner_run_id
               AND e.lower_stitch_lsn=l.lower_stitch_lsn
               AND e.lower_stitch_seq=l.lower_stitch_seq AND e.state='exported'
             JOIN m3_restart_importers i ON i.restart_id=l.restart_id
               AND i.importer_id=?2 AND i.state='acknowledged'
             JOIN logical_range_pins p ON p.pin_id=l.pin_id
               AND p.owner_id=l.generation_id AND p.capture_epoch=l.capture_epoch
               AND p.start_seq=l.lower_stitch_seq AND p.state='active'
             JOIN backfill_generations g ON g.generation_id=l.generation_id
               AND g.run_id=l.backfill_run_id AND g.generation=l.generation
               AND g.state IN ('prepared','copying')
             JOIN backfill_runs b ON b.run_id=l.backfill_run_id
               AND b.destination_id=l.destination_id AND b.capture_epoch=l.capture_epoch
             JOIN source_transactions t ON t.transaction_id=l.source_transaction_id
               AND t.capture_epoch=l.capture_epoch AND t.last_seq=l.lower_stitch_seq
               AND t.end_lsn=l.lower_stitch_lsn AND t.state='committed'
             JOIN source_state s ON s.singleton=1 AND s.capture_epoch=l.capture_epoch
               AND s.slot_name=l.slot_name AND s.source_system_id=t.source_system_id
               AND s.database_id=t.database_id AND s.slot_name=t.slot_name
             JOIN runtime_ownership o ON o.run_id=l.owner_run_id AND o.state='held'
               AND o.connection_generation=?4 AND o.ownership_deadline_mono_ms>=?5
             WHERE l.restart_id=?1 AND l.owner_run_id=?3",
            params![
                restart_id,
                importer_id,
                writer.attestation().run_id,
                connection_generation,
                now
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(SnapshotVersionError::Store)?;
    let Some((epoch, generation, lower_lsn)) = stored else {
        return Err(SnapshotVersionError::Stale);
    };
    if epoch != position.capture_epoch.get().to_string()
        || generation
            != i64::try_from(position.generation.get()).map_err(|_| SnapshotVersionError::Stale)?
    {
        return Err(SnapshotVersionError::Stale);
    }
    let lsn = u64::from_str_radix(&lower_lsn, 16).map_err(|_| SnapshotVersionError::Stale)?;
    let source =
        SourceVersion::from_decoded(position.capture_epoch, ReceivedLsn::from_wire(lsn), 0, 0);
    MutationVersion::from_snapshot(source, position).map_err(|_| SnapshotVersionError::Stale)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m1_ordering::{
        SourceSlotIdentity, VersionComparison, WalIdentityInput, compare_versions,
        source_version_for_row,
    };
    use crate::m1_source_identity::{
        CanonicalKeyComponent, Fingerprint, LogicalTableIdentity, SourceIdentity,
        supported_protocol_fingerprint,
    };
    use crate::m1_transition_kernel::{CaptureEpoch, DestinationGeneration};
    use crate::m3_restart::{LowerStitchRequest, prepare_lower_stitch};
    use crate::m3_restart_export::record_guard_for_test;
    use crate::m3_restart_import::{ImporterAssignment, prepare_assignments};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn prepared() -> WriterConnection {
        let path = std::env::temp_dir().join(format!(
            "m3-restart-version-{}-{}.db",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut writer = crate::m2_schema::open_writer(&path, "owner", 1, 1).unwrap();
        writer.connection().execute_batch(
            "INSERT INTO source_transactions VALUES('tx1','1','sys','db','slot','1','0000000000000010',1,1,1,'sum1','committed');
             INSERT INTO source_transactions VALUES('tx2','1','sys','db','slot','2','0000000000000020',2,2,1,'sum2','committed');
             INSERT INTO journal_events(journal_seq,event_id,transaction_id,transaction_ordinal,capture_epoch,control_kind,payload,payload_hash) VALUES(1,'e1','tx1',0,'1','heartbeat',X'01','hash1');
             INSERT INTO journal_events(journal_seq,event_id,transaction_id,transaction_ordinal,capture_epoch,control_kind,payload,payload_hash) VALUES(2,'e2','tx2',0,'1','heartbeat',X'02','hash2');
             INSERT INTO source_state(singleton,capture_epoch,source_system_id,timeline_id,database_id,slot_name,plugin,publication_fingerprint,protocol_fingerprint,durable_transaction_end_lsn,durable_transaction_id,durable_journal_seq) VALUES(1,'1','sys','1','db','slot','pgoutput','pub','proto','0000000000000020','tx2',2);
             INSERT INTO runtime_ownership(run_id,backend_pid,connection_nonce,ownership_deadline_mono_ms,state,connection_generation) VALUES('owner',1,'nonce',1000,'held',1);
             INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('dest','archive','cfg','1',1);
             INSERT INTO backfill_runs(run_id,destination_id,capture_epoch,state) VALUES('backfill','dest','1','paused');
             INSERT INTO backfill_generations(generation_id,run_id,generation,state) VALUES('old','backfill',1,'invalidated');",
        ).unwrap();
        let lower = prepare_lower_stitch(
            &mut writer,
            LowerStitchRequest {
                restart_id: "restart",
                owner_run_id: "owner",
                backfill_run_id: "backfill",
                generation_id: "new",
                destination_id: "dest",
                capture_epoch: "1",
                generation: 2,
                slot_name: "slot",
                configuration_fingerprint: "cfg",
                table_set_fingerprint: "tables",
                now_mono_ms: 1,
            },
        )
        .unwrap();
        let receipt = record_guard_for_test(&mut writer, &lower, "owner", 41, 1).unwrap();
        prepare_assignments(&mut writer, &receipt, &[ImporterAssignment {
            importer_id: "worker",
            assigned_ranges_digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        }], 1).unwrap();
        writer.connection().execute("UPDATE m3_restart_exports SET exporter_backend_pid=42,snapshot_token=?1,state='exported' WHERE restart_id='restart'",["snapshot-token"]).unwrap();
        writer.connection().execute("UPDATE m3_restart_importers SET backend_pid=43,snapshot_schema_fingerprint='schema',state='acknowledged',revision=revision+1 WHERE restart_id='restart' AND importer_id='worker'",[]).unwrap();
        writer
    }

    fn source_identity() -> SourceIdentity {
        SourceIdentity {
            system_identifier: 42,
            timeline: 1,
            database_identity: 16_384,
            slot_name: "slot".into(),
            plugin: "pgoutput".into(),
            publication_fingerprint: Fingerprint::digest(b"publication"),
            protocol_fingerprint: supported_protocol_fingerprint(),
        }
    }

    fn position() -> SnapshotIdentityInput {
        SnapshotIdentityInput {
            capture_epoch: CaptureEpoch::from_store(1),
            generation: DestinationGeneration::from_store(2),
            logical_table_id: LogicalTableIdentity::derive(&source_identity(), "public", "items"),
            chunk_id: 7,
            key: vec![CanonicalKeyComponent::from_canonical(17, b"key".to_vec()).unwrap()],
        }
    }

    #[test]
    fn persisted_lower_stamps_snapshot_before_later_wal() {
        let writer = prepared();
        let snapshot =
            version_for_snapshot_row(&writer, "restart", "worker", position(), 10).unwrap();
        assert_eq!(snapshot.source().commit_lsn().get(), 32);
        assert_eq!(snapshot.source().transaction_id(), 0);
        assert_eq!(snapshot.source().ordinal(), 0);
        assert_eq!(snapshot.origin_rank(), 0);
        assert_eq!(snapshot.mutation_ordinal(), 0);
        let epoch = CaptureEpoch::from_store(1);
        let source = source_version_for_row(epoch, ReceivedLsn::from_wire(33), 42, 0).unwrap();
        let wal = MutationVersion::from_wal(
            source,
            WalIdentityInput {
                capture_epoch: epoch,
                source_slot_identity: SourceSlotIdentity::derive(&source_identity()),
                transaction_end_lsn: ReceivedLsn::from_wire(34),
                row_ordinal: 0,
                mutation_ordinal: 0,
            },
        )
        .unwrap();
        assert_eq!(compare_versions(&snapshot, &wal), VersionComparison::Less);
        let same_lsn_source =
            source_version_for_row(epoch, ReceivedLsn::from_wire(32), 43, 0).unwrap();
        let same_lsn_wal = MutationVersion::from_wal(
            same_lsn_source,
            WalIdentityInput {
                capture_epoch: epoch,
                source_slot_identity: SourceSlotIdentity::derive(&source_identity()),
                transaction_end_lsn: ReceivedLsn::from_wire(33),
                row_ordinal: 0,
                mutation_ordinal: 0,
            },
        )
        .unwrap();
        assert_eq!(
            compare_versions(&snapshot, &same_lsn_wal),
            VersionComparison::Less
        );
        let mut other_epoch = position();
        other_epoch.capture_epoch = CaptureEpoch::from_store(2);
        assert!(matches!(
            version_for_snapshot_row(&writer, "restart", "worker", other_epoch, 10),
            Err(SnapshotVersionError::Stale)
        ));
        let mut other_generation = position();
        other_generation.generation = DestinationGeneration::from_store(3);
        assert!(matches!(
            version_for_snapshot_row(&writer, "restart", "worker", other_generation, 10),
            Err(SnapshotVersionError::Stale)
        ));
        assert!(matches!(
            version_for_snapshot_row(&writer, "missing", "worker", position(), 10),
            Err(SnapshotVersionError::Stale)
        ));
        assert!(matches!(
            version_for_snapshot_row(&writer, "restart", "worker", position(), 1001),
            Err(SnapshotVersionError::Stale)
        ));
    }

    #[test]
    fn invalidated_generation_or_inactive_pin_has_no_snapshot_version() {
        let writer = prepared();
        writer
            .connection()
            .execute(
                "UPDATE backfill_generations SET state='invalidated' WHERE generation_id='new'",
                [],
            )
            .unwrap();
        assert!(matches!(
            version_for_snapshot_row(&writer, "restart", "worker", position(), 10),
            Err(SnapshotVersionError::Stale)
        ));
        let writer = prepared();
        writer.connection().execute("UPDATE logical_range_pins SET state='release_pending',revision=revision+1 WHERE owner_id='new'",[]).unwrap();
        assert!(matches!(
            version_for_snapshot_row(&writer, "restart", "worker", position(), 10),
            Err(SnapshotVersionError::Stale)
        ));
    }
}
