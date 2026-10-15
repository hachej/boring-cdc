//! Existing-slot snapshot export after a durable lower stitch.

use crate::m2_schema::WriterConnection;
use crate::m3_bootstrap::{BootstrapError, GuardSession, query_pid};
use crate::m3_restart::LowerStitch;
use rusqlite::{OptionalExtension, params};
use std::fmt;

const INSTALL_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS m3_restart_exports(
 restart_id TEXT PRIMARY KEY REFERENCES m3_restart_lower_stitches(restart_id),
 generation_id TEXT NOT NULL REFERENCES backfill_generations(generation_id),
 owner_run_id TEXT NOT NULL,
 lower_stitch_lsn TEXT NOT NULL,
 lower_stitch_seq INTEGER NOT NULL,
 guard_backend_pid INTEGER NOT NULL CHECK(guard_backend_pid>0),
 exporter_backend_pid INTEGER CHECK(exporter_backend_pid>0),
 snapshot_token TEXT,
 state TEXT NOT NULL CHECK(state IN ('guard_held','exported')),
 CHECK((state='guard_held' AND exporter_backend_pid IS NULL AND snapshot_token IS NULL)
    OR (state='exported' AND exporter_backend_pid IS NOT NULL AND snapshot_token IS NOT NULL))
) STRICT;
CREATE TRIGGER IF NOT EXISTS m3_restart_export_guard_immutable
 BEFORE UPDATE OF restart_id,generation_id,owner_run_id,lower_stitch_lsn,lower_stitch_seq,guard_backend_pid
 ON m3_restart_exports BEGIN SELECT RAISE(ABORT,'restart export guard is immutable'); END;
CREATE TRIGGER IF NOT EXISTS m3_restart_export_no_delete
 BEFORE DELETE ON m3_restart_exports BEGIN SELECT RAISE(ABORT,'restart export is immutable'); END;
"#;

pub enum ExportError {
    Stale,
    Source(BootstrapError),
    Store(rusqlite::Error),
}
impl From<rusqlite::Error> for ExportError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Store(value)
    }
}
impl fmt::Display for ExportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Stale => "M3_RESTART_EXPORT_STALE",
            Self::Source(_) => "M3_RESTART_EXPORT_SOURCE_FAILED",
            Self::Store(_) => "M3_RESTART_EXPORT_STORE_FAILED",
        })
    }
}
impl fmt::Debug for ExportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

pub struct GuardReceipt {
    pub(crate) restart_id: String,
    pub(crate) owner_run_id: String,
    pub(crate) generation_id: String,
    pub(crate) lower_stitch_lsn: String,
    pub(crate) lower_stitch_seq: u64,
    pub(crate) guard_backend_pid: i32,
}

/// Records the held DDL guard only while the original lower pin and sole writer remain valid.
/// A failed or interrupted export leaves this one-use generation in `guard_held` for invalidation.
pub fn record_guard_acquired(
    writer: &mut WriterConnection,
    lower: &LowerStitch,
    owner_run_id: &str,
    guard: &mut GuardSession,
    now_mono_ms: u64,
) -> Result<GuardReceipt, ExportError> {
    guard.keepalive().map_err(ExportError::Source)?;
    record_guard_pid(writer, lower, owner_run_id, guard.backend_pid, now_mono_ms)
}

fn record_guard_pid(
    writer: &mut WriterConnection,
    lower: &LowerStitch,
    owner_run_id: &str,
    guard_backend_pid: i32,
    now_mono_ms: u64,
) -> Result<GuardReceipt, ExportError> {
    if guard_backend_pid <= 0 || writer.attestation().run_id != owner_run_id {
        return Err(ExportError::Stale);
    }
    let now = i64::try_from(now_mono_ms).map_err(|_| ExportError::Stale)?;
    let connection_generation = i64::try_from(writer.attestation().connection_generation)
        .map_err(|_| ExportError::Stale)?;
    let seq = i64::try_from(lower.lower_stitch_seq).map_err(|_| ExportError::Stale)?;
    writer.connection().execute_batch(INSTALL_SQL)?;
    let changed = writer.connection().execute(
        "INSERT INTO m3_restart_exports(restart_id,generation_id,owner_run_id,lower_stitch_lsn,
         lower_stitch_seq,guard_backend_pid,state)
         SELECT l.restart_id,l.generation_id,l.owner_run_id,l.lower_stitch_lsn,l.lower_stitch_seq,?7,'guard_held'
         FROM m3_restart_lower_stitches l
         JOIN backfill_generations g ON g.generation_id=l.generation_id AND g.state='prepared'
         JOIN logical_range_pins p ON p.pin_id=l.pin_id AND p.state='active'
         JOIN runtime_ownership o ON o.run_id=l.owner_run_id AND o.state='held'
         WHERE l.restart_id=?1 AND l.generation_id=?2 AND l.owner_run_id=?3
         AND l.lower_stitch_lsn=?4 AND l.lower_stitch_seq=?5 AND p.start_seq=?5
         AND o.connection_generation=?6 AND o.ownership_deadline_mono_ms>=?8",
        params![lower.restart_id,lower.generation_id,owner_run_id,lower.lower_stitch_lsn,seq,connection_generation,guard_backend_pid,now],
    )?;
    if changed != 1 {
        return Err(ExportError::Stale);
    }
    Ok(GuardReceipt {
        restart_id: lower.restart_id.clone(),
        owner_run_id: owner_run_id.into(),
        generation_id: lower.generation_id.clone(),
        lower_stitch_lsn: lower.lower_stitch_lsn.clone(),
        lower_stitch_seq: lower.lower_stitch_seq,
        guard_backend_pid,
    })
}

#[cfg(test)]
pub(crate) fn record_guard_for_test(
    writer: &mut WriterConnection,
    lower: &LowerStitch,
    owner_run_id: &str,
    guard_backend_pid: i32,
    now_mono_ms: u64,
) -> Result<GuardReceipt, ExportError> {
    record_guard_pid(writer, lower, owner_run_id, guard_backend_pid, now_mono_ms)
}

pub struct ExistingSlotExporter {
    connection: pg_walstream::PgReplicationConnection,
    backend_pid: i32,
    snapshot_token: String,
    persisted: bool,
}
impl ExistingSlotExporter {
    /// Opens a SQL transaction against an already existing slot. It never creates a slot or starts
    /// another replication stream. The caller retains the guard and the existing capture session.
    pub fn open(
        dsn: &str,
        writer: &WriterConnection,
        guard: &mut GuardSession,
        receipt: &GuardReceipt,
    ) -> Result<Self, ExportError> {
        if guard.backend_pid != receipt.guard_backend_pid {
            return Err(ExportError::Stale);
        }
        guard.keepalive().map_err(ExportError::Source)?;
        let assigned: bool = writer.connection().query_row(
            "SELECT EXISTS(SELECT 1 FROM m3_restart_importers WHERE restart_id=?1 AND state='assigned')",
            [&receipt.restart_id],
            |r| r.get(0),
        )?;
        if !assigned {
            return Err(ExportError::Stale);
        }
        let mut connection = pg_walstream::PgReplicationConnection::connect(dsn).map_err(|_| {
            ExportError::Source(BootstrapError::Conflict("M3_RESTART_EXPORT_CONNECT_FAILED"))
        })?;
        let backend_pid = query_pid(&mut connection).map_err(ExportError::Source)?;
        if backend_pid == guard.backend_pid {
            return Err(ExportError::Stale);
        }
        connection
            .exec("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .map_err(|_| {
                ExportError::Source(BootstrapError::Conflict("M3_RESTART_EXPORT_BEGIN_FAILED"))
            })?;
        let snapshot_token = connection
            .exec("SELECT pg_export_snapshot()")
            .ok()
            .and_then(|r| r.get_value(0, 0))
            .filter(|v| {
                !v.is_empty()
                    && v.len() <= 1_024
                    && v.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            })
            .ok_or(ExportError::Source(BootstrapError::Conflict(
                "M3_RESTART_EXPORT_TOKEN_MISSING",
            )))?;
        Ok(Self {
            connection,
            backend_pid,
            snapshot_token,
            persisted: false,
        })
    }

    /// Commits the one-use token before any importer can receive it.
    pub fn persist(
        &mut self,
        writer: &mut WriterConnection,
        guard: &mut GuardSession,
        receipt: &GuardReceipt,
        now_mono_ms: u64,
    ) -> Result<(), ExportError> {
        if self.persisted
            || writer.attestation().run_id != receipt.owner_run_id
            || guard.backend_pid != receipt.guard_backend_pid
            || !self.connection.is_alive()
        {
            return Err(ExportError::Stale);
        }
        guard.keepalive().map_err(ExportError::Source)?;
        persist_export_row(
            writer,
            receipt,
            self.backend_pid,
            &self.snapshot_token,
            now_mono_ms,
        )?;
        self.persisted = true;
        Ok(())
    }

    pub fn snapshot_token(&self) -> Result<&str, ExportError> {
        if self.persisted && self.connection.is_alive() {
            Ok(&self.snapshot_token)
        } else {
            Err(ExportError::Stale)
        }
    }
}

fn persist_export_row(
    writer: &mut WriterConnection,
    receipt: &GuardReceipt,
    exporter_backend_pid: i32,
    snapshot_token: &str,
    now_mono_ms: u64,
) -> Result<(), ExportError> {
    if exporter_backend_pid <= 0
        || exporter_backend_pid == receipt.guard_backend_pid
        || snapshot_token.is_empty()
        || snapshot_token.len() > 1_024
        || !snapshot_token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        || writer.attestation().run_id != receipt.owner_run_id
    {
        return Err(ExportError::Stale);
    }
    let now = i64::try_from(now_mono_ms).map_err(|_| ExportError::Stale)?;
    let connection_generation = i64::try_from(writer.attestation().connection_generation)
        .map_err(|_| ExportError::Stale)?;
    let changed = writer.connection().execute(
            "UPDATE m3_restart_exports SET exporter_backend_pid=?2,snapshot_token=?3,state='exported'
             WHERE restart_id=?1 AND state='guard_held' AND owner_run_id=?4 AND generation_id=?5
             AND lower_stitch_lsn=?6 AND lower_stitch_seq=?7 AND guard_backend_pid=?8
             AND EXISTS(SELECT 1 FROM backfill_generations g WHERE g.generation_id=?5 AND g.state='prepared')
             AND EXISTS(SELECT 1 FROM m3_restart_lower_stitches l JOIN logical_range_pins p
               ON p.pin_id=l.pin_id WHERE l.restart_id=?1 AND p.state='active' AND p.start_seq=?7)
             AND EXISTS(SELECT 1 FROM runtime_ownership o WHERE o.run_id=?4 AND o.state='held'
               AND o.connection_generation=?9 AND o.ownership_deadline_mono_ms>=?10)",
            params![receipt.restart_id,exporter_backend_pid,snapshot_token,receipt.owner_run_id,
                receipt.generation_id,receipt.lower_stitch_lsn,receipt.lower_stitch_seq as i64,
                receipt.guard_backend_pid,connection_generation,now],
    )?;
    if changed != 1 {
        return Err(ExportError::Stale);
    }
    Ok(())
}

/// Returns the durable export state without exposing a one-use snapshot token after process loss.
pub fn export_state(
    writer: &WriterConnection,
    restart_id: &str,
) -> Result<Option<String>, ExportError> {
    writer
        .connection()
        .query_row(
            "SELECT state FROM m3_restart_exports WHERE restart_id=?1",
            [restart_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ExportError::Store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m3_restart::{prepare_lower_stitch, tests as lower_fixture};

    #[test]
    fn guard_and_export_bind_the_same_pinned_lower_once() {
        let mut writer = lower_fixture::fixture();
        let lower = prepare_lower_stitch(&mut writer, lower_fixture::request()).unwrap();
        let receipt = record_guard_pid(&mut writer, &lower, "owner", 41, 1).unwrap();
        assert_eq!(
            export_state(&writer, "restart").unwrap().as_deref(),
            Some("guard_held")
        );
        assert!(matches!(
            record_guard_pid(&mut writer, &lower, "owner", 42, 1),
            Err(ExportError::Store(_))
        ));
        assert!(matches!(
            persist_export_row(&mut writer, &receipt, 41, "token", 1),
            Err(ExportError::Stale)
        ));
        persist_export_row(&mut writer, &receipt, 42, "00000001-00000002-1", 1).unwrap();
        assert_eq!(
            export_state(&writer, "restart").unwrap().as_deref(),
            Some("exported")
        );
        assert!(matches!(
            persist_export_row(&mut writer, &receipt, 43, "replacement", 1),
            Err(ExportError::Stale)
        ));
        let token: String = writer
            .connection()
            .query_row(
                "SELECT snapshot_token FROM m3_restart_exports WHERE restart_id='restart'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(token, "00000001-00000002-1");
    }

    #[test]
    fn releasing_pin_and_expired_owner_prevent_export() {
        let mut writer = lower_fixture::fixture();
        let lower = prepare_lower_stitch(&mut writer, lower_fixture::request()).unwrap();
        assert!(matches!(
            record_guard_pid(&mut writer, &lower, "owner", 41, 1001),
            Err(ExportError::Stale)
        ));
        writer
            .connection()
            .execute(
                "UPDATE logical_range_pins SET state='release_pending',revision=revision+1 WHERE pin_id=?1",
                [&lower.pin_id],
            )
            .unwrap();
        assert!(matches!(
            record_guard_pid(&mut writer, &lower, "owner", 41, 1),
            Err(ExportError::Stale)
        ));
        assert_eq!(export_state(&writer, "restart").unwrap(), None);
    }

    #[test]
    fn lost_exporter_before_token_persistence_is_not_reusable() {
        let mut writer = lower_fixture::fixture();
        let lower = prepare_lower_stitch(&mut writer, lower_fixture::request()).unwrap();
        let receipt = record_guard_pid(&mut writer, &lower, "owner", 41, 1).unwrap();
        assert_eq!(
            export_state(&writer, "restart").unwrap().as_deref(),
            Some("guard_held")
        );
        writer
            .connection()
            .execute(
                "UPDATE backfill_generations SET state='invalidated' WHERE generation_id='new'",
                [],
            )
            .unwrap();
        assert!(matches!(
            persist_export_row(&mut writer, &receipt, 42, "lost-token", 1),
            Err(ExportError::Stale)
        ));
        assert_eq!(
            export_state(&writer, "restart").unwrap().as_deref(),
            Some("guard_held")
        );
    }
}

#[cfg(test)]
mod live_tests {
    use super::*;
    use crate::m1_ordering::SnapshotIdentityInput;
    use crate::m1_source_identity::{CanonicalKeyComponent, LogicalTableIdentity};
    use crate::m1_transition_kernel::{CaptureEpoch, DestinationGeneration};
    use crate::m3_bootstrap::{SessionBounds, digest_assignment};
    use crate::m3_restart::{LowerStitchRequest, prepare_lower_stitch};
    use crate::m3_restart_import::{
        ImportRequest, ImporterAssignment, exporter_release_permitted, import_and_acknowledge,
        prepare_assignments,
    };
    use crate::m3_restart_version::tests as version_fixture;
    use crate::m3_restart_version::version_for_snapshot_row;

    #[test]
    #[ignore = "requires isolated PostgreSQL 17.6 with logical replication"]
    fn existing_slot_export_is_importable_without_slot_creation() {
        let dsn = std::env::var("BORING_CDC_M3_DSN").expect("BORING_CDC_M3_DSN");
        let slot = format!("m3_restart_export_{}", std::process::id());
        let mut admin = pg_walstream::PgReplicationConnection::connect(&dsn).unwrap();
        admin.exec("CREATE TABLE IF NOT EXISTS public.m3_restart_export_fixture(id bigint PRIMARY KEY)")
            .unwrap();
        admin
            .exec(&format!(
                "SELECT pg_create_logical_replication_slot('{slot}','pgoutput')"
            ))
            .unwrap();
        let mut writer = version_fixture::numeric_fixture();
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
        let relations = vec!["public.m3_restart_export_fixture".to_string()];
        let bounds = SessionBounds {
            statement_timeout_ms: 10_000,
            idle_timeout_ms: 30_000,
        };
        let mut guard = GuardSession::acquire(
            &dsn,
            &relations,
            bounds.statement_timeout_ms,
            bounds.idle_timeout_ms,
        )
        .unwrap();
        guard.verify_relations(&relations).unwrap();
        let receipt = record_guard_acquired(&mut writer, &lower, "owner", &mut guard, 1).unwrap();
        let digest = digest_assignment(b"all");
        prepare_assignments(
            &mut writer,
            &receipt,
            &[ImporterAssignment {
                importer_id: "worker",
                assigned_ranges_digest: &digest,
            }],
            1,
        )
        .unwrap();
        let mut exporter = ExistingSlotExporter::open(&dsn, &writer, &mut guard, &receipt).unwrap();
        assert!(matches!(exporter.snapshot_token(), Err(ExportError::Stale)));
        exporter
            .persist(&mut writer, &mut guard, &receipt, 1)
            .unwrap();
        assert!(!exporter_release_permitted(&writer, &receipt, &mut guard, &exporter, 1).unwrap());
        let importer = import_and_acknowledge(
            &mut writer,
            &receipt,
            &mut guard,
            &exporter,
            ImportRequest {
                dsn: &dsn,
                importer_id: "worker",
                relations: &relations,
                schema_fingerprint: "schema",
                bounds,
                now_mono_ms: 1,
            },
        )
        .unwrap();
        assert!(exporter_release_permitted(&writer, &receipt, &mut guard, &exporter, 1).unwrap());
        let version = version_for_snapshot_row(
            &writer,
            "restart",
            "worker",
            SnapshotIdentityInput {
                capture_epoch: CaptureEpoch::from_store(1),
                generation: DestinationGeneration::from_store(2),
                logical_table_id: LogicalTableIdentity::derive(
                    &version_fixture::source_identity(),
                    "public",
                    "m3_restart_export_fixture",
                ),
                chunk_id: 1,
                key: vec![
                    CanonicalKeyComponent::from_canonical(20, 1i64.to_be_bytes().to_vec()).unwrap(),
                ],
            },
            1,
        )
        .unwrap();
        assert_eq!(version.source().commit_lsn().get(), 32);
        assert_eq!(version.origin_rank(), 0);
        let slot_count = admin
            .exec(&format!(
                "SELECT count(*)::text FROM pg_replication_slots WHERE slot_name='{slot}'"
            ))
            .unwrap()
            .get_value(0, 0)
            .unwrap();
        assert_eq!(slot_count, "1");
        drop(importer);
        drop(exporter);
        drop(guard);
        admin
            .exec(&format!("SELECT pg_drop_replication_slot('{slot}')"))
            .unwrap();
    }
}
