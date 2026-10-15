//! One-use importer acknowledgements for an existing-slot snapshot.

use crate::m2_schema::WriterConnection;
use crate::m3_bootstrap::{GuardSession, ImportedSnapshotSession, ImporterSession, SessionBounds};
use crate::m3_restart_export::{ExistingSlotExporter, GuardReceipt};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use std::{collections::HashSet, fmt};

const INSTALL_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS m3_restart_importers(
 restart_id TEXT NOT NULL REFERENCES m3_restart_exports(restart_id),
 importer_id TEXT NOT NULL,
 assigned_ranges_digest TEXT NOT NULL,
 backend_pid INTEGER CHECK(backend_pid>0),
 snapshot_schema_fingerprint TEXT,
 state TEXT NOT NULL CHECK(state IN ('assigned','acknowledged')),
 revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0),
 PRIMARY KEY(restart_id,importer_id),
 CHECK((state='assigned' AND backend_pid IS NULL AND snapshot_schema_fingerprint IS NULL)
    OR (state='acknowledged' AND backend_pid IS NOT NULL AND snapshot_schema_fingerprint IS NOT NULL))
) STRICT;
CREATE TRIGGER IF NOT EXISTS m3_restart_importer_revision
 BEFORE UPDATE ON m3_restart_importers WHEN NEW.revision!=OLD.revision+1
 BEGIN SELECT RAISE(ABORT,'stale restart importer revision'); END;
CREATE TRIGGER IF NOT EXISTS m3_restart_importer_identity
 BEFORE UPDATE OF restart_id,importer_id,assigned_ranges_digest ON m3_restart_importers
 BEGIN SELECT RAISE(ABORT,'restart importer assignment is immutable'); END;
CREATE TRIGGER IF NOT EXISTS m3_restart_importer_no_delete
 BEFORE DELETE ON m3_restart_importers BEGIN SELECT RAISE(ABORT,'restart importer is immutable'); END;
CREATE TRIGGER IF NOT EXISTS m3_restart_importer_no_late_insert
 BEFORE INSERT ON m3_restart_importers
 WHEN NOT EXISTS(SELECT 1 FROM m3_restart_exports e WHERE e.restart_id=NEW.restart_id AND e.state='guard_held')
 BEGIN SELECT RAISE(ABORT,'restart importer assignment is late'); END;
"#;

#[derive(Clone, Copy)]
pub struct ImporterAssignment<'a> {
    pub importer_id: &'a str,
    pub assigned_ranges_digest: &'a str,
}

pub struct ImportRequest<'a> {
    pub dsn: &'a str,
    pub importer_id: &'a str,
    pub relations: &'a [String],
    pub schema_fingerprint: &'a str,
    pub bounds: SessionBounds,
    pub now_mono_ms: u64,
}

pub enum ImportError {
    Stale,
    Source,
    Store(rusqlite::Error),
}
impl From<rusqlite::Error> for ImportError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Store(value)
    }
}
impl fmt::Display for ImportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Stale => "M3_RESTART_IMPORT_STALE",
            Self::Source => "M3_RESTART_IMPORT_SOURCE_FAILED",
            Self::Store(_) => "M3_RESTART_IMPORT_STORE_FAILED",
        })
    }
}
impl fmt::Debug for ImportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.is_ascii()
        && !value.chars().any(char::is_control)
}
fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Persists the complete immutable importer roster while the held guard has no exported token.
pub fn prepare_assignments(
    writer: &mut WriterConnection,
    receipt: &GuardReceipt,
    assignments: &[ImporterAssignment<'_>],
    now_mono_ms: u64,
) -> Result<(), ImportError> {
    if assignments.is_empty()
        || assignments.len() > 16
        || writer.attestation().run_id != receipt.owner_run_id
    {
        return Err(ImportError::Stale);
    }
    let mut ids = HashSet::new();
    if assignments.iter().any(|assignment| {
        !valid_id(assignment.importer_id)
            || !valid_digest(assignment.assigned_ranges_digest)
            || !ids.insert(assignment.importer_id)
    }) {
        return Err(ImportError::Stale);
    }
    let now = i64::try_from(now_mono_ms).map_err(|_| ImportError::Stale)?;
    let connection_generation = i64::try_from(writer.attestation().connection_generation)
        .map_err(|_| ImportError::Stale)?;
    writer.connection().execute_batch(INSTALL_SQL)?;
    let tx = writer
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let valid: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM m3_restart_exports e
         JOIN m3_restart_lower_stitches l ON l.restart_id=e.restart_id
         JOIN logical_range_pins p ON p.pin_id=l.pin_id AND p.state='active'
         JOIN backfill_generations g ON g.generation_id=e.generation_id AND g.state='prepared'
         JOIN runtime_ownership o ON o.run_id=e.owner_run_id AND o.state='held'
         WHERE e.restart_id=?1 AND e.owner_run_id=?2 AND e.generation_id=?3
         AND e.guard_backend_pid=?4 AND e.state='guard_held'
         AND e.lower_stitch_lsn=?5 AND e.lower_stitch_seq=?6 AND p.start_seq=?6
         AND o.connection_generation=?7 AND o.ownership_deadline_mono_ms>=?8
         AND NOT EXISTS(SELECT 1 FROM m3_restart_importers i WHERE i.restart_id=e.restart_id))",
        params![
            receipt.restart_id,
            receipt.owner_run_id,
            receipt.generation_id,
            receipt.guard_backend_pid,
            receipt.lower_stitch_lsn,
            receipt.lower_stitch_seq as i64,
            connection_generation,
            now
        ],
        |r| r.get(0),
    )?;
    if !valid {
        return Err(ImportError::Stale);
    }
    for assignment in assignments {
        tx.execute(
            "INSERT INTO m3_restart_importers(restart_id,importer_id,assigned_ranges_digest,state)
             VALUES(?1,?2,?3,'assigned')",
            params![
                receipt.restart_id,
                assignment.importer_id,
                assignment.assigned_ranges_digest
            ],
        )?;
    }
    tx.commit()?;
    Ok(())
}

/// Imports the exact live token as the first transaction statement, verifies guarded relations,
/// then commits the acknowledgement before returning the importer capability to a worker.
pub fn import_and_acknowledge(
    writer: &mut WriterConnection,
    receipt: &GuardReceipt,
    guard: &mut GuardSession,
    exporter: &ExistingSlotExporter,
    request: ImportRequest<'_>,
) -> Result<ImportedSnapshotSession, ImportError> {
    if guard.backend_pid != receipt.guard_backend_pid
        || !valid_id(request.importer_id)
        || !valid_id(request.schema_fingerprint)
        || request.relations.is_empty()
        || writer.attestation().run_id != receipt.owner_run_id
    {
        return Err(ImportError::Stale);
    }
    let assignment_digest: Option<String> = writer
        .connection()
        .query_row(
            "SELECT assigned_ranges_digest FROM m3_restart_importers
         WHERE restart_id=?1 AND importer_id=?2 AND state='assigned'",
            params![receipt.restart_id, request.importer_id],
            |r| r.get(0),
        )
        .optional()?;
    let digest = assignment_digest.ok_or(ImportError::Stale)?;
    guard.keepalive().map_err(|_| ImportError::Source)?;
    let token = exporter.snapshot_token().map_err(|_| ImportError::Stale)?;
    let mut importer = ImporterSession::import(
        request.dsn,
        token,
        request.bounds.statement_timeout_ms,
        request.bounds.idle_timeout_ms,
    )
    .map_err(|_| ImportError::Source)?;
    importer
        .verify_relations(request.relations)
        .map_err(|_| ImportError::Source)?;
    guard.keepalive().map_err(|_| ImportError::Source)?;
    exporter.snapshot_token().map_err(|_| ImportError::Stale)?;
    record_acknowledgement(
        writer,
        receipt,
        request.importer_id,
        &digest,
        importer.backend_pid,
        request.schema_fingerprint,
        request.now_mono_ms,
    )?;
    Ok(importer.into_acknowledged(
        request.importer_id.into(),
        digest,
        request.schema_fingerprint.into(),
        request.relations.to_vec(),
    ))
}

fn record_acknowledgement(
    writer: &mut WriterConnection,
    receipt: &GuardReceipt,
    importer_id: &str,
    assignment_digest: &str,
    backend_pid: i32,
    schema_fingerprint: &str,
    now_mono_ms: u64,
) -> Result<(), ImportError> {
    if backend_pid <= 0
        || !valid_id(schema_fingerprint)
        || writer.attestation().run_id != receipt.owner_run_id
    {
        return Err(ImportError::Stale);
    }
    let now = i64::try_from(now_mono_ms).map_err(|_| ImportError::Stale)?;
    let connection_generation = i64::try_from(writer.attestation().connection_generation)
        .map_err(|_| ImportError::Stale)?;
    let changed = writer.connection().execute(
        "UPDATE m3_restart_importers SET backend_pid=?4,snapshot_schema_fingerprint=?5,
         state='acknowledged',revision=revision+1
         WHERE restart_id=?1 AND importer_id=?2 AND assigned_ranges_digest=?3 AND state='assigned'
         AND EXISTS(SELECT 1 FROM m3_restart_exports e
           JOIN m3_restart_lower_stitches l ON l.restart_id=e.restart_id
           JOIN logical_range_pins p ON p.pin_id=l.pin_id AND p.state='active'
           JOIN backfill_generations g ON g.generation_id=e.generation_id AND g.state='prepared'
           JOIN runtime_ownership o ON o.run_id=e.owner_run_id AND o.state='held'
           WHERE e.restart_id=?1 AND e.owner_run_id=?6 AND e.generation_id=?7
           AND e.guard_backend_pid=?8 AND e.lower_stitch_lsn=?9 AND e.lower_stitch_seq=?10
           AND p.start_seq=?10 AND e.state='exported'
           AND o.connection_generation=?11 AND o.ownership_deadline_mono_ms>=?12)",
        params![
            receipt.restart_id,
            importer_id,
            assignment_digest,
            backend_pid,
            schema_fingerprint,
            receipt.owner_run_id,
            receipt.generation_id,
            receipt.guard_backend_pid,
            receipt.lower_stitch_lsn,
            receipt.lower_stitch_seq as i64,
            connection_generation,
            now
        ],
    )?;
    if changed != 1 {
        return Err(ImportError::Stale);
    }
    Ok(())
}

/// Release is allowed only after every assigned importer acknowledgement is durable.
pub fn exporter_release_permitted(
    writer: &WriterConnection,
    receipt: &GuardReceipt,
    guard: &mut GuardSession,
    exporter: &ExistingSlotExporter,
    now_mono_ms: u64,
) -> Result<bool, ImportError> {
    if guard.backend_pid != receipt.guard_backend_pid {
        return Err(ImportError::Stale);
    }
    guard.keepalive().map_err(|_| ImportError::Source)?;
    exporter.snapshot_token().map_err(|_| ImportError::Stale)?;
    persisted_release_proof(writer, receipt, now_mono_ms)
}

fn persisted_release_proof(
    writer: &WriterConnection,
    receipt: &GuardReceipt,
    now_mono_ms: u64,
) -> Result<bool, ImportError> {
    if writer.attestation().run_id != receipt.owner_run_id {
        return Err(ImportError::Stale);
    }
    let now = i64::try_from(now_mono_ms).map_err(|_| ImportError::Stale)?;
    let connection_generation = i64::try_from(writer.attestation().connection_generation)
        .map_err(|_| ImportError::Stale)?;
    writer
        .connection()
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM m3_restart_exports e
         JOIN m3_restart_lower_stitches l ON l.restart_id=e.restart_id
         JOIN logical_range_pins p ON p.pin_id=l.pin_id AND p.state='active'
         JOIN backfill_generations g ON g.generation_id=e.generation_id AND g.state='prepared'
         JOIN runtime_ownership o ON o.run_id=e.owner_run_id AND o.state='held'
         WHERE e.restart_id=?1 AND e.owner_run_id=?2 AND e.generation_id=?3 AND e.state='exported'
         AND e.lower_stitch_lsn=?4 AND e.lower_stitch_seq=?5 AND p.start_seq=?5
         AND o.connection_generation=?6 AND o.ownership_deadline_mono_ms>=?7
         AND EXISTS(SELECT 1 FROM m3_restart_importers i WHERE i.restart_id=e.restart_id)
         AND NOT EXISTS(SELECT 1 FROM m3_restart_importers i
           WHERE i.restart_id=e.restart_id AND i.state!='acknowledged'))",
            params![
                receipt.restart_id,
                receipt.owner_run_id,
                receipt.generation_id,
                receipt.lower_stitch_lsn,
                receipt.lower_stitch_seq as i64,
                connection_generation,
                now
            ],
            |r| r.get(0),
        )
        .map_err(ImportError::Store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m3_restart::{prepare_lower_stitch, tests as lower_fixture};
    use crate::m3_restart_export::record_guard_for_test;

    const DIGEST_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const DIGEST_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn prepared() -> (WriterConnection, GuardReceipt) {
        let mut writer = lower_fixture::fixture();
        let lower = prepare_lower_stitch(&mut writer, lower_fixture::request()).unwrap();
        let receipt = record_guard_for_test(&mut writer, &lower, "owner", 41, 1).unwrap();
        (writer, receipt)
    }

    #[test]
    fn immutable_roster_requires_exact_acks_before_release() {
        let (mut writer, receipt) = prepared();
        let roster = [
            ImporterAssignment {
                importer_id: "worker-a",
                assigned_ranges_digest: DIGEST_A,
            },
            ImporterAssignment {
                importer_id: "worker-b",
                assigned_ranges_digest: DIGEST_B,
            },
        ];
        prepare_assignments(&mut writer, &receipt, &roster, 1).unwrap();
        assert!(!persisted_release_proof(&writer, &receipt, 1).unwrap());
        assert!(matches!(
            prepare_assignments(&mut writer, &receipt, &roster, 1),
            Err(ImportError::Stale)
        ));
        writer.connection().execute(
            "UPDATE m3_restart_exports SET exporter_backend_pid=42,snapshot_token='snapshot-token',state='exported' WHERE restart_id='restart'",
            [],
        ).unwrap();
        assert!(matches!(
            record_acknowledgement(&mut writer, &receipt, "worker-a", DIGEST_B, 43, "schema", 1),
            Err(ImportError::Stale)
        ));
        record_acknowledgement(&mut writer, &receipt, "worker-a", DIGEST_A, 43, "schema", 1)
            .unwrap();
        assert!(!persisted_release_proof(&writer, &receipt, 1).unwrap());
        assert!(matches!(
            record_acknowledgement(&mut writer, &receipt, "worker-a", DIGEST_A, 43, "schema", 1),
            Err(ImportError::Stale)
        ));
        record_acknowledgement(&mut writer, &receipt, "worker-b", DIGEST_B, 44, "schema", 1)
            .unwrap();
        assert!(persisted_release_proof(&writer, &receipt, 1).unwrap());
    }

    #[test]
    fn duplicate_roster_or_expired_owner_creates_no_assignments() {
        let (mut writer, receipt) = prepared();
        let duplicates = [
            ImporterAssignment {
                importer_id: "worker",
                assigned_ranges_digest: DIGEST_A,
            },
            ImporterAssignment {
                importer_id: "worker",
                assigned_ranges_digest: DIGEST_B,
            },
        ];
        assert!(matches!(
            prepare_assignments(&mut writer, &receipt, &duplicates, 1),
            Err(ImportError::Stale)
        ));
        let one = [ImporterAssignment {
            importer_id: "worker",
            assigned_ranges_digest: DIGEST_A,
        }];
        assert!(matches!(
            prepare_assignments(&mut writer, &receipt, &one, 1001),
            Err(ImportError::Stale)
        ));
        let count: i64 = writer
            .connection()
            .query_row("SELECT count(*) FROM m3_restart_importers", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(count, 0);
    }
}
