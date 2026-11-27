use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};
use std::time::{Duration, Instant};

use pg_walstream::PgReplicationConnection;
use rusqlite::{OptionalExtension, params};

use crate::article1_capture::{CaptureConfig, CaptureFailure, preflight_active_capture};
use crate::m1_config::LoadedConfig;
use crate::m1_decoder::RelationContract;
use crate::m1_source_identity::Fingerprint;
use crate::m2_journal::{JournalError, JournalWriterService};
use crate::m2_schema::WriterConnection;
use crate::m3_bootstrap::{
    BootstrapError, BootstrapRuntime, BootstrapStore, GuardSession, ImporterAssignment,
    ImporterIdentity, PrepareIntent, SessionBounds,
};
use crate::m3_fence::{
    FenceDispatch, FenceError, FenceIntentInput, FenceStore, execute_fence_dispatch,
};
use crate::m3_planner::{
    BoundedRangeSource, CanonicalKey, ChunkBatch, ChunkClaim, KeyPartType, PlanInput, PlannerError,
    PlannerLimits, PlannerStore, PostgresRangeSource, PreparedChunkRead, SourceBinding,
    whole_domain_assignment,
};

const IMPORTER: &str = "initial-copy";
const DESTINATION: &str = "clickhouse";

fn bootstrap_error(error: BootstrapError) -> CaptureFailure {
    let code = match error {
        BootstrapError::Invalid(code) | BootstrapError::Conflict(code) => code,
        BootstrapError::Sqlite(_) => "M3_BOOTSTRAP_STORE_FAILED",
    };
    CaptureFailure::at("bootstrap", code)
}

fn fail(code: &'static str) -> CaptureFailure {
    CaptureFailure::at("bootstrap", code)
}

fn planner_error(error: PlannerError) -> JournalError {
    match error {
        PlannerError::Invalid(code) => JournalError::Invalid(code),
        PlannerError::Conflict(code) => JournalError::Conflict(code),
        PlannerError::Limit(code) => JournalError::Limit(code),
        PlannerError::StaleGeneration => JournalError::Conflict("M3_STALE_GENERATION"),
        PlannerError::Sqlite(error) => error.into(),
    }
}

fn fence_error(error: FenceError) -> JournalError {
    match error {
        FenceError::Invalid(code) => JournalError::Invalid(code),
        FenceError::Conflict(code) => JournalError::Conflict(code),
        FenceError::Sqlite(error) => error.into(),
        FenceError::Source(_) => JournalError::Unavailable("M3_FENCE_SOURCE_FAILED"),
    }
}

fn journal_error(error: JournalError) -> CaptureFailure {
    let code = match error {
        JournalError::Invalid(code)
        | JournalError::Conflict(code)
        | JournalError::Limit(code)
        | JournalError::Unavailable(code) => code,
        _ => "M3_BOOTSTRAP_WRITER_FAILED",
    };
    fail(code)
}

fn mono_ms() -> Result<u64, JournalError> {
    let mut value = std::mem::MaybeUninit::<libc::timespec>::uninit();
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, value.as_mut_ptr()) } != 0 {
        return Err(JournalError::Unavailable("M3_MONOTONIC_CLOCK_FAILED"));
    }
    let value = unsafe { value.assume_init() };
    Ok(value.tv_sec as u64 * 1000 + value.tv_nsec as u64 / 1_000_000)
}

enum WorkerCommand {
    Read(PreparedChunkRead),
    Fence(FenceDispatch),
}

enum WorkerResult {
    Plan(Vec<CanonicalKey>, u64),
    Chunk(ChunkBatch),
    Dispatched,
}

enum CopyPhase {
    Planning,
    Reading(ChunkClaim),
    Dispatching,
    Observing,
    Complete,
}

pub(crate) struct InitialCopy {
    plan: PlanInput,
    binding: SourceBinding,
    fence: FenceIntentInput,
    guard: GuardSession,
    importer: ImporterIdentity,
    limits: PlannerLimits,
    command: SyncSender<WorkerCommand>,
    result: Receiver<Result<WorkerResult, JournalError>>,
    cancelled: Arc<AtomicBool>,
    phase: CopyPhase,
    started: Instant,
    next_guard_poll: Instant,
    next_impact_sample: Instant,
    next_fence_poll: Instant,
    guard_interval: Duration,
    lifetime: Duration,
    exporter_age_at_release_ms: u64,
    ddl_bound_ms: u64,
}

impl Drop for InitialCopy {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }
}

pub(crate) struct StartedBootstrap {
    pub writer: WriterConnection,
    pub copy: InitialCopy,
    pub connection: PgReplicationConnection,
    pub contracts: std::collections::BTreeMap<u32, RelationContract>,
}

pub(crate) fn start(
    writer: WriterConnection,
    config: &LoadedConfig,
    capture: &CaptureConfig,
    capture_epoch: &str,
    run_id: &str,
    now_ms: i64,
) -> Result<StartedBootstrap, CaptureFailure> {
    let public = config.public();
    let [table] = public.tables.as_slice() else {
        return Err(fail("M3_SINGLE_RELATION_PROFILE_REQUIRED"));
    };
    let generation = i64::try_from(public.clickhouse.destination_generation)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| fail("M3_GENERATION_INVALID"))?;
    let (system_id, db_id): (String, String) = writer.connection().query_row(
        "SELECT source_system_id,database_id FROM source_state WHERE singleton=1 AND durable_journal_seq IS NULL",
        [], |row| Ok((row.get(0)?, row.get(1)?)),
    ).map_err(|_| fail("M3_SOURCE_STATE_UNAVAILABLE"))?;
    let already_prepared: bool = writer
        .connection()
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM bootstrap_intents)",
            [],
            |row| row.get(0),
        )
        .map_err(|_| fail("M3_BOOTSTRAP_STORE_FAILED"))?;
    if already_prepared {
        return Err(fail("M3_BOOTSTRAP_RECOVERY_REQUIRED"));
    }
    let table_set_fingerprint = Fingerprint::from_lower_hex(&config.fingerprints().table_set)
        .ok_or_else(|| fail("M3_TABLE_SET_FINGERPRINT_INVALID"))?
        .bytes();
    let epoch = capture_epoch
        .parse()
        .map_err(|_| fail("M3_CAPTURE_EPOCH_NOT_CANONICAL"))?;
    let mut nonce = [0u8; 16];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut rng| rng.read_exact(&mut nonce))
        .map_err(|_| fail("M3_FENCE_NONCE_UNAVAILABLE"))?;
    let dsn = config
        .runtime_dsn()
        .ok_or_else(|| fail("M3_RUNTIME_DSN_UNAVAILABLE"))?;
    let control_dsn = config
        .control_writer_dsn()
        .ok_or_else(|| fail("M3_CONTROL_DSN_UNAVAILABLE"))?
        .to_owned();
    let importer_assignment = whole_domain_assignment();
    let intent_id = format!("bootstrap:{run_id}");
    let generation_id = format!("backfill:{run_id}");
    let fence = FenceIntentInput {
        intent_id: format!("fence:{run_id}"),
        generation_id: generation_id.clone(),
        bootstrap_intent_id: intent_id.clone(),
        capture_epoch: epoch,
        generation,
        nonce,
        table_set_fingerprint,
        anchor_id: format!("anchor:{run_id}"),
        expires_at: format!(
            "unix-ms:{}",
            (now_ms as u64).saturating_add(public.retention.replay_window_ms.0)
        ),
    };
    writer.connection().execute(
        "INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES(?1,'clickhouse',?2,?3,?4) ON CONFLICT(destination_id) DO NOTHING",
        params![DESTINATION, config.fingerprints().destination, capture_epoch, generation],
    ).map_err(|_| fail("M3_DESTINATION_STATE_FAILED"))?;
    let started = Instant::now();
    let (mut bootstrap, exporter_age_at_release_ms) = BootstrapRuntime::start_with_exporter_bound(
        writer,
        PrepareIntent {
            intent_id: intent_id.clone(),
            capture_epoch: capture_epoch.to_owned(),
            source_system_id: system_id,
            database_id: db_id,
            slot_name: public.source.slot.clone(),
            generation: generation as u64,
            table_set_fingerprint: config.fingerprints().table_set.clone(),
            configuration_fingerprint: config.fingerprints().destination.clone(),
            importers: vec![ImporterAssignment {
                importer_id: IMPORTER.to_owned(),
                assigned_ranges_digest: importer_assignment.clone(),
            }],
            created_at: format!("unix-ms:{now_ms}"),
        },
        dsn,
        std::slice::from_ref(&table.source_relation),
        &public.source.publication,
        SessionBounds {
            statement_timeout_ms: public.backfill.session_timeout_ms.0,
            idle_timeout_ms: public.backfill.session_timeout_ms.0,
        },
        0,
        Some(Duration::from_millis(
            public.backfill.exporter_lifetime_ms.0,
        )),
    )
    .map_err(bootstrap_error)?;
    let importer = bootstrap.take_importer(IMPORTER).map_err(bootstrap_error)?;
    let (writer, guard, connection, pid) = bootstrap.into_live_parts();
    crate::m3_planner::install(writer.connection()).map_err(|e| journal_error(planner_error(e)))?;
    writer
        .connection()
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS m3_live_source_impact(
            intent_id TEXT PRIMARY KEY REFERENCES bootstrap_intents(intent_id),
            details TEXT NOT NULL CHECK(json_valid(details)),
            failure_code TEXT
        ) STRICT;",
        )
        .map_err(|_| fail("M3_STATUS_STORE_FAILED"))?;

    let contracts = preflight_active_capture(capture, pid)?;
    let relation = contracts
        .values()
        .find(|contract| {
            format!("{}.{}", contract.relation.namespace, contract.relation.name)
                == table.source_relation
        })
        .ok_or_else(|| fail("M3_RELATION_CONTRACT_MISSING"))?;
    let copy = ReadySnapshot {
        fence,
        backfill_run_id: run_id.to_owned(),
        start_seq: 0,
        started,
        exporter_age_at_release_ms,
        guard,
        importer,
    }
    .start_copy(config.public(), relation, control_dsn)?;
    Ok(StartedBootstrap {
        writer,
        copy,
        connection,
        contracts,
    })
}

/// Capture drains all WAL through this source position before pinning a real journal commit.
/// The sampled position is only a wait target and never becomes the snapshot version.
pub(crate) fn restart_drain_target(dsn: &str) -> Result<u64, CaptureFailure> {
    let mut sql =
        PgReplicationConnection::connect(dsn).map_err(|_| fail("M3_RESTART_SOURCE_UNAVAILABLE"))?;
    let value = sql
        .exec("SELECT pg_current_wal_lsn()::text")
        .ok()
        .and_then(|r| r.get_value(0, 0))
        .ok_or_else(|| fail("M3_RESTART_DRAIN_POSITION_UNAVAILABLE"))?;
    let (hi, lo) = value
        .split_once('/')
        .ok_or_else(|| fail("M3_RESTART_DRAIN_POSITION_INVALID"))?;
    let parse = |part| {
        u32::from_str_radix(part, 16)
            .map(u64::from)
            .map_err(|_| fail("M3_RESTART_DRAIN_POSITION_INVALID"))
    };
    Ok((parse(hi)? << 32) | parse(lo)?)
}

struct InterruptedCopy {
    intent: String,
    run: String,
    generation_id: String,
    generation: u64,
    backends: [Option<i32>; 4],
}

pub(crate) struct RestartSnapshot {
    pub public: crate::m1_config::PublicConfig,
    pub fingerprints: crate::m1_config::Fingerprints,
    pub dsn: String,
    pub control_dsn: String,
    pub source: crate::m1_source_identity::SourceIdentity,
    pub relation: RelationContract,
    pub capture_epoch: String,
    pub run_id: String,
    pub drained_through: u64,
    pub now_ms: i64,
}

impl RestartSnapshot {
    pub(crate) fn start(
        self,
        writer: &crate::m5_clickhouse_loop::DestinationWriterClient,
    ) -> Result<InitialCopy, JournalError> {
        use crate::{m3_restart, m3_restart_export, m3_restart_import};
        let public = &self.public;
        let [table] = public.tables.as_slice() else {
            return Err(JournalError::Invalid("M3_SINGLE_RELATION_PROFILE_REQUIRED"));
        };
        let dsn = self.dsn.as_str();
        let hold = Duration::from_millis(public.source.maximum_operation_ms.0);
        let epoch = self.capture_epoch.clone();
        let configuration = self.fingerprints.destination.clone();
        let table_set = self.fingerprints.table_set.clone();
        let prior = writer.service_backfill(hold, move |writer| {
            writer.connection().query_row(
                "SELECT x.intent_id,p.run_id,p.generation_id,g.generation,x.guard_backend_pid,
                        x.exporter_backend_pid,x.capture_backend_pid,i.backend_pid
                   FROM m3_bootstrap_runtime x JOIN bootstrap_intents b USING(intent_id)
                   JOIN m3_planner_runs p ON p.bootstrap_intent_id=x.intent_id
                   JOIN backfill_generations g USING(generation_id)
                   JOIN m3_bootstrap_importers i ON i.intent_id=x.intent_id AND i.importer_id=p.importer_id
                   JOIN backfill_runs r ON r.run_id=p.run_id
                   JOIN destinations d ON d.destination_id=r.destination_id
                  WHERE b.capture_epoch=?1 AND d.destination_id=?2
                    AND x.configuration_fingerprint=?3 AND x.table_set_fingerprint=?4
                    AND g.state IN ('copying','fencing','invalidated')
                    AND g.generation=d.generation
                    AND NOT EXISTS(SELECT 1 FROM bootstrap_anchors a WHERE a.capture_epoch=b.capture_epoch AND a.generation=g.generation AND a.state='complete')
               ORDER BY g.generation DESC LIMIT 1",
                params![epoch,DESTINATION,configuration,table_set],
                |r| Ok(InterruptedCopy {
                    intent: r.get(0)?, run: r.get(1)?, generation_id: r.get(2)?,
                    generation: r.get(3)?, backends: [r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?],
                }),
            ).optional().map_err(JournalError::from)
        })?;
        let Some(InterruptedCopy {
            intent: old_intent,
            run: backfill_run_id,
            generation_id: old_generation_id,
            generation: previous,
            backends,
        }) = prior
        else {
            return Err(JournalError::Conflict("M3_BOOTSTRAP_RECOVERY_REQUIRED"));
        };
        let mut source_connection = PgReplicationConnection::connect(dsn)
            .map_err(|_| JournalError::Unavailable("M3_RESTART_SOURCE_UNAVAILABLE"))?;
        for pid in backends.into_iter().flatten() {
            let count = source_connection
                .exec(&format!(
                    "SELECT count(*)::text FROM pg_stat_activity WHERE pid={pid}"
                ))
                .ok()
                .and_then(|rows| rows.get_value(0, 0));
            if count.as_deref() != Some("0") {
                return Err(JournalError::Conflict("M3_RESTART_OLD_SESSION_STILL_LIVE"));
            }
        }
        drop(source_connection);
        let generation = previous
            .checked_add(1)
            .ok_or(JournalError::Invalid("M3_GENERATION_INVALID"))?;
        let restart_id = format!("restart:{}", self.run_id);
        let generation_id = format!("backfill:{}", self.run_id);
        let pin_id = restart_id.clone();
        let candidate_id = generation_id.clone();
        let run_id = backfill_run_id.clone();
        let owner = self.run_id.clone();
        let epoch = self.capture_epoch.clone();
        let slot = self.source.slot_name.clone();
        let configuration = self.fingerprints.destination.clone();
        let table_set = self.fingerprints.table_set.clone();
        let lower = writer.service_backfill(hold, move |writer| {
            let state: String = writer.connection().query_row(
                "SELECT state FROM backfill_generations WHERE generation_id=?1",
                [&old_generation_id],
                |r| r.get(0),
            )?;
            if state != "invalidated" {
                BootstrapStore::borrow_writer(&mut *writer)
                    .invalidate_generation(&old_intent, "guard")
                    .map_err(|e| JournalError::Conflict(bootstrap_error(e).code))?;
            }
            m3_restart::prepare_lower_stitch(
                writer,
                m3_restart::LowerStitchRequest {
                    restart_id: &pin_id,
                    owner_run_id: &owner,
                    backfill_run_id: &run_id,
                    generation_id: &candidate_id,
                    destination_id: DESTINATION,
                    capture_epoch: &epoch,
                    generation,
                    slot_name: &slot,
                    configuration_fingerprint: &configuration,
                    table_set_fingerprint: &table_set,
                    now_mono_ms: mono_ms()?,
                },
            )
            .map_err(|_| JournalError::Conflict("M3_RESTART_LOWER_STALE"))
        })?;
        let started = Instant::now();
        let bounds = SessionBounds {
            statement_timeout_ms: public.backfill.session_timeout_ms.0,
            idle_timeout_ms: public.backfill.session_timeout_ms.0,
        };
        let relations = std::slice::from_ref(&table.source_relation);
        let mut guard = GuardSession::acquire(
            dsn,
            relations,
            bounds.statement_timeout_ms,
            bounds.idle_timeout_ms,
        )
        .map_err(|e| JournalError::Conflict(bootstrap_error(e).code))?;
        guard
            .verify_relations(relations)
            .map_err(|e| JournalError::Conflict(bootstrap_error(e).code))?;
        let mut catalog = PgReplicationConnection::connect(dsn)
            .map_err(|_| JournalError::Unavailable("M3_RESTART_SOURCE_UNAVAILABLE"))?;
        let guarded_schema = crate::m2_catalog_reader::read_simple_relation(
            &mut catalog,
            &public.source.publication,
            &self.source,
            &self.relation,
        )
        .and_then(|c| {
            c.encode()
                .map_err(|_| crate::m2_catalog_reader::CatalogError::Invalid)
        })
        .map_err(|_| JournalError::Conflict("M3_RESTART_GUARD_SCHEMA_MISMATCH"))?;
        drop(catalog);
        let guard_pid = guard.backend_pid;
        let pin = lower.clone();
        let owner = self.run_id.clone();
        let assignment = whole_domain_assignment();
        let assigned = assignment.clone();
        let receipt = writer.service_backfill(hold, move |writer| {
            let receipt =
                m3_restart_export::record_guard_pid(writer, &pin, &owner, guard_pid, mono_ms()?)
                    .map_err(|_| JournalError::Conflict("M3_RESTART_GUARD_FAILED"))?;
            m3_restart_import::prepare_assignments(
                writer,
                &receipt,
                &[m3_restart_import::ImporterAssignment {
                    importer_id: IMPORTER,
                    assigned_ranges_digest: &assigned,
                }],
                mono_ms()?,
            )
            .map_err(|_| JournalError::Conflict("M3_RESTART_ASSIGNMENT_FAILED"))?;
            Ok(receipt)
        })?;
        let exported = Instant::now();
        let mut exporter =
            m3_restart_export::ExistingSlotExporter::open_sql(dsn, &receipt, Some(&bounds))
                .map_err(|_| JournalError::Conflict("M3_RESTART_EXPORT_FAILED"))?;
        let (pid, snapshot) = exporter.export_response();
        let export_binding = receipt.clone();
        writer.service_backfill(hold, move |writer| {
            m3_restart_export::persist_export_row(
                writer,
                &export_binding,
                pid,
                &snapshot,
                mono_ms()?,
            )
            .map_err(|_| JournalError::Conflict("M3_RESTART_EXPORT_PERSIST_FAILED"))
        })?;
        exporter.note_persisted();
        let mut importer = crate::m3_bootstrap::ImporterSession::import(
            dsn,
            exporter
                .snapshot_token()
                .map_err(|_| JournalError::Conflict("M3_RESTART_EXPORT_FAILED"))?,
            bounds.statement_timeout_ms,
            bounds.idle_timeout_ms,
        )
        .map_err(|_| JournalError::Conflict("M3_RESTART_IMPORT_FAILED"))?;
        importer
            .verify_relations(relations)
            .map_err(|_| JournalError::Conflict("M3_RESTART_IMPORT_FAILED"))?;
        let proof = importer
            .canonical_schema(
                &public.source.publication,
                &self.source,
                &self.relation,
                &self.capture_epoch,
            )
            .map_err(|_| JournalError::Conflict("M3_RESTART_IMPORT_FAILED"))?;
        if proof.canonical_schema != guarded_schema {
            return Err(JournalError::Conflict("M3_RESTART_GUARD_SCHEMA_MISMATCH"));
        }
        guard
            .keepalive()
            .map_err(|_| JournalError::Conflict("M3_RESTART_GUARD_FAILED"))?;
        if exported.elapsed().as_millis() >= u128::from(public.backfill.exporter_lifetime_ms.0)
            || exporter.snapshot_token().is_err()
        {
            return Err(JournalError::Conflict("M3_RESTART_EXPORT_RELEASE_FAILED"));
        }
        let pid = importer.backend_pid;
        let schema = proof.schema_fingerprint.clone();
        let export_proof = receipt.clone();
        let assigned = assignment.clone();
        writer.service_backfill(hold, move |writer| {
            m3_restart_import::record_acknowledgement(
                writer,
                &export_proof,
                IMPORTER,
                &assigned,
                pid,
                &schema,
                mono_ms()?,
            )
            .map_err(|_| JournalError::Conflict("M3_RESTART_IMPORT_FAILED"))?;
            if !m3_restart_import::persisted_release_proof(writer, &export_proof, mono_ms()?)
                .map_err(|_| JournalError::Conflict("M3_RESTART_EXPORT_RELEASE_FAILED"))?
            {
                return Err(JournalError::Conflict("M3_RESTART_EXPORT_RELEASE_FAILED"));
            }
            Ok(())
        })?;
        drop(exporter);
        let exporter_age_at_release_ms = exported.elapsed().as_millis() as u64;
        let importer = importer.into_acknowledged(
            IMPORTER.to_owned(),
            assignment,
            proof.schema_fingerprint.clone(),
            relations.to_vec(),
        );
        let pin = lower.clone();
        let now_ms = self.now_ms;
        writer.service_backfill(hold, move |writer| {
            bind_restart_to_planner(writer, &pin, &proof, previous, generation, now_ms)
        })?;
        let mut nonce = [0u8; 16];
        std::fs::File::open("/dev/urandom")
            .and_then(|mut rng| rng.read_exact(&mut nonce))
            .map_err(|_| JournalError::Unavailable("M3_FENCE_NONCE_UNAVAILABLE"))?;
        let table_set_fingerprint = Fingerprint::from_lower_hex(&self.fingerprints.table_set)
            .ok_or(JournalError::Invalid("M3_TABLE_SET_FINGERPRINT_INVALID"))?
            .bytes();
        let fence = FenceIntentInput {
            intent_id: format!("fence:{}", self.run_id),
            generation_id,
            bootstrap_intent_id: restart_id,
            capture_epoch: self
                .capture_epoch
                .parse()
                .map_err(|_| JournalError::Invalid("M3_CAPTURE_EPOCH_NOT_CANONICAL"))?,
            generation: generation
                .try_into()
                .map_err(|_| JournalError::Invalid("M3_GENERATION_INVALID"))?,
            nonce,
            table_set_fingerprint,
            anchor_id: format!("anchor:{}", self.run_id),
            expires_at: format!(
                "unix-ms:{}",
                (self.now_ms as u64).saturating_add(public.retention.replay_window_ms.0)
            ),
        };
        ReadySnapshot {
            fence,
            backfill_run_id,
            start_seq: lower.lower_stitch_seq,
            started,
            exporter_age_at_release_ms,
            guard,
            importer,
        }
        .start_copy(public, &self.relation, self.control_dsn)
        .map_err(|e| JournalError::Conflict(e.code))
    }
}

fn bind_restart_to_planner(
    writer: &mut WriterConnection,
    lower: &crate::m3_restart::LowerStitch,
    schema: &crate::m3_bootstrap::SnapshotSchemaProof,
    previous: u64,
    generation: u64,
    now_ms: i64,
) -> Result<(), JournalError> {
    crate::m3_planner::install(writer.connection()).map_err(planner_error)?;
    let tx = writer.connection_mut().transaction()?;
    let intent = &lower.restart_id;
    let choose = tx.execute(
        "INSERT INTO bootstrap_intents(intent_id,capture_epoch,source_system_id,database_id,slot_name,state,created_at)
         SELECT ?1,s.capture_epoch,s.source_system_id,s.database_id,s.slot_name,'copying',?2
         FROM source_state s JOIN m3_restart_lower_stitches l ON l.capture_epoch=s.capture_epoch AND l.slot_name=s.slot_name
         WHERE l.restart_id=?1 AND s.singleton=1",
        params![intent, format!("unix-ms:{now_ms}")],
    )?;
    let ready = tx.execute(
        "INSERT INTO m3_bootstrap_runtime(intent_id,generation,table_set_fingerprint,configuration_fingerprint,
           snapshot_token,consistent_lsn,start_seq,exporter_liveness,guard_liveness,snapshot_promotable,
           feedback_gate_open,state, exporter_backend_pid,guard_backend_pid)
         SELECT l.restart_id,l.generation,l.table_set_fingerprint,l.configuration_fingerprint,
           e.snapshot_token,l.lower_stitch_lsn,l.lower_stitch_seq,'released','held',1,1,'exporter_released',
           e.exporter_backend_pid,e.guard_backend_pid
         FROM m3_restart_lower_stitches l JOIN m3_restart_exports e ON e.restart_id=l.restart_id
         JOIN m3_restart_importers i ON i.restart_id=l.restart_id AND i.importer_id=?2
         WHERE l.restart_id=?1 AND e.state='exported' AND i.state='acknowledged'
           AND i.snapshot_schema_fingerprint=?3",
        params![intent,IMPORTER,schema.schema_fingerprint],
    )?;
    let importer = tx.execute(
        "INSERT INTO m3_bootstrap_importers(intent_id,importer_id,assigned_ranges_digest,snapshot_schema_fingerprint,backend_pid,state)
         SELECT restart_id,importer_id,assigned_ranges_digest,snapshot_schema_fingerprint,backend_pid,'acknowledged'
         FROM m3_restart_importers WHERE restart_id=?1 AND importer_id=?2 AND state='acknowledged'",
        params![intent,IMPORTER],
    )?;
    tx.execute(
        "INSERT INTO m3_feedback_gates(intent_id,generation,state) VALUES(?1,?2,'open')",
        params![intent, generation],
    )?;
    tx.execute("INSERT INTO m3_snapshot_schema_proofs(intent_id,importer_id,capture_epoch,relation_id,namespace,relation_name,schema_fingerprint,canonical_schema,schema_checksum) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
        params![intent,IMPORTER,schema.capture_epoch,schema.relation_id,schema.namespace,schema.relation_name,schema.schema_fingerprint,schema.canonical_schema,schema.schema_checksum])?;
    let selected = tx.execute(
        "UPDATE destinations SET generation=?2,revision=revision+1 WHERE destination_id=?1 AND generation=?3 AND capture_epoch=?4
         AND NOT EXISTS(SELECT 1 FROM destination_checkpoints WHERE destination_id=?1)
         AND current_failure_id IS NULL",
        params![DESTINATION,generation,previous,schema.capture_epoch],
    )?;
    if [choose, ready, importer, selected] != [1; 4] {
        return Err(JournalError::Conflict("M3_RESTART_BINDING_STALE"));
    }
    tx.commit()?;
    Ok(())
}

struct ReadySnapshot {
    fence: FenceIntentInput,
    backfill_run_id: String,
    start_seq: u64,
    started: Instant,
    exporter_age_at_release_ms: u64,
    guard: GuardSession,
    importer: crate::m3_bootstrap::ImportedSnapshotSession,
}

fn planner_limits(
    public: &crate::m1_config::PublicConfig,
) -> Result<PlannerLimits, CaptureFailure> {
    let chunk_duration = Duration::from_millis(public.backfill.chunk_duration_ms.0);
    let writer_hold = Duration::from_millis(public.source.maximum_operation_ms.0);
    let limit_size = |value| usize::try_from(value).map_err(|_| fail("M3_BACKFILL_BOUNDS_INVALID"));
    PlannerLimits {
        chunk_rows: limit_size(public.backfill.chunk_rows)?,
        chunk_bytes: limit_size(public.backfill.chunk_bytes.0)?,
        chunk_duration,
        writer_hold,
        concurrency: 1,
        source_impact_bytes: limit_size(public.backfill.source_impact_bytes.0)?,
        max_rows_per_second: public
            .backfill
            .chunk_rows
            .saturating_mul(1_000)
            .div_ceil(public.backfill.chunk_duration_ms.0)
            .max(1_000),
    }
    .validate()
    .map_err(|e| journal_error(planner_error(e)))
}

impl ReadySnapshot {
    fn start_copy(
        self,
        public: &crate::m1_config::PublicConfig,
        relation: &RelationContract,
        control_dsn: String,
    ) -> Result<InitialCopy, CaptureFailure> {
        let table = &public.tables[0];
        let mut importer = self.importer;
        let guard = self.guard;
        let importer_identity = importer.identity().map_err(bootstrap_error)?;
        let snapshot_schema_fingerprint = importer.schema_fingerprint().to_owned();
        let importer_assignment = whole_domain_assignment();
        let limits = planner_limits(public)?;
        let writer_hold = limits.writer_hold;
        let key_names = relation
            .key_columns
            .iter()
            .map(|index| relation.relation.columns[*index].name.as_str())
            .collect::<Vec<_>>();
        if !key_names
            .iter()
            .copied()
            .eq(table.replica_key.iter().map(String::as_str))
        {
            return Err(fail("M3_REPLICA_KEY_MISMATCH"));
        }
        let key_schema = relation
            .key_columns
            .iter()
            .map(|index| match relation.relation.columns[*index].type_oid {
                20 | 21 | 23 => Ok(KeyPartType::I64),
                2950 => Ok(KeyPartType::Uuid),
                _ => Err(fail("M3_KEY_TYPE_UNSUPPORTED")),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut source = PostgresRangeSource::from_imported(
            importer,
            IMPORTER,
            &importer_assignment,
            &snapshot_schema_fingerprint,
            &table.source_relation,
            &key_names,
        )
        .map_err(|e| journal_error(planner_error(e)))?;
        let binding = source
            .binding()
            .cloned()
            .ok_or_else(|| fail("M3_SOURCE_BINDING_MISMATCH"))?;
        let (command, commands) = sync_channel(1);
        let (results, result) = sync_channel(1);
        let cancelled = Arc::new(AtomicBool::new(false));
        let worker_cancel = Arc::clone(&cancelled);
        let worker_schema = key_schema.clone();
        std::thread::Builder::new()
            .name("initial-snapshot-reader".into())
            .spawn(move || {
                let planned = source
                    .discover_boundaries(&worker_schema, limits, &worker_cancel)
                    .map(|(boundaries, rows)| WorkerResult::Plan(boundaries, rows))
                    .map_err(planner_error);
                if results.send(planned).is_err() {
                    return;
                }
                while let Ok(command) = commands.recv() {
                    if worker_cancel.load(Ordering::Relaxed) {
                        break;
                    }
                    match command {
                        WorkerCommand::Read(read) => {
                            if results
                                .send(
                                    read.read(&mut source)
                                        .map(WorkerResult::Chunk)
                                        .map_err(planner_error),
                                )
                                .is_err()
                            {
                                break;
                            }
                        }
                        WorkerCommand::Fence(dispatch) => {
                            drop(source);
                            let dispatch = PgReplicationConnection::connect(&control_dsn)
                                .map_err(|_| JournalError::Unavailable("M3_FENCE_SOURCE_FAILED"))
                                .and_then(|mut control| {
                                    control
                                        .exec(&format!(
                                            "SET statement_timeout={}",
                                            writer_hold.as_millis()
                                        ))
                                        .map_err(|_| {
                                            JournalError::Unavailable("M3_FENCE_SOURCE_FAILED")
                                        })?;
                                    execute_fence_dispatch(dispatch, &mut control)
                                        .map_err(fence_error)
                                });
                            let _ = results.send(dispatch.map(|()| WorkerResult::Dispatched));
                            return;
                        }
                    }
                }
            })
            .map_err(|_| fail("M3_IMPORTER_THREAD_UNAVAILABLE"))?;
        let plan = PlanInput {
            run_id: self.backfill_run_id,
            generation_id: self.fence.generation_id.clone(),
            destination_id: DESTINATION.to_owned(),
            capture_epoch: self.fence.capture_epoch.to_string(),
            generation: self.fence.generation as u64,
            bootstrap_intent_id: self.fence.bootstrap_intent_id.clone(),
            importer_id: IMPORTER.to_owned(),
            snapshot_schema_fingerprint,
            key_schema,
            boundaries: vec![],
            estimated_rows: 0,
            start_seq: self.start_seq,
            started_mono_ms: mono_ms().map_err(journal_error)?,
        };
        let guard_interval = Duration::from_millis(public.backfill.guard_keepalive_ms.0.min(250));
        let lifetime = Duration::from_millis(
            public
                .backfill
                .guard_lifetime_ms
                .0
                .min(public.backfill.importer_lifetime_ms.0),
        );
        Ok(InitialCopy {
            plan,
            binding,
            fence: self.fence,
            guard,
            importer: importer_identity,
            limits,
            command,
            result,
            cancelled,
            phase: CopyPhase::Planning,
            started: self.started,
            next_guard_poll: Instant::now(),
            next_impact_sample: Instant::now(),
            next_fence_poll: Instant::now(),
            guard_interval,
            lifetime,
            exporter_age_at_release_ms: self.exporter_age_at_release_ms,
            ddl_bound_ms: public.backfill.ddl_waiter_bound_ms.0,
        })
    }
}

impl InitialCopy {
    pub(crate) fn intent_id(&self) -> String {
        self.plan.bootstrap_intent_id.clone()
    }

    pub(crate) fn generation(&self) -> u64 {
        self.plan.generation
    }

    pub(crate) fn complete(&self) -> bool {
        matches!(self.phase, CopyPhase::Complete)
    }

    pub(crate) fn tick(&mut self, writer: &mut JournalWriterService) -> Result<(), CaptureFailure> {
        if self.complete() {
            return Ok(());
        }
        if self.started.elapsed() >= self.lifetime {
            return Err(fail("M3_SNAPSHOT_LIFETIME_EXCEEDED"));
        }
        if Instant::now() >= self.next_guard_poll {
            self.guard.keepalive().map_err(bootstrap_error)?;
            let waiter_exceeded = self
                .guard
                .conflicting_waiter_exceeded(self.ddl_bound_ms)
                .map_err(bootstrap_error)?;
            if waiter_exceeded || Instant::now() >= self.next_impact_sample {
                let mut details = self
                    .guard
                    .source_impact(&self.importer)
                    .map_err(bootstrap_error)?;
                details["exporter_age_at_release_ms"] =
                    serde_json::json!(self.exporter_age_at_release_ms);
                let intent = self.intent_id();
                let code = waiter_exceeded.then_some("M3_DDL_WAITER");
                writer.service_backfill(self.limits.writer_hold, move |writer| {
                    writer.connection().execute(
                        "INSERT INTO m3_live_source_impact VALUES(?1,?2,?3) ON CONFLICT(intent_id) DO UPDATE SET details=excluded.details,failure_code=excluded.failure_code",
                        params![intent, details.to_string(), code],
                    )?;
                    Ok(())
                }).map_err(journal_error)?;
                self.next_impact_sample = Instant::now() + Duration::from_secs(1);
            }
            if waiter_exceeded {
                return Err(fail("M3_DDL_WAITER"));
            }
            self.next_guard_poll = Instant::now() + self.guard_interval;
        }
        if matches!(self.phase, CopyPhase::Observing) {
            if Instant::now() < self.next_fence_poll {
                return Ok(());
            }
            self.next_fence_poll = Instant::now() + Duration::from_millis(25);
            let fence = format!("fence:{}", self.fence.intent_id);
            let observed = writer.service_backfill(self.limits.writer_hold, move |writer| {
                writer.connection().query_row(
                    "SELECT EXISTS(SELECT 1 FROM durable_capture_fences WHERE fence_id=?1 AND first_proof=1)",
                    [&fence], |r| r.get::<_, bool>(0),
                ).map_err(JournalError::from)
            }).map_err(journal_error)?;
            if observed {
                self.release_guard(writer)?;
                self.phase = CopyPhase::Complete;
            }
            return Ok(());
        }
        let result = match self.result.try_recv() {
            Ok(result) => result.map_err(journal_error)?,
            Err(TryRecvError::Empty) => return Ok(()),
            Err(TryRecvError::Disconnected) => return Err(fail("M3_IMPORTER_WORKER_LOST")),
        };
        let limits = self.limits;
        match (&self.phase, result) {
            (CopyPhase::Planning, WorkerResult::Plan(boundaries, rows)) => {
                let mut plan = self.plan.clone();
                plan.boundaries = boundaries;
                plan.estimated_rows = rows;
                writer
                    .service_backfill(limits.writer_hold, move |writer| {
                        PlannerStore::from_writer(writer, limits)
                            .and_then(|mut store| store.persist_whole_domain_plan(&plan))
                            .map_err(planner_error)
                    })
                    .map_err(journal_error)?;
                self.next_chunk(writer)
            }
            (CopyPhase::Reading(claim), WorkerResult::Chunk(batch)) => {
                let claim = claim.clone();
                writer
                    .service_backfill(limits.writer_hold, move |writer| {
                        let now = mono_ms()?;
                        PlannerStore::from_writer(writer, limits)
                            .and_then(|mut store| store.commit_chunk_measured(&claim, &batch, now))
                            .map_err(planner_error)
                    })
                    .map_err(journal_error)?;
                self.next_chunk(writer)
            }
            (CopyPhase::Dispatching, WorkerResult::Dispatched) => {
                let intent = self.fence.intent_id.clone();
                writer
                    .service_backfill(limits.writer_hold, move |writer| {
                        FenceStore::borrow_writer(writer)
                            .mark_dispatched(&intent, 1)
                            .map_err(fence_error)
                    })
                    .map_err(journal_error)?;
                self.phase = CopyPhase::Observing;
                Ok(())
            }
            _ => Err(fail("M3_BOOTSTRAP_WORKER_STATE_INVALID")),
        }
    }

    fn next_chunk(&mut self, writer: &mut JournalWriterService) -> Result<(), CaptureFailure> {
        let limits = self.limits;
        let generation = self.plan.generation_id.clone();
        let binding = self.binding.clone();
        let next = writer
            .service_backfill(limits.writer_hold, move |writer| {
                let mut store = PlannerStore::from_writer(writer, limits).map_err(planner_error)?;
                let Some(claim) = store
                    .claim_next(
                        &generation,
                        IMPORTER,
                        mono_ms()?,
                        limits.chunk_duration.saturating_add(Duration::from_secs(2)),
                    )
                    .map_err(planner_error)?
                else {
                    store.finish_copy(&generation).map_err(planner_error)?;
                    return Ok(None);
                };
                let read = store
                    .prepare_read(&claim, Some(&binding))
                    .map_err(planner_error)?;
                Ok(Some((claim, read)))
            })
            .map_err(journal_error)?;
        let command = if let Some((claim, read)) = next {
            self.phase = CopyPhase::Reading(claim);
            WorkerCommand::Read(read)
        } else {
            let fence = self.fence.clone();
            let dispatch = writer
                .service_backfill(limits.writer_hold, move |writer| {
                    FenceStore::borrow_writer(writer)
                        .prepare_after_copy(&fence)
                        .map_err(fence_error)
                })
                .map_err(journal_error)?;
            self.phase = CopyPhase::Dispatching;
            WorkerCommand::Fence(dispatch)
        };
        self.command
            .try_send(command)
            .map_err(|_| fail("M3_IMPORTER_WORKER_UNAVAILABLE"))
    }

    fn release_guard(&mut self, writer: &mut JournalWriterService) -> Result<(), CaptureFailure> {
        self.guard.release_and_verify().map_err(bootstrap_error)?;
        let intent = self.intent_id();
        writer.service_backfill(self.limits.writer_hold, move |writer| {
            let released = writer.connection().execute(
                "UPDATE m3_bootstrap_runtime SET guard_liveness='released',revision=revision+1 WHERE intent_id=?1 AND guard_liveness IN ('held','lost')",
                [&intent],
            )?;
            if released != 1 {
                return Err(JournalError::Conflict("M3_GUARD_RELEASE_STALE"));
            }
            Ok(())
        }).map_err(journal_error)
    }

    pub(crate) fn invalidate(
        &mut self,
        writer: &mut JournalWriterService,
        lost: &'static str,
    ) -> Result<(), CaptureFailure> {
        if self.complete() {
            return Ok(());
        }
        self.cancelled.store(true, Ordering::Relaxed);
        let intent = self.intent_id();
        writer
            .service_backfill(self.limits.writer_hold, move |writer| {
                BootstrapStore::borrow_writer(writer)
                    .invalidate_generation(&intent, lost)
                    .map_err(|_| JournalError::Conflict("M3_INVALIDATION_STALE"))
            })
            .map_err(journal_error)?;
        let importer = self
            .guard
            .end_importer(&self.importer)
            .map_err(bootstrap_error);
        let guard = self.release_guard(writer);
        importer.and(guard)
    }
}

pub(crate) enum BootstrapFeedbackGate<'a> {
    None,
    Initial(crate::m3_fence::PersistedFeedbackGate<'a>),
}

impl crate::m2_capture_runtime::FeedbackGate for BootstrapFeedbackGate<'_> {
    fn permit(
        &mut self,
        durable_end_lsn: Option<u64>,
    ) -> crate::m2_capture_runtime::FeedbackPermit {
        match self {
            Self::None => crate::m2_capture_runtime::NoSnapshotGate.permit(durable_end_lsn),
            Self::Initial(gate) => gate.permit(durable_end_lsn),
        }
    }
}

/// Read the latest bootstrap progress without opening a source connection or a SQLite writer.
pub fn status_extension(path: &std::path::Path) -> Result<Option<serde_json::Value>, JournalError> {
    let db =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    db.busy_timeout(Duration::from_millis(250))?;
    let tx = db.unchecked_transaction()?;
    let installed: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='m3_live_source_impact')",
        [], |row| row.get(0),
    )?;
    if !installed {
        return Ok(None);
    }
    type StatusRow = (
        u64,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let latest: Option<StatusRow> = tx.query_row(
        "SELECT r.generation,r.state,r.exporter_liveness,r.guard_liveness,p.run_id,o.details,o.failure_code,g.state
         FROM m3_bootstrap_runtime r
         JOIN bootstrap_intents b USING(intent_id)
         LEFT JOIN m3_planner_runs p ON p.bootstrap_intent_id=r.intent_id
         LEFT JOIN m3_live_source_impact o ON o.intent_id=r.intent_id
         LEFT JOIN backfill_generations g ON g.generation_id=p.generation_id
         ORDER BY b.rowid DESC LIMIT 1",
        [], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?)),
    ).optional()?;
    let Some((generation, state, exporter, guard, run, details, failure, generation_state)) =
        latest
    else {
        return Ok(None);
    };
    let mut progress = run
        .as_deref()
        .map(|run| crate::m3_planner::read_progress(&tx, run, mono_ms()?).map_err(planner_error))
        .transpose()?;
    if let (Some(progress), Some(state)) = (&mut progress, &generation_state) {
        progress.state.clone_from(state);
        if state != "copying" {
            progress.eta_ms = None;
        }
    }
    let source: Option<serde_json::Value> = details
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|_| JournalError::Conflict("M3_STATUS_IMPACT_INVALID"))?;
    Ok(Some(serde_json::json!({
        "generation": generation,
        "bootstrap_state": state,
        "generation_state": generation_state,
        "exporter_liveness": exporter,
        "guard_liveness": guard,
        "progress": progress,
        "source_impact_last_sample": source,
        "failure_code": failure
    })))
}
