//! ClickHouse delivery while source capture remains on its own CopyBoth loop.

use percent_encoding::percent_decode_str;
use rusqlite::OptionalExtension;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use url::Url;

use crate::failure_policy::{
    CompletionToken, FailedBoundary, FailureClass, FailureRecord, PolicyEvent,
    PreparedFailureOperation, StableErrorCode, load_failure, transition,
};
use crate::m1_transition_kernel::{Randomness, TransitionContext, VirtualClock};
use crate::m2_journal::{
    JournalError, JournalWriterService, read_complete_range, read_exact_complete_range,
};
use crate::m2_leases::{LeaseIdentity, LeaseToken};
use crate::m2_schema::WriterConnection;
use crate::m4_clickhouse_adapter::ProductionClickHouseAdapter;
use crate::m4_clickhouse_durability::{
    BatchContext, DurabilityError, PreparedBatch, RemoteBatch, dispatch_and_readback,
    finalize_owned_batch, mark_owned_batch_dispatched, prepare_owned_batch, prepare_retry_batch,
};
use crate::m4_clickhouse_wire::PinnedClickHouseWire;

#[derive(Clone)]
pub struct ClickHouseConnection {
    endpoint: String,
    user: String,
    password: String,
}

struct InFlight {
    batch: PreparedBatch,
    checkpoint_revision: Option<u64>,
    receiver: Receiver<Result<RemoteBatch, DurabilityError>>,
    failure: Option<FailureRecord>,
}

struct Retry {
    batch: PreparedBatch,
    checkpoint_revision: Option<u64>,
    failure: FailureRecord,
}

enum Phase {
    Idle,
    InFlight(InFlight),
    Retry(Retry),
    Blocked,
}

/// One anchored ClickHouse candidate. Each tick does at most one bounded SQLite writer action.
/// The remote call runs on another thread and never holds the capture writer.
pub struct ClickHousePump {
    journal_path: PathBuf,
    connection: ClickHouseConnection,
    lease: LeaseToken,
    max_events: usize,
    max_bytes: usize,
    lease_ttl_ms: u64,
    phase: Phase,
}

pub struct PumpStart {
    pub journal_path: PathBuf,
    pub capture_epoch: String,
    pub run_id: String,
    pub destination_fingerprint: String,
    pub public_endpoint: String,
    pub runtime_dsn: String,
    pub max_events: usize,
    pub max_bytes: usize,
    pub lease_ttl_ms: u64,
}

type WriterRequest = Box<dyn FnOnce(&mut JournalWriterService) + Send>;

pub struct DestinationWriterClient {
    sender: SyncSender<WriterRequest>,
}

pub struct DestinationWriterInbox {
    receiver: Receiver<WriterRequest>,
}

pub fn writer_channel() -> (DestinationWriterClient, DestinationWriterInbox) {
    let (sender, receiver) = mpsc::sync_channel(8);
    (
        DestinationWriterClient { sender },
        DestinationWriterInbox { receiver },
    )
}

impl DestinationWriterClient {
    fn service_destination<T: Send + 'static>(
        &mut self,
        max_writer_hold: Duration,
        action: impl FnOnce(&mut WriterConnection) -> Result<T, JournalError> + Send + 'static,
    ) -> Result<T, JournalError> {
        let (response, result) = mpsc::sync_channel(1);
        self.sender
            .send(Box::new(move |writer| {
                let _ = response.send(writer.service_destination(max_writer_hold, action));
            }))
            .map_err(|_| JournalError::Conflict("destination writer unavailable"))?;
        result
            .recv()
            .map_err(|_| JournalError::Conflict("destination writer unavailable"))?
    }
}

impl DestinationWriterInbox {
    /// Service at most one short destination action per capture control tick.
    pub fn service_one(&mut self, writer: &mut JournalWriterService) -> bool {
        if let Ok(request) = self.receiver.try_recv() {
            request(writer);
            true
        } else {
            false
        }
    }
}

fn monotonic_ms() -> Result<u64, &'static str> {
    let mut time = std::mem::MaybeUninit::<libc::timespec>::uninit();
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, time.as_mut_ptr()) } != 0 {
        return Err("M5_CLOCK_UNAVAILABLE");
    }
    let time = unsafe { time.assume_init() };
    u64::try_from(time.tv_sec)
        .ok()
        .and_then(|seconds| seconds.checked_mul(1000))
        .and_then(|milliseconds| milliseconds.checked_add((time.tv_nsec / 1_000_000) as u64))
        .ok_or("M5_CLOCK_INVALID")
}

fn unix_ms() -> Result<u64, &'static str> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .map_err(|_| "M5_CLOCK_INVALID")
}

struct OneRandom(u64);
impl Randomness for OneRandom {
    fn next_u64(&mut self) -> u64 {
        self.0
    }
}

fn random_u64() -> Result<u64, &'static str> {
    use std::io::Read;
    let mut bytes = [0; 8];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut bytes))
        .map_err(|_| "M5_RANDOM_UNAVAILABLE")?;
    Ok(u64::from_be_bytes(bytes))
}

impl ClickHousePump {
    /// Start only after a complete anchor and verified snapshot batch exist for the configured
    /// destination. An absent candidate is not converted into a suffix-only live generation.
    pub fn start(
        writer: &mut DestinationWriterClient,
        start: PumpStart,
    ) -> Result<Option<Self>, &'static str> {
        let PumpStart {
            journal_path,
            capture_epoch,
            run_id,
            destination_fingerprint,
            public_endpoint,
            runtime_dsn,
            max_events,
            max_bytes,
            lease_ttl_ms,
        } = start;
        let epoch = capture_epoch.clone();
        let fingerprint = destination_fingerprint.clone();
        let selected = writer
            .service_destination(Duration::from_secs(5), move |owner| {
                owner
                    .connection()
                    .query_row(
                        "SELECT d.destination_id,d.generation,a.anchor_id FROM destinations d JOIN bootstrap_anchors a ON a.capture_epoch=d.capture_epoch AND a.generation=d.generation AND a.state='complete' AND a.start_seq=0 WHERE d.kind='clickhouse' AND d.capture_epoch=?1 AND d.configuration_fingerprint=?2 LIMIT 1",
                        rusqlite::params![epoch, fingerprint],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?, row.get::<_, String>(2)?)),
                    )
                    .optional()
                    .map_err(JournalError::from)
            })
            .map_err(|_| "M5_DESTINATION_STORE_FAILED")?;
        let Some((destination_id, generation, anchor_id)) = selected else {
            return Ok(None);
        };
        let connection = ClickHouseConnection::parse(&public_endpoint, &runtime_dsn)?;
        let baseline_context = BatchContext {
            destination_id: destination_id.clone(),
            capture_epoch: capture_epoch.clone(),
            generation,
            configuration_fingerprint: destination_fingerprint.clone(),
            lease_id: "preflight".to_owned(),
            run_id: run_id.clone(),
        };
        let baseline_anchor = anchor_id.clone();
        let verified = writer
            .service_destination(Duration::from_secs(5), move |owner| {
                let transaction = owner.connection_mut().transaction()?;
                let result = crate::m4_initial_load::baseline_verified(
                    &transaction,
                    &baseline_context,
                    &baseline_anchor,
                );
                transaction.rollback()?;
                Ok(result)
            })
            .map_err(|_| "M5_BASELINE_STORE_FAILED")?
            .map_err(|_| "M5_BASELINE_INVALID")?;
        if !verified {
            return Ok(None);
        }
        let identity = LeaseIdentity {
            lease_id: format!("{run_id}:clickhouse:{generation}"),
            destination_id,
            capture_epoch,
            anchor_id: Some(anchor_id.clone()),
            generation,
            configuration_fingerprint: destination_fingerprint,
            run_id,
        };
        let acquired = writer
            .service_destination(Duration::from_secs(5), move |owner| {
                Ok(crate::m2_leases::acquire(
                    owner,
                    identity,
                    monotonic_ms().map_err(|_| JournalError::Invalid("clock invalid"))?,
                    lease_ttl_ms,
                ))
            })
            .map_err(|_| "M5_LEASE_STORE_FAILED")?
            .map_err(|_| "M5_LEASE_UNAVAILABLE")?;
        let failure_row = {
            let destination_id = acquired.identity.destination_id.clone();
            writer
                .service_destination(Duration::from_secs(5), move |owner| {
                    owner.connection().query_row(
                        "SELECT f.failure_id,f.failed_boundary_start_seq,f.failed_boundary_end_seq,f.retry_class,f.armed FROM destinations d JOIN processing_failures f ON f.failure_id=d.current_failure_id WHERE d.destination_id=?1",
                        [&destination_id],
                        |row| Ok((row.get::<_,String>(0)?,row.get::<_,Option<u64>>(1)?,row.get::<_,Option<u64>>(2)?,row.get::<_,String>(3)?,row.get::<_,bool>(4)?)),
                    ).optional().map_err(JournalError::from)
                })
                .map_err(|_| "M5_FAILURE_LOAD_FAILED")?
        };
        let phase = match failure_row {
            None => Phase::Idle,
            Some((failure_id, Some(first), Some(last), retry_class, true))
                if retry_class == "transient" =>
            {
                let range = read_exact_complete_range(
                    &journal_path,
                    first,
                    last,
                    max_events,
                    max_bytes,
                    Duration::from_secs(5),
                )
                .map_err(|_| "M5_FAILED_RANGE_UNAVAILABLE")?;
                let context = BatchContext {
                    destination_id: acquired.identity.destination_id.clone(),
                    capture_epoch: acquired.identity.capture_epoch.clone(),
                    generation: acquired.identity.generation,
                    configuration_fingerprint: acquired.identity.configuration_fingerprint.clone(),
                    lease_id: acquired.identity.lease_id.clone(),
                    run_id: acquired.identity.run_id.clone(),
                };
                let epoch = context.capture_epoch.clone();
                let generation = context.generation;
                let id = failure_id.clone();
                let (batch, failure) = writer
                    .service_destination(Duration::from_secs(5), move |owner| {
                        let transaction = owner.connection_mut().transaction()?;
                        let boundary = FailedBoundary::Destination {
                            capture_epoch: epoch,
                            generation,
                            first_seq: first,
                            last_seq: last,
                        };
                        let failure = load_failure(&transaction, &id, boundary)?;
                        let batch = prepare_retry_batch(&transaction, &context, range, &id);
                        transaction.commit()?;
                        Ok((batch, failure))
                    })
                    .map_err(|_| "M5_FAILED_BATCH_LOAD_FAILED")?;
                let batch = batch.map_err(|_| "M5_FAILED_BATCH_INVALID")?;
                let failure = failure.ok_or("M5_FAILED_RECORD_MISSING")?;
                let destination_id = acquired.identity.destination_id.clone();
                let revision = writer
                    .service_destination(Duration::from_secs(5), move |owner| {
                        owner.connection().query_row(
                            "SELECT revision FROM destination_checkpoints WHERE destination_id=?1",
                            [&destination_id],
                            |row| row.get::<_,u64>(0),
                        ).optional().map_err(JournalError::from)
                    })
                    .map_err(|_| "M5_CHECKPOINT_READ_FAILED")?;
                Phase::Retry(Retry {
                    batch,
                    checkpoint_revision: revision,
                    failure,
                })
            }
            Some(_) => Phase::Blocked,
        };
        Ok(Some(Self {
            journal_path,
            connection,
            lease: acquired,
            max_events,
            max_bytes,
            lease_ttl_ms,
            phase,
        }))
    }

    pub fn tick(&mut self, writer: &mut DestinationWriterClient) -> Result<(), &'static str> {
        self.renew_if_due(writer)?;
        let phase = std::mem::replace(&mut self.phase, Phase::Idle);
        match phase {
            Phase::Idle => self.start_next(writer),
            Phase::Blocked => {
                self.phase = Phase::Blocked;
                Ok(())
            }
            Phase::Retry(retry) => {
                if retry
                    .failure
                    .next_retry_at_ms
                    .is_some_and(|due| unix_ms().is_ok_and(|now| now >= due))
                {
                    self.dispatch(retry.batch, retry.checkpoint_revision, Some(retry.failure))
                } else {
                    self.phase = Phase::Retry(retry);
                    Ok(())
                }
            }
            Phase::InFlight(in_flight) => match in_flight.receiver.try_recv() {
                Ok(Ok(remote)) => self.finish_remote(writer, in_flight, remote),
                Ok(Err(error)) => self.record_remote_failure(writer, in_flight, error),
                Err(TryRecvError::Empty) => {
                    self.phase = Phase::InFlight(in_flight);
                    Ok(())
                }
                Err(TryRecvError::Disconnected) => {
                    self.record_remote_failure(writer, in_flight, DurabilityError::Dispatch)
                }
            },
        }
    }

    fn renew_if_due(&mut self, writer: &mut DestinationWriterClient) -> Result<(), &'static str> {
        let now = monotonic_ms()?;
        if now.saturating_add(self.lease_ttl_ms / 2) < self.lease.expires_mono_ms {
            return Ok(());
        }
        let token = self.lease.clone();
        let ttl = self.lease_ttl_ms;
        self.lease = writer
            .service_destination(Duration::from_secs(5), move |owner| {
                Ok(crate::m2_leases::renew(owner, &token, now, ttl))
            })
            .map_err(|_| "M5_LEASE_STORE_FAILED")?
            .map_err(|_| "M5_LEASE_LOST")?;
        Ok(())
    }

    fn start_next(&mut self, writer: &mut DestinationWriterClient) -> Result<(), &'static str> {
        let destination_id = self.lease.identity.destination_id.clone();
        let (after_seq, revision) = writer
            .service_destination(Duration::from_secs(5), move |owner| {
                owner
                    .connection()
                    .query_row(
                        "SELECT journal_seq,revision FROM destination_checkpoints WHERE destination_id=?1",
                        [&destination_id],
                        |row| Ok((row.get::<_, u64>(0)?, Some(row.get::<_, u64>(1)?))),
                    )
                    .optional()
                    .map(|value| value.unwrap_or((0, None)))
                    .map_err(JournalError::from)
            })
            .map_err(|_| "M5_CHECKPOINT_READ_FAILED")?;
        let Some(range) = read_complete_range(
            &self.journal_path,
            after_seq,
            self.max_events,
            self.max_bytes,
            Duration::from_secs(5),
        )
        .map_err(|_| "M5_JOURNAL_READ_FAILED")?
        else {
            return Ok(());
        };
        let token = self.lease.clone();
        let prepared = writer
            .service_destination(Duration::from_secs(5), move |owner| {
                let mut now = || monotonic_ms().unwrap_or(u64::MAX);
                let prepared = prepare_owned_batch(owner, &token, &mut now, range);
                if let Ok(batch) = &prepared {
                    mark_owned_batch_dispatched(owner, &token, &mut now, batch).map_err(|_| {
                        JournalError::Conflict("destination intent dispatch failed")
                    })?;
                }
                Ok(prepared)
            })
            .map_err(|_| "M5_BATCH_PREPARE_FAILED")?
            .map_err(|_| "M5_BATCH_INVALID")?;
        self.dispatch(prepared, revision, None)
    }

    fn dispatch(
        &mut self,
        batch: PreparedBatch,
        checkpoint_revision: Option<u64>,
        failure: Option<FailureRecord>,
    ) -> Result<(), &'static str> {
        let (sender, receiver) = mpsc::sync_channel(1);
        let connection = self.connection.clone();
        let remote_batch = batch.clone();
        std::thread::Builder::new()
            .name("clickhouse-batch".into())
            .spawn(move || {
                let result = PinnedClickHouseWire::new(connection.client())
                    .map(ProductionClickHouseAdapter::new)
                    .map_err(|_| DurabilityError::Dispatch)
                    .and_then(|mut adapter| dispatch_and_readback(&mut adapter, &remote_batch));
                let _ = sender.send(result);
            })
            .map_err(|_| "M5_DESTINATION_THREAD_UNAVAILABLE")?;
        self.phase = Phase::InFlight(InFlight {
            batch,
            checkpoint_revision,
            receiver,
            failure,
        });
        Ok(())
    }

    fn finish_remote(
        &mut self,
        writer: &mut DestinationWriterClient,
        in_flight: InFlight,
        remote: RemoteBatch,
    ) -> Result<(), &'static str> {
        if let Some(failure) = in_flight.failure {
            let token = CompletionToken {
                failure_id: failure.failure_id.clone(),
                fingerprint: failure.fingerprint.clone(),
                capture_epoch: self.lease.identity.capture_epoch.clone(),
                generation: Some(self.lease.identity.generation),
                attempt: failure.attempt,
            };
            let now = VirtualClock::new(unix_ms()?);
            let mut random = OneRandom(0);
            let mut context = TransitionContext {
                clock: &now,
                randomness: &mut random,
            };
            let action = transition(Some(&failure), PolicyEvent::Completed(token), &mut context);
            let operation = PreparedFailureOperation::from_policy_action(
                action,
                Some(failure.failure_id.clone()),
            )
            .ok_or("M5_FAILURE_CLEAR_REJECTED")?;
            writer
                .service_destination(Duration::from_secs(5), move |owner| {
                    let transaction = owner.connection_mut().transaction()?;
                    operation.execute(&transaction)?;
                    transaction.commit()?;
                    Ok(())
                })
                .map_err(|_| "M5_FAILURE_CLEAR_FAILED")?;
        }
        let token = self.lease.clone();
        let batch = in_flight.batch;
        let finalized = writer
            .service_destination(Duration::from_secs(5), move |owner| {
                let mut now = || monotonic_ms().unwrap_or(u64::MAX);
                Ok(finalize_owned_batch(
                    owner,
                    &token,
                    &mut now,
                    &batch,
                    &remote,
                    in_flight.checkpoint_revision,
                ))
            })
            .map_err(|_| "M5_CHECKPOINT_STORE_FAILED")?;
        finalized.map_err(|_| "M5_CHECKPOINT_REJECTED")?;
        self.phase = Phase::Idle;
        Ok(())
    }

    fn record_remote_failure(
        &mut self,
        writer: &mut DestinationWriterClient,
        in_flight: InFlight,
        error: DurabilityError,
    ) -> Result<(), &'static str> {
        let (class, code) = match error {
            DurabilityError::Dispatch | DurabilityError::ReadbackMissing => (
                FailureClass::TransientDestination,
                StableErrorCode::TransportUnavailable,
            ),
            _ => (FailureClass::Integrity, StableErrorCode::ChecksumMismatch),
        };
        let batch = in_flight.batch.clone();
        let context = BatchContext {
            destination_id: self.lease.identity.destination_id.clone(),
            capture_epoch: self.lease.identity.capture_epoch.clone(),
            generation: self.lease.identity.generation,
            configuration_fingerprint: self.lease.identity.configuration_fingerprint.clone(),
            lease_id: self.lease.identity.lease_id.clone(),
            run_id: self.lease.identity.run_id.clone(),
        };
        let now = unix_ms()?;
        let seed = random_u64()?;
        let recorded = writer
            .service_destination(Duration::from_secs(5), move |owner| {
                let transaction = owner.connection_mut().transaction()?;
                let clock = VirtualClock::new(now);
                let mut random = OneRandom(seed);
                let mut policy = TransitionContext {
                    clock: &clock,
                    randomness: &mut random,
                };
                let recorded = crate::m4_clickhouse_durability::persist_failure(
                    &transaction,
                    &context,
                    &batch,
                    class,
                    code,
                    &mut policy,
                );
                transaction.commit()?;
                Ok(recorded)
            })
            .map_err(|_| "M5_FAILURE_STORE_FAILED")?
            .map_err(|_| "M5_FAILURE_REJECTED")?;
        self.phase = if recorded.next_retry_at_ms.is_some() {
            Phase::Retry(Retry {
                batch: in_flight.batch,
                checkpoint_revision: in_flight.checkpoint_revision,
                failure: recorded,
            })
        } else {
            Phase::Blocked
        };
        Ok(())
    }
}

impl ClickHouseConnection {
    /// Keep the secret URL tied to the public, fingerprinted endpoint. No credential or URL is
    /// included in a parse failure returned to the operator.
    pub fn parse(public_endpoint: &str, runtime_dsn: &str) -> Result<Self, &'static str> {
        let public = Url::parse(public_endpoint).map_err(|_| "M5_CLICKHOUSE_ENDPOINT_INVALID")?;
        let runtime = Url::parse(runtime_dsn).map_err(|_| "M5_CLICKHOUSE_DSN_INVALID")?;
        if public.scheme() != "https"
            || public.origin() != runtime.origin()
            || public.path() != runtime.path()
            || public.query().is_some()
            || runtime.query().is_some()
            || public.fragment().is_some()
            || runtime.fragment().is_some()
            || public.username() != ""
            || public.password().is_some()
        {
            return Err("M5_CLICKHOUSE_DSN_MISMATCH");
        }
        let user = percent_decode_str(runtime.username())
            .decode_utf8()
            .map_err(|_| "M5_CLICKHOUSE_DSN_INVALID")?
            .into_owned();
        let password = percent_decode_str(runtime.password().ok_or("M5_CLICKHOUSE_DSN_INVALID")?)
            .decode_utf8()
            .map_err(|_| "M5_CLICKHOUSE_DSN_INVALID")?
            .into_owned();
        if user.is_empty() || password.is_empty() {
            return Err("M5_CLICKHOUSE_DSN_INVALID");
        }
        Ok(Self {
            endpoint: public.to_string(),
            user,
            password,
        })
    }

    pub fn client(&self) -> clickhouse::Client {
        clickhouse::Client::default()
            .with_url(&self.endpoint)
            .with_user(&self.user)
            .with_password(&self.password)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn destination_thread_uses_the_capture_owned_sqlite_writer() {
        let path = std::env::temp_dir().join(format!(
            "m5-writer-channel-{}-{}.sqlite",
            std::process::id(),
            random_u64().unwrap()
        ));
        let owner = crate::m2_schema::open_writer(&path, "run", 1, 0).unwrap();
        let store = crate::m2_journal::JournalStore::new(
            owner,
            crate::m2_journal::SourceIdentity {
                capture_epoch: "1".into(),
                source_system_id: "source".into(),
                timeline_id: "timeline".into(),
                database_id: "database".into(),
                slot_name: "slot".into(),
                publication_fingerprint: "publication".into(),
                protocol_fingerprint: "pgoutput-v1".into(),
            },
            crate::m2_journal::CommitLimits {
                max_events: 8,
                max_copied_bytes: 1024,
                max_writer_hold: Duration::from_secs(2),
            },
        )
        .unwrap();
        let mut writer = JournalWriterService::new(store, [8, 4, 2, 1], 4).unwrap();
        let (mut client, mut inbox) = writer_channel();
        let remote = std::thread::spawn(move || {
            client.service_destination(Duration::from_secs(1), |owner| {
                owner
                    .connection()
                    .execute("CREATE TABLE worker_boundary(value INTEGER)", [])?;
                Ok(())
            })
        });
        let mut serviced = false;
        for _ in 0..100 {
            if inbox.service_one(&mut writer) {
                serviced = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(serviced);
        remote.join().unwrap().unwrap();
        assert_eq!(
            writer
                .service_destination(Duration::from_secs(1), |owner| {
                    owner
                        .connection()
                        .query_row(
                            "SELECT count(*) FROM sqlite_master WHERE name='worker_boundary'",
                            [],
                            |row| row.get::<_, u64>(0),
                        )
                        .map_err(JournalError::from)
                })
                .unwrap(),
            1
        );
    }

    #[test]
    fn runtime_credentials_must_target_fingerprinted_https_endpoint() {
        let connection = ClickHouseConnection::parse(
            "https://clickhouse.example:8443",
            "https://runtime:p%40ss@clickhouse.example:8443",
        )
        .unwrap();
        assert_eq!(connection.endpoint, "https://clickhouse.example:8443/");
        assert_eq!(connection.user, "runtime");
        assert_eq!(connection.password, "p@ss");
        for secret in [
            "http://runtime:secret@clickhouse.example:8443",
            "https://runtime:secret@other.example:8443",
            "https://runtime:secret@clickhouse.example:8443?database=other",
        ] {
            assert!(
                ClickHouseConnection::parse("https://clickhouse.example:8443", secret).is_err()
            );
        }
    }
}
