//! Durable ClickHouse batch acceptance over copied M2 journal ranges.
//!
//! This module owns destination state only. It consumes an already-copied complete journal range,
//! drops no M2 reader handles across remote work, records an immutable intent before dispatch, and
//! advances the destination checkpoint only in the same SQLite transaction that records verified
//! marker readback. ClickHouse transport implementations remain outside this pure coordination
//! boundary so driver errors and credentials cannot leak into durable state.

use crate::failure_policy::{
    Component, FailedBoundary, FailureClass, FailureObservation, FailureRecord, FingerprintInput,
    PolicyAction, PolicyEvent, PreparedFailureOperation, SafeContextKey, SafeContextValue,
    StableErrorCode, load_failure, transition,
};
use crate::m1_transition_kernel::TransitionContext;
use crate::m2_journal::{CopiedEvent, CopiedRange};
use crate::m2_leases::{
    LeaseError, LeaseToken, SideEffectAdapter, SideEffectOutcome, complete_local,
    dispatch as dispatch_with_lease,
};
use crate::m2_schema::WriterConnection;
use crate::m4_clickhouse_schema::object_fingerprint;
use rusqlite::{OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;

pub const FAILURE_CONDITION: &str = "clickhouse_durability_unverified";
pub const FAILURE_BOUNDARY: &str = "clickhouse_batch_acceptance";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchContext {
    pub destination_id: String,
    pub capture_epoch: String,
    pub generation: u64,
    pub configuration_fingerprint: String,
    pub lease_id: String,
    pub run_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DestinationEvent {
    pub journal_seq: u64,
    pub transaction_ordinal: u64,
    pub mutation_ordinal: u32,
    pub connector_event_id: String,
    pub relation_schema_fingerprint: String,
    pub payload: Vec<u8>,
    pub payload_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchMarker {
    pub capture_epoch: u64,
    pub generation: u64,
    pub batch_id: String,
    pub first_journal_seq: u64,
    pub last_journal_seq: u64,
    pub event_count: u64,
    pub ordered_event_digest: String,
    pub object_fingerprint: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedBatch {
    pub intent_id: String,
    pub destination_id: String,
    pub capture_epoch_text: String,
    pub configuration_fingerprint: String,
    pub lease_id: String,
    pub run_id: String,
    pub complete_transaction_id: String,
    pub events: Vec<DestinationEvent>,
    pub marker: BatchMarker,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteEventIdentity {
    pub connector_event_id: String,
    pub payload_hash: String,
    /// Canonical bytes reconstructed from the actual stored key, operation, source-version,
    /// schema identity, and tagged column values. Returning only the stored hash is forbidden.
    pub canonical_payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteBatch {
    pub marker: BatchMarker,
    /// All physical rows for this batch. Exact duplicate identities are accepted; one identity
    /// associated with two payload hashes is an integrity conflict.
    pub events: Vec<RemoteEventIdentity>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DispatchError;

/// The adapter must use the fixed synchronous settings from `m4_clickhouse_schema` and return only
/// after each insert is acknowledged. Readback must be a fresh server query, never a client cache.
pub trait ClickHouseBatchExecutor {
    fn insert_events_synchronously(&mut self, batch: &PreparedBatch) -> Result<(), DispatchError>;
    /// Reconstruct actual stored payloads before any marker is inserted.
    fn read_events(
        &mut self,
        batch: &PreparedBatch,
    ) -> Result<Vec<RemoteEventIdentity>, DispatchError>;
    fn insert_marker_synchronously(&mut self, marker: &BatchMarker) -> Result<(), DispatchError>;
    fn read_batch(&mut self, marker: &BatchMarker) -> Result<Option<RemoteBatch>, DispatchError>;
    /// Recompute the live correctness-object/settings fingerprint from server metadata.
    fn inspect_object_fingerprint(&mut self) -> Result<String, DispatchError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Reconciliation {
    ReplayRequired,
    Verified,
}

fn lease_error(error: LeaseError) -> DurabilityError {
    match error {
        LeaseError::Stale => DurabilityError::OwnershipLost,
        LeaseError::Invalid(_) | LeaseError::Conflict => {
            DurabilityError::Conflict("lease validation failed")
        }
        LeaseError::Sqlite(_) => DurabilityError::Sqlite("lease persistence failed".into()),
        LeaseError::External(_) => DurabilityError::Dispatch,
    }
}

fn context_from_lease(token: &LeaseToken) -> BatchContext {
    BatchContext {
        destination_id: token.identity.destination_id.clone(),
        capture_epoch: token.identity.capture_epoch.clone(),
        generation: token.identity.generation,
        configuration_fingerprint: token.identity.configuration_fingerprint.clone(),
        lease_id: token.identity.lease_id.clone(),
        run_id: token.identity.run_id.clone(),
    }
}

/// Production entry point for intent creation. M2 validates the complete lease incarnation,
/// runtime ownership deadline, and monotonic expiry in the same local transaction.
pub fn prepare_owned_batch<C>(
    writer: &mut WriterConnection,
    token: &LeaseToken,
    clock_mono_ms: &mut C,
    range: CopiedRange,
) -> Result<PreparedBatch, DurabilityError>
where
    C: FnMut() -> u64,
{
    let context = context_from_lease(token);
    complete_local(writer, token, clock_mono_ms, |transaction| {
        prepare_batch(transaction, &context, range).map_err(|error| match error {
            DurabilityError::OwnershipLost => LeaseError::Stale,
            DurabilityError::Sqlite(message) => LeaseError::Sqlite(message),
            _ => LeaseError::Conflict,
        })
    })
    .map_err(lease_error)
}

struct BatchSideEffect<'a, E> {
    executor: &'a mut E,
    batch: &'a PreparedBatch,
}
impl<E: ClickHouseBatchExecutor> SideEffectAdapter for BatchSideEffect<'_, E> {
    type Artifact = RemoteBatch;

    fn apply_candidate(&mut self, _external_namespace: &str) -> Result<Self::Artifact, String> {
        dispatch_and_readback(self.executor, self.batch)
            .map_err(|_| "clickhouse_batch_dispatch_failed".to_owned())
    }
}

/// Production dispatch path. The ambiguous-effect phase is committed first, then M2 checks the
/// exact lease token immediately before and after the synchronous ClickHouse operation.
pub fn dispatch_owned_batch<C>(
    writer: &mut WriterConnection,
    token: &LeaseToken,
    clock_mono_ms: &mut C,
    executor: &mut impl ClickHouseBatchExecutor,
    batch: &PreparedBatch,
) -> Result<RemoteBatch, DurabilityError>
where
    C: FnMut() -> u64,
{
    let context = context_from_lease(token);
    if batch.destination_id != context.destination_id
        || batch.capture_epoch_text != context.capture_epoch
        || batch.marker.generation != context.generation
        || batch.configuration_fingerprint != context.configuration_fingerprint
        || batch.lease_id != context.lease_id
        || batch.run_id != context.run_id
    {
        return Err(DurabilityError::OwnershipLost);
    }
    complete_local(writer, token, clock_mono_ms, |transaction| {
        mark_insert_dispatched(transaction, batch).map_err(|error| match error {
            DurabilityError::Sqlite(message) => LeaseError::Sqlite(message),
            _ => LeaseError::Conflict,
        })
    })
    .map_err(lease_error)?;
    let mut adapter = BatchSideEffect { executor, batch };
    match dispatch_with_lease(writer, token, clock_mono_ms, &mut adapter).map_err(lease_error)? {
        SideEffectOutcome::Live(remote) => Ok(remote),
        SideEffectOutcome::StaleArtifact(_) => Err(DurabilityError::OwnershipLost),
    }
}

/// Production finalization path. M2 holds the sole-writer transaction and validates the exact
/// lease token on both sides of the checkpoint/intent CAS.
pub fn finalize_owned_batch<C>(
    writer: &mut WriterConnection,
    token: &LeaseToken,
    clock_mono_ms: &mut C,
    batch: &PreparedBatch,
    remote: &RemoteBatch,
    expected_checkpoint_revision: Option<u64>,
) -> Result<(), DurabilityError>
where
    C: FnMut() -> u64,
{
    let context = context_from_lease(token);
    complete_local(writer, token, clock_mono_ms, |transaction| {
        finalize_checkpoint(
            transaction,
            &context,
            batch,
            remote,
            expected_checkpoint_revision,
        )
        .map_err(|error| match error {
            DurabilityError::OwnershipLost => LeaseError::Stale,
            DurabilityError::Sqlite(message) => LeaseError::Sqlite(message),
            _ => LeaseError::Conflict,
        })
    })
    .map_err(lease_error)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurabilityError {
    Invalid(&'static str),
    Conflict(&'static str),
    OwnershipLost,
    Dispatch,
    ReadbackMissing,
    Sqlite(String),
}

impl fmt::Display for DurabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}
impl std::error::Error for DurabilityError {}
impl From<rusqlite::Error> for DurabilityError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error.to_string())
    }
}

/// Persist (or adopt) an immutable intent. The caller obtained `range` through M2's bounded copied
/// reader; no SQLite read snapshot is retained by this value or by subsequent remote operations.
pub fn prepare_batch(
    transaction: &Transaction<'_>,
    context: &BatchContext,
    range: CopiedRange,
) -> Result<PreparedBatch, DurabilityError> {
    if range.events.is_empty()
        || range.first_seq == 0
        || range.last_seq < range.first_seq
        || range.events.first().map(|event| event.journal_seq) != Some(range.first_seq)
        || range.events.last().map(|event| event.journal_seq) != Some(range.last_seq)
    {
        return Err(DurabilityError::Invalid("invalid copied range"));
    }
    validate_context(context)?;
    validate_destination_and_lease(transaction, context)?;
    validate_checkpoint_cursor(transaction, context, range.first_seq)?;

    let complete_transaction_id = range
        .events
        .last()
        .map(|event| event.transaction_id.clone())
        .ok_or(DurabilityError::Invalid("empty copied range"))?;
    let boundary: Option<String> = transaction
        .query_row(
            "SELECT transaction_id FROM source_transactions WHERE capture_epoch=?1 AND last_seq=?2 AND state='committed'",
            params![context.capture_epoch, range.last_seq],
            |row| row.get(0),
        )
        .optional()?;
    if boundary.as_deref() != Some(complete_transaction_id.as_str()) {
        return Err(DurabilityError::Conflict(
            "range does not end at its persisted complete transaction",
        ));
    }

    let events = destination_events(&range.events)?;
    let ordered_event_digest = ordered_event_digest(&events);
    let object_fingerprint = object_fingerprint();
    let intent_id = batch_id(
        context,
        range.first_seq,
        range.last_seq,
        &ordered_event_digest,
        &object_fingerprint,
    );
    let marker = BatchMarker {
        capture_epoch: context
            .capture_epoch
            .parse()
            .map_err(|_| DurabilityError::Invalid("capture epoch is not ClickHouse UInt64"))?,
        generation: context.generation,
        batch_id: intent_id.clone(),
        first_journal_seq: range.first_seq,
        last_journal_seq: range.last_seq,
        event_count: events.len() as u64,
        ordered_event_digest: ordered_event_digest.clone(),
        object_fingerprint,
    };

    transaction.execute(
        "INSERT INTO clickhouse_batch_intents(intent_id,destination_id,capture_epoch,generation,first_seq,last_seq,payload_checksum,state) VALUES(?1,?2,?3,?4,?5,?6,?7,'prepared') ON CONFLICT(intent_id) DO NOTHING",
        params![intent_id, context.destination_id, context.capture_epoch, context.generation, range.first_seq, range.last_seq, ordered_event_digest],
    )?;
    let persisted: Option<(String, String, u64, u64, u64, String, String)> = transaction
        .query_row(
            "SELECT destination_id,capture_epoch,generation,first_seq,last_seq,payload_checksum,state FROM clickhouse_batch_intents WHERE intent_id=?1",
            [&intent_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?)),
        )
        .optional()?;
    let expected = (
        context.destination_id.clone(),
        context.capture_epoch.clone(),
        context.generation,
        range.first_seq,
        range.last_seq,
        ordered_event_digest,
    );
    match persisted {
        Some((destination, epoch, generation, first, last, digest, state))
            if destination == expected.0
                && epoch == expected.1
                && generation == expected.2
                && first == expected.3
                && last == expected.4
                && digest == expected.5
                && matches!(state.as_str(), "prepared" | "dispatched" | "verified") => {}
        _ => {
            return Err(DurabilityError::Conflict(
                "same batch ID has different intent",
            ));
        }
    }

    Ok(PreparedBatch {
        intent_id: intent_id.clone(),
        destination_id: context.destination_id.clone(),
        capture_epoch_text: context.capture_epoch.clone(),
        configuration_fingerprint: context.configuration_fingerprint.clone(),
        lease_id: context.lease_id.clone(),
        run_id: context.run_id.clone(),
        complete_transaction_id,
        events,
        marker,
    })
}

/// Durably records that an external insert may have happened. It is deliberately committed by the
/// caller before dispatch so a crash can never leave an untracked ambiguous external effect.
pub fn mark_insert_dispatched(
    transaction: &Transaction<'_>,
    batch: &PreparedBatch,
) -> Result<(), DurabilityError> {
    validate_prepared_batch(transaction, batch)?;
    let changed = transaction.execute(
        "UPDATE clickhouse_batch_intents SET state='dispatched' WHERE intent_id=?1 AND state='prepared'",
        [&batch.intent_id],
    )?;
    if changed == 1 || intent_state(transaction, &batch.intent_id)? == "dispatched" {
        Ok(())
    } else {
        Err(DurabilityError::Conflict(
            "batch cannot enter dispatched state",
        ))
    }
}

/// Issue synchronous event and marker inserts, then prove discoverability through server readback.
/// Replaying this operation is safe: exact duplicate event IDs and an exact duplicate marker are
/// accepted by `verify_remote_batch`.
pub fn dispatch_and_readback(
    executor: &mut impl ClickHouseBatchExecutor,
    batch: &PreparedBatch,
) -> Result<RemoteBatch, DurabilityError> {
    executor
        .insert_events_synchronously(batch)
        .map_err(|DispatchError| DurabilityError::Dispatch)?;
    let events = executor
        .read_events(batch)
        .map_err(|DispatchError| DurabilityError::Dispatch)?;
    verify_remote_events(batch, &events)?;
    executor
        .insert_marker_synchronously(&batch.marker)
        .map_err(|DispatchError| DurabilityError::Dispatch)?;
    let remote = executor
        .read_batch(&batch.marker)
        .map_err(|DispatchError| DurabilityError::Dispatch)?
        .ok_or(DurabilityError::ReadbackMissing)?;
    let live_fingerprint = executor
        .inspect_object_fingerprint()
        .map_err(|DispatchError| DurabilityError::Dispatch)?;
    if live_fingerprint != batch.marker.object_fingerprint {
        return Err(DurabilityError::Conflict(
            "ClickHouse object fingerprint drift",
        ));
    }
    verify_remote_batch(batch, &remote)?;
    Ok(remote)
}

/// On restart, an exact marker plus exact event identities adopts the prior write. Missing marker
/// means replay; any partial, same-ID/different-payload, or marker conflict blocks ClickHouse.
pub fn reconcile_remote(
    executor: &mut impl ClickHouseBatchExecutor,
    batch: &PreparedBatch,
) -> Result<Reconciliation, DurabilityError> {
    let Some(remote) = executor
        .read_batch(&batch.marker)
        .map_err(|DispatchError| DurabilityError::Dispatch)?
    else {
        return Ok(Reconciliation::ReplayRequired);
    };
    let live_fingerprint = executor
        .inspect_object_fingerprint()
        .map_err(|DispatchError| DurabilityError::Dispatch)?;
    if live_fingerprint != batch.marker.object_fingerprint {
        return Err(DurabilityError::Conflict(
            "ClickHouse object fingerprint drift",
        ));
    }
    verify_remote_batch(batch, &remote)?;
    Ok(Reconciliation::Verified)
}

/// Atomically mark the intent verified and move the checkpoint to the complete transaction at the
/// end of this batch. Lease/configuration/generation and the old checkpoint revision are rechecked
/// after remote work, preventing a stale completion from advancing a replacement generation.
pub fn finalize_checkpoint(
    transaction: &Transaction<'_>,
    context: &BatchContext,
    batch: &PreparedBatch,
    remote: &RemoteBatch,
    expected_checkpoint_revision: Option<u64>,
) -> Result<(), DurabilityError> {
    if batch.destination_id != context.destination_id
        || batch.capture_epoch_text != context.capture_epoch
        || batch.marker.generation != context.generation
        || batch.configuration_fingerprint != context.configuration_fingerprint
        || batch.lease_id != context.lease_id
        || batch.run_id != context.run_id
    {
        return Err(DurabilityError::Conflict("stale batch context"));
    }
    validate_destination_and_lease(transaction, context)?;
    validate_prepared_batch(transaction, batch)?;
    verify_remote_batch(batch, remote)?;
    if intent_state(transaction, &batch.intent_id)? != "dispatched" {
        return Err(DurabilityError::Conflict("batch was not dispatched"));
    }

    let changed = match expected_checkpoint_revision {
        Some(revision) => transaction.execute(
            "UPDATE destination_checkpoints SET complete_transaction_id=?1,journal_seq=?2,revision=revision+1 WHERE destination_id=?3 AND capture_epoch=?4 AND configuration_fingerprint=?5 AND generation=?6 AND journal_seq=?7 AND revision=?8 AND current_failure_id IS NULL",
            params![batch.complete_transaction_id, batch.marker.last_journal_seq, context.destination_id, context.capture_epoch, context.configuration_fingerprint, context.generation, batch.marker.first_journal_seq - 1, revision],
        )?,
        None if batch.marker.first_journal_seq == 1 => transaction.execute(
            "INSERT INTO destination_checkpoints(destination_id,capture_epoch,anchor_id,configuration_fingerprint,generation,complete_transaction_id,journal_seq,current_failure_id,revision) SELECT ?1,?2,NULL,?3,?4,?5,?6,NULL,0 WHERE NOT EXISTS(SELECT 1 FROM destination_checkpoints WHERE destination_id=?1)",
            params![context.destination_id, context.capture_epoch, context.configuration_fingerprint, context.generation, batch.complete_transaction_id, batch.marker.last_journal_seq],
        )?,
        None => 0,
    };
    if changed != 1 {
        return Err(DurabilityError::Conflict(
            "checkpoint compare-and-swap failed",
        ));
    }
    let verified = transaction.execute(
        "UPDATE clickhouse_batch_intents SET state='verified' WHERE intent_id=?1 AND state='dispatched'",
        [&batch.intent_id],
    )?;
    if verified != 1 {
        return Err(DurabilityError::Conflict(
            "intent finalization compare-and-swap failed",
        ));
    }
    Ok(())
}

/// Run the unchanged shared policy and persist its exact CAS projection in the caller's sole-writer
/// transaction. This is the ClickHouse domain adapter; it does not define a second retry policy.
pub fn persist_failure(
    transaction: &Transaction<'_>,
    context: &BatchContext,
    batch: &PreparedBatch,
    class: FailureClass,
    code: StableErrorCode,
    transition_context: &mut TransitionContext<'_>,
) -> Result<FailureRecord, DurabilityError> {
    let boundary = FailedBoundary::Destination {
        capture_epoch: context.capture_epoch.clone(),
        generation: context.generation,
        first_seq: batch.marker.first_journal_seq,
        last_seq: batch.marker.last_journal_seq,
    };
    let current_failure_id: Option<String> = transaction.query_row(
        "SELECT current_failure_id FROM destinations WHERE destination_id=?1",
        [&context.destination_id],
        |row| row.get(0),
    )?;
    let current = current_failure_id
        .as_deref()
        .map(|failure_id| load_failure(transaction, failure_id, boundary.clone()))
        .transpose()?
        .flatten();
    let action = transition(
        current.as_ref(),
        PolicyEvent::Observe(failure_observation(context, batch, class, code)),
        transition_context,
    );
    let record = match &action {
        PolicyAction::Persist(record) => record.clone(),
        PolicyAction::Suppressed => current.ok_or(DurabilityError::Conflict(
            "failure policy suppressed without persisted state",
        ))?,
        _ => {
            return Err(DurabilityError::Conflict(
                "failure policy rejected observation",
            ));
        }
    };
    if let Some(operation) =
        PreparedFailureOperation::from_policy_action(action, current_failure_id)
    {
        operation.execute(transaction)?;
    }
    Ok(record)
}

pub fn failure_observation(
    context: &BatchContext,
    batch: &PreparedBatch,
    class: FailureClass,
    code: StableErrorCode,
) -> FailureObservation {
    FailureObservation {
        destination_id: Some(context.destination_id.clone()),
        fingerprint: FingerprintInput {
            component: Component::ClickHouse,
            class,
            code,
            boundary: FailedBoundary::Destination {
                capture_epoch: context.capture_epoch.clone(),
                generation: context.generation,
                first_seq: batch.marker.first_journal_seq,
                last_seq: batch.marker.last_journal_seq,
            },
            relevant_configuration_fingerprint: context.configuration_fingerprint.clone(),
            context: BTreeMap::from([
                (SafeContextKey::Operation, SafeContextValue::Publish),
                (
                    SafeContextKey::DestinationKind,
                    SafeContextValue::ClickHouse,
                ),
            ]),
        },
    }
}

fn validate_context(context: &BatchContext) -> Result<(), DurabilityError> {
    if context.destination_id.is_empty()
        || context.capture_epoch.is_empty()
        || context.generation == 0
        || context.configuration_fingerprint.is_empty()
        || context.lease_id.is_empty()
        || context.run_id.is_empty()
    {
        return Err(DurabilityError::Invalid("incomplete batch context"));
    }
    Ok(())
}

fn validate_destination_and_lease(
    transaction: &Transaction<'_>,
    context: &BatchContext,
) -> Result<(), DurabilityError> {
    let valid_destination: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM destinations WHERE destination_id=?1 AND kind='clickhouse' AND capture_epoch=?2 AND generation=?3 AND configuration_fingerprint=?4 AND current_failure_id IS NULL)",
        params![context.destination_id, context.capture_epoch, context.generation, context.configuration_fingerprint],
        |row| row.get(0),
    )?;
    let valid_lease: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM destination_generation_leases WHERE lease_id=?1 AND destination_id=?2 AND capture_epoch=?3 AND generation=?4 AND configuration_fingerprint=?5 AND run_id=?6 AND state='held')",
        params![context.lease_id, context.destination_id, context.capture_epoch, context.generation, context.configuration_fingerprint, context.run_id],
        |row| row.get(0),
    )?;
    if valid_destination && valid_lease {
        Ok(())
    } else {
        Err(DurabilityError::OwnershipLost)
    }
}

fn validate_checkpoint_cursor(
    transaction: &Transaction<'_>,
    context: &BatchContext,
    first_seq: u64,
) -> Result<(), DurabilityError> {
    let checkpoint: Option<(String, u64, String, u64)> = transaction
        .query_row(
            "SELECT capture_epoch,generation,configuration_fingerprint,journal_seq FROM destination_checkpoints WHERE destination_id=?1",
            [&context.destination_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    match checkpoint {
        None if first_seq == 1 => Ok(()),
        Some((epoch, generation, fingerprint, journal_seq))
            if epoch == context.capture_epoch
                && generation == context.generation
                && fingerprint == context.configuration_fingerprint
                && journal_seq.checked_add(1) == Some(first_seq) =>
        {
            Ok(())
        }
        _ => Err(DurabilityError::Conflict(
            "batch does not start at checkpoint cursor",
        )),
    }
}

fn validate_prepared_batch(
    transaction: &Transaction<'_>,
    batch: &PreparedBatch,
) -> Result<(), DurabilityError> {
    let row: Option<(String, String, u64, u64, u64, String)> = transaction
        .query_row(
            "SELECT destination_id,capture_epoch,generation,first_seq,last_seq,payload_checksum FROM clickhouse_batch_intents WHERE intent_id=?1",
            [&batch.intent_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
        )
        .optional()?;
    if row
        == Some((
            batch.destination_id.clone(),
            batch.capture_epoch_text.clone(),
            batch.marker.generation,
            batch.marker.first_journal_seq,
            batch.marker.last_journal_seq,
            batch.marker.ordered_event_digest.clone(),
        ))
    {
        Ok(())
    } else {
        Err(DurabilityError::Conflict(
            "persisted intent differs from batch",
        ))
    }
}

fn intent_state(transaction: &Transaction<'_>, intent_id: &str) -> Result<String, DurabilityError> {
    transaction
        .query_row(
            "SELECT state FROM clickhouse_batch_intents WHERE intent_id=?1",
            [intent_id],
            |row| row.get(0),
        )
        .optional()?
        .ok_or(DurabilityError::Conflict("batch intent missing"))
}

fn destination_events(events: &[CopiedEvent]) -> Result<Vec<DestinationEvent>, DurabilityError> {
    let mut destination = Vec::new();
    let mut last_seq = None;
    let mut control_kind: Option<&str> = None;
    for event in events {
        if last_seq.is_some_and(|seq| event.journal_seq != seq + 1)
            || format!("{:x}", Sha256::digest(&event.payload)) != event.payload_hash
        {
            return Err(DurabilityError::Conflict("journal event content mismatch"));
        }
        last_seq = Some(event.journal_seq);
        if let Some(kind) = event.control_kind.as_deref() {
            if !matches!(kind, "heartbeat" | "capture_fence")
                || event.relation_schema_fingerprint.is_some()
                || !destination.is_empty()
                || control_kind.is_some_and(|previous| previous != kind)
            {
                return Err(DurabilityError::Conflict(
                    "invalid or mixed control transaction",
                ));
            }
            control_kind = Some(kind);
            continue;
        }
        if control_kind.is_some() {
            return Err(DurabilityError::Conflict(
                "invalid or mixed control transaction",
            ));
        }
        let relation_schema_fingerprint = event
            .relation_schema_fingerprint
            .clone()
            .ok_or(DurabilityError::Conflict("row event lacks relation schema"))?;
        destination.push(DestinationEvent {
            journal_seq: event.journal_seq,
            transaction_ordinal: event.transaction_ordinal,
            mutation_ordinal: event.mutation_ordinal,
            connector_event_id: event.event_id.clone(),
            relation_schema_fingerprint,
            payload: event.payload.clone(),
            payload_hash: event.payload_hash.clone(),
        });
    }
    Ok(destination)
}

fn ordered_event_digest(events: &[DestinationEvent]) -> String {
    let mut digest = Sha256::new();
    hash_part(&mut digest, b"boring-cdc/clickhouse-ordered-events/v1");
    for event in events {
        hash_part(&mut digest, &event.journal_seq.to_be_bytes());
        hash_part(&mut digest, event.connector_event_id.as_bytes());
        hash_part(&mut digest, event.payload_hash.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

fn batch_id(
    context: &BatchContext,
    first_seq: u64,
    last_seq: u64,
    ordered_event_digest: &str,
    object_fingerprint: &str,
) -> String {
    let mut digest = Sha256::new();
    for value in [
        "boring-cdc/clickhouse-batch/v1",
        &context.destination_id,
        &context.capture_epoch,
        &context.generation.to_string(),
        &first_seq.to_string(),
        &last_seq.to_string(),
        ordered_event_digest,
        &context.configuration_fingerprint,
        object_fingerprint,
    ] {
        hash_part(&mut digest, value.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

fn verify_remote_batch(batch: &PreparedBatch, remote: &RemoteBatch) -> Result<(), DurabilityError> {
    if remote.marker != batch.marker {
        return Err(DurabilityError::Conflict("remote batch marker mismatch"));
    }
    verify_remote_events(batch, &remote.events)
}

fn verify_remote_events(
    batch: &PreparedBatch,
    remote_events: &[RemoteEventIdentity],
) -> Result<(), DurabilityError> {
    let expected = batch
        .events
        .iter()
        .map(|event| {
            (
                event.connector_event_id.as_str(),
                (event.payload_hash.as_str(), event.payload.as_slice()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if expected.len() != batch.events.len() {
        return Err(DurabilityError::Conflict(
            "duplicate connector event ID in intent",
        ));
    }
    let mut observed = BTreeMap::<&str, (&str, &[u8])>::new();
    for event in remote_events {
        let recomputed = format!("{:x}", Sha256::digest(&event.canonical_payload));
        if recomputed != event.payload_hash {
            return Err(DurabilityError::Conflict(
                "remote payload differs from its stored hash",
            ));
        }
        let value = (
            event.payload_hash.as_str(),
            event.canonical_payload.as_slice(),
        );
        match observed.insert(&event.connector_event_id, value) {
            Some(previous) if previous != value => {
                return Err(DurabilityError::Conflict(
                    "remote connector event ID has multiple payloads",
                ));
            }
            _ => {}
        }
    }
    if observed != expected || observed.len() as u64 != batch.marker.event_count {
        return Err(DurabilityError::Conflict(
            "remote event set is incomplete or foreign",
        ));
    }
    Ok(())
}

fn hash_part(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m1_transition_kernel::{Randomness, VirtualClock};
    use crate::m2_leases::{LeaseIdentity, acquire};
    use crate::m2_schema::{WriterConnection, open_writer};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct FixedRandomness;
    impl Randomness for FixedRandomness {
        fn next_u64(&mut self) -> u64 {
            7
        }
    }

    struct FakeClickHouse {
        remote: Option<RemoteBatch>,
        event_inserts: usize,
        marker_inserts: usize,
    }
    impl ClickHouseBatchExecutor for FakeClickHouse {
        fn insert_events_synchronously(
            &mut self,
            batch: &PreparedBatch,
        ) -> Result<(), DispatchError> {
            self.event_inserts += 1;
            let events = batch
                .events
                .iter()
                .map(|event| RemoteEventIdentity {
                    connector_event_id: event.connector_event_id.clone(),
                    payload_hash: event.payload_hash.clone(),
                    canonical_payload: event.payload.clone(),
                })
                .collect();
            self.remote = Some(RemoteBatch {
                marker: batch.marker.clone(),
                events,
            });
            Ok(())
        }
        fn read_events(
            &mut self,
            _batch: &PreparedBatch,
        ) -> Result<Vec<RemoteEventIdentity>, DispatchError> {
            Ok(self
                .remote
                .as_ref()
                .map(|remote| remote.events.clone())
                .unwrap_or_default())
        }
        fn insert_marker_synchronously(
            &mut self,
            marker: &BatchMarker,
        ) -> Result<(), DispatchError> {
            self.marker_inserts += 1;
            self.remote.as_mut().ok_or(DispatchError)?.marker = marker.clone();
            Ok(())
        }
        fn read_batch(
            &mut self,
            _marker: &BatchMarker,
        ) -> Result<Option<RemoteBatch>, DispatchError> {
            Ok(self.remote.clone())
        }
        fn inspect_object_fingerprint(&mut self) -> Result<String, DispatchError> {
            Ok(object_fingerprint())
        }
    }

    fn writer(name: &str) -> (PathBuf, WriterConnection) {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("m4-durability-{name}-{unique}.sqlite"));
        let writer = open_writer(&path, "test-run", 1, 1_000).unwrap();
        (path, writer)
    }

    fn context() -> BatchContext {
        BatchContext {
            destination_id: "clickhouse-a".into(),
            capture_epoch: "7".into(),
            generation: 2,
            configuration_fingerprint: "cfg-a".into(),
            lease_id: "lease-a".into(),
            run_id: "run-a".into(),
        }
    }

    fn seed(writer: &mut WriterConnection) {
        let connection = writer.connection_mut();
        connection.execute("INSERT INTO source_transactions(transaction_id,capture_epoch,source_system_id,database_id,slot_name,xid,end_lsn,first_seq,last_seq,event_count,payload_checksum,state) VALUES('tx-a','7','source','db','slot','1','0000000000000002',1,2,2,'sum','committed')", []).unwrap();
        connection.execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('clickhouse-a','clickhouse','cfg-a','7',2)", []).unwrap();
        connection.execute("INSERT INTO destination_generation_leases(lease_id,destination_id,capture_epoch,anchor_id,generation,configuration_fingerprint,run_id,expires_mono_ms,state) VALUES('lease-a','clickhouse-a','7',NULL,2,'cfg-a','run-a',999999,'held')", []).unwrap();
    }

    fn copied(control_only: bool) -> CopiedRange {
        let payloads: [&[u8]; 2] = if control_only {
            [b"heartbeat", b"capture-fence"]
        } else {
            [b"row-a", b"row-b"]
        };
        let events = payloads
            .into_iter()
            .enumerate()
            .map(|(index, payload)| CopiedEvent {
                journal_seq: index as u64 + 1,
                transaction_id: "tx-a".into(),
                transaction_ordinal: 0,
                mutation_ordinal: index as u32,
                event_id: format!("event-{index}"),
                relation_schema_fingerprint: (!control_only).then(|| "schema-a".into()),
                source_relation_id: (!control_only).then(|| "relation-a".into()),
                control_kind: control_only.then(|| "heartbeat".into()),
                payload: payload.to_vec(),
                payload_hash: format!("{:x}", Sha256::digest(payload)),
            })
            .collect();
        CopiedRange {
            events,
            first_seq: 1,
            last_seq: 2,
            copied_bytes: 512,
        }
    }

    fn prepare(writer: &mut WriterConnection, range: CopiedRange) -> PreparedBatch {
        let transaction = writer.connection_mut().transaction().unwrap();
        let batch = prepare_batch(&transaction, &context(), range).unwrap();
        transaction.commit().unwrap();
        batch
    }

    fn mark(writer: &mut WriterConnection, batch: &PreparedBatch) {
        let transaction = writer.connection_mut().transaction().unwrap();
        mark_insert_dispatched(&transaction, batch).unwrap();
        transaction.commit().unwrap();
    }

    fn owned_lease(writer: &mut WriterConnection) -> LeaseToken {
        writer
            .connection_mut()
            .execute("DELETE FROM destination_generation_leases", [])
            .unwrap();
        writer.connection_mut().execute("INSERT INTO runtime_ownership(run_id,backend_pid,connection_nonce,ownership_deadline_mono_ms,state,connection_generation,expected_close_token,loss_evidence,takeover_evidence,revision) VALUES('run-a',1,'nonce-a',10000,'held',1,NULL,NULL,NULL,0)", []).unwrap();
        acquire(
            writer,
            LeaseIdentity {
                lease_id: "lease-a".into(),
                destination_id: "clickhouse-a".into(),
                capture_epoch: "7".into(),
                anchor_id: None,
                generation: 2,
                configuration_fingerprint: "cfg-a".into(),
                run_id: "run-a".into(),
            },
            100,
            1_000,
        )
        .unwrap()
    }

    #[test]
    fn production_path_uses_full_m2_lease_fence_around_remote_effect() {
        let (_path, mut writer) = writer("owned");
        seed(&mut writer);
        let token = owned_lease(&mut writer);
        let mut now = || 100;
        let batch = prepare_owned_batch(&mut writer, &token, &mut now, copied(false)).unwrap();
        let mut clickhouse = FakeClickHouse {
            remote: None,
            event_inserts: 0,
            marker_inserts: 0,
        };
        let remote =
            dispatch_owned_batch(&mut writer, &token, &mut now, &mut clickhouse, &batch).unwrap();
        finalize_owned_batch(&mut writer, &token, &mut now, &batch, &remote, None).unwrap();
        assert_eq!(
            writer
                .connection()
                .query_row(
                    "SELECT journal_seq FROM destination_checkpoints",
                    [],
                    |row| row.get::<_, u64>(0),
                )
                .unwrap(),
            2
        );

        let (_path, mut expired_writer) = self::writer("expired-owned");
        seed(&mut expired_writer);
        let expired = owned_lease(&mut expired_writer);
        let mut before = || 100;
        let expired_batch =
            prepare_owned_batch(&mut expired_writer, &expired, &mut before, copied(false)).unwrap();
        let mut after_expiry = || 1_101;
        let mut never_called = FakeClickHouse {
            remote: None,
            event_inserts: 0,
            marker_inserts: 0,
        };
        assert_eq!(
            dispatch_owned_batch(
                &mut expired_writer,
                &expired,
                &mut after_expiry,
                &mut never_called,
                &expired_batch,
            ),
            Err(DurabilityError::OwnershipLost)
        );
        assert_eq!(never_called.event_inserts, 0);
    }

    #[test]
    fn synchronous_readback_then_atomic_checkpoint_is_idempotent() {
        let (_path, mut writer) = writer("happy");
        seed(&mut writer);
        let batch = prepare(&mut writer, copied(false));
        mark(&mut writer, &batch);
        let mut clickhouse = FakeClickHouse {
            remote: None,
            event_inserts: 0,
            marker_inserts: 0,
        };
        let remote = dispatch_and_readback(&mut clickhouse, &batch).unwrap();
        assert_eq!(
            reconcile_remote(&mut clickhouse, &batch),
            Ok(Reconciliation::Verified)
        );
        let transaction = writer.connection_mut().transaction().unwrap();
        finalize_checkpoint(&transaction, &context(), &batch, &remote, None).unwrap();
        transaction.commit().unwrap();
        assert_eq!(
            writer
                .connection()
                .query_row(
                    "SELECT journal_seq FROM destination_checkpoints",
                    [],
                    |row| row.get::<_, u64>(0)
                )
                .unwrap(),
            2
        );
        assert_eq!(
            writer
                .connection()
                .query_row("SELECT state FROM clickhouse_batch_intents", [], |row| {
                    row.get::<_, String>(0)
                })
                .unwrap(),
            "verified"
        );
        assert_eq!(
            (clickhouse.event_inserts, clickhouse.marker_inserts),
            (1, 1)
        );
    }

    #[test]
    fn crash_after_insert_is_adopted_and_same_id_conflict_blocks_checkpoint() {
        let (_path, mut writer) = writer("reconcile");
        seed(&mut writer);
        let batch = prepare(&mut writer, copied(false));
        mark(&mut writer, &batch);
        let mut clickhouse = FakeClickHouse {
            remote: None,
            event_inserts: 0,
            marker_inserts: 0,
        };
        let remote = dispatch_and_readback(&mut clickhouse, &batch).unwrap();
        assert_eq!(
            reconcile_remote(&mut clickhouse, &batch),
            Ok(Reconciliation::Verified)
        );
        // Payload-only corruption preserves event ID, stored hash, and marker.
        clickhouse.remote.as_mut().unwrap().events[0].canonical_payload = b"tampered".to_vec();
        assert!(matches!(
            reconcile_remote(&mut clickhouse, &batch),
            Err(DurabilityError::Conflict(_))
        ));
        let transaction = writer.connection_mut().transaction().unwrap();
        assert!(matches!(
            finalize_checkpoint(
                &transaction,
                &context(),
                &batch,
                clickhouse.remote.as_ref().unwrap(),
                None
            ),
            Err(DurabilityError::Conflict(_))
        ));
        transaction.rollback().unwrap();
        assert_eq!(
            writer
                .connection()
                .query_row("SELECT count(*) FROM destination_checkpoints", [], |row| {
                    row.get::<_, u64>(0)
                })
                .unwrap(),
            0
        );
        assert_eq!(remote.marker, batch.marker);
    }

    #[test]
    fn heartbeat_and_capture_fence_advance_without_user_rows() {
        for kind in ["heartbeat", "capture_fence"] {
            let (_path, mut writer) = writer(kind);
            seed(&mut writer);
            let mut range = copied(true);
            for event in &mut range.events {
                event.control_kind = Some(kind.into());
            }
            let batch = prepare(&mut writer, range);
            assert!(batch.events.is_empty());
            assert_eq!(batch.marker.event_count, 0);
            mark(&mut writer, &batch);
            let mut clickhouse = FakeClickHouse {
                remote: None,
                event_inserts: 0,
                marker_inserts: 0,
            };
            let remote = dispatch_and_readback(&mut clickhouse, &batch).unwrap();
            let transaction = writer.connection_mut().transaction().unwrap();
            finalize_checkpoint(&transaction, &context(), &batch, &remote, None).unwrap();
            transaction.commit().unwrap();
            assert_eq!(
                writer
                    .connection()
                    .query_row(
                        "SELECT journal_seq FROM destination_checkpoints",
                        [],
                        |row| row.get::<_, u64>(0),
                    )
                    .unwrap(),
                2
            );
        }
    }

    #[test]
    fn malformed_or_mixed_control_transactions_fail_before_intent() {
        let (_path, mut writer) = writer("bad-control");
        seed(&mut writer);
        let mut range = copied(true);
        range.events[1].control_kind = Some("capture_fence".into());
        let transaction = writer.connection_mut().transaction().unwrap();
        assert_eq!(
            prepare_batch(&transaction, &context(), range),
            Err(DurabilityError::Conflict(
                "invalid or mixed control transaction"
            ))
        );
        transaction.rollback().unwrap();
    }

    #[test]
    fn stale_lease_or_checkpoint_completion_cannot_advance() {
        let (_path, mut writer) = writer("stale");
        seed(&mut writer);
        let batch = prepare(&mut writer, copied(false));
        mark(&mut writer, &batch);
        let mut clickhouse = FakeClickHouse {
            remote: None,
            event_inserts: 0,
            marker_inserts: 0,
        };
        let remote = dispatch_and_readback(&mut clickhouse, &batch).unwrap();
        writer.connection_mut().execute("UPDATE destination_generation_leases SET state='fenced',revision=revision+1 WHERE lease_id='lease-a'", []).unwrap();
        let transaction = writer.connection_mut().transaction().unwrap();
        assert_eq!(
            finalize_checkpoint(&transaction, &context(), &batch, &remote, None),
            Err(DurabilityError::OwnershipLost)
        );
        transaction.rollback().unwrap();
        assert_eq!(
            writer
                .connection()
                .query_row("SELECT count(*) FROM destination_checkpoints", [], |row| {
                    row.get::<_, u64>(0)
                })
                .unwrap(),
            0
        );
    }

    #[test]
    fn shared_failure_policy_adapter_persists_attempts_and_exact_boundary() {
        let (_path, mut writer) = writer("policy-persistence");
        seed(&mut writer);
        let batch = prepare(&mut writer, copied(false));
        let clock = VirtualClock::new(10_000);
        let mut randomness = FixedRandomness;
        let transaction = writer.connection_mut().transaction().unwrap();
        let mut transition_context = TransitionContext {
            clock: &clock,
            randomness: &mut randomness,
        };
        let first = persist_failure(
            &transaction,
            &context(),
            &batch,
            FailureClass::TransientDestination,
            StableErrorCode::TransportUnavailable,
            &mut transition_context,
        )
        .unwrap();
        transaction.commit().unwrap();
        assert_eq!(first.attempt, 1);
        assert_eq!(writer.connection().query_row(
            "SELECT current_failure_id FROM destinations WHERE destination_id='clickhouse-a'",
            [],
            |row| row.get::<_, String>(0),
        ).unwrap(), first.failure_id);

        let clock = VirtualClock::new(20_000);
        let mut randomness = FixedRandomness;
        let transaction = writer.connection_mut().transaction().unwrap();
        let mut transition_context = TransitionContext {
            clock: &clock,
            randomness: &mut randomness,
        };
        let second = persist_failure(
            &transaction,
            &context(),
            &batch,
            FailureClass::TransientDestination,
            StableErrorCode::TransportUnavailable,
            &mut transition_context,
        )
        .unwrap();
        transaction.commit().unwrap();
        assert_eq!(second.failure_id, first.failure_id);
        assert_eq!(second.attempt, 2);
    }

    #[test]
    fn shared_failure_policy_adapter_keeps_exact_failed_boundary() {
        let (_path, mut writer) = writer("policy");
        seed(&mut writer);
        let batch = prepare(&mut writer, copied(false));
        let observation = failure_observation(
            &context(),
            &batch,
            FailureClass::TransientDestination,
            StableErrorCode::TransportUnavailable,
        );
        assert_eq!(observation.destination_id.as_deref(), Some("clickhouse-a"));
        assert_eq!(observation.fingerprint.component, Component::ClickHouse);
        assert_eq!(
            observation.fingerprint.boundary,
            FailedBoundary::Destination {
                capture_epoch: "7".into(),
                generation: 2,
                first_seq: 1,
                last_seq: 2
            }
        );
    }
}
