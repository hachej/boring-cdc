//! Production ClickHouse adapter wiring.
//!
//! The network client is deliberately a narrow injected transport: credentials and raw driver
//! errors stay below this boundary, while this adapter fixes synchronous settings and maps fresh
//! typed readback into the durability and audit coordinators.

use crate::m4_clickhouse_audit::{
    AuditDispatchError, AuditIdentity, ClickHouseAuditExecutor, DestinationContractObservation,
    StoredAuditBatch,
};
use crate::m4_clickhouse_durability::{
    BatchMarker, ClickHouseBatchExecutor, DispatchError, PreparedBatch, RemoteBatch,
    RemoteEventIdentity,
};
#[cfg(test)]
use crate::m4_clickhouse_schema::INSERT_SETTINGS;

/// Query-scoped members of the full fingerprinted insert contract. The fsync members are
/// MergeTree table settings installed by DDL and are verified through contract inspection; sending
/// them as query settings is rejected by ClickHouse 25.8.
pub const SYNCHRONOUS_QUERY_SETTINGS: [(&str, u8); 4] = [
    ("async_insert", 0),
    ("wait_for_async_insert", 1),
    ("insert_quorum", 1),
    ("insert_deduplicate", 0),
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WireRequest<'a> {
    InsertEvents {
        batch: &'a PreparedBatch,
        settings: &'static [(&'static str, u8)],
    },
    ReadEventsFresh {
        batch: &'a PreparedBatch,
    },
    InsertMarker {
        marker: &'a BatchMarker,
        settings: &'static [(&'static str, u8)],
    },
    ReadBatchFresh {
        marker: &'a BatchMarker,
    },
    InspectContractFresh,
    ReadAuditRangeFresh {
        identity: &'a AuditIdentity,
        first_seq: u64,
        last_seq: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WireResponse {
    Acknowledged,
    Events(Vec<RemoteEventIdentity>),
    Batch(Option<RemoteBatch>),
    Contract(DestinationContractObservation),
    AuditBatch(StoredAuditBatch),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedactedWireError;

/// Implemented by the pinned ClickHouse client. Implementations accept structured values rather
/// than SQL strings and must map every driver failure to `RedactedWireError` before returning.
pub trait ClickHouseWire {
    fn execute(&mut self, request: WireRequest<'_>) -> Result<WireResponse, RedactedWireError>;
}

pub struct ProductionClickHouseAdapter<W> {
    wire: W,
}
impl<W> ProductionClickHouseAdapter<W> {
    pub fn new(wire: W) -> Self {
        Self { wire }
    }
    pub fn into_inner(self) -> W {
        self.wire
    }
}

impl<W: ClickHouseWire> ClickHouseBatchExecutor for ProductionClickHouseAdapter<W> {
    fn insert_events_synchronously(&mut self, batch: &PreparedBatch) -> Result<(), DispatchError> {
        acknowledged(self.wire.execute(WireRequest::InsertEvents {
            batch,
            settings: &SYNCHRONOUS_QUERY_SETTINGS,
        }))
    }
    fn read_events(
        &mut self,
        batch: &PreparedBatch,
    ) -> Result<Vec<RemoteEventIdentity>, DispatchError> {
        match self.wire.execute(WireRequest::ReadEventsFresh { batch }) {
            Ok(WireResponse::Events(events)) => Ok(events),
            _ => Err(DispatchError),
        }
    }
    fn insert_marker_synchronously(&mut self, marker: &BatchMarker) -> Result<(), DispatchError> {
        acknowledged(self.wire.execute(WireRequest::InsertMarker {
            marker,
            settings: &SYNCHRONOUS_QUERY_SETTINGS,
        }))
    }
    fn read_batch(&mut self, marker: &BatchMarker) -> Result<Option<RemoteBatch>, DispatchError> {
        match self.wire.execute(WireRequest::ReadBatchFresh { marker }) {
            Ok(WireResponse::Batch(batch)) => Ok(batch),
            _ => Err(DispatchError),
        }
    }
    fn inspect_object_fingerprint(&mut self) -> Result<String, DispatchError> {
        match self.wire.execute(WireRequest::InspectContractFresh) {
            Ok(WireResponse::Contract(observation)) => Ok(observation.object_fingerprint),
            _ => Err(DispatchError),
        }
    }
}

impl<W: ClickHouseWire> ClickHouseAuditExecutor for ProductionClickHouseAdapter<W> {
    fn inspect_contract(&mut self) -> Result<DestinationContractObservation, AuditDispatchError> {
        match self.wire.execute(WireRequest::InspectContractFresh) {
            Ok(WireResponse::Contract(observation)) => Ok(observation),
            _ => Err(AuditDispatchError),
        }
    }
    fn read_stored_range(
        &mut self,
        identity: &AuditIdentity,
        first_seq: u64,
        last_seq: u64,
    ) -> Result<StoredAuditBatch, AuditDispatchError> {
        match self.wire.execute(WireRequest::ReadAuditRangeFresh {
            identity,
            first_seq,
            last_seq,
        }) {
            Ok(WireResponse::AuditBatch(batch)) => Ok(batch),
            _ => Err(AuditDispatchError),
        }
    }
}

fn acknowledged(response: Result<WireResponse, RedactedWireError>) -> Result<(), DispatchError> {
    match response {
        Ok(WireResponse::Acknowledged) => Ok(()),
        _ => Err(DispatchError),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    #[derive(Default)]
    struct RecordingWire {
        responses: VecDeque<WireResponse>,
        synchronous_settings_seen: usize,
        fresh_reads_seen: usize,
    }
    impl ClickHouseWire for RecordingWire {
        fn execute(&mut self, request: WireRequest<'_>) -> Result<WireResponse, RedactedWireError> {
            match request {
                WireRequest::InsertEvents { settings, .. }
                | WireRequest::InsertMarker { settings, .. } => {
                    assert_eq!(settings, SYNCHRONOUS_QUERY_SETTINGS);
                    self.synchronous_settings_seen += 1;
                }
                WireRequest::ReadEventsFresh { .. }
                | WireRequest::ReadBatchFresh { .. }
                | WireRequest::InspectContractFresh
                | WireRequest::ReadAuditRangeFresh { .. } => self.fresh_reads_seen += 1,
            }
            self.responses.pop_front().ok_or(RedactedWireError)
        }
    }

    fn batch() -> PreparedBatch {
        PreparedBatch {
            intent_id: "batch".into(),
            destination_id: "ch".into(),
            capture_epoch_text: "1".into(),
            configuration_fingerprint: "cfg".into(),
            lease_id: "lease".into(),
            run_id: "run".into(),
            complete_transaction_id: "tx".into(),
            events: vec![],
            marker: BatchMarker {
                capture_epoch: 1,
                generation: 1,
                batch_id: "batch".into(),
                first_journal_seq: 1,
                last_journal_seq: 1,
                event_count: 0,
                ordered_event_digest: "digest".into(),
                object_fingerprint: "objects".into(),
            },
        }
    }

    #[test]
    fn production_wiring_forces_sync_writes_and_fresh_readback() {
        let batch = batch();
        let remote = RemoteBatch {
            marker: batch.marker.clone(),
            events: vec![],
        };
        let wire = RecordingWire {
            responses: VecDeque::from([
                WireResponse::Acknowledged,
                WireResponse::Events(vec![]),
                WireResponse::Acknowledged,
                WireResponse::Batch(Some(remote)),
                WireResponse::Contract(DestinationContractObservation {
                    selector_fingerprint: "s".into(),
                    object_fingerprint: "objects".into(),
                    settings_fingerprint: "x".into(),
                    expected_settings_fingerprint: "x".into(),
                    event_identity_conflicts: 0,
                    marker_identity_conflicts: 0,
                }),
            ]),
            ..Default::default()
        };
        let mut adapter = ProductionClickHouseAdapter::new(wire);
        adapter.insert_events_synchronously(&batch).unwrap();
        adapter.read_events(&batch).unwrap();
        adapter.insert_marker_synchronously(&batch.marker).unwrap();
        adapter.read_batch(&batch.marker).unwrap();
        adapter.inspect_object_fingerprint().unwrap();
        let wire = adapter.into_inner();
        assert_eq!(wire.synchronous_settings_seen, 2);
        assert_eq!(wire.fresh_reads_seen, 3);
        assert!(INSERT_SETTINGS.contains(&("fsync_after_insert", 1)));
        assert!(
            !SYNCHRONOUS_QUERY_SETTINGS
                .iter()
                .any(|(name, _)| name.starts_with("fsync_"))
        );
    }
}
