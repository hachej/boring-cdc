//! Deterministic mutation convergence for ClickHouse append-only history.
//!
//! This is deliberately destination-side: it consumes complete mutation identities copied by the
//! durable M4 batch contract and never changes capture, journal, or spool semantics. Physical
//! retries remain observable while logical state is selected by connector event identity first and
//! the complete source version second.

use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SourceVersion {
    pub lsn_u64: u64,
    pub origin_rank: u8,
    pub transaction_ordinal: u64,
    pub mutation_ordinal: u8,
    pub connector_event_id: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    Snapshot,
    Insert,
    Update,
    Delete,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationKind {
    Delete,
    Upsert,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CellState {
    AbsentForSchema,
    ExplicitNull,
    UnchangedToast,
    ExplicitValue(Vec<u8>),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Cell {
    pub column_id: u32,
    pub type_oid: u32,
    pub typmod: i32,
    pub state: CellState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationEvent {
    pub capture_epoch: u64,
    pub generation: u64,
    pub logical_table_id: [u8; 32],
    pub relation_schema_fingerprint: [u8; 32],
    pub canonical_key: Vec<u8>,
    pub key_hash: [u8; 32],
    pub before_key: Option<Vec<u8>>,
    pub operation: Operation,
    pub mutation_kind: MutationKind,
    pub version: SourceVersion,
    pub journal_seq: u64,
    pub cells: Vec<Cell>,
    pub payload_hash: [u8; 32],
}

impl MutationEvent {
    pub fn seal(mut self) -> Self {
        self.payload_hash = canonical_payload_hash(&self);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentRow {
    pub canonical_key: Vec<u8>,
    pub cells: BTreeMap<u32, Cell>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Tombstone {
    pub canonical_key: Vec<u8>,
    pub version: SourceVersion,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConvergedState {
    pub rows: BTreeMap<Vec<u8>, CurrentRow>,
    pub tombstones: BTreeMap<Vec<u8>, Tombstone>,
    pub physical_attempts: usize,
    pub logical_events: usize,
    pub duplicate_attempts: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MutationError {
    EmptyStream,
    MixedNamespace,
    DuplicateColumn(u32),
    PayloadHashMismatch { event_id: [u8; 32] },
    EventIdentityConflict { event_id: [u8; 32] },
    MissingToastPredecessor { column_id: u32 },
    KeyChangeWithUnchangedToast,
    InvalidKeyChange,
    CheckpointGap,
}

fn part(d: &mut Sha256, bytes: &[u8]) {
    d.update((bytes.len() as u64).to_be_bytes());
    d.update(bytes);
}

/// Hashes every canonical identity/value field without UTF-8 conversion or delimiter ambiguity.
pub fn canonical_payload_hash(event: &MutationEvent) -> [u8; 32] {
    let mut d = Sha256::new();
    part(&mut d, b"boring-cdc/m4-mutation/v1");
    d.update(event.capture_epoch.to_be_bytes());
    d.update(event.generation.to_be_bytes());
    d.update(event.logical_table_id);
    d.update(event.relation_schema_fingerprint);
    part(&mut d, &event.canonical_key);
    d.update(event.key_hash);
    match &event.before_key {
        Some(key) => {
            d.update([1]);
            part(&mut d, key);
        }
        None => d.update([0]),
    }
    d.update([event.operation as u8, event.mutation_kind as u8]);
    d.update(event.version.lsn_u64.to_be_bytes());
    d.update([event.version.origin_rank]);
    d.update(event.version.transaction_ordinal.to_be_bytes());
    d.update([event.version.mutation_ordinal]);
    d.update(event.version.connector_event_id);
    d.update(event.journal_seq.to_be_bytes());
    d.update((event.cells.len() as u64).to_be_bytes());
    for cell in &event.cells {
        d.update(cell.column_id.to_be_bytes());
        d.update(cell.type_oid.to_be_bytes());
        d.update(cell.typmod.to_be_bytes());
        match &cell.state {
            CellState::AbsentForSchema => d.update([0]),
            CellState::ExplicitNull => d.update([1]),
            CellState::UnchangedToast => d.update([2]),
            CellState::ExplicitValue(value) => {
                d.update([3]);
                part(&mut d, value);
            }
        }
    }
    d.finalize().into()
}

/// Expands a primary-key update into an old-key tombstone followed by a new-key upsert. Callers
/// supply the two positional event IDs; unchanged TOAST is rejected before either row is emitted.
pub fn expand_key_change(
    mut old: MutationEvent,
    mut new: MutationEvent,
) -> Result<[MutationEvent; 2], MutationError> {
    if old.canonical_key == new.canonical_key
        || old.capture_epoch != new.capture_epoch
        || old.generation != new.generation
        || old.logical_table_id != new.logical_table_id
        || old.version.mutation_ordinal >= new.version.mutation_ordinal
    {
        return Err(MutationError::InvalidKeyChange);
    }
    if new
        .cells
        .iter()
        .any(|cell| cell.state == CellState::UnchangedToast)
    {
        return Err(MutationError::KeyChangeWithUnchangedToast);
    }
    old.operation = Operation::Delete;
    old.mutation_kind = MutationKind::Delete;
    old.before_key = None;
    old.cells.clear();
    new.operation = Operation::Update;
    new.mutation_kind = MutationKind::Upsert;
    new.before_key = Some(old.canonical_key.clone());
    old.payload_hash = canonical_payload_hash(&old);
    new.payload_hash = canonical_payload_hash(&new);
    Ok([old, new])
}

/// Produces exact current state from any physical order, including rows visible before or after
/// ClickHouse background merges. Identity conflicts are detected before source-version winners.
pub fn converge(events: &[MutationEvent]) -> Result<ConvergedState, MutationError> {
    let first = events.first().ok_or(MutationError::EmptyStream)?;
    let namespace = (
        first.capture_epoch,
        first.generation,
        first.logical_table_id,
    );
    let mut identities: HashMap<[u8; 32], ([u8; 32], [u8; 32])> = HashMap::new();
    let mut logical = Vec::new();
    let mut duplicate_attempts = 0;
    for event in events {
        if (
            event.capture_epoch,
            event.generation,
            event.logical_table_id,
        ) != namespace
        {
            return Err(MutationError::MixedNamespace);
        }
        let canonical = canonical_payload_hash(event);
        if canonical != event.payload_hash {
            return Err(MutationError::PayloadHashMismatch {
                event_id: event.version.connector_event_id,
            });
        }
        match identities.get(&event.version.connector_event_id) {
            Some((stored, stored_canonical))
                if *stored != event.payload_hash || *stored_canonical != canonical =>
            {
                return Err(MutationError::EventIdentityConflict {
                    event_id: event.version.connector_event_id,
                });
            }
            Some(_) => {
                duplicate_attempts += 1;
                continue;
            }
            None => {
                identities.insert(
                    event.version.connector_event_id,
                    (event.payload_hash, canonical),
                );
                logical.push(event);
            }
        }
    }
    logical.sort_by_key(|event| event.version);
    let mut rows: BTreeMap<Vec<u8>, CurrentRow> = BTreeMap::new();
    let mut tombstones = BTreeMap::new();
    for event in &logical {
        let mut seen = BTreeMap::new();
        for cell in &event.cells {
            if seen.insert(cell.column_id, ()).is_some() {
                return Err(MutationError::DuplicateColumn(cell.column_id));
            }
        }
        if event.operation == Operation::Delete || event.mutation_kind == MutationKind::Delete {
            rows.remove(&event.canonical_key);
            tombstones.insert(
                event.canonical_key.clone(),
                Tombstone {
                    canonical_key: event.canonical_key.clone(),
                    version: event.version,
                },
            );
            continue;
        }
        let row = rows
            .entry(event.canonical_key.clone())
            .or_insert_with(|| CurrentRow {
                canonical_key: event.canonical_key.clone(),
                cells: BTreeMap::new(),
            });
        for cell in &event.cells {
            match &cell.state {
                CellState::UnchangedToast if !row.cells.contains_key(&cell.column_id) => {
                    return Err(MutationError::MissingToastPredecessor {
                        column_id: cell.column_id,
                    });
                }
                CellState::UnchangedToast => {}
                CellState::AbsentForSchema => {
                    row.cells.insert(
                        cell.column_id,
                        Cell {
                            state: CellState::ExplicitNull,
                            ..cell.clone()
                        },
                    );
                }
                CellState::ExplicitNull | CellState::ExplicitValue(_) => {
                    row.cells.insert(cell.column_id, cell.clone());
                }
            }
        }
        tombstones.remove(&event.canonical_key);
    }
    Ok(ConvergedState {
        rows,
        tombstones,
        physical_attempts: events.len(),
        logical_events: logical.len(),
        duplicate_attempts,
    })
}

/// Checkpoint movement is independent from logical dedupe: a verified replay of an already-covered
/// range is a no-op, while a new range must begin at the next complete journal boundary.
pub fn advance_checkpoint(current: u64, first: u64, last: u64) -> Result<u64, MutationError> {
    if first == 0 || last < first {
        return Err(MutationError::CheckpointGap);
    }
    if last <= current {
        return Ok(current);
    }
    if first != current + 1 {
        return Err(MutationError::CheckpointGap);
    }
    Ok(last)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    fn id(n: u8) -> [u8; 32] {
        [n; 32]
    }
    fn event(
        key: &[u8],
        idn: u8,
        lsn: u64,
        ordinal: u8,
        op: Operation,
        kind: MutationKind,
        cells: Vec<Cell>,
    ) -> MutationEvent {
        MutationEvent {
            capture_epoch: 7,
            generation: 3,
            logical_table_id: id(9),
            relation_schema_fingerprint: id(8),
            canonical_key: key.to_vec(),
            key_hash: id(6),
            before_key: None,
            operation: op,
            mutation_kind: kind,
            version: SourceVersion {
                lsn_u64: lsn,
                origin_rank: 1,
                transaction_ordinal: 4,
                mutation_ordinal: ordinal,
                connector_event_id: id(idn),
            },
            journal_seq: lsn,
            cells,
            payload_hash: [0; 32],
        }
        .seal()
    }
    fn value(column_id: u32, bytes: &[u8]) -> Cell {
        Cell {
            column_id,
            type_oid: 25,
            typmod: -1,
            state: CellState::ExplicitValue(bytes.to_vec()),
        }
    }
    #[test]
    fn identical_physical_retries_collapse_before_winner_selection() {
        let e = event(
            b"\0full-key",
            1,
            10,
            0,
            Operation::Insert,
            MutationKind::Upsert,
            vec![value(1, b"v")],
        );
        let state = converge(&[e.clone(), e]).unwrap();
        assert_eq!(
            (
                state.physical_attempts,
                state.logical_events,
                state.duplicate_attempts
            ),
            (2, 1, 1)
        );
        assert_eq!(state.rows.len(), 1);
    }
    #[test]
    fn same_event_id_with_payload_mismatch_blocks() {
        let a = event(
            b"k",
            1,
            10,
            0,
            Operation::Insert,
            MutationKind::Upsert,
            vec![value(1, b"a")],
        );
        let mut b = event(
            b"k",
            1,
            10,
            0,
            Operation::Insert,
            MutationKind::Upsert,
            vec![value(1, b"b")],
        );
        b.version.connector_event_id = a.version.connector_event_id;
        b.payload_hash = canonical_payload_hash(&b);
        assert!(matches!(
            converge(&[a, b]),
            Err(MutationError::EventIdentityConflict { .. })
        ));
    }
    #[test]
    fn repeated_key_total_order_ignores_physical_and_journal_order() {
        let old = event(
            b"k",
            1,
            20,
            0,
            Operation::Update,
            MutationKind::Upsert,
            vec![value(1, b"old")],
        );
        let mut new = event(
            b"k",
            2,
            21,
            0,
            Operation::Update,
            MutationKind::Upsert,
            vec![value(1, b"new")],
        );
        new.journal_seq = 1;
        new.payload_hash = canonical_payload_hash(&new);
        let state = converge(&[new, old]).unwrap();
        assert_eq!(
            state.rows[b"k".as_slice()].cells[&1].state,
            CellState::ExplicitValue(b"new".to_vec())
        );
    }
    #[test]
    fn delete_reinsert_resets_predecessor_and_late_snapshot_loses() {
        let insert = event(
            b"k",
            1,
            10,
            0,
            Operation::Insert,
            MutationKind::Upsert,
            vec![value(1, b"old")],
        );
        let delete = event(
            b"k",
            2,
            20,
            0,
            Operation::Delete,
            MutationKind::Delete,
            vec![],
        );
        let toast = event(
            b"k",
            3,
            30,
            0,
            Operation::Insert,
            MutationKind::Upsert,
            vec![Cell {
                column_id: 1,
                type_oid: 25,
                typmod: -1,
                state: CellState::UnchangedToast,
            }],
        );
        assert_eq!(
            converge(&[toast, insert.clone(), delete.clone()]),
            Err(MutationError::MissingToastPredecessor { column_id: 1 })
        );
        let reinsert = event(
            b"k",
            4,
            30,
            0,
            Operation::Insert,
            MutationKind::Upsert,
            vec![value(1, b"fresh")],
        );
        let state = converge(&[reinsert, insert, delete]).unwrap();
        assert_eq!(
            state.rows[b"k".as_slice()].cells[&1].state,
            CellState::ExplicitValue(b"fresh".to_vec())
        );
    }
    #[test]
    fn tombstone_wins_and_remains_observable() {
        let insert = event(
            b"k",
            1,
            10,
            0,
            Operation::Insert,
            MutationKind::Upsert,
            vec![value(1, b"v")],
        );
        let delete = event(
            b"k",
            2,
            11,
            0,
            Operation::Delete,
            MutationKind::Delete,
            vec![],
        );
        let state = converge(&[delete, insert]).unwrap();
        assert!(state.rows.is_empty());
        assert_eq!(state.tombstones[b"k".as_slice()].version.lsn_u64, 11);
    }
    #[test]
    fn key_change_is_old_delete_then_new_upsert_with_full_binary_keys() {
        let old = event(
            b"\0old\xff",
            1,
            50,
            0,
            Operation::Update,
            MutationKind::Upsert,
            vec![],
        );
        let new = event(
            b"\0new\xfe",
            2,
            50,
            1,
            Operation::Update,
            MutationKind::Upsert,
            vec![value(1, b"v")],
        );
        let expanded = expand_key_change(old, new).unwrap();
        let state = converge(&expanded).unwrap();
        assert!(state.rows.contains_key(b"\0new\xfe".as_slice()));
        assert!(state.tombstones.contains_key(b"\0old\xff".as_slice()));
        assert_eq!(
            expanded[1].before_key.as_deref(),
            Some(b"\0old\xff".as_slice())
        );
    }
    #[test]
    fn key_change_with_unchanged_toast_blocks_before_output() {
        let old = event(
            b"old",
            1,
            50,
            0,
            Operation::Update,
            MutationKind::Upsert,
            vec![],
        );
        let new = event(
            b"new",
            2,
            50,
            1,
            Operation::Update,
            MutationKind::Upsert,
            vec![Cell {
                column_id: 1,
                type_oid: 25,
                typmod: -1,
                state: CellState::UnchangedToast,
            }],
        );
        assert_eq!(
            expand_key_change(old, new),
            Err(MutationError::KeyChangeWithUnchangedToast)
        );
    }
    #[test]
    fn replay_checkpoint_is_independent_and_never_double_advances() {
        assert_eq!(advance_checkpoint(9, 10, 12), Ok(12));
        assert_eq!(advance_checkpoint(12, 10, 12), Ok(12));
        assert_eq!(
            advance_checkpoint(12, 14, 15),
            Err(MutationError::CheckpointGap)
        );
    }
    #[test]
    fn cross_epoch_comparison_is_rejected() {
        let a = event(
            b"k",
            1,
            1,
            0,
            Operation::Insert,
            MutationKind::Upsert,
            vec![],
        );
        let mut b = event(
            b"k",
            2,
            2,
            0,
            Operation::Update,
            MutationKind::Upsert,
            vec![],
        );
        b.capture_epoch = 8;
        b.payload_hash = canonical_payload_hash(&b);
        assert_eq!(converge(&[a, b]), Err(MutationError::MixedNamespace));
    }
}
