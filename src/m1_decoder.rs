//! Bounded, side-effect-free PostgreSQL CopyBoth/pgoutput protocol decoder.
//!
//! The transport feeds one PostgreSQL `CopyData` payload at a time.  This module never
//! acknowledges received bytes: callers can create a standby-status packet only from an
//! explicitly supplied durable complete-transaction boundary.

use crate::m1_transition_kernel::DurableSourceBoundary;
use std::collections::BTreeMap;

// M0-RECONCILED: boring-cdc-d-pg-protocol (RECOMMENDED PostgreSQL majors).
pub const SUPPORTED_POSTGRES_MAJORS: [u16; 3] = [15, 16, 17];
// M0-RECONCILED: boring-cdc-d-pg-protocol (RECOMMENDED protocol zero sentinel).
pub const PROTOCOL_ZERO_SENTINEL: u64 = 0;
// M0-RECONCILED: boring-cdc-d-pg-protocol (RECOMMENDED PostgreSQL-to-Unix epoch offset).
const PG_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WireLimits {
    pub max_copy_data_bytes: usize,
    pub max_pgoutput_message_bytes: usize,
    pub max_columns: usize,
    pub max_tuple_bytes: usize,
    pub max_relations: usize,
}

impl Default for WireLimits {
    fn default() -> Self {
        Self {
            max_copy_data_bytes: 1_048_576,
            max_pgoutput_message_bytes: 1_048_551,
            max_columns: 1_024,
            max_tuple_bytes: 1_048_576,
            // M0-RECONCILED: boring-cdc-d-pg-protocol (RECOMMENDED relation-cache bound).
            max_relations: 4_096,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FailureClass {
    Wire,
    Unsupported,
    ProtocolState,
    Contract,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodeFailure {
    pub class: FailureClass,
    pub fingerprint: &'static str,
    pub failed_boundary: &'static str,
    pub recovery: &'static str,
}

impl DecodeFailure {
    fn new(class: FailureClass, fingerprint: &'static str) -> Self {
        Self {
            class,
            fingerprint,
            failed_boundary: "before_feedback",
            recovery: "inspect_and_reseed_if_continuity_changed",
        }
    }
}

type Result<T> = std::result::Result<T, DecodeFailure>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TupleValue {
    Null,
    UnchangedToast,
    Text(Vec<u8>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Column {
    pub key: bool,
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Relation {
    pub id: u32,
    pub namespace: String,
    pub name: String,
    pub replica_identity: u8,
    pub columns: Vec<Column>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationContract {
    pub relation: Relation,
    pub key_columns: Vec<usize>,
    pub control: Option<ControlContract>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlContract {
    pub immutable_key: Vec<Vec<u8>>,
    pub mutable_columns: Vec<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RowKind {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OldTupleKind {
    Key,
    Full,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowChange {
    pub xid: u32,
    pub ordinal: u64,
    pub relation_id: u32,
    pub kind: RowKind,
    pub old_kind: Option<OldTupleKind>,
    pub old: Option<Vec<TupleValue>>,
    pub new: Option<Vec<TupleValue>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PgoutputEvent {
    Begin {
        final_lsn: u64,
        commit_time: i64,
        xid: u32,
    },
    RelationNeedsValidation(Relation),
    RelationMetadata(Relation),
    Origin {
        origin_lsn: u64,
        name: String,
    },
    Row(RowChange),
    Commit {
        flags: u8,
        commit_lsn: u64,
        end_lsn: u64,
        commit_time: i64,
        row_count: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CopyBothEvent {
    Keepalive {
        wal_end: u64,
        server_time: i64,
        reply_requested: bool,
    },
    XLogData {
        wal_start: u64,
        wal_end: u64,
        server_time: i64,
        event: PgoutputEvent,
    },
}

#[derive(Clone, Debug)]
struct Transaction {
    xid: u32,
    final_lsn: u64,
    next_ordinal: u64,
    failed: bool,
}

pub struct Decoder {
    limits: WireLimits,
    relations: BTreeMap<u32, RelationContract>,
    pending_relations: BTreeMap<u32, Relation>,
    transaction: Option<Transaction>,
    blocked: Option<DecodeFailure>,
}

impl Decoder {
    pub fn new(limits: WireLimits) -> Self {
        Self {
            limits,
            relations: BTreeMap::new(),
            pending_relations: BTreeMap::new(),
            transaction: None,
            blocked: None,
        }
    }

    pub fn is_feedback_blocked(&self) -> bool {
        self.blocked.is_some() || self.transaction.as_ref().is_some_and(|t| t.failed)
    }
    pub fn failure(&self) -> Option<&DecodeFailure> {
        self.blocked.as_ref()
    }

    /// Bind the exact catalog-validated contract after a `RelationNeedsValidation` event.
    pub fn admit_relation(&mut self, contract: RelationContract) -> Result<()> {
        let pending = self
            .pending_relations
            .remove(&contract.relation.id)
            .ok_or_else(|| fail(FailureClass::Contract, "RELATION_VALIDATION_NOT_PENDING"))?;
        let relation_key_columns = pending
            .columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| column.key.then_some(index))
            .collect::<Vec<_>>();
        if pending != contract.relation
            || contract.key_columns.is_empty()
            || contract.key_columns != relation_key_columns
        {
            return self.block(fail(FailureClass::Contract, "RELATION_CONTRACT_MISMATCH"));
        }
        if let Some(control) = &contract.control
            && (control.immutable_key.len() != contract.key_columns.len()
                || control
                    .mutable_columns
                    .iter()
                    .any(|&i| i >= pending.columns.len() || contract.key_columns.contains(&i)))
        {
            return self.block(fail(FailureClass::Contract, "CONTROL_CONTRACT_INVALID"));
        }
        self.relations.insert(contract.relation.id, contract);
        Ok(())
    }

    pub fn decode_copy_data(&mut self, frame: &[u8]) -> Result<CopyBothEvent> {
        if let Some(failure) = &self.blocked {
            return Err(failure.clone());
        }
        match self.decode_copy_data_inner(frame) {
            Ok(event) => Ok(event),
            Err(failure) => self.block(failure),
        }
    }

    fn decode_copy_data_inner(&mut self, frame: &[u8]) -> Result<CopyBothEvent> {
        if frame.len() > self.limits.max_copy_data_bytes {
            return self.block(fail(FailureClass::Wire, "COPY_DATA_LIMIT_EXCEEDED"));
        }
        let mut c = Cursor::new(frame);
        match c.u8()? {
            b'k' => {
                let wal_end = c.u64()?;
                let server_time = c.i64()?;
                let reply_requested = match c.u8()? {
                    0 => false,
                    1 => true,
                    _ => return Err(fail(FailureClass::Wire, "KEEPALIVE_REPLY_FLAG_INVALID")),
                };
                let event = CopyBothEvent::Keepalive {
                    wal_end,
                    server_time,
                    reply_requested,
                };
                c.finish()?;
                Ok(event)
            }
            b'w' => {
                let wal_start = c.u64()?;
                let wal_end = c.u64()?;
                let server_time = c.i64()?;
                let payload = c.rest();
                if payload.is_empty() || payload.len() > self.limits.max_pgoutput_message_bytes {
                    return self.block(fail(FailureClass::Wire, "PGOUTPUT_MESSAGE_LIMIT_EXCEEDED"));
                }
                let event = match self.decode_pgoutput(payload) {
                    Ok(event) => event,
                    Err(failure) => return self.block(failure),
                };
                Ok(CopyBothEvent::XLogData {
                    wal_start,
                    wal_end,
                    server_time,
                    event,
                })
            }
            _ => self.block(fail(FailureClass::Wire, "COPY_BOTH_FRAME_UNSUPPORTED")),
        }
    }

    /// Complete frontend standby-status payload (`CopyData` body), derived only from durability.
    pub fn standby_status(
        boundary: DurableSourceBoundary,
        unix_time_micros: i64,
        reply_requested: bool,
    ) -> Result<Vec<u8>> {
        Self::encode_standby_status(
            boundary.transaction_end_lsn().get(),
            unix_time_micros,
            reply_requested,
        )
    }

    fn encode_standby_status(
        durable_lsn: u64,
        unix_time_micros: i64,
        reply_requested: bool,
    ) -> Result<Vec<u8>> {
        let pg_time = unix_time_micros
            .checked_sub(PG_EPOCH_UNIX_MICROS)
            .ok_or_else(|| fail(FailureClass::Wire, "STATUS_TIMESTAMP_OUT_OF_RANGE"))?;
        let mut out = Vec::with_capacity(34);
        out.push(b'r');
        for _ in 0..3 {
            out.extend_from_slice(&durable_lsn.to_be_bytes());
        }
        out.extend_from_slice(&pg_time.to_be_bytes());
        out.push(u8::from(reply_requested));
        Ok(out)
    }

    fn decode_pgoutput(&mut self, bytes: &[u8]) -> Result<PgoutputEvent> {
        let mut c = Cursor::new(bytes);
        let tag = c.u8()?;
        let decoded = match tag {
            b'B' => {
                if self.transaction.is_some() {
                    return self.block(fail(FailureClass::ProtocolState, "NESTED_BEGIN"));
                }
                let final_lsn = c.u64()?;
                let commit_time = c.i64()?;
                let xid = c.u32()?;
                c.finish()?;
                self.transaction = Some(Transaction {
                    xid,
                    final_lsn,
                    next_ordinal: 0,
                    failed: false,
                });
                PgoutputEvent::Begin {
                    final_lsn,
                    commit_time,
                    xid,
                }
            }
            b'R' => {
                let relation = decode_relation(&mut c, self.limits.max_columns)?;
                c.finish()?;
                if !self.relations.contains_key(&relation.id)
                    && !self.pending_relations.contains_key(&relation.id)
                    && self
                        .relations
                        .len()
                        .saturating_add(self.pending_relations.len())
                        >= self.limits.max_relations
                {
                    return self.block(fail(FailureClass::Wire, "RELATION_CACHE_LIMIT_EXCEEDED"));
                }
                if self
                    .relations
                    .get(&relation.id)
                    .is_some_and(|old| old.relation == relation)
                {
                    return Ok(PgoutputEvent::RelationMetadata(relation));
                }
                self.relations.remove(&relation.id);
                self.pending_relations.insert(relation.id, relation.clone());
                PgoutputEvent::RelationNeedsValidation(relation)
            }
            b'O' => {
                self.require_transaction("ORIGIN_OUTSIDE_TRANSACTION")?;
                let origin_lsn = c.u64()?;
                let name = c.cstr()?;
                c.finish()?;
                PgoutputEvent::Origin { origin_lsn, name }
            }
            b'I' => self.decode_insert(&mut c)?,
            b'U' => self.decode_update(&mut c)?,
            b'D' => self.decode_delete(&mut c)?,
            b'C' => {
                if !self.pending_relations.is_empty() {
                    return self
                        .block(fail(FailureClass::Contract, "RELATION_VALIDATION_REQUIRED"));
                }
                let tx = self
                    .transaction
                    .take()
                    .ok_or_else(|| fail(FailureClass::ProtocolState, "COMMIT_WITHOUT_BEGIN"))?;
                let flags = c.u8()?;
                let commit_lsn = c.u64()?;
                let end_lsn = c.u64()?;
                let commit_time = c.i64()?;
                c.finish()?;
                if flags != 0 || end_lsn < commit_lsn || commit_lsn != tx.final_lsn {
                    return self.block(fail(FailureClass::Wire, "COMMIT_INVALID"));
                }
                PgoutputEvent::Commit {
                    flags,
                    commit_lsn,
                    end_lsn,
                    commit_time,
                    row_count: tx.next_ordinal,
                }
            }
            b'T' => return self.block(fail(FailureClass::Unsupported, "TRUNCATE_REQUIRES_RESEED")),
            b'S' | b'E' | b'c' | b'A' => {
                return self.block(fail(
                    FailureClass::Unsupported,
                    "STREAMED_TRANSACTION_UNSUPPORTED",
                ));
            }
            b'b' | b'P' | b'K' | b'r' => {
                return self.block(fail(
                    FailureClass::Unsupported,
                    "TWO_PHASE_TRANSACTION_UNSUPPORTED",
                ));
            }
            b'M' => {
                return self.block(fail(
                    FailureClass::Unsupported,
                    "LOGICAL_MESSAGE_UNSUPPORTED",
                ));
            }
            b'Y' => return self.block(fail(FailureClass::Unsupported, "TYPE_MESSAGE_UNSUPPORTED")),
            _ => {
                return self.block(fail(
                    FailureClass::Unsupported,
                    "PGOUTPUT_MESSAGE_UNSUPPORTED",
                ));
            }
        };
        Ok(decoded)
    }

    fn decode_insert(&mut self, c: &mut Cursor<'_>) -> Result<PgoutputEvent> {
        let relation_id = c.u32()?;
        let contract = self.contract(relation_id)?.clone();
        if contract.control.is_some() {
            return self.block(fail(FailureClass::Contract, "CONTROL_INSERT_FORBIDDEN"));
        }
        if c.u8()? != b'N' {
            return self.block(fail(FailureClass::Wire, "INSERT_TUPLE_MARKER_INVALID"));
        }
        let new = decode_tuple(c, &self.limits)?;
        c.finish()?;
        validate_width(&contract, &new)?;
        validate_full_key(&contract, &new)?;
        self.row(relation_id, RowKind::Insert, None, None, Some(new))
    }

    fn decode_update(&mut self, c: &mut Cursor<'_>) -> Result<PgoutputEvent> {
        let relation_id = c.u32()?;
        let contract = self.contract(relation_id)?.clone();
        let marker = c.u8()?;
        let (old, old_kind) = match marker {
            b'K' | b'O' => {
                let kind = if marker == b'K' {
                    OldTupleKind::Key
                } else {
                    OldTupleKind::Full
                };
                let tuple = decode_tuple(c, &self.limits)?;
                if c.u8()? != b'N' {
                    return self.block(fail(FailureClass::Wire, "UPDATE_NEW_TUPLE_MISSING"));
                }
                (Some(tuple), Some(kind))
            }
            b'N' => (None, None),
            _ => return self.block(fail(FailureClass::Wire, "UPDATE_TUPLE_MARKER_INVALID")),
        };
        let new = decode_tuple(c, &self.limits)?;
        c.finish()?;
        validate_width(&contract, &new)?;
        validate_full_key(&contract, &new)?;
        let new_key = full_key(&contract, &new);
        let old_key = match (old.as_deref(), old_kind) {
            (Some(values), Some(OldTupleKind::Key)) => {
                validate_compact_key(&contract, values)?;
                Some(compact_key(values))
            }
            (Some(values), Some(OldTupleKind::Full)) => {
                validate_width(&contract, values)?;
                validate_full_key(&contract, values)?;
                Some(full_key(&contract, values))
            }
            (None, None) => None,
            _ => unreachable!("old tuple and marker are constructed together"),
        };
        let key_changed = old_key.as_ref().is_some_and(|key| key != &new_key);
        if key_changed && new.contains(&TupleValue::UnchangedToast) {
            return self.block(fail(FailureClass::Contract, "KEY_CHANGE_UNCHANGED_TOAST"));
        }
        if let Some(control) = &contract.control {
            if old_key.as_ref().unwrap_or(&new_key) != &control.immutable_key
                || new_key != control.immutable_key
            {
                return self.block(fail(FailureClass::Contract, "CONTROL_KEY_CHANGED"));
            }
            let unobservable_fixed_column = (0..contract.relation.columns.len()).any(|i| {
                !contract.key_columns.contains(&i) && !control.mutable_columns.contains(&i)
            });
            if old_kind != Some(OldTupleKind::Full) && unobservable_fixed_column {
                return self.block(fail(FailureClass::Contract, "CONTROL_OLD_ROW_REQUIRED"));
            }
            if let (Some(old_values), Some(OldTupleKind::Full)) = (old.as_deref(), old_kind) {
                for i in 0..new.len() {
                    if !contract.key_columns.contains(&i)
                        && !control.mutable_columns.contains(&i)
                        && old_values[i] != new[i]
                    {
                        return self.block(fail(
                            FailureClass::Contract,
                            "CONTROL_UNEXPECTED_COLUMN_MUTATION",
                        ));
                    }
                }
            }
        }
        self.row(relation_id, RowKind::Update, old_kind, old, Some(new))
    }

    fn decode_delete(&mut self, c: &mut Cursor<'_>) -> Result<PgoutputEvent> {
        let relation_id = c.u32()?;
        let contract = self.contract(relation_id)?.clone();
        if contract.control.is_some() {
            return self.block(fail(FailureClass::Contract, "CONTROL_DELETE_FORBIDDEN"));
        }
        let old_kind = match c.u8()? {
            b'K' => OldTupleKind::Key,
            b'O' => OldTupleKind::Full,
            _ => return self.block(fail(FailureClass::Wire, "DELETE_OLD_TUPLE_MISSING")),
        };
        let old = decode_tuple(c, &self.limits)?;
        c.finish()?;
        match old_kind {
            OldTupleKind::Key => validate_compact_key(&contract, &old)?,
            OldTupleKind::Full => {
                validate_width(&contract, &old)?;
                validate_full_key(&contract, &old)?;
            }
        }
        self.row(
            relation_id,
            RowKind::Delete,
            Some(old_kind),
            Some(old),
            None,
        )
    }

    fn row(
        &mut self,
        relation_id: u32,
        kind: RowKind,
        old_kind: Option<OldTupleKind>,
        old: Option<Vec<TupleValue>>,
        new: Option<Vec<TupleValue>>,
    ) -> Result<PgoutputEvent> {
        let tx = self
            .transaction
            .as_mut()
            .ok_or_else(|| fail(FailureClass::ProtocolState, "ROW_OUTSIDE_TRANSACTION"))?;
        let ordinal = tx.next_ordinal;
        tx.next_ordinal = tx
            .next_ordinal
            .checked_add(1)
            .ok_or_else(|| fail(FailureClass::Wire, "ROW_ORDINAL_OVERFLOW"))?;
        Ok(PgoutputEvent::Row(RowChange {
            xid: tx.xid,
            ordinal,
            relation_id,
            kind,
            old_kind,
            old,
            new,
        }))
    }

    fn contract(&self, id: u32) -> Result<&RelationContract> {
        if self.pending_relations.contains_key(&id) {
            return Err(fail(FailureClass::Contract, "RELATION_VALIDATION_REQUIRED"));
        }
        self.relations
            .get(&id)
            .ok_or_else(|| fail(FailureClass::Contract, "RELATION_UNKNOWN"))
    }
    fn require_transaction(&self, code: &'static str) -> Result<()> {
        if self.transaction.is_some() {
            Ok(())
        } else {
            Err(fail(FailureClass::ProtocolState, code))
        }
    }
    fn block<T>(&mut self, failure: DecodeFailure) -> Result<T> {
        if let Some(tx) = &mut self.transaction {
            tx.failed = true;
        }
        self.blocked = Some(failure.clone());
        Err(failure)
    }
}

fn fail(class: FailureClass, fingerprint: &'static str) -> DecodeFailure {
    DecodeFailure::new(class, fingerprint)
}
fn validate_width(contract: &RelationContract, values: &[TupleValue]) -> Result<()> {
    if values.len() == contract.relation.columns.len() {
        Ok(())
    } else {
        Err(fail(FailureClass::Wire, "TUPLE_COLUMN_COUNT_MISMATCH"))
    }
}
fn validate_full_key(contract: &RelationContract, values: &[TupleValue]) -> Result<()> {
    if contract
        .key_columns
        .iter()
        .any(|&i| !matches!(values.get(i), Some(TupleValue::Text(_))))
    {
        Err(fail(FailureClass::Contract, "CANONICAL_KEY_INCOMPLETE"))
    } else {
        Ok(())
    }
}
fn validate_compact_key(contract: &RelationContract, values: &[TupleValue]) -> Result<()> {
    if values.len() != contract.key_columns.len()
        || values
            .iter()
            .any(|value| !matches!(value, TupleValue::Text(_)))
    {
        Err(fail(FailureClass::Contract, "CANONICAL_KEY_INCOMPLETE"))
    } else {
        Ok(())
    }
}
fn full_key(contract: &RelationContract, values: &[TupleValue]) -> Vec<Vec<u8>> {
    contract
        .key_columns
        .iter()
        .filter_map(|&i| match values.get(i) {
            Some(TupleValue::Text(v)) => Some(v.clone()),
            _ => None,
        })
        .collect()
}
fn compact_key(values: &[TupleValue]) -> Vec<Vec<u8>> {
    values
        .iter()
        .filter_map(|value| match value {
            TupleValue::Text(value) => Some(value.clone()),
            _ => None,
        })
        .collect()
}

fn decode_relation(c: &mut Cursor<'_>, max_columns: usize) -> Result<Relation> {
    let id = c.u32()?;
    let namespace = c.cstr()?;
    let name = c.cstr()?;
    let replica_identity = c.u8()?;
    let count = c.u16()? as usize;
    if count == 0 || count > max_columns {
        return Err(fail(FailureClass::Wire, "RELATION_COLUMN_LIMIT_EXCEEDED"));
    }
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        let flags = c.u8()?;
        if flags & !1 != 0 {
            return Err(fail(
                FailureClass::Unsupported,
                "RELATION_COLUMN_FLAGS_UNSUPPORTED",
            ));
        }
        columns.push(Column {
            key: flags == 1,
            name: c.cstr()?,
            type_oid: c.u32()?,
            type_modifier: c.i32()?,
        });
    }
    Ok(Relation {
        id,
        namespace,
        name,
        replica_identity,
        columns,
    })
}

fn decode_tuple(c: &mut Cursor<'_>, limits: &WireLimits) -> Result<Vec<TupleValue>> {
    let count = c.u16()? as usize;
    if count > limits.max_columns {
        return Err(fail(FailureClass::Wire, "TUPLE_COLUMN_LIMIT_EXCEEDED"));
    }
    let mut total = 0usize;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(match c.u8()? {
            b'n' => TupleValue::Null,
            b'u' => TupleValue::UnchangedToast,
            b't' => {
                let len = c.u32()? as usize;
                total = total
                    .checked_add(len)
                    .ok_or_else(|| fail(FailureClass::Wire, "TUPLE_LENGTH_OVERFLOW"))?;
                if total > limits.max_tuple_bytes {
                    return Err(fail(FailureClass::Wire, "TUPLE_BYTE_LIMIT_EXCEEDED"));
                }
                TupleValue::Text(c.bytes(len)?.to_vec())
            }
            b'b' => return Err(fail(FailureClass::Unsupported, "BINARY_TUPLE_UNSUPPORTED")),
            _ => return Err(fail(FailureClass::Wire, "TUPLE_VALUE_KIND_INVALID")),
        });
    }
    Ok(values)
}

struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }
    fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(n)
            .ok_or_else(|| fail(FailureClass::Wire, "FRAME_LENGTH_OVERFLOW"))?;
        let out = self
            .bytes
            .get(self.at..end)
            .ok_or_else(|| fail(FailureClass::Wire, "FRAME_TRUNCATED"))?;
        self.at = end;
        Ok(out)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.bytes(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.bytes(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.bytes(4)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.bytes(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.bytes(8)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.bytes(8)?.try_into().unwrap()))
    }
    fn cstr(&mut self) -> Result<String> {
        let tail = self
            .bytes
            .get(self.at..)
            .ok_or_else(|| fail(FailureClass::Wire, "FRAME_TRUNCATED"))?;
        let n = tail
            .iter()
            .position(|b| *b == 0)
            .ok_or_else(|| fail(FailureClass::Wire, "CSTRING_UNTERMINATED"))?;
        let raw = self.bytes(n)?;
        self.bytes(1)?;
        let text = std::str::from_utf8(raw)
            .map_err(|_| fail(FailureClass::Wire, "CSTRING_UTF8_INVALID"))?;
        if text.is_empty() {
            return Err(fail(FailureClass::Wire, "CSTRING_EMPTY"));
        }
        Ok(text.to_owned())
    }
    fn rest(&mut self) -> &'a [u8] {
        let out = &self.bytes[self.at..];
        self.at = self.bytes.len();
        out
    }
    fn finish(&self) -> Result<()> {
        if self.at == self.bytes.len() {
            Ok(())
        } else {
            Err(fail(FailureClass::Wire, "FRAME_TRAILING_BYTES"))
        }
    }
}

#[cfg(test)]
const DECODER_CASE_IDS: [&str; 10] = [
    "SCN-M1-DECODER-COPYBOTH",
    "SCN-M1-DECODER-STATUS",
    "SCN-M1-DECODER-TRANSACTION",
    "SCN-M1-DECODER-RELATION",
    "SCN-M1-DECODER-KEYS",
    "SCN-M1-DECODER-CONTROL",
    "SCN-M1-DECODER-UNSUPPORTED",
    "SCN-M1-DECODER-BOUNDS",
    "SCN-M1-DECODER-RECONNECT",
    "SCN-M1-DECODER-PG-MATRIX",
];

#[cfg(test)]
pub mod tests {
    use super::*;
    fn xlog(msg: Vec<u8>) -> Vec<u8> {
        let mut f = vec![b'w'];
        f.extend(10u64.to_be_bytes());
        f.extend(20u64.to_be_bytes());
        f.extend(30i64.to_be_bytes());
        f.extend(msg);
        f
    }
    fn begin(xid: u32) -> Vec<u8> {
        let mut v = vec![b'B'];
        v.extend(10u64.to_be_bytes());
        v.extend(1i64.to_be_bytes());
        v.extend(xid.to_be_bytes());
        v
    }
    fn commit() -> Vec<u8> {
        let mut v = vec![b'C', 0];
        v.extend(10u64.to_be_bytes());
        v.extend(11u64.to_be_bytes());
        v.extend(2i64.to_be_bytes());
        v
    }
    fn relation(id: u32, control: bool) -> Relation {
        Relation {
            id,
            namespace: if control {
                "boring_cdc_control"
            } else {
                "public"
            }
            .into(),
            name: if control { "heartbeat" } else { "items" }.into(),
            replica_identity: b'd',
            columns: vec![
                Column {
                    key: true,
                    name: "id".into(),
                    type_oid: 25,
                    type_modifier: -1,
                },
                Column {
                    key: false,
                    name: "value".into(),
                    type_oid: 25,
                    type_modifier: -1,
                },
                Column {
                    key: false,
                    name: "guard".into(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        }
    }
    fn rel_wire(r: &Relation) -> Vec<u8> {
        let mut v = vec![b'R'];
        v.extend(r.id.to_be_bytes());
        v.extend(r.namespace.as_bytes());
        v.push(0);
        v.extend(r.name.as_bytes());
        v.push(0);
        v.push(r.replica_identity);
        v.extend((r.columns.len() as u16).to_be_bytes());
        for c in &r.columns {
            v.push(c.key as u8);
            v.extend(c.name.as_bytes());
            v.push(0);
            v.extend(c.type_oid.to_be_bytes());
            v.extend(c.type_modifier.to_be_bytes());
        }
        v
    }
    fn tuple(vals: &[TupleValue]) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend((vals.len() as u16).to_be_bytes());
        for x in vals {
            match x {
                TupleValue::Null => v.push(b'n'),
                TupleValue::UnchangedToast => v.push(b'u'),
                TupleValue::Text(x) => {
                    v.push(b't');
                    v.extend((x.len() as u32).to_be_bytes());
                    v.extend(x)
                }
            }
        }
        v
    }
    fn row(tag: u8, id: u32, markers: &[u8], tuples: &[Vec<TupleValue>]) -> Vec<u8> {
        let mut v = vec![tag];
        v.extend(id.to_be_bytes());
        for (m, t) in markers.iter().zip(tuples) {
            v.push(*m);
            v.extend(tuple(t));
        }
        v
    }
    fn admitted(control: bool) -> Decoder {
        let mut d = Decoder::new(WireLimits::default());
        let r = relation(7, control);
        assert!(matches!(
            d.decode_copy_data(&xlog(rel_wire(&r))).unwrap(),
            CopyBothEvent::XLogData {
                event: PgoutputEvent::RelationNeedsValidation(_),
                ..
            }
        ));
        d.admit_relation(RelationContract {
            relation: r,
            key_columns: vec![0],
            control: control.then(|| ControlContract {
                immutable_key: vec![b"fixed".to_vec()],
                mutable_columns: vec![1],
            }),
        })
        .unwrap();
        d
    }
    fn text(s: &str) -> TupleValue {
        TupleValue::Text(s.as_bytes().to_vec())
    }
    fn from_hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let digit = |byte: u8| match byte {
                    b'0'..=b'9' => byte - b'0',
                    b'a'..=b'f' => byte - b'a' + 10,
                    _ => panic!("invalid fixture hex"),
                };
                digit(pair[0]) * 16 + digit(pair[1])
            })
            .collect()
    }

    // SCENARIO: SCN-M1-DECODER-COPYBOTH
    // SCENARIO: SCN-M1-DECODER-STATUS
    #[test]
    fn copy_both_keepalive_and_complete_status_packet() {
        let mut d = Decoder::new(WireLimits::default());
        let mut k = vec![b'k'];
        k.extend(55u64.to_be_bytes());
        k.extend(77i64.to_be_bytes());
        k.push(1);
        assert_eq!(
            d.decode_copy_data(&k).unwrap(),
            CopyBothEvent::Keepalive {
                wal_end: 55,
                server_time: 77,
                reply_requested: true
            }
        );
        let p = Decoder::encode_standby_status(42, PG_EPOCH_UNIX_MICROS + 9, true).unwrap();
        assert_eq!(p.len(), 34);
        assert_eq!(p[0], b'r');
        assert_eq!(&p[1..9], &42u64.to_be_bytes());
        assert_eq!(&p[9..17], &42u64.to_be_bytes());
        assert_eq!(&p[17..25], &42u64.to_be_bytes());
        assert_eq!(&p[25..33], &9i64.to_be_bytes());
        assert_eq!(p[33], 1);

        let mut bad = vec![b'k'];
        bad.extend(55u64.to_be_bytes());
        bad.extend(77i64.to_be_bytes());
        bad.push(2);
        assert_eq!(
            Decoder::new(WireLimits::default())
                .decode_copy_data(&bad)
                .unwrap_err()
                .fingerprint,
            "KEEPALIVE_REPLY_FLAG_INVALID"
        );
    }
    // SCENARIO: SCN-M1-DECODER-PG-MATRIX
    #[test]
    fn postgres_major_golden_wire_corpus_decodes() {
        let corpus: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/m1_decoder/pgoutput-v1.json"
        ))
        .unwrap();
        for vector in corpus["vectors"].as_array().unwrap() {
            assert!(
                SUPPORTED_POSTGRES_MAJORS
                    .contains(&(vector["postgres_major"].as_u64().unwrap() as u16))
            );
            let frames = vector["frames_hex"].as_array().unwrap();
            let mut d = Decoder::new(WireLimits::default());
            let relation_event = d
                .decode_copy_data(&from_hex(frames[0].as_str().unwrap()))
                .unwrap();
            let CopyBothEvent::XLogData {
                event: PgoutputEvent::RelationNeedsValidation(relation),
                ..
            } = relation_event
            else {
                panic!("golden relation")
            };
            d.admit_relation(RelationContract {
                relation,
                key_columns: vec![0],
                control: None,
            })
            .unwrap();
            let mut ordinals = Vec::new();
            let mut saw_begin = false;
            let mut saw_origin = false;
            let mut saw_commit = false;
            let mut saw_keepalive = false;
            for frame in &frames[1..] {
                match d
                    .decode_copy_data(&from_hex(frame.as_str().unwrap()))
                    .unwrap()
                {
                    CopyBothEvent::XLogData {
                        event: PgoutputEvent::Begin { xid, .. },
                        ..
                    } => {
                        assert_eq!(xid, vector["expected"]["xid"].as_u64().unwrap() as u32);
                        saw_begin = true;
                    }
                    CopyBothEvent::XLogData {
                        event: PgoutputEvent::Origin { name, .. },
                        ..
                    } => {
                        assert_eq!(name, vector["expected"]["origin"].as_str().unwrap());
                        saw_origin = true;
                    }
                    CopyBothEvent::XLogData {
                        event: PgoutputEvent::Row(row),
                        ..
                    } => ordinals.push(row.ordinal),
                    CopyBothEvent::XLogData {
                        event:
                            PgoutputEvent::Commit {
                                commit_lsn,
                                end_lsn,
                                ..
                            },
                        ..
                    } => {
                        assert_eq!(
                            commit_lsn,
                            vector["expected"]["commit_lsn"].as_u64().unwrap()
                        );
                        assert_eq!(end_lsn, vector["expected"]["end_lsn"].as_u64().unwrap());
                        saw_commit = true;
                    }
                    CopyBothEvent::Keepalive {
                        reply_requested, ..
                    } => {
                        assert_eq!(
                            reply_requested,
                            vector["expected"]["keepalive_reply_requested"]
                                .as_bool()
                                .unwrap()
                        );
                        saw_keepalive = true;
                    }
                    _ => {}
                }
            }
            assert_eq!(ordinals, vec![0, 1, 2]);
            assert!(saw_begin && saw_origin && saw_commit && saw_keepalive);
        }
    }

    // SCENARIO: SCN-M1-DECODER-TRANSACTION
    #[test]
    fn golden_transaction_preserves_row_only_ordinals_and_origin() {
        let mut d = admitted(false);
        assert!(matches!(
            d.decode_copy_data(&xlog(begin(99))).unwrap(),
            CopyBothEvent::XLogData {
                event: PgoutputEvent::Begin { xid: 99, .. },
                ..
            }
        ));
        let r = relation(7, false);
        assert!(matches!(
            d.decode_copy_data(&xlog(rel_wire(&r))).unwrap(),
            CopyBothEvent::XLogData {
                event: PgoutputEvent::RelationMetadata(_),
                ..
            }
        ));
        let mut o = vec![b'O'];
        o.extend(8u64.to_be_bytes());
        o.extend(b"upstream\0");
        assert!(matches!(
            d.decode_copy_data(&xlog(o)).unwrap(),
            CopyBothEvent::XLogData {
                event: PgoutputEvent::Origin { .. },
                ..
            }
        ));
        let i = row(
            b'I',
            7,
            b"N",
            &[vec![text("a"), text("v"), TupleValue::Null]],
        );
        let u = row(
            b'U',
            7,
            b"KN",
            &[
                vec![text("a")],
                vec![text("a"), text("v2"), TupleValue::Null],
            ],
        );
        let del = row(b'D', 7, b"K", &[vec![text("a")]]);
        for (wire, want) in [(i, 0), (u, 1), (del, 2)] {
            assert!(
                matches!(d.decode_copy_data(&xlog(wire)).unwrap(),CopyBothEvent::XLogData{event:PgoutputEvent::Row(RowChange{ordinal,..}),..} if ordinal==want)
            );
        }
        assert!(matches!(
            d.decode_copy_data(&xlog(commit())).unwrap(),
            CopyBothEvent::XLogData {
                event: PgoutputEvent::Commit {
                    row_count: 3,
                    end_lsn: 11,
                    ..
                },
                ..
            }
        ));

        crate::m1_raw_demo::emit_asserted_case(
            "SCN-M1-RAW-FIXED-SEED",
            "decoded",
            "unchanged_until_durable",
            "raw_event_normalized",
        );
    }
    // SCENARIO: SCN-M1-DECODER-RELATION
    #[test]
    fn changed_relation_requires_synchronous_validation() {
        let mut d = admitted(false);
        d.decode_copy_data(&xlog(begin(1))).unwrap();
        let mut changed = relation(7, false);
        changed.columns[1].type_oid = 20;
        d.decode_copy_data(&xlog(rel_wire(&changed))).unwrap();
        let err = d.decode_copy_data(&xlog(commit())).unwrap_err();
        assert_eq!(err.fingerprint, "RELATION_VALIDATION_REQUIRED");

        let mut d = admitted(false);
        d.decode_copy_data(&xlog(begin(1))).unwrap();
        d.decode_copy_data(&xlog(rel_wire(&changed))).unwrap();
        let err = d
            .decode_copy_data(&xlog(row(
                b'I',
                7,
                b"N",
                &[vec![text("a"), text("v"), TupleValue::Null]],
            )))
            .unwrap_err();
        assert_eq!(err.fingerprint, "RELATION_VALIDATION_REQUIRED");
    }
    // SCENARIO: SCN-M1-DECODER-KEYS
    #[test]
    fn key_and_toast_failures_block_feedback() {
        for vals in [
            vec![TupleValue::Null, text("v"), TupleValue::Null],
            vec![TupleValue::UnchangedToast, text("v"), TupleValue::Null],
        ] {
            let mut d = admitted(false);
            d.decode_copy_data(&xlog(begin(1))).unwrap();
            let e = d
                .decode_copy_data(&xlog(row(b'D', 7, b"K", &[vals])))
                .unwrap_err();
            assert_eq!(e.fingerprint, "CANONICAL_KEY_INCOMPLETE");
            assert!(d.is_feedback_blocked());
        }
        let mut d = Decoder::new(WireLimits::default());
        let mut composite = relation(9, false);
        composite.columns[2].key = true;
        d.decode_copy_data(&xlog(rel_wire(&composite))).unwrap();
        assert_eq!(
            d.admit_relation(RelationContract {
                relation: composite.clone(),
                key_columns: vec![0],
                control: None
            })
            .unwrap_err()
            .fingerprint,
            "RELATION_CONTRACT_MISMATCH"
        );
        let mut d = Decoder::new(WireLimits::default());
        d.decode_copy_data(&xlog(rel_wire(&composite))).unwrap();
        d.admit_relation(RelationContract {
            relation: composite,
            key_columns: vec![0, 2],
            control: None,
        })
        .unwrap();
        d.decode_copy_data(&xlog(begin(1))).unwrap();
        assert!(
            d.decode_copy_data(&xlog(row(b'D', 9, b"K", &[vec![text("a"), text("g")]])))
                .is_ok()
        );

        let mut d = admitted(false);
        d.decode_copy_data(&xlog(begin(1))).unwrap();
        let e = d
            .decode_copy_data(&xlog(row(
                b'U',
                7,
                b"KN",
                &[
                    vec![text("old")],
                    vec![text("new"), TupleValue::UnchangedToast, text("g")],
                ],
            )))
            .unwrap_err();
        assert_eq!(e.fingerprint, "KEY_CHANGE_UNCHANGED_TOAST");
    }
    // SCENARIO: SCN-M1-DECODER-CONTROL
    #[test]
    fn control_relation_allows_only_fixed_key_update_and_mutable_columns() {
        let valid = row(
            b'U',
            7,
            b"ON",
            &[
                vec![text("fixed"), text("old"), text("same")],
                vec![text("fixed"), text("new"), text("same")],
            ],
        );
        let mut d = admitted(true);
        d.decode_copy_data(&xlog(begin(1))).unwrap();
        assert!(d.decode_copy_data(&xlog(valid)).is_ok());

        let mut d = admitted(true);
        d.relations
            .get_mut(&7)
            .unwrap()
            .control
            .as_mut()
            .unwrap()
            .mutable_columns = vec![1, 2];
        d.decode_copy_data(&xlog(begin(1))).unwrap();
        let without_old = row(
            b'U',
            7,
            b"N",
            &[vec![text("fixed"), text("new"), text("changed")]],
        );
        assert!(d.decode_copy_data(&xlog(without_old)).is_ok());

        for (wire, code) in [
            (
                row(
                    b'I',
                    7,
                    b"N",
                    &[vec![text("fixed"), text("new"), text("same")]],
                ),
                "CONTROL_INSERT_FORBIDDEN",
            ),
            (
                row(b'D', 7, b"K", &[vec![text("fixed")]]),
                "CONTROL_DELETE_FORBIDDEN",
            ),
            (
                row(
                    b'U',
                    7,
                    b"KN",
                    &[
                        vec![text("fixed")],
                        vec![text("other"), text("new"), text("same")],
                    ],
                ),
                "CONTROL_KEY_CHANGED",
            ),
            (
                row(
                    b'U',
                    7,
                    b"ON",
                    &[
                        vec![text("fixed"), text("old"), text("same")],
                        vec![text("fixed"), text("new"), text("changed")],
                    ],
                ),
                "CONTROL_UNEXPECTED_COLUMN_MUTATION",
            ),
        ] {
            let mut d = admitted(true);
            d.decode_copy_data(&xlog(begin(1))).unwrap();
            let e = d.decode_copy_data(&xlog(wire)).unwrap_err();
            assert_eq!(e.fingerprint, code);
            assert!(d.is_feedback_blocked());
        }
    }
    // SCENARIO: SCN-M1-DECODER-UNSUPPORTED
    #[test]
    fn unsupported_messages_and_binary_truncate_fail_closed() {
        for (tag, code) in [
            (b'T', "TRUNCATE_REQUIRES_RESEED"),
            (b'S', "STREAMED_TRANSACTION_UNSUPPORTED"),
            (b'E', "STREAMED_TRANSACTION_UNSUPPORTED"),
            (b'c', "STREAMED_TRANSACTION_UNSUPPORTED"),
            (b'A', "STREAMED_TRANSACTION_UNSUPPORTED"),
            (b'b', "TWO_PHASE_TRANSACTION_UNSUPPORTED"),
            (b'P', "TWO_PHASE_TRANSACTION_UNSUPPORTED"),
            (b'K', "TWO_PHASE_TRANSACTION_UNSUPPORTED"),
            (b'r', "TWO_PHASE_TRANSACTION_UNSUPPORTED"),
            (b'M', "LOGICAL_MESSAGE_UNSUPPORTED"),
            (b'Y', "TYPE_MESSAGE_UNSUPPORTED"),
            (b'?', "PGOUTPUT_MESSAGE_UNSUPPORTED"),
        ] {
            let mut d = Decoder::new(WireLimits::default());
            let e = d.decode_copy_data(&xlog(vec![tag])).unwrap_err();
            assert_eq!(e.fingerprint, code);
            assert!(d.is_feedback_blocked());
        }
        let mut d = admitted(false);
        d.decode_copy_data(&xlog(begin(1))).unwrap();
        let mut i = vec![b'I'];
        i.extend(7u32.to_be_bytes());
        i.push(b'N');
        i.extend(3u16.to_be_bytes());
        i.push(b't');
        i.extend(1u32.to_be_bytes());
        i.push(b'a');
        i.push(b'b');
        let e = d.decode_copy_data(&xlog(i)).unwrap_err();
        assert_eq!(e.fingerprint, "BINARY_TUPLE_UNSUPPORTED");

        crate::m1_raw_demo::emit_asserted_case(
            "SCN-M1-RAW-UNSUPPORTED-PROTOCOL",
            "blocked",
            "unchanged",
            "unsupported_protocol",
        );
    }
    // SCENARIO: SCN-M1-DECODER-BOUNDS
    #[test]
    fn malformed_and_bounded_frames_never_panic_or_ack() {
        for n in 0..80 {
            let mut d = Decoder::new(WireLimits {
                max_copy_data_bytes: 32,
                max_pgoutput_message_bytes: 7,
                max_columns: 2,
                max_tuple_bytes: 2,
                max_relations: 2,
            });
            let bytes = vec![0xff; n];
            let _ = d.decode_copy_data(&bytes);
        }
        let mut d = Decoder::new(WireLimits {
            max_copy_data_bytes: 1,
            ..WireLimits::default()
        });
        assert_eq!(
            d.decode_copy_data(&[b'k', 0]).unwrap_err().fingerprint,
            "COPY_DATA_LIMIT_EXCEEDED"
        );
        assert!(d.is_feedback_blocked());

        let mut d = Decoder::new(WireLimits {
            max_pgoutput_message_bytes: 1,
            ..WireLimits::default()
        });
        assert_eq!(
            d.decode_copy_data(&xlog(begin(1))).unwrap_err().fingerprint,
            "PGOUTPUT_MESSAGE_LIMIT_EXCEEDED"
        );
        let mut d = Decoder::new(WireLimits {
            max_columns: 2,
            ..WireLimits::default()
        });
        assert_eq!(
            d.decode_copy_data(&xlog(rel_wire(&relation(7, false))))
                .unwrap_err()
                .fingerprint,
            "RELATION_COLUMN_LIMIT_EXCEEDED"
        );
        let mut d = admitted(false);
        d.limits.max_columns = 2;
        d.decode_copy_data(&xlog(begin(1))).unwrap();
        assert_eq!(
            d.decode_copy_data(&xlog(row(
                b'I',
                7,
                b"N",
                &[vec![text("a"), text("v"), text("g")]]
            )))
            .unwrap_err()
            .fingerprint,
            "TUPLE_COLUMN_LIMIT_EXCEEDED"
        );
        let mut d = admitted(false);
        d.limits.max_tuple_bytes = 1;
        d.decode_copy_data(&xlog(begin(1))).unwrap();
        assert_eq!(
            d.decode_copy_data(&xlog(row(
                b'I',
                7,
                b"N",
                &[vec![text("aa"), text("v"), text("g")]]
            )))
            .unwrap_err()
            .fingerprint,
            "TUPLE_BYTE_LIMIT_EXCEEDED"
        );
        let mut d = Decoder::new(WireLimits {
            max_relations: 1,
            ..WireLimits::default()
        });
        d.decode_copy_data(&xlog(rel_wire(&relation(7, false))))
            .unwrap();
        let mut second = relation(8, false);
        second.name = "other".into();
        assert_eq!(
            d.decode_copy_data(&xlog(rel_wire(&second)))
                .unwrap_err()
                .fingerprint,
            "RELATION_CACHE_LIMIT_EXCEEDED"
        );
        let mut d = Decoder::new(WireLimits::default());
        assert_eq!(
            d.decode_copy_data(b"w").unwrap_err().fingerprint,
            "FRAME_TRUNCATED"
        );
    }
    // SCENARIO: SCN-M1-DECODER-RECONNECT
    #[test]
    fn reconnect_metadata_does_not_need_transaction_or_ordinal() {
        let mut d = admitted(false);
        let r = relation(7, false);
        assert!(matches!(
            d.decode_copy_data(&xlog(rel_wire(&r))).unwrap(),
            CopyBothEvent::XLogData {
                event: PgoutputEvent::RelationMetadata(_),
                ..
            }
        ));
        assert!(!d.is_feedback_blocked());

        crate::m1_raw_demo::emit_asserted_case(
            "SCN-M1-RAW-COPYBOTH-RESTART",
            "resume_safe",
            "durable_only",
            "copyboth_restart_safe",
        );
    }
    #[test]
    fn contract_inventory_exactly_matches_executable_scenarios() {
        let value: serde_json::Value =
            serde_json::from_str(include_str!("../contracts/m1/decoder-cases.json")).unwrap();
        let actual = value["cases"]
            .as_array()
            .unwrap()
            .iter()
            .map(|case| case["id"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(actual, DECODER_CASE_IDS);
    }

    #[test]
    fn provisional_protocol_matrix_is_explicit() {
        assert_eq!(SUPPORTED_POSTGRES_MAJORS, [15, 16, 17]);
        assert_eq!(PROTOCOL_ZERO_SENTINEL, 0);
    }
}
