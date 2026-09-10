//! Provider-neutral deterministic workload and correctness oracle.
//!
//! Canonical key/value/oracle contracts are provisionally pinned to the owner-card recommendations.
//! The executable component observes actual PostgreSQL trigger events independently from ledger
//! writes. An unavailable event boundary is always a non-pass.

use sha2::{Digest, Sha256};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::fmt;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

const DIGEST_DOMAIN: &[u8] = b"boring-cdc/workload-set/v1";
const STATE_DOMAIN: &[u8] = b"boring-cdc/workload-state/v1";
// M0-PROVISIONAL: boring-cdc-d-keys
const KEYS_CONTRACT_DIGEST: [u8; 32] = [
    0x30, 0xc1, 0x4e, 0x8b, 0x95, 0x3c, 0x11, 0xdf, 0xb9, 0xab, 0x4a, 0xc1, 0x0c, 0xcd, 0xe0, 0xcb,
    0xad, 0x9c, 0x7a, 0xe4, 0xd2, 0x50, 0x97, 0xd6, 0x22, 0x58, 0xe7, 0xd7, 0x1d, 0x7a, 0x51, 0x0d,
];
// M0-PROVISIONAL: boring-cdc-d-values
const VALUES_CONTRACT_DIGEST: [u8; 32] = [
    0xb0, 0x3d, 0x04, 0x46, 0x0a, 0x78, 0xc4, 0xcd, 0x0b, 0x02, 0x81, 0x79, 0x52, 0xe6, 0xbc, 0xc2,
    0x1d, 0x89, 0xc9, 0xb7, 0x12, 0xb0, 0x27, 0xb1, 0xb8, 0x66, 0xcf, 0x5e, 0x62, 0xc7, 0xac, 0xc8,
];
// M0-PROVISIONAL: boring-cdc-d-oracle
const ORACLE_CONTRACT_DIGEST: [u8; 32] = [
    0x8b, 0x74, 0x5b, 0xab, 0xbe, 0x15, 0x2a, 0xc6, 0x5c, 0xc5, 0x37, 0xc0, 0xe0, 0x1f, 0x3e, 0x18,
    0x14, 0xb7, 0x85, 0x44, 0x3d, 0x4a, 0x63, 0xaa, 0xdd, 0xc0, 0x8f, 0x56, 0xfb, 0xa8, 0x57, 0x74,
];

#[must_use]
pub const fn provisional_contract_digests() -> ContractDigests {
    ContractDigests {
        keys: KEYS_CONTRACT_DIGEST,
        values: VALUES_CONTRACT_DIGEST,
        oracle: ORACLE_CONTRACT_DIGEST,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContractDigests {
    pub keys: [u8; 32],
    pub values: [u8; 32],
    pub oracle: [u8; 32],
}

impl ContractDigests {
    pub fn validate(self) -> Result<Self, OracleFailure> {
        if self.keys == [0; 32] || self.values == [0; 32] || self.oracle == [0; 32] {
            return Err(OracleFailure::new(
                "contract",
                "WORKLOAD_CONTRACT_DIGEST_UNRESOLVED",
                "before_workload_generation",
            ));
        }
        if self != provisional_contract_digests() {
            return Err(OracleFailure::new(
                "contract",
                "WORKLOAD_CONTRACT_DIGEST_MISMATCH",
                "before_workload_generation",
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Profile {
    Smoke,
    Component,
}

impl Profile {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Smoke => "smoke-v1",
            Self::Component => "component-v1",
        }
    }

    #[must_use]
    pub const fn max_records(self) -> usize {
        match self {
            Self::Smoke => 64,
            Self::Component => 4096,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum EntityTable {
    Customers,
    Products,
    Orders,
    OrderItems,
}

impl EntityTable {
    const fn tag(self) -> u8 {
        match self {
            Self::Customers => 0,
            Self::Products => 1,
            Self::Orders => 2,
            Self::OrderItems => 3,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

impl Operation {
    const fn tag(self) -> u8 {
        match self {
            Self::Insert => 0,
            Self::Update => 1,
            Self::Delete => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordKind {
    BusinessMutation,
    Fence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerEntry {
    pub run_id: [u8; 16],
    pub mutation_seq: u64,
    pub mutation_id: [u8; 16],
    pub transaction_group_id: [u8; 16],
    pub transaction_ordinal: u32,
    pub entity_table: EntityTable,
    /// Opaque canonical-key digest produced under `ContractDigests::keys`.
    pub key: [u8; 32],
    pub operation: Operation,
    /// Opaque typed-row digest produced under `ContractDigests::values`; absent for deletes.
    pub expected_after_hash: Option<[u8; 32]>,
    pub committed_at_micros: i64,
    pub record_kind: RecordKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BusinessObservation {
    pub transaction_group_id: [u8; 16],
    pub transaction_ordinal: u32,
    pub entity_table: EntityTable,
    pub key: [u8; 32],
    pub operation: Operation,
    pub observed_after_hash: Option<[u8; 32]>,
    /// Provider-local retry identity. It is used only to reject conflicting physical retries.
    pub physical_id: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CorrelationKey {
    transaction_group_id: [u8; 16],
    transaction_ordinal: u32,
    entity_table: EntityTable,
    key: [u8; 32],
    operation: Operation,
}

impl CorrelationKey {
    fn observed(row: &BusinessObservation) -> Self {
        Self {
            transaction_group_id: row.transaction_group_id,
            transaction_ordinal: row.transaction_ordinal,
            entity_table: row.entity_table,
            key: row.key,
            operation: row.operation,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProofStatus {
    Pass,
    Fail,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProofDimension {
    pub status: ProofStatus,
    pub expected_count: usize,
    pub observed_count: usize,
    pub missing: Vec<String>,
    pub unexpected: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SequenceDiagnostics {
    pub gaps: Vec<(u64, u64)>,
    pub duplicates: Vec<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OracleReport {
    pub ledger_delivery: ProofDimension,
    pub business_event_delivery: ProofDimension,
    pub final_state_convergence: ProofDimension,
    pub sequence: SequenceDiagnostics,
    pub ledger_sorted_digest: [u8; 32],
    pub business_sorted_digest: Option<[u8; 32]>,
    pub final_typed_checksum: [u8; 32],
}

impl OracleReport {
    #[must_use]
    pub fn passed(&self) -> bool {
        self.ledger_delivery.status == ProofStatus::Pass
            && self.business_event_delivery.status == ProofStatus::Pass
            && self.final_state_convergence.status == ProofStatus::Pass
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkloadFixture {
    pub seed: u64,
    pub profile: Profile,
    pub contracts: ContractDigests,
    pub ledger: Vec<LedgerEntry>,
    pub business_events: Vec<BusinessObservation>,
    pub final_state: BTreeMap<(EntityTable, [u8; 32]), [u8; 32]>,
    pub watermark: u64,
}

fn framed_hash(domain: &[u8], fields: &[&[u8]]) -> [u8; 32] {
    let mut hash = Sha256::new();
    for field in std::iter::once(domain).chain(fields.iter().copied()) {
        hash.update((field.len() as u64).to_be_bytes());
        hash.update(field);
    }
    hash.finalize().into()
}

fn seeded(seed: u64, label: &[u8], n: u64) -> [u8; 32] {
    framed_hash(
        b"boring-cdc/workload-fixture/v1",
        &[&seed.to_be_bytes(), label, &n.to_be_bytes()],
    )
}

fn id16(seed: u64, label: &[u8], n: u64) -> [u8; 16] {
    let digest = seeded(seed, label, n);
    digest[..16].try_into().expect("fixed slice")
}

/// Builds the fixed provider-neutral corpus under the three pinned contract digests.
pub fn deterministic_fixture(
    seed: u64,
    profile: Profile,
    contracts: ContractDigests,
) -> Result<WorkloadFixture, OracleFailure> {
    let contracts = contracts.validate()?;
    let run_id = id16(seed, b"run", 0);
    let customer = seeded(seed, b"customer-key", 1);
    let customer_new = seeded(seed, b"customer-key", 2);
    let product = seeded(seed, b"product-key", 1);
    let order = seeded(seed, b"order-key", 1);
    let item = seeded(seed, b"item-key", 1);
    let specs = [
        (
            10,
            0,
            EntityTable::Customers,
            customer,
            Operation::Insert,
            Some(seeded(seed, b"row", 1)),
        ),
        (
            12,
            0,
            EntityTable::Products,
            product,
            Operation::Insert,
            Some(seeded(seed, b"row", 2)),
        ),
        (
            13,
            0,
            EntityTable::Orders,
            order,
            Operation::Insert,
            Some(seeded(seed, b"row", 3)),
        ),
        (
            14,
            0,
            EntityTable::OrderItems,
            item,
            Operation::Insert,
            Some(seeded(seed, b"row", 4)),
        ),
        // Same key is changed twice in one transaction; the first value is later overwritten.
        (
            17,
            0,
            EntityTable::Customers,
            customer,
            Operation::Update,
            Some(seeded(seed, b"row", 5)),
        ),
        (
            18,
            1,
            EntityTable::Customers,
            customer,
            Operation::Update,
            Some(seeded(seed, b"row", 6)),
        ),
        (
            21,
            0,
            EntityTable::OrderItems,
            item,
            Operation::Delete,
            None,
        ),
        (
            24,
            0,
            EntityTable::OrderItems,
            item,
            Operation::Insert,
            Some(seeded(seed, b"row", 7)),
        ),
        // Canonical-key change expansion: old tombstone then new-key upsert.
        (
            27,
            0,
            EntityTable::Customers,
            customer,
            Operation::Delete,
            None,
        ),
        (
            28,
            1,
            EntityTable::Customers,
            customer_new,
            Operation::Insert,
            Some(seeded(seed, b"row", 8)),
        ),
    ];
    let mut ledger = Vec::with_capacity(specs.len());
    let mut business_events = Vec::with_capacity(specs.len());
    let mut final_state = BTreeMap::new();
    for (index, (seq, ordinal, table, key, operation, after)) in specs.into_iter().enumerate() {
        let group_n = match seq {
            17 | 18 => 5,
            27 | 28 => 9,
            _ => index as u64,
        };
        let group = id16(seed, b"group", group_n);
        let mutation_id = id16(seed, b"mutation", seq);
        ledger.push(LedgerEntry {
            run_id,
            mutation_seq: seq,
            mutation_id,
            transaction_group_id: group,
            transaction_ordinal: ordinal,
            entity_table: table,
            key,
            operation,
            expected_after_hash: after,
            committed_at_micros: 1_700_000_000_000_000 + seq as i64,
            record_kind: RecordKind::BusinessMutation,
        });
        business_events.push(BusinessObservation {
            transaction_group_id: group,
            transaction_ordinal: ordinal,
            entity_table: table,
            key,
            operation,
            observed_after_hash: after,
            physical_id: seeded(seed, b"physical", index as u64).to_vec(),
        });
        match after {
            Some(hash) => {
                final_state.insert((table, key), hash);
            }
            None => {
                final_state.remove(&(table, key));
            }
        }
    }
    ledger.push(LedgerEntry {
        run_id,
        mutation_seq: 30,
        mutation_id: id16(seed, b"fence", 30),
        transaction_group_id: id16(seed, b"fence-group", 30),
        transaction_ordinal: 0,
        entity_table: EntityTable::Customers,
        key: seeded(seed, b"fence-key", 30),
        operation: Operation::Insert,
        expected_after_hash: None,
        committed_at_micros: 1_700_000_000_000_030,
        record_kind: RecordKind::Fence,
    });
    if ledger.len() > profile.max_records() {
        return Err(OracleFailure::new(
            "limit",
            "WORKLOAD_PROFILE_RECORD_LIMIT",
            "before_generation",
        ));
    }
    Ok(WorkloadFixture {
        seed,
        profile,
        contracts,
        watermark: ledger.last().map_or(0, |row| row.mutation_seq),
        ledger,
        business_events,
        final_state,
    })
}

fn ledger_leaf(row: &LedgerEntry) -> [u8; 32] {
    let after = row.expected_after_hash.unwrap_or([0; 32]);
    framed_hash(
        DIGEST_DOMAIN,
        &[
            &row.run_id,
            &row.transaction_group_id,
            &row.transaction_ordinal.to_be_bytes(),
            &[row.entity_table.tag()],
            &row.key,
            &[row.operation.tag()],
            &after,
            &[matches!(row.record_kind, RecordKind::Fence) as u8],
        ],
    )
}

fn ledger_set_leaf(row: &LedgerEntry) -> [u8; 32] {
    let canonical = ledger_leaf(row);
    framed_hash(DIGEST_DOMAIN, &[&row.mutation_id, &canonical])
}

fn business_leaf(row: &BusinessObservation) -> [u8; 32] {
    let after = row.observed_after_hash.unwrap_or([0; 32]);
    framed_hash(
        DIGEST_DOMAIN,
        &[
            &row.transaction_group_id,
            &row.transaction_ordinal.to_be_bytes(),
            &[row.entity_table.tag()],
            &row.key,
            &[row.operation.tag()],
            &after,
        ],
    )
}

fn sorted_digest<I>(items: I, max_records: usize) -> Result<[u8; 32], OracleFailure>
where
    I: IntoIterator<Item = [u8; 32]>,
{
    let mut values: Vec<_> = items.into_iter().collect();
    if values.len() > max_records {
        return Err(OracleFailure::new(
            "limit",
            "WORKLOAD_SORT_RECORD_LIMIT",
            "before_oracle_compare",
        ));
    }
    values.sort_unstable();
    let mut hash = Sha256::new();
    hash.update((DIGEST_DOMAIN.len() as u64).to_be_bytes());
    hash.update(DIGEST_DOMAIN);
    for value in values {
        hash.update(value);
    }
    Ok(hash.finalize().into())
}

/// Sorts fixed-size digest records with bounded memory and a caller-owned scratch directory.
/// Chunk files contain no source payloads and are removed on every successful return.
pub fn external_sorted_digest<I>(
    items: I,
    scratch: &Path,
    chunk_items: usize,
    max_records: usize,
) -> Result<[u8; 32], OracleFailure>
where
    I: IntoIterator<Item = [u8; 32]>,
{
    if chunk_items == 0 {
        return Err(OracleFailure::new(
            "contract",
            "WORKLOAD_SORT_CHUNK_ZERO",
            "before_oracle_compare",
        ));
    }
    fs::create_dir_all(scratch).map_err(|_| {
        OracleFailure::new("io", "WORKLOAD_SORT_SCRATCH_IO", "before_oracle_compare")
    })?;
    let mut paths = Vec::new();
    let mut chunk = Vec::with_capacity(chunk_items);
    let mut count = 0usize;
    for item in items {
        count = count.checked_add(1).ok_or_else(|| {
            OracleFailure::new(
                "limit",
                "WORKLOAD_SORT_RECORD_LIMIT",
                "before_oracle_compare",
            )
        })?;
        if count > max_records {
            cleanup_chunks(&paths);
            return Err(OracleFailure::new(
                "limit",
                "WORKLOAD_SORT_RECORD_LIMIT",
                "before_oracle_compare",
            ));
        }
        chunk.push(item);
        if chunk.len() == chunk_items {
            paths.push(write_chunk(scratch, paths.len(), &mut chunk)?);
        }
    }
    if !chunk.is_empty() {
        paths.push(write_chunk(scratch, paths.len(), &mut chunk)?);
    }

    let result = merge_chunks(&paths);
    cleanup_chunks(&paths);
    result
}

fn write_chunk(
    scratch: &Path,
    index: usize,
    values: &mut Vec<[u8; 32]>,
) -> Result<std::path::PathBuf, OracleFailure> {
    values.sort_unstable();
    let path = scratch.join(format!("workload-sort-{index:08}.bin"));
    let file = File::create(&path).map_err(|_| {
        OracleFailure::new("io", "WORKLOAD_SORT_SCRATCH_IO", "before_oracle_compare")
    })?;
    let mut writer = BufWriter::new(file);
    for value in values.iter() {
        writer.write_all(value).map_err(|_| {
            OracleFailure::new("io", "WORKLOAD_SORT_SCRATCH_IO", "before_oracle_compare")
        })?;
    }
    writer.flush().map_err(|_| {
        OracleFailure::new("io", "WORKLOAD_SORT_SCRATCH_IO", "before_oracle_compare")
    })?;
    values.clear();
    Ok(path)
}

fn read_digest(reader: &mut BufReader<File>) -> Result<Option<[u8; 32]>, OracleFailure> {
    let mut value = [0u8; 32];
    let mut offset = 0;
    while offset < value.len() {
        match reader.read(&mut value[offset..]) {
            Ok(0) if offset == 0 => return Ok(None),
            Ok(0) => {
                return Err(OracleFailure::new(
                    "io",
                    "WORKLOAD_SORT_CHUNK_TRUNCATED",
                    "before_oracle_compare",
                ));
            }
            Ok(read) => offset += read,
            Err(_) => {
                return Err(OracleFailure::new(
                    "io",
                    "WORKLOAD_SORT_SCRATCH_IO",
                    "before_oracle_compare",
                ));
            }
        }
    }
    Ok(Some(value))
}

fn merge_chunks(paths: &[std::path::PathBuf]) -> Result<[u8; 32], OracleFailure> {
    let mut readers = paths
        .iter()
        .map(|path| {
            File::open(path).map(BufReader::new).map_err(|_| {
                OracleFailure::new("io", "WORKLOAD_SORT_SCRATCH_IO", "before_oracle_compare")
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(value) = read_digest(reader)? {
            heap.push(Reverse((value, index)));
        }
    }
    let mut hash = Sha256::new();
    hash.update((DIGEST_DOMAIN.len() as u64).to_be_bytes());
    hash.update(DIGEST_DOMAIN);
    while let Some(Reverse((value, index))) = heap.pop() {
        hash.update(value);
        if let Some(next) = read_digest(&mut readers[index])? {
            heap.push(Reverse((next, index)));
        }
    }
    Ok(hash.finalize().into())
}

fn cleanup_chunks(paths: &[std::path::PathBuf]) {
    for path in paths {
        let _ = fs::remove_file(path);
    }
}

fn key_label(key: CorrelationKey) -> String {
    let digest = framed_hash(
        b"boring-cdc/workload-diagnostic/v1",
        &[
            &key.transaction_group_id,
            &key.transaction_ordinal.to_be_bytes(),
            &[key.entity_table.tag()],
            &key.key,
            &[key.operation.tag()],
        ],
    );
    digest[..8].iter().map(|b| format!("{b:02x}")).collect()
}

fn sequence_diagnostics(rows: &[LedgerEntry]) -> SequenceDiagnostics {
    let mut seqs: Vec<_> = rows.iter().map(|row| row.mutation_seq).collect();
    seqs.sort_unstable();
    let mut gaps = Vec::new();
    let mut duplicates = Vec::new();
    for pair in seqs.windows(2) {
        if pair[0] == pair[1] {
            duplicates.push(pair[0]);
        } else if pair[1] > pair[0] + 1 {
            gaps.push((pair[0] + 1, pair[1] - 1));
        }
    }
    SequenceDiagnostics { gaps, duplicates }
}

fn state_checksum(state: &BTreeMap<(EntityTable, [u8; 32]), [u8; 32]>) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update((STATE_DOMAIN.len() as u64).to_be_bytes());
    hash.update(STATE_DOMAIN);
    for ((table, key), value) in state {
        hash.update([table.tag()]);
        hash.update(key);
        hash.update(value);
    }
    hash.finalize().into()
}

pub fn evaluate(
    expected: &WorkloadFixture,
    delivered_ledger: &[LedgerEntry],
    observed_business: Option<&[BusinessObservation]>,
    observed_final_state: &BTreeMap<(EntityTable, [u8; 32]), [u8; 32]>,
) -> Result<OracleReport, OracleFailure> {
    expected.contracts.validate()?;
    let limit = expected.profile.max_records();
    if delivered_ledger.len() > limit
        || observed_business.is_some_and(|rows| rows.len() > limit * 2)
    {
        return Err(OracleFailure::new(
            "limit",
            "WORKLOAD_OBSERVATION_LIMIT",
            "before_oracle_compare",
        ));
    }

    let expected_ledger: BTreeMap<_, _> = expected
        .ledger
        .iter()
        .map(|r| (r.mutation_id, ledger_leaf(r)))
        .collect();
    let mut delivered_ledger_map = BTreeMap::new();
    let mut ledger_duplicates = Vec::new();
    for row in delivered_ledger {
        if delivered_ledger_map
            .insert(row.mutation_id, ledger_leaf(row))
            .is_some()
        {
            ledger_duplicates.push(hex16(&row.mutation_id));
        }
    }
    let ledger_missing = expected_ledger
        .keys()
        .filter(|k| !delivered_ledger_map.contains_key(*k))
        .map(hex16)
        .collect::<Vec<_>>();
    let ledger_unexpected = delivered_ledger_map
        .keys()
        .filter(|k| !expected_ledger.contains_key(*k))
        .map(hex16)
        .chain(ledger_duplicates)
        .collect::<Vec<_>>();
    let ledger_mismatch = expected_ledger.iter().any(|(k, v)| {
        delivered_ledger_map
            .get(k)
            .is_some_and(|actual| actual != v)
    });
    let ledger_delivery = ProofDimension {
        status: if ledger_missing.is_empty() && ledger_unexpected.is_empty() && !ledger_mismatch {
            ProofStatus::Pass
        } else {
            ProofStatus::Fail
        },
        expected_count: expected_ledger.len(),
        observed_count: delivered_ledger_map.len(),
        missing: ledger_missing,
        unexpected: ledger_unexpected,
    };

    let expected_business: BTreeMap<_, _> = expected
        .business_events
        .iter()
        .map(|r| (CorrelationKey::observed(r), business_leaf(r)))
        .collect();
    let (business_event_delivery, business_sorted_digest) = match observed_business {
        None => (
            ProofDimension {
                status: ProofStatus::Unavailable,
                expected_count: expected_business.len(),
                observed_count: 0,
                missing: vec!["event_boundary_unavailable".into()],
                unexpected: vec![],
            },
            None,
        ),
        Some(rows) => {
            let mut physical = BTreeMap::<Vec<u8>, [u8; 32]>::new();
            let mut observed = BTreeMap::new();
            let mut ambiguous = Vec::new();
            for row in rows {
                let leaf = business_leaf(row);
                if let Some(prior) = physical.insert(row.physical_id.clone(), leaf) {
                    if prior != leaf {
                        ambiguous.push("conflicting_physical_retry".into());
                    }
                    continue;
                }
                let key = CorrelationKey::observed(row);
                if observed.insert(key, leaf).is_some() {
                    ambiguous.push(key_label(key));
                }
            }
            let missing = expected_business
                .keys()
                .filter(|k| !observed.contains_key(*k))
                .map(|k| key_label(*k))
                .collect::<Vec<_>>();
            let unexpected = observed
                .keys()
                .filter(|k| !expected_business.contains_key(*k))
                .map(|k| key_label(*k))
                .chain(ambiguous)
                .collect::<Vec<_>>();
            let mismatch = expected_business
                .iter()
                .any(|(k, v)| observed.get(k).is_some_and(|actual| actual != v));
            let status = if missing.is_empty() && unexpected.is_empty() && !mismatch {
                ProofStatus::Pass
            } else {
                ProofStatus::Fail
            };
            (
                ProofDimension {
                    status,
                    expected_count: expected_business.len(),
                    observed_count: observed.len(),
                    missing,
                    unexpected,
                },
                Some(sorted_digest(observed.values().copied(), limit)?),
            )
        }
    };

    let expected_checksum = state_checksum(&expected.final_state);
    let observed_checksum = state_checksum(observed_final_state);
    let final_state_convergence = ProofDimension {
        status: if expected_checksum == observed_checksum {
            ProofStatus::Pass
        } else {
            ProofStatus::Fail
        },
        expected_count: expected.final_state.len(),
        observed_count: observed_final_state.len(),
        missing: if expected_checksum == observed_checksum {
            vec![]
        } else {
            vec!["typed_state_checksum_mismatch".into()]
        },
        unexpected: vec![],
    };

    let scratch = std::env::temp_dir().join(format!(
        "boring-cdc-workload-oracle-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let ledger_sorted_digest = external_sorted_digest(
        delivered_ledger.iter().map(ledger_set_leaf),
        &scratch,
        256,
        limit,
    )?;
    let business_sorted_digest = match observed_business {
        Some(rows) => Some(external_sorted_digest(
            rows.iter()
                .map(business_leaf)
                .collect::<std::collections::BTreeSet<_>>(),
            &scratch,
            256,
            limit * 2,
        )?),
        None => business_sorted_digest,
    };
    let _ = fs::remove_dir(&scratch);
    Ok(OracleReport {
        ledger_delivery,
        business_event_delivery,
        final_state_convergence,
        sequence: sequence_diagnostics(delivered_ledger),
        ledger_sorted_digest,
        business_sorted_digest,
        final_typed_checksum: observed_checksum,
    })
}

fn hex16(value: &[u8; 16]) -> String {
    value.iter().map(|b| format!("{b:02x}")).collect()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OracleFailure {
    pub class: &'static str,
    pub fingerprint: &'static str,
    pub failed_boundary: &'static str,
    pub allowed_actions: &'static str,
    pub recovery: &'static str,
}

impl OracleFailure {
    fn new(class: &'static str, fingerprint: &'static str, failed_boundary: &'static str) -> Self {
        Self {
            class,
            fingerprint,
            failed_boundary,
            allowed_actions: "inspect_or_supply_approved_contract",
            recovery: "checkpoint_unchanged",
        }
    }
}

impl fmt::Display for OracleFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.class, self.fingerprint)
    }
}
impl std::error::Error for OracleFailure {}

#[cfg(test)]
pub mod tests {
    use super::*;

    fn contracts() -> ContractDigests {
        provisional_contract_digests()
    }
    fn fixture() -> WorkloadFixture {
        deterministic_fixture(7, Profile::Smoke, contracts()).unwrap()
    }

    #[test]
    fn clean_fixed_seed_is_reproducible_and_sequence_gaps_are_diagnostic() {
        let a = fixture();
        let b = fixture();
        assert_eq!(a, b);
        let report = evaluate(&a, &a.ledger, Some(&a.business_events), &a.final_state).unwrap();
        assert!(report.passed());
        eprintln!(
            "ledger={}",
            report
                .ledger_sorted_digest
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
        eprintln!(
            "business={}",
            report
                .business_sorted_digest
                .unwrap()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
        eprintln!(
            "state={}",
            report
                .final_typed_checksum
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
        assert!(!report.sequence.gaps.is_empty());
        assert!(report.sequence.duplicates.is_empty());
    }

    #[test]
    fn business_only_overwritten_omission_fails_despite_ledger_and_final_state() {
        let fixture = fixture();
        let observed: Vec<_> = fixture
            .business_events
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != 4)
            .map(|(_, row)| row.clone())
            .collect();
        let report = evaluate(
            &fixture,
            &fixture.ledger,
            Some(&observed),
            &fixture.final_state,
        )
        .unwrap();
        assert_eq!(report.ledger_delivery.status, ProofStatus::Pass);
        assert_eq!(report.business_event_delivery.status, ProofStatus::Fail);
        assert_eq!(report.final_state_convergence.status, ProofStatus::Pass);
        assert!(!report.passed());
    }

    #[test]
    fn ledger_only_omission_fails_despite_business_and_final_state() {
        let fixture = fixture();
        let delivered: Vec<_> = fixture
            .ledger
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != 4)
            .map(|(_, row)| row.clone())
            .collect();
        let report = evaluate(
            &fixture,
            &delivered,
            Some(&fixture.business_events),
            &fixture.final_state,
        )
        .unwrap();
        assert_eq!(report.ledger_delivery.status, ProofStatus::Fail);
        assert_eq!(report.business_event_delivery.status, ProofStatus::Pass);
        assert_eq!(report.final_state_convergence.status, ProofStatus::Pass);
    }

    #[test]
    fn retry_duplicates_are_local_only_and_conflicts_fail() {
        let fixture = fixture();
        let mut retries = fixture.business_events.clone();
        retries.push(retries[0].clone());
        assert!(
            evaluate(
                &fixture,
                &fixture.ledger,
                Some(&retries),
                &fixture.final_state
            )
            .unwrap()
            .passed()
        );
        let mut conflict = fixture.business_events[0].clone();
        conflict.operation = Operation::Delete;
        retries.push(conflict);
        assert_eq!(
            evaluate(
                &fixture,
                &fixture.ledger,
                Some(&retries),
                &fixture.final_state
            )
            .unwrap()
            .business_event_delivery
            .status,
            ProofStatus::Fail
        );
    }

    #[test]
    fn missing_provider_event_boundary_is_never_a_pass() {
        let fixture = fixture();
        let report = evaluate(&fixture, &fixture.ledger, None, &fixture.final_state).unwrap();
        assert_eq!(
            report.business_event_delivery.status,
            ProofStatus::Unavailable
        );
        assert!(!report.passed());
    }

    #[test]
    fn delete_reinsert_and_key_change_have_distinct_ordered_correlations() {
        let fixture = fixture();
        let item = &fixture.ledger[6..8];
        assert_eq!(
            (item[0].operation, item[1].operation),
            (Operation::Delete, Operation::Insert)
        );
        let key_change = &fixture.ledger[8..10];
        assert_eq!(
            (
                key_change[0].transaction_group_id,
                key_change[0].transaction_ordinal
            ),
            (key_change[1].transaction_group_id, 0)
        );
        assert_eq!(key_change[1].transaction_ordinal, 1);
        assert_ne!(key_change[0].key, key_change[1].key);
    }

    #[test]
    fn external_sort_matches_bounded_sort_and_cleans_chunks() {
        let values = (0..19)
            .rev()
            .map(|n| seeded(7, b"sort", n))
            .collect::<Vec<_>>();
        let root =
            std::env::temp_dir().join(format!("boring-cdc-workload-sort-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let expected = sorted_digest(values.clone(), values.len()).unwrap();
        let actual = external_sorted_digest(values, &root, 3, 19).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
        fs::remove_dir(&root).unwrap();
    }

    #[test]
    fn zero_contract_digest_and_observation_overflow_fail_closed() {
        assert_eq!(
            deterministic_fixture(
                7,
                Profile::Smoke,
                ContractDigests {
                    keys: [0; 32],
                    values: [2; 32],
                    oracle: [3; 32]
                }
            )
            .unwrap_err()
            .fingerprint,
            "WORKLOAD_CONTRACT_DIGEST_UNRESOLVED"
        );
        let fixture = fixture();
        let too_many = vec![fixture.business_events[0].clone(); 129];
        assert_eq!(
            evaluate(
                &fixture,
                &fixture.ledger,
                Some(&too_many),
                &fixture.final_state
            )
            .unwrap_err()
            .fingerprint,
            "WORKLOAD_OBSERVATION_LIMIT"
        );
    }
}
