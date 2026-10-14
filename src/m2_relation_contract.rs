//! Exact M0 relation contract encoding for catalog-backed schemas.
//!
//! The pgoutput `Relation` message lacks several fields below. A caller must obtain the full
//! catalog facts and bind them to its source identity before persisting this encoding.

use crate::m1_source_identity::LogicalTableIdentity;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaMode {
    Default,
    Nothing,
    Full,
    Index,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalColumn {
    pub attnum: i16,
    pub logical_ordinal: Option<u32>,
    pub name: String,
    pub dropped: bool,
    pub type_oid: u32,
    pub typmod: i32,
    pub collation_oid: u32,
    pub nullable: bool,
    pub default_expression_hash: Option<[u8; 32]>,
    pub generated_expression_hash: Option<[u8; 32]>,
    pub identity_expression_hash: Option<[u8; 32]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationCatalogContract {
    pub logical_table: LogicalTableIdentity,
    pub relation_id: u32,
    pub columns: Vec<PhysicalColumn>,
    pub replica_mode: ReplicaMode,
    pub replica_index_definition_hash: Option<[u8; 32]>,
    pub effective_key_definition_hash: Option<[u8; 32]>,
    pub partition_root_logical_table_id: Option<[u8; 32]>,
    pub partition_routing_definition_hash: Option<[u8; 32]>,
    pub partition_key_hash: Option<[u8; 32]>,
    pub partition_bounds_hash: Option<[u8; 32]>,
    pub publication_action_bits: u8,
    pub publication_member: bool,
    pub publication_row_filter_hash: Option<[u8; 32]>,
    pub publication_column_projection_hash: Option<[u8; 32]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelationContractError {
    InvalidColumn,
    InvalidPublication,
    TooManyColumns,
}

impl RelationCatalogContract {
    pub fn encode(&self) -> Result<Vec<u8>, RelationContractError> {
        let count =
            u32::try_from(self.columns.len()).map_err(|_| RelationContractError::TooManyColumns)?;
        if self.publication_action_bits > 15 {
            return Err(RelationContractError::InvalidPublication);
        }
        let mut bytes = Vec::new();
        field(&mut bytes, &self.logical_table.fingerprint().bytes());
        bytes.extend(self.relation_id.to_be_bytes());
        bytes.extend(count.to_be_bytes());
        let mut previous_attnum = None;
        for column in &self.columns {
            if column.attnum <= 0
                || previous_attnum.is_some_and(|previous| column.attnum <= previous)
                || column.dropped != column.logical_ordinal.is_none()
                || column.dropped != column.name.is_empty()
            {
                return Err(RelationContractError::InvalidColumn);
            }
            previous_attnum = Some(column.attnum);
            bytes.extend(column.attnum.to_be_bytes());
            bytes.extend(column.logical_ordinal.unwrap_or(u32::MAX).to_be_bytes());
            field(&mut bytes, column.name.as_bytes());
            bytes.push(u8::from(column.dropped));
            bytes.extend(column.type_oid.to_be_bytes());
            bytes.extend(column.typmod.to_be_bytes());
            bytes.extend(column.collation_oid.to_be_bytes());
            bytes.push(u8::from(column.nullable));
            optional_hash(&mut bytes, column.default_expression_hash);
            optional_hash(&mut bytes, column.generated_expression_hash);
            optional_hash(&mut bytes, column.identity_expression_hash);
        }
        bytes.push(match self.replica_mode {
            ReplicaMode::Default => 0,
            ReplicaMode::Nothing => 1,
            ReplicaMode::Full => 2,
            ReplicaMode::Index => 3,
        });
        for hash in [
            self.replica_index_definition_hash,
            self.effective_key_definition_hash,
            self.partition_root_logical_table_id,
            self.partition_routing_definition_hash,
            self.partition_key_hash,
            self.partition_bounds_hash,
        ] {
            optional_hash(&mut bytes, hash);
        }
        bytes.push(self.publication_action_bits);
        bytes.push(u8::from(self.publication_member));
        optional_hash(&mut bytes, self.publication_row_filter_hash);
        optional_hash(&mut bytes, self.publication_column_projection_hash);
        Ok(bytes)
    }
}

fn field(out: &mut Vec<u8>, value: &[u8]) {
    out.extend((value.len() as u64).to_be_bytes());
    out.extend(value);
}

fn optional_hash(out: &mut Vec<u8>, hash: Option<[u8; 32]>) {
    field(out, hash.as_ref().map_or(&[], |bytes| bytes));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m1_source_identity::{Fingerprint, SourceIdentity};

    fn hash(value: &serde_json::Value) -> Option<[u8; 32]> {
        value.as_str().map(|hex| {
            let mut bytes = [0_u8; 32];
            for (index, pair) in hex.as_bytes().chunks_exact(2).enumerate() {
                bytes[index] = u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap();
            }
            bytes
        })
    }

    #[test]
    fn encodes_exact_m0_relation_vector() {
        let vectors: serde_json::Value = serde_json::from_str(include_str!(
            "../fixtures/m0/event-format/golden-vectors.json"
        ))
        .unwrap();
        let primitives = &vectors["identity_primitives"];
        let input = &primitives["relation_fingerprint"]["input"];
        let table = &primitives["logical_table"]["input"];
        let source = SourceIdentity {
            system_identifier: table["system_identifier"].as_u64().unwrap(),
            timeline: 1,
            database_identity: 16384,
            slot_name: "slot".into(),
            plugin: "pgoutput".into(),
            publication_fingerprint: Fingerprint::digest(b"publication"),
            protocol_fingerprint: Fingerprint::digest(b"protocol"),
        };
        let logical_table = LogicalTableIdentity::derive(
            &source,
            table["schema"].as_str().unwrap(),
            table["table"].as_str().unwrap(),
        );
        let columns = input["columns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| PhysicalColumn {
                attnum: value["attnum"].as_i64().unwrap() as i16,
                logical_ordinal: value["logical_ordinal"].as_u64().map(|v| v as u32),
                name: value["name"].as_str().unwrap().into(),
                dropped: value["dropped"].as_bool().unwrap(),
                type_oid: value["type_oid"].as_u64().unwrap() as u32,
                typmod: value["typmod"].as_i64().unwrap() as i32,
                collation_oid: value["collation_oid"].as_u64().unwrap() as u32,
                nullable: value["nullable"].as_bool().unwrap(),
                default_expression_hash: hash(&value["default_expression_hash"]),
                generated_expression_hash: hash(&value["generated_expression_hash"]),
                identity_expression_hash: hash(&value["identity_expression_hash"]),
            })
            .collect();
        let contract = RelationCatalogContract {
            logical_table,
            relation_id: input["relation_id"].as_u64().unwrap() as u32,
            columns,
            replica_mode: ReplicaMode::Default,
            replica_index_definition_hash: hash(&input["replica_index_definition_hash"]),
            effective_key_definition_hash: hash(&input["effective_key_definition_hash"]),
            partition_root_logical_table_id: hash(&input["partition_root_logical_table_id"]),
            partition_routing_definition_hash: hash(&input["partition_routing_definition_hash"]),
            partition_key_hash: hash(&input["partition_key_hash"]),
            partition_bounds_hash: hash(&input["partition_bounds_hash"]),
            publication_action_bits: input["publication_action_bits"].as_u64().unwrap() as u8,
            publication_member: input["publication_member"].as_bool().unwrap(),
            publication_row_filter_hash: hash(&input["publication_row_filter_hash"]),
            publication_column_projection_hash: hash(&input["publication_column_projection_hash"]),
        };
        assert_eq!(
            contract.encode().unwrap(),
            include_bytes!("../tests/fixtures/m1_ordering/relation-contract.bin")
        );
    }
}
