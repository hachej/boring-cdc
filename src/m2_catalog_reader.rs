//! Catalog acquisition for the first admitted, unpartitioned relation profile.
//!
//! Every unimplemented catalog feature is checked and rejected. A pgoutput Relation message
//! supplies only a subset of the M0 relation fingerprint inputs and must never substitute for
//! these queries.

use crate::m1_decoder::RelationContract;
use crate::m1_source_identity::SourceIdentity;
use crate::m2_relation_contract::{
    PhysicalColumn, RelationCatalogContract, ReplicaMode, simple_primary_key_hash,
};
use pg_walstream::PgReplicationConnection;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogError {
    Query,
    Missing,
    Invalid,
    Drift,
    Unsupported,
}

/// Read one catalog version while the caller holds the source ownership lock. The restricted
/// profile deliberately rejects expressions, generated/identity columns, partitions, row filters,
/// column projections, non-default replica identity, and complex keys before producing a hash.
pub fn read_simple_relation(
    connection: &mut PgReplicationConnection,
    publication: &str,
    source: &SourceIdentity,
    expected: &RelationContract,
) -> Result<RelationCatalogContract, CatalogError> {
    connection
        .exec("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .map_err(|_| CatalogError::Query)?;
    let result = read_simple_relation_in_snapshot(connection, publication, source, expected);
    let finish = if result.is_ok() { "COMMIT" } else { "ROLLBACK" };
    connection.exec(finish).map_err(|_| CatalogError::Query)?;
    result
}

fn read_simple_relation_in_snapshot(
    connection: &mut PgReplicationConnection,
    publication: &str,
    source: &SourceIdentity,
    expected: &RelationContract,
) -> Result<RelationCatalogContract, CatalogError> {
    let relation_id = expected.relation.id;
    let query = format!(
        "SELECT n.nspname,c.relname,c.relreplident,c.relkind,c.relispartition::text,\
         a.attnum::text,a.attname,a.attisdropped::text,a.atttypid::text,a.atttypmod::text,\
         a.attcollation::text,(NOT a.attnotnull)::text,a.attgenerated,a.attidentity,\
         (d.oid IS NOT NULL)::text,\
         EXISTS(SELECT 1 FROM pg_inherits h WHERE h.inhrelid=c.oid OR h.inhparent=c.oid)::text \
         FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace \
         JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 \
         LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum \
         WHERE c.oid={relation_id} ORDER BY a.attnum"
    );
    let rows = connection.exec(&query).map_err(|_| CatalogError::Query)?;
    if rows.ntuples() == 0 {
        return Err(CatalogError::Missing);
    }
    let first = |column| value(&rows, 0, column);
    let namespace = first(0)?;
    let name = first(1)?;
    if namespace != expected.relation.namespace || name != expected.relation.name {
        return Err(CatalogError::Drift);
    }
    if first(2)? != "d" || first(3)? != "r" || first(4)? != "false" || first(15)? != "false" {
        return Err(CatalogError::Unsupported);
    }
    let mut columns = Vec::new();
    let mut logical_ordinal = 0_u32;
    let mut previous_attnum = 0_i16;
    for row in 0..rows.ntuples() {
        if value(&rows, row, 0)? != namespace
            || value(&rows, row, 1)? != name
            || value(&rows, row, 2)? != "d"
            || value(&rows, row, 3)? != "r"
            || value(&rows, row, 4)? != "false"
            || value(&rows, row, 15)? != "false"
        {
            return Err(CatalogError::Drift);
        }
        let attnum = parse::<i16>(&rows, row, 5)?;
        if attnum <= previous_attnum {
            return Err(CatalogError::Invalid);
        }
        previous_attnum = attnum;
        let dropped = flag(&rows, row, 7)?;
        let column_name = if dropped {
            String::new()
        } else {
            value(&rows, row, 6)?
        };
        let ordinal = if dropped {
            None
        } else {
            let current = logical_ordinal;
            logical_ordinal = logical_ordinal
                .checked_add(1)
                .ok_or(CatalogError::Invalid)?;
            Some(current)
        };
        if !value(&rows, row, 12)?.is_empty()
            || !value(&rows, row, 13)?.is_empty()
            || flag(&rows, row, 14)?
        {
            return Err(CatalogError::Unsupported);
        }
        columns.push(PhysicalColumn {
            attnum,
            logical_ordinal: ordinal,
            name: column_name,
            dropped,
            type_oid: parse(&rows, row, 8)?,
            typmod: parse(&rows, row, 9)?,
            collation_oid: parse(&rows, row, 10)?,
            nullable: flag(&rows, row, 11)?,
            default_expression_hash: None,
            generated_expression_hash: None,
            identity_expression_hash: None,
        });
    }
    let live_columns = columns.iter().filter(|column| !column.dropped);
    if live_columns.count() != expected.relation.columns.len()
        || columns
            .iter()
            .filter(|column| !column.dropped)
            .zip(&expected.relation.columns)
            .any(|(actual, expected)| {
                actual.name != expected.name
                    || actual.type_oid != expected.type_oid
                    || actual.typmod != expected.type_modifier
            })
    {
        return Err(CatalogError::Drift);
    }

    let key_query = format!(
        "SELECT pg_get_constraintdef(oid,false) FROM pg_constraint \
         WHERE conrelid={relation_id} AND contype='p'"
    );
    let keys = connection
        .exec(&key_query)
        .map_err(|_| CatalogError::Query)?;
    if keys.ntuples() != 1 || expected.key_columns.len() != 1 {
        return Err(CatalogError::Unsupported);
    }
    let key_index = expected.key_columns[0];
    let key_name = &expected
        .relation
        .columns
        .get(key_index)
        .ok_or(CatalogError::Drift)?
        .name;
    let canonical_key = format!("PRIMARY KEY ({key_name})");
    if value(&keys, 0, 0)? != canonical_key {
        return Err(CatalogError::Unsupported);
    }
    let effective_key_definition_hash =
        simple_primary_key_hash(key_name).map_err(|_| CatalogError::Unsupported)?;

    let publication = publication.replace('\'', "''");
    let publication_query = format!(
        "SELECT p.pubinsert::text,p.pubupdate::text,p.pubdelete::text,p.pubtruncate::text,\
         p.puballtables::text,(r.prqual IS NULL)::text,(r.prattrs IS NULL)::text \
         FROM pg_publication p JOIN pg_publication_rel r ON r.prpubid=p.oid \
         WHERE p.pubname='{publication}' AND r.prrelid={relation_id}"
    );
    let publication = connection
        .exec(&publication_query)
        .map_err(|_| CatalogError::Query)?;
    if publication.ntuples() != 1 {
        return Err(CatalogError::Missing);
    }
    if flag(&publication, 0, 4)? || !flag(&publication, 0, 5)? || !flag(&publication, 0, 6)? {
        return Err(CatalogError::Unsupported);
    }
    let action_bits =
        [(0, 1_u8), (1, 2), (2, 4), (3, 8)]
            .into_iter()
            .try_fold(0_u8, |bits, (column, bit)| {
                Ok::<_, CatalogError>(
                    bits | if flag(&publication, 0, column)? {
                        bit
                    } else {
                        0
                    },
                )
            })?;
    Ok(RelationCatalogContract {
        logical_table: crate::m1_source_identity::LogicalTableIdentity::derive(
            source, &namespace, &name,
        ),
        relation_id,
        columns,
        replica_mode: ReplicaMode::Default,
        replica_index_definition_hash: None,
        effective_key_definition_hash: Some(effective_key_definition_hash),
        partition_root_logical_table_id: None,
        partition_routing_definition_hash: None,
        partition_key_hash: None,
        partition_bounds_hash: None,
        publication_action_bits: action_bits,
        publication_member: true,
        publication_row_filter_hash: None,
        publication_column_projection_hash: None,
    })
}

fn value(rows: &pg_walstream::PgResult, row: i32, column: i32) -> Result<String, CatalogError> {
    rows.get_value(row, column).ok_or(CatalogError::Invalid)
}

fn parse<T: std::str::FromStr>(
    rows: &pg_walstream::PgResult,
    row: i32,
    column: i32,
) -> Result<T, CatalogError> {
    value(rows, row, column)?
        .parse()
        .map_err(|_| CatalogError::Invalid)
}

fn flag(rows: &pg_walstream::PgResult, row: i32, column: i32) -> Result<bool, CatalogError> {
    match value(rows, row, column)?.as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(CatalogError::Invalid),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::m1_decoder::{Column, Relation};
    use crate::m1_source_identity::Fingerprint;

    #[test]
    #[ignore = "requires an isolated PostgreSQL 16 database in BCDC_CATALOG_DSN"]
    fn live_catalog_reads_simple_table_and_rejects_filter() {
        let dsn = std::env::var("BCDC_CATALOG_DSN").unwrap();
        let mut connection = PgReplicationConnection::connect(&dsn).unwrap();
        connection
            .exec("DROP PUBLICATION IF EXISTS cdc_catalog_probe")
            .unwrap();
        connection
            .exec("DROP TABLE IF EXISTS public.cdc_catalog_probe")
            .unwrap();
        connection
            .exec(
                "CREATE TABLE public.cdc_catalog_probe (id bigint PRIMARY KEY, memo text NOT NULL)",
            )
            .unwrap();
        connection
            .exec("CREATE PUBLICATION cdc_catalog_probe FOR TABLE public.cdc_catalog_probe")
            .unwrap();
        let relation_id = connection
            .exec("SELECT 'public.cdc_catalog_probe'::regclass::oid::text")
            .unwrap()
            .get_value(0, 0)
            .unwrap()
            .parse()
            .unwrap();
        let expected = RelationContract {
            relation: Relation {
                id: relation_id,
                namespace: "public".into(),
                name: "cdc_catalog_probe".into(),
                replica_identity: b'd',
                columns: vec![
                    Column {
                        key: true,
                        name: "id".into(),
                        type_oid: 20,
                        type_modifier: -1,
                    },
                    Column {
                        key: false,
                        name: "memo".into(),
                        type_oid: 25,
                        type_modifier: -1,
                    },
                ],
            },
            key_columns: vec![0],
            control: None,
        };
        let source = SourceIdentity {
            system_identifier: 1,
            timeline: 1,
            database_identity: 1,
            slot_name: "slot".into(),
            plugin: "pgoutput".into(),
            publication_fingerprint: Fingerprint::digest(b"publication"),
            protocol_fingerprint: Fingerprint::digest(b"protocol"),
        };
        let catalog =
            read_simple_relation(&mut connection, "cdc_catalog_probe", &source, &expected).unwrap();
        assert_eq!(catalog.columns.len(), 2);
        assert_eq!(catalog.publication_action_bits, 15);
        assert_eq!(
            catalog.effective_key_definition_hash,
            Some(simple_primary_key_hash("id").unwrap())
        );
        let encoding = catalog.encode().unwrap();
        assert!(!encoding.is_empty());
        connection
            .exec("ALTER PUBLICATION cdc_catalog_probe SET TABLE public.cdc_catalog_probe WHERE (id > 0)")
            .unwrap();
        assert_eq!(
            read_simple_relation(&mut connection, "cdc_catalog_probe", &source, &expected),
            Err(CatalogError::Unsupported)
        );
        connection
            .exec("DROP PUBLICATION cdc_catalog_probe")
            .unwrap();
        connection
            .exec("DROP TABLE public.cdc_catalog_probe")
            .unwrap();
    }
}
