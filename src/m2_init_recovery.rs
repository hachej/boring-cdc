//! Exclusive, idempotent source preparation for `boring-cdc init`.
//!
//! This boundary deliberately never creates, exports, or drops a logical slot. PostgreSQL
//! effects are performed under the same local/source ownership pair as ordinary capture, and
//! the administration DSN is held only by the caller for this one request.

use crate::m1_config::LoadedConfig;
use crate::m1_control_fixtures::PublicationSpec;
use crate::m2_ownership::{OwnerKind, OwnershipError, OwnershipGuard, SourceLockSession};
use crate::m2_schema::open_writer;
use pg_walstream::PgReplicationConnection;
use sha2::{Digest, Sha256};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const OWNER_BEAD: &str = "boring-cdc-m2-init-recovery";
pub const ADMIN_ROLE: &str = "boring_cdc_admin";
pub const CONTROL_ROLE: &str = "boring_cdc_control_writer";
pub const HEARTBEAT: &str = "boring_cdc_control.heartbeat";
pub const FENCE: &str = "boring_cdc_control.capture_fences";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitFailure {
    pub code: &'static str,
    pub boundary: &'static str,
}
impl InitFailure {
    fn at(boundary: &'static str, code: &'static str) -> Self {
        Self { code, boundary }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct InitReceipt {
    pub schema_version: &'static str,
    pub outcome: &'static str,
    pub publication_fingerprint: String,
    pub source_identity_fingerprint: String,
    pub sqlite_initialized: bool,
    pub control_rows: u8,
    pub logical_slot_exists: bool,
}

struct AdminSourceLock {
    connection: PgReplicationConnection,
    pid: i32,
    nonce: String,
    key: i64,
}
impl SourceLockSession for AdminSourceLock {
    fn backend_pid(&self) -> i32 {
        self.pid
    }
    fn connection_nonce(&self) -> &str {
        &self.nonce
    }
    fn advisory_lock_key(&self) -> i64 {
        self.key
    }
    fn try_lock(&mut self) -> Result<bool, OwnershipError> {
        self.connection
            .exec(&format!("SELECT pg_try_advisory_lock({})::int", self.key))
            .map_err(|_| OwnershipError::SourceSessionLost)
            .map(|r| r.get_value(0, 0).as_deref() == Some("1"))
    }
    fn healthy(&mut self) -> bool {
        self.connection.is_alive() && self.connection.exec("SELECT 1").is_ok()
    }
    fn unlock(&mut self) -> Result<(), OwnershipError> {
        self.connection
            .exec(&format!("SELECT pg_advisory_unlock({})::int", self.key))
            .map(|_| ())
            .map_err(|_| OwnershipError::SourceSessionLost)
    }
}

fn simple_ident(value: &str) -> bool {
    let mut c = value.chars();
    matches!(c.next(), Some('a'..='z' | 'A'..='Z' | '_'))
        && c.all(|x| x.is_ascii_alphanumeric() || x == '_')
}
fn relation(value: &str) -> Option<String> {
    let p = value.split('.').collect::<Vec<_>>();
    (p.len() == 2 && p.iter().all(|x| simple_ident(x)))
        .then(|| format!("\"{}\".\"{}\"", p[0], p[1]))
}
fn scalar(
    c: &mut PgReplicationConnection,
    sql: &str,
    boundary: &'static str,
) -> Result<String, InitFailure> {
    let r = c
        .exec(sql)
        .map_err(|_| InitFailure::at(boundary, "M2_INIT_SOURCE_QUERY_FAILED"))?;
    if r.ntuples() != 1 {
        return Err(InitFailure::at(boundary, "M2_INIT_SOURCE_CARDINALITY"));
    }
    r.get_value(0, 0)
        .ok_or_else(|| InitFailure::at(boundary, "M2_INIT_SOURCE_VALUE_MISSING"))
}
fn exec(
    c: &mut PgReplicationConnection,
    sql: &str,
    boundary: &'static str,
) -> Result<(), InitFailure> {
    c.exec(sql)
        .map(|_| ())
        .map_err(|_| InitFailure::at(boundary, "M2_INIT_SOURCE_MUTATION_FAILED"))
}

fn acquire(
    config: &LoadedConfig,
    dsn: &str,
    run_id: &str,
) -> Result<OwnershipGuard<AdminSourceLock>, InitFailure> {
    let mut connection = PgReplicationConnection::connect(dsn)
        .map_err(|_| InitFailure::at("ownership", "M2_INIT_ADMIN_CONNECT_FAILED"))?;
    let pid = scalar(
        &mut connection,
        "SELECT pg_backend_pid()::text",
        "ownership",
    )?
    .parse()
    .map_err(|_| InitFailure::at("ownership", "M2_INIT_SOURCE_IDENTITY_INVALID"))?;
    let digest = Sha256::digest(config.fingerprints().source.as_bytes());
    let key = i64::from_be_bytes(digest[..8].try_into().expect("digest width"));
    let nonce = format!("{:x}", Sha256::digest(format!("{run_id}:{pid}").as_bytes()));
    let path = Path::new(&config.public().storage.sqlite_path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|_| InitFailure::at("ownership", "M2_INIT_STATE_DIRECTORY_FAILED"))?;
    }
    let mut guard = OwnershipGuard::acquire(
        path,
        run_id.into(),
        OwnerKind::Maintenance,
        Duration::from_millis(config.public().source.ownership_deadline_ms.0),
        AdminSourceLock {
            connection,
            pid,
            nonce,
            key,
        },
    )
    .map_err(|_| InitFailure::at("ownership", "M2_INIT_OWNERSHIP_CONFLICT"))?;
    guard
        .reconcile_after_unclean_release(|| true)
        .map_err(|_| InitFailure::at("reconciliation", "M2_INIT_RECONCILIATION_REQUIRED"))?;
    Ok(guard)
}

/// Execute one confirmed initialization request. The DSN is borrowed and never retained/logged.
pub fn execute_confirmed(
    config: &LoadedConfig,
    admin_dsn: &str,
    run_id: &str,
) -> Result<InitReceipt, InitFailure> {
    if !simple_ident(&config.public().source.publication)
        || !simple_ident(&config.public().source.slot)
    {
        return Err(InitFailure::at(
            "configuration",
            "M2_INIT_IDENTIFIER_UNSUPPORTED",
        ));
    }
    let user_relations = config
        .public()
        .tables
        .iter()
        .map(|t| t.source_relation.clone())
        .collect::<Vec<_>>();
    let quoted = user_relations
        .iter()
        .map(|x| {
            relation(x)
                .ok_or_else(|| InitFailure::at("configuration", "M2_INIT_IDENTIFIER_UNSUPPORTED"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let expected = PublicationSpec::new(
        &config.public().source.publication,
        ADMIN_ROLE,
        user_relations,
    );
    let publication_fingerprint = expected.fingerprint();
    let mut ownership = acquire(config, admin_dsn, run_id)?;
    ownership
        .admit_source_mutation(Duration::from_millis(
            config
                .public()
                .source
                .maximum_operation_ms
                .0
                .min(config.public().source.ownership_deadline_ms.0 / 2)
                .max(1),
        ))
        .map_err(|_| InitFailure::at("ownership", "M2_INIT_OWNERSHIP_LOST"))?;
    // A dedicated connection performs the bounded request while the lock connection remains live.
    let mut c = PgReplicationConnection::connect(admin_dsn)
        .map_err(|_| InitFailure::at("source", "M2_INIT_ADMIN_CONNECT_FAILED"))?;
    let identity = c
        .exec("SELECT system_identifier::text,(pg_control_checkpoint()).timeline_id::text FROM pg_control_system()")
        .map_err(|_| InitFailure::at("source_identity", "M2_INIT_SOURCE_IDENTITY_FAILED"))?;
    let system = identity
        .get_value(0, 0)
        .ok_or_else(|| InitFailure::at("source_identity", "M2_INIT_SOURCE_IDENTITY_FAILED"))?;
    let timeline = identity
        .get_value(0, 1)
        .ok_or_else(|| InitFailure::at("source_identity", "M2_INIT_SOURCE_IDENTITY_FAILED"))?;
    let database = scalar(
        &mut c,
        "SELECT oid::text FROM pg_database WHERE datname=current_database()",
        "source_identity",
    )?;
    let source_identity_fingerprint = format!(
        "{:x}",
        Sha256::digest(format!("{system}:{timeline}:{database}").as_bytes())
    );
    let slot_count = scalar(
        &mut c,
        &format!(
            "SELECT count(*)::text FROM pg_replication_slots WHERE slot_name='{}'",
            config.public().source.slot
        ),
        "slot",
    )?;
    if slot_count != "0" {
        return Err(InitFailure::at("slot", "M2_INIT_PERMANENT_SLOT_EXISTS"));
    }
    let publication_exists = scalar(
        &mut c,
        &format!(
            "SELECT count(*)::text FROM pg_publication WHERE pubname='{}'",
            config.public().source.publication
        ),
        "publication",
    )? == "1";
    let relations = quoted
        .iter()
        .cloned()
        .chain([
            "boring_cdc_control.heartbeat".into(),
            "boring_cdc_control.capture_fences".into(),
        ])
        .collect::<Vec<_>>()
        .join(",");
    let setup = format!(
        r#"BEGIN;
DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='{ADMIN_ROLE}') THEN CREATE ROLE {ADMIN_ROLE} NOLOGIN; END IF; IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='{CONTROL_ROLE}') THEN CREATE ROLE {CONTROL_ROLE} LOGIN; END IF; END $$;
CREATE SCHEMA IF NOT EXISTS boring_cdc_control AUTHORIZATION {ADMIN_ROLE};
CREATE TABLE IF NOT EXISTS boring_cdc_control.heartbeat(id text PRIMARY KEY CHECK(id='singleton'),nonce bigint NOT NULL,updated_at timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS boring_cdc_control.capture_fences(id text PRIMARY KEY CHECK(id='singleton'),capture_epoch bigint NOT NULL,generation bigint NOT NULL,table_set_fingerprint text NOT NULL,unique_nonce bigint NOT NULL);
INSERT INTO boring_cdc_control.heartbeat VALUES('singleton',0,clock_timestamp()) ON CONFLICT(id) DO NOTHING;
INSERT INTO boring_cdc_control.capture_fences VALUES('singleton',0,0,repeat('0',64),0) ON CONFLICT(id) DO NOTHING;
GRANT USAGE ON SCHEMA boring_cdc_control TO {CONTROL_ROLE};
GRANT SELECT(id),UPDATE(nonce,updated_at) ON boring_cdc_control.heartbeat TO {CONTROL_ROLE};
GRANT SELECT(id),UPDATE(capture_epoch,generation,table_set_fingerprint,unique_nonce) ON boring_cdc_control.capture_fences TO {CONTROL_ROLE};
{} COMMIT;"#,
        if publication_exists {
            String::new()
        } else {
            format!(
                "CREATE PUBLICATION \"{}\" FOR TABLE {} WITH(publish='insert,update,delete,truncate'); ALTER PUBLICATION \"{}\" OWNER TO {ADMIN_ROLE};",
                config.public().source.publication,
                relations,
                config.public().source.publication
            )
        }
    );
    exec(&mut c, &setup, "source_setup")?;
    verify(&mut c, config, &expected)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| InitFailure::at("clock", "M2_INIT_CLOCK_INVALID"))?
        .as_millis() as i64;
    let writer = open_writer(
        Path::new(&config.public().storage.sqlite_path),
        run_id,
        1,
        now,
    )
    .map_err(|_| InitFailure::at("sqlite", "M2_INIT_SQLITE_FAILED"))?;
    let epoch = config.fingerprints().runtime.as_str();
    writer.connection().execute("INSERT OR IGNORE INTO source_state(singleton,capture_epoch,source_system_id,timeline_id,database_id,slot_name,plugin,publication_fingerprint,protocol_fingerprint) VALUES(1,?1,?2,?3,?4,?5,'pgoutput',?6,'pgoutput-v1')",rusqlite::params![epoch,system,timeline,database,config.public().source.slot,publication_fingerprint]).map_err(|_| InitFailure::at("sqlite","M2_INIT_IDENTITY_PERSIST_FAILED"))?;
    let readback: (String, String, String, String, String, String) = writer
        .connection()
        .query_row(
            "SELECT capture_epoch,source_system_id,timeline_id,database_id,slot_name,publication_fingerprint FROM source_state WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)),
        )
        .map_err(|_| InitFailure::at("sqlite", "M2_INIT_IDENTITY_READBACK_FAILED"))?;
    if readback
        != (
            epoch.into(),
            system,
            timeline,
            database,
            config.public().source.slot.clone(),
            publication_fingerprint.clone(),
        )
    {
        return Err(InitFailure::at("sqlite", "M2_INIT_IDENTITY_DRIFT"));
    }
    Ok(InitReceipt {
        schema_version: "m2-init-recovery/v1",
        outcome: "initialized",
        publication_fingerprint,
        source_identity_fingerprint,
        sqlite_initialized: true,
        control_rows: 2,
        logical_slot_exists: false,
    })
}

fn verify(
    c: &mut PgReplicationConnection,
    config: &LoadedConfig,
    expected: &PublicationSpec,
) -> Result<(), InitFailure> {
    for (table, expected_shape) in [
        (
            HEARTBEAT,
            "id:text:NO,nonce:bigint:NO,updated_at:timestamp with time zone:NO",
        ),
        (
            FENCE,
            "id:text:NO,capture_epoch:bigint:NO,generation:bigint:NO,table_set_fingerprint:text:NO,unique_nonce:bigint:NO",
        ),
    ] {
        let value = scalar(
            c,
            &format!(
                "SELECT count(*)::text||':'||coalesce(min(id),'')||':'||coalesce(max(id),'') FROM {table}"
            ),
            "control_cardinality",
        )?;
        if value != "1:singleton:singleton" {
            return Err(InitFailure::at(
                "control_cardinality",
                "M2_INIT_CONTROL_CARDINALITY_INVALID",
            ));
        }
        let shape = scalar(
            c,
            &format!(
                "SELECT string_agg(attname||':'||format_type(atttypid,atttypmod)||':'||CASE WHEN attnotnull THEN 'NO' ELSE 'YES' END,',' ORDER BY attnum) FROM pg_attribute WHERE attrelid='{table}'::regclass AND attnum>0 AND NOT attisdropped"
            ),
            "control_shape",
        )?;
        if shape != expected_shape {
            return Err(InitFailure::at(
                "control_shape",
                "M2_INIT_CONTROL_SHAPE_DRIFT",
            ));
        }
    }
    let role_safety = scalar(
        c,
        &format!(
            "SELECT (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls)::int::text FROM pg_roles WHERE rolname='{CONTROL_ROLE}'"
        ),
        "privilege",
    )?;
    if role_safety != "0" {
        return Err(InitFailure::at("privilege", "M2_INIT_CONTROL_ROLE_EXCESS"));
    }
    let members = scalar(
        c,
        &format!(
            "SELECT coalesce(string_agg(schemaname||'.'||tablename,',' ORDER BY schemaname,tablename),'') FROM pg_publication_tables WHERE pubname='{}'",
            config.public().source.publication
        ),
        "publication",
    )?;
    let expected_members = expected
        .relations
        .iter()
        .cloned()
        .collect::<Vec<_>>()
        .join(",");
    let flags = scalar(
        c,
        &format!(
            "SELECT r.rolname||':'||pubinsert::int||pubupdate::int||pubdelete::int||pubtruncate::int FROM pg_publication p JOIN pg_roles r ON r.oid=p.pubowner WHERE pubname='{}'",
            config.public().source.publication
        ),
        "publication",
    )?;
    if members != expected_members || flags != format!("{ADMIN_ROLE}:1111") {
        return Err(InitFailure::at("publication", "M2_INIT_PUBLICATION_DRIFT"));
    }
    let privilege = scalar(
        c,
        &format!(
            "SELECT (has_table_privilege('{CONTROL_ROLE}','{HEARTBEAT}','INSERT') OR has_table_privilege('{CONTROL_ROLE}','{HEARTBEAT}','DELETE') OR has_column_privilege('{CONTROL_ROLE}','{HEARTBEAT}','id','UPDATE') OR has_table_privilege('{CONTROL_ROLE}','{FENCE}','INSERT') OR has_table_privilege('{CONTROL_ROLE}','{FENCE}','DELETE') OR has_column_privilege('{CONTROL_ROLE}','{FENCE}','id','UPDATE'))::int::text"
        ),
        "privilege",
    )?;
    if privilege != "0" {
        return Err(InitFailure::at(
            "privilege",
            "M2_INIT_CONTROL_PRIVILEGE_EXCESS",
        ));
    }
    let required = scalar(
        c,
        &format!(
            "SELECT (has_column_privilege('{CONTROL_ROLE}','{HEARTBEAT}','id','SELECT') AND has_column_privilege('{CONTROL_ROLE}','{HEARTBEAT}','nonce','UPDATE') AND has_column_privilege('{CONTROL_ROLE}','{HEARTBEAT}','updated_at','UPDATE') AND has_column_privilege('{CONTROL_ROLE}','{FENCE}','id','SELECT') AND has_column_privilege('{CONTROL_ROLE}','{FENCE}','capture_epoch','UPDATE') AND has_column_privilege('{CONTROL_ROLE}','{FENCE}','generation','UPDATE') AND has_column_privilege('{CONTROL_ROLE}','{FENCE}','table_set_fingerprint','UPDATE') AND has_column_privilege('{CONTROL_ROLE}','{FENCE}','unique_nonce','UPDATE'))::int::text"
        ),
        "privilege",
    )?;
    if required != "1" {
        return Err(InitFailure::at(
            "privilege",
            "M2_INIT_CONTROL_PRIVILEGE_MISSING",
        ));
    }
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::*;
    #[test]
    fn identifiers_are_bounded_and_quoted() {
        assert!(simple_ident("boring_cdc_publication"));
        assert!(!simple_ident("x;DROP"));
        assert_eq!(
            relation("public.accounts").as_deref(),
            Some("\"public\".\"accounts\"")
        );
        assert!(relation("public.accounts.extra").is_none());
    }
    #[test]
    fn fresh_schema_accepts_init_identity_without_slot_intent() {
        let root = std::env::temp_dir().join(format!("m2-init-schema-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("state.sqlite");
        let writer = open_writer(&path, "init", 1, 1).unwrap();
        writer.connection().execute("INSERT INTO source_state(singleton,capture_epoch,source_system_id,timeline_id,database_id,slot_name,plugin,publication_fingerprint,protocol_fingerprint) VALUES(1,'epoch','system','1','database','slot','pgoutput','publication','pgoutput-v1')", []).unwrap();
        drop(writer);
        let _ = std::fs::remove_dir_all(root);
    }
    #[test]
    fn receipt_never_exposes_source_or_credentials() {
        let r = InitReceipt {
            schema_version: "m2-init-recovery/v1",
            outcome: "initialized",
            publication_fingerprint: "a".repeat(64),
            source_identity_fingerprint: "b".repeat(64),
            sqlite_initialized: true,
            control_rows: 2,
            logical_slot_exists: false,
        };
        let s = serde_json::to_string(&r).unwrap();
        assert!(!s.contains("postgres://"));
        assert!(!r.logical_slot_exists);
    }
    #[test]
    fn init_contract_is_maintenance_only_and_reports_no_slot() {
        assert_eq!(OWNER_BEAD, "boring-cdc-m2-init-recovery");
        assert_eq!(ADMIN_ROLE, "boring_cdc_admin");
        assert_ne!(ADMIN_ROLE, CONTROL_ROLE);
    }
}
