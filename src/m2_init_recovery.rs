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
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
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

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct PersistedPlan {
    plan_digest: String,
    token: String,
    config_fingerprint: String,
    expires_unix_ms: u64,
    state: String,
}

fn plan_path(store: &Path) -> PathBuf {
    store.with_extension("init-plan.json")
}
fn sync_parent(path: &Path) -> Result<(), InitFailure> {
    let parent = path
        .parent()
        .ok_or_else(|| InitFailure::at("plan", "M2_INIT_PLAN_IO"))?;
    std::fs::File::open(parent)
        .and_then(|f| f.sync_all())
        .map_err(|_| InitFailure::at("plan", "M2_INIT_PLAN_IO"))
}

/// Issue a CSPRNG-backed, expiring confirmation plan under the local ownership sidecar.
pub fn issue_plan(
    store: &Path,
    config_fingerprint: &str,
    now_ms: u64,
) -> Result<(String, String), InitFailure> {
    if let Some(parent) = store.parent() {
        std::fs::create_dir_all(parent).map_err(|_| InitFailure::at("plan", "M2_INIT_PLAN_IO"))?;
    }
    let lock_path = store.with_extension("ownership.lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&lock_path)
        .map_err(|_| InitFailure::at("ownership", "M2_INIT_OWNERSHIP_CONFLICT"))?;
    lock.try_lock()
        .map_err(|_| InitFailure::at("ownership", "M2_INIT_OWNERSHIP_CONFLICT"))?;
    let existing_path = plan_path(store);
    if existing_path.exists() {
        let state = std::fs::read(&existing_path)
            .ok()
            .and_then(|b| serde_json::from_slice::<PersistedPlan>(&b).ok())
            .map(|p| p.state);
        return Err(InitFailure::at(
            "reconciliation",
            if state.as_deref() == Some("executing") {
                "M2_INIT_RECONCILIATION_REQUIRED"
            } else {
                "M2_INIT_PLAN_ALREADY_ISSUED"
            },
        ));
    }
    let mut nonce = [0u8; 16];
    OpenOptions::new()
        .read(true)
        .open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut nonce))
        .map_err(|_| InitFailure::at("plan", "M2_INIT_RANDOM_UNAVAILABLE"))?;
    let token = nonce.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let plan_digest = format!(
        "{:x}",
        Sha256::digest(format!("m2-init/v1:{config_fingerprint}:{token}").as_bytes())
    );
    let plan = PersistedPlan {
        plan_digest: plan_digest.clone(),
        token: token.clone(),
        config_fingerprint: config_fingerprint.into(),
        expires_unix_ms: now_ms.saturating_add(300_000),
        state: "issued".into(),
    };
    let path = plan_path(store);
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&path)
        .map_err(|_| InitFailure::at("plan", "M2_INIT_PLAN_IO"))?;
    f.write_all(
        &serde_json::to_vec(&plan).map_err(|_| InitFailure::at("plan", "M2_INIT_PLAN_IO"))?,
    )
    .and_then(|_| f.sync_all())
    .map_err(|_| InitFailure::at("plan", "M2_INIT_PLAN_IO"))?;
    sync_parent(&path)?;
    Ok((plan_digest, token))
}

/// Holds the plan-file lock across confirmation, source effects, and durable completion.
pub struct PlanExecution {
    file: std::fs::File,
    pub plan_digest: String,
}
/// Atomically claims an issued plan, or resumes the exact same nonterminal plan after a crash.
pub fn consume_plan(
    store: &Path,
    config_fingerprint: &str,
    token: &str,
    now_ms: u64,
) -> Result<PlanExecution, InitFailure> {
    let path = plan_path(store);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|_| InitFailure::at("confirmation", "M2_INIT_CONFIRMATION_INVALID"))?;
    let meta = file
        .metadata()
        .map_err(|_| InitFailure::at("confirmation", "M2_INIT_CONFIRMATION_INVALID"))?;
    if meta.permissions().mode() & 0o077 != 0 || !meta.is_file() {
        return Err(InitFailure::at(
            "confirmation",
            "M2_INIT_CONFIRMATION_INVALID",
        ));
    }
    file.try_lock()
        .map_err(|_| InitFailure::at("ownership", "M2_INIT_OWNERSHIP_CONFLICT"))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|_| InitFailure::at("confirmation", "M2_INIT_CONFIRMATION_INVALID"))?;
    let mut plan: PersistedPlan = serde_json::from_slice(&bytes)
        .map_err(|_| InitFailure::at("confirmation", "M2_INIT_CONFIRMATION_INVALID"))?;
    if !matches!(plan.state.as_str(), "issued" | "executing")
        || plan.token != token
        || plan.config_fingerprint != config_fingerprint
        || (plan.state == "issued" && now_ms > plan.expires_unix_ms)
    {
        return Err(InitFailure::at(
            "confirmation",
            "M2_INIT_CONFIRMATION_INVALID",
        ));
    }
    plan.state = "executing".into();
    file.set_len(0)
        .and_then(|_| {
            use std::io::Seek;
            file.rewind()
        })
        .and_then(|_| file.write_all(&serde_json::to_vec(&plan).unwrap()))
        .and_then(|_| file.sync_all())
        .map_err(|_| InitFailure::at("confirmation", "M2_INIT_CONFIRMATION_INVALID"))?;
    Ok(PlanExecution {
        file,
        plan_digest: plan.plan_digest,
    })
}
pub fn complete_plan(execution: PlanExecution, store: &Path) -> Result<(), InitFailure> {
    execution
        .file
        .sync_all()
        .map_err(|_| InitFailure::at("confirmation", "M2_INIT_PLAN_COMPLETION_FAILED"))?;
    let path = plan_path(store);
    std::fs::remove_file(&path)
        .map_err(|_| InitFailure::at("confirmation", "M2_INIT_PLAN_COMPLETION_FAILED"))?;
    sync_parent(&path)
        .map_err(|_| InitFailure::at("confirmation", "M2_INIT_PLAN_COMPLETION_FAILED"))
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
    expected: &PublicationSpec,
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
    let live=connection.exec("SELECT system_identifier::text,(pg_control_checkpoint()).timeline_id::text,(SELECT oid::text FROM pg_database WHERE datname=current_database()) FROM pg_control_system()")
        .map_err(|_|InitFailure::at("ownership","M2_INIT_SOURCE_IDENTITY_INVALID"))?;
    let system_identifier: u64 = live
        .get_value(0, 0)
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| InitFailure::at("ownership", "M2_INIT_SOURCE_IDENTITY_INVALID"))?;
    let timeline: u32 = live
        .get_value(0, 1)
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| InitFailure::at("ownership", "M2_INIT_SOURCE_IDENTITY_INVALID"))?;
    let database_identity: u32 = live
        .get_value(0, 2)
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| InitFailure::at("ownership", "M2_INIT_SOURCE_IDENTITY_INVALID"))?;
    let definition = serde_json::to_vec(expected)
        .map_err(|_| InitFailure::at("ownership", "M2_INIT_SOURCE_IDENTITY_INVALID"))?;
    let identity = crate::m1_source_identity::SourceIdentity {
        system_identifier,
        timeline,
        database_identity,
        slot_name: config.public().source.slot.clone(),
        plugin: "pgoutput".into(),
        publication_fingerprint: crate::m1_source_identity::publication_fingerprint(
            &config.public().source.publication,
            &definition,
        ),
        protocol_fingerprint: crate::m1_source_identity::supported_protocol_fingerprint(),
    };
    identity
        .validate()
        .map_err(|_| InitFailure::at("ownership", "M2_INIT_SOURCE_IDENTITY_INVALID"))?;
    let key = identity.advisory_lock_key();
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
    let mut ownership = acquire(config, &expected, admin_dsn, run_id)?;
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
    // The advisory-lock connection itself performs every source read and mutation. If it dies,
    // PostgreSQL aborts its transaction while releasing the lock; no detached session can commit.
    let c = &mut ownership.source_session_mut().connection;
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
        c,
        "SELECT oid::text FROM pg_database WHERE datname=current_database()",
        "source_identity",
    )?;
    let source_identity_fingerprint = format!(
        "{:x}",
        Sha256::digest(format!("{system}:{timeline}:{database}").as_bytes())
    );
    let slot_count = scalar(
        c,
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
        c,
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
CREATE TABLE IF NOT EXISTS boring_cdc_control.heartbeat(id smallint PRIMARY KEY CHECK(id=1),nonce bigint NOT NULL,updated_at timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS boring_cdc_control.capture_fences(id smallint PRIMARY KEY CHECK(id=1),capture_epoch bigint NOT NULL,generation bigint NOT NULL,table_set_fingerprint bytea NOT NULL CHECK(octet_length(table_set_fingerprint)=32),unique_nonce bytea NOT NULL CHECK(octet_length(unique_nonce)=16));
INSERT INTO boring_cdc_control.heartbeat VALUES(1,0,'-infinity') ON CONFLICT(id) DO NOTHING;
INSERT INTO boring_cdc_control.capture_fences VALUES(1,0,0,decode(repeat('00',32),'hex'),decode(repeat('00',16),'hex')) ON CONFLICT(id) DO NOTHING;
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
    exec(c, &setup, "source_setup")?;
    verify(c, config, &expected)?;
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
            "id:smallint:NO,nonce:bigint:NO,updated_at:timestamp with time zone:NO",
        ),
        (
            FENCE,
            "id:smallint:NO,capture_epoch:bigint:NO,generation:bigint:NO,table_set_fingerprint:bytea:NO,unique_nonce:bytea:NO",
        ),
    ] {
        let value = scalar(
            c,
            &format!(
                "SELECT count(*)::text||':'||coalesce(min(id)::text,'')||':'||coalesce(max(id)::text,'') FROM {table}"
            ),
            "control_cardinality",
        )?;
        if value != "1:1:1" {
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
        let constraints = scalar(
            c,
            &format!(
                "SELECT ((SELECT array_agg(a.attname::text ORDER BY a.attname::text)=ARRAY['id'] FROM pg_index i JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey) WHERE i.indrelid='{table}'::regclass AND i.indisprimary) AND EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='{table}'::regclass AND contype='c' AND pg_get_expr(conbin,conrelid)='(id = 1)'))::int::text"
            ),
            "control_shape",
        )?;
        if constraints != "1" {
            return Err(InitFailure::at(
                "control_shape",
                "M2_INIT_CONTROL_KEY_NOT_IMMUTABLE",
            ));
        }
        if table == FENCE {
            let lengths = scalar(
                c,
                &format!(
                    "SELECT (EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='{FENCE}'::regclass AND contype='c' AND convalidated AND pg_get_expr(conbin,conrelid)='(octet_length(table_set_fingerprint) = 32)') AND EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='{FENCE}'::regclass AND contype='c' AND convalidated AND pg_get_expr(conbin,conrelid)='(octet_length(unique_nonce) = 16)'))::int::text"
                ),
                "control_shape",
            )?;
            if lengths != "1" {
                return Err(InitFailure::at(
                    "control_shape",
                    "M2_INIT_CONTROL_LENGTH_CONSTRAINT_INVALID",
                ));
            }
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
    let memberships = scalar(
        c,
        &format!(
            "SELECT count(*)::text FROM pg_auth_members WHERE roleid=(SELECT oid FROM pg_roles WHERE rolname='{CONTROL_ROLE}') OR member=(SELECT oid FROM pg_roles WHERE rolname='{CONTROL_ROLE}')"
        ),
        "privilege",
    )?;
    if memberships != "0" {
        return Err(InitFailure::at(
            "privilege",
            "M2_INIT_CONTROL_ROLE_MEMBERSHIP_EXCESS",
        ));
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
            "SELECT (has_schema_privilege('{CONTROL_ROLE}','boring_cdc_control','CREATE') OR has_table_privilege('{CONTROL_ROLE}','{HEARTBEAT}','SELECT,INSERT,DELETE,TRUNCATE,REFERENCES,TRIGGER') OR has_column_privilege('{CONTROL_ROLE}','{HEARTBEAT}','nonce','SELECT') OR has_column_privilege('{CONTROL_ROLE}','{HEARTBEAT}','updated_at','SELECT') OR has_column_privilege('{CONTROL_ROLE}','{HEARTBEAT}','id','UPDATE') OR has_table_privilege('{CONTROL_ROLE}','{FENCE}','SELECT,INSERT,DELETE,TRUNCATE,REFERENCES,TRIGGER') OR has_column_privilege('{CONTROL_ROLE}','{FENCE}','capture_epoch','SELECT') OR has_column_privilege('{CONTROL_ROLE}','{FENCE}','generation','SELECT') OR has_column_privilege('{CONTROL_ROLE}','{FENCE}','table_set_fingerprint','SELECT') OR has_column_privilege('{CONTROL_ROLE}','{FENCE}','unique_nonce','SELECT') OR has_column_privilege('{CONTROL_ROLE}','{FENCE}','id','UPDATE'))::int::text"
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
    fn confirmation_plan_is_random_expiring_and_one_shot() {
        let root = std::env::temp_dir().join(format!("m2-init-plan-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let store = root.join("state.sqlite");
        let (digest, token) = issue_plan(&store, "config", 100).unwrap();
        assert_eq!(token.len(), 32);
        let execution = consume_plan(&store, "config", &token, 200).unwrap();
        assert_eq!(execution.plan_digest, digest);
        assert!(matches!(
            consume_plan(&store, "config", &token, 201),
            Err(InitFailure {
                code: "M2_INIT_OWNERSHIP_CONFLICT",
                ..
            })
        ));
        drop(execution); // models process death after the intent became nonterminal
        let resumed = consume_plan(&store, "config", &token, 202).unwrap();
        assert_eq!(resumed.plan_digest, digest);
        complete_plan(resumed, &store).unwrap();
        let (_, next) = issue_plan(&store, "config", 300).unwrap();
        assert_ne!(token, next);
        let next_execution = consume_plan(&store, "config", &next, 301).unwrap();
        complete_plan(next_execution, &store).unwrap();
        let (_, expired) = issue_plan(&store, "config", 0).unwrap();
        assert!(matches!(
            consume_plan(&store, "config", &expired, 300_001),
            Err(InitFailure {
                code: "M2_INIT_CONFIRMATION_INVALID",
                ..
            })
        ));
        let _ = std::fs::remove_dir_all(root);
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
