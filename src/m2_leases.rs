//! Generation-scoped leases and pre-side-effect fencing.
//!
//! SQLite CAS protects local state. External effects additionally receive a namespace derived from
//! the full immutable lease identity, so a completion which becomes stale after dispatch is retained
//! as a non-live artifact and can never advance a checkpoint or selector.

use crate::m2_journal::sha256;
use crate::m2_schema::WriterConnection;
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaseIdentity {
    pub lease_id: String,
    pub destination_id: String,
    pub capture_epoch: String,
    pub anchor_id: Option<String>,
    pub generation: u64,
    pub configuration_fingerprint: String,
    pub run_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaseToken {
    pub identity: LeaseIdentity,
    pub expires_mono_ms: u64,
    pub revision: u64,
    incarnation: u64,
    external_namespace: String,
}
impl LeaseToken {
    pub fn external_namespace(&self) -> &str {
        &self.external_namespace
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LeaseError {
    Invalid(&'static str),
    Stale,
    Conflict,
    Sqlite(String),
    External(String),
}
impl std::fmt::Display for LeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for LeaseError {}
impl From<rusqlite::Error> for LeaseError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Sqlite(value.to_string())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SideEffectOutcome<T> {
    Live(T),
    StaleArtifact(T),
}

pub trait SideEffectAdapter {
    type Artifact;
    /// Persist an immutable candidate artifact. Adapters must never mutate a live selector here.
    fn apply_candidate(&mut self, external_namespace: &str) -> Result<Self::Artifact, String>;
}

fn nonempty(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256
}
fn as_i64(value: u64) -> Result<i64, LeaseError> {
    i64::try_from(value).map_err(|_| LeaseError::Invalid("integer exceeds SQLite range"))
}
fn framed(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(&(value.len() as u64).to_be_bytes());
    target.extend_from_slice(value);
}

/// Opaque, collision-resistant namespace. No raw source or destination identifier is exposed.
pub fn derive_external_namespace(
    identity: &LeaseIdentity,
    incarnation: u64,
) -> Result<String, LeaseError> {
    if !nonempty(&identity.destination_id)
        || !nonempty(&identity.capture_epoch)
        || !nonempty(&identity.configuration_fingerprint)
        || !nonempty(&identity.run_id)
        || identity.generation == 0
        || identity.anchor_id.as_ref().is_some_and(|v| !nonempty(v))
    {
        return Err(LeaseError::Invalid("invalid lease identity"));
    }
    let mut encoded = b"boring-cdc-side-effect-namespace-v1".to_vec();
    for value in [
        identity.lease_id.as_bytes(),
        identity.destination_id.as_bytes(),
        identity.capture_epoch.as_bytes(),
        &identity.generation.to_be_bytes(),
        identity.configuration_fingerprint.as_bytes(),
        identity.anchor_id.as_deref().unwrap_or("").as_bytes(),
        identity.run_id.as_bytes(),
        &incarnation.to_be_bytes(),
    ] {
        framed(&mut encoded, value);
    }
    Ok(format!("generation-{}", sha256(&encoded)))
}

fn valid_in(
    transaction: &Transaction<'_>,
    token: &LeaseToken,
    now: u64,
) -> Result<bool, LeaseError> {
    if derive_external_namespace(&token.identity, token.incarnation)? != token.external_namespace {
        return Ok(false);
    }
    let now = as_i64(now)?;
    let expires = as_i64(token.expires_mono_ms)?;
    let revision = as_i64(token.revision)?;
    let generation = as_i64(token.identity.generation)?;
    let found: Option<i64> = transaction
        .query_row(
            "SELECT 1 FROM destination_generation_leases l
             JOIN destinations d ON d.destination_id=l.destination_id
             JOIN runtime_ownership r ON r.run_id=l.run_id
             WHERE l.lease_id=?1 AND l.destination_id=?2 AND l.capture_epoch=?3
               AND l.anchor_id IS ?4 AND l.generation=?5 AND l.configuration_fingerprint=?6
               AND l.run_id=?7 AND l.expires_mono_ms=?8 AND l.revision=?9 AND l.state='held'
               AND l.expires_mono_ms>?10
               AND d.capture_epoch=l.capture_epoch AND d.generation=l.generation
               AND d.configuration_fingerprint=l.configuration_fingerprint
               AND r.state='held' AND r.ownership_deadline_mono_ms>?10",
            params![
                token.identity.lease_id,
                token.identity.destination_id,
                token.identity.capture_epoch,
                token.identity.anchor_id,
                generation,
                token.identity.configuration_fingerprint,
                token.identity.run_id,
                expires,
                revision,
                now
            ],
            |row| row.get(0),
        )
        .optional()?;
    Ok(found.is_some())
}

pub fn acquire(
    writer: &mut WriterConnection,
    identity: LeaseIdentity,
    now_mono_ms: u64,
    ttl_ms: u64,
) -> Result<LeaseToken, LeaseError> {
    if !nonempty(&identity.lease_id) || ttl_ms == 0 {
        return Err(LeaseError::Invalid("lease id and positive TTL required"));
    }
    let generation = as_i64(identity.generation)?;
    let now = as_i64(now_mono_ms)?;
    let expires_u64 = now_mono_ms
        .checked_add(ttl_ms)
        .ok_or(LeaseError::Invalid("lease expiry overflow"))?;
    let expires = as_i64(expires_u64)?;
    let transaction = writer
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let destination: Option<i64> = transaction
        .query_row(
            "SELECT 1 FROM destinations WHERE destination_id=?1 AND capture_epoch=?2
             AND generation=?3 AND configuration_fingerprint=?4",
            params![
                identity.destination_id,
                identity.capture_epoch,
                generation,
                identity.configuration_fingerprint
            ],
            |row| row.get(0),
        )
        .optional()?;
    let owner: Option<i64> = transaction
        .query_row(
            "SELECT 1 FROM runtime_ownership WHERE run_id=?1 AND state='held'
             AND ownership_deadline_mono_ms>?2",
            params![identity.run_id, now],
            |row| row.get(0),
        )
        .optional()?;
    let anchor_ok = match &identity.anchor_id {
        None => true,
        Some(anchor) => transaction
            .query_row(
                "SELECT 1 FROM bootstrap_anchors WHERE anchor_id=?1 AND capture_epoch=?2
                 AND generation=?3 AND state='complete'",
                params![anchor, identity.capture_epoch, generation],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .is_some(),
    };
    if destination.is_none() || owner.is_none() || !anchor_ok {
        return Err(LeaseError::Stale);
    }
    transaction.execute(
        "UPDATE destination_generation_leases SET state='fenced',revision=revision+1
         WHERE destination_id=?1 AND state='held'
           AND (capture_epoch<>?2 OR generation<>?3 OR configuration_fingerprint<>?4)",
        params![
            identity.destination_id,
            identity.capture_epoch,
            generation,
            identity.configuration_fingerprint
        ],
    )?;
    // The schema intentionally keeps one current lease row per generation. Once a terminal row has
    // no live authority, replace it atomically with a new immutable lease identity; old tokens can
    // no longer validate and their unique candidate namespaces remain non-live reconciliation data.
    transaction.execute(
        "UPDATE destination_generation_leases SET state='expired',revision=revision+1
         WHERE destination_id=?1 AND capture_epoch=?2 AND generation=?3
           AND state='held' AND expires_mono_ms<=?4",
        params![
            identity.destination_id,
            identity.capture_epoch,
            generation,
            now
        ],
    )?;
    let prior_revision: Option<i64> = transaction
        .query_row(
            "SELECT revision FROM destination_generation_leases
         WHERE destination_id=?1 AND capture_epoch=?2 AND generation=?3
           AND state IN ('fenced','expired','released')",
            params![identity.destination_id, identity.capture_epoch, generation],
            |row| row.get(0),
        )
        .optional()?;
    let incarnation = match prior_revision {
        Some(value) => value
            .checked_add(1)
            .ok_or(LeaseError::Invalid("lease incarnation overflow"))?,
        None => 0,
    };
    transaction.execute(
        "DELETE FROM destination_generation_leases
         WHERE destination_id=?1 AND capture_epoch=?2 AND generation=?3
           AND state IN ('fenced','expired','released')",
        params![identity.destination_id, identity.capture_epoch, generation],
    )?;
    let namespace = derive_external_namespace(
        &identity,
        u64::try_from(incarnation)
            .map_err(|_| LeaseError::Invalid("negative lease incarnation"))?,
    )?;
    let inserted = transaction.execute(
        "INSERT OR IGNORE INTO destination_generation_leases
         (lease_id,destination_id,capture_epoch,anchor_id,generation,configuration_fingerprint,run_id,expires_mono_ms,state,revision)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'held',?9)",
        params![identity.lease_id, identity.destination_id, identity.capture_epoch, identity.anchor_id, generation, identity.configuration_fingerprint, identity.run_id, expires, incarnation],
    )?;
    if inserted != 1 {
        return Err(LeaseError::Conflict);
    }
    transaction.commit()?;
    Ok(LeaseToken {
        identity,
        expires_mono_ms: expires_u64,
        revision: u64::try_from(incarnation)
            .map_err(|_| LeaseError::Invalid("negative lease incarnation"))?,
        incarnation: u64::try_from(incarnation)
            .map_err(|_| LeaseError::Invalid("negative lease incarnation"))?,
        external_namespace: namespace,
    })
}

pub fn renew(
    writer: &mut WriterConnection,
    token: &LeaseToken,
    now_mono_ms: u64,
    ttl_ms: u64,
) -> Result<LeaseToken, LeaseError> {
    if ttl_ms == 0 {
        return Err(LeaseError::Invalid("positive TTL required"));
    }
    let expires_u64 = now_mono_ms
        .checked_add(ttl_ms)
        .ok_or(LeaseError::Invalid("lease expiry overflow"))?;
    let transaction = writer
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    if !valid_in(&transaction, token, now_mono_ms)? {
        return Err(LeaseError::Stale);
    }
    let changed = transaction.execute(
        "UPDATE destination_generation_leases SET expires_mono_ms=?1,revision=revision+1
         WHERE lease_id=?2 AND revision=?3 AND state='held'",
        params![
            as_i64(expires_u64)?,
            token.identity.lease_id,
            as_i64(token.revision)?
        ],
    )?;
    if changed != 1 {
        return Err(LeaseError::Stale);
    }
    transaction.commit()?;
    Ok(LeaseToken {
        identity: token.identity.clone(),
        expires_mono_ms: expires_u64,
        revision: token.revision + 1,
        incarnation: token.incarnation,
        external_namespace: token.external_namespace().to_owned(),
    })
}

pub fn expire_due(writer: &mut WriterConnection, now_mono_ms: u64) -> Result<usize, LeaseError> {
    Ok(writer.connection().execute(
        "UPDATE destination_generation_leases SET state='expired',revision=revision+1
         WHERE state='held' AND expires_mono_ms<=?1",
        [as_i64(now_mono_ms)?],
    )?)
}

pub fn release(
    writer: &mut WriterConnection,
    token: &LeaseToken,
    now_mono_ms: u64,
) -> Result<(), LeaseError> {
    let transaction = writer
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    if !valid_in(&transaction, token, now_mono_ms)? {
        return Err(LeaseError::Stale);
    }
    let changed = transaction.execute(
        "UPDATE destination_generation_leases SET state='released',revision=revision+1
         WHERE lease_id=?1 AND revision=?2 AND state='held'",
        params![token.identity.lease_id, as_i64(token.revision)?],
    )?;
    if changed != 1 {
        return Err(LeaseError::Stale);
    }
    transaction.commit()?;
    Ok(())
}

/// Validate immediately before dispatch, then validate again after dispatch. A stale completion is
/// returned as an artifact for reconciliation but is never eligible for local completion CAS.
pub fn dispatch<A, C>(
    writer: &mut WriterConnection,
    token: &LeaseToken,
    clock_mono_ms: &mut C,
    adapter: &mut A,
) -> Result<SideEffectOutcome<A::Artifact>, LeaseError>
where
    A: SideEffectAdapter,
    C: FnMut() -> u64,
{
    let before_now = clock_mono_ms();
    let before = writer
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    if !valid_in(&before, token, before_now)? {
        return Err(LeaseError::Stale);
    }
    before.commit()?;
    let namespace = derive_external_namespace(&token.identity, token.incarnation)?;
    let artifact = adapter
        .apply_candidate(&namespace)
        .map_err(LeaseError::External)?;
    let after_now = clock_mono_ms();
    let after = writer
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let live = valid_in(&after, token, after_now)?;
    after.commit()?;
    Ok(if live {
        SideEffectOutcome::Live(artifact)
    } else {
        SideEffectOutcome::StaleArtifact(artifact)
    })
}

/// Run a local completion only while the exact lease tuple remains current. The callback and CAS
/// execute in one IMMEDIATE transaction, which is the checkpoint/promotion boundary.
pub fn complete_local<T, F, C>(
    writer: &mut WriterConnection,
    token: &LeaseToken,
    clock_mono_ms: &mut C,
    completion: F,
) -> Result<T, LeaseError>
where
    F: FnOnce(&Transaction<'_>) -> Result<T, LeaseError>,
    C: FnMut() -> u64,
{
    let before_now = clock_mono_ms();
    let transaction = writer
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    if !valid_in(&transaction, token, before_now)? {
        return Err(LeaseError::Stale);
    }
    let result = completion(&transaction)?;
    let after_now = clock_mono_ms();
    if !valid_in(&transaction, token, after_now)? {
        return Err(LeaseError::Stale);
    }
    transaction.commit()?;
    Ok(result)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PromotionIntent {
    pub intent_id: String,
    pub promotion_fence: u64,
    pub external_namespace: String,
}

/// Allocate the increasing local fence and persist an immutable promotion intent. M2 deliberately
/// does not execute an external selector switch.
pub fn prepare_promotion(
    writer: &mut WriterConnection,
    token: &LeaseToken,
    now_mono_ms: u64,
    intent_id: &str,
    candidate_generation: u64,
    expected_selector_digest: &str,
) -> Result<PromotionIntent, LeaseError> {
    if !nonempty(intent_id)
        || !nonempty(expected_selector_digest)
        || candidate_generation == 0
        || token.identity.anchor_id.is_none()
    {
        return Err(LeaseError::Invalid("invalid promotion intent"));
    }
    let transaction = writer
        .connection_mut()
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    if !valid_in(&transaction, token, now_mono_ms)? {
        return Err(LeaseError::Stale);
    }
    let high: i64 = transaction.query_row(
        "SELECT highest_external_fence FROM destinations WHERE destination_id=?1",
        [&token.identity.destination_id],
        |row| row.get(0),
    )?;
    let fence = high
        .checked_add(1)
        .ok_or(LeaseError::Invalid("fence overflow"))?;
    transaction.execute(
        "INSERT INTO destination_promotion_intents
         (intent_id,destination_id,capture_epoch,old_generation,candidate_generation,anchor_id,
          configuration_fingerprint,promotion_fence,expected_selector_digest,state,revision)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,'prepared',0)",
        params![
            intent_id,
            token.identity.destination_id,
            token.identity.capture_epoch,
            as_i64(token.identity.generation)?,
            as_i64(candidate_generation)?,
            token.identity.anchor_id,
            token.identity.configuration_fingerprint,
            fence,
            expected_selector_digest
        ],
    )?;
    transaction.commit()?;
    Ok(PromotionIntent {
        intent_id: intent_id.to_owned(),
        promotion_fence: u64::try_from(fence).map_err(|_| LeaseError::Invalid("negative fence"))?,
        external_namespace: token.external_namespace().to_owned(),
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::m2_schema::open_writer;
    use rusqlite::Connection;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(1);
    fn writer() -> (WriterConnection, PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "boring-cdc-m2-leases-{}-{}.sqlite",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let writer = open_writer(&path, "writer", 1, 0).unwrap();
        writer.connection().execute("INSERT INTO runtime_ownership(run_id,backend_pid,connection_nonce,ownership_deadline_mono_ms,state,connection_generation) VALUES('run-a',1,'nonce-a',1000,'held',1)", []).unwrap();
        writer.connection().execute("INSERT INTO destinations(destination_id,kind,configuration_fingerprint,capture_epoch,generation) VALUES('dest-a','archive','config-a','epoch-a',1)", []).unwrap();
        (writer, path)
    }
    fn identity(generation: u64) -> LeaseIdentity {
        LeaseIdentity {
            lease_id: format!("lease-{generation}"),
            destination_id: "dest-a".into(),
            capture_epoch: "epoch-a".into(),
            anchor_id: None,
            generation,
            configuration_fingerprint: "config-a".into(),
            run_id: "run-a".into(),
        }
    }
    fn cleanup(path: PathBuf) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }

    #[test]
    fn acquire_renew_expire_and_stale_cas_are_exact() {
        let (mut writer, path) = writer();
        let first = acquire(&mut writer, identity(1), 10, 20).unwrap();
        assert_eq!(
            acquire(&mut writer, identity(1), 10, 20),
            Err(LeaseError::Conflict)
        );
        let renewed = renew(&mut writer, &first, 20, 30).unwrap();
        assert_eq!(renew(&mut writer, &first, 20, 30), Err(LeaseError::Stale));
        assert_eq!(expire_due(&mut writer, 49).unwrap(), 0);
        assert_eq!(expire_due(&mut writer, 50).unwrap(), 1);
        assert_eq!(renew(&mut writer, &renewed, 50, 30), Err(LeaseError::Stale));
        let mut replacement_identity = identity(1);
        replacement_identity.lease_id = "lease-replacement".into();
        let replacement = acquire(&mut writer, replacement_identity, 50, 30).unwrap();
        assert_ne!(
            replacement.external_namespace(),
            renewed.external_namespace()
        );
        assert_eq!(renew(&mut writer, &renewed, 51, 30), Err(LeaseError::Stale));
        cleanup(path);
    }

    #[test]
    fn released_generation_can_be_reacquired_by_successor_runtime() {
        let (mut writer, path) = writer();
        let old = acquire(&mut writer, identity(1), 10, 100).unwrap();
        release(&mut writer, &old, 11).unwrap();
        let mut successor = identity(1);
        successor.lease_id = "lease-successor".into();
        let current = acquire(&mut writer, successor, 12, 100).unwrap();
        assert_ne!(old.external_namespace(), current.external_namespace());
        assert_eq!(renew(&mut writer, &old, 13, 100), Err(LeaseError::Stale));
        let current = renew(&mut writer, &current, 13, 100).unwrap();
        release(&mut writer, &current, 14).unwrap();
        let recycled_identity = identity(1);
        let recycled = acquire(&mut writer, recycled_identity, 15, 100).unwrap();
        assert_ne!(old.external_namespace(), recycled.external_namespace());
        assert_eq!(renew(&mut writer, &old, 16, 100), Err(LeaseError::Stale));
        cleanup(path);
    }

    struct Fake {
        namespaces: Vec<String>,
    }
    impl SideEffectAdapter for Fake {
        type Artifact = String;
        fn apply_candidate(&mut self, namespace: &str) -> Result<String, String> {
            self.namespaces.push(namespace.to_owned());
            Ok(format!("artifact:{namespace}"))
        }
    }

    #[test]
    fn pause_and_detach_prevent_dispatch_and_checkpoint() {
        let (mut writer, path) = writer();
        let token = acquire(&mut writer, identity(1), 10, 100).unwrap();
        writer
            .connection()
            .execute(
                "UPDATE destination_generation_leases SET state='released',revision=revision+1",
                [],
            )
            .unwrap();
        let mut fake = Fake { namespaces: vec![] };
        assert_eq!(
            dispatch(&mut writer, &token, &mut || 11, &mut fake),
            Err(LeaseError::Stale)
        );
        assert!(fake.namespaces.is_empty());
        let changed = complete_local(&mut writer, &token, &mut || 11, |tx| {
            Ok(tx.execute("UPDATE destinations SET revision=revision+1", [])?)
        });
        assert_eq!(changed, Err(LeaseError::Stale));
        cleanup(path);
    }

    #[test]
    fn same_generation_identity_cycle_permanently_fences_old_worker() {
        let (mut writer, path) = writer();
        // Promotion needs an anchored lease. The predecessor-owned anchor triggers are disabled only
        // to build the deterministic fixture, as in the dedicated promotion test below.
        writer.connection().execute_batch("DROP TRIGGER complete_anchor_requires_fence; DROP TRIGGER complete_anchor_update_requires_fence; DROP TRIGGER anchor_v2_update; INSERT INTO bootstrap_anchors(anchor_id,capture_epoch,generation,start_seq,snapshot_boundary_lsn,snapshot_complete_seq,post_copy_fence_nonce,post_copy_fence_lsn,post_copy_fence_seq,table_set_fingerprint,snapshot_schema_fingerprints,state,expires_at) VALUES('anchor-a','epoch-a',1,0,'0000000000000001',0,'nonce','0000000000000001',0,'tables','[\"schema\"]','building','never'); UPDATE bootstrap_anchors SET state='complete' WHERE anchor_id='anchor-a';").unwrap();
        let mut old_identity = identity(1);
        old_identity.anchor_id = Some("anchor-a".into());
        let old = acquire(&mut writer, old_identity, 10, 100).unwrap();

        writer.connection().execute("UPDATE destinations SET capture_epoch='epoch-b',configuration_fingerprint='config-b',revision=revision+1 WHERE destination_id='dest-a'", []).unwrap();
        let mut replacement_identity = identity(1);
        replacement_identity.capture_epoch = "epoch-b".into();
        replacement_identity.configuration_fingerprint = "config-b".into();
        replacement_identity.lease_id = "lease-b".into();
        let replacement = acquire(&mut writer, replacement_identity, 11, 100).unwrap();
        assert_ne!(old.external_namespace(), replacement.external_namespace());

        // Cycling the destination row back to A must not revive the still-unexpired A token.
        writer.connection().execute("UPDATE destinations SET capture_epoch='epoch-a',configuration_fingerprint='config-a',revision=revision+1 WHERE destination_id='dest-a'", []).unwrap();
        let mut fake = Fake { namespaces: vec![] };
        assert_eq!(
            dispatch(&mut writer, &old, &mut || 12, &mut fake),
            Err(LeaseError::Stale)
        );
        assert!(fake.namespaces.is_empty());
        let revision_before: i64 = writer
            .connection()
            .query_row(
                "SELECT revision FROM destinations WHERE destination_id='dest-a'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            complete_local(&mut writer, &old, &mut || 12, |tx| {
                tx.execute(
                    "UPDATE destinations SET revision=revision+1 WHERE destination_id='dest-a'",
                    [],
                )?;
                Ok(())
            }),
            Err(LeaseError::Stale)
        );
        let revision_after: i64 = writer
            .connection()
            .query_row(
                "SELECT revision FROM destinations WHERE destination_id='dest-a'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(revision_before, revision_after);
        assert_eq!(
            prepare_promotion(&mut writer, &old, 12, "cycled-intent", 2, "selector-a"),
            Err(LeaseError::Stale)
        );
        let intents: i64 = writer
            .connection()
            .query_row(
                "SELECT COUNT(*) FROM destination_promotion_intents",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(intents, 0);
        cleanup(path);
    }

    #[test]
    fn reseed_generation_and_configuration_fence_old_workers() {
        let (mut writer, path) = writer();
        let old = acquire(&mut writer, identity(1), 10, 100).unwrap();
        writer.connection().execute("UPDATE destinations SET capture_epoch='epoch-b',generation=2,configuration_fingerprint='config-b',revision=revision+1", []).unwrap();
        let mut fake = Fake { namespaces: vec![] };
        assert_eq!(
            dispatch(&mut writer, &old, &mut || 11, &mut fake),
            Err(LeaseError::Stale)
        );
        assert_eq!(
            complete_local(&mut writer, &old, &mut || 11, |_| Ok(())),
            Err(LeaseError::Stale)
        );
        cleanup(path);
    }

    struct RacingFake {
        db: PathBuf,
        namespaces: Vec<String>,
    }
    impl SideEffectAdapter for RacingFake {
        type Artifact = String;
        fn apply_candidate(&mut self, namespace: &str) -> Result<String, String> {
            self.namespaces.push(namespace.to_owned());
            let db = Connection::open(&self.db).map_err(|e| e.to_string())?;
            db.execute(
                "UPDATE destination_generation_leases SET state='fenced',revision=revision+1",
                [],
            )
            .map_err(|e| e.to_string())?;
            Ok("stale-object".into())
        }
    }

    #[test]
    fn two_runtime_race_keeps_stale_artifact_out_of_live_state() {
        let (mut writer, path) = writer();
        let token = acquire(&mut writer, identity(1), 10, 100).unwrap();
        let mut fake = RacingFake {
            db: path.clone(),
            namespaces: vec![],
        };
        assert_eq!(
            dispatch(&mut writer, &token, &mut || 11, &mut fake).unwrap(),
            SideEffectOutcome::StaleArtifact("stale-object".into())
        );
        assert_eq!(fake.namespaces, vec![token.external_namespace().to_owned()]);
        assert_eq!(
            complete_local(&mut writer, &token, &mut || 11, |_| Ok(())),
            Err(LeaseError::Stale)
        );
        cleanup(path);
    }

    #[test]
    fn expiry_during_effect_or_completion_is_stale_and_rolls_back() {
        let (mut writer, path) = writer();
        let token = acquire(&mut writer, identity(1), 10, 20).unwrap();
        let mut fake = Fake { namespaces: vec![] };
        let mut dispatch_times = [11_u64, 31].into_iter();
        assert!(matches!(
            dispatch(
                &mut writer,
                &token,
                &mut || dispatch_times.next().unwrap(),
                &mut fake
            ),
            Ok(SideEffectOutcome::StaleArtifact(_))
        ));
        let before: i64 = writer
            .connection()
            .query_row(
                "SELECT revision FROM destinations WHERE destination_id='dest-a'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let mut completion_times = [11_u64, 31].into_iter();
        assert_eq!(
            complete_local(
                &mut writer,
                &token,
                &mut || completion_times.next().unwrap(),
                |tx| {
                    tx.execute(
                        "UPDATE destinations SET revision=revision+1 WHERE destination_id='dest-a'",
                        [],
                    )?;
                    Ok(())
                }
            ),
            Err(LeaseError::Stale)
        );
        let after: i64 = writer
            .connection()
            .query_row(
                "SELECT revision FROM destinations WHERE destination_id='dest-a'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(before, after);
        cleanup(path);
    }

    #[test]
    fn tampered_namespace_ownership_loss_and_external_error_fail_closed() {
        let (mut writer, path) = writer();
        let token = acquire(&mut writer, identity(1), 10, 100).unwrap();
        let mut tampered = token.clone();
        tampered.external_namespace = "generation-attacker-selected".into();
        let mut fake = Fake { namespaces: vec![] };
        assert_eq!(
            dispatch(&mut writer, &tampered, &mut || 11, &mut fake),
            Err(LeaseError::Stale)
        );
        assert!(fake.namespaces.is_empty());
        struct Broken;
        impl SideEffectAdapter for Broken {
            type Artifact = ();
            fn apply_candidate(&mut self, _: &str) -> Result<(), String> {
                Err("redacted-adapter-code".into())
            }
        }
        assert_eq!(
            dispatch(&mut writer, &token, &mut || 11, &mut Broken),
            Err(LeaseError::External("redacted-adapter-code".into()))
        );
        writer.connection().execute("UPDATE runtime_ownership SET state='lost',revision=revision+1 WHERE run_id='run-a'", []).unwrap();
        assert_eq!(
            dispatch(&mut writer, &token, &mut || 11, &mut fake),
            Err(LeaseError::Stale)
        );
        cleanup(path);
    }

    #[test]
    fn invalid_identities_ttls_and_overflows_are_rejected() {
        let (mut writer, path) = writer();
        let mut invalid = identity(0);
        invalid.destination_id.clear();
        assert!(matches!(
            derive_external_namespace(&invalid, 0),
            Err(LeaseError::Invalid(_))
        ));
        assert!(matches!(
            acquire(&mut writer, identity(1), 10, 0),
            Err(LeaseError::Invalid(_))
        ));
        assert!(matches!(
            acquire(&mut writer, identity(1), u64::MAX, 1),
            Err(LeaseError::Invalid(_))
        ));
        let token = acquire(&mut writer, identity(1), 10, 20).unwrap();
        assert!(matches!(
            renew(&mut writer, &token, 11, 0),
            Err(LeaseError::Invalid(_))
        ));
        cleanup(path);
    }

    #[test]
    fn namespaces_are_epoch_generation_anchor_and_config_scoped() {
        let base = identity(1);
        let first = derive_external_namespace(&base, 0).unwrap();
        for changed in [
            LeaseIdentity {
                capture_epoch: "epoch-b".into(),
                ..base.clone()
            },
            LeaseIdentity {
                generation: 2,
                ..base.clone()
            },
            LeaseIdentity {
                anchor_id: Some("anchor-a".into()),
                ..base.clone()
            },
            LeaseIdentity {
                configuration_fingerprint: "config-b".into(),
                ..base.clone()
            },
        ] {
            assert_ne!(first, derive_external_namespace(&changed, 0).unwrap());
        }
        assert!(!first.contains("dest-a"));
    }

    #[test]
    fn promotion_intent_allocates_increasing_fence_and_stale_token_cannot_prepare() {
        let (mut writer, path) = writer();
        // Complete anchors are predecessor-owned and heavily proof-gated; this fixture inserts a
        // building anchor then marks it complete only after disabling predecessor triggers.
        writer.connection().execute_batch("DROP TRIGGER complete_anchor_requires_fence; DROP TRIGGER complete_anchor_update_requires_fence; DROP TRIGGER anchor_v2_update; INSERT INTO bootstrap_anchors(anchor_id,capture_epoch,generation,start_seq,snapshot_boundary_lsn,snapshot_complete_seq,post_copy_fence_nonce,post_copy_fence_lsn,post_copy_fence_seq,table_set_fingerprint,snapshot_schema_fingerprints,state,expires_at) VALUES('anchor-a','epoch-a',1,0,'0000000000000001',0,'nonce','0000000000000001',0,'tables','[\"schema\"]','building','never'); UPDATE bootstrap_anchors SET state='complete' WHERE anchor_id='anchor-a';").unwrap();
        let mut with_anchor = identity(1);
        with_anchor.anchor_id = Some("anchor-a".into());
        let token = acquire(&mut writer, with_anchor, 10, 100).unwrap();
        let p = prepare_promotion(&mut writer, &token, 11, "intent-a", 2, "selector-a").unwrap();
        assert_eq!(p.promotion_fence, 1);
        assert_eq!(
            writer
                .connection()
                .query_row(
                    "SELECT state FROM destination_promotion_intents WHERE intent_id='intent-a'",
                    [],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
            "prepared"
        );
        writer
            .connection()
            .execute(
                "UPDATE destination_generation_leases SET state='fenced',revision=revision+1",
                [],
            )
            .unwrap();
        assert_eq!(
            prepare_promotion(&mut writer, &token, 12, "intent-b", 3, "selector-b"),
            Err(LeaseError::Stale)
        );
        cleanup(path);
    }
}
