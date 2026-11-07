//! Transactional artifact publication catalog.
//!
//! Builds and uploaded objects are invisible to readers. The only publication
//! API verifies every immutable object through [`BlobStore::head`] before one
//! SQLite transaction records `VERIFIED` and crosses the `PUBLISHED` visibility
//! boundary.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use artifact_contract::{ArtifactCacheKey, ManifestError, ValidatedArtifactManifest};
use blob_store::{BlobStore, ObjectMetadata, StorageError};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use sync_types::{ArtifactId, ObjectId, PlanId};
use thiserror::Error;
use uuid::Uuid;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS artifacts (
  artifact_id        TEXT PRIMARY KEY,
  state              TEXT NOT NULL,
  revision           INTEGER NOT NULL,
  manifest_json      BLOB NOT NULL,
  cache_key_json     TEXT NOT NULL,
  table_set_hash     TEXT NOT NULL,
  lease_token        TEXT,
  lease_owner        TEXT,
  lease_expires_ms   INTEGER,
  created_at_ms      INTEGER NOT NULL,
  updated_at_ms      INTEGER NOT NULL,
  CHECK (state IN ('BUILDING','UPLOADING','UPLOADED','VERIFIED','PUBLISHED','RETIRED')),
  CHECK (revision >= 0),
  CHECK ((state IN ('BUILDING','UPLOADING','UPLOADED','VERIFIED')) =
         (lease_token IS NOT NULL AND lease_owner IS NOT NULL AND lease_expires_ms IS NOT NULL))
) STRICT;

CREATE TABLE IF NOT EXISTS artifact_objects (
  artifact_id        TEXT NOT NULL REFERENCES artifacts(artifact_id) ON DELETE CASCADE,
  object_id          TEXT NOT NULL,
  expected_bytes     INTEGER NOT NULL CHECK (expected_bytes >= 0),
  expected_sha256    TEXT NOT NULL,
  PRIMARY KEY (artifact_id, object_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS artifact_tables (
  artifact_id        TEXT NOT NULL REFERENCES artifacts(artifact_id) ON DELETE CASCADE,
  table_id           TEXT NOT NULL,
  PRIMARY KEY (artifact_id, table_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS artifact_audit (
  audit_id           INTEGER PRIMARY KEY,
  artifact_id        TEXT NOT NULL,
  from_state         TEXT,
  to_state           TEXT NOT NULL,
  revision           INTEGER NOT NULL,
  occurred_at_ms     INTEGER NOT NULL,
  reason             TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS artifact_plan_leases (
  plan_id            TEXT NOT NULL,
  artifact_id        TEXT NOT NULL REFERENCES artifacts(artifact_id) ON DELETE CASCADE,
  expires_ms         INTEGER NOT NULL,
  PRIMARY KEY (plan_id, artifact_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS artifact_build_abandonments (
  artifact_id              TEXT PRIMARY KEY REFERENCES artifacts(artifact_id) ON DELETE CASCADE,
  abandoned_build_token    TEXT NOT NULL,
  cleanup_token            TEXT NOT NULL,
  cleanup_owner            TEXT NOT NULL,
  abandoned_at_ms          INTEGER NOT NULL,
  cleanup_not_before_ms    INTEGER NOT NULL,
  cleanup_lease_expires_ms INTEGER NOT NULL,
  reason                   TEXT NOT NULL,
  CHECK (cleanup_not_before_ms >= abandoned_at_ms),
  CHECK (cleanup_lease_expires_ms > cleanup_not_before_ms)
) STRICT;

CREATE TABLE IF NOT EXISTS artifact_build_cleanup_receipts (
  artifact_id             TEXT NOT NULL,
  cleanup_token           TEXT NOT NULL,
  completed_at_ms         INTEGER NOT NULL,
  deleted_object_count    INTEGER NOT NULL CHECK (deleted_object_count >= 0),
  shared_object_count     INTEGER NOT NULL CHECK (shared_object_count >= 0),
  PRIMARY KEY (artifact_id, cleanup_token)
) STRICT, WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS artifacts_published_compatibility
ON artifacts(cache_key_json, table_set_hash, artifact_id)
WHERE state = 'PUBLISHED';

CREATE INDEX IF NOT EXISTS artifact_tables_by_table
ON artifact_tables(table_id, artifact_id);

CREATE INDEX IF NOT EXISTS artifact_audit_by_artifact
ON artifact_audit(artifact_id, audit_id);

CREATE INDEX IF NOT EXISTS artifact_plan_leases_by_artifact
ON artifact_plan_leases(artifact_id, expires_ms);
"#;

/// The catalog schema version this build creates and opens. A catalog an
/// earlier release wrote is refused, not migrated.
const CATALOG_SCHEMA_VERSION: i64 = 4;

pub type Result<T> = std::result::Result<T, CatalogError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ArtifactState {
    Building,
    Uploading,
    Uploaded,
    Verified,
    Published,
    Retired,
}

impl ArtifactState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Building => "BUILDING",
            Self::Uploading => "UPLOADING",
            Self::Uploaded => "UPLOADED",
            Self::Verified => "VERIFIED",
            Self::Published => "PUBLISHED",
            Self::Retired => "RETIRED",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "BUILDING" => Ok(Self::Building),
            "UPLOADING" => Ok(Self::Uploading),
            "UPLOADED" => Ok(Self::Uploaded),
            "VERIFIED" => Ok(Self::Verified),
            "PUBLISHED" => Ok(Self::Published),
            "RETIRED" => Ok(Self::Retired),
            _ => Err(CatalogError::CorruptState(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BuildLease {
    artifact_id: ArtifactId,
    token: String,
}

#[derive(Debug, Clone)]
pub struct BuildRegistration {
    lease: BuildLease,
    state: ArtifactState,
}

/// Exact authority to clean one explicitly abandoned, never-published build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildCleanupLease {
    artifact_id: ArtifactId,
    token: String,
    owner: String,
    not_before_ms: i64,
    expires_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildCleanupOutcome {
    Cleaned,
    AlreadyCleaned,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildCleanupReport {
    pub outcome: BuildCleanupOutcome,
    /// Objects referenced only by the abandoned build and submitted to the
    /// provider's idempotent delete operation.
    pub deleted_object_count: u64,
    /// Objects retained because another catalog build/publication references
    /// the same immutable object identity.
    pub shared_object_count: u64,
}

impl BuildRegistration {
    pub fn lease(&self) -> &BuildLease {
        &self.lease
    }

    pub const fn state(&self) -> ArtifactState {
        self.state
    }
}

impl BuildLease {
    pub fn artifact_id(&self) -> &ArtifactId {
        &self.artifact_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedArtifactQuery {
    pub cache_key: ArtifactCacheKey,
    pub table_set_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishOutcome {
    Published,
    AlreadyPublished,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEntry {
    pub from_state: Option<ArtifactState>,
    pub to_state: ArtifactState,
    pub revision: u64,
    pub occurred_at_ms: i64,
    pub reason: String,
}

pub struct SqliteArtifactCatalog {
    connection: Connection,
}

impl SqliteArtifactCatalog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Ok(metadata) = std::fs::symlink_metadata(&path) {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(CatalogError::UnsafeCatalogPath(path));
            }
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut connection = Connection::open(&path)?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", true)?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        let mode: String =
            connection.pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(CatalogError::JournalMode(mode));
        }
        install_schema(&mut connection, &path)?;
        Ok(Self { connection })
    }

    /// Bounds waits for another catalog writer. Production defaults to five
    /// seconds; tests and deployment composition may select a smaller finite
    /// bound, but zero would make publication races spuriously fail.
    pub fn set_busy_timeout(&self, timeout: std::time::Duration) -> Result<()> {
        if timeout.is_zero() {
            return Err(CatalogError::InvalidBusyTimeout);
        }
        self.connection.busy_timeout(timeout)?;
        Ok(())
    }

    /// Executes the same bounded catalog read that bootstrap depends on.
    /// Readiness must prove the SQLite catalog is still reachable instead of
    /// treating an already-open file descriptor as sufficient evidence.
    pub fn probe_reachable(&self) -> Result<()> {
        let _: i64 = self.connection.query_row(
            "SELECT count(*) FROM artifacts WHERE state = 'PUBLISHED'",
            [],
            |row| row.get(0),
        )?;
        Ok(())
    }

    pub fn register_build(
        &mut self,
        manifest: &ValidatedArtifactManifest,
        lease_owner: &str,
        now_ms: i64,
        lease_duration_ms: i64,
    ) -> Result<BuildLease> {
        if lease_owner.is_empty() || lease_duration_ms <= 0 {
            return Err(CatalogError::InvalidLease);
        }
        let lease_expires_ms = now_ms
            .checked_add(lease_duration_ms)
            .ok_or(CatalogError::TimestampOverflow)?;
        let artifact_id = manifest.manifest().artifact_id.clone();
        let manifest_json = serde_json::to_vec(manifest.manifest())?;
        let cache_key_json = serde_json::to_string(&manifest.cache_key())?;
        let objects = manifest_objects(manifest)?;
        let token = Uuid::new_v4().to_string();

        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let exists: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM artifacts WHERE artifact_id = ?1)",
            [artifact_id.as_str()],
            |row| row.get(0),
        )?;
        if exists {
            return Err(CatalogError::ArtifactAlreadyExists(artifact_id));
        }
        transaction.execute(
            "INSERT INTO artifacts (artifact_id, state, revision, manifest_json, cache_key_json, \
             table_set_hash, lease_token, lease_owner, lease_expires_ms, created_at_ms, updated_at_ms) \
             VALUES (?1, 'BUILDING', 0, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
            params![
                artifact_id.as_str(),
                manifest_json,
                cache_key_json,
                manifest.manifest().compatibility.table_set_hash,
                token,
                lease_owner,
                lease_expires_ms,
                now_ms,
            ],
        )?;
        for (object, metadata) in objects {
            transaction.execute(
                "INSERT INTO artifact_objects \
                 (artifact_id, object_id, expected_bytes, expected_sha256) \
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    artifact_id.as_str(),
                    object.as_str(),
                    sqlite_i64(metadata.bytes, "expected_bytes")?,
                    metadata.sha256,
                ],
            )?;
        }
        for table in &manifest.manifest().tables {
            transaction.execute(
                "INSERT INTO artifact_tables (artifact_id, table_id) VALUES (?1, ?2)",
                params![artifact_id.as_str(), table.table_id.as_str()],
            )?;
        }
        insert_audit(
            &transaction,
            &artifact_id,
            None,
            ArtifactState::Building,
            0,
            now_ms,
            "build registered",
        )?;
        transaction.commit()?;
        Ok(BuildLease { artifact_id, token })
    }

    /// Registers a new build or re-fences an identical interrupted build.
    ///
    /// A retry by the same lease owner invalidates the previous in-process
    /// capability. A different owner may resume only after expiry. Immutable
    /// manifest bytes must match exactly.
    pub fn register_or_resume_build(
        &mut self,
        manifest: &ValidatedArtifactManifest,
        lease_owner: &str,
        now_ms: i64,
        lease_duration_ms: i64,
    ) -> Result<BuildRegistration> {
        match self.register_build(manifest, lease_owner, now_ms, lease_duration_ms) {
            Ok(lease) => Ok(BuildRegistration {
                lease,
                state: ArtifactState::Building,
            }),
            Err(CatalogError::ArtifactAlreadyExists(_)) => {
                self.resume_identical_build(manifest, lease_owner, now_ms, lease_duration_ms)
            }
            Err(error) => Err(error),
        }
    }

    fn resume_identical_build(
        &mut self,
        manifest: &ValidatedArtifactManifest,
        lease_owner: &str,
        now_ms: i64,
        lease_duration_ms: i64,
    ) -> Result<BuildRegistration> {
        if lease_owner.is_empty() || lease_duration_ms <= 0 {
            return Err(CatalogError::InvalidLease);
        }
        let expires = now_ms
            .checked_add(lease_duration_ms)
            .ok_or(CatalogError::TimestampOverflow)?;
        let artifact = &manifest.manifest().artifact_id;
        let expected_manifest = serde_json::to_vec(manifest.manifest())?;
        let token = Uuid::new_v4().to_string();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row = load_build_row(&transaction, artifact)?;
        reject_abandoned_build(&transaction, artifact)?;
        let stored_manifest: Vec<u8> = transaction.query_row(
            "SELECT manifest_json FROM artifacts WHERE artifact_id = ?1",
            [artifact.as_str()],
            |row| row.get(0),
        )?;
        if stored_manifest != expected_manifest {
            return Err(CatalogError::ArtifactManifestConflict(artifact.clone()));
        }
        if row.state == ArtifactState::Published {
            transaction.commit()?;
            return Ok(BuildRegistration {
                lease: BuildLease {
                    artifact_id: artifact.clone(),
                    token,
                },
                state: ArtifactState::Published,
            });
        }
        if !matches!(
            row.state,
            ArtifactState::Building
                | ArtifactState::Uploading
                | ArtifactState::Uploaded
                | ArtifactState::Verified
        ) {
            return Err(CatalogError::ArtifactIsNotBuilding(row.state));
        }
        let same_owner = row.lease_owner.as_deref() == Some(lease_owner);
        let expired = row
            .lease_expires_ms
            .is_some_and(|prior_expiry| now_ms >= prior_expiry);
        if !same_owner && !expired {
            return Err(CatalogError::BuildLeaseStillActive {
                expires: row
                    .lease_expires_ms
                    .ok_or(CatalogError::BuildLeaseMissing)?,
                now_ms,
            });
        }
        let revision = next_revision(row.revision)?;
        transaction.execute(
            "UPDATE artifacts SET revision = ?1, lease_token = ?2, lease_owner = ?3, \
             lease_expires_ms = ?4, updated_at_ms = ?5 WHERE artifact_id = ?6",
            params![
                sqlite_i64(revision, "revision")?,
                token,
                lease_owner,
                expires,
                now_ms,
                artifact.as_str(),
            ],
        )?;
        insert_audit(
            &transaction,
            artifact,
            Some(row.state),
            row.state,
            revision,
            now_ms,
            "identical interrupted build resumed",
        )?;
        transaction.commit()?;
        Ok(BuildRegistration {
            lease: BuildLease {
                artifact_id: artifact.clone(),
                token,
            },
            state: row.state,
        })
    }

    pub fn renew_lease(
        &mut self,
        lease: &BuildLease,
        now_ms: i64,
        lease_duration_ms: i64,
    ) -> Result<()> {
        if lease_duration_ms <= 0 {
            return Err(CatalogError::InvalidLease);
        }
        let expires = now_ms
            .checked_add(lease_duration_ms)
            .ok_or(CatalogError::TimestampOverflow)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row = load_build_row(&transaction, &lease.artifact_id)?;
        validate_lease(&row, lease, now_ms)?;
        transaction.execute(
            "UPDATE artifacts SET lease_expires_ms = ?1, updated_at_ms = ?2 \
             WHERE artifact_id = ?3",
            params![expires, now_ms, lease.artifact_id.as_str()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Re-fences a cancelled or failed build and schedules its never-visible
    /// objects for catalog-authoritative cleanup after a grace period.
    ///
    /// The cleanup lease starts after `grace_ms` and remains valid for
    /// `cleanup_lease_duration_ms`. Repeating an abandonment with the exact
    /// original build capability and cleanup owner returns the same cleanup
    /// capability, including after a lost response.
    #[allow(clippy::too_many_arguments)]
    pub fn abandon_build(
        &mut self,
        lease: &BuildLease,
        cleanup_owner: &str,
        now_ms: i64,
        grace_ms: i64,
        cleanup_lease_duration_ms: i64,
        reason: &str,
    ) -> Result<BuildCleanupLease> {
        if cleanup_owner.is_empty()
            || grace_ms < 0
            || cleanup_lease_duration_ms <= 0
            || reason.is_empty()
        {
            return Err(CatalogError::InvalidBuildCleanup);
        }
        let not_before_ms = now_ms
            .checked_add(grace_ms)
            .ok_or(CatalogError::TimestampOverflow)?;
        let expires_ms = not_before_ms
            .checked_add(cleanup_lease_duration_ms)
            .ok_or(CatalogError::TimestampOverflow)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row = load_build_row(&transaction, &lease.artifact_id)?;
        if let Some(abandonment) = load_build_abandonment(&transaction, &lease.artifact_id)? {
            if abandonment.abandoned_build_token == lease.token
                && abandonment.cleanup_owner == cleanup_owner
            {
                transaction.commit()?;
                return Ok(abandonment.capability(lease.artifact_id.clone()));
            }
            return Err(CatalogError::ArtifactBuildAbandoned(
                lease.artifact_id.clone(),
            ));
        }
        if !active_build_state(row.state) {
            return Err(CatalogError::ArtifactIsNotBuilding(row.state));
        }
        validate_lease(&row, lease, now_ms)?;
        let cleanup_token = Uuid::new_v4().to_string();
        let revision = next_revision(row.revision)?;
        transaction.execute(
            "UPDATE artifacts
                SET revision = ?1, lease_token = ?2, lease_owner = ?3,
                    lease_expires_ms = ?4, updated_at_ms = ?5
              WHERE artifact_id = ?6",
            params![
                sqlite_i64(revision, "revision")?,
                cleanup_token,
                cleanup_owner,
                expires_ms,
                now_ms,
                lease.artifact_id.as_str(),
            ],
        )?;
        transaction.execute(
            "INSERT INTO artifact_build_abandonments
                (artifact_id, abandoned_build_token, cleanup_token,
                 cleanup_owner, abandoned_at_ms, cleanup_not_before_ms,
                 cleanup_lease_expires_ms, reason)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                lease.artifact_id.as_str(),
                lease.token,
                cleanup_token,
                cleanup_owner,
                now_ms,
                not_before_ms,
                expires_ms,
                reason,
            ],
        )?;
        insert_audit(
            &transaction,
            &lease.artifact_id,
            Some(row.state),
            row.state,
            revision,
            now_ms,
            reason,
        )?;
        transaction.commit()?;
        Ok(BuildCleanupLease {
            artifact_id: lease.artifact_id.clone(),
            token: cleanup_token,
            owner: cleanup_owner.to_owned(),
            not_before_ms,
            expires_ms,
        })
    }

    /// Takes over cleanup only after the prior cleanup capability expires.
    pub fn take_over_abandoned_build_cleanup(
        &mut self,
        artifact: &ArtifactId,
        cleanup_owner: &str,
        now_ms: i64,
        cleanup_lease_duration_ms: i64,
    ) -> Result<BuildCleanupLease> {
        if cleanup_owner.is_empty() || cleanup_lease_duration_ms <= 0 {
            return Err(CatalogError::InvalidBuildCleanup);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row = load_build_row(&transaction, artifact)?;
        if !active_build_state(row.state) {
            return Err(CatalogError::ArtifactIsNotBuilding(row.state));
        }
        let abandonment = load_build_abandonment(&transaction, artifact)?
            .ok_or_else(|| CatalogError::ArtifactBuildIsNotAbandoned(artifact.clone()))?;
        if now_ms < abandonment.cleanup_lease_expires_ms {
            return Err(CatalogError::BuildCleanupLeaseStillActive {
                expires: abandonment.cleanup_lease_expires_ms,
                now_ms,
            });
        }
        let expires_ms = now_ms
            .checked_add(cleanup_lease_duration_ms)
            .ok_or(CatalogError::TimestampOverflow)?;
        let cleanup_token = Uuid::new_v4().to_string();
        let revision = next_revision(row.revision)?;
        transaction.execute(
            "UPDATE artifacts
                SET revision = ?1, lease_token = ?2, lease_owner = ?3,
                    lease_expires_ms = ?4, updated_at_ms = ?5
              WHERE artifact_id = ?6",
            params![
                sqlite_i64(revision, "revision")?,
                cleanup_token,
                cleanup_owner,
                expires_ms,
                now_ms,
                artifact.as_str(),
            ],
        )?;
        transaction.execute(
            "UPDATE artifact_build_abandonments
                SET cleanup_token = ?1, cleanup_owner = ?2,
                    cleanup_lease_expires_ms = ?3
              WHERE artifact_id = ?4",
            params![cleanup_token, cleanup_owner, expires_ms, artifact.as_str()],
        )?;
        insert_audit(
            &transaction,
            artifact,
            Some(row.state),
            row.state,
            revision,
            now_ms,
            "abandoned build cleanup taken over",
        )?;
        transaction.commit()?;
        Ok(BuildCleanupLease {
            artifact_id: artifact.clone(),
            token: cleanup_token,
            owner: cleanup_owner.to_owned(),
            not_before_ms: abandonment.cleanup_not_before_ms,
            expires_ms,
        })
    }

    /// Deletes only immutable objects that no other catalog row references,
    /// then atomically removes the abandoned build registration.
    ///
    /// The immediate catalog transaction remains held across provider deletes,
    /// preventing publication or registration from racing the authoritative
    /// reachability decision. Provider delete is idempotent, so a crash after
    /// any subset of deletes can safely retry the same capability.
    pub async fn cleanup_abandoned_build<S: BlobStore + ?Sized>(
        &mut self,
        lease: &BuildCleanupLease,
        store: &S,
        now_ms: i64,
    ) -> Result<BuildCleanupReport> {
        self.cleanup_abandoned_build_with_hook(lease, store, now_ms, &mut |_, _| Ok(()))
            .await
    }

    async fn cleanup_abandoned_build_with_hook<S, F>(
        &mut self,
        lease: &BuildCleanupLease,
        store: &S,
        now_ms: i64,
        after_delete: &mut F,
    ) -> Result<BuildCleanupReport>
    where
        S: BlobStore + ?Sized,
        F: FnMut(&ObjectId, usize) -> Result<()>,
    {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let artifact_exists: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM artifacts WHERE artifact_id = ?1)",
            [lease.artifact_id.as_str()],
            |row| row.get(0),
        )?;
        if !artifact_exists {
            let receipt = load_cleanup_receipt(&transaction, lease)?
                .ok_or(CatalogError::StaleBuildCleanupLease)?;
            transaction.commit()?;
            return Ok(BuildCleanupReport {
                outcome: BuildCleanupOutcome::AlreadyCleaned,
                deleted_object_count: receipt.deleted_object_count,
                shared_object_count: receipt.shared_object_count,
            });
        }
        let row = load_build_row(&transaction, &lease.artifact_id)?;
        if !active_build_state(row.state) {
            return Err(CatalogError::ArtifactIsNotBuilding(row.state));
        }
        let abandonment = load_build_abandonment(&transaction, &lease.artifact_id)?
            .ok_or_else(|| CatalogError::ArtifactBuildIsNotAbandoned(lease.artifact_id.clone()))?;
        validate_cleanup_lease(&abandonment, lease, now_ms)?;
        let objects = abandoned_build_objects(&transaction, &lease.artifact_id)?;
        let mut deleted_object_count = 0_u64;
        let mut shared_object_count = 0_u64;
        for (index, (object, shared)) in objects.iter().enumerate() {
            if *shared {
                shared_object_count = shared_object_count
                    .checked_add(1)
                    .ok_or(CatalogError::CleanupCountOverflow)?;
                continue;
            }
            store.delete(object).await?;
            deleted_object_count = deleted_object_count
                .checked_add(1)
                .ok_or(CatalogError::CleanupCountOverflow)?;
            after_delete(object, index)?;
        }
        let revision = next_revision(row.revision)?;
        insert_audit(
            &transaction,
            &lease.artifact_id,
            Some(row.state),
            row.state,
            revision,
            now_ms,
            "abandoned build cleanup committed",
        )?;
        let deleted = transaction.execute(
            "DELETE FROM artifacts WHERE artifact_id = ?1",
            [lease.artifact_id.as_str()],
        )?;
        if deleted != 1 {
            return Err(CatalogError::UnknownArtifact(lease.artifact_id.clone()));
        }
        transaction.execute(
            "INSERT INTO artifact_build_cleanup_receipts
                (artifact_id, cleanup_token, completed_at_ms,
                 deleted_object_count, shared_object_count)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                lease.artifact_id.as_str(),
                lease.token,
                now_ms,
                sqlite_i64(deleted_object_count, "deleted_object_count")?,
                sqlite_i64(shared_object_count, "shared_object_count")?,
            ],
        )?;
        transaction.commit()?;
        Ok(BuildCleanupReport {
            outcome: BuildCleanupOutcome::Cleaned,
            deleted_object_count,
            shared_object_count,
        })
    }

    /// Re-fences an abandoned build without changing its immutable manifest or
    /// losing already uploaded objects. A live lease can never be stolen.
    pub fn take_over_expired_build(
        &mut self,
        artifact: &ArtifactId,
        lease_owner: &str,
        now_ms: i64,
        lease_duration_ms: i64,
    ) -> Result<BuildLease> {
        if lease_owner.is_empty() || lease_duration_ms <= 0 {
            return Err(CatalogError::InvalidLease);
        }
        let expires = now_ms
            .checked_add(lease_duration_ms)
            .ok_or(CatalogError::TimestampOverflow)?;
        let token = Uuid::new_v4().to_string();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row = load_build_row(&transaction, artifact)?;
        reject_abandoned_build(&transaction, artifact)?;
        if !matches!(
            row.state,
            ArtifactState::Building
                | ArtifactState::Uploading
                | ArtifactState::Uploaded
                | ArtifactState::Verified
        ) {
            return Err(CatalogError::ArtifactIsNotBuilding(row.state));
        }
        let prior_expiry = row
            .lease_expires_ms
            .ok_or(CatalogError::BuildLeaseMissing)?;
        if now_ms < prior_expiry {
            return Err(CatalogError::BuildLeaseStillActive {
                expires: prior_expiry,
                now_ms,
            });
        }
        let revision = next_revision(row.revision)?;
        transaction.execute(
            "UPDATE artifacts SET revision = ?1, lease_token = ?2, lease_owner = ?3, \
             lease_expires_ms = ?4, updated_at_ms = ?5 WHERE artifact_id = ?6",
            params![
                sqlite_i64(revision, "revision")?,
                token,
                lease_owner,
                expires,
                now_ms,
                artifact.as_str(),
            ],
        )?;
        insert_audit(
            &transaction,
            artifact,
            Some(row.state),
            row.state,
            revision,
            now_ms,
            "expired build lease taken over",
        )?;
        transaction.commit()?;
        Ok(BuildLease {
            artifact_id: artifact.clone(),
            token,
        })
    }

    pub fn mark_uploading(&mut self, lease: &BuildLease, now_ms: i64) -> Result<u64> {
        self.transition_build(
            lease,
            ArtifactState::Building,
            ArtifactState::Uploading,
            now_ms,
            "upload started",
        )
    }

    pub fn mark_uploaded(&mut self, lease: &BuildLease, now_ms: i64) -> Result<u64> {
        self.transition_build(
            lease,
            ArtifactState::Uploading,
            ArtifactState::Uploaded,
            now_ms,
            "all objects uploaded",
        )
    }

    pub async fn verify_and_publish<S: BlobStore + ?Sized>(
        &mut self,
        lease: &BuildLease,
        store: &S,
        now_ms: i64,
    ) -> Result<PublishOutcome> {
        self.verify_and_publish_admitted(lease, store, now_ms, || true)
            .await
    }

    /// [`Self::verify_and_publish`], except that the artifact becomes visible
    /// only if `admit` holds. `admit` is called once, after the objects are
    /// verified and inside the transaction that would make the artifact
    /// visible; a refused build stays where it was, for its owner to abandon.
    /// A caller that keeps whatever `admit` reads from changing until this
    /// returns never publishes what `admit` refuses.
    pub async fn verify_and_publish_admitted<S: BlobStore + ?Sized>(
        &mut self,
        lease: &BuildLease,
        store: &S,
        now_ms: i64,
        admit: impl FnOnce() -> bool,
    ) -> Result<PublishOutcome> {
        // Keep the immediate transaction open across object verification. GC
        // and publication state transitions using this catalog therefore
        // cannot delete or retire an object between HEAD and visibility.
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row = load_build_row(&transaction, &lease.artifact_id)?;
        let already_published = row.state == ArtifactState::Published;
        if !already_published {
            validate_lease(&row, lease, now_ms)?;
        }
        if !already_published && row.state != ArtifactState::Uploaded {
            return Err(CatalogError::InvalidTransition {
                actual: row.state,
                expected: ArtifactState::Uploaded,
                next: ArtifactState::Published,
            });
        }
        let objects = expected_objects(&transaction, &lease.artifact_id)?;
        for (object, expected) in objects {
            let Some(actual) = store.head(&object).await? else {
                if already_published {
                    quarantine_published(
                        &transaction,
                        &lease.artifact_id,
                        row.revision,
                        now_ms,
                        "published object disappeared",
                    )?;
                    transaction.commit()?;
                }
                return Err(CatalogError::MissingObject(object));
            };
            if actual != expected {
                if already_published {
                    quarantine_published(
                        &transaction,
                        &lease.artifact_id,
                        row.revision,
                        now_ms,
                        "published object metadata changed",
                    )?;
                    transaction.commit()?;
                }
                return Err(CatalogError::ObjectMetadataMismatch {
                    object,
                    expected,
                    actual,
                });
            }
        }
        if already_published {
            transaction.commit()?;
            return Ok(PublishOutcome::AlreadyPublished);
        }
        if !admit() {
            return Err(CatalogError::PublicationRefused(lease.artifact_id.clone()));
        }
        let verified_revision = next_revision(row.revision)?;
        update_state(
            &transaction,
            &lease.artifact_id,
            ArtifactState::Verified,
            verified_revision,
            now_ms,
            true,
        )?;
        insert_audit(
            &transaction,
            &lease.artifact_id,
            Some(ArtifactState::Uploaded),
            ArtifactState::Verified,
            verified_revision,
            now_ms,
            "object metadata verified",
        )?;
        let published_revision = next_revision(verified_revision)?;
        update_state(
            &transaction,
            &lease.artifact_id,
            ArtifactState::Published,
            published_revision,
            now_ms,
            false,
        )?;
        insert_audit(
            &transaction,
            &lease.artifact_id,
            Some(ArtifactState::Verified),
            ArtifactState::Published,
            published_revision,
            now_ms,
            "catalog visibility committed",
        )?;
        transaction.commit()?;
        Ok(PublishOutcome::Published)
    }

    pub fn published(
        &self,
        query: &PublishedArtifactQuery,
    ) -> Result<Vec<ValidatedArtifactManifest>> {
        let cache_key_json = serde_json::to_string(&query.cache_key)?;
        let mut statement = self.connection.prepare(
            "SELECT manifest_json FROM artifacts \
             WHERE state = 'PUBLISHED' AND cache_key_json = ?1 AND table_set_hash = ?2 \
             ORDER BY artifact_id",
        )?;
        let rows = statement.query_map(params![cache_key_json, query.table_set_hash], |row| {
            row.get::<_, Vec<u8>>(0)
        })?;
        let mut manifests = Vec::new();
        for bytes in rows {
            let manifest = ValidatedArtifactManifest::parse(&bytes?)?;
            if manifest.cache_key() != query.cache_key
                || manifest.manifest().compatibility.table_set_hash != query.table_set_hash
            {
                return Err(CatalogError::CorruptPublishedIndex);
            }
            manifests.push(manifest);
        }
        Ok(manifests)
    }

    /// Bounded compatibility lookup restricted to artifacts intersecting the
    /// query-derived table set. The normalized table index prevents unrelated
    /// publication history from consuming the planner's candidate budget.
    pub fn published_compatible_for_tables(
        &self,
        cache_key: &ArtifactCacheKey,
        tables: &std::collections::BTreeSet<sync_types::TableId>,
        limit: usize,
    ) -> Result<Vec<ValidatedArtifactManifest>> {
        if tables.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let cache_key_json = serde_json::to_string(cache_key)?;
        let mut values = Vec::with_capacity(tables.len() + 2);
        values.push(rusqlite::types::Value::Text(cache_key_json));
        values.extend(
            tables
                .iter()
                .map(|table| rusqlite::types::Value::Text(table.to_string())),
        );
        values.push(rusqlite::types::Value::Integer(sqlite_i64(
            u64::try_from(limit.saturating_add(1)).unwrap_or(u64::MAX),
            "candidate_limit",
        )?));
        let table_placeholders = (0..tables.len())
            .map(|index| format!("?{}", index + 2))
            .collect::<Vec<_>>()
            .join(",");
        let limit_parameter = tables.len() + 2;
        let sql = format!(
            "SELECT DISTINCT artifacts.manifest_json, artifacts.table_set_hash \
             FROM artifacts \
             JOIN artifact_tables ON artifact_tables.artifact_id = artifacts.artifact_id \
             WHERE artifacts.state = 'PUBLISHED' \
               AND artifacts.cache_key_json = ?1 \
               AND artifact_tables.table_id IN ({table_placeholders}) \
             ORDER BY artifacts.artifact_id \
             LIMIT ?{limit_parameter}"
        );
        let mut statement = self.connection.prepare(&sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(values.iter()), |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut manifests = Vec::new();
        for row in rows {
            let (bytes, indexed_table_set_hash) = row?;
            let manifest = ValidatedArtifactManifest::parse(&bytes)?;
            let intersects = manifest
                .manifest()
                .tables
                .iter()
                .any(|table| tables.contains(&table.table_id));
            if manifest.cache_key() != *cache_key
                || manifest.manifest().compatibility.table_set_hash != indexed_table_set_hash
                || !intersects
            {
                return Err(CatalogError::CorruptPublishedIndex);
            }
            manifests.push(manifest);
        }
        Ok(manifests)
    }

    /// Revalidates all immutable objects for one visible artifact.
    ///
    /// Catalog-coordinated GC cannot race this check because the immediate
    /// transaction remains held through every provider `HEAD`. A missing or
    /// changed object atomically removes the artifact from bootstrap visibility.
    pub async fn reverify_published<S: BlobStore + ?Sized>(
        &mut self,
        artifact: &ArtifactId,
        store: &S,
        now_ms: i64,
    ) -> Result<ValidatedArtifactManifest> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row = load_build_row(&transaction, artifact)?;
        if row.state != ArtifactState::Published {
            return Err(CatalogError::ArtifactIsNotPublished(row.state));
        }
        let objects = expected_objects(&transaction, artifact)?;
        for (object, expected) in objects {
            let Some(actual) = store.head(&object).await? else {
                quarantine_published(
                    &transaction,
                    artifact,
                    row.revision,
                    now_ms,
                    "published object disappeared during bootstrap verification",
                )?;
                transaction.commit()?;
                return Err(CatalogError::MissingObject(object));
            };
            if actual != expected {
                quarantine_published(
                    &transaction,
                    artifact,
                    row.revision,
                    now_ms,
                    "published object metadata changed during bootstrap verification",
                )?;
                transaction.commit()?;
                return Err(CatalogError::ObjectMetadataMismatch {
                    object,
                    expected,
                    actual,
                });
            }
        }
        let bytes: Vec<u8> = transaction.query_row(
            "SELECT manifest_json FROM artifacts WHERE artifact_id = ?1",
            [artifact.as_str()],
            |row| row.get(0),
        )?;
        let manifest = ValidatedArtifactManifest::parse(&bytes)?;
        transaction.commit()?;
        Ok(manifest)
    }

    /// Pins selected published artifacts through a dynamic plan's expiry.
    ///
    /// Retirement and deletion transitions consult these leases. A failed plan
    /// may leave conservative references until expiry, but an issued plan can
    /// never lose catalog-coordinated object reachability mid-bootstrap.
    pub fn pin_published_for_plan(
        &mut self,
        plan: &PlanId,
        artifacts: &[ArtifactId],
        expires_ms: i64,
        now_ms: i64,
    ) -> Result<()> {
        if expires_ms <= now_ms {
            return Err(CatalogError::InvalidPlanLease);
        }
        let unique: std::collections::BTreeSet<_> = artifacts.iter().collect();
        if unique.len() != artifacts.len() {
            return Err(CatalogError::DuplicatePlanArtifact);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "DELETE FROM artifact_plan_leases WHERE expires_ms <= ?1",
            [now_ms],
        )?;
        for artifact in artifacts {
            let row = load_build_row(&transaction, artifact)?;
            if row.state != ArtifactState::Published {
                return Err(CatalogError::ArtifactIsNotPublished(row.state));
            }
            transaction.execute(
                "INSERT INTO artifact_plan_leases (plan_id, artifact_id, expires_ms) \
                 VALUES (?1, ?2, ?3) \
                 ON CONFLICT(plan_id, artifact_id) DO UPDATE SET expires_ms = excluded.expires_ms",
                params![plan.as_str(), artifact.as_str(), expires_ms],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn state(&self, artifact: &ArtifactId) -> Result<Option<ArtifactState>> {
        let value = self
            .connection
            .query_row(
                "SELECT state FROM artifacts WHERE artifact_id = ?1",
                [artifact.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        value.map(|value| ArtifactState::parse(&value)).transpose()
    }

    pub fn manifest(&self, artifact: &ArtifactId) -> Result<Option<ValidatedArtifactManifest>> {
        let bytes = self
            .connection
            .query_row(
                "SELECT manifest_json FROM artifacts WHERE artifact_id = ?1",
                [artifact.as_str()],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?;
        bytes
            .map(|bytes| ValidatedArtifactManifest::parse(&bytes).map_err(CatalogError::Manifest))
            .transpose()
    }

    pub fn retire(&mut self, artifact: &ArtifactId, now_ms: i64, reason: &str) -> Result<u64> {
        self.transition_operator(
            artifact,
            ArtifactState::Published,
            ArtifactState::Retired,
            now_ms,
            reason,
        )
    }

    /// Deletes every retired artifact no plan pins, and each of its objects
    /// that no other artifact names, then returns the deleted artifacts' ids.
    /// Each artifact goes in one transaction, so an object two retired
    /// artifacts share is deleted with the second.
    pub async fn delete_retired<S: BlobStore + ?Sized>(
        &mut self,
        store: &S,
        now_ms: i64,
    ) -> Result<Vec<ArtifactId>> {
        let candidates = {
            let mut statement = self.connection.prepare(
                "SELECT artifact_id FROM artifacts WHERE state = 'RETIRED' ORDER BY artifact_id",
            )?;
            let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let mut deleted = Vec::new();
        for candidate in candidates {
            let artifact = ArtifactId::new(candidate)?;
            let transaction = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)?;
            let row = load_build_row(&transaction, &artifact)?;
            if row.state != ArtifactState::Retired
                || pinned_by_plan(&transaction, &artifact, now_ms)?
            {
                continue;
            }
            for (object, shared) in abandoned_build_objects(&transaction, &artifact)? {
                if !shared {
                    store.delete(&object).await?;
                }
            }
            insert_audit(
                &transaction,
                &artifact,
                Some(ArtifactState::Retired),
                ArtifactState::Retired,
                next_revision(row.revision)?,
                now_ms,
                "retired artifact deleted",
            )?;
            transaction.execute(
                "DELETE FROM artifacts WHERE artifact_id = ?1",
                [artifact.as_str()],
            )?;
            transaction.commit()?;
            deleted.push(artifact);
        }
        Ok(deleted)
    }

    /// Retires a published artifact whatever plans pin it, because a change
    /// after its cut makes it wrong to plan from. No later plan selects it; a
    /// plan that already holds it keeps its objects until
    /// [`Self::delete_retired_artifact`] finds no lease on it.
    pub fn retire_superseded(
        &mut self,
        artifact: &ArtifactId,
        now_ms: i64,
        reason: &str,
    ) -> Result<u64> {
        if reason.is_empty() {
            return Err(CatalogError::EmptyReason);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row = load_build_row(&transaction, artifact)?;
        match row.state {
            ArtifactState::Retired => {
                transaction.commit()?;
                return Ok(row.revision);
            }
            ArtifactState::Published => {}
            actual => {
                return Err(CatalogError::InvalidTransition {
                    actual,
                    expected: ArtifactState::Published,
                    next: ArtifactState::Retired,
                })
            }
        }
        let revision = next_revision(row.revision)?;
        update_state(
            &transaction,
            artifact,
            ArtifactState::Retired,
            revision,
            now_ms,
            false,
        )?;
        insert_audit(
            &transaction,
            artifact,
            Some(ArtifactState::Published),
            ArtifactState::Retired,
            revision,
            now_ms,
            reason,
        )?;
        transaction.commit()?;
        Ok(revision)
    }

    /// Every artifact in `state`, in id order.
    pub fn artifacts_in_state(&self, state: ArtifactState) -> Result<Vec<ArtifactId>> {
        let mut statement = self
            .connection
            .prepare("SELECT artifact_id FROM artifacts WHERE state = ?1 ORDER BY artifact_id")?;
        let rows = statement.query_map([state.as_str()], |row| row.get::<_, String>(0))?;
        let mut artifacts = Vec::new();
        for id in rows {
            artifacts.push(ArtifactId::new(id?).map_err(|_| CatalogError::CorruptPublishedIndex)?);
        }
        Ok(artifacts)
    }

    /// Deletes one retired artifact that no plan pins: each of its objects no
    /// other artifact references, then its row. Returns how many objects were
    /// deleted, or `None`, deleting nothing, while a plan still pins it.
    pub async fn delete_retired_artifact<S: BlobStore + ?Sized>(
        &mut self,
        artifact: &ArtifactId,
        store: &S,
        now_ms: i64,
    ) -> Result<Option<u64>> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row = load_build_row(&transaction, artifact)?;
        if row.state != ArtifactState::Retired {
            return Err(CatalogError::InvalidTransition {
                actual: row.state,
                expected: ArtifactState::Retired,
                next: ArtifactState::Retired,
            });
        }
        if pinned_by_plan(&transaction, artifact, now_ms)? {
            return Ok(None);
        }
        let mut deleted = 0_u64;
        for (object, shared) in abandoned_build_objects(&transaction, artifact)? {
            if !shared {
                store.delete(&object).await?;
                deleted = deleted
                    .checked_add(1)
                    .ok_or(CatalogError::CleanupCountOverflow)?;
            }
        }
        let revision = next_revision(row.revision)?;
        insert_audit(
            &transaction,
            artifact,
            Some(ArtifactState::Retired),
            ArtifactState::Retired,
            revision,
            now_ms,
            "retired artifact deleted",
        )?;
        transaction.execute(
            "DELETE FROM artifacts WHERE artifact_id = ?1",
            [artifact.as_str()],
        )?;
        transaction.commit()?;
        Ok(Some(deleted))
    }

    #[cfg(test)]
    pub fn audit(&self, artifact: &ArtifactId) -> Result<Vec<AuditEntry>> {
        let mut statement = self.connection.prepare(
            "SELECT from_state, to_state, revision, occurred_at_ms, reason \
             FROM artifact_audit WHERE artifact_id = ?1 ORDER BY audit_id",
        )?;
        let rows = statement.query_map([artifact.as_str()], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;
        let mut entries = Vec::new();
        for row in rows {
            let (from, to, revision, occurred_at_ms, reason) = row?;
            entries.push(AuditEntry {
                from_state: from.as_deref().map(ArtifactState::parse).transpose()?,
                to_state: ArtifactState::parse(&to)?,
                revision: u64::try_from(revision)
                    .map_err(|_| CatalogError::CorruptRevision(revision))?,
                occurred_at_ms,
                reason,
            });
        }
        Ok(entries)
    }

    fn transition_build(
        &mut self,
        lease: &BuildLease,
        expected: ArtifactState,
        next: ArtifactState,
        now_ms: i64,
        reason: &str,
    ) -> Result<u64> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row = load_build_row(&transaction, &lease.artifact_id)?;
        validate_lease(&row, lease, now_ms)?;
        if row.state == next {
            transaction.commit()?;
            return Ok(row.revision);
        }
        if row.state != expected {
            return Err(CatalogError::InvalidTransition {
                actual: row.state,
                expected,
                next,
            });
        }
        let revision = next_revision(row.revision)?;
        update_state(
            &transaction,
            &lease.artifact_id,
            next,
            revision,
            now_ms,
            true,
        )?;
        insert_audit(
            &transaction,
            &lease.artifact_id,
            Some(expected),
            next,
            revision,
            now_ms,
            reason,
        )?;
        transaction.commit()?;
        Ok(revision)
    }

    fn transition_operator(
        &mut self,
        artifact: &ArtifactId,
        expected: ArtifactState,
        next: ArtifactState,
        now_ms: i64,
        reason: &str,
    ) -> Result<u64> {
        if reason.is_empty() {
            return Err(CatalogError::EmptyReason);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row = load_build_row(&transaction, artifact)?;
        let pinned_until: Option<i64> = transaction.query_row(
            "SELECT max(expires_ms) FROM artifact_plan_leases \
             WHERE artifact_id = ?1 AND expires_ms > ?2",
            params![artifact.as_str(), now_ms],
            |row| row.get(0),
        )?;
        if let Some(expires_ms) = pinned_until {
            return Err(CatalogError::ArtifactPinnedByPlan { expires_ms });
        }
        if row.state == next {
            transaction.commit()?;
            return Ok(row.revision);
        }
        if row.state != expected {
            return Err(CatalogError::InvalidTransition {
                actual: row.state,
                expected,
                next,
            });
        }
        let revision = next_revision(row.revision)?;
        update_state(&transaction, artifact, next, revision, now_ms, false)?;
        insert_audit(
            &transaction,
            artifact,
            Some(expected),
            next,
            revision,
            now_ms,
            reason,
        )?;
        transaction.commit()?;
        Ok(revision)
    }
}

/// Creates the schema in a new catalog and opens one already at
/// `CATALOG_SCHEMA_VERSION`. A new file reads `user_version = 0` and holds no
/// schema objects; a version-0 file that holds any is an earlier release's
/// unversioned catalog, refused like every earlier version.
fn install_schema(connection: &mut Connection, path: &Path) -> Result<()> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let version: i64 = transaction.pragma_query_value(None, "user_version", |row| row.get(0))?;
    let schema_objects: i64 =
        transaction.query_row("SELECT count(*) FROM sqlite_schema", [], |row| row.get(0))?;
    match version {
        CATALOG_SCHEMA_VERSION => {}
        0 if schema_objects == 0 => {}
        0..CATALOG_SCHEMA_VERSION => {
            let state_dir = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or(Path::new("."))
                .to_path_buf();
            return Err(CatalogError::EarlierSchemaVersion { state_dir, version });
        }
        _ => return Err(CatalogError::UnsupportedSchemaVersion(version)),
    }
    transaction.execute_batch(SCHEMA)?;
    transaction.pragma_update(None, "user_version", CATALOG_SCHEMA_VERSION)?;
    transaction.commit()?;
    Ok(())
}

fn expected_objects(
    connection: &Connection,
    artifact: &ArtifactId,
) -> Result<Vec<(ObjectId, ObjectMetadata)>> {
    let mut statement = connection.prepare(
        "SELECT object_id, expected_bytes, expected_sha256 \
         FROM artifact_objects WHERE artifact_id = ?1 ORDER BY object_id",
    )?;
    let rows = statement.query_map([artifact.as_str()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let mut objects = Vec::new();
    for row in rows {
        let (object, bytes, digest) = row?;
        objects.push((
            ObjectId::new(object).map_err(CatalogError::Identifier)?,
            ObjectMetadata::new(
                u64::try_from(bytes).map_err(|_| CatalogError::CorruptObjectLength(bytes))?,
                digest,
            )?,
        ));
    }
    if objects.is_empty() {
        return Err(CatalogError::NoObjects(artifact.clone()));
    }
    Ok(objects)
}

#[derive(Debug)]
struct BuildRow {
    state: ArtifactState,
    revision: u64,
    lease_token: Option<String>,
    lease_owner: Option<String>,
    lease_expires_ms: Option<i64>,
}

#[derive(Debug)]
struct BuildAbandonmentRow {
    abandoned_build_token: String,
    cleanup_token: String,
    cleanup_owner: String,
    cleanup_not_before_ms: i64,
    cleanup_lease_expires_ms: i64,
}

impl BuildAbandonmentRow {
    fn capability(&self, artifact_id: ArtifactId) -> BuildCleanupLease {
        BuildCleanupLease {
            artifact_id,
            token: self.cleanup_token.clone(),
            owner: self.cleanup_owner.clone(),
            not_before_ms: self.cleanup_not_before_ms,
            expires_ms: self.cleanup_lease_expires_ms,
        }
    }
}

#[derive(Debug)]
struct CleanupReceipt {
    deleted_object_count: u64,
    shared_object_count: u64,
}

fn active_build_state(state: ArtifactState) -> bool {
    matches!(
        state,
        ArtifactState::Building
            | ArtifactState::Uploading
            | ArtifactState::Uploaded
            | ArtifactState::Verified
    )
}

fn load_build_abandonment(
    transaction: &Transaction<'_>,
    artifact: &ArtifactId,
) -> Result<Option<BuildAbandonmentRow>> {
    transaction
        .query_row(
            "SELECT abandoned_build_token, cleanup_token, cleanup_owner,
                    cleanup_not_before_ms, cleanup_lease_expires_ms
               FROM artifact_build_abandonments
              WHERE artifact_id = ?1",
            [artifact.as_str()],
            |row| {
                Ok(BuildAbandonmentRow {
                    abandoned_build_token: row.get(0)?,
                    cleanup_token: row.get(1)?,
                    cleanup_owner: row.get(2)?,
                    cleanup_not_before_ms: row.get(3)?,
                    cleanup_lease_expires_ms: row.get(4)?,
                })
            },
        )
        .optional()
        .map_err(CatalogError::Sqlite)
}

fn reject_abandoned_build(transaction: &Transaction<'_>, artifact: &ArtifactId) -> Result<()> {
    if load_build_abandonment(transaction, artifact)?.is_some() {
        return Err(CatalogError::ArtifactBuildAbandoned(artifact.clone()));
    }
    Ok(())
}

fn validate_cleanup_lease(
    abandonment: &BuildAbandonmentRow,
    lease: &BuildCleanupLease,
    now_ms: i64,
) -> Result<()> {
    if abandonment.cleanup_token != lease.token
        || abandonment.cleanup_owner != lease.owner
        || abandonment.cleanup_not_before_ms != lease.not_before_ms
        || abandonment.cleanup_lease_expires_ms != lease.expires_ms
    {
        return Err(CatalogError::StaleBuildCleanupLease);
    }
    if now_ms < abandonment.cleanup_not_before_ms {
        return Err(CatalogError::BuildCleanupGraceActive {
            not_before: abandonment.cleanup_not_before_ms,
            now_ms,
        });
    }
    if now_ms >= abandonment.cleanup_lease_expires_ms {
        return Err(CatalogError::BuildCleanupLeaseExpired {
            expires: abandonment.cleanup_lease_expires_ms,
            now_ms,
        });
    }
    Ok(())
}

fn abandoned_build_objects(
    transaction: &Transaction<'_>,
    artifact: &ArtifactId,
) -> Result<Vec<(ObjectId, bool)>> {
    let mut statement = transaction.prepare(
        "SELECT current.object_id,
                EXISTS(
                    SELECT 1 FROM artifact_objects AS other
                     WHERE other.object_id = current.object_id
                       AND other.artifact_id <> current.artifact_id
                )
           FROM artifact_objects AS current
          WHERE current.artifact_id = ?1
          ORDER BY current.object_id",
    )?;
    let rows = statement.query_map([artifact.as_str()], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?))
    })?;
    let mut objects = Vec::new();
    for row in rows {
        let (object, shared) = row?;
        objects.push((ObjectId::new(object)?, shared));
    }
    if objects.is_empty() {
        return Err(CatalogError::NoObjects(artifact.clone()));
    }
    Ok(objects)
}

fn load_cleanup_receipt(
    transaction: &Transaction<'_>,
    lease: &BuildCleanupLease,
) -> Result<Option<CleanupReceipt>> {
    transaction
        .query_row(
            "SELECT deleted_object_count, shared_object_count
               FROM artifact_build_cleanup_receipts
              WHERE artifact_id = ?1 AND cleanup_token = ?2",
            params![lease.artifact_id.as_str(), lease.token],
            |row| {
                let deleted: i64 = row.get(0)?;
                let shared: i64 = row.get(1)?;
                Ok((deleted, shared))
            },
        )
        .optional()?
        .map(|(deleted, shared)| {
            Ok(CleanupReceipt {
                deleted_object_count: u64::try_from(deleted)
                    .map_err(|_| CatalogError::CorruptCleanupCount(deleted))?,
                shared_object_count: u64::try_from(shared)
                    .map_err(|_| CatalogError::CorruptCleanupCount(shared))?,
            })
        })
        .transpose()
}

/// Whether a plan's pin on `artifact` is still in force at `now_ms`.
fn pinned_by_plan(
    transaction: &Transaction<'_>,
    artifact: &ArtifactId,
    now_ms: i64,
) -> Result<bool> {
    Ok(transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM artifact_plan_leases \
         WHERE artifact_id = ?1 AND expires_ms > ?2)",
        params![artifact.as_str(), now_ms],
        |row| row.get(0),
    )?)
}

fn load_build_row(transaction: &Transaction<'_>, artifact: &ArtifactId) -> Result<BuildRow> {
    transaction
        .query_row(
            "SELECT state, revision, lease_token, lease_owner, lease_expires_ms \
             FROM artifacts WHERE artifact_id = ?1",
            [artifact.as_str()],
            decode_build_row,
        )
        .optional()?
        .ok_or_else(|| CatalogError::UnknownArtifact(artifact.clone()))
}

fn decode_build_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<BuildRow> {
    let state: String = row.get(0)?;
    let revision: i64 = row.get(1)?;
    Ok(BuildRow {
        state: ArtifactState::parse(&state).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        revision: u64::try_from(revision).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                1,
                rusqlite::types::Type::Integer,
                Box::new(CatalogError::CorruptRevision(revision)),
            )
        })?,
        lease_token: row.get(2)?,
        lease_owner: row.get(3)?,
        lease_expires_ms: row.get(4)?,
    })
}

fn validate_lease(row: &BuildRow, lease: &BuildLease, now_ms: i64) -> Result<()> {
    let token = row
        .lease_token
        .as_deref()
        .ok_or(CatalogError::BuildLeaseMissing)?;
    if token != lease.token {
        return Err(CatalogError::StaleBuildLease);
    }
    let expires = row
        .lease_expires_ms
        .ok_or(CatalogError::BuildLeaseMissing)?;
    if now_ms >= expires {
        return Err(CatalogError::BuildLeaseExpired { expires, now_ms });
    }
    Ok(())
}

fn update_state(
    transaction: &Transaction<'_>,
    artifact: &ArtifactId,
    state: ArtifactState,
    revision: u64,
    now_ms: i64,
    retain_lease: bool,
) -> Result<()> {
    let revision = sqlite_i64(revision, "revision")?;
    if retain_lease {
        transaction.execute(
            "UPDATE artifacts SET state = ?1, revision = ?2, updated_at_ms = ?3 \
             WHERE artifact_id = ?4",
            params![state.as_str(), revision, now_ms, artifact.as_str()],
        )?;
    } else {
        transaction.execute(
            "UPDATE artifacts SET state = ?1, revision = ?2, updated_at_ms = ?3, \
             lease_token = NULL, lease_owner = NULL, lease_expires_ms = NULL \
             WHERE artifact_id = ?4",
            params![state.as_str(), revision, now_ms, artifact.as_str()],
        )?;
    }
    Ok(())
}

fn quarantine_published(
    transaction: &Transaction<'_>,
    artifact: &ArtifactId,
    revision: u64,
    now_ms: i64,
    reason: &str,
) -> Result<()> {
    let next = next_revision(revision)?;
    update_state(
        transaction,
        artifact,
        ArtifactState::Retired,
        next,
        now_ms,
        false,
    )?;
    insert_audit(
        transaction,
        artifact,
        Some(ArtifactState::Published),
        ArtifactState::Retired,
        next,
        now_ms,
        reason,
    )
}

#[allow(clippy::too_many_arguments)]
fn insert_audit(
    transaction: &Transaction<'_>,
    artifact: &ArtifactId,
    from: Option<ArtifactState>,
    to: ArtifactState,
    revision: u64,
    now_ms: i64,
    reason: &str,
) -> Result<()> {
    transaction.execute(
        "INSERT INTO artifact_audit \
         (artifact_id, from_state, to_state, revision, occurred_at_ms, reason) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            artifact.as_str(),
            from.map(ArtifactState::as_str),
            to.as_str(),
            sqlite_i64(revision, "revision")?,
            now_ms,
            reason,
        ],
    )?;
    Ok(())
}

fn manifest_objects(
    manifest: &ValidatedArtifactManifest,
) -> Result<BTreeMap<ObjectId, ObjectMetadata>> {
    let manifest = manifest.manifest();
    let mut objects = BTreeMap::new();
    insert_object(
        &mut objects,
        manifest.decoded.object_id.clone(),
        ObjectMetadata::new(manifest.decoded.bytes, manifest.decoded.sha256.clone())?,
    )?;
    for representation in &manifest.representations {
        insert_object(
            &mut objects,
            representation.object_id.clone(),
            ObjectMetadata::new(
                representation.encoded_bytes,
                representation.encoded_sha256.clone(),
            )?,
        )?;
    }
    Ok(objects)
}

fn insert_object(
    objects: &mut BTreeMap<ObjectId, ObjectMetadata>,
    object: ObjectId,
    metadata: ObjectMetadata,
) -> Result<()> {
    if objects
        .insert(object.clone(), metadata.clone())
        .is_some_and(|existing| existing != metadata)
    {
        return Err(CatalogError::ManifestObjectConflict(object));
    }
    Ok(())
}

fn next_revision(revision: u64) -> Result<u64> {
    revision
        .checked_add(1)
        .ok_or(CatalogError::RevisionExhausted)
}

fn sqlite_i64(value: u64, field: &'static str) -> Result<i64> {
    i64::try_from(value).map_err(|_| CatalogError::SqliteIntegerRange { field, value })
}

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("artifact catalog busy timeout must be positive")]
    InvalidBusyTimeout,
    #[error("artifact catalog I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("artifact catalog SQLite operation failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("artifact catalog JSON operation failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("stored artifact manifest is invalid: {0}")]
    Manifest(#[from] ManifestError),
    #[error("blob store verification failed: {0}")]
    Storage(#[from] StorageError),
    #[error("stored object ID is invalid: {0}")]
    Identifier(#[from] sync_types::IdentifierError),
    #[error("artifact catalog path is unsafe: {0}")]
    UnsafeCatalogPath(PathBuf),
    #[error("artifact catalog journal mode is {0}, expected WAL")]
    JournalMode(String),
    #[error("artifact catalog schema version {0} is unsupported")]
    UnsupportedSchemaVersion(i64),
    #[error("{state_dir} holds an artifact catalog an earlier release of calque wrote (schema version {version}), and calque does not migrate it.", state_dir = state_dir.display())]
    EarlierSchemaVersion { state_dir: PathBuf, version: i64 },
    #[error("build lease owner/duration is invalid")]
    InvalidLease,
    #[error("abandoned-build cleanup owner, grace, duration, or reason is invalid")]
    InvalidBuildCleanup,
    #[error("build lease timestamp overflowed")]
    TimestampOverflow,
    #[error("artifact already exists: {0}")]
    ArtifactAlreadyExists(ArtifactId),
    #[error("artifact {0} was refused publication")]
    PublicationRefused(ArtifactId),
    #[error("artifact ID {0} already exists with different immutable manifest bytes")]
    ArtifactManifestConflict(ArtifactId),
    #[error("artifact does not exist: {0}")]
    UnknownArtifact(ArtifactId),
    #[error("artifact catalog state is corrupt: {0}")]
    CorruptState(String),
    #[error("published artifact index columns disagree with the stored manifest")]
    CorruptPublishedIndex,
    #[error("artifact revision is corrupt: {0}")]
    CorruptRevision(i64),
    #[error("artifact object length is corrupt: {0}")]
    CorruptObjectLength(i64),
    #[error("artifact build lease is missing")]
    BuildLeaseMissing,
    #[error("artifact build lease is stale")]
    StaleBuildLease,
    #[error("artifact build lease expired at {expires}, current time is {now_ms}")]
    BuildLeaseExpired { expires: i64, now_ms: i64 },
    #[error("artifact build lease remains active until {expires}, current time is {now_ms}")]
    BuildLeaseStillActive { expires: i64, now_ms: i64 },
    #[error("artifact in state {0:?} is not an active build")]
    ArtifactIsNotBuilding(ArtifactState),
    #[error("artifact build is already abandoned: {0}")]
    ArtifactBuildAbandoned(ArtifactId),
    #[error("artifact build is not abandoned: {0}")]
    ArtifactBuildIsNotAbandoned(ArtifactId),
    #[error("abandoned-build cleanup lease is stale")]
    StaleBuildCleanupLease,
    #[error(
        "abandoned-build cleanup remains in grace until {not_before}, current time is {now_ms}"
    )]
    BuildCleanupGraceActive { not_before: i64, now_ms: i64 },
    #[error("abandoned-build cleanup lease expired at {expires}, current time is {now_ms}")]
    BuildCleanupLeaseExpired { expires: i64, now_ms: i64 },
    #[error(
        "abandoned-build cleanup lease remains active until {expires}, current time is {now_ms}"
    )]
    BuildCleanupLeaseStillActive { expires: i64, now_ms: i64 },
    #[error("abandoned-build cleanup object count overflowed")]
    CleanupCountOverflow,
    #[error("stored abandoned-build cleanup count is corrupt: {0}")]
    CorruptCleanupCount(i64),
    #[error("artifact in state {0:?} is not published")]
    ArtifactIsNotPublished(ArtifactState),
    #[error("artifact is pinned by an issued bootstrap plan until {expires_ms}")]
    ArtifactPinnedByPlan { expires_ms: i64 },
    #[error("bootstrap plan lease must expire after its creation time")]
    InvalidPlanLease,
    #[error("bootstrap plan contains the same artifact more than once")]
    DuplicatePlanArtifact,
    #[error("invalid artifact transition from {actual:?}; expected {expected:?} before {next:?}")]
    InvalidTransition {
        actual: ArtifactState,
        expected: ArtifactState,
        next: ArtifactState,
    },
    #[error("artifact object is missing: {0}")]
    MissingObject(ObjectId),
    #[error(
        "artifact object metadata differs for {object}: expected {expected:?}, found {actual:?}"
    )]
    ObjectMetadataMismatch {
        object: ObjectId,
        expected: ObjectMetadata,
        actual: ObjectMetadata,
    },
    #[error("artifact manifest assigns conflicting content to object {0}")]
    ManifestObjectConflict(ObjectId),
    #[error("artifact has no immutable objects: {0}")]
    NoObjects(ArtifactId),
    #[error("operator transition reason must not be empty")]
    EmptyReason,
    #[error("artifact revision space is exhausted")]
    RevisionExhausted,
    #[error("{field} value {value} exceeds SQLite's signed integer range")]
    SqliteIntegerRange { field: &'static str, value: u64 },
}

impl CatalogError {
    /// Whether only `calque destroy` gets past this refusal. The command needs
    /// the application's database URL variable, which the catalog does not
    /// know, so the caller names it.
    pub fn requires_destroying_state(&self) -> bool {
        matches!(self, Self::EarlierSchemaVersion { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::BTreeSet,
        io,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use artifact_contract::RepresentationEncoding;
    use async_trait::async_trait;
    use blob_store::{ByteStream, LocalFilesystemBlobStore, PutOutcome};
    use sha2::{Digest, Sha256};
    use sync_types::TableId;
    use tokio::io::AsyncWriteExt;

    const EXPECTED_CATALOG_SCHEMA_VERSION: i64 = 4;

    fn digest(bytes: &[u8]) -> String {
        let digest = Sha256::digest(bytes);
        format!("{digest:x}")
    }

    fn manifest(bytes: &[u8]) -> ValidatedArtifactManifest {
        manifest_with_artifact_id(bytes, "artifact-1")
    }

    fn manifest_with_artifact_id(bytes: &[u8], artifact_id: &str) -> ValidatedArtifactManifest {
        let zstd_digest = digest(&encoded_fixture(RepresentationEncoding::Zstd, bytes));
        let digest = digest(bytes);
        let value = serde_json::json!({
            "protocolVersion": 1,
            "artifactId": artifact_id,
            "createdAt": "2026-07-14T12:00:00Z",
            "decoded": {
                "objectId": format!("sha256:{digest}"),
                "sha256": digest,
                "bytes": bytes.len(),
                "mediaType": "application/vnd.calque.direct-columnar.v4",
                "format": "directColumnar",
                "schemaHash": format!("sha256:{}", "a".repeat(64)),
                "collectionSetHash": format!("sha256:{}", "8".repeat(64)),
                "formatVersion": 4,
                "identity": { "manifestVersion": "manifest-1", "authorizationScopeVersion": "scope-1" },
                "schema": {
                    "format": "calque-direct-columnar",
                    "version": 4,
                    "tables": [{
                        "name": "tasks",
                        "primaryKey": ["id"],
                        "columns": [{ "name": "id", "type": "int64", "nullable": false }]
                    }]
                },
                "chunks": { "bytes": 4194304, "sha256": [format!("sha256:{digest}")] }
            },
            "pit": {
                "sourceEpoch": "epoch-1",
                "snapshotCursor": "cursor-1",
                "copyConsistentLsn": "0/A",
                "cutMessageLsn": "0/B",
                "cutMarkerId": "build-1"
            },
            "compatibility": {
                "publicationGeneration": 1,
                "schemaGeneration": 2,
                "authorizationGeneration": 3,
                "authorizationPolicyHash": format!("sha256:{}", "9".repeat(64)),
                "authorizationScopeHash": format!("sha256:{}", "b".repeat(64)),
                "tableSetHash": format!("sha256:{}", "c".repeat(64)),
                "shardProfileHash": format!("sha256:{}", "d".repeat(64)),
                "indexProfileHash": format!("sha256:{}", "e".repeat(64)),
                "typeCodecGeneration": 4,
                "collationGeneration": 5
            },
            "tables": [{
                "tableId": "public.tasks",
                "sourceSchema": "public",
                "sourceTable": "tasks",
                "localSchema": "main",
                "localTable": "tasks",
                "schemaHash": format!("sha256:{}", "f".repeat(64)),
                "columnProfileHash": format!("sha256:{}", "1".repeat(64)),
                "rowCount": 2,
                "primaryKeyCodec": "pk-v1/protobuf",
                "columns": [{ "columnId": 1, "name": "id" }]
            }],
            "representations": [
                {
                    "representationId": format!("sha256:{zstd_digest}"),
                    "objectId": format!("sha256:{zstd_digest}"),
                    "encoding": "zstd",
                    "encodedSha256": zstd_digest,
                    "encodedBytes": bytes.len() + 1,
                    "zstd": { "level": 3, "windowLog": 23 }
                },
                {
                    "representationId": format!("sha256:{digest}"),
                    "objectId": format!("sha256:{digest}"),
                    "encoding": "identity",
                    "encodedSha256": digest,
                    "encodedBytes": bytes.len()
                }
            ],
            "memory": {
                "estimatedPeakBytes": 1000,
                "recommendedGrowthReserveBytes": 500
            }
        });
        ValidatedArtifactManifest::parse(&serde_json::to_vec(&value).unwrap()).unwrap()
    }

    fn encoded_fixture(encoding: RepresentationEncoding, decoded: &[u8]) -> Vec<u8> {
        let prefix = match encoding {
            RepresentationEncoding::Zstd => b'z',
            RepresentationEncoding::Identity => return decoded.to_vec(),
            RepresentationEncoding::BrotliLargeWindow => b'b',
            RepresentationEncoding::PackedBrotliLargeWindow => b'p',
        };
        let mut encoded = Vec::with_capacity(decoded.len() + 1);
        encoded.push(prefix);
        encoded.extend_from_slice(decoded);
        encoded
    }

    fn stream(bytes: &[u8]) -> ByteStream {
        let (mut writer, reader) = tokio::io::duplex(bytes.len().max(1));
        let bytes = bytes.to_vec();
        tokio::spawn(async move {
            writer.write_all(&bytes).await.unwrap();
        });
        Box::pin(reader)
    }

    async fn upload_all_manifest_objects(
        store: &LocalFilesystemBlobStore,
        manifest: &ValidatedArtifactManifest,
        bytes: &[u8],
    ) {
        let decoded_metadata = ObjectMetadata::new(bytes.len() as u64, digest(bytes)).unwrap();
        store
            .put_if_absent(
                &manifest.manifest().decoded.object_id,
                stream(bytes),
                &decoded_metadata,
            )
            .await
            .unwrap();
        for representation in &manifest.manifest().representations {
            let encoded = encoded_fixture(representation.encoding, bytes);
            let metadata = ObjectMetadata::new(encoded.len() as u64, digest(&encoded)).unwrap();
            store
                .put_if_absent(&representation.object_id, stream(&encoded), &metadata)
                .await
                .unwrap();
        }
    }

    fn query(manifest: &ValidatedArtifactManifest) -> PublishedArtifactQuery {
        PublishedArtifactQuery {
            cache_key: manifest.cache_key(),
            table_set_hash: manifest.manifest().compatibility.table_set_hash.clone(),
        }
    }

    fn catalog_schema_version(path: &Path) -> i64 {
        Connection::open(path)
            .unwrap()
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap()
    }

    #[test]
    fn catalog_rejects_a_future_schema_version_without_changing_it() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("catalog.sqlite3");
        drop(SqliteArtifactCatalog::open(&path).unwrap());
        let future = EXPECTED_CATALOG_SCHEMA_VERSION + 1;
        Connection::open(&path)
            .unwrap()
            .pragma_update(None, "user_version", future)
            .unwrap();

        assert!(matches!(
            SqliteArtifactCatalog::open(&path),
            Err(CatalogError::UnsupportedSchemaVersion(version)) if version == future
        ));
        assert_eq!(catalog_schema_version(&path), future);
    }

    /// An earlier release's catalog, versioned or not, is refused rather than
    /// migrated and left as it was, and the refusal names its state directory.
    /// A new catalog and one already at the current version are the control:
    /// both open.
    #[test]
    fn an_earlier_catalog_is_refused_naming_its_state_directory() {
        for earlier in 0..EXPECTED_CATALOG_SCHEMA_VERSION {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("catalog.sqlite3");
            drop(SqliteArtifactCatalog::open(&path).unwrap());
            drop(SqliteArtifactCatalog::open(&path).unwrap());
            assert_eq!(
                catalog_schema_version(&path),
                EXPECTED_CATALOG_SCHEMA_VERSION
            );
            Connection::open(&path)
                .unwrap()
                .pragma_update(None, "user_version", earlier)
                .unwrap();

            let error = SqliteArtifactCatalog::open(&path)
                .err()
                .expect("an earlier catalog is refused");
            assert!(
                matches!(
                    error,
                    CatalogError::EarlierSchemaVersion { version, .. } if version == earlier
                ),
                "{error}"
            );
            assert!(error.requires_destroying_state(), "{error}");
            assert_eq!(
                error.to_string(),
                format!(
                    "{} holds an artifact catalog an earlier release of calque wrote (schema version {earlier}), and calque does not migrate it.",
                    directory.path().display()
                )
            );
            assert_eq!(catalog_schema_version(&path), earlier);
        }
    }

    /// A later release's catalog and a failed read leave the state directory
    /// as it is: neither asks for `calque destroy`.
    #[test]
    fn only_an_earlier_catalog_requires_destroying_state() {
        for error in [
            CatalogError::UnsupportedSchemaVersion(EXPECTED_CATALOG_SCHEMA_VERSION + 1),
            CatalogError::Io(io::Error::other("catalog read failed")),
        ] {
            assert!(!error.requires_destroying_state(), "{error}");
        }
    }

    async fn published_artifact(
        catalog: &mut SqliteArtifactCatalog,
        store: &LocalFilesystemBlobStore,
        bytes: &[u8],
        artifact_id: &str,
    ) -> ValidatedArtifactManifest {
        let manifest = manifest_with_artifact_id(bytes, artifact_id);
        let lease = catalog
            .register_build(&manifest, "worker-1", 1_000, 1_000_000)
            .unwrap();
        catalog.mark_uploading(&lease, 1_100).unwrap();
        upload_all_manifest_objects(store, &manifest, bytes).await;
        catalog.mark_uploaded(&lease, 1_200).unwrap();
        assert_eq!(
            catalog
                .verify_and_publish(&lease, store, 1_300)
                .await
                .unwrap(),
            PublishOutcome::Published
        );
        manifest
    }

    /// A superseded artifact leaves the published set at once, pinned or not,
    /// and its objects outlive every plan that pins it and every artifact
    /// that shares them.
    #[tokio::test]
    async fn a_superseded_artifact_is_retired_through_its_pins_and_deleted_after_them() {
        let directory = tempfile::tempdir().unwrap();
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("blobs"), 1024).unwrap();
        let bytes = b"one tenant's working set";
        let old = published_artifact(&mut catalog, &store, bytes, "artifact-old").await;
        let new = published_artifact(&mut catalog, &store, bytes, "artifact-new").await;
        let old_id = old.manifest().artifact_id.clone();
        let new_id = new.manifest().artifact_id.clone();
        catalog
            .pin_published_for_plan(
                &PlanId::new("plan-1").unwrap(),
                std::slice::from_ref(&old_id),
                20_000,
                10_000,
            )
            .unwrap();
        assert!(matches!(
            catalog.retire(&old_id, 10_001, "replaced"),
            Err(CatalogError::ArtifactPinnedByPlan { .. })
        ));
        catalog
            .retire_superseded(&old_id, 10_001, "a row moved between tenants")
            .unwrap();
        assert_eq!(
            catalog.artifacts_in_state(ArtifactState::Retired).unwrap(),
            std::slice::from_ref(&old_id)
        );
        assert_eq!(
            catalog
                .artifacts_in_state(ArtifactState::Published)
                .unwrap(),
            std::slice::from_ref(&new_id)
        );
        let published = catalog.published(&query(&old)).unwrap();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].manifest().artifact_id, new_id);
        assert!(matches!(
            catalog
                .delete_retired_artifact(&new_id, &store, 10_002)
                .await,
            Err(CatalogError::InvalidTransition { .. })
        ));

        // The plan still holds the old artifact: nothing is deleted, one by
        // one or all at once.
        assert!(catalog
            .delete_retired(&store, 10_002)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            catalog
                .delete_retired_artifact(&old_id, &store, 10_002)
                .await
                .unwrap(),
            None
        );
        // Once it has expired, the old row goes, and each object it shares
        // with the published artifact stays.
        assert_eq!(
            catalog
                .delete_retired_artifact(&old_id, &store, 20_001)
                .await
                .unwrap(),
            Some(0)
        );
        assert_eq!(catalog.state(&old_id).unwrap(), None);
        let decoded = new.manifest().decoded.object_id.clone();
        assert!(store.head(&decoded).await.unwrap().is_some());

        catalog.retire(&new_id, 20_002, "evicted").unwrap();
        let deleted = catalog
            .delete_retired_artifact(&new_id, &store, 20_003)
            .await
            .unwrap()
            .unwrap();
        let objects = new
            .manifest()
            .representations
            .iter()
            .map(|representation| &representation.object_id)
            .chain([&decoded])
            .collect::<BTreeSet<_>>();
        assert_eq!(deleted, u64::try_from(objects.len()).unwrap());
        assert!(store.head(&decoded).await.unwrap().is_none());
        for representation in &new.manifest().representations {
            assert!(store
                .head(&representation.object_id)
                .await
                .unwrap()
                .is_none());
        }
    }

    #[tokio::test]
    async fn only_verified_objects_cross_the_atomic_publication_boundary() {
        let directory = tempfile::tempdir().unwrap();
        let catalog_path = directory.path().join("catalog.sqlite3");
        let blob_root = directory.path().join("blobs");
        let bytes = b"finalized artifact bytes";
        let manifest = manifest(bytes);
        let mut catalog = SqliteArtifactCatalog::open(&catalog_path).unwrap();
        let store = LocalFilesystemBlobStore::open(&blob_root, 1024).unwrap();
        let lease = catalog
            .register_build(&manifest, "worker-1", 1_000, 1_000)
            .unwrap();

        assert_eq!(catalog.published(&query(&manifest)).unwrap().len(), 0);
        assert_eq!(catalog.mark_uploading(&lease, 1_100).unwrap(), 1);
        assert_eq!(catalog.mark_uploading(&lease, 1_101).unwrap(), 1);
        let object = manifest.manifest().decoded.object_id.clone();
        upload_all_manifest_objects(&store, &manifest, bytes).await;
        assert_eq!(catalog.mark_uploaded(&lease, 1_200).unwrap(), 2);
        assert_eq!(catalog.mark_uploaded(&lease, 1_201).unwrap(), 2);
        assert_eq!(
            catalog
                .verify_and_publish(&lease, &store, 1_300)
                .await
                .unwrap(),
            PublishOutcome::Published
        );
        assert_eq!(
            catalog
                .verify_and_publish(&lease, &store, 9_999)
                .await
                .unwrap(),
            PublishOutcome::AlreadyPublished
        );
        assert_eq!(
            catalog
                .published_compatible_for_tables(
                    &manifest.cache_key(),
                    &BTreeSet::from([TableId::new("public.tasks").unwrap()]),
                    10,
                )
                .unwrap()
                .len(),
            1
        );
        assert!(catalog
            .published_compatible_for_tables(
                &manifest.cache_key(),
                &BTreeSet::from([TableId::new("public.unrelated").unwrap()]),
                10,
            )
            .unwrap()
            .is_empty());
        catalog
            .pin_published_for_plan(
                &PlanId::new("plan-1").unwrap(),
                &[lease.artifact_id().clone()],
                20_000,
                10_000,
            )
            .unwrap();
        // A pin that has already expired, or that names one artifact twice,
        // is refused before it is written.
        let plan = PlanId::new("plan-2").unwrap();
        assert!(matches!(
            catalog.pin_published_for_plan(&plan, &[lease.artifact_id().clone()], 10_000, 10_000),
            Err(CatalogError::InvalidPlanLease)
        ));
        assert!(matches!(
            catalog.pin_published_for_plan(
                &plan,
                &[lease.artifact_id().clone(), lease.artifact_id().clone()],
                20_000,
                10_000,
            ),
            Err(CatalogError::DuplicatePlanArtifact)
        ));
        assert!(matches!(
            catalog.retire(lease.artifact_id(), 10_001, "operator cleanup"),
            Err(CatalogError::ArtifactPinnedByPlan { expires_ms: 20_000 })
        ));
        store.delete(&object).await.unwrap();
        assert!(matches!(
            catalog.verify_and_publish(&lease, &store, 10_000).await,
            Err(CatalogError::MissingObject(_))
        ));
        assert_eq!(catalog.published(&query(&manifest)).unwrap().len(), 0);
        assert_eq!(
            catalog.state(lease.artifact_id()).unwrap(),
            Some(ArtifactState::Retired)
        );
        assert_eq!(
            catalog
                .audit(lease.artifact_id())
                .unwrap()
                .iter()
                .map(|entry| entry.to_state)
                .collect::<Vec<_>>(),
            vec![
                ArtifactState::Building,
                ArtifactState::Uploading,
                ArtifactState::Uploaded,
                ArtifactState::Verified,
                ArtifactState::Published,
                ArtifactState::Retired,
            ]
        );

        drop(catalog);
        let reopened = SqliteArtifactCatalog::open(&catalog_path).unwrap();
        assert_eq!(reopened.published(&query(&manifest)).unwrap().len(), 0);
    }

    /// A refused build is neither visible nor moved, and the same lease
    /// publishes it once `admit` holds.
    #[tokio::test]
    async fn an_admitted_publication_is_visible_only_once_admit_holds() {
        let directory = tempfile::tempdir().unwrap();
        let bytes = b"admitted bytes";
        let manifest = manifest(bytes);
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("blobs"), 1024).unwrap();
        let lease = catalog
            .register_build(&manifest, "worker", 100, 1_000)
            .unwrap();
        catalog.mark_uploading(&lease, 110).unwrap();
        upload_all_manifest_objects(&store, &manifest, bytes).await;
        catalog.mark_uploaded(&lease, 120).unwrap();
        let mut called = 0;
        assert!(matches!(
            catalog
                .verify_and_publish_admitted(&lease, &store, 130, || {
                    called += 1;
                    false
                })
                .await,
            Err(CatalogError::PublicationRefused(artifact)) if &artifact == lease.artifact_id()
        ));
        assert_eq!(called, 1);
        assert_eq!(
            catalog.state(lease.artifact_id()).unwrap(),
            Some(ArtifactState::Uploaded)
        );
        assert!(catalog.published(&query(&manifest)).unwrap().is_empty());
        assert_eq!(
            catalog
                .verify_and_publish_admitted(&lease, &store, 140, || true)
                .await
                .unwrap(),
            PublishOutcome::Published
        );
        assert_eq!(catalog.published(&query(&manifest)).unwrap(), [manifest]);
    }

    async fn publish_fixture(
        catalog: &mut SqliteArtifactCatalog,
        store: &LocalFilesystemBlobStore,
        bytes: &[u8],
        artifact_id: &str,
    ) -> ValidatedArtifactManifest {
        let manifest = manifest_with_artifact_id(bytes, artifact_id);
        let lease = catalog
            .register_build(&manifest, "worker", 100, 1_000)
            .unwrap();
        catalog.mark_uploading(&lease, 110).unwrap();
        upload_all_manifest_objects(store, &manifest, bytes).await;
        catalog.mark_uploaded(&lease, 120).unwrap();
        catalog
            .verify_and_publish(&lease, store, 130)
            .await
            .unwrap();
        manifest
    }

    /// An expired pin stops holding an artifact, an unexpired pin still holds
    /// its own, and an object two retired artifacts share goes with the
    /// second.
    #[tokio::test]
    async fn expired_pins_let_retired_artifacts_and_their_objects_be_deleted() {
        let directory = tempfile::tempdir().unwrap();
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("blobs"), 1024).unwrap();
        let old = publish_fixture(&mut catalog, &store, b"old cut", "cut-old").await;
        let twin = publish_fixture(&mut catalog, &store, b"old cut", "cut-old-twin").await;
        let kept = publish_fixture(&mut catalog, &store, b"kept cut", "cut-kept").await;
        let id = |manifest: &ValidatedArtifactManifest| manifest.manifest().artifact_id.clone();
        catalog
            .pin_published_for_plan(&PlanId::new("plan-sooner").unwrap(), &[id(&old)], 300, 200)
            .unwrap();
        catalog
            .pin_published_for_plan(
                &PlanId::new("plan-later").unwrap(),
                &[id(&kept)],
                50_000,
                200,
            )
            .unwrap();
        assert!(matches!(
            catalog.retire(&id(&old), 299, "re-cut"),
            Err(CatalogError::ArtifactPinnedByPlan { expires_ms: 300 })
        ));

        catalog.retire(&id(&old), 300, "re-cut").unwrap();
        catalog.retire(&id(&twin), 300, "re-cut").unwrap();
        assert!(matches!(
            catalog.retire(&id(&kept), 300, "re-cut"),
            Err(CatalogError::ArtifactPinnedByPlan { expires_ms: 50_000 })
        ));
        assert_eq!(
            catalog.delete_retired(&store, 400).await.unwrap(),
            [id(&old), id(&twin)]
        );
        assert_eq!(catalog.state(&id(&old)).unwrap(), None);
        assert_eq!(catalog.state(&id(&twin)).unwrap(), None);
        assert!(store
            .head(&old.manifest().decoded.object_id)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            catalog.audit(&id(&old)).unwrap().last().unwrap().reason,
            "retired artifact deleted"
        );
        assert!(catalog
            .delete_retired(&store, 400)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            catalog.published(&query(&kept)).unwrap(),
            std::slice::from_ref(&kept)
        );
        assert!(store
            .head(&kept.manifest().decoded.object_id)
            .await
            .unwrap()
            .is_some());
    }

    /// A published artifact sharing a retired one's objects keeps them.
    #[tokio::test]
    async fn deleting_a_retired_artifact_keeps_objects_a_published_one_names() {
        let directory = tempfile::tempdir().unwrap();
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("blobs"), 1024).unwrap();
        let retired = publish_fixture(&mut catalog, &store, b"shared cut", "cut-retired").await;
        let live = publish_fixture(&mut catalog, &store, b"shared cut", "cut-live").await;
        catalog
            .retire(&retired.manifest().artifact_id, 300, "re-cut")
            .unwrap();
        assert_eq!(
            catalog.delete_retired(&store, 400).await.unwrap(),
            [retired.manifest().artifact_id.clone()]
        );
        assert!(store
            .head(&live.manifest().decoded.object_id)
            .await
            .unwrap()
            .is_some());
        assert_eq!(catalog.published(&query(&live)).unwrap(), [live]);
    }

    #[tokio::test]
    async fn missing_or_wrong_object_metadata_cannot_publish() {
        let directory = tempfile::tempdir().unwrap();
        let bytes = b"expected bytes";
        let manifest = manifest(bytes);
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("blobs"), 1024).unwrap();
        let lease = catalog
            .register_build(&manifest, "worker", 100, 1_000)
            .unwrap();
        catalog.mark_uploading(&lease, 110).unwrap();
        catalog.mark_uploaded(&lease, 120).unwrap();
        assert!(matches!(
            catalog.verify_and_publish(&lease, &store, 130).await,
            Err(CatalogError::MissingObject(_))
        ));
        assert_eq!(
            catalog.state(lease.artifact_id()).unwrap(),
            Some(ArtifactState::Uploaded)
        );

        let object = manifest.manifest().decoded.object_id.clone();
        upload_all_manifest_objects(&store, &manifest, bytes).await;
        assert_eq!(
            catalog
                .verify_and_publish(&lease, &store, 140)
                .await
                .unwrap(),
            PublishOutcome::Published
        );

        let mut wrong_catalog =
            SqliteArtifactCatalog::open(directory.path().join("wrong-catalog.sqlite3")).unwrap();
        let wrong_store =
            LocalFilesystemBlobStore::open(directory.path().join("wrong-blobs"), 1024).unwrap();
        let wrong_lease = wrong_catalog
            .register_build(&manifest, "worker", 100, 1_000)
            .unwrap();
        wrong_catalog.mark_uploading(&wrong_lease, 110).unwrap();
        wrong_catalog.mark_uploaded(&wrong_lease, 120).unwrap();
        for representation in manifest
            .manifest()
            .representations
            .iter()
            .filter(|representation| representation.object_id != object)
        {
            let encoded = encoded_fixture(representation.encoding, bytes);
            wrong_store
                .put_if_absent(
                    &representation.object_id,
                    stream(&encoded),
                    &ObjectMetadata::new(encoded.len() as u64, digest(&encoded)).unwrap(),
                )
                .await
                .unwrap();
        }
        let wrong = b"wrong bytes";
        let wrong_metadata = ObjectMetadata::new(wrong.len() as u64, digest(wrong)).unwrap();
        wrong_store
            .put_if_absent(&object, stream(wrong), &wrong_metadata)
            .await
            .unwrap();
        assert!(matches!(
            wrong_catalog
                .verify_and_publish(&wrong_lease, &wrong_store, 140)
                .await,
            Err(CatalogError::ObjectMetadataMismatch { .. })
        ));
        assert_eq!(wrong_catalog.published(&query(&manifest)).unwrap().len(), 0);
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum DeterministicHeadFault {
        ProviderUnavailable,
        ProviderDenied,
        NotVisible,
        WrongBytes,
        WrongSha256,
    }

    struct DeterministicHeadFaultStore<'a> {
        inner: &'a LocalFilesystemBlobStore,
        seed: usize,
        fault: DeterministicHeadFault,
        fail_on: usize,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl BlobStore for DeterministicHeadFaultStore<'_> {
        async fn put_if_absent(
            &self,
            object: &ObjectId,
            body: ByteStream,
            metadata: &ObjectMetadata,
        ) -> blob_store::Result<PutOutcome> {
            self.inner.put_if_absent(object, body, metadata).await
        }

        async fn head(&self, object: &ObjectId) -> blob_store::Result<Option<ObjectMetadata>> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call != self.fail_on {
                return self.inner.head(object).await;
            }
            match self.fault {
                DeterministicHeadFault::ProviderUnavailable => Err(StorageError::Provider {
                    operation: "head-object",
                    code: format!("InjectedUnavailable{}", self.seed),
                }),
                DeterministicHeadFault::ProviderDenied => Err(StorageError::Provider {
                    operation: "head-object",
                    code: format!("InjectedDenied{}", self.seed),
                }),
                DeterministicHeadFault::NotVisible => Ok(None),
                DeterministicHeadFault::WrongBytes => {
                    let mut metadata = self
                        .inner
                        .head(object)
                        .await?
                        .ok_or_else(|| StorageError::ObjectDisappeared(object.clone()))?;
                    metadata.bytes += 1;
                    Ok(Some(metadata))
                }
                DeterministicHeadFault::WrongSha256 => {
                    let mut metadata = self
                        .inner
                        .head(object)
                        .await?
                        .ok_or_else(|| StorageError::ObjectDisappeared(object.clone()))?;
                    metadata.sha256 = "0".repeat(64);
                    Ok(Some(metadata))
                }
            }
        }

        async fn get(&self, object: &ObjectId) -> blob_store::Result<Option<ByteStream>> {
            self.inner.get(object).await
        }

        async fn delete(&self, object: &ObjectId) -> blob_store::Result<()> {
            self.inner.delete(object).await
        }
    }

    #[tokio::test]
    async fn one_hundred_deterministic_provider_faults_never_publish() {
        let directory = tempfile::tempdir().unwrap();
        let bytes = b"provider fault matrix bytes";
        let manifest = manifest(bytes);
        let store =
            LocalFilesystemBlobStore::open(directory.path().join("fault-blobs"), 1024).unwrap();
        upload_all_manifest_objects(&store, &manifest, bytes).await;
        let object_count = std::iter::once(&manifest.manifest().decoded.object_id)
            .chain(
                manifest
                    .manifest()
                    .representations
                    .iter()
                    .map(|representation| &representation.object_id),
            )
            .collect::<BTreeSet<_>>()
            .len();
        assert!(object_count > 0);
        let fault_kinds = [
            DeterministicHeadFault::ProviderUnavailable,
            DeterministicHeadFault::ProviderDenied,
            DeterministicHeadFault::NotVisible,
            DeterministicHeadFault::WrongBytes,
            DeterministicHeadFault::WrongSha256,
        ];
        let mut covered = BTreeSet::new();

        for seed in 0..100 {
            let mut catalog = SqliteArtifactCatalog::open(
                directory.path().join(format!("fault-{seed:03}.sqlite3")),
            )
            .unwrap();
            let lease = catalog
                .register_build(&manifest, "fault-matrix", 100, 1_000)
                .unwrap();
            catalog.mark_uploading(&lease, 110).unwrap();
            catalog.mark_uploaded(&lease, 120).unwrap();
            let fault = fault_kinds[seed % fault_kinds.len()];
            let fail_on = (seed / fault_kinds.len()) % object_count;
            covered.insert((fault, fail_on));
            let faulted = DeterministicHeadFaultStore {
                inner: &store,
                seed,
                fault,
                fail_on,
                calls: AtomicUsize::new(0),
            };
            assert!(
                catalog
                    .verify_and_publish(&lease, &faulted, 130)
                    .await
                    .is_err(),
                "seed {seed} crossed publication despite its injected provider fault"
            );
            assert_eq!(
                catalog.state(lease.artifact_id()).unwrap(),
                Some(ArtifactState::Uploaded),
                "seed {seed} changed catalog state before complete verification"
            );
            assert!(
                catalog.published(&query(&manifest)).unwrap().is_empty(),
                "seed {seed} exposed an unverified artifact"
            );
            assert_eq!(
                catalog
                    .verify_and_publish(&lease, &store, 140)
                    .await
                    .unwrap(),
                PublishOutcome::Published,
                "seed {seed} did not recover through an unfaulted exact retry"
            );
        }
        assert_eq!(
            covered.len(),
            fault_kinds.len() * object_count,
            "the 100 schedules must cover every fault-kind by object-order pair"
        );
    }

    #[test]
    fn lease_expiry_ordering_and_duplicate_identity_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let manifest = manifest(b"bytes");
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let lease = catalog
            .register_build(&manifest, "worker", 1_000, 10)
            .unwrap();
        assert!(matches!(
            catalog.mark_uploaded(&lease, 1_001),
            Err(CatalogError::InvalidTransition { .. })
        ));
        assert!(matches!(
            catalog.mark_uploading(&lease, 1_010),
            Err(CatalogError::BuildLeaseExpired { .. })
        ));
        assert!(matches!(
            catalog.register_build(&manifest, "worker-2", 2_000, 10),
            Err(CatalogError::ArtifactAlreadyExists(_))
        ));
    }

    #[test]
    fn lease_renewal_extends_only_a_live_matching_capability() {
        let directory = tempfile::tempdir().unwrap();
        let manifest = manifest(b"bytes");
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let lease = catalog
            .register_build(&manifest, "worker", 1_000, 10)
            .unwrap();
        catalog.renew_lease(&lease, 1_005, 100).unwrap();
        catalog.mark_uploading(&lease, 1_050).unwrap();

        let mut stale = lease.clone();
        stale.token = Uuid::new_v4().to_string();
        assert!(matches!(
            catalog.renew_lease(&stale, 1_051, 100),
            Err(CatalogError::StaleBuildLease)
        ));

        assert!(matches!(
            catalog.take_over_expired_build(lease.artifact_id(), "worker-2", 1_060, 100),
            Err(CatalogError::BuildLeaseStillActive { .. })
        ));
        let replacement = catalog
            .take_over_expired_build(lease.artifact_id(), "worker-2", 1_105, 100)
            .unwrap();
        assert!(matches!(
            catalog.mark_uploaded(&lease, 1_106),
            Err(CatalogError::StaleBuildLease)
        ));
        assert_eq!(catalog.mark_uploaded(&replacement, 1_106).unwrap(), 3);
    }

    #[test]
    fn identical_interrupted_build_resumes_and_refences_the_old_worker() {
        let directory = tempfile::tempdir().unwrap();
        let first_manifest = manifest(b"bytes");
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let original = catalog
            .register_build(&first_manifest, "worker", 1_000, 1_000)
            .unwrap();
        catalog.mark_uploading(&original, 1_010).unwrap();

        let resumed = catalog
            .register_or_resume_build(&first_manifest, "worker", 1_020, 1_000)
            .unwrap();
        assert_eq!(resumed.state(), ArtifactState::Uploading);
        assert!(matches!(
            catalog.mark_uploaded(&original, 1_030),
            Err(CatalogError::StaleBuildLease)
        ));
        catalog.mark_uploaded(resumed.lease(), 1_030).unwrap();

        let conflicting = manifest(b"different bytes");
        assert!(matches!(
            catalog.register_or_resume_build(&conflicting, "worker", 1_040, 1_000),
            Err(CatalogError::ArtifactManifestConflict(_))
        ));
    }

    #[tokio::test]
    async fn abandoned_build_cleanup_is_graced_fenced_and_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let manifest = manifest(b"abandoned bytes");
        let artifact = manifest.manifest().artifact_id.clone();
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("objects"), 1024).unwrap();
        let build = catalog
            .register_build(&manifest, "builder", 100, 100)
            .unwrap();
        let cleanup = catalog
            .abandon_build(&build, "cleanup-a", 101, 10, 10, "snapshot cancelled")
            .unwrap();
        assert_eq!(cleanup.artifact_id, artifact);
        assert_eq!(cleanup.owner, "cleanup-a");
        assert_eq!(cleanup.not_before_ms, 111);
        assert_eq!(cleanup.expires_ms, 121);
        assert_eq!(
            catalog
                .abandon_build(&build, "cleanup-a", 102, 99, 99, "lost response retry")
                .unwrap(),
            cleanup
        );
        assert!(matches!(
            catalog.mark_uploading(&build, 103),
            Err(CatalogError::StaleBuildLease)
        ));
        assert!(matches!(
            catalog.register_or_resume_build(&manifest, "builder", 103, 100),
            Err(CatalogError::ArtifactBuildAbandoned(_))
        ));
        assert!(matches!(
            catalog.cleanup_abandoned_build(&cleanup, &store, 110).await,
            Err(CatalogError::BuildCleanupGraceActive { .. })
        ));

        let mut forged = cleanup.clone();
        forged.token = Uuid::new_v4().to_string();
        assert!(matches!(
            catalog.cleanup_abandoned_build(&forged, &store, 111).await,
            Err(CatalogError::StaleBuildCleanupLease)
        ));
        assert!(matches!(
            catalog.take_over_abandoned_build_cleanup(&artifact, "cleanup-b", 120, 20),
            Err(CatalogError::BuildCleanupLeaseStillActive { .. })
        ));
        assert!(matches!(
            catalog.cleanup_abandoned_build(&cleanup, &store, 121).await,
            Err(CatalogError::BuildCleanupLeaseExpired {
                expires: 121,
                now_ms: 121
            })
        ));
        let replacement = catalog
            .take_over_abandoned_build_cleanup(&artifact, "cleanup-b", 121, 20)
            .unwrap();
        assert!(matches!(
            catalog.cleanup_abandoned_build(&cleanup, &store, 122).await,
            Err(CatalogError::StaleBuildCleanupLease)
        ));
        let report = catalog
            .cleanup_abandoned_build(&replacement, &store, 122)
            .await
            .unwrap();
        assert_eq!(report.outcome, BuildCleanupOutcome::Cleaned);
        assert_eq!(report.deleted_object_count, 2);
        assert_eq!(report.shared_object_count, 0);
        assert_eq!(catalog.state(&artifact).unwrap(), None);
        let repeated = catalog
            .cleanup_abandoned_build(&replacement, &store, 123)
            .await
            .unwrap();
        assert_eq!(
            repeated,
            BuildCleanupReport {
                outcome: BuildCleanupOutcome::AlreadyCleaned,
                deleted_object_count: 2,
                shared_object_count: 0,
            }
        );
    }

    #[tokio::test]
    async fn cleanup_retains_every_object_referenced_by_a_published_artifact() {
        let directory = tempfile::tempdir().unwrap();
        let bytes = b"shared immutable bytes";
        let abandoned_manifest = manifest_with_artifact_id(bytes, "artifact-abandoned");
        let published_manifest = manifest_with_artifact_id(bytes, "artifact-published");
        let abandoned_artifact = abandoned_manifest.manifest().artifact_id.clone();
        let published_artifact = published_manifest.manifest().artifact_id.clone();
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("objects"), 1024).unwrap();

        let published_build = catalog
            .register_build(&published_manifest, "publisher", 100, 1_000)
            .unwrap();
        catalog.mark_uploading(&published_build, 101).unwrap();
        upload_all_manifest_objects(&store, &published_manifest, bytes).await;
        catalog.mark_uploaded(&published_build, 102).unwrap();
        assert_eq!(
            catalog
                .verify_and_publish(&published_build, &store, 103)
                .await
                .unwrap(),
            PublishOutcome::Published
        );
        let abandoned_build = catalog
            .register_build(&abandoned_manifest, "builder", 104, 1_000)
            .unwrap();
        let cleanup = catalog
            .abandon_build(&abandoned_build, "cleanup", 105, 0, 100, "failed build")
            .unwrap();
        let report = catalog
            .cleanup_abandoned_build(&cleanup, &store, 105)
            .await
            .unwrap();
        assert_eq!(report.deleted_object_count, 0);
        assert_eq!(report.shared_object_count, 2);
        assert_eq!(catalog.state(&abandoned_artifact).unwrap(), None);
        assert_eq!(
            catalog.state(&published_artifact).unwrap(),
            Some(ArtifactState::Published)
        );
        assert_eq!(
            catalog
                .published(&query(&published_manifest))
                .unwrap()
                .len(),
            1
        );
        for representation in &published_manifest.manifest().representations {
            assert!(store
                .head(&representation.object_id)
                .await
                .unwrap()
                .is_some());
        }
        assert!(matches!(
            catalog.abandon_build(
                &published_build,
                "cleanup",
                106,
                0,
                100,
                "must not delete published",
            ),
            Err(CatalogError::ArtifactIsNotBuilding(
                ArtifactState::Published
            ))
        ));
    }

    #[tokio::test]
    async fn interrupted_cleanup_retries_missing_objects_and_commits_once() {
        let directory = tempfile::tempdir().unwrap();
        let bytes = b"partially deleted orphan bytes";
        let manifest = manifest(bytes);
        let artifact = manifest.manifest().artifact_id.clone();
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("objects"), 1024).unwrap();
        let build = catalog
            .register_build(&manifest, "builder", 100, 1_000)
            .unwrap();
        catalog.mark_uploading(&build, 101).unwrap();
        upload_all_manifest_objects(&store, &manifest, bytes).await;
        let cleanup = catalog
            .abandon_build(&build, "cleanup", 102, 0, 1_000, "injected interruption")
            .unwrap();
        let result = catalog
            .cleanup_abandoned_build_with_hook(&cleanup, &store, 103, &mut |_, index| {
                if index == 0 {
                    return Err(CatalogError::Io(io::Error::other(
                        "injected cleanup interruption",
                    )));
                }
                Ok(())
            })
            .await;
        assert!(matches!(result, Err(CatalogError::Io(_))));
        assert_eq!(
            catalog.state(&artifact).unwrap(),
            Some(ArtifactState::Uploading)
        );
        let mut missing_after_interruption = 0;
        for representation in &manifest.manifest().representations {
            if store
                .head(&representation.object_id)
                .await
                .unwrap()
                .is_none()
            {
                missing_after_interruption += 1;
            }
        }
        assert_eq!(missing_after_interruption, 1);

        let report = catalog
            .cleanup_abandoned_build(&cleanup, &store, 104)
            .await
            .unwrap();
        assert_eq!(report.deleted_object_count, 2);
        assert_eq!(catalog.state(&artifact).unwrap(), None);
        for representation in &manifest.manifest().representations {
            assert_eq!(store.head(&representation.object_id).await.unwrap(), None);
        }
        assert_eq!(
            catalog
                .cleanup_abandoned_build(&cleanup, &store, 105)
                .await
                .unwrap()
                .outcome,
            BuildCleanupOutcome::AlreadyCleaned
        );
    }
}
