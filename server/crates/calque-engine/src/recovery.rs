//! Stopped-runtime backup and restore for one local Calque source.
//!
//! A recovery unit holds the PostgreSQL control rows, the cursor and signing
//! secrets, the source registry, the artifact catalog and the immutable
//! objects, so a restore never yields a catalog whose artifacts nothing can
//! fetch. A complete marker is written last. Restore accepts neither an
//! existing state directory nor any existing row for the source.
//!
//! [`RecoveryObjects`] says where a deployment keeps objects outside its state
//! directory, so the unit carries them at the same source epoch as everything
//! else.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use artifact_contract::ValidatedArtifactManifest;
use blob_store::{BlobStore, ObjectMetadata};
use postgres::{Client, GenericClient, IsolationLevel, Transaction};
use rusqlite::{types::ValueRef, Connection, OpenFlags, OptionalExtension as _};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use source_runtime::PostgresFenceStore;
use sync_types::{CursorCodec, ObjectId, SourceEpoch, SourceId};
use thiserror::Error;

use crate::{guard::PostgresConnectionFamily, DatabaseUrl, StateStore};

/// A unit of another version is refused by version, naming the fix, rather
/// than as an `OracleMismatch` that reads like a corrupt backup.
const RECOVERY_FORMAT_VERSION: u32 = 3;
/// Version 2 predates the compaction watermark. Its unit restores
/// with none recorded, as an in-place upgrade from its release leaves it.
const OLDEST_READABLE_FORMAT_VERSION: u32 = 2;
const COMPLETE_FILE: &str = "recovery.json";
const CONTROL_DIRECTORY: &str = "control";
const STATE_DIRECTORY: &str = "state";
const CATALOG_FILE: &str = "catalog.sqlite3";
const OBJECT_DIRECTORY: &str = "objects";
const COPY_BUFFER_BYTES: usize = 64 * 1024;
const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;
const OBJECT_PATH_DOMAIN: &[u8] = b"calque.blob-store.object-path.v1\0";

const STATE_FILES: &[&str] = &["state.json", "registry.json", "secrets.json"];

#[derive(Clone, Copy)]
struct ControlTable {
    name: &'static str,
    columns: &'static str,
    order_by: &'static str,
}

impl ControlTable {
    /// The columns a unit of `format_version` carries for this table.
    fn columns_in(&self, format_version: u32) -> &'static str {
        if format_version < 3 && self.name == "canonical_log_retention" {
            "source_id, source_epoch, minimum_retained_sequence, next_build_lease_token"
        } else {
            self.columns
        }
    }
}

/// The source's `calque_control` tables a unit carries and a restore replaces.
///
/// `authorization_fence` is left out: the fence only rises, and putting back
/// the row a unit carried would lower a fence raised after the unit was taken
/// and reopen a revoked generation.
const CONTROL_TABLES: &[ControlTable] = &[
    ControlTable {
        name: "source_fence_lease",
        columns: "source_id, source_epoch, fence_token, owner_id, lease_phase, expires_at, source_backend_pid",
        order_by: "source_id",
    },
    ControlTable {
        name: "canonical_log_writer_fence",
        columns: "source_id, source_epoch, fence_token",
        order_by: "source_id",
    },
    ControlTable {
        name: "source_slot_enrollment",
        columns: "source_id, source_epoch, slot_name, publication_name, system_identifier, database_name, timeline_id, previous_timeline_id, pgoutput_profile, initial_lsn, durable_canonical_lsn",
        order_by: "source_id",
    },
    ControlTable {
        name: "canonical_log_head",
        columns: "source_id, source_epoch, head_sequence",
        order_by: "source_id",
    },
    ControlTable {
        name: "canonical_log_record",
        columns: "source_id, source_epoch, sequence, transaction_identity, entry_bytes, entry_sha256",
        order_by: "sequence",
    },
    ControlTable {
        name: "canonical_log_retention",
        columns: "source_id, source_epoch, minimum_retained_sequence, next_build_lease_token, retired_compacted_through_lsn",
        order_by: "source_id",
    },
    ControlTable {
        name: "canonical_log_plan_retention_lease",
        columns: "source_id, source_epoch, plan_id, minimum_sequence, expires_at",
        order_by: "source_epoch, plan_id",
    },
    ControlTable {
        name: "canonical_log_build_retention_lease",
        columns: "source_id, source_epoch, build_id, owner_id, lease_token, minimum_sequence, expires_at",
        order_by: "source_epoch, build_id",
    },
    ControlTable {
        name: "canonical_log_retired_record",
        columns: "source_id, source_epoch, original_sequence, transaction_identity, entry_sha256",
        order_by: "original_sequence",
    },
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecoveryOracle {
    pub control_sha256: String,
    pub catalog_sha256: String,
    pub state_sha256: String,
    pub objects_sha256: String,
    pub aggregate_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RecoveryManifest {
    format_version: u32,
    pub source_id: String,
    pub source_epoch: String,
    slot_name: String,
    durable_canonical_lsn: String,
    pub oracle: RecoveryOracle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryReport {
    pub source_id: SourceId,
    pub source_epoch: SourceEpoch,
    pub oracle: RecoveryOracle,
}

/// Where a unit's object half comes from, and where a restore puts it back.
///
/// Under the filesystem store the `state/objects` tree is the deployment's
/// object namespace, so the unit copies it. Under a provider store the objects
/// live in a bucket, so the unit exports the published objects at the epoch the
/// rest of the unit was taken at, and `restore` puts them back into the
/// configured store before it commits. The layout is
/// `objects/<first two hex>/<digest>` either way, so a unit taken from a bucket
/// restores into a filesystem deployment and back.
#[derive(Clone, Copy)]
pub enum RecoveryObjects<'a> {
    /// The deployment's objects are the state directory's object tree.
    StateDirectory,
    /// The deployment's objects are in this store.
    Provider(&'a dyn BlobStore),
}

impl RecoveryObjects<'_> {
    fn provider(&self) -> Option<&dyn BlobStore> {
        match self {
            Self::StateDirectory => None,
            Self::Provider(store) => Some(*store),
        }
    }
}

/// One PUBLISHED row of `artifact_objects`, and the file the object namespace
/// holds it under.
struct PublishedObject {
    object_id: String,
    expected_bytes: u64,
    expected_sha256: String,
}

impl PublishedObject {
    /// The path both shipped adapters agree on, under an object namespace
    /// root: the filesystem store's directory, and the S3 store's key suffix.
    fn relative_path(&self) -> PathBuf {
        let digest = hex::encode(Sha256::digest(
            [OBJECT_PATH_DOMAIN, self.object_id.as_bytes()].concat(),
        ));
        Path::new("objects").join(&digest[..2]).join(digest)
    }

    fn identifier(&self) -> Result<ObjectId, RecoveryError> {
        Ok(ObjectId::new(self.object_id.clone())?)
    }
}

fn published_objects(connection: &Connection) -> Result<Vec<PublishedObject>, RecoveryError> {
    let mut statement = connection.prepare(
        "SELECT objects.object_id, objects.expected_bytes, objects.expected_sha256
           FROM artifact_objects AS objects
           JOIN artifacts ON artifacts.artifact_id = objects.artifact_id
          WHERE artifacts.state = 'PUBLISHED'
          ORDER BY objects.artifact_id, objects.object_id",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let mut objects = Vec::new();
    for row in rows {
        let (object_id, expected_bytes, expected_sha256) = row?;
        let expected_bytes = u64::try_from(expected_bytes)
            .map_err(|_| RecoveryError::PublishedObjectMismatch(object_id.clone()))?;
        objects.push(PublishedObject {
            object_id,
            expected_bytes,
            expected_sha256,
        });
    }
    Ok(objects)
}

/// One runtime for one export or import.  The recovery commands are
/// synchronous and single-purpose, and the provider adapters are async.
fn object_runtime() -> Result<tokio::runtime::Runtime, RecoveryError> {
    Ok(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?)
}

/// Writes every PUBLISHED object of the unit's catalog into the unit, read
/// from the deployment's store.
///
/// Each object is verified against the catalog row as it is written, so a
/// bucket that has lost an object, or holds different bytes under its key,
/// refuses the backup instead of producing a unit its restore would reject. A
/// catalog with no published object refuses too, with the `NoPublishedObject`
/// the restore side reports.
fn export_published_objects(
    store: &dyn BlobStore,
    catalog: &Path,
    objects_root: &Path,
) -> Result<(), RecoveryError> {
    let connection = open_catalog_read_only(catalog)?;
    let objects = published_objects(&connection)?;
    if objects.is_empty() {
        return Err(RecoveryError::NoPublishedObject);
    }
    require_absent(objects_root)?;
    fs::create_dir(objects_root)?;
    let runtime = object_runtime()?;
    for object in &objects {
        let relative = object.relative_path();
        let destination = objects_root.join(&relative);
        let parent = destination
            .parent()
            .ok_or_else(|| RecoveryError::UnsafePath(destination.clone()))?;
        fs::create_dir_all(parent)?;
        let mut stream = runtime
            .block_on(store.get(&object.identifier()?))?
            .ok_or_else(|| {
                RecoveryError::PublishedObjectAbsentFromStore(object.object_id.clone())
            })?;
        let mut output = create_new_private(&destination)?;
        let mut digest = Sha256::new();
        let mut bytes = 0_u64;
        let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
        loop {
            let read = runtime.block_on(tokio::io::AsyncReadExt::read(&mut stream, &mut buffer))?;
            if read == 0 {
                break;
            }
            bytes = bytes
                .checked_add(read as u64)
                .ok_or(RecoveryError::SizeOverflow)?;
            digest.update(&buffer[..read]);
            output.write_all(&buffer[..read])?;
        }
        output.sync_all()?;
        if bytes != object.expected_bytes
            || hex::encode(digest.finalize()) != object.expected_sha256
        {
            return Err(RecoveryError::PublishedObjectMismatch(
                object.object_id.clone(),
            ));
        }
        sync_directory(parent)?;
    }
    sync_directory(objects_root)?;
    Ok(())
}

/// Puts the unit's object half back into the deployment's store, and proves
/// each object is there afterwards.
///
/// `put_if_absent` leaves an object the bucket already holds alone, and the
/// `head` that follows proves each object is there, uploaded or not. A store
/// that answers with different bytes or a different length is refused.
fn import_published_objects(
    store: &dyn BlobStore,
    catalog: &Path,
    objects_root: &Path,
) -> Result<(), RecoveryError> {
    let connection = open_catalog_read_only(catalog)?;
    let objects = published_objects(&connection)?;
    if objects.is_empty() {
        return Err(RecoveryError::NoPublishedObject);
    }
    // Two passes: the whole object half is proved against the catalog before
    // anything is uploaded into the operator's bucket.
    for object in &objects {
        let source = objects_root.join(object.relative_path());
        let metadata = fs::symlink_metadata(&source)
            .map_err(|_| RecoveryError::PublishedObjectMismatch(object.object_id.clone()))?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() != object.expected_bytes
            || sha256_file(&source)? != object.expected_sha256
        {
            return Err(RecoveryError::PublishedObjectMismatch(
                object.object_id.clone(),
            ));
        }
    }
    let runtime = object_runtime()?;
    for object in &objects {
        let source = objects_root.join(object.relative_path());
        let expected = ObjectMetadata::new(object.expected_bytes, object.expected_sha256.clone())?;
        let identifier = object.identifier()?;
        let body = runtime.block_on(tokio::fs::File::open(&source))?;
        runtime.block_on(store.put_if_absent(&identifier, Box::pin(body), &expected))?;
        match runtime.block_on(store.head(&identifier))? {
            Some(observed) if observed == expected => {}
            Some(_) => {
                return Err(RecoveryError::PublishedObjectMismatch(
                    object.object_id.clone(),
                ))
            }
            None => {
                return Err(RecoveryError::PublishedObjectAbsentFromStore(
                    object.object_id.clone(),
                ))
            }
        }
    }
    Ok(())
}

/// Create one recovery unit while the source lease and logical slot are idle.
///
/// `database` must have been obtained from the environment by the CLI.  This
/// API never accepts or constructs a database connection from command-line
/// arguments.
pub fn create_recovery_backup(
    database: &DatabaseUrl,
    state_root: &Path,
    destination: &Path,
    objects: RecoveryObjects<'_>,
) -> Result<RecoveryReport, RecoveryError> {
    require_absent(destination)?;
    let state_store = StateStore::open_existing(state_root)?;
    let state = state_store.read_state()?;
    state_store.read_registry()?;
    state_store.read_secrets()?;
    let identity = crate::inspect_local_identity(database)?;
    if identity != state.source_identity {
        return Err(RecoveryError::SourceIdentityMismatch {
            state_dir: state_root.to_path_buf(),
            persisted: describe_source_identity(&state.source_identity),
            connected: describe_source_identity(&identity),
        });
    }

    let catalog = state_root.join(CATALOG_FILE);
    require_regular_file(&catalog)?;

    let mut client = connect(database)?;
    let mut transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()?;
    transaction.batch_execute("SET LOCAL synchronous_commit = on")?;
    lock_and_require_stopped(
        &mut transaction,
        &state.names.source_id,
        &state.names.slot_name,
    )?;
    validate_slot_continuity(
        &mut transaction,
        &state.names.source_id,
        &state.names.slot_name,
    )?;
    let durable_canonical_lsn: String = transaction
        .query_one(
            "SELECT durable_canonical_lsn::text
               FROM calque_control.source_slot_enrollment
              WHERE source_id = $1 AND slot_name = $2",
            &[&state.names.source_id.as_str(), &state.names.slot_name],
        )?
        .get(0);
    checkpoint_and_validate_catalog(&catalog)?;

    fs::create_dir(destination)?;
    let control_root = destination.join(CONTROL_DIRECTORY);
    let backup_state_root = destination.join(STATE_DIRECTORY);
    fs::create_dir(&control_root)?;
    fs::create_dir(&backup_state_root)?;
    copy_control_to_directory(&mut transaction, &state.names.source_id, &control_root)?;

    for name in STATE_FILES {
        copy_regular_file(&state_root.join(name), &backup_state_root.join(name))?;
    }
    copy_regular_file(&catalog, &backup_state_root.join(CATALOG_FILE))?;
    // The object half, from wherever this deployment keeps it. It consults the
    // catalog already copied into the unit, so the objects and the rows that
    // name them come from one checkpointed reading.
    match objects.provider() {
        None => copy_tree(
            &state_root.join(OBJECT_DIRECTORY),
            &backup_state_root.join(OBJECT_DIRECTORY),
        )?,
        Some(store) => export_published_objects(
            store,
            &backup_state_root.join(CATALOG_FILE),
            &backup_state_root.join(OBJECT_DIRECTORY),
        )?,
    }
    transaction.commit()?;

    let oracle =
        recovery_oracle_from_backup(destination, &state.names.source_id, &state.source_epoch)?;
    let manifest = RecoveryManifest {
        format_version: RECOVERY_FORMAT_VERSION,
        source_id: state.names.source_id.to_string(),
        source_epoch: state.source_epoch.to_string(),
        slot_name: state.names.slot_name.clone(),
        durable_canonical_lsn,
        oracle: oracle.clone(),
    };
    write_json_new(&destination.join(COMPLETE_FILE), &manifest)?;
    sync_directory(&control_root)?;
    sync_directory(&backup_state_root)?;
    sync_directory(destination)?;

    Ok(RecoveryReport {
        source_id: state.names.source_id,
        source_epoch: state.source_epoch,
        oracle,
    })
}

/// How `restore` treats `calque_control` rows that already exist for the
/// unit's source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExistingSourceRows {
    /// The absent-only contract: any existing row for the source refuses the
    /// restore.
    Refuse,
    /// Replace the rows of the same source, provided it is stopped: its
    /// ownership lease is unowned or expired and its logical slot is inactive.
    /// The slot-continuity gate applies unchanged, so the rows this discards
    /// can only describe history the unit already continues.
    ReplaceStopped,
}

/// Restore a complete unit into an absent state directory and a database with
/// no rows for this source, or -- with [`ExistingSourceRows::ReplaceStopped`]
/// -- into a database whose rows for this same stopped source are replaced.
/// PostgreSQL schemas may already contain other sources; those rows are never
/// modified.
pub fn restore_recovery_backup(
    database: &DatabaseUrl,
    backup: &Path,
    target_state_root: &Path,
    existing_rows: ExistingSourceRows,
    objects: RecoveryObjects<'_>,
) -> Result<RecoveryReport, RecoveryError> {
    require_absent(target_state_root)?;
    let manifest = read_manifest(backup)?;
    let source_id = SourceId::new(manifest.source_id.clone())?;
    let source_epoch = SourceEpoch::new(manifest.source_epoch.clone())?;
    let backup_oracle = recovery_oracle_from_backup(backup, &source_id, &source_epoch)?;
    if backup_oracle != manifest.oracle {
        return Err(RecoveryError::OracleMismatch);
    }

    let backup_state_store = StateStore::open_existing(backup.join(STATE_DIRECTORY))?;
    let state = backup_state_store.read_state()?;
    backup_state_store.read_registry()?;
    backup_state_store.read_secrets()?;
    if state.names.source_id != source_id
        || state.source_epoch != source_epoch
        || state.names.slot_name != manifest.slot_name
    {
        return Err(RecoveryError::BackupIdentityMismatch);
    }

    install_control_schema(database)?;
    let mut client = connect(database)?;
    require_restore_slot_continuity(
        &mut client,
        &state.names.slot_name,
        &manifest.durable_canonical_lsn,
    )?;
    let mut transaction = client.transaction()?;
    transaction.batch_execute("SET LOCAL synchronous_commit = on")?;
    match existing_rows {
        ExistingSourceRows::Refuse => require_source_absent(&mut transaction, &source_id)?,
        ExistingSourceRows::ReplaceStopped => {
            replace_stopped_source_rows(&mut transaction, &source_id, &state.names.slot_name)?;
        }
    }

    copy_tree(&backup.join(STATE_DIRECTORY), target_state_root)?;
    let restored_state_store = StateStore::open_existing(target_state_root)?;
    let restored_state = restored_state_store.read_state()?;
    restored_state_store.read_registry()?;
    restored_state_store.read_secrets()?;
    if restored_state != state {
        return Err(RecoveryError::BackupIdentityMismatch);
    }
    restore_control_from_directory(
        &mut transaction,
        &backup.join(CONTROL_DIRECTORY),
        manifest.format_version,
    )?;
    advance_fence_sequence(&mut transaction, &source_id)?;
    // The object half goes back before the control rows commit, so a store
    // that will not take an object, or will not answer for one afterwards,
    // fails while this restore can still be abandoned.
    if let Some(store) = objects.provider() {
        import_published_objects(
            store,
            &target_state_root.join(CATALOG_FILE),
            &target_state_root.join(OBJECT_DIRECTORY),
        )?;
        // A deployment on a provider store keeps no object tree, and `serve`
        // never reads one there; leaving the unit's copy would only double the
        // state directory.
        fs::remove_dir_all(target_state_root.join(OBJECT_DIRECTORY))?;
    }
    // The final verification reads the restored rows through this
    // transaction, where they are already visible, so a unit it refuses
    // commits nothing.
    let report = verify_restored_in(
        &mut transaction,
        database,
        target_state_root,
        &manifest,
        objects,
    )?;
    transaction.commit()?;
    Ok(report)
}

fn verify_restored_recovery(
    database: &DatabaseUrl,
    state_root: &Path,
    expected: &RecoveryManifest,
    objects: RecoveryObjects<'_>,
) -> Result<RecoveryReport, RecoveryError> {
    let mut client = connect(database)?;
    let mut transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()?;
    let report = verify_restored_in(&mut transaction, database, state_root, expected, objects)?;
    transaction.commit()?;
    Ok(report)
}

/// Checks a restored state directory and the source's control rows, as
/// `transaction` reads them, against the unit `expected` describes.
fn verify_restored_in(
    transaction: &mut Transaction<'_>,
    database: &DatabaseUrl,
    state_root: &Path,
    expected: &RecoveryManifest,
    objects: RecoveryObjects<'_>,
) -> Result<RecoveryReport, RecoveryError> {
    let state_store = StateStore::open_existing(state_root)?;
    let state = state_store.read_state()?;
    let registry = state_store.read_registry()?;
    let secrets = state_store.read_secrets()?;
    let identity = crate::inspect_local_identity(database)?;
    if identity != state.source_identity {
        return Err(RecoveryError::SourceIdentityMismatch {
            state_dir: state_root.to_path_buf(),
            persisted: describe_source_identity(&state.source_identity),
            connected: describe_source_identity(&identity),
        });
    }

    lock_and_require_stopped(transaction, &state.names.source_id, &state.names.slot_name)?;
    validate_slot_continuity(transaction, &state.names.source_id, &state.names.slot_name)?;
    let window = canonical_bounds(transaction, &state.names.source_id, &state.source_epoch)?;
    validate_catalog_cursors(
        &state_root.join(CATALOG_FILE),
        state_root,
        &state.source_epoch,
        CursorCodec::new(*secrets.cursor_secret.as_bytes())?,
        window,
        registry.tables().len(),
        objects,
    )?;

    let control_sha256 =
        hash_control_from_database(transaction, &state.names.source_id, expected.format_version)?;
    let catalog_sha256 = catalog_logical_sha256(&state_root.join(CATALOG_FILE))?;
    let state_sha256 = hash_named_files(state_root, STATE_FILES)?;
    let objects_sha256 = hash_objects(objects, state_root, &state_root.join(CATALOG_FILE))?;
    let oracle = assemble_oracle(control_sha256, catalog_sha256, state_sha256, objects_sha256);
    if oracle != expected.oracle
        || (expected.format_version < 3
            && compaction_watermark_recorded(transaction, &state.names.source_id)?)
    {
        return Err(RecoveryError::OracleMismatch);
    }
    Ok(RecoveryReport {
        source_id: state.names.source_id,
        source_epoch: state.source_epoch,
        oracle,
    })
}

/// Verify restored state against the oracle embedded in a complete recovery
/// unit. This is the command-line surface used by the operations runbook.
pub fn verify_restored_recovery_backup(
    database: &DatabaseUrl,
    backup: &Path,
    state_root: &Path,
    objects: RecoveryObjects<'_>,
) -> Result<RecoveryReport, RecoveryError> {
    let manifest = read_manifest(backup)?;
    verify_restored_recovery(database, state_root, &manifest, objects)
}

/// Whether the source's log records a compaction watermark, which a unit that
/// predates it cannot have restored.
fn compaction_watermark_recorded(
    transaction: &mut Transaction<'_>,
    source_id: &SourceId,
) -> Result<bool, RecoveryError> {
    Ok(transaction
        .query_one(
            "SELECT EXISTS(
                 SELECT 1 FROM calque_control.canonical_log_retention
                  WHERE source_id = $1 AND retired_compacted_through_lsn IS NOT NULL)",
            &[&source_id.as_str()],
        )?
        .get(0))
}

/// Installs the control schema the restored rows go into. It opens no log
/// writer: the rows a restore replaces may be ones an earlier release wrote,
/// and the restored log is judged when `serve` connects to it.
/// Every recovery statement's `statement_timeout`. A request that hears
/// nothing from the server for this long plus `postgres_connect::REPLY_MARGIN`
/// fails the connection.
const RECOVERY_STATEMENT_TIMEOUT: Duration = Duration::from_secs(30);

fn connect(database: &DatabaseUrl) -> Result<Client, RecoveryError> {
    let mut client = database
        .endpoint(PostgresConnectionFamily::Recovery)
        .map_err(crate::LocalSourceError::from)?
        .bound_replies(RECOVERY_STATEMENT_TIMEOUT)
        .connect()?;
    client.batch_execute(&format!(
        "SET statement_timeout = {}; SET lock_timeout = '2s'; \
         SET idle_in_transaction_session_timeout = '60s'",
        RECOVERY_STATEMENT_TIMEOUT.as_millis()
    ))?;
    Ok(client)
}

fn install_control_schema(database: &DatabaseUrl) -> Result<(), RecoveryError> {
    drop(PostgresFenceStore::connect(database.expose())?);
    let mut client = connect(database)?;
    canonical_log::PostgresFencedCanonicalLog::install_schema(&mut client)?;
    Ok(())
}

fn lock_and_require_stopped(
    transaction: &mut Transaction<'_>,
    source_id: &SourceId,
    slot_name: &str,
) -> Result<(), RecoveryError> {
    if !lock_lease_if_idle(transaction, source_id)? {
        return Err(RecoveryError::SourceNotEnrolled);
    }
    require_slot_inactive(transaction, slot_name)
}

/// Locks the source's ownership lease row for the rest of the transaction and
/// reports whether one exists.  A lease that is owned and unexpired is a live
/// runtime, and nothing in this module may proceed under it.
fn lock_lease_if_idle(
    transaction: &mut Transaction<'_>,
    source_id: &SourceId,
) -> Result<bool, RecoveryError> {
    let row = transaction.query_opt(
        "SELECT lease_phase, expires_at IS NULL OR expires_at <= clock_timestamp()
           FROM calque_control.source_fence_lease
          WHERE source_id = $1
          FOR UPDATE",
        &[&source_id.as_str()],
    )?;
    let Some(row) = row else {
        return Ok(false);
    };
    let phase: String = row.get(0);
    let expired_or_unset: bool = row.get(1);
    if phase != "unowned" && !expired_or_unset {
        return Err(RecoveryError::ActiveSourceLease);
    }
    Ok(true)
}

/// Removes every control row of the unit's own source so the unit's rows can
/// take their place, under the same stopped-source gates as `backup`: an
/// existing lease row is locked `FOR UPDATE` for the rest of the transaction
/// and must be unowned or expired, and the slot must be inactive. Rows of
/// other sources are untouched.
///
/// A source whose lease row is already gone is still replaced, but there is
/// then nothing to lock, so a concurrent enrolment is held off by the lease
/// table's primary key: one of the two writers fails, never both commit.
fn replace_stopped_source_rows(
    transaction: &mut Transaction<'_>,
    source_id: &SourceId,
    slot_name: &str,
) -> Result<(), RecoveryError> {
    // `Ok(false)`, no lease row to lock, is restorable here.
    let _lease_row_locked: bool = lock_lease_if_idle(transaction, source_id)?;
    require_slot_inactive(transaction, slot_name)?;
    for table in CONTROL_TABLES.iter().rev() {
        let query = format!(
            "DELETE FROM calque_control.{} WHERE source_id = $1",
            table.name
        );
        transaction.execute(&query, &[&source_id.as_str()])?;
    }
    Ok(())
}

fn require_slot_inactive(
    client: &mut impl GenericClient,
    slot_name: &str,
) -> Result<(), RecoveryError> {
    let row = client.query_opt(
        "SELECT active_pid FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
        &[&slot_name],
    )?;
    let Some(row) = row else {
        return Err(RecoveryError::ReplicationSlotMissing);
    };
    if row.get::<_, Option<i32>>(0).is_some() {
        return Err(RecoveryError::ActiveReplicationSlot);
    }
    Ok(())
}

fn require_restore_slot_continuity(
    client: &mut Client,
    slot_name: &str,
    durable_canonical_lsn: &str,
) -> Result<(), RecoveryError> {
    let row = client.query_opt(
        "SELECT plugin, database = current_database(), active_pid,
                confirmed_flush_lsn <= $2::text::pg_lsn
           FROM pg_catalog.pg_replication_slots
          WHERE slot_name = $1",
        &[&slot_name, &durable_canonical_lsn],
    )?;
    let Some(row) = row else {
        return Err(RecoveryError::ReplicationSlotMissing);
    };
    if row.get::<_, Option<i32>>(2).is_some() {
        return Err(RecoveryError::ActiveReplicationSlot);
    }
    let plugin: String = row.get(0);
    let same_database: bool = row.get(1);
    let continuous: bool = row.get(3);
    if plugin != "pgoutput" || !same_database || !continuous {
        return Err(RecoveryError::SlotContinuityMismatch);
    }
    Ok(())
}

fn validate_slot_continuity(
    client: &mut impl GenericClient,
    source_id: &SourceId,
    slot_name: &str,
) -> Result<(), RecoveryError> {
    let row = client.query_opt(
        "SELECT slot.plugin, slot.database = current_database(),
                slot.confirmed_flush_lsn <= enrollment.durable_canonical_lsn::pg_lsn
           FROM pg_catalog.pg_replication_slots AS slot
           JOIN calque_control.source_slot_enrollment AS enrollment
             ON enrollment.slot_name = slot.slot_name
          WHERE enrollment.source_id = $1 AND slot.slot_name = $2",
        &[&source_id.as_str(), &slot_name],
    )?;
    let Some(row) = row else {
        return Err(RecoveryError::ReplicationSlotMissing);
    };
    let plugin: String = row.get(0);
    let same_database: bool = row.get(1);
    let continuous: bool = row.get(2);
    if plugin != "pgoutput" || !same_database || !continuous {
        return Err(RecoveryError::SlotContinuityMismatch);
    }
    Ok(())
}

fn require_source_absent(
    transaction: &mut Transaction<'_>,
    source_id: &SourceId,
) -> Result<(), RecoveryError> {
    for table in CONTROL_TABLES {
        let query = format!(
            "SELECT EXISTS(SELECT 1 FROM calque_control.{} WHERE source_id = $1)",
            table.name
        );
        let exists: bool = transaction
            .query_one(&query, &[&source_id.as_str()])?
            .get(0);
        if exists {
            return Err(RecoveryError::SourceRowsAlreadyExist(table.name));
        }
    }
    Ok(())
}

fn copy_control_to_directory(
    transaction: &mut Transaction<'_>,
    source_id: &SourceId,
    directory: &Path,
) -> Result<(), RecoveryError> {
    let quoted: String = transaction
        .query_one("SELECT quote_literal($1::text)", &[&source_id.as_str()])?
        .get(0);
    for table in CONTROL_TABLES {
        let query = format!(
            "COPY (SELECT {} FROM calque_control.{} WHERE source_id = {} ORDER BY {}) TO STDOUT WITH (FORMAT binary)",
            table.columns, table.name, quoted, table.order_by
        );
        let mut reader = transaction.copy_out(&query)?;
        let path = directory.join(format!("{}.bin", table.name));
        let mut output = create_new_private(&path)?;
        io::copy(&mut reader, &mut output)?;
        output.sync_all()?;
    }
    Ok(())
}

fn restore_control_from_directory(
    transaction: &mut Transaction<'_>,
    directory: &Path,
    format_version: u32,
) -> Result<(), RecoveryError> {
    for table in CONTROL_TABLES {
        let path = directory.join(format!("{}.bin", table.name));
        require_regular_file(&path)?;
        let query = format!(
            "COPY calque_control.{} ({}) FROM STDIN WITH (FORMAT binary)",
            table.name,
            table.columns_in(format_version)
        );
        let mut input = File::open(path)?;
        let mut writer = transaction.copy_in(&query)?;
        io::copy(&mut input, &mut writer)?;
        writer.finish()?;
    }
    Ok(())
}

fn advance_fence_sequence(
    transaction: &mut Transaction<'_>,
    source_id: &SourceId,
) -> Result<(), RecoveryError> {
    transaction.query_one(
        "SELECT setval('calque_control.source_fence_token_seq',
                       GREATEST(
                           (SELECT last_value FROM calque_control.source_fence_token_seq),
                           COALESCE((SELECT fence_token FROM calque_control.source_fence_lease
                                      WHERE source_id = $1), 1)
                       ), true)",
        &[&source_id.as_str()],
    )?;
    Ok(())
}

/// The retained canonical sequence range every published artifact's cursor has
/// to sit inside.
#[derive(Clone, Copy)]
struct CanonicalWindow {
    minimum: u64,
    head: u64,
}

fn canonical_bounds(
    client: &mut impl GenericClient,
    source_id: &SourceId,
    source_epoch: &SourceEpoch,
) -> Result<CanonicalWindow, RecoveryError> {
    let row = client.query_one(
        "SELECT retention.source_epoch, retention.minimum_retained_sequence,
                COALESCE(head.head_sequence, 0)
           FROM calque_control.canonical_log_retention AS retention
           LEFT JOIN calque_control.canonical_log_head AS head USING (source_id, source_epoch)
          WHERE retention.source_id = $1",
        &[&source_id.as_str()],
    )?;
    let stored_epoch: String = row.get(0);
    let minimum: i64 = row.get(1);
    let head: i64 = row.get(2);
    if stored_epoch != source_epoch.as_str() || minimum <= 0 || head < 0 || minimum > head + 1 {
        return Err(RecoveryError::CanonicalBoundsInvalid);
    }
    Ok(CanonicalWindow {
        minimum: u64::try_from(minimum).map_err(|_| RecoveryError::CanonicalBoundsInvalid)?,
        head: u64::try_from(head).map_err(|_| RecoveryError::CanonicalBoundsInvalid)?,
    })
}

fn checkpoint_and_validate_catalog(path: &Path) -> Result<(), RecoveryError> {
    let connection = Connection::open(path)?;
    connection.busy_timeout(Duration::from_secs(5))?;
    let checkpoint: (i64, i64, i64) =
        connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    if checkpoint.0 != 0 || checkpoint.1 != checkpoint.2 {
        return Err(RecoveryError::CatalogCheckpointIncomplete);
    }
    validate_catalog_connection(&connection)?;
    drop(connection);
    for suffix in ["-wal", "-shm"] {
        let sidecar = PathBuf::from(format!("{}{suffix}", path.display()));
        if sidecar.exists() {
            return Err(RecoveryError::CatalogSidecarPresent(sidecar));
        }
    }
    Ok(())
}

fn validate_catalog_connection(connection: &Connection) -> Result<(), RecoveryError> {
    let integrity: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        return Err(RecoveryError::CatalogIntegrity(integrity));
    }
    let violation: Option<(String, i64)> = connection
        .query_row("PRAGMA foreign_key_check", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()?;
    if violation.is_some() {
        return Err(RecoveryError::CatalogForeignKeyViolation);
    }
    Ok(())
}

fn validate_catalog_cursors(
    catalog_path: &Path,
    state_root: &Path,
    source_epoch: &SourceEpoch,
    cursor_codec: CursorCodec,
    window: CanonicalWindow,
    registry_table_count: usize,
    objects: RecoveryObjects<'_>,
) -> Result<(), RecoveryError> {
    let connection = open_catalog_read_only(catalog_path)?;
    validate_catalog_connection(&connection)?;
    let mut statement = connection.prepare(
        "SELECT artifact_id, manifest_json FROM artifacts WHERE state = 'PUBLISHED' ORDER BY artifact_id",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
    })?;
    let mut published = 0_usize;
    for row in rows {
        let (stored_id, bytes) = row?;
        let manifest = ValidatedArtifactManifest::parse(&bytes)?;
        if manifest.manifest().artifact_id.as_str() != stored_id
            || &manifest.manifest().pit.source_epoch != source_epoch
            || manifest.manifest().tables.is_empty()
            || manifest.manifest().tables.len() > registry_table_count
        {
            return Err(RecoveryError::CatalogIdentityMismatch);
        }
        let position = cursor_codec
            .decode_for_epoch(&manifest.manifest().pit.snapshot_cursor, source_epoch)?;
        if position.sequence.saturating_add(1) < window.minimum || position.sequence > window.head {
            return Err(RecoveryError::ArtifactOutsideCanonicalWindow);
        }
        published += 1;
    }
    if published == 0 {
        return Err(RecoveryError::NoPublishedArtifact);
    }
    validate_published_objects(&connection, state_root, objects)?;
    Ok(())
}

/// Every PUBLISHED object has to be present, whole, and under the digest the
/// catalog row claims, in the object namespace this deployment serves
/// artifacts from: the state directory's tree under the filesystem store, the
/// bucket under a provider store.
fn validate_published_objects(
    connection: &Connection,
    state_root: &Path,
    objects: RecoveryObjects<'_>,
) -> Result<(), RecoveryError> {
    let published = published_objects(connection)?;
    if published.is_empty() {
        return Err(RecoveryError::NoPublishedObject);
    }
    let provider = match objects.provider() {
        None => None,
        Some(store) => Some((store, object_runtime()?)),
    };
    let objects_root = state_root.join(OBJECT_DIRECTORY);
    for object in &published {
        let Some((store, runtime)) = provider.as_ref() else {
            validate_object_file(&objects_root.join(object.relative_path()), object)?;
            continue;
        };
        let expected = ObjectMetadata::new(object.expected_bytes, object.expected_sha256.clone())?;
        match runtime.block_on(store.head(&object.identifier()?))? {
            Some(observed) if observed == expected => {}
            Some(_) => {
                return Err(RecoveryError::PublishedObjectMismatch(
                    object.object_id.clone(),
                ))
            }
            None => {
                return Err(RecoveryError::PublishedObjectAbsentFromStore(
                    object.object_id.clone(),
                ))
            }
        }
    }
    Ok(())
}

/// One object file, against the catalog row that publishes it.
fn validate_object_file(path: &Path, object: &PublishedObject) -> Result<(), RecoveryError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| RecoveryError::PublishedObjectMismatch(object.object_id.clone()))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() != object.expected_bytes
        || sha256_file(path)? != object.expected_sha256
    {
        return Err(RecoveryError::PublishedObjectMismatch(
            object.object_id.clone(),
        ));
    }
    Ok(())
}

/// The object half of the oracle: the PUBLISHED objects of the catalog beside
/// it, read from wherever this deployment keeps them.
///
/// The definition is the same for every store, so a unit taken on `filesystem`
/// restores into a provider store without an `OracleMismatch` over objects
/// only its tree held. The published set is the only set every store can be
/// required to produce: a bucket is shared and has no tree to walk, and
/// `artifact_objects` rows exist from `BUILDING` on, before a byte is
/// uploaded. Anything else a filesystem unit carries is copied both ways but
/// not covered.
fn hash_objects(
    objects: RecoveryObjects<'_>,
    state_root: &Path,
    catalog: &Path,
) -> Result<String, RecoveryError> {
    let connection = open_catalog_read_only(catalog)?;
    let mut published = published_objects(&connection)?;
    published.sort_by_key(PublishedObject::relative_path);
    let provider = match objects.provider() {
        None => None,
        Some(store) => Some((store, object_runtime()?)),
    };
    let objects_root = state_root.join(OBJECT_DIRECTORY);
    let mut digest = Sha256::new();
    digest.update(b"calque.recovery.objects.v1\0");
    for object in &published {
        let relative = object.relative_path();
        let name = relative
            .to_str()
            .ok_or_else(|| RecoveryError::NonUtf8Path(relative.clone()))?;
        hash_field(&mut digest, name.as_bytes());
        let Some((store, runtime)) = provider.as_ref() else {
            hash_file_into(&mut digest, &objects_root.join(&relative))?;
            continue;
        };
        let mut stream = runtime
            .block_on(store.get(&object.identifier()?))?
            .ok_or_else(|| {
                RecoveryError::PublishedObjectAbsentFromStore(object.object_id.clone())
            })?;
        let mut bytes = 0_u64;
        let mut body = Sha256::new();
        let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
        loop {
            let read = runtime.block_on(tokio::io::AsyncReadExt::read(&mut stream, &mut buffer))?;
            if read == 0 {
                break;
            }
            bytes = bytes
                .checked_add(read as u64)
                .ok_or(RecoveryError::SizeOverflow)?;
            body.update(&buffer[..read]);
        }
        digest.update(bytes.to_be_bytes());
        digest.update(body.finalize());
    }
    Ok(hex::encode(digest.finalize()))
}

fn recovery_oracle_from_backup(
    backup: &Path,
    source_id: &SourceId,
    source_epoch: &SourceEpoch,
) -> Result<RecoveryOracle, RecoveryError> {
    let state_root = backup.join(STATE_DIRECTORY);
    let state_store = StateStore::open_existing(&state_root)?;
    let state = state_store.read_state()?;
    if state.names.source_id != *source_id || state.source_epoch != *source_epoch {
        return Err(RecoveryError::BackupIdentityMismatch);
    }
    state_store.read_registry()?;
    state_store.read_secrets()?;
    let control_sha256 = hash_control_directory(&backup.join(CONTROL_DIRECTORY))?;
    let catalog_sha256 = catalog_logical_sha256(&state_root.join(CATALOG_FILE))?;
    let state_sha256 = hash_named_files(&state_root, STATE_FILES)?;
    // A unit is always a tree, read through the published-set definition
    // `verify_restored_recovery` uses against any store.
    let objects_sha256 = hash_objects(
        RecoveryObjects::StateDirectory,
        &state_root,
        &state_root.join(CATALOG_FILE),
    )?;
    Ok(assemble_oracle(
        control_sha256,
        catalog_sha256,
        state_sha256,
        objects_sha256,
    ))
}

fn assemble_oracle(
    control_sha256: String,
    catalog_sha256: String,
    state_sha256: String,
    objects_sha256: String,
) -> RecoveryOracle {
    let mut digest = Sha256::new();
    digest.update(b"calque.recovery.oracle.v1\0");
    for component in [
        &control_sha256,
        &catalog_sha256,
        &state_sha256,
        &objects_sha256,
    ] {
        hash_field(&mut digest, component.as_bytes());
    }
    RecoveryOracle {
        control_sha256,
        catalog_sha256,
        state_sha256,
        objects_sha256,
        aggregate_sha256: hex::encode(digest.finalize()),
    }
}

fn hash_control_directory(directory: &Path) -> Result<String, RecoveryError> {
    let mut digest = Sha256::new();
    digest.update(b"calque.recovery.control.v1\0");
    for table in CONTROL_TABLES {
        hash_field(&mut digest, table.name.as_bytes());
        hash_file_into(&mut digest, &directory.join(format!("{}.bin", table.name)))?;
    }
    Ok(hex::encode(digest.finalize()))
}

fn hash_control_from_database(
    client: &mut impl GenericClient,
    source_id: &SourceId,
    format_version: u32,
) -> Result<String, RecoveryError> {
    let quoted: String = client
        .query_one("SELECT quote_literal($1::text)", &[&source_id.as_str()])?
        .get(0);
    let mut digest = Sha256::new();
    digest.update(b"calque.recovery.control.v1\0");
    for table in CONTROL_TABLES {
        hash_field(&mut digest, table.name.as_bytes());
        let query = format!(
            "COPY (SELECT {} FROM calque_control.{} WHERE source_id = {} ORDER BY {}) TO STDOUT WITH (FORMAT binary)",
            table.columns_in(format_version),
            table.name,
            quoted,
            table.order_by
        );
        let mut reader = client.copy_out(&query)?;
        hash_reader(&mut digest, &mut reader)?;
    }
    Ok(hex::encode(digest.finalize()))
}

const CATALOG_TABLES: &[(&str, &str)] = &[
    ("artifacts", "artifact_id"),
    ("artifact_objects", "artifact_id, object_id"),
    ("artifact_tables", "artifact_id, table_id"),
    ("artifact_audit", "audit_id"),
    ("artifact_plan_leases", "plan_id, artifact_id"),
    ("artifact_build_abandonments", "artifact_id"),
    (
        "artifact_build_cleanup_receipts",
        "artifact_id, cleanup_token",
    ),
];

fn catalog_logical_sha256(path: &Path) -> Result<String, RecoveryError> {
    let connection = open_catalog_read_only(path)?;
    validate_catalog_connection(&connection)?;
    let mut digest = Sha256::new();
    digest.update(b"calque.recovery.catalog.v1\0");
    for (table, order) in CATALOG_TABLES {
        hash_field(&mut digest, table.as_bytes());
        let mut statement =
            connection.prepare(&format!("SELECT * FROM {table} ORDER BY {order}"))?;
        let columns = statement.column_count();
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            digest.update((columns as u64).to_be_bytes());
            for index in 0..columns {
                match row.get_ref(index)? {
                    ValueRef::Null => digest.update([0]),
                    ValueRef::Integer(value) => {
                        digest.update([1]);
                        digest.update(value.to_be_bytes());
                    }
                    ValueRef::Real(value) => {
                        digest.update([2]);
                        digest.update(value.to_bits().to_be_bytes());
                    }
                    ValueRef::Text(value) => {
                        digest.update([3]);
                        hash_field(&mut digest, value);
                    }
                    ValueRef::Blob(value) => {
                        digest.update([4]);
                        hash_field(&mut digest, value);
                    }
                }
            }
        }
    }
    Ok(hex::encode(digest.finalize()))
}

fn open_catalog_read_only(path: &Path) -> Result<Connection, RecoveryError> {
    require_regular_file(path)?;
    Ok(Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?)
}

fn hash_named_files(root: &Path, names: &[&str]) -> Result<String, RecoveryError> {
    let mut digest = Sha256::new();
    digest.update(b"calque.recovery.state.v1\0");
    for name in names {
        hash_field(&mut digest, name.as_bytes());
        hash_file_into(&mut digest, &root.join(name))?;
    }
    Ok(hex::encode(digest.finalize()))
}

/// Walks a tree for the tests that ask what a unit actually carries.
#[cfg(test)]
fn collect_tree_files(
    root: &Path,
    current: &Path,
    files: &mut Vec<PathBuf>,
) -> Result<(), RecoveryError> {
    require_directory(current)?;
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() {
            return Err(RecoveryError::UnsafePath(entry.path()));
        }
        if metadata.is_dir() {
            collect_tree_files(root, &entry.path(), files)?;
        } else if metadata.is_file() {
            files.push(
                entry
                    .path()
                    .strip_prefix(root)
                    .map_err(|_| RecoveryError::UnsafePath(entry.path()))?
                    .to_path_buf(),
            );
        } else {
            return Err(RecoveryError::UnsafePath(entry.path()));
        }
    }
    Ok(())
}

fn copy_tree(source: &Path, destination: &Path) -> Result<(), RecoveryError> {
    require_directory(source)?;
    require_absent(destination)?;
    fs::create_dir(destination)?;
    let mut entries = fs::read_dir(source)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let metadata = fs::symlink_metadata(&source_path)?;
        if metadata.file_type().is_symlink() {
            return Err(RecoveryError::UnsafePath(source_path));
        }
        if metadata.is_dir() {
            copy_tree(&source_path, &destination_path)?;
        } else if metadata.is_file() {
            copy_regular_file(&source_path, &destination_path)?;
        } else {
            return Err(RecoveryError::UnsafePath(source_path));
        }
    }
    sync_directory(destination)?;
    Ok(())
}

fn copy_regular_file(source: &Path, destination: &Path) -> Result<(), RecoveryError> {
    require_regular_file(source)?;
    require_absent(destination)?;
    let mut input = File::open(source)?;
    let mut output = create_new_private(destination)?;
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        output.write_all(&buffer[..read])?;
    }
    output.sync_all()?;
    Ok(())
}

fn hash_file_into(digest: &mut Sha256, path: &Path) -> Result<(), RecoveryError> {
    require_regular_file(path)?;
    let mut input = File::open(path)?;
    hash_reader(digest, &mut input)
}

fn hash_reader(digest: &mut Sha256, reader: &mut impl Read) -> Result<(), RecoveryError> {
    let mut bytes = 0_u64;
    let mut file_digest = Sha256::new();
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(read as u64)
            .ok_or(RecoveryError::SizeOverflow)?;
        file_digest.update(&buffer[..read]);
    }
    digest.update(bytes.to_be_bytes());
    digest.update(file_digest.finalize());
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String, RecoveryError> {
    let mut digest = Sha256::new();
    let mut input = File::open(path)?;
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex::encode(digest.finalize()))
}

fn hash_field(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
}

fn read_manifest(root: &Path) -> Result<RecoveryManifest, RecoveryError> {
    require_directory(root)?;
    let path = root.join(COMPLETE_FILE);
    match path.symlink_metadata() {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(RecoveryError::UnsafePath(path));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(RecoveryError::IncompleteBackup(root.to_path_buf()));
        }
        Err(error) => return Err(error.into()),
    }
    let mut bytes = Vec::new();
    File::open(path)?
        .take(MAX_MANIFEST_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(RecoveryError::ManifestTooLarge);
    }
    let manifest: RecoveryManifest = serde_json::from_slice(&bytes)?;
    if !(OLDEST_READABLE_FORMAT_VERSION..=RECOVERY_FORMAT_VERSION)
        .contains(&manifest.format_version)
    {
        return Err(RecoveryError::UnsupportedFormat(manifest.format_version));
    }
    Ok(manifest)
}

fn write_json_new(path: &Path, value: &impl Serialize) -> Result<(), RecoveryError> {
    let bytes = serde_json::to_vec_pretty(value)?;
    let mut output = create_new_private(path)?;
    output.write_all(&bytes)?;
    output.sync_all()?;
    Ok(())
}

fn create_new_private(path: &Path) -> Result<File, io::Error> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

fn require_absent(path: &Path) -> Result<(), RecoveryError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(RecoveryError::TargetExists(path.to_path_buf())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn require_regular_file(path: &Path) -> Result<(), RecoveryError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(RecoveryError::UnsafePath(path.to_path_buf()));
    }
    Ok(())
}

fn require_directory(path: &Path) -> Result<(), RecoveryError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(RecoveryError::UnsafePath(path.to_path_buf()));
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), RecoveryError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn describe_source_identity(identity: &source_control::SourceIdentity) -> String {
    format!(
        "database {} on PostgreSQL system {} timeline {} (previous {})",
        identity.database_name,
        identity.system_identifier,
        identity.timeline_id,
        identity.previous_timeline_id
    )
}

#[derive(Debug, Error)]
pub enum RecoveryError {
    #[error("recovery I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("recovery PostgreSQL operation failed: {0}")]
    Postgres(#[from] postgres::Error),
    #[error("recovery SQLite operation failed: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("recovery JSON operation failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    State(#[from] crate::StateError),
    #[error(transparent)]
    Source(#[from] crate::LocalSourceError),
    #[error(transparent)]
    Fence(#[from] source_runtime::FenceError),
    #[error(transparent)]
    Canonical(#[from] canonical_log::LogError),
    #[error(transparent)]
    Identifier(#[from] sync_types::IdentifierError),
    #[error(transparent)]
    Cursor(#[from] sync_types::CursorError),
    #[error(transparent)]
    Manifest(#[from] artifact_contract::ManifestError),
    #[error("recovery target already exists at {0}; backup and restore never overwrite, so choose an absent path or inspect and remove the incomplete target before retrying")]
    TargetExists(PathBuf),
    #[error("recovery path is not a safe regular file or directory: {0}")]
    UnsafePath(PathBuf),
    #[error("recovery path is not UTF-8: {0}")]
    NonUtf8Path(PathBuf),
    /// The command names the remedy, since only it knows the variable it read
    /// the connection string from.
    #[error("connected {connected}, but state at {state_dir} belongs to {persisted}.")]
    SourceIdentityMismatch {
        state_dir: PathBuf,
        persisted: String,
        connected: String,
    },
    #[error("backup source identity differs across recovery components")]
    BackupIdentityMismatch,
    #[error("source is not enrolled in the PostgreSQL control store")]
    SourceNotEnrolled,
    #[error("source still has a live ownership lease; stop the calque process that serves it, wait for it to exit, then retry")]
    ActiveSourceLease,
    #[error(
        "source logical replication slot is active; stop the process using the slot, then retry"
    )]
    ActiveReplicationSlot,
    #[error("source logical replication slot is missing; restore cannot prove CDC continuity, so destroy and re-enroll the source for a full snapshot")]
    ReplicationSlotMissing,
    #[error("replication slot is ahead of or incompatible with restored canonical truth; restore cannot prove CDC continuity, so destroy and re-enroll the source for a full snapshot")]
    SlotContinuityMismatch,
    #[error("restore found existing source rows in calque_control.{0}; restore is absent-only by default: rerun with --replace-source-rows if that source is stopped and this unit continues its logical slot. Do not run destroy first; it drops the slot restore needs")]
    SourceRowsAlreadyExist(&'static str),
    #[error("backup at {0} is incomplete: recovery.json is missing; use a directory produced by a successful calque backup")]
    IncompleteBackup(PathBuf),
    #[error("artifact catalog WAL checkpoint did not finish")]
    CatalogCheckpointIncomplete,
    #[error("artifact catalog sidecar remains after stopped-runtime checkpoint: {0}")]
    CatalogSidecarPresent(PathBuf),
    #[error("artifact catalog integrity check failed: {0}")]
    CatalogIntegrity(String),
    #[error("artifact catalog foreign-key check failed")]
    CatalogForeignKeyViolation,
    #[error("artifact catalog identity does not match restored state")]
    CatalogIdentityMismatch,
    #[error("published artifact cursor is outside restored canonical history")]
    ArtifactOutsideCanonicalWindow,
    #[error("recovery catalog has no published artifact")]
    NoPublishedArtifact,
    #[error("recovery catalog has no published object")]
    NoPublishedObject,
    #[error("published object is missing or corrupt: {0}")]
    PublishedObjectMismatch(String),
    #[error("published object {0} is absent from the configured object store; a recovery unit must carry every object its catalog publishes, and a restore must put every one of them back, or the restored deployment serves artifacts nothing can fetch")]
    PublishedObjectAbsentFromStore(String),
    #[error("recovery object store operation failed: {0}")]
    ObjectStore(#[from] blob_store::StorageError),
    #[error("restored canonical floor/head are invalid")]
    CanonicalBoundsInvalid,
    #[error("recovery logical SHA-256 oracle differs")]
    OracleMismatch,
    #[error("recovery manifest exceeds its byte limit")]
    ManifestTooLarge,
    #[error("recovery unit is format version {0}, and this build reads versions {} to {}; restore it with the calque release that took it, or take a fresh unit with calque backup from a deployment running this build", OLDEST_READABLE_FORMAT_VERSION, RECOVERY_FORMAT_VERSION)]
    UnsupportedFormat(u32),
    #[error("recovery byte count overflowed")]
    SizeOverflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live_database_url() -> Option<String> {
        std::env::var("CALQUE_CONTROL_DATABASE_URL")
            .ok()
            .or_else(|| std::env::var("CALQUE_DATABASE_URL").ok())
    }

    /// A store that holds whatever it is told to hold, and answers exactly
    /// that. Its faults are the ones a real bucket has: an object that is
    /// gone, an object whose bytes are not the catalog's, and a `put` that
    /// reports success and keeps nothing.
    struct MemoryObjectStore {
        objects: std::sync::Mutex<std::collections::BTreeMap<String, Vec<u8>>>,
        swallow_puts: bool,
        corrupt_reads: bool,
    }

    impl MemoryObjectStore {
        fn new() -> Self {
            Self {
                objects: std::sync::Mutex::new(std::collections::BTreeMap::new()),
                swallow_puts: false,
                corrupt_reads: false,
            }
        }

        fn swallowing_puts() -> Self {
            Self {
                swallow_puts: true,
                ..Self::new()
            }
        }

        /// A store whose `head` describes what it was given and whose `get`
        /// serves those bytes with the first one flipped: the object gate
        /// passes, and only the oracle's hash of the bytes can tell.
        fn corrupting_reads() -> Self {
            Self {
                corrupt_reads: true,
                ..Self::new()
            }
        }

        fn insert(&self, object_id: &str, body: &[u8]) {
            self.objects
                .lock()
                .unwrap()
                .insert(object_id.to_owned(), body.to_vec());
        }

        fn ids(&self) -> Vec<String> {
            self.objects.lock().unwrap().keys().cloned().collect()
        }

        fn body(&self, object_id: &str) -> Option<Vec<u8>> {
            self.objects.lock().unwrap().get(object_id).cloned()
        }
    }

    #[async_trait::async_trait]
    impl BlobStore for MemoryObjectStore {
        async fn put_if_absent(
            &self,
            object: &ObjectId,
            mut body: blob_store::ByteStream,
            _metadata: &ObjectMetadata,
        ) -> blob_store::Result<blob_store::PutOutcome> {
            let mut bytes = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut body, &mut bytes).await?;
            if self.swallow_puts {
                return Ok(blob_store::PutOutcome::Created);
            }
            let mut objects = self.objects.lock().unwrap();
            if objects.contains_key(object.as_str()) {
                return Ok(blob_store::PutOutcome::AlreadyPresent);
            }
            objects.insert(object.as_str().to_owned(), bytes);
            Ok(blob_store::PutOutcome::Created)
        }

        async fn head(&self, object: &ObjectId) -> blob_store::Result<Option<ObjectMetadata>> {
            Ok(self
                .body(object.as_str())
                .map(|bytes| ObjectMetadata::for_bytes(&bytes)))
        }

        async fn get(
            &self,
            object: &ObjectId,
        ) -> blob_store::Result<Option<blob_store::ByteStream>> {
            Ok(self.body(object.as_str()).map(|mut bytes| {
                if self.corrupt_reads {
                    bytes[0] ^= 0xff;
                }
                Box::pin(std::io::Cursor::new(bytes)) as blob_store::ByteStream
            }))
        }

        async fn delete(&self, object: &ObjectId) -> blob_store::Result<()> {
            self.objects.lock().unwrap().remove(object.as_str());
            Ok(())
        }
    }

    /// The two published objects of one catalog, plus one belonging to an
    /// artifact that was never published.
    const FIRST_OBJECT: &str = "artifacts/alpha/projects.part";
    const SECOND_OBJECT: &str = "artifacts/alpha/tasks.part";
    const RETIRED_OBJECT: &str = "artifacts/beta/projects.part";

    /// A catalog with the real schema -- `SqliteArtifactCatalog::open` installs
    /// it -- carrying one PUBLISHED artifact with two objects and one RETIRED
    /// artifact with one. Only the PUBLISHED artifact's objects are a unit's
    /// object half.
    fn object_half_catalog(path: &Path) -> Vec<(&'static str, Vec<u8>)> {
        drop(artifact_catalog::SqliteArtifactCatalog::open(path).unwrap());
        let bodies = vec![
            (FIRST_OBJECT, b"projects-columnar-bytes".to_vec()),
            (SECOND_OBJECT, b"tasks-columnar-bytes".to_vec()),
            (RETIRED_OBJECT, b"never-published-bytes".to_vec()),
        ];
        let connection = Connection::open(path).unwrap();
        for (artifact, state) in [("alpha", "PUBLISHED"), ("beta", "RETIRED")] {
            connection
                .execute(
                    "INSERT INTO artifacts (artifact_id, state, revision, manifest_json,
                                            cache_key_json, table_set_hash, created_at_ms,
                                            updated_at_ms)
                     VALUES (?1, ?2, 1, x'00', '{}', '00', 0, 0)",
                    rusqlite::params![artifact, state],
                )
                .unwrap();
        }
        for (object_id, body) in &bodies {
            let artifact = if *object_id == RETIRED_OBJECT {
                "beta"
            } else {
                "alpha"
            };
            connection
                .execute(
                    "INSERT INTO artifact_objects (artifact_id, object_id, expected_bytes,
                                                   expected_sha256)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        artifact,
                        object_id,
                        body.len() as i64,
                        hex::encode(Sha256::digest(body))
                    ],
                )
                .unwrap();
        }
        drop(connection);
        bodies
    }

    fn stocked_store(bodies: &[(&'static str, Vec<u8>)]) -> MemoryObjectStore {
        let store = MemoryObjectStore::new();
        for (object_id, body) in bodies {
            store.insert(object_id, body);
        }
        store
    }

    /// A unit taken from a deployment whose objects are in a provider store
    /// carries those objects: every PUBLISHED one, none of the others, at the
    /// path the object namespace holds them under.
    #[test]
    fn a_unit_taken_from_a_provider_store_carries_every_published_object() {
        let temporary = tempfile::tempdir().unwrap();
        let catalog = temporary.path().join(CATALOG_FILE);
        let bodies = object_half_catalog(&catalog);
        let store = stocked_store(&bodies);
        let unit_state = temporary.path().join("unit-state");
        fs::create_dir(&unit_state).unwrap();

        export_published_objects(&store, &catalog, &unit_state.join(OBJECT_DIRECTORY)).unwrap();

        let mut carried = Vec::new();
        collect_tree_files(
            &unit_state.join(OBJECT_DIRECTORY),
            &unit_state.join(OBJECT_DIRECTORY),
            &mut carried,
        )
        .unwrap();
        assert_eq!(carried.len(), 2, "{carried:?}");
        for (object_id, body) in &bodies[..2] {
            let published = PublishedObject {
                object_id: (*object_id).to_owned(),
                expected_bytes: body.len() as u64,
                expected_sha256: hex::encode(Sha256::digest(body)),
            };
            let path = unit_state
                .join(OBJECT_DIRECTORY)
                .join(published.relative_path());
            assert_eq!(&fs::read(&path).unwrap(), body, "{}", path.display());
        }
        // And the unit's own object gate accepts it, against the tree and
        // against the store it came from.
        let connection = open_catalog_read_only(&catalog).unwrap();
        validate_published_objects(&connection, &unit_state, RecoveryObjects::StateDirectory)
            .unwrap();
        validate_published_objects(&connection, &unit_state, RecoveryObjects::Provider(&store))
            .unwrap();
    }

    /// A bucket that has lost a published object cannot produce a unit, and the
    /// refusal names the object rather than reporting a directory that came out
    /// short.
    #[test]
    fn a_backup_refuses_a_provider_store_that_cannot_serve_a_published_object() {
        let temporary = tempfile::tempdir().unwrap();
        let catalog = temporary.path().join(CATALOG_FILE);
        let bodies = object_half_catalog(&catalog);

        let missing = MemoryObjectStore::new();
        missing.insert(FIRST_OBJECT, &bodies[0].1);
        let error = export_published_objects(
            &missing,
            &catalog,
            &temporary.path().join("missing-unit-objects"),
        )
        .unwrap_err();
        assert!(
            matches!(&error, RecoveryError::PublishedObjectAbsentFromStore(id) if id == SECOND_OBJECT),
            "{error}"
        );
        assert!(error.to_string().contains(SECOND_OBJECT), "{error}");

        let wrong = stocked_store(&bodies);
        wrong.insert(SECOND_OBJECT, b"not the published bytes");
        let error = export_published_objects(
            &wrong,
            &catalog,
            &temporary.path().join("wrong-unit-objects"),
        )
        .unwrap_err();
        assert!(
            matches!(&error, RecoveryError::PublishedObjectMismatch(id) if id == SECOND_OBJECT),
            "{error}"
        );
    }

    /// The other half: a restore puts the unit's objects back into the store
    /// the restored deployment reads from, and the object gate then passes
    /// against that store rather than only against the tree on disk.
    #[test]
    fn a_restore_puts_the_object_half_back_into_the_deployment_store() {
        let temporary = tempfile::tempdir().unwrap();
        let catalog = temporary.path().join(CATALOG_FILE);
        let bodies = object_half_catalog(&catalog);
        let source = stocked_store(&bodies);
        let unit_state = temporary.path().join("unit-state");
        fs::create_dir(&unit_state).unwrap();
        export_published_objects(&source, &catalog, &unit_state.join(OBJECT_DIRECTORY)).unwrap();

        let empty = MemoryObjectStore::new();
        let connection = open_catalog_read_only(&catalog).unwrap();
        // Before: the store the restored deployment would serve from is empty,
        // and the gate says so.
        let error =
            validate_published_objects(&connection, &unit_state, RecoveryObjects::Provider(&empty))
                .unwrap_err();
        assert!(
            matches!(&error, RecoveryError::PublishedObjectAbsentFromStore(id) if id == FIRST_OBJECT),
            "{error}"
        );

        import_published_objects(&empty, &catalog, &unit_state.join(OBJECT_DIRECTORY)).unwrap();

        assert_eq!(empty.ids(), vec![FIRST_OBJECT, SECOND_OBJECT]);
        assert_eq!(empty.body(FIRST_OBJECT).as_ref(), Some(&bodies[0].1));
        assert_eq!(empty.body(SECOND_OBJECT).as_ref(), Some(&bodies[1].1));
        assert!(
            empty.body(RETIRED_OBJECT).is_none(),
            "a restore puts back what the catalog publishes, and nothing else",
        );
        validate_published_objects(&connection, &unit_state, RecoveryObjects::Provider(&empty))
            .unwrap();
    }

    /// A store that takes a `put` and keeps nothing is caught by the
    /// read-back, before a restore commits a catalog whose published artifacts
    /// name objects the deployment cannot fetch.
    #[test]
    fn a_restore_refuses_a_store_that_did_not_keep_what_it_was_given() {
        let temporary = tempfile::tempdir().unwrap();
        let catalog = temporary.path().join(CATALOG_FILE);
        let bodies = object_half_catalog(&catalog);
        let source = stocked_store(&bodies);
        let unit_state = temporary.path().join("unit-state");
        fs::create_dir(&unit_state).unwrap();
        export_published_objects(&source, &catalog, &unit_state.join(OBJECT_DIRECTORY)).unwrap();

        let swallowing = MemoryObjectStore::swallowing_puts();
        let error =
            import_published_objects(&swallowing, &catalog, &unit_state.join(OBJECT_DIRECTORY))
                .unwrap_err();
        assert!(
            matches!(&error, RecoveryError::PublishedObjectAbsentFromStore(id) if id == FIRST_OBJECT),
            "{error}"
        );
    }

    /// A unit whose object half is absent -- deleted, truncated, or never
    /// written -- is refused before anything is put anywhere, and the message
    /// names the object that is missing.
    #[test]
    fn a_unit_whose_object_half_is_absent_is_refused_by_name() {
        let temporary = tempfile::tempdir().unwrap();
        let catalog = temporary.path().join(CATALOG_FILE);
        let bodies = object_half_catalog(&catalog);
        let source = stocked_store(&bodies);
        let unit_state = temporary.path().join("unit-state");
        fs::create_dir(&unit_state).unwrap();
        let unit_objects = unit_state.join(OBJECT_DIRECTORY);
        export_published_objects(&source, &catalog, &unit_objects).unwrap();

        let second = PublishedObject {
            object_id: SECOND_OBJECT.to_owned(),
            expected_bytes: bodies[1].1.len() as u64,
            expected_sha256: hex::encode(Sha256::digest(&bodies[1].1)),
        };
        fs::remove_file(unit_objects.join(second.relative_path())).unwrap();

        let empty = MemoryObjectStore::new();
        let error = import_published_objects(&empty, &catalog, &unit_objects).unwrap_err();
        assert!(
            matches!(&error, RecoveryError::PublishedObjectMismatch(id) if id == SECOND_OBJECT),
            "{error}"
        );
        assert!(
            empty.ids().is_empty(),
            "nothing may be put anywhere while the unit is known to be incomplete",
        );
        let connection = open_catalog_read_only(&catalog).unwrap();
        assert!(matches!(
            validate_published_objects(&connection, &unit_state, RecoveryObjects::StateDirectory),
            Err(RecoveryError::PublishedObjectMismatch(_))
        ));
    }

    /// A deployment on the filesystem store, stocked through the adapter the
    /// server actually writes through, so the tree is laid out the way a real
    /// one is -- `staging` beside `objects`, and one object that no artifact
    /// publishes.
    fn filesystem_deployment(state_root: &Path, bodies: &[(&'static str, Vec<u8>)]) {
        let store = blob_store::LocalFilesystemBlobStore::open(
            state_root.join(OBJECT_DIRECTORY),
            crate::MAX_LOCAL_ARTIFACT_BYTES,
        )
        .unwrap();
        let runtime = object_runtime().unwrap();
        for (object_id, body) in bodies {
            let metadata = ObjectMetadata::for_bytes(body);
            runtime
                .block_on(store.put_if_absent(
                    &ObjectId::new((*object_id).to_owned()).unwrap(),
                    Box::pin(std::io::Cursor::new(body.clone())),
                    &metadata,
                ))
                .unwrap();
        }
    }

    /// A unit taken on the filesystem store, restored into a provider store.
    /// The unit carries the RETIRED artifact's object as well as the two
    /// published ones, while a bucket can hash only the published set, so both
    /// halves of the oracle must be the published set.
    #[test]
    fn a_unit_taken_on_the_filesystem_store_keeps_its_oracle_when_restored_into_a_provider() {
        let temporary = tempfile::tempdir().unwrap();
        let deployment = temporary.path().join("filesystem-deployment-state");
        fs::create_dir(&deployment).unwrap();
        let catalog = deployment.join(CATALOG_FILE);
        let bodies = object_half_catalog(&catalog);
        filesystem_deployment(&deployment, &bodies);

        // What `create_recovery_backup` puts in the unit under
        // `RecoveryObjects::StateDirectory`: the tree, whole.
        let unit_state = temporary.path().join("unit-state");
        fs::create_dir(&unit_state).unwrap();
        copy_regular_file(&catalog, &unit_state.join(CATALOG_FILE)).unwrap();
        copy_tree(
            &deployment.join(OBJECT_DIRECTORY),
            &unit_state.join(OBJECT_DIRECTORY),
        )
        .unwrap();
        let mut carried = Vec::new();
        collect_tree_files(
            &unit_state.join(OBJECT_DIRECTORY),
            &unit_state.join(OBJECT_DIRECTORY),
            &mut carried,
        )
        .unwrap();
        assert_eq!(
            carried.len(),
            3,
            "the unit carries the unpublished object too: {carried:?}",
        );

        // The number the manifest records, and the number the restored
        // deployment on a bucket computes for itself.
        let recorded = hash_objects(
            RecoveryObjects::StateDirectory,
            &unit_state,
            &unit_state.join(CATALOG_FILE),
        )
        .unwrap();
        let bucket = MemoryObjectStore::new();
        import_published_objects(
            &bucket,
            &unit_state.join(CATALOG_FILE),
            &unit_state.join(OBJECT_DIRECTORY),
        )
        .unwrap();
        let restored_state = temporary.path().join("restored-bucket-state");
        fs::create_dir(&restored_state).unwrap();
        copy_regular_file(&catalog, &restored_state.join(CATALOG_FILE)).unwrap();
        let observed = hash_objects(
            RecoveryObjects::Provider(&bucket),
            &restored_state,
            &restored_state.join(CATALOG_FILE),
        )
        .unwrap();
        assert_eq!(
            recorded, observed,
            "a filesystem unit restored into a bucket must verify; a mismatch here is an \
             unretryable FAILED restore with the control rows already committed",
        );
    }

    /// The object half of the oracle is one number, whether it is read off a
    /// unit's tree or out of the store the unit came from, so a deployment on
    /// a bucket can be verified and a unit moves between stores without its
    /// oracle changing.
    #[test]
    fn the_object_oracle_is_one_number_from_the_unit_tree_or_from_the_store() {
        let temporary = tempfile::tempdir().unwrap();
        let catalog = temporary.path().join(CATALOG_FILE);
        let bodies = object_half_catalog(&catalog);
        let store = stocked_store(&bodies);
        let unit_state = temporary.path().join("unit-state");
        fs::create_dir(&unit_state).unwrap();
        export_published_objects(&store, &catalog, &unit_state.join(OBJECT_DIRECTORY)).unwrap();

        let from_tree =
            hash_objects(RecoveryObjects::StateDirectory, &unit_state, &catalog).unwrap();
        // The provider answer reads no tree at all: the path it is given holds
        // none, as on a deployment whose objects are in a bucket.
        let deployment = temporary.path().join("bucket-deployment-state");
        fs::create_dir(&deployment).unwrap();
        let from_store =
            hash_objects(RecoveryObjects::Provider(&store), &deployment, &catalog).unwrap();
        assert_eq!(from_tree, from_store);
        assert!(!deployment.join(OBJECT_DIRECTORY).exists());

        // And it is a hash of the bytes, not of the row count: one changed
        // object changes it.
        store.insert(SECOND_OBJECT, b"different published bytes");
        assert_ne!(
            from_store,
            hash_objects(RecoveryObjects::Provider(&store), &deployment, &catalog).unwrap()
        );
    }

    /// A catalog with nothing published is not a recovery unit, whichever
    /// store it came from: the same `NoPublishedObject` the restore side
    /// reports.
    #[test]
    fn a_catalog_with_no_published_object_refuses_a_provider_backup() {
        let temporary = tempfile::tempdir().unwrap();
        let catalog = temporary.path().join(CATALOG_FILE);
        drop(artifact_catalog::SqliteArtifactCatalog::open(&catalog).unwrap());
        let store = MemoryObjectStore::new();
        assert!(matches!(
            export_published_objects(&store, &catalog, &temporary.path().join("empty-objects")),
            Err(RecoveryError::NoPublishedObject)
        ));
        assert!(!temporary.path().join("empty-objects").exists());
    }

    const CATALOG_CURSOR_SECRET: [u8; 32] = [0x5a; 32];

    /// The checked-in example manifest with its snapshot cursor at canonical
    /// `sequence`, signed under [`CATALOG_CURSOR_SECRET`].
    fn manifest_at(sequence: u64) -> artifact_contract::ArtifactManifest {
        let mut manifest = ValidatedArtifactManifest::parse(include_bytes!(
            "../../../../spec/artifact-manifest.example.json"
        ))
        .unwrap()
        .manifest()
        .clone();
        manifest.pit.snapshot_cursor =
            CursorCodec::new(CATALOG_CURSOR_SECRET)
                .unwrap()
                .encode(&sync_types::CursorPosition {
                    source_epoch: manifest.pit.source_epoch.clone(),
                    sequence,
                });
        manifest
    }

    /// Runs the catalog gate of a restore's verification over a catalog with
    /// one artifact row, `manifest` stored under `stored_id` in `state`, whose
    /// one object the provider store holds. The window is sequences 5 to 9 and
    /// the registry has one table, unless `registry_tables` says otherwise.
    fn catalog_gate(
        stored_id: &str,
        state: &str,
        manifest: &artifact_contract::ArtifactManifest,
        epoch: &SourceEpoch,
        registry_tables: usize,
    ) -> Result<(), RecoveryError> {
        let temporary = tempfile::tempdir().unwrap();
        let catalog = temporary.path().join(CATALOG_FILE);
        drop(artifact_catalog::SqliteArtifactCatalog::open(&catalog).unwrap());
        let body = b"published-columnar-bytes";
        let connection = Connection::open(&catalog).unwrap();
        connection
            .execute(
                "INSERT INTO artifacts (artifact_id, state, revision, manifest_json,
                                        cache_key_json, table_set_hash, created_at_ms,
                                        updated_at_ms)
                 VALUES (?1, ?2, 1, ?3, '{}', '00', 0, 0)",
                rusqlite::params![stored_id, state, serde_json::to_vec(manifest).unwrap()],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO artifact_objects (artifact_id, object_id, expected_bytes,
                                               expected_sha256)
                 VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    stored_id,
                    FIRST_OBJECT,
                    body.len() as i64,
                    hex::encode(Sha256::digest(body))
                ],
            )
            .unwrap();
        drop(connection);
        let store = stocked_store(&[(FIRST_OBJECT, body.to_vec())]);
        validate_catalog_cursors(
            &catalog,
            temporary.path(),
            epoch,
            CursorCodec::new(CATALOG_CURSOR_SECRET).unwrap(),
            CanonicalWindow {
                minimum: 5,
                head: 9,
            },
            registry_tables,
            RecoveryObjects::Provider(&store),
        )
    }

    /// A restored catalog may only publish artifacts this source can serve: on
    /// its epoch, under the id the row is stored at, over tables the registry
    /// has, and at a cursor the retained log can continue from. An artifact
    /// at sequence 4 under a minimum of 5 still continues, because the first
    /// record it needs is 5.
    #[test]
    fn the_catalog_gate_refuses_an_artifact_the_restored_source_cannot_serve() {
        let manifest = manifest_at(4);
        let id = manifest.artifact_id.as_str().to_owned();
        let epoch = manifest.pit.source_epoch.clone();
        for sequence in [4, 9] {
            catalog_gate(&id, "PUBLISHED", &manifest_at(sequence), &epoch, 1).unwrap();
        }

        let other_epoch = SourceEpoch::new("source_42/epoch_8").unwrap();
        for (case, outcome) in [
            (
                "a row stored under another id",
                catalog_gate("artifact_other", "PUBLISHED", &manifest, &epoch, 1),
            ),
            (
                "an artifact on another epoch",
                catalog_gate(&id, "PUBLISHED", &manifest, &other_epoch, 1),
            ),
            (
                "more tables than the registry",
                catalog_gate(&id, "PUBLISHED", &manifest, &epoch, 0),
            ),
        ] {
            assert!(
                matches!(outcome, Err(RecoveryError::CatalogIdentityMismatch)),
                "{case}: {outcome:?}"
            );
        }
        for sequence in [3, 10] {
            let outcome = catalog_gate(&id, "PUBLISHED", &manifest_at(sequence), &epoch, 1);
            assert!(
                matches!(outcome, Err(RecoveryError::ArtifactOutsideCanonicalWindow)),
                "sequence {sequence}: {outcome:?}"
            );
        }
        let mut forged = manifest.clone();
        forged.pit.snapshot_cursor =
            CursorCodec::new([0xa5; 32])
                .unwrap()
                .encode(&sync_types::CursorPosition {
                    source_epoch: epoch.clone(),
                    sequence: 4,
                });
        assert!(matches!(
            catalog_gate(&id, "PUBLISHED", &forged, &epoch, 1),
            Err(RecoveryError::Cursor(
                sync_types::CursorError::InvalidSignature
            ))
        ));
        assert!(matches!(
            catalog_gate(&id, "RETIRED", &manifest, &epoch, 1),
            Err(RecoveryError::NoPublishedArtifact)
        ));
    }

    #[test]
    fn aggregate_oracle_binds_every_component_and_is_deterministic() {
        let first = assemble_oracle(
            "11".repeat(32),
            "22".repeat(32),
            "33".repeat(32),
            "44".repeat(32),
        );
        let same = assemble_oracle(
            "11".repeat(32),
            "22".repeat(32),
            "33".repeat(32),
            "44".repeat(32),
        );
        assert_eq!(first, same);
        assert_ne!(
            first,
            assemble_oracle(
                "11".repeat(32),
                "22".repeat(32),
                "33".repeat(32),
                "45".repeat(32),
            )
        );
    }

    #[test]
    fn recovery_refusals_explain_the_safe_next_step() {
        let temporary = tempfile::tempdir().unwrap();
        let incomplete = temporary.path().join("incomplete-backup");
        fs::create_dir(&incomplete).unwrap();
        let error = read_manifest(&incomplete).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!(
                "backup at {} is incomplete: recovery.json is missing; use a directory produced by a successful calque backup",
                incomplete.display()
            )
        );
        assert_eq!(fs::read_dir(&incomplete).unwrap().count(), 0);

        // A unit older than the oracle definition is refused by version, and
        // the refusal says to take a new one rather than reading like a
        // corrupt backup.
        assert_eq!(
            RecoveryError::UnsupportedFormat(1).to_string(),
            "recovery unit is format version 1, and this build reads versions 2 to 3; restore it with the calque release that took it, or take a fresh unit with calque backup from a deployment running this build"
        );
        assert_eq!(
            RecoveryError::ActiveSourceLease.to_string(),
            "source still has a live ownership lease; stop the calque process that serves it, wait for it to exit, then retry"
        );
        assert_eq!(
            RecoveryError::ActiveReplicationSlot.to_string(),
            "source logical replication slot is active; stop the process using the slot, then retry"
        );
        assert_eq!(
            RecoveryError::ReplicationSlotMissing.to_string(),
            "source logical replication slot is missing; restore cannot prove CDC continuity, so destroy and re-enroll the source for a full snapshot"
        );
        assert_eq!(
            RecoveryError::SlotContinuityMismatch.to_string(),
            "replication slot is ahead of or incompatible with restored canonical truth; restore cannot prove CDC continuity, so destroy and re-enroll the source for a full snapshot"
        );
        assert_eq!(
            RecoveryError::SourceRowsAlreadyExist("sources").to_string(),
            "restore found existing source rows in calque_control.sources; restore is absent-only by default: rerun with --replace-source-rows if that source is stopped and this unit continues its logical slot. Do not run destroy first; it drops the slot restore needs"
        );
        assert_eq!(
            RecoveryError::TargetExists("backup".into()).to_string(),
            "recovery target already exists at backup; backup and restore never overwrite, so choose an absent path or inspect and remove the incomplete target before retrying"
        );
        assert_eq!(
            RecoveryError::SourceIdentityMismatch {
                state_dir: ".calque-dev".into(),
                persisted:
                    "database calque_dev on PostgreSQL system 111 timeline 1 (previous 1)"
                        .to_owned(),
                connected:
                    "database calque_dev on PostgreSQL system 222 timeline 2 (previous 1)"
                        .to_owned(),
            }
            .to_string(),
            "connected database calque_dev on PostgreSQL system 222 timeline 2 (previous 1), but state at .calque-dev belongs to database calque_dev on PostgreSQL system 111 timeline 1 (previous 1)."
        );
    }

    /// The object oracle read off a tree binds the name and the bytes of every
    /// object the catalog publishes, and nothing else: a bucket cannot be
    /// asked what else is under the prefix, so a number that moved when an
    /// unpublished file appeared could not be the same on both stores.
    #[test]
    fn the_tree_oracle_binds_published_names_and_bytes_and_only_those() {
        let temporary = tempfile::tempdir().unwrap();
        let state_root = temporary.path().join("deployment-state");
        fs::create_dir(&state_root).unwrap();
        let catalog = state_root.join(CATALOG_FILE);
        let bodies = object_half_catalog(&catalog);
        filesystem_deployment(&state_root, &bodies);

        let before = hash_objects(RecoveryObjects::StateDirectory, &state_root, &catalog).unwrap();
        assert_eq!(
            before,
            hash_objects(RecoveryObjects::StateDirectory, &state_root, &catalog).unwrap()
        );

        // Something the catalog does not publish, beside the objects it does.
        fs::write(
            state_root.join(OBJECT_DIRECTORY).join("staging/abandoned"),
            b"never visible",
        )
        .unwrap();
        assert_eq!(
            before,
            hash_objects(RecoveryObjects::StateDirectory, &state_root, &catalog).unwrap(),
            "the oracle covers the published set, not whatever else the tree holds",
        );

        // A published object's bytes, changed under it.
        let first = PublishedObject {
            object_id: FIRST_OBJECT.to_owned(),
            expected_bytes: bodies[0].1.len() as u64,
            expected_sha256: hex::encode(Sha256::digest(&bodies[0].1)),
        };
        let path = state_root
            .join(OBJECT_DIRECTORY)
            .join(first.relative_path());
        fs::write(&path, b"different published bytes").unwrap();
        assert_ne!(
            before,
            hash_objects(RecoveryObjects::StateDirectory, &state_root, &catalog).unwrap()
        );

        // And a published object that is gone is a refusal, not a shorter hash.
        fs::remove_file(&path).unwrap();
        assert!(matches!(
            hash_objects(RecoveryObjects::StateDirectory, &state_root, &catalog),
            Err(RecoveryError::Io(error)) if error.kind() == io::ErrorKind::NotFound
        ));
    }

    #[test]
    fn catalog_oracle_is_logical_and_detects_a_row_change() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join(CATALOG_FILE);
        let catalog = artifact_catalog::SqliteArtifactCatalog::open(&path).unwrap();
        drop(catalog);
        let before = catalog_logical_sha256(&path).unwrap();
        let connection = Connection::open(&path).unwrap();
        connection
            .execute(
                "INSERT INTO artifact_build_cleanup_receipts
                 (artifact_id, cleanup_token, completed_at_ms, deleted_object_count, shared_object_count)
                 VALUES ('artifact', 'token', 1, 0, 0)",
                [],
            )
            .unwrap();
        drop(connection);
        assert_ne!(before, catalog_logical_sha256(&path).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn copy_tree_rejects_symlinks_and_never_overwrites_a_target() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("source");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("file"), b"owned").unwrap();
        symlink(source.join("file"), source.join("link")).unwrap();
        assert!(matches!(
            copy_tree(&source, &temporary.path().join("target")),
            Err(RecoveryError::UnsafePath(_))
        ));

        let existing = temporary.path().join("existing");
        fs::create_dir(&existing).unwrap();
        assert!(matches!(
            copy_tree(&source, &existing),
            Err(RecoveryError::TargetExists(_))
        ));
    }

    /// One live source, enrolled against the live database and snapshotted
    /// onto the filesystem store, with one published artifact in its catalog
    /// and its runtime still running. The PostgreSQL objects and the source
    /// table go away again when this drops, whatever the test did.
    struct LiveSource {
        database: DatabaseUrl,
        raw_database: String,
        state: crate::LocalState,
        table_name: String,
        root: PathBuf,
        prepared: crate::PreparedLocalSource,
        signal: snapshot_pipeline::CanonicalHeadSignal,
        metrics: crate::OperationsMetrics,
        runtime: Option<crate::LocalSourceRuntime>,
    }

    impl LiveSource {
        fn enrol(root: PathBuf) -> Self {
            let live = Self::enrol_without_snapshot(root);
            live.snapshot().unwrap();
            live
        }

        /// The enrolled source with its runtime running and nothing published.
        fn enrol_without_snapshot(root: PathBuf) -> Self {
            use snapshot_pipeline::CanonicalHeadSignal;
            use source_control::QualifiedTable;

            let raw_database = live_database_url().expect("live recovery database URL");
            let database = DatabaseUrl::parse(&raw_database).unwrap();
            let suffix = uuid::Uuid::new_v4().simple().to_string();
            let table_name = format!("recovery_{suffix}");
            let state = crate::LocalState::new(
                crate::inspect_local_identity(&database).unwrap(),
                SourceEpoch::new(format!("recovery-epoch-{suffix}")).unwrap(),
                crate::LocalNames {
                    source_id: SourceId::new(format!("recovery-source-{suffix}")).unwrap(),
                    publication_name: format!("recovery_publication_{suffix}"),
                    slot_name: format!("recovery_slot_{suffix}"),
                },
                vec![QualifiedTable {
                    schema: "public".to_owned(),
                    table: table_name.clone(),
                }],
            )
            .unwrap();

            let mut source = Client::connect(&raw_database, postgres::NoTls).unwrap();
            source
                .batch_execute(&format!(
                    "CREATE TABLE public.\"{table_name}\" (id bigint PRIMARY KEY, value text NOT NULL); \
                     INSERT INTO public.\"{table_name}\" VALUES (1, 'restored')"
                ))
                .unwrap();

            let state_store = StateStore::open(&root).unwrap();
            state_store.write_state(&state).unwrap();
            let secrets = state_store.load_or_create_secrets().unwrap();
            let prepared = crate::prepare_local_source(&database, &state, &secrets, None).unwrap();
            state_store.write_registry(&prepared.registry).unwrap();

            let metrics = crate::OperationsMetrics::new();
            let signal = CanonicalHeadSignal::new();
            let runtime = crate::start_local_source_runtime(
                prepared.clone(),
                crate::LocalSourceRuntimeConfig::new(
                    &database,
                    &state,
                    &root,
                    format!("recovery-owner-{suffix}"),
                )
                .with_metrics(metrics.clone()),
                signal.clone(),
            )
            .unwrap();

            Self {
                database,
                raw_database,
                state,
                table_name,
                root,
                prepared,
                signal,
                metrics,
                runtime: Some(runtime),
            }
        }

        /// Publishes this source's initial artifact on its state directory, as a
        /// process starting on it does.
        fn snapshot(&self) -> Result<crate::LocalSnapshotOutcome, crate::LocalSnapshotError> {
            self.snapshot_through(self.database.clone())
        }

        /// `snapshot`, with the build's connections made through `database`
        /// while the runtime keeps the one it started on.
        fn snapshot_through(
            &self,
            database: DatabaseUrl,
        ) -> Result<crate::LocalSnapshotOutcome, crate::LocalSnapshotError> {
            use tokio_util::sync::CancellationToken;

            let mut catalog = self.catalog();
            let store: std::sync::Arc<dyn blob_store::ManagedBlobStore> = std::sync::Arc::new(
                blob_store::LocalFilesystemBlobStore::open(
                    self.root.join(OBJECT_DIRECTORY),
                    crate::MAX_LOCAL_ARTIFACT_BYTES,
                )
                .unwrap(),
            );
            let builder = crate::ArtifactBuilder {
                database_url: database,
                state_root: self.root.clone(),
                catalog_path: self.root.join(CATALOG_FILE),
                store,
                barrier_provider: self
                    .runtime
                    .as_ref()
                    .expect("the source is running")
                    .barrier_provider(),
                head_signal: self.signal.clone(),
                cancellation: CancellationToken::new(),
                metrics: self.metrics.clone(),
            };
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(builder.ensure_initial_snapshot(&self.prepared, &mut catalog))
        }

        fn catalog(&self) -> artifact_catalog::SqliteArtifactCatalog {
            artifact_catalog::SqliteArtifactCatalog::open(self.root.join(CATALOG_FILE)).unwrap()
        }

        /// Stops the source. `backup` refuses a live ownership lease, so this
        /// is what a recovery cycle does first.
        fn stop(&mut self) {
            if let Some(runtime) = self.runtime.take() {
                runtime.shutdown().unwrap();
            }
        }
    }

    impl Drop for LiveSource {
        fn drop(&mut self) {
            if let Some(runtime) = self.runtime.take() {
                let _ = runtime.shutdown();
            }
            let _ = crate::destroy_local_source(&self.database, &self.state);
            if let Ok(mut client) = Client::connect(&self.raw_database, postgres::NoTls) {
                let _ = client.batch_execute(&format!(
                    "DROP TABLE IF EXISTS public.\"{}\"",
                    self.table_name.replace('"', "\"\"")
                ));
            }
        }
    }

    #[test]
    #[ignore = "requires CALQUE_CONTROL_DATABASE_URL or CALQUE_DATABASE_URL"]
    fn live_backup_restore_round_trip_matches_logical_oracle() {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        let backup_root = temporary.path().join("backup");
        let restored_root = temporary.path().join("restored");
        let mut live = LiveSource::enrol(source_root.clone());
        let database = live.database.clone();
        let raw_database = live.raw_database.clone();
        let state = live.state.clone();

        let active_backup = temporary.path().join("active-backup");
        assert!(matches!(
            create_recovery_backup(
                &database,
                &source_root,
                &active_backup,
                RecoveryObjects::StateDirectory
            ),
            Err(RecoveryError::ActiveSourceLease)
        ));
        assert!(!active_backup.exists());
        live.stop();

        let backed_up = create_recovery_backup(
            &database,
            &source_root,
            &backup_root,
            RecoveryObjects::StateDirectory,
        )
        .unwrap();
        let manifest = read_manifest(&backup_root).unwrap();
        let mut continuity = Client::connect(&raw_database, postgres::NoTls).unwrap();
        require_restore_slot_continuity(
            &mut continuity,
            &manifest.slot_name,
            &manifest.durable_canonical_lsn,
        )
        .unwrap();
        assert!(matches!(
            require_restore_slot_continuity(&mut continuity, &manifest.slot_name, "0/0"),
            Err(RecoveryError::SlotContinuityMismatch)
        ));

        let refused_restore = temporary.path().join("refused-restore");
        assert!(matches!(
            restore_recovery_backup(
                &database,
                &backup_root,
                &refused_restore,
                ExistingSourceRows::Refuse,
                RecoveryObjects::StateDirectory
            ),
            Err(RecoveryError::SourceRowsAlreadyExist(_))
        ));
        assert!(!refused_restore.exists());

        // An owned, unexpired lease is a live runtime even while the slot is
        // momentarily inactive (a takeover in its provisional window); the
        // replace path refuses it as `backup` does.
        let mut control = Client::connect(&raw_database, postgres::NoTls).unwrap();
        let leased = control
            .execute(
                "UPDATE calque_control.source_fence_lease
                    SET lease_phase = 'provisional', owner_id = 'recovery-test-holder',
                        expires_at = clock_timestamp() + interval '1 hour'
                  WHERE source_id = $1",
                &[&state.names.source_id.as_str()],
            )
            .unwrap();
        assert_eq!(leased, 1);
        let leased_restore = temporary.path().join("leased-restore");
        assert!(matches!(
            restore_recovery_backup(
                &database,
                &backup_root,
                &leased_restore,
                ExistingSourceRows::ReplaceStopped,
                RecoveryObjects::StateDirectory
            ),
            Err(RecoveryError::ActiveSourceLease)
        ));
        assert!(!leased_restore.exists());
        let released = control
            .execute(
                "UPDATE calque_control.source_fence_lease
                    SET lease_phase = 'unowned', owner_id = NULL, expires_at = NULL,
                        source_backend_pid = NULL
                  WHERE source_id = $1",
                &[&state.names.source_id.as_str()],
            )
            .unwrap();
        assert_eq!(released, 1);

        // The stopped source's own rows are still there; the replace path
        // takes them out under the same idle-lease and inactive-slot gates
        // and puts the unit's rows in their place.
        let restored = restore_recovery_backup(
            &database,
            &backup_root,
            &restored_root,
            ExistingSourceRows::ReplaceStopped,
            RecoveryObjects::StateDirectory,
        )
        .unwrap();
        assert_eq!(backed_up.oracle, restored.oracle);
        assert_eq!(
            verify_restored_recovery_backup(
                &database,
                &backup_root,
                &restored_root,
                RecoveryObjects::StateDirectory,
            )
            .unwrap()
            .oracle,
            backed_up.oracle
        );

        // Control-row loss with the slot intact is the absent-only case: no
        // row of the source remains, and the unit's rows go in without the
        // flag.
        lose_control_rows(&mut control, &state.names.source_id);
        let restored_absent_root = temporary.path().join("restored-absent");
        let restored_absent = restore_recovery_backup(
            &database,
            &backup_root,
            &restored_absent_root,
            ExistingSourceRows::Refuse,
            RecoveryObjects::StateDirectory,
        )
        .unwrap();
        assert_eq!(backed_up.oracle, restored_absent.oracle);
    }

    /// Deletes every row a unit carries for `source_id`, as a lost control
    /// schema would leave it with the slot intact.
    fn lose_control_rows(control: &mut Client, source_id: &SourceId) {
        let mut transaction = control.transaction().unwrap();
        for table in CONTROL_TABLES.iter().rev() {
            transaction
                .execute(
                    &format!(
                        "DELETE FROM calque_control.{} WHERE source_id = $1",
                        table.name
                    ),
                    &[&source_id.as_str()],
                )
                .unwrap();
        }
        transaction.commit().unwrap();
    }

    /// A unit carries no authorization fence, so restoring one taken before a
    /// revocation leaves the revoked generation fenced. `serve` starts by
    /// establishing the fence at its manifest's generation, and that call
    /// answers the database's value, not the unit's.
    #[test]
    #[ignore = "requires CALQUE_CONTROL_DATABASE_URL or CALQUE_DATABASE_URL"]
    fn a_restore_of_an_older_unit_cannot_lower_the_authorization_fence() {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        let backup_root = temporary.path().join("backup");
        let mut live = LiveSource::enrol(source_root.clone());
        let database = live.database.clone();
        let source_id = live.state.names.source_id.clone();
        let fence = crate::AuthorizationFenceStore::new(database.clone(), source_id.clone());
        let _fence_row = FenceRowCleanup {
            raw_database: live.raw_database.clone(),
            source_id: source_id.clone(),
        };
        assert_eq!(fence.establish(3).unwrap(), 3);
        live.stop();
        create_recovery_backup(
            &database,
            &source_root,
            &backup_root,
            RecoveryObjects::StateDirectory,
        )
        .unwrap();

        // Generation 3 is revoked after the unit was taken.
        assert_eq!(fence.establish(7).unwrap(), 7);

        restore_recovery_backup(
            &database,
            &backup_root,
            &temporary.path().join("replaced"),
            ExistingSourceRows::ReplaceStopped,
            RecoveryObjects::StateDirectory,
        )
        .unwrap();
        assert_eq!(
            fence.establish(3).unwrap(),
            7,
            "a restore over the stopped source lowered the authorization fence",
        );

        let mut control = Client::connect(&live.raw_database, postgres::NoTls).unwrap();
        lose_control_rows(&mut control, &source_id);
        restore_recovery_backup(
            &database,
            &backup_root,
            &temporary.path().join("absent"),
            ExistingSourceRows::Refuse,
            RecoveryObjects::StateDirectory,
        )
        .unwrap();
        assert_eq!(
            fence.establish(3).unwrap(),
            7,
            "a restore after control-row loss lowered the authorization fence",
        );
    }

    /// The final verification is the restore's last check, and a unit it
    /// refuses leaves the source's control rows as they were: here, absent.
    /// The same unit into a store that serves what it was given then
    /// restores, so the refusal is the store's fault and nothing else.
    #[test]
    #[ignore = "requires CALQUE_CONTROL_DATABASE_URL or CALQUE_DATABASE_URL"]
    fn a_restore_its_verification_refuses_commits_no_control_row() {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        let backup_root = temporary.path().join("backup");
        let mut live = LiveSource::enrol(source_root.clone());
        let database = live.database.clone();
        let source_id = live.state.names.source_id.clone();
        live.stop();
        let backed_up = create_recovery_backup(
            &database,
            &source_root,
            &backup_root,
            RecoveryObjects::StateDirectory,
        )
        .unwrap();
        let mut control = Client::connect(&live.raw_database, postgres::NoTls).unwrap();
        lose_control_rows(&mut control, &source_id);
        let source_rows = |control: &mut Client| -> Vec<(&'static str, i64)> {
            CONTROL_TABLES
                .iter()
                .map(|table| {
                    let query = format!(
                        "SELECT count(*) FROM calque_control.{} WHERE source_id = $1",
                        table.name
                    );
                    let count: i64 = control
                        .query_one(&query, &[&source_id.as_str()])
                        .unwrap()
                        .get(0);
                    (table.name, count)
                })
                .collect()
        };
        let absent: Vec<_> = CONTROL_TABLES.iter().map(|table| (table.name, 0)).collect();
        assert_eq!(source_rows(&mut control), absent);

        let corrupting = MemoryObjectStore::corrupting_reads();
        let error = restore_recovery_backup(
            &database,
            &backup_root,
            &temporary.path().join("refused"),
            ExistingSourceRows::Refuse,
            RecoveryObjects::Provider(&corrupting),
        )
        .unwrap_err();
        assert!(matches!(error, RecoveryError::OracleMismatch), "{error}");
        assert_eq!(
            source_rows(&mut control),
            absent,
            "a restore its final verification refused committed control rows",
        );

        let sound = MemoryObjectStore::new();
        let restored = restore_recovery_backup(
            &database,
            &backup_root,
            &temporary.path().join("restored"),
            ExistingSourceRows::Refuse,
            RecoveryObjects::Provider(&sound),
        )
        .unwrap();
        assert_eq!(restored.oracle, backed_up.oracle);
        assert!(source_rows(&mut control)
            .iter()
            .any(|(_, count)| *count > 0));
    }

    /// A unit the previous release took has no compaction watermark. It
    /// restores with none recorded, as upgrading that release in place leaves
    /// the log, and its verification refuses a watermark it did not carry.
    #[test]
    #[ignore = "requires CALQUE_CONTROL_DATABASE_URL or CALQUE_DATABASE_URL"]
    fn a_unit_from_before_the_compaction_watermark_restores_without_one() {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        let backup_root = temporary.path().join("backup");
        let mut live = LiveSource::enrol(source_root.clone());
        let database = live.database.clone();
        let source_id = live.state.names.source_id.clone();
        live.stop();
        create_recovery_backup(
            &database,
            &source_root,
            &backup_root,
            RecoveryObjects::StateDirectory,
        )
        .unwrap();

        // The unit as the previous release wrote it: four retention columns,
        // format version 2, and the oracle over those files.
        let mut control = Client::connect(&live.raw_database, postgres::NoTls).unwrap();
        let retention = backup_root
            .join(CONTROL_DIRECTORY)
            .join("canonical_log_retention.bin");
        fs::remove_file(&retention).unwrap();
        let mut reader = control
            .copy_out(&format!(
                "COPY (SELECT source_id, source_epoch, minimum_retained_sequence, next_build_lease_token
                         FROM calque_control.canonical_log_retention
                        WHERE source_id = '{}' ORDER BY source_id) TO STDOUT WITH (FORMAT binary)",
                source_id.as_str()
            ))
            .unwrap();
        io::copy(&mut reader, &mut File::create(&retention).unwrap()).unwrap();
        drop(reader);
        let mut manifest = read_manifest(&backup_root).unwrap();
        manifest.format_version = 2;
        manifest.oracle =
            recovery_oracle_from_backup(&backup_root, &source_id, &live.state.source_epoch)
                .unwrap();
        fs::write(
            backup_root.join(COMPLETE_FILE),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let record_watermark = |control: &mut Client| {
            let updated = control
                .execute(
                    "UPDATE calque_control.canonical_log_retention
                        SET retired_compacted_through_lsn = durable.lsn
                       FROM (SELECT durable_canonical_lsn::pg_lsn AS lsn
                               FROM calque_control.source_slot_enrollment
                              WHERE source_id = $1) AS durable
                      WHERE source_id = $1",
                    &[&source_id.as_str()],
                )
                .unwrap();
            assert_eq!(updated, 1);
        };
        let watermark = |control: &mut Client| -> Option<String> {
            control
                .query_one(
                    "SELECT retired_compacted_through_lsn::text
                       FROM calque_control.canonical_log_retention WHERE source_id = $1",
                    &[&source_id.as_str()],
                )
                .unwrap()
                .get(0)
        };
        record_watermark(&mut control);
        let restored_root = temporary.path().join("restored");
        let restored = restore_recovery_backup(
            &database,
            &backup_root,
            &restored_root,
            ExistingSourceRows::ReplaceStopped,
            RecoveryObjects::StateDirectory,
        )
        .unwrap();
        assert_eq!(restored.oracle, manifest.oracle);
        assert_eq!(watermark(&mut control), None);
        verify_restored_recovery_backup(
            &database,
            &backup_root,
            &restored_root,
            RecoveryObjects::StateDirectory,
        )
        .unwrap();

        record_watermark(&mut control);
        let error = verify_restored_recovery_backup(
            &database,
            &backup_root,
            &restored_root,
            RecoveryObjects::StateDirectory,
        )
        .unwrap_err();
        assert!(matches!(error, RecoveryError::OracleMismatch), "{error}");
    }

    /// A unit whose manifest names another epoch or slot than the state
    /// directory it carries is refused before a restore writes anything. A
    /// retained floor past the log head fails verification, and a source whose
    /// slot is gone cannot be backed up.
    #[test]
    #[ignore = "requires CALQUE_CONTROL_DATABASE_URL or CALQUE_DATABASE_URL"]
    fn a_unit_or_a_log_window_that_contradicts_itself_is_refused() {
        let temporary = tempfile::tempdir().unwrap();
        let source_root = temporary.path().join("source");
        let backup_root = temporary.path().join("backup");
        let mut live = LiveSource::enrol(source_root.clone());
        let database = live.database.clone();
        let source_id = live.state.names.source_id.clone();
        live.stop();
        create_recovery_backup(
            &database,
            &source_root,
            &backup_root,
            RecoveryObjects::StateDirectory,
        )
        .unwrap();

        let manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(backup_root.join(COMPLETE_FILE)).unwrap()).unwrap();
        for (field, value) in [
            ("slotName", "recovery_slot_other"),
            ("sourceEpoch", "recovery-epoch-other"),
        ] {
            let tampered = temporary.path().join(format!("tampered-{field}"));
            copy_tree(&backup_root, &tampered).unwrap();
            let mut edited = manifest.clone();
            edited[field] = serde_json::Value::from(value);
            fs::write(
                tampered.join(COMPLETE_FILE),
                serde_json::to_vec(&edited).unwrap(),
            )
            .unwrap();
            let target = temporary.path().join(format!("restored-{field}"));
            let outcome = restore_recovery_backup(
                &database,
                &tampered,
                &target,
                ExistingSourceRows::ReplaceStopped,
                RecoveryObjects::StateDirectory,
            );
            assert!(
                matches!(outcome, Err(RecoveryError::BackupIdentityMismatch)),
                "{field}: {outcome:?}"
            );
            assert!(!target.exists(), "{field}");
        }

        let verify = || {
            verify_restored_recovery_backup(
                &database,
                &backup_root,
                &source_root,
                RecoveryObjects::StateDirectory,
            )
        };
        verify().unwrap();
        let mut control = Client::connect(&live.raw_database, postgres::NoTls).unwrap();
        let moved = control
            .execute(
                "UPDATE calque_control.canonical_log_retention AS retention
                    SET minimum_retained_sequence = COALESCE(
                            (SELECT head.head_sequence FROM calque_control.canonical_log_head AS head
                              WHERE head.source_id = retention.source_id
                                AND head.source_epoch = retention.source_epoch), 0) + 2
                  WHERE retention.source_id = $1",
                &[&source_id.as_str()],
            )
            .unwrap();
        assert_eq!(moved, 1);
        let outcome = verify();
        assert!(
            matches!(outcome, Err(RecoveryError::CanonicalBoundsInvalid)),
            "{outcome:?}"
        );

        control
            .execute(
                "SELECT pg_drop_replication_slot($1)",
                &[&live.state.names.slot_name],
            )
            .unwrap();
        let slotless = temporary.path().join("slotless-backup");
        let outcome = create_recovery_backup(
            &database,
            &source_root,
            &slotless,
            RecoveryObjects::StateDirectory,
        );
        assert!(
            matches!(outcome, Err(RecoveryError::ReplicationSlotMissing)),
            "{outcome:?}"
        );
        assert!(!slotless.exists());
    }

    /// `destroy_local_source` leaves the fence row, so a test that raised one
    /// removes it itself.
    struct FenceRowCleanup {
        raw_database: String,
        source_id: SourceId,
    }

    impl Drop for FenceRowCleanup {
        fn drop(&mut self) {
            if let Ok(mut client) = Client::connect(&self.raw_database, postgres::NoTls) {
                let _ = client.execute(
                    "DELETE FROM calque_control.authorization_fence WHERE source_id = $1",
                    &[&self.source_id.as_str()],
                );
            }
        }
    }

    /// A build whose PostgreSQL freezes while the DDL guard waits for a table
    /// lock fails once that connection's reply bound runs out, 33 s after
    /// its statement began, and the error says the server stopped answering.
    #[test]
    #[ignore = "requires CALQUE_CONTROL_DATABASE_URL or CALQUE_DATABASE_URL"]
    fn a_snapshot_whose_postgres_freezes_mid_preparation_fails_at_its_reply_bound() {
        let temporary = tempfile::tempdir().unwrap();
        let live = LiveSource::enrol_without_snapshot(temporary.path().join("source"));
        let relay = postgres_connect::relay::SilenceableRelay::start(&live.raw_database);
        let through = DatabaseUrl::parse(relay.url()).unwrap();
        let mut ddl = Client::connect(&live.raw_database, postgres::NoTls).unwrap();
        ddl.batch_execute(&format!(
            "BEGIN; LOCK TABLE public.\"{}\" IN ACCESS EXCLUSIVE MODE",
            live.table_name
        ))
        .unwrap();

        let raw_database = live.raw_database.clone();
        let live_table = live.table_name.clone();
        let (sender, receiver) = std::sync::mpsc::channel();
        let building = std::thread::spawn(move || {
            let started = std::time::Instant::now();
            let outcome = live.snapshot_through(through);
            let _ = sender.send((outcome, started.elapsed()));
            live
        });
        let mut watcher = Client::connect(&raw_database, postgres::NoTls).unwrap();
        let waiting = std::time::Instant::now();
        while watcher
            .query_one(
                "SELECT count(*) FROM pg_stat_activity
                  WHERE application_name = 'calque-snapshot' AND wait_event_type = 'Lock'",
                &[],
            )
            .unwrap()
            .get::<_, i64>(0)
            == 0
        {
            assert!(
                waiting.elapsed() < std::time::Duration::from_secs(20),
                "the DDL guard never waited for the lock"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        relay.silence();
        // The server goes on without the client for the 33 s the bound takes
        // to run out. Ending the guard's backend and releasing the lock now
        // leaves no transaction open for the other live tests, whose slot
        // creation waits on every open transaction. Only this test's table
        // picks the guard out from the other tests' snapshot backends.
        watcher
            .execute(
                "SELECT pg_terminate_backend(l.pid) FROM pg_locks l
                   JOIN pg_class c ON c.oid = l.relation
                  WHERE c.relname = $1 AND l.pid <> pg_backend_pid()
                    AND l.pid IN (SELECT pid FROM pg_stat_activity
                                   WHERE application_name = 'calque-snapshot')",
                &[&live_table],
            )
            .unwrap();
        ddl.batch_execute("ROLLBACK").unwrap();

        let received = receiver.recv_timeout(std::time::Duration::from_secs(50));
        let (outcome, elapsed) =
            received.expect("the build on a silent PostgreSQL had not returned after 50 s");
        let live = building.join().unwrap();
        let error = outcome.expect_err("a frozen PostgreSQL cannot publish");
        assert!(
            elapsed >= std::time::Duration::from_secs(30)
                && elapsed < std::time::Duration::from_secs(40),
            "the build returned after {elapsed:?}: {error:?}"
        );
        assert!(
            matches!(&error, crate::LocalSnapshotError::ConnectionLost(cause)
                if cause.contains("the server sent nothing while a reply was awaited")),
            "{error:?}"
        );
        drop(live);
    }

    /// One S3-compatible store, from the same `CALQUE_S3_*` names `serve` and
    /// the blob-store conformance suite read. `prefix` is the caller's: two
    /// stores in one test must not share one, or the second deployment would
    /// be reading the first one's objects and proving nothing.
    fn live_object_store(staging: &Path, prefix: &str) -> Option<blob_store::S3BlobStore> {
        let endpoint = std::env::var("CALQUE_S3_ENDPOINT").ok()?;
        let bucket = std::env::var("CALQUE_S3_BUCKET").ok()?;
        let access_key_id = std::env::var("CALQUE_S3_ACCESS_KEY").ok()?;
        let secret_access_key = std::env::var("CALQUE_S3_SECRET_KEY").ok()?;
        Some(
            blob_store::S3BlobStore::connect(
                blob_store::S3Endpoint {
                    region: std::env::var("CALQUE_S3_REGION")
                        .unwrap_or_else(|_| "us-east-1".to_owned()),
                    endpoint_url: Some(endpoint),
                    force_path_style: true,
                    access_key_id,
                    secret_access_key,
                    session_token: None,
                },
                blob_store::S3BlobStoreConfig::new(
                    bucket,
                    prefix,
                    staging,
                    crate::MAX_LOCAL_ARTIFACT_BYTES,
                )
                .unwrap(),
            )
            .unwrap(),
        )
    }

    /// Every PUBLISHED object of a catalog, with the bytes the deployment's
    /// own object tree holds for it.
    fn published_bytes(catalog: &Path, objects_root: &Path) -> Vec<(ObjectId, Vec<u8>)> {
        let connection = open_catalog_read_only(catalog).unwrap();
        published_objects(&connection)
            .unwrap()
            .into_iter()
            .map(|object| {
                let bytes = fs::read(objects_root.join(object.relative_path())).unwrap();
                assert_eq!(bytes.len() as u64, object.expected_bytes);
                (object.identifier().unwrap(), bytes)
            })
            .collect()
    }

    fn read_through(store: &dyn BlobStore, object: &ObjectId) -> Option<Vec<u8>> {
        let runtime = object_runtime().unwrap();
        let mut stream = runtime.block_on(store.get(object)).unwrap()?;
        let mut bytes = Vec::new();
        runtime
            .block_on(tokio::io::AsyncReadExt::read_to_end(
                &mut stream,
                &mut bytes,
            ))
            .unwrap();
        Some(bytes)
    }

    /// A unit taken on the filesystem store, restored into a real S3 bucket,
    /// and the artifact read back out of the store afterwards; then a second
    /// unit taken from the restored S3 deployment and restored into a second
    /// prefix. Nothing here is a double: `ensure_initial_snapshot` wrote the
    /// object tree, and the store is `S3BlobStore` against a real provider.
    #[test]
    #[ignore = "requires a live database and CALQUE_S3_* for an isolated bucket"]
    fn a_filesystem_unit_restores_into_a_provider_store_and_serves_from_it() {
        let temporary = tempfile::tempdir().unwrap();
        let run = uuid::Uuid::new_v4().simple().to_string();
        let first_prefix = format!("recovery-provider-{run}-first");
        let second_prefix = format!("recovery-provider-{run}-second");
        let Some(first) = live_object_store(&temporary.path().join("staging-first"), &first_prefix)
        else {
            panic!("CALQUE_S3_ENDPOINT, _BUCKET, _ACCESS_KEY and _SECRET_KEY are required");
        };
        let second = live_object_store(&temporary.path().join("staging-second"), &second_prefix)
            .expect("the same environment opens a second prefix");

        let source_root = temporary.path().join("source");
        let mut live = LiveSource::enrol(source_root.clone());
        let database = live.database.clone();
        let expected = published_bytes(
            &source_root.join(CATALOG_FILE),
            &source_root.join(OBJECT_DIRECTORY),
        );
        assert!(
            !expected.is_empty(),
            "the snapshot published no object, so there is nothing to restore",
        );

        // A crash mid-upload leaves a file in `staging`, which `backup` copies
        // with the rest of the tree. Without such a file the tree equals the
        // published set, and the restore below would pass even if the oracle
        // hashed the whole tree.
        fs::write(
            source_root
                .join(OBJECT_DIRECTORY)
                .join("staging/interrupted-upload"),
            b"never-visible bytes from a crashed put",
        )
        .unwrap();

        struct BucketCleanup<'a> {
            stores: Vec<&'a blob_store::S3BlobStore>,
            objects: Vec<ObjectId>,
        }
        impl Drop for BucketCleanup<'_> {
            fn drop(&mut self) {
                let Ok(runtime) = object_runtime() else {
                    return;
                };
                for store in &self.stores {
                    for object in &self.objects {
                        let _ = runtime.block_on(store.delete(object));
                    }
                }
            }
        }
        let _bucket_cleanup = BucketCleanup {
            stores: vec![&first, &second],
            objects: expected.iter().map(|(id, _)| id.clone()).collect(),
        };

        live.stop();
        let unit = temporary.path().join("filesystem-unit");
        let taken = create_recovery_backup(
            &database,
            &source_root,
            &unit,
            RecoveryObjects::StateDirectory,
        )
        .unwrap();

        // Filesystem unit into a bucket.
        let restored_root = temporary.path().join("restored-on-s3");
        let restored = restore_recovery_backup(
            &database,
            &unit,
            &restored_root,
            ExistingSourceRows::ReplaceStopped,
            RecoveryObjects::Provider(&first),
        )
        .unwrap();
        assert_eq!(taken.oracle, restored.oracle);
        assert!(
            !restored_root.join(OBJECT_DIRECTORY).exists(),
            "a deployment on a bucket keeps no object tree",
        );
        // A rerun is refused once the target state directory exists, so a
        // restore that failed its oracle here would have no way forward.
        assert!(matches!(
            restore_recovery_backup(
                &database,
                &unit,
                &restored_root,
                ExistingSourceRows::ReplaceStopped,
                RecoveryObjects::Provider(&first),
            ),
            Err(RecoveryError::TargetExists(_))
        ));

        // Read back through the store the restored deployment serves from.
        for (object, bytes) in &expected {
            assert_eq!(
                read_through(&first, object).as_ref(),
                Some(bytes),
                "{} is not readable through the restored deployment's store",
                object.as_str(),
            );
        }
        assert_eq!(
            verify_restored_recovery_backup(
                &database,
                &unit,
                &restored_root,
                RecoveryObjects::Provider(&first),
            )
            .unwrap()
            .oracle,
            taken.oracle,
        );

        // And the other direction: a unit taken from the bucket deployment,
        // restored into a second one.
        let provider_unit = temporary.path().join("provider-unit");
        let taken_from_s3 = create_recovery_backup(
            &database,
            &restored_root,
            &provider_unit,
            RecoveryObjects::Provider(&first),
        )
        .unwrap();
        assert_eq!(taken_from_s3.oracle, taken.oracle);

        let second_root = temporary.path().join("restored-on-second-s3");
        let restored_again = restore_recovery_backup(
            &database,
            &provider_unit,
            &second_root,
            ExistingSourceRows::ReplaceStopped,
            RecoveryObjects::Provider(&second),
        )
        .unwrap();
        assert_eq!(restored_again.oracle, taken.oracle);
        for (object, bytes) in &expected {
            assert_eq!(read_through(&second, object).as_ref(), Some(bytes));
        }
    }
}
