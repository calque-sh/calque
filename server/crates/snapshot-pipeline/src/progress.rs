use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

use canonical_log::{SnapshotBuildId, SnapshotBuildOwnerId};
use serde::{Deserialize, Serialize};
use snapshot_coordinator::{
    SnapshotPressureEvaluation, SnapshotPressureLevel, SnapshotPressureReason,
    SnapshotPressureSample,
};
use sync_types::{ArtifactId, SourceEpoch};
use thiserror::Error;

const STATUS_FORMAT_VERSION: u32 = 4;
/// Copy progress between table completions is persisted at most this often
/// or after this many decoded bytes, whichever comes first, since the copy
/// batch that reports it waits for its fsyncs. Nothing resumes a copy from
/// these counters, so a skipped persist only leaves the status file up to a
/// second behind.
const COPY_PROGRESS_PERSIST_INTERVAL_MS: i64 = 1_000;
const COPY_PROGRESS_PERSIST_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SnapshotBuildStage {
    Admitted,
    RetentionLeased,
    Exported,
    Copying,
    RollingForward,
    WaitingForPersistentBarrier,
    Finalizing,
    Uploading,
    Publishing,
    CleaningUp,
    Published,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SnapshotBuildDisposition {
    Retry,
    Rebuild,
    OperatorAction,
    Cancelled,
}

/// Stable, non-secret lifecycle reasons. Raw PostgreSQL, provider, SQL, row,
/// token, and connection-string text never enters the durable operator record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SnapshotBuildFailureCode {
    ProcessInterrupted,
    CallerCancelled,
    LeaseLost,
    RetentionCrossed,
    CopyWorkerFailed,
    ExporterLost,
    TemporaryStreamLost,
    PersistentBarrierTimeout,
    PersistentBarrierCollision,
    SchemaChanged,
    SourceEpochChanged,
    WalPressureLimit,
    CatalogXidAgeLimit,
    CopyTransactionAgeLimit,
    PressureSamplingUncertain,
    BuildDurationLimit,
    NoProgressLimit,
    FinalizationFailed,
    StorageFailed,
    CatalogFailed,
    CleanupFailed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SnapshotBuildProgress {
    pub format_version: u32,
    pub build_id: String,
    pub owner_id: String,
    pub artifact_id: String,
    pub private_work_dir: String,
    pub source_epoch: SourceEpoch,
    pub stage: SnapshotBuildStage,
    pub revision: u64,
    pub started_at_unix_millis: i64,
    pub last_progress_at_unix_millis: i64,
    pub ended_at_unix_millis: Option<i64>,
    pub cancellation_requested_at_unix_millis: Option<i64>,
    pub cleanup_completed_at_unix_millis: Option<i64>,
    pub disposition: Option<SnapshotBuildDisposition>,
    pub failure_code: Option<SnapshotBuildFailureCode>,
    pub tables_total: u64,
    pub tables_completed: u64,
    pub rows_copied: u64,
    pub decoded_value_bytes: u64,
    pub committed_batches: u64,
    pub active_copy_workers: u64,
    pub peak_active_copy_workers: u64,
    pub queued_copy_tables: u64,
    pub barrier_minimum_sequence: Option<u64>,
    pub barrier_head_sequence: Option<u64>,
    pub barrier_records_scanned: u64,
    pub retained_wal_bytes: Option<u64>,
    pub catalog_xid_age: Option<u64>,
    pub oldest_copy_transaction_millis: Option<u64>,
    pub pressure_level: Option<SnapshotPressureLevel>,
    pub pressure_reasons: Vec<SnapshotPressureReason>,
}

/// Durable single-writer lifecycle journal for one private snapshot build.
///
/// Every persisted update is written to a new file, synced and atomically
/// renamed; the directory is synced only where `syncs_directory` says. Every
/// stage boundary is persisted, copy progress only once
/// `COPY_PROGRESS_PERSIST_INTERVAL_MS` or `COPY_PROGRESS_PERSIST_BYTES` has
/// passed since the last persist, and the next persist carries whatever was
/// held back. An uncertain journal write must cancel the build, because
/// operators and cleanup recovery may otherwise lose its last safe boundary.
pub struct SnapshotBuildJournal {
    root: PathBuf,
    path: PathBuf,
    progress: SnapshotBuildProgress,
    /// When, and at how many decoded bytes, copy progress was last persisted.
    copy_persisted: Option<(i64, u64)>,
}

impl SnapshotBuildJournal {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        root: impl AsRef<Path>,
        build_id: &SnapshotBuildId,
        owner_id: &SnapshotBuildOwnerId,
        artifact_id: &ArtifactId,
        private_work_dir: &Path,
        source_epoch: SourceEpoch,
        tables_total: u64,
        now_ms: i64,
    ) -> Result<Self, SnapshotBuildProgressError> {
        validate_timestamp(now_ms)?;
        let root = prepare_root(root.as_ref())?;
        let private_work_dir = validate_private_work_dir(private_work_dir, build_id)?;
        let path = status_path(&root, build_id);
        if path.exists() {
            return Err(SnapshotBuildProgressError::AlreadyExists);
        }
        let mut journal = Self {
            root,
            path,
            copy_persisted: None,
            progress: SnapshotBuildProgress {
                format_version: STATUS_FORMAT_VERSION,
                build_id: build_id.as_str().to_owned(),
                owner_id: owner_id.as_str().to_owned(),
                artifact_id: artifact_id.as_str().to_owned(),
                private_work_dir,
                source_epoch,
                stage: SnapshotBuildStage::Admitted,
                revision: 0,
                started_at_unix_millis: now_ms,
                last_progress_at_unix_millis: now_ms,
                ended_at_unix_millis: None,
                cancellation_requested_at_unix_millis: None,
                cleanup_completed_at_unix_millis: None,
                disposition: None,
                failure_code: None,
                tables_total,
                tables_completed: 0,
                rows_copied: 0,
                decoded_value_bytes: 0,
                committed_batches: 0,
                active_copy_workers: 0,
                peak_active_copy_workers: 0,
                queued_copy_tables: tables_total,
                barrier_minimum_sequence: None,
                barrier_head_sequence: None,
                barrier_records_scanned: 0,
                retained_wal_bytes: None,
                catalog_xid_age: None,
                oldest_copy_transaction_millis: None,
                pressure_level: None,
                pressure_reasons: Vec::new(),
            },
        };
        journal.persist()?;
        Ok(journal)
    }

    pub fn reopen(
        root: impl AsRef<Path>,
        build_id: &SnapshotBuildId,
    ) -> Result<Self, SnapshotBuildProgressError> {
        let root = prepare_root(root.as_ref())?;
        cleanup_orphan_temp_files(&root, Some(build_id))?;
        let path = status_path(&root, build_id);
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(SnapshotBuildProgressError::UnsafeStatus(path));
        }
        let bytes = fs::read(&path)?;
        let progress: SnapshotBuildProgress = serde_json::from_slice(&bytes)?;
        validate_progress(&progress)?;
        if progress.build_id != build_id.as_str() {
            return Err(SnapshotBuildProgressError::IdentityMismatch);
        }
        Ok(Self {
            root,
            path,
            progress,
            copy_persisted: None,
        })
    }

    /// Enumerates durable status records without following symlinks. Startup
    /// recovery is exclusive, so orphaned atomic-write temp files are removed
    /// before a journal can attempt to reuse the interrupted revision.
    pub fn scan(
        root: impl AsRef<Path>,
    ) -> Result<Vec<SnapshotBuildId>, SnapshotBuildProgressError> {
        let root = prepare_root(root.as_ref())?;
        cleanup_orphan_temp_files(&root, None)?;
        let mut builds = Vec::new();
        for entry in fs::read_dir(&root)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_symlink() || !file_type.is_file() {
                return Err(SnapshotBuildProgressError::UnsafeStatus(entry.path()));
            }
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| SnapshotBuildProgressError::UnsafeStatus(entry.path()))?;
            let Some(stem) = name.strip_suffix(".json") else {
                return Err(SnapshotBuildProgressError::UnsafeStatus(entry.path()));
            };
            let build = SnapshotBuildId::new(stem.to_owned())
                .map_err(|_| SnapshotBuildProgressError::UnsafeStatus(entry.path()))?;
            let journal = Self::reopen(&root, &build)?;
            if journal.progress.build_id != stem {
                return Err(SnapshotBuildProgressError::IdentityMismatch);
            }
            builds.push(build);
        }
        builds.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        Ok(builds)
    }

    /// Removes a fully cleaned, unpublished recovery journal so the same
    /// deterministic build identity can be retried. The exact artifact,
    /// workspace, and source epoch must still match the caller's recovery
    /// contract; published or incomplete journals are never removable here.
    pub fn remove_recovered_unpublished(
        root: impl AsRef<Path>,
        build_id: &SnapshotBuildId,
        artifact_id: &ArtifactId,
        private_work_dir: &Path,
        source_epoch: &SourceEpoch,
    ) -> Result<(), SnapshotBuildProgressError> {
        let journal = Self::reopen(root, build_id)?;
        let private_work_dir = validate_private_work_dir(private_work_dir, build_id)?;
        if journal.progress.artifact_id != artifact_id.as_str()
            || journal.progress.private_work_dir != private_work_dir
            || &journal.progress.source_epoch != source_epoch
        {
            return Err(SnapshotBuildProgressError::IdentityMismatch);
        }
        if !matches!(
            journal.progress.stage,
            SnapshotBuildStage::Failed | SnapshotBuildStage::Cancelled
        ) || journal.progress.cleanup_completed_at_unix_millis.is_none()
        {
            return Err(SnapshotBuildProgressError::RemovalNotAllowed);
        }
        fs::remove_file(&journal.path)?;
        File::open(&journal.root)?.sync_all()?;
        Ok(())
    }

    /// Removes a published build's journal once its artifact is the catalog's
    /// to keep or delete. The artifact must be the one the journal names.
    pub fn remove_published(
        root: impl AsRef<Path>,
        build_id: &SnapshotBuildId,
        artifact_id: &ArtifactId,
    ) -> Result<(), SnapshotBuildProgressError> {
        let journal = Self::reopen(root, build_id)?;
        if journal.progress.artifact_id != artifact_id.as_str() {
            return Err(SnapshotBuildProgressError::IdentityMismatch);
        }
        if journal.progress.stage != SnapshotBuildStage::Published {
            return Err(SnapshotBuildProgressError::RemovalNotAllowed);
        }
        fs::remove_file(&journal.path)?;
        File::open(&journal.root)?.sync_all()?;
        Ok(())
    }

    /// Removes the journal of a build that has ended: published, or failed or
    /// cancelled with its cleanup complete. A scope cut again and again keeps
    /// no journal of each cut once nothing in it is left to recover.
    pub fn remove_ended(
        root: impl AsRef<Path>,
        build_id: &SnapshotBuildId,
    ) -> Result<(), SnapshotBuildProgressError> {
        let journal = Self::reopen(root, build_id)?;
        let ended = match journal.progress.stage {
            SnapshotBuildStage::Published => true,
            SnapshotBuildStage::Failed | SnapshotBuildStage::Cancelled => {
                journal.progress.cleanup_completed_at_unix_millis.is_some()
            }
            _ => false,
        };
        if !ended {
            return Err(SnapshotBuildProgressError::RemovalNotAllowed);
        }
        fs::remove_file(&journal.path)?;
        File::open(&journal.root)?.sync_all()?;
        Ok(())
    }

    pub const fn progress(&self) -> &SnapshotBuildProgress {
        &self.progress
    }

    pub const fn is_terminal(&self) -> bool {
        is_terminal(self.progress.stage)
    }

    /// Reconciles a journal lagging the authoritative catalog commit. It is
    /// broader than the normal stage graph: a crash or failed status fsync may
    /// leave any nonterminal stage behind after the catalog has durably
    /// committed `PUBLISHED`.
    pub fn reconcile_published(&mut self, now_ms: i64) -> Result<(), SnapshotBuildProgressError> {
        if self.progress.stage == SnapshotBuildStage::Published {
            return Ok(());
        }
        validate_timestamp_order(self.progress.last_progress_at_unix_millis, now_ms)?;
        self.progress.stage = SnapshotBuildStage::Published;
        self.progress.last_progress_at_unix_millis = now_ms;
        self.progress.ended_at_unix_millis = Some(now_ms);
        self.progress.cancellation_requested_at_unix_millis = None;
        self.progress.cleanup_completed_at_unix_millis = None;
        self.progress.disposition = None;
        self.progress.failure_code = None;
        self.progress.active_copy_workers = 0;
        self.bump_and_persist()
    }

    /// Completes startup recovery after all never-visible state has been
    /// removed. A journal already in CLEANING_UP preserves its original typed
    /// failure; earlier stages are recorded as process interruption.
    pub(crate) fn reconcile_interrupted_cleanup(
        &mut self,
        now_ms: i64,
    ) -> Result<(), SnapshotBuildProgressError> {
        if is_terminal(self.progress.stage) {
            return Ok(());
        }
        if self.progress.stage != SnapshotBuildStage::CleaningUp {
            self.begin_cleanup(
                SnapshotBuildFailureCode::ProcessInterrupted,
                SnapshotBuildDisposition::Retry,
                now_ms,
            )?;
        }
        self.finish_cleanup(now_ms)
    }

    pub fn transition(
        &mut self,
        next: SnapshotBuildStage,
        now_ms: i64,
    ) -> Result<(), SnapshotBuildProgressError> {
        validate_timestamp_order(self.progress.last_progress_at_unix_millis, now_ms)?;
        if !valid_transition(self.progress.stage, next) {
            return Err(SnapshotBuildProgressError::InvalidTransition {
                current: self.progress.stage,
                next,
            });
        }
        self.progress.stage = next;
        self.progress.last_progress_at_unix_millis = now_ms;
        if next == SnapshotBuildStage::Copying {
            self.copy_persisted = Some((now_ms, self.progress.decoded_value_bytes));
        }
        if next == SnapshotBuildStage::Published {
            self.progress.ended_at_unix_millis = Some(now_ms);
        }
        self.bump_and_persist()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_copy_progress(
        &mut self,
        tables_completed: u64,
        rows_copied: u64,
        decoded_value_bytes: u64,
        committed_batches: u64,
        active_workers: u64,
        queued_tables: u64,
        now_ms: i64,
    ) -> Result<(), SnapshotBuildProgressError> {
        if self.progress.stage != SnapshotBuildStage::Copying
            || tables_completed < self.progress.tables_completed
            || tables_completed > self.progress.tables_total
            || rows_copied < self.progress.rows_copied
            || decoded_value_bytes < self.progress.decoded_value_bytes
            || committed_batches < self.progress.committed_batches
            // A worker remains active until its task exit is observed, so the
            // final table-completion callback may legitimately report one
            // active worker with zero unfinished tables.
            || active_workers > self.progress.tables_total
            || tables_completed
                .checked_add(queued_tables)
                .is_none_or(|accounted| accounted > self.progress.tables_total)
        {
            return Err(SnapshotBuildProgressError::InvalidCopyProgress);
        }
        validate_timestamp_order(self.progress.last_progress_at_unix_millis, now_ms)?;
        let due = self.copy_persisted.is_none_or(|(at, bytes)| {
            now_ms.saturating_sub(at) >= COPY_PROGRESS_PERSIST_INTERVAL_MS
                || decoded_value_bytes.saturating_sub(bytes) >= COPY_PROGRESS_PERSIST_BYTES
        });
        self.progress.tables_completed = tables_completed;
        self.progress.rows_copied = rows_copied;
        self.progress.decoded_value_bytes = decoded_value_bytes;
        self.progress.committed_batches = committed_batches;
        self.progress.active_copy_workers = active_workers;
        self.progress.peak_active_copy_workers =
            self.progress.peak_active_copy_workers.max(active_workers);
        self.progress.queued_copy_tables = queued_tables;
        self.progress.last_progress_at_unix_millis = now_ms;
        if !due {
            return Ok(());
        }
        self.copy_persisted = Some((now_ms, decoded_value_bytes));
        self.bump_and_persist()
    }

    pub(crate) fn record_barrier_progress(
        &mut self,
        minimum_sequence: u64,
        head_sequence: u64,
        records_scanned: u64,
        now_ms: i64,
    ) -> Result<(), SnapshotBuildProgressError> {
        if self.progress.stage != SnapshotBuildStage::WaitingForPersistentBarrier
            || minimum_sequence == 0
            || head_sequence.saturating_add(1) < minimum_sequence
            || self
                .progress
                .barrier_minimum_sequence
                .is_some_and(|prior| prior != minimum_sequence)
            || self
                .progress
                .barrier_head_sequence
                .is_some_and(|prior| head_sequence < prior)
            || records_scanned < self.progress.barrier_records_scanned
        {
            return Err(SnapshotBuildProgressError::InvalidBarrierProgress);
        }
        validate_timestamp_order(self.progress.last_progress_at_unix_millis, now_ms)?;
        self.progress.barrier_minimum_sequence = Some(minimum_sequence);
        self.progress.barrier_head_sequence = Some(head_sequence);
        self.progress.barrier_records_scanned = records_scanned;
        self.progress.last_progress_at_unix_millis = now_ms;
        self.bump_and_persist()
    }

    pub(crate) fn record_pressure(
        &mut self,
        sample: &SnapshotPressureSample,
        evaluation: &SnapshotPressureEvaluation,
        now_ms: i64,
    ) -> Result<(), SnapshotBuildProgressError> {
        if is_terminal(self.progress.stage) {
            return Err(SnapshotBuildProgressError::Terminal);
        }
        validate_timestamp_order(self.progress.last_progress_at_unix_millis, now_ms)?;
        self.progress.retained_wal_bytes = Some(sample.retained_wal_bytes);
        self.progress.catalog_xid_age = Some(sample.catalog_xid_age);
        self.progress.oldest_copy_transaction_millis =
            sample.oldest_copy_or_export_transaction_millis;
        self.progress.pressure_level = Some(evaluation.level);
        self.progress
            .pressure_reasons
            .clone_from(&evaluation.reasons);
        self.progress.last_progress_at_unix_millis = now_ms;
        self.bump_and_persist()
    }

    pub(crate) fn begin_cleanup(
        &mut self,
        code: SnapshotBuildFailureCode,
        disposition: SnapshotBuildDisposition,
        now_ms: i64,
    ) -> Result<(), SnapshotBuildProgressError> {
        if is_terminal(self.progress.stage) || self.progress.stage == SnapshotBuildStage::CleaningUp
        {
            return Err(SnapshotBuildProgressError::Terminal);
        }
        validate_timestamp_order(self.progress.last_progress_at_unix_millis, now_ms)?;
        self.progress.stage = SnapshotBuildStage::CleaningUp;
        self.progress.failure_code = Some(code);
        self.progress.disposition = Some(disposition);
        self.progress.last_progress_at_unix_millis = now_ms;
        if disposition == SnapshotBuildDisposition::Cancelled {
            self.progress.cancellation_requested_at_unix_millis = Some(now_ms);
        }
        self.bump_and_persist()
    }

    pub(crate) fn finish_cleanup(&mut self, now_ms: i64) -> Result<(), SnapshotBuildProgressError> {
        if self.progress.stage != SnapshotBuildStage::CleaningUp {
            return Err(SnapshotBuildProgressError::InvalidTransition {
                current: self.progress.stage,
                next: SnapshotBuildStage::Failed,
            });
        }
        validate_timestamp_order(self.progress.last_progress_at_unix_millis, now_ms)?;
        let next = if self.progress.disposition == Some(SnapshotBuildDisposition::Cancelled) {
            SnapshotBuildStage::Cancelled
        } else {
            SnapshotBuildStage::Failed
        };
        self.progress.stage = next;
        self.progress.last_progress_at_unix_millis = now_ms;
        self.progress.cleanup_completed_at_unix_millis = Some(now_ms);
        self.progress.ended_at_unix_millis = Some(now_ms);
        self.progress.active_copy_workers = 0;
        self.bump_and_persist()
    }

    fn bump_and_persist(&mut self) -> Result<(), SnapshotBuildProgressError> {
        self.progress.revision = self
            .progress
            .revision
            .checked_add(1)
            .ok_or(SnapshotBuildProgressError::RevisionExhausted)?;
        self.persist()
    }

    fn persist(&mut self) -> Result<(), SnapshotBuildProgressError> {
        let sync_directory = syncs_directory(&self.progress);
        validate_progress(&self.progress)?;
        let bytes = serde_json::to_vec(&self.progress)?;
        let temp = self.root.join(format!(
            ".{}.{}.tmp",
            self.progress.build_id, self.progress.revision
        ));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        if let Err(error) = (|| -> io::Result<()> {
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&temp, &self.path)?;
            if sync_directory {
                File::open(&self.root)?.sync_all()?;
            }
            Ok(())
        })() {
            let _ = fs::remove_file(&temp);
            return Err(error.into());
        }
        Ok(())
    }
}

fn validate_private_work_dir(
    private_work_dir: &Path,
    build_id: &SnapshotBuildId,
) -> Result<String, SnapshotBuildProgressError> {
    if !private_work_dir.is_absolute()
        || private_work_dir.file_name().and_then(|name| name.to_str()) != Some(build_id.as_str())
    {
        return Err(SnapshotBuildProgressError::UnsafePrivateWorkDir(
            private_work_dir.to_path_buf(),
        ));
    }
    private_work_dir.to_str().map(str::to_owned).ok_or_else(|| {
        SnapshotBuildProgressError::UnsafePrivateWorkDir(private_work_dir.to_path_buf())
    })
}

fn cleanup_orphan_temp_files(
    root: &Path,
    only_build: Option<&SnapshotBuildId>,
) -> Result<(), SnapshotBuildProgressError> {
    let mut removed = false;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(SnapshotBuildProgressError::UnsafeStatus(entry.path()));
        };
        let Some(body) = name
            .strip_prefix('.')
            .and_then(|name| name.strip_suffix(".tmp"))
        else {
            continue;
        };
        let Some((build, revision)) = body.rsplit_once('.') else {
            continue;
        };
        let parsed = SnapshotBuildId::new(build.to_owned())
            .map_err(|_| SnapshotBuildProgressError::UnsafeStatus(entry.path()))?;
        revision
            .parse::<u64>()
            .map_err(|_| SnapshotBuildProgressError::UnsafeStatus(entry.path()))?;
        if only_build.is_some_and(|expected| expected != &parsed) {
            continue;
        }
        let file_type = entry.file_type()?;
        if file_type.is_symlink() || !file_type.is_file() {
            return Err(SnapshotBuildProgressError::UnsafeStatus(entry.path()));
        }
        fs::remove_file(entry.path())?;
        removed = true;
    }
    if removed {
        File::open(root)?.sync_all()?;
    }
    Ok(())
}

fn prepare_root(root: &Path) -> Result<PathBuf, SnapshotBuildProgressError> {
    if let Ok(metadata) = fs::symlink_metadata(root) {
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(SnapshotBuildProgressError::UnsafeRoot(root.to_path_buf()));
        }
    } else {
        fs::create_dir_all(root)?;
    }
    Ok(root.to_path_buf())
}

fn status_path(root: &Path, build_id: &SnapshotBuildId) -> PathBuf {
    root.join(format!("{}.json", build_id.as_str()))
}

fn validate_progress(progress: &SnapshotBuildProgress) -> Result<(), SnapshotBuildProgressError> {
    if progress.format_version != STATUS_FORMAT_VERSION
        || progress.build_id.is_empty()
        || progress.owner_id.is_empty()
        || progress.artifact_id.is_empty()
        || !Path::new(&progress.private_work_dir).is_absolute()
        || Path::new(&progress.private_work_dir)
            .file_name()
            .and_then(|name| name.to_str())
            != Some(progress.build_id.as_str())
        || SnapshotBuildId::new(progress.build_id.clone()).is_err()
        || SnapshotBuildOwnerId::new(progress.owner_id.clone()).is_err()
        || ArtifactId::new(progress.artifact_id.clone()).is_err()
        || progress.started_at_unix_millis < 0
        || progress.last_progress_at_unix_millis < progress.started_at_unix_millis
        || progress.tables_completed > progress.tables_total
        || progress.active_copy_workers > progress.tables_total
        || progress.peak_active_copy_workers > progress.tables_total
        || progress.active_copy_workers > progress.peak_active_copy_workers
        || progress.queued_copy_tables > progress.tables_total
        || progress
            .ended_at_unix_millis
            .is_some_and(|ended| ended < progress.last_progress_at_unix_millis)
        || (is_terminal(progress.stage) != progress.ended_at_unix_millis.is_some())
        || (matches!(
            progress.stage,
            SnapshotBuildStage::Failed | SnapshotBuildStage::Cancelled
        ) != progress.cleanup_completed_at_unix_millis.is_some())
    {
        return Err(SnapshotBuildProgressError::Corrupt);
    }
    Ok(())
}

fn validate_timestamp(now_ms: i64) -> Result<(), SnapshotBuildProgressError> {
    if now_ms < 0 {
        Err(SnapshotBuildProgressError::InvalidTimestamp)
    } else {
        Ok(())
    }
}

fn validate_timestamp_order(prior: i64, now_ms: i64) -> Result<(), SnapshotBuildProgressError> {
    validate_timestamp(now_ms)?;
    if now_ms < prior {
        Err(SnapshotBuildProgressError::InvalidTimestamp)
    } else {
        Ok(())
    }
}

/// Whether persisting `progress` also syncs the journal's directory: at the
/// journal's creation, revision 0, and once it is cleaning up or terminal.
/// Recovery handles every other stage alike, and a power loss that undoes a
/// later rename leaves the complete earlier revision, since each file is
/// synced before its rename. Skipping the sync saves a full flush per persist.
const fn syncs_directory(progress: &SnapshotBuildProgress) -> bool {
    progress.revision == 0
        || is_terminal(progress.stage)
        || matches!(progress.stage, SnapshotBuildStage::CleaningUp)
}

const fn is_terminal(stage: SnapshotBuildStage) -> bool {
    matches!(
        stage,
        SnapshotBuildStage::Published | SnapshotBuildStage::Failed | SnapshotBuildStage::Cancelled
    )
}

/// A build rolls forward to the cut, then copies, and does both before the
/// barrier wait.
fn valid_transition(current: SnapshotBuildStage, next: SnapshotBuildStage) -> bool {
    matches!(
        (current, next),
        (
            SnapshotBuildStage::Admitted,
            SnapshotBuildStage::RetentionLeased
        ) | (
            SnapshotBuildStage::RetentionLeased,
            SnapshotBuildStage::Exported
        ) | (
            SnapshotBuildStage::Exported,
            SnapshotBuildStage::RollingForward
        ) | (
            SnapshotBuildStage::RollingForward,
            SnapshotBuildStage::Copying
        ) | (
            SnapshotBuildStage::Copying,
            SnapshotBuildStage::WaitingForPersistentBarrier
        ) | (
            SnapshotBuildStage::WaitingForPersistentBarrier,
            SnapshotBuildStage::Finalizing
        ) | (
            SnapshotBuildStage::Finalizing,
            SnapshotBuildStage::Uploading
        ) | (
            SnapshotBuildStage::Uploading,
            SnapshotBuildStage::Publishing
        ) | (
            SnapshotBuildStage::Publishing,
            SnapshotBuildStage::Published
        )
    )
}

#[derive(Debug, Error)]
pub enum SnapshotBuildProgressError {
    #[error("snapshot build status already exists")]
    AlreadyExists,
    #[error("snapshot build status root is unsafe: {0}")]
    UnsafeRoot(PathBuf),
    #[error("snapshot build status entry is unsafe: {0}")]
    UnsafeStatus(PathBuf),
    #[error("snapshot build private workspace identity is unsafe: {0}")]
    UnsafePrivateWorkDir(PathBuf),
    #[error("snapshot build status has an invalid or regressing timestamp")]
    InvalidTimestamp,
    #[error("snapshot build status identity does not match its requested build")]
    IdentityMismatch,
    #[error("snapshot build status cannot be removed before unpublished cleanup is complete")]
    RemovalNotAllowed,
    #[error("snapshot build status is corrupt or uses an unsupported format")]
    Corrupt,
    #[error("snapshot build status revision space is exhausted")]
    RevisionExhausted,
    #[error("snapshot build is already terminal or cleaning up")]
    Terminal,
    #[error("snapshot build transition from {current:?} to {next:?} is invalid")]
    InvalidTransition {
        current: SnapshotBuildStage,
        next: SnapshotBuildStage,
    },
    #[error("snapshot COPY progress is non-monotonic or inconsistent with table admission")]
    InvalidCopyProgress,
    #[error("snapshot persistent-barrier progress is non-monotonic or invalid")]
    InvalidBarrierProgress,
    #[error("snapshot build status I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("snapshot private work directory cleanup failed before publication: {0}")]
    PrivateWorkDirCleanup(#[source] io::Error),
    #[error("snapshot build status serialization failed: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn journal() -> (tempfile::TempDir, SnapshotBuildJournal) {
        let root = tempfile::tempdir().unwrap();
        let build = SnapshotBuildId::new("build-progress-1").unwrap();
        let owner = SnapshotBuildOwnerId::new("worker-progress-1").unwrap();
        let artifact = ArtifactId::new("artifact-progress-1").unwrap();
        let journal = SnapshotBuildJournal::create(
            root.path(),
            &build,
            &owner,
            &artifact,
            &root.path().join(build.as_str()),
            SourceEpoch::new("epoch-progress-1").unwrap(),
            2,
            1_000,
        )
        .unwrap();
        (root, journal)
    }

    #[test]
    fn a_build_rolls_forward_then_copies_once_and_in_no_other_order() {
        let invalid = |result: Result<(), SnapshotBuildProgressError>| {
            matches!(
                result,
                Err(SnapshotBuildProgressError::InvalidTransition { .. })
            )
        };

        let (_root, mut journal) = journal();
        journal
            .transition(SnapshotBuildStage::RetentionLeased, 1_001)
            .unwrap();
        journal
            .transition(SnapshotBuildStage::Exported, 1_002)
            .unwrap();
        assert!(invalid(
            journal.transition(SnapshotBuildStage::Copying, 1_003)
        ));
        journal
            .transition(SnapshotBuildStage::RollingForward, 1_003)
            .unwrap();
        assert!(invalid(journal.transition(
            SnapshotBuildStage::WaitingForPersistentBarrier,
            1_004
        )));
        journal
            .transition(SnapshotBuildStage::Copying, 1_004)
            .unwrap();
        journal
            .record_copy_progress(2, 20, 160, 2, 0, 0, 1_005)
            .unwrap();
        assert!(invalid(
            journal.transition(SnapshotBuildStage::RollingForward, 1_006)
        ));
        journal
            .transition(SnapshotBuildStage::WaitingForPersistentBarrier, 1_006)
            .unwrap();
    }

    #[test]
    fn persists_monotonic_success_lifecycle_and_reopens_exact_state() {
        let (root, mut journal) = journal();
        journal
            .transition(SnapshotBuildStage::RetentionLeased, 1_001)
            .unwrap();
        journal
            .transition(SnapshotBuildStage::Exported, 1_002)
            .unwrap();
        journal
            .transition(SnapshotBuildStage::RollingForward, 1_003)
            .unwrap();
        journal
            .transition(SnapshotBuildStage::Copying, 1_004)
            .unwrap();
        journal
            .record_copy_progress(0, 10, 80, 1, 2, 0, 1_005)
            .unwrap();
        journal
            .record_copy_progress(2, 20, 160, 2, 0, 0, 1_006)
            .unwrap();
        journal
            .transition(SnapshotBuildStage::WaitingForPersistentBarrier, 1_007)
            .unwrap();
        journal.record_barrier_progress(1, 2, 2, 1_008).unwrap();
        for (stage, time) in [
            (SnapshotBuildStage::Finalizing, 1_009),
            (SnapshotBuildStage::Uploading, 1_010),
            (SnapshotBuildStage::Publishing, 1_011),
            (SnapshotBuildStage::Published, 1_012),
        ] {
            journal.transition(stage, time).unwrap();
        }
        let build = SnapshotBuildId::new("build-progress-1").unwrap();
        let reopened = SnapshotBuildJournal::reopen(root.path(), &build).unwrap();
        assert_eq!(reopened.progress(), journal.progress());
        assert_eq!(reopened.progress().stage, SnapshotBuildStage::Published);
        assert_eq!(reopened.progress().ended_at_unix_millis, Some(1_012));
    }

    #[test]
    fn a_published_build_syncs_the_journal_directory_only_at_creation_and_publication() {
        let (_root, mut journal) = journal();
        let mut synced = vec![syncs_directory(journal.progress())];
        for (stage, time) in [
            (SnapshotBuildStage::RetentionLeased, 1_001),
            (SnapshotBuildStage::Exported, 1_002),
            (SnapshotBuildStage::RollingForward, 1_003),
            (SnapshotBuildStage::Copying, 1_004),
        ] {
            journal.transition(stage, time).unwrap();
            synced.push(syncs_directory(journal.progress()));
        }
        journal
            .record_copy_progress(2, 20, 160, 2, 0, 0, 2_005)
            .unwrap();
        synced.push(syncs_directory(journal.progress()));
        journal
            .transition(SnapshotBuildStage::WaitingForPersistentBarrier, 2_006)
            .unwrap();
        synced.push(syncs_directory(journal.progress()));
        journal.record_barrier_progress(1, 2, 2, 2_007).unwrap();
        synced.push(syncs_directory(journal.progress()));
        for (stage, time) in [
            (SnapshotBuildStage::Finalizing, 2_008),
            (SnapshotBuildStage::Uploading, 2_009),
            (SnapshotBuildStage::Publishing, 2_010),
            (SnapshotBuildStage::Published, 2_011),
        ] {
            journal.transition(stage, time).unwrap();
            synced.push(syncs_directory(journal.progress()));
        }
        assert_eq!(
            synced,
            [true, false, false, false, false, false, false, false, false, false, false, true]
        );
    }

    #[test]
    fn a_failed_build_syncs_the_journal_directory_at_cleanup_and_failure() {
        let (_root, mut journal) = journal();
        journal
            .transition(SnapshotBuildStage::RetentionLeased, 1_001)
            .unwrap();
        assert!(!syncs_directory(journal.progress()));
        journal
            .begin_cleanup(
                SnapshotBuildFailureCode::CopyWorkerFailed,
                SnapshotBuildDisposition::Retry,
                1_002,
            )
            .unwrap();
        assert!(syncs_directory(journal.progress()));
        journal.finish_cleanup(1_003).unwrap();
        assert_eq!(journal.progress().stage, SnapshotBuildStage::Failed);
        assert!(syncs_directory(journal.progress()));
    }

    fn persisted(root: &Path) -> SnapshotBuildProgress {
        SnapshotBuildJournal::reopen(root, &SnapshotBuildId::new("build-progress-1").unwrap())
            .unwrap()
            .progress()
            .clone()
    }

    fn copying(journal: &mut SnapshotBuildJournal) {
        journal
            .transition(SnapshotBuildStage::RetentionLeased, 1_001)
            .unwrap();
        journal
            .transition(SnapshotBuildStage::Exported, 1_002)
            .unwrap();
        journal
            .transition(SnapshotBuildStage::RollingForward, 1_003)
            .unwrap();
        journal
            .transition(SnapshotBuildStage::Copying, 1_004)
            .unwrap();
    }

    #[test]
    fn copy_progress_persists_only_by_interval_or_bytes() {
        let (root, mut journal) = journal();
        copying(&mut journal);
        let mib = 1024 * 1024;
        let revision = persisted(root.path()).revision;

        // Held back: the transition into Copying, at 1_004, was persisted, and
        // neither a first report nor a table completion is due by itself.
        journal
            .record_copy_progress(0, 10, mib, 1, 2, 0, 1_010)
            .unwrap();
        journal
            .record_copy_progress(1, 20, 2 * mib, 2, 1, 0, 1_500)
            .unwrap();
        assert_eq!(journal.progress().tables_completed, 1);
        let on_disk = persisted(root.path());
        assert_eq!(on_disk.revision, revision);
        assert_eq!(on_disk.stage, SnapshotBuildStage::Copying);
        assert_eq!(on_disk.rows_copied, 0);
        assert_eq!(on_disk.tables_completed, 0);
        // 64 MiB since the copy began.
        journal
            .record_copy_progress(1, 30, 64 * mib, 3, 1, 0, 1_600)
            .unwrap();
        assert_eq!(persisted(root.path()).rows_copied, 30);
        journal
            .record_copy_progress(1, 40, 65 * mib, 4, 1, 0, 2_599)
            .unwrap();
        assert_eq!(persisted(root.path()).rows_copied, 30);
        // A second since the last persist.
        journal
            .record_copy_progress(1, 50, 66 * mib, 5, 1, 0, 2_600)
            .unwrap();
        assert_eq!(persisted(root.path()).rows_copied, 50);
        journal
            .record_copy_progress(2, 60, 67 * mib, 6, 1, 0, 2_601)
            .unwrap();
        assert_eq!(persisted(root.path()).tables_completed, 1);
        // The next stage boundary carries what was held back.
        journal
            .transition(SnapshotBuildStage::WaitingForPersistentBarrier, 2_602)
            .unwrap();
        let reopened = persisted(root.path());
        assert_eq!(&reopened, journal.progress());
        assert_eq!(reopened.tables_completed, 2);
        assert_eq!(reopened.rows_copied, 60);
        assert_eq!(reopened.committed_batches, 6);
    }

    #[test]
    fn a_copy_under_a_second_persists_only_at_its_stage_boundaries() {
        let (root, mut journal) = journal();
        copying(&mut journal);
        let revision = persisted(root.path()).revision;
        journal
            .record_copy_progress(1, 10, 80, 1, 1, 1, 1_010)
            .unwrap();
        journal
            .record_copy_progress(2, 20, 160, 2, 1, 0, 1_011)
            .unwrap();
        assert_eq!(persisted(root.path()).revision, revision);
        journal
            .transition(SnapshotBuildStage::WaitingForPersistentBarrier, 1_012)
            .unwrap();
        let reopened = persisted(root.path());
        assert_eq!(reopened.revision, revision + 1);
        assert_eq!(reopened.tables_completed, 2);
        assert_eq!(reopened.rows_copied, 20);
    }

    #[test]
    fn a_build_interrupted_mid_copy_recovers_from_its_last_persisted_progress() {
        let (root, mut journal) = journal();
        copying(&mut journal);
        journal
            .record_copy_progress(0, 10, 80, 1, 2, 0, 2_004)
            .unwrap();
        journal
            .record_copy_progress(0, 20, 160, 2, 2, 0, 2_011)
            .unwrap();
        // The process dies here: the held-back report never reaches the file.
        drop(journal);

        let build = SnapshotBuildId::new("build-progress-1").unwrap();
        assert_eq!(
            SnapshotBuildJournal::scan(root.path()).unwrap(),
            vec![build.clone()]
        );
        let mut recovered = SnapshotBuildJournal::reopen(root.path(), &build).unwrap();
        assert_eq!(recovered.progress().stage, SnapshotBuildStage::Copying);
        assert_eq!(recovered.progress().rows_copied, 10);
        recovered.reconcile_interrupted_cleanup(2_020).unwrap();
        let terminal = persisted(root.path());
        assert_eq!(terminal.stage, SnapshotBuildStage::Failed);
        assert_eq!(
            terminal.failure_code,
            Some(SnapshotBuildFailureCode::ProcessInterrupted)
        );
        assert_eq!(terminal.disposition, Some(SnapshotBuildDisposition::Retry));
        assert_eq!(terminal.rows_copied, 10);

        // The same build identity is released for a fresh copy.
        SnapshotBuildJournal::remove_recovered_unpublished(
            root.path(),
            &build,
            &ArtifactId::new("artifact-progress-1").unwrap(),
            &root.path().join(build.as_str()),
            &SourceEpoch::new("epoch-progress-1").unwrap(),
        )
        .unwrap();
        let mut retried = SnapshotBuildJournal::create(
            root.path(),
            &build,
            &SnapshotBuildOwnerId::new("worker-progress-1").unwrap(),
            &ArtifactId::new("artifact-progress-1").unwrap(),
            &root.path().join(build.as_str()),
            SourceEpoch::new("epoch-progress-1").unwrap(),
            2,
            2_030,
        )
        .unwrap();
        assert_eq!(retried.progress().rows_copied, 0);
        retried
            .transition(SnapshotBuildStage::RetentionLeased, 2_031)
            .unwrap();
        assert_eq!(
            persisted(root.path()).stage,
            SnapshotBuildStage::RetentionLeased
        );
    }

    #[test]
    fn cleanup_records_only_typed_sanitized_failure_and_terminal_boundary() {
        let (root, mut journal) = journal();
        journal
            .transition(SnapshotBuildStage::RetentionLeased, 1_001)
            .unwrap();
        journal
            .begin_cleanup(
                SnapshotBuildFailureCode::LeaseLost,
                SnapshotBuildDisposition::Retry,
                1_002,
            )
            .unwrap();
        journal.finish_cleanup(1_003).unwrap();
        let build = SnapshotBuildId::new("build-progress-1").unwrap();
        let reopened = SnapshotBuildJournal::reopen(root.path(), &build).unwrap();
        assert_eq!(reopened.progress().stage, SnapshotBuildStage::Failed);
        assert_eq!(
            reopened.progress().failure_code,
            Some(SnapshotBuildFailureCode::LeaseLost)
        );
        assert_eq!(
            reopened.progress().cleanup_completed_at_unix_millis,
            Some(1_003)
        );
        let serialized =
            fs::read_to_string(root.path().join(format!("{}.json", build.as_str()))).unwrap();
        assert!(!serialized.contains("password"));
        assert!(!serialized.contains("postgresql://"));
        assert!(!serialized.contains("reason"));
    }

    #[test]
    fn startup_recovery_reconciles_catalog_commit_and_interrupted_cleanup() {
        let (root, mut published) = journal();
        published
            .transition(SnapshotBuildStage::RetentionLeased, 1_001)
            .unwrap();
        published.reconcile_published(1_002).unwrap();
        assert_eq!(published.progress().stage, SnapshotBuildStage::Published);
        assert_eq!(published.progress().tables_completed, 0);
        assert!(published.progress().failure_code.is_none());
        assert!(published.progress().disposition.is_none());

        let second_root = tempfile::tempdir().unwrap();
        let build = SnapshotBuildId::new("build-progress-interrupted").unwrap();
        let owner = SnapshotBuildOwnerId::new("worker-progress-interrupted").unwrap();
        let artifact = ArtifactId::new("artifact-progress-interrupted").unwrap();
        let mut interrupted = SnapshotBuildJournal::create(
            second_root.path(),
            &build,
            &owner,
            &artifact,
            &second_root.path().join(build.as_str()),
            SourceEpoch::new("epoch-progress-interrupted").unwrap(),
            1,
            2_000,
        )
        .unwrap();
        interrupted.reconcile_interrupted_cleanup(2_001).unwrap();
        assert_eq!(interrupted.progress().stage, SnapshotBuildStage::Failed);
        assert_eq!(
            interrupted.progress().failure_code,
            Some(SnapshotBuildFailureCode::ProcessInterrupted)
        );
        assert_eq!(
            interrupted.progress().disposition,
            Some(SnapshotBuildDisposition::Retry)
        );
        interrupted.reconcile_published(2_002).unwrap();
        assert_eq!(interrupted.progress().stage, SnapshotBuildStage::Published);
        assert!(interrupted.progress().failure_code.is_none());
        assert!(interrupted
            .progress()
            .cleanup_completed_at_unix_millis
            .is_none());

        let reopened = SnapshotBuildJournal::reopen(
            root.path(),
            &SnapshotBuildId::new("build-progress-1").unwrap(),
        )
        .unwrap();
        assert_eq!(reopened.progress().stage, SnapshotBuildStage::Published);
    }

    #[test]
    fn cleaned_unpublished_recovery_can_release_its_identity_for_retry() {
        let (root, mut journal) = journal();
        journal.reconcile_interrupted_cleanup(1_001).unwrap();
        let build = SnapshotBuildId::new("build-progress-1").unwrap();
        let artifact = ArtifactId::new("artifact-progress-1").unwrap();
        let epoch = SourceEpoch::new("epoch-progress-1").unwrap();
        SnapshotBuildJournal::remove_recovered_unpublished(
            root.path(),
            &build,
            &artifact,
            &root.path().join(build.as_str()),
            &epoch,
        )
        .unwrap();
        SnapshotBuildJournal::create(
            root.path(),
            &build,
            &SnapshotBuildOwnerId::new("worker-progress-retry").unwrap(),
            &artifact,
            &root.path().join(build.as_str()),
            epoch,
            2,
            1_002,
        )
        .unwrap();
    }

    #[test]
    fn recovery_journal_removal_rejects_incomplete_published_or_wrong_identity() {
        let (root, mut journal) = journal();
        let build = SnapshotBuildId::new("build-progress-1").unwrap();
        let artifact = ArtifactId::new("artifact-progress-1").unwrap();
        let epoch = SourceEpoch::new("epoch-progress-1").unwrap();
        let remove = |artifact: &ArtifactId| {
            SnapshotBuildJournal::remove_recovered_unpublished(
                root.path(),
                &build,
                artifact,
                &root.path().join(build.as_str()),
                &epoch,
            )
        };
        assert!(matches!(
            remove(&artifact),
            Err(SnapshotBuildProgressError::RemovalNotAllowed)
        ));
        assert!(matches!(
            remove(&ArtifactId::new("artifact-wrong").unwrap()),
            Err(SnapshotBuildProgressError::IdentityMismatch)
        ));
        journal.reconcile_published(1_001).unwrap();
        assert!(matches!(
            remove(&artifact),
            Err(SnapshotBuildProgressError::RemovalNotAllowed)
        ));
    }

    #[test]
    fn only_a_published_journal_of_its_own_artifact_is_removed_as_published() {
        let (root, mut journal) = journal();
        let build = SnapshotBuildId::new("build-progress-1").unwrap();
        let artifact = ArtifactId::new("artifact-progress-1").unwrap();
        assert!(matches!(
            SnapshotBuildJournal::remove_published(root.path(), &build, &artifact),
            Err(SnapshotBuildProgressError::RemovalNotAllowed)
        ));
        journal.reconcile_published(1_001).unwrap();
        assert!(matches!(
            SnapshotBuildJournal::remove_published(
                root.path(),
                &build,
                &ArtifactId::new("artifact-wrong").unwrap()
            ),
            Err(SnapshotBuildProgressError::IdentityMismatch)
        ));
        SnapshotBuildJournal::remove_published(root.path(), &build, &artifact).unwrap();
        assert!(SnapshotBuildJournal::scan(root.path()).unwrap().is_empty());
    }

    #[test]
    fn an_ended_build_alone_gives_up_its_journal() {
        let (root, mut journal) = journal();
        let build = SnapshotBuildId::new("build-progress-1").unwrap();
        assert!(matches!(
            SnapshotBuildJournal::remove_ended(root.path(), &build),
            Err(SnapshotBuildProgressError::RemovalNotAllowed)
        ));
        journal.reconcile_published(1_001).unwrap();
        SnapshotBuildJournal::remove_ended(root.path(), &build).unwrap();
        assert!(SnapshotBuildJournal::scan(root.path()).unwrap().is_empty());

        let (root, mut journal) = self::journal();
        journal.reconcile_interrupted_cleanup(1_001).unwrap();
        SnapshotBuildJournal::remove_ended(root.path(), &build).unwrap();
        assert!(SnapshotBuildJournal::scan(root.path()).unwrap().is_empty());
    }

    #[test]
    fn journal_scan_validates_filename_identity_and_rejects_symlinks() {
        let (root, _journal) = journal();
        let scanned = SnapshotBuildJournal::scan(root.path()).unwrap();
        assert_eq!(scanned.len(), 1);
        assert_eq!(scanned[0].as_str(), "build-progress-1");

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(
                root.path().join("build-progress-1.json"),
                root.path().join("unsafe-link.json"),
            )
            .unwrap();
            assert!(matches!(
                SnapshotBuildJournal::scan(root.path()),
                Err(SnapshotBuildProgressError::UnsafeStatus(_))
            ));
        }
    }

    #[test]
    fn reopen_removes_crash_left_next_revision_before_reconciliation() {
        let (root, journal) = journal();
        let build = SnapshotBuildId::new("build-progress-1").unwrap();
        let next_revision = journal.progress().revision + 1;
        let orphan = root
            .path()
            .join(format!(".{}.{next_revision}.tmp", build.as_str()));
        fs::write(&orphan, b"durable but not renamed").unwrap();
        File::open(root.path()).unwrap().sync_all().unwrap();

        let mut reopened = SnapshotBuildJournal::reopen(root.path(), &build).unwrap();
        assert!(!orphan.exists());
        reopened
            .transition(SnapshotBuildStage::RetentionLeased, 1_001)
            .unwrap();
        assert_eq!(reopened.progress().revision, next_revision);
    }

    #[test]
    fn persists_sanitized_pressure_warning_facts() {
        let (root, mut journal) = journal();
        journal
            .transition(SnapshotBuildStage::RetentionLeased, 1_001)
            .unwrap();
        let sample = SnapshotPressureSample {
            retained_wal_bytes: 1_024,
            catalog_xid_age: 42,
            oldest_copy_or_export_transaction_millis: Some(250),
            wal_status: snapshot_coordinator::SnapshotWalStatus::Extended,
            safe_wal_size_bytes: Some(8_192),
            exporter_backend_pid: 10,
            copy_backend_count: 2,
        };
        let evaluation = SnapshotPressureEvaluation {
            level: SnapshotPressureLevel::Warning,
            reasons: vec![SnapshotPressureReason::WalStatus {
                level: SnapshotPressureLevel::Warning,
                status: snapshot_coordinator::SnapshotWalStatus::Extended,
            }],
        };
        journal
            .record_pressure(&sample, &evaluation, 1_002)
            .unwrap();

        let reopened = SnapshotBuildJournal::reopen(
            root.path(),
            &SnapshotBuildId::new("build-progress-1").unwrap(),
        )
        .unwrap();
        assert_eq!(reopened.progress().retained_wal_bytes, Some(1_024));
        assert_eq!(
            reopened.progress().pressure_level,
            Some(SnapshotPressureLevel::Warning)
        );
        assert_eq!(reopened.progress().pressure_reasons, evaluation.reasons);
    }

    #[test]
    fn rejects_skipped_stage_and_inconsistent_copy_accounting() {
        let (_root, mut journal) = journal();
        assert!(matches!(
            journal.transition(SnapshotBuildStage::Copying, 1_001),
            Err(SnapshotBuildProgressError::InvalidTransition { .. })
        ));
        copying(&mut journal);
        assert!(matches!(
            journal.record_copy_progress(2, 1, 1, 1, 1, 1, 1_005),
            Err(SnapshotBuildProgressError::InvalidCopyProgress)
        ));
    }
}
