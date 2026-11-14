use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use artifact_catalog::{ArtifactState, BuildCleanupLease, CatalogError, SqliteArtifactCatalog};
use blob_store::{BlobStore, ManagedBlobStore};
use canonical_log::{SnapshotBuildId, SnapshotBuildOwnerId, SnapshotBuildRetention};
use pgoutput_decoder::DecoderLimits;
use snapshot_builder::{
    BoundedCopyLimits, DirectBuildSession, EncoderLimits, SnapshotBuildPlan,
    SnapshotBuildStateError, SnapshotCopyWorkload, SnapshotRollForwardError,
    SnapshotRollForwardStats,
};
use snapshot_coordinator::{
    emit_cut_marker, import_snapshot, CoordinatorError, CutIdentity, ImportBarrier,
    RollForwardError, SnapshotCoordinator, SnapshotCopyReservation, SnapshotCopyWorker,
    SnapshotDdlGuard, SnapshotPressureError, SnapshotPressurePolicy, SnapshotPressureTarget,
    SnapshotStreamConfig,
};
use source_control::{ActivatedSource, SchemaRegistry};
use sync_types::SourceEpoch;
use thiserror::Error;
use tokio_postgres::Client;
use tokio_util::sync::CancellationToken;

use crate::{
    publish_direct_with_proven_cut_before_publish_controlled, wait_for_persistent_barrier,
    BarrierWaitError, BarrierWaitRequest, CanonicalBarrierRead, CanonicalHeadSignal,
    SnapshotBuildDisposition, SnapshotBuildFailureCode, SnapshotBuildJournal,
    SnapshotBuildLeaseGuard, SnapshotBuildLeaseGuardError, SnapshotBuildProgressError,
    SnapshotBuildStage, SnapshotLifecycleWatchdog, SnapshotPressureMonitor,
    SnapshotPublicationError, SnapshotPublicationPhase, SnapshotPublicationReport,
    SnapshotPublicationRequest, SnapshotWatchdogConfig, SnapshotWatchdogError,
    SnapshotWatchdogFailureReason, SnapshotWatchdogState,
};

/// Connected PostgreSQL sessions exclusively owned by one snapshot job.
/// Keeping them in one value prevents unrelated runtime work from reusing a
/// transaction that imports the exported snapshot or owns the DDL guard.
pub struct SnapshotBuildConnections {
    pub control: Client,
    pub ddl_guard: Option<Client>,
    pub copy_workers: Vec<Client>,
    pub pressure: Option<Client>,
}

/// How long a build may go without a pressure sample when samples time out
/// before its watchdog stops it. A host stall of a few seconds then delays
/// the next sample instead of failing a build that may have copied for
/// minutes; a source whose lease lapses after as long loses the build anyway.
const PRESSURE_BLIND_LIMIT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
pub struct SnapshotBuildSafetyPolicy {
    pub watchdog: SnapshotWatchdogConfig,
    pub pressure_interval: Duration,
    pub pressure_sample_timeout: Duration,
    pub pressure_blind_limit: Duration,
    pub pressure: SnapshotPressurePolicy,
}

impl SnapshotBuildSafetyPolicy {
    pub fn new(
        watchdog: SnapshotWatchdogConfig,
        pressure_interval: Duration,
        pressure_sample_timeout: Duration,
        pressure: SnapshotPressurePolicy,
    ) -> Result<Self, SnapshotBuildJobError> {
        if pressure_interval.is_zero() || pressure_sample_timeout.is_zero() {
            return Err(SnapshotBuildJobError::InvalidConfiguration);
        }
        Ok(Self {
            watchdog,
            pressure_interval,
            pressure_sample_timeout,
            pressure_blind_limit: PRESSURE_BLIND_LIMIT,
            pressure,
        })
    }
}

/// The only file name a build's artifact path may end in: the direct-columnar
/// artifact the build writes into its private work directory and publishes.
pub const SNAPSHOT_ARTIFACT_FILE_NAME: &str = "artifact.calque";

#[derive(Debug, Clone)]
pub struct SnapshotBuildJobConfig {
    pub build_id: SnapshotBuildId,
    pub owner_id: SnapshotBuildOwnerId,
    pub journal_root: PathBuf,
    pub artifact_path: PathBuf,
    pub encoder_limits: EncoderLimits,
    pub replication_connection: String,
    pub stream: SnapshotStreamConfig,
    pub retention_ttl: Duration,
    pub retention_renewal_interval: Duration,
    pub copy_limits: BoundedCopyLimits,
    pub roll_forward_timeout: Duration,
    pub decoder_limits: DecoderLimits,
    pub barrier_wait: BarrierWaitRequest,
    pub safety: SnapshotBuildSafetyPolicy,
    pub registry_revalidation: postgres_connect::Endpoint,
    pub publication: SnapshotPublicationRequest,
}

pub struct SnapshotBuildSupervisor {
    config: SnapshotBuildJobConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotBuildRecoveryOutcome {
    AlreadyTerminal,
    Published {
        operational_cleanup: SnapshotOperationalCleanup,
    },
    InterruptedBuildCleaned,
    WaitingForCatalogLease {
        retry_at_unix_millis: i64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotOperationalCleanup {
    pub journal_reconciled: SnapshotCleanupStep,
    pub private_workspace_removed: SnapshotCleanupStep,
    pub blob_staging_removed: SnapshotCleanupStep,
    pub retention_released: SnapshotCleanupStep,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotCleanupStep {
    Complete,
    Pending(SnapshotCleanupFailureCode),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotCleanupFailureCode {
    JournalPersistence,
    PrivateWorkspace,
    BlobStaging,
    RetentionSourceMismatch,
    RetentionConflict,
    RetentionUnavailable,
    Deadline,
    TaskFailure,
}

impl SnapshotBuildSupervisor {
    pub fn new(config: SnapshotBuildJobConfig) -> Result<Self, SnapshotBuildJobError> {
        let private_work_dir = config.artifact_path.parent();
        if config.replication_connection.is_empty()
            || config.copy_limits.max_workers == 0
            || config.retention_ttl.is_zero()
            || config.retention_renewal_interval.is_zero()
            || config.retention_renewal_interval > config.retention_ttl / 2
            || config.roll_forward_timeout.is_zero()
            || config.artifact_path.as_os_str().is_empty()
            || !config.artifact_path.is_absolute()
            || config
                .artifact_path
                .file_name()
                .and_then(|name| name.to_str())
                != Some(SNAPSHOT_ARTIFACT_FILE_NAME)
            || private_work_dir
                .and_then(Path::file_name)
                .and_then(|name| name.to_str())
                != Some(config.build_id.as_str())
        {
            return Err(SnapshotBuildJobError::InvalidConfiguration);
        }
        Ok(Self { config })
    }

    /// Reconciles one interrupted build during exclusive process startup.
    ///
    /// Recovery never uses the configured build owner to bypass a live lease.
    /// It waits for the catalog's authoritative lease expiry, takes a fresh
    /// recovery capability, and only then removes never-published objects.
    /// `ManagedBlobStore::cleanup_staging` is process-global, so the
    /// caller must invoke this before starting any snapshot publication task.
    pub async fn recover_interrupted_local<P, S>(
        &self,
        provider: Arc<Mutex<P>>,
        catalog: &mut SqliteArtifactCatalog,
        store: &Arc<S>,
    ) -> Result<SnapshotBuildRecoveryOutcome, SnapshotBuildJobError>
    where
        P: SnapshotBuildRetention + CanonicalBarrierRead + Send + 'static,
        S: ManagedBlobStore + ?Sized + 'static,
    {
        let mut journal =
            SnapshotBuildJournal::reopen(&self.config.journal_root, &self.config.build_id)?;
        if journal.progress().artifact_id != self.config.publication.artifact.artifact_id.as_str()
            || self.config.artifact_path.parent().and_then(Path::to_str)
                != Some(journal.progress().private_work_dir.as_str())
        {
            return Err(SnapshotBuildJobError::RecoveryIdentityMismatch);
        }

        let artifact = &self.config.publication.artifact.artifact_id;
        let now = now_ms()?;
        match catalog
            .state(artifact)
            .map_err(SnapshotPublicationError::from)?
        {
            Some(ArtifactState::Published | ArtifactState::Retired) => {
                // Catalog visibility is authoritative even if a prior process
                // persisted FAILED/CANCELLED afterward. Record product success
                // first; all remaining work is operational follow-up.
                let journal_reconciled = match journal.reconcile_published(now) {
                    Ok(()) => SnapshotCleanupStep::Complete,
                    Err(_) => {
                        SnapshotCleanupStep::Pending(SnapshotCleanupFailureCode::JournalPersistence)
                    }
                };
                let private_workspace_removed = cleanup_step(
                    cleanup_private_work_dir_controlled(
                        &self.config.artifact_path,
                        self.config.safety.watchdog.no_progress_timeout(),
                    )
                    .await,
                    SnapshotCleanupFailureCode::PrivateWorkspace,
                );
                let blob_staging_removed = cleanup_step(
                    cleanup_local_staging_controlled(
                        store,
                        self.config.safety.watchdog.no_progress_timeout(),
                    )
                    .await,
                    SnapshotCleanupFailureCode::BlobStaging,
                );
                let retention_released = match verify_recovery_source_epoch(
                    Arc::clone(&provider),
                    &journal.progress().source_epoch,
                    self.config.safety.watchdog.no_progress_timeout(),
                )
                .await
                {
                    Ok(()) => cleanup_step(
                        fence_and_release_recovered_retention_exclusive(
                            Arc::clone(&provider),
                            &self.config,
                            journal.progress(),
                        )
                        .await,
                        SnapshotCleanupFailureCode::RetentionUnavailable,
                    ),
                    Err(error) => retention_cleanup_step(error),
                };
                Ok(SnapshotBuildRecoveryOutcome::Published {
                    operational_cleanup: SnapshotOperationalCleanup {
                        journal_reconciled,
                        private_workspace_removed,
                        blob_staging_removed,
                        retention_released,
                    },
                })
            }
            Some(
                ArtifactState::Building
                | ArtifactState::Uploading
                | ArtifactState::Uploaded
                | ArtifactState::Verified,
            ) => {
                verify_recovery_source_epoch(
                    Arc::clone(&provider),
                    &journal.progress().source_epoch,
                    self.config.safety.watchdog.no_progress_timeout(),
                )
                .await?;
                let recovery_owner = format!("{}:startup-recovery", self.config.owner_id.as_str());
                let cleanup = match catalog.take_over_expired_build(
                    artifact,
                    &recovery_owner,
                    now,
                    self.config.publication.lease_duration_ms,
                ) {
                    Ok(lease) => catalog
                        .abandon_build(
                            &lease,
                            &recovery_owner,
                            now,
                            0,
                            self.config.publication.lease_duration_ms,
                            "snapshot build interrupted by process restart",
                        )
                        .map_err(SnapshotPublicationError::from)?,
                    Err(CatalogError::ArtifactBuildAbandoned(_)) => {
                        match catalog.take_over_abandoned_build_cleanup(
                            artifact,
                            &recovery_owner,
                            now,
                            self.config.publication.lease_duration_ms,
                        ) {
                            Ok(cleanup) => cleanup,
                            Err(CatalogError::BuildCleanupLeaseStillActive { expires, .. }) => {
                                return Ok(SnapshotBuildRecoveryOutcome::WaitingForCatalogLease {
                                    retry_at_unix_millis: expires,
                                });
                            }
                            Err(error) => {
                                return Err(SnapshotPublicationError::from(error).into());
                            }
                        }
                    }
                    Err(CatalogError::BuildLeaseStillActive { expires, .. }) => {
                        return Ok(SnapshotBuildRecoveryOutcome::WaitingForCatalogLease {
                            retry_at_unix_millis: expires,
                        });
                    }
                    Err(error) => return Err(SnapshotPublicationError::from(error).into()),
                };
                tokio::time::timeout(
                    self.config.safety.watchdog.no_progress_timeout(),
                    catalog.cleanup_abandoned_build(&cleanup, store.as_ref(), now),
                )
                .await
                .map_err(|_| SnapshotBuildJobError::CleanupTimedOut)?
                .map_err(SnapshotPublicationError::from)?;
                cleanup_private_work_dir_controlled(
                    &self.config.artifact_path,
                    self.config.safety.watchdog.no_progress_timeout(),
                )
                .await?;
                cleanup_local_staging_controlled(
                    store,
                    self.config.safety.watchdog.no_progress_timeout(),
                )
                .await?;
                fence_and_release_recovered_retention_exclusive(
                    Arc::clone(&provider),
                    &self.config,
                    journal.progress(),
                )
                .await?;
                let was_terminal = journal.is_terminal();
                journal.reconcile_interrupted_cleanup(now)?;
                Ok(if was_terminal {
                    SnapshotBuildRecoveryOutcome::AlreadyTerminal
                } else {
                    SnapshotBuildRecoveryOutcome::InterruptedBuildCleaned
                })
            }
            None => {
                if journal.progress().stage == SnapshotBuildStage::Published {
                    return Err(SnapshotBuildJobError::RecoveryCatalogHistoryMissing);
                }
                verify_recovery_source_epoch(
                    Arc::clone(&provider),
                    &journal.progress().source_epoch,
                    self.config.safety.watchdog.no_progress_timeout(),
                )
                .await?;
                cleanup_private_work_dir_controlled(
                    &self.config.artifact_path,
                    self.config.safety.watchdog.no_progress_timeout(),
                )
                .await?;
                cleanup_local_staging_controlled(
                    store,
                    self.config.safety.watchdog.no_progress_timeout(),
                )
                .await?;
                fence_and_release_recovered_retention_exclusive(
                    provider,
                    &self.config,
                    journal.progress(),
                )
                .await?;
                let was_terminal = journal.is_terminal();
                journal.reconcile_interrupted_cleanup(now)?;
                Ok(if was_terminal {
                    SnapshotBuildRecoveryOutcome::AlreadyTerminal
                } else {
                    SnapshotBuildRecoveryOutcome::InterruptedBuildCleaned
                })
            }
        }
    }

    /// Discovers every durable build journal that startup recovery must audit.
    pub fn interrupted_builds(
        journal_root: impl AsRef<Path>,
    ) -> Result<Vec<SnapshotBuildId>, SnapshotBuildJobError> {
        SnapshotBuildJournal::scan(journal_root).map_err(Into::into)
    }

    /// Owns a build from durable admission through publication or complete
    /// terminal cleanup. The retention capability is acquired before export,
    /// renewed across every stage, and released only after `PUBLISHED` or the
    /// cleanup journal reaches its durable terminal boundary.
    ///
    /// `on_stage` is the test/operations observer boundary for exact lifecycle
    /// failpoints. The observer runs immediately after each stage transition
    /// the journal records. It may cancel the caller token, but it cannot mint
    /// progress or bypass cleanup.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_controlled<P, S, O>(
        &self,
        provider: Arc<Mutex<P>>,
        signal: &CanonicalHeadSignal,
        mut connections: SnapshotBuildConnections,
        registry: &SchemaRegistry,
        activated_source: &ActivatedSource,
        plan: SnapshotBuildPlan,
        catalog: &mut SqliteArtifactCatalog,
        store: &S,
        cancellation: &CancellationToken,
        mut on_stage: O,
    ) -> Result<SnapshotPublicationReport, SnapshotBuildJobError>
    where
        P: SnapshotBuildRetention + CanonicalBarrierRead + Send + 'static,
        S: BlobStore + ?Sized,
        O: FnMut(SnapshotBuildStage),
    {
        let tables_total = u64::try_from(plan.tables().len())
            .map_err(|_| SnapshotBuildJobError::InvalidConfiguration)?;
        let mut journal = SnapshotBuildJournal::create(
            &self.config.journal_root,
            &self.config.build_id,
            &self.config.owner_id,
            &self.config.publication.artifact.artifact_id,
            self.config
                .artifact_path
                .parent()
                .ok_or(SnapshotBuildJobError::InvalidConfiguration)?,
            activated_source.source_epoch().clone(),
            tables_total,
            now_ms()?,
        )?;
        on_stage(SnapshotBuildStage::Admitted);
        if let Err(error) = prepare_private_work_dir_controlled(
            &self.config.artifact_path,
            self.config.safety.watchdog.no_progress_timeout(),
        )
        .await
        {
            journal.begin_cleanup(
                SnapshotBuildFailureCode::CleanupFailed,
                SnapshotBuildDisposition::OperatorAction,
                now_ms()?,
            )?;
            on_stage(SnapshotBuildStage::CleaningUp);
            cleanup_private_work_dir_controlled(
                &self.config.artifact_path,
                self.config.safety.watchdog.no_progress_timeout(),
            )
            .await?;
            journal.finish_cleanup(now_ms()?)?;
            on_stage(SnapshotBuildStage::Failed);
            return Err(error);
        }
        let lease_guard = match SnapshotBuildLeaseGuard::acquire(
            Arc::clone(&provider),
            self.config.build_id.clone(),
            self.config.owner_id.clone(),
            self.config.retention_ttl,
            self.config.retention_renewal_interval,
            cancellation,
        )
        .await
        {
            Ok(guard) => guard,
            Err(error) => {
                journal.begin_cleanup(
                    SnapshotBuildFailureCode::LeaseLost,
                    SnapshotBuildDisposition::Retry,
                    now_ms()?,
                )?;
                on_stage(SnapshotBuildStage::CleaningUp);
                cleanup_private_work_dir_controlled(
                    &self.config.artifact_path,
                    self.config.safety.watchdog.no_progress_timeout(),
                )
                .await?;
                journal.finish_cleanup(now_ms()?)?;
                on_stage(SnapshotBuildStage::Failed);
                return Err(error.into());
            }
        };
        journal.transition(SnapshotBuildStage::RetentionLeased, now_ms()?)?;
        on_stage(SnapshotBuildStage::RetentionLeased);
        let operation_cancellation = lease_guard.cancellation_token();
        let watchdog = SnapshotLifecycleWatchdog::spawn(
            self.config.safety.watchdog,
            operation_cancellation.clone(),
            None,
        );
        watchdog.heartbeat();
        let mut pressure_revision = 0;

        let mut result = self
            .execute(
                provider,
                signal,
                &mut connections,
                registry,
                activated_source,
                plan,
                catalog,
                store,
                &operation_cancellation,
                &lease_guard,
                &watchdog,
                &mut pressure_revision,
                &mut journal,
                &mut on_stage,
            )
            .await;
        let publication_committed = result.is_ok();
        // `execute` returns success only after the catalog has committed
        // PUBLISHED. A watchdog join failure after that boundary is
        // post-publication cleanup uncertainty, not product failure.
        if let Ok(watchdog_state) = watchdog.stop().await {
            if !publication_committed {
                if let Err(error) =
                    record_watchdog_state(&watchdog_state, &mut pressure_revision, &mut journal)
                {
                    result = Err(error);
                }
            }
        }

        match result {
            Ok(report) => {
                // Journal fsync, observer and retention release are cleanup
                // after the PUBLISHED commit and never turn it into a build
                // failure; a failed release expires on its own.
                if let Ok(now) = now_ms() {
                    if let Err(error) = journal.reconcile_published(now) {
                        tracing::warn!(%error, "the published build's journal was not reconciled");
                    }
                }
                on_stage(SnapshotBuildStage::Published);
                if let Err(error) = lease_guard.release().await {
                    tracing::warn!(%error, "the published build did not release its lease; the lease expires on its own");
                }
                Ok(report)
            }
            Err(error) => {
                let publication_started = matches!(
                    journal.progress().stage,
                    SnapshotBuildStage::Finalizing
                        | SnapshotBuildStage::Uploading
                        | SnapshotBuildStage::Publishing
                );
                let error = if publication_started {
                    match self.cleanup_failed_publication(catalog, store).await {
                        Ok(()) if cancellation.is_cancelled() => SnapshotBuildJobError::Cancelled,
                        Ok(()) => error,
                        Err(cleanup_error) => cleanup_error,
                    }
                } else {
                    error
                };
                let (code, disposition) = classify_failure(
                    &error,
                    cancellation.is_cancelled(),
                    lease_guard.is_healthy(),
                );
                let lease_healthy = lease_guard.is_healthy();
                let cleanup_uncertain = cleanup_state_is_uncertain(&error);
                let mut terminalization_error = None;
                match now_ms().and_then(|now| {
                    journal
                        .begin_cleanup(code, disposition, now)
                        .map_err(SnapshotBuildJobError::from)
                }) {
                    Ok(()) => {
                        on_stage(SnapshotBuildStage::CleaningUp);
                        if let Err(cleanup_error) = cleanup_private_work_dir_controlled(
                            &self.config.artifact_path,
                            self.config.safety.watchdog.no_progress_timeout(),
                        )
                        .await
                        {
                            terminalization_error = Some(cleanup_error);
                        } else if !cleanup_uncertain {
                            match now_ms().and_then(|now| {
                                journal
                                    .finish_cleanup(now)
                                    .map_err(SnapshotBuildJobError::from)
                            }) {
                                Ok(()) => on_stage(journal.progress().stage),
                                Err(cleanup_error) => terminalization_error = Some(cleanup_error),
                            }
                        }
                    }
                    Err(cleanup_error) => terminalization_error = Some(cleanup_error),
                }

                // No journal or filesystem failure may abandon the renewal
                // owner. Join it on every path before returning. Uncertain or
                // unhealthy authority is retained until expiry; only a fully
                // terminalized healthy build releases the exact capability.
                let lease_result =
                    if cleanup_uncertain || terminalization_error.is_some() || !lease_healthy {
                        lease_guard.stop_without_release().await
                    } else {
                        lease_guard.release().await
                    };
                if let Err(lease_error) = lease_result {
                    return Err(lease_error.into());
                }
                if let Some(cleanup_error) = terminalization_error {
                    return Err(cleanup_error);
                }
                Err(error)
            }
        }
    }

    async fn cleanup_failed_publication<S: BlobStore + ?Sized>(
        &self,
        catalog: &mut SqliteArtifactCatalog,
        store: &S,
    ) -> Result<(), SnapshotBuildJobError> {
        let artifact = &self.config.publication.artifact.artifact_id;
        match catalog
            .state(artifact)
            .map_err(SnapshotPublicationError::from)?
        {
            Some(ArtifactState::Published | ArtifactState::Retired) => {
                return Err(SnapshotBuildJobError::PublicationVisibilityUncertain);
            }
            None => return Ok(()),
            Some(
                ArtifactState::Building
                | ArtifactState::Uploading
                | ArtifactState::Uploaded
                | ArtifactState::Verified,
            ) => {}
        }
        let Some(manifest) = catalog
            .manifest(artifact)
            .map_err(SnapshotPublicationError::from)?
        else {
            return Ok(());
        };
        let now = now_ms()?;
        let cleanup_lease_duration_ms =
            cleanup_lease_duration_ms(self.config.publication.publication_cleanup_timeout)?;
        let mut cleanup = match catalog.register_or_resume_build(
            &manifest,
            &self.config.publication.lease_owner,
            now,
            self.config.publication.lease_duration_ms,
        ) {
            Ok(registration) => catalog
                .abandon_build(
                    registration.lease(),
                    &self.config.publication.lease_owner,
                    now,
                    0,
                    cleanup_lease_duration_ms,
                    "snapshot build lifecycle failed",
                )
                .map_err(SnapshotPublicationError::AbandonedBuildCleanup)?,
            Err(CatalogError::ArtifactBuildAbandoned(_)) => {
                take_over_cleanup_after_expiry(
                    catalog,
                    artifact,
                    &self.config.publication.lease_owner,
                    cleanup_lease_duration_ms,
                )
                .await?
            }
            Err(error) => return Err(SnapshotPublicationError::from(error).into()),
        };

        for attempt in 0..2 {
            let cleanup_result = tokio::time::timeout(
                self.config.publication.publication_cleanup_timeout,
                catalog.cleanup_abandoned_build(&cleanup, store, now_ms()?),
            )
            .await;
            let failure = match cleanup_result {
                Ok(Ok(_)) => return Ok(()),
                Ok(Err(error)) => SnapshotBuildJobError::Publication(
                    SnapshotPublicationError::AbandonedBuildCleanup(error),
                ),
                Err(_) => SnapshotBuildJobError::CleanupTimedOut,
            };
            if attempt == 1 {
                return Err(failure);
            }
            cleanup = take_over_cleanup_after_expiry(
                catalog,
                artifact,
                &self.config.publication.lease_owner,
                cleanup_lease_duration_ms,
            )
            .await?;
        }
        unreachable!("bounded cleanup retry loop always returns")
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute<P, S, O>(
        &self,
        provider: Arc<Mutex<P>>,
        signal: &CanonicalHeadSignal,
        connections: &mut SnapshotBuildConnections,
        registry: &SchemaRegistry,
        activated_source: &ActivatedSource,
        plan: SnapshotBuildPlan,
        catalog: &mut SqliteArtifactCatalog,
        store: &S,
        cancellation: &CancellationToken,
        lease_guard: &SnapshotBuildLeaseGuard<P>,
        watchdog: &SnapshotLifecycleWatchdog,
        pressure_revision: &mut u64,
        journal: &mut SnapshotBuildJournal,
        on_stage: &mut O,
    ) -> Result<SnapshotPublicationReport, SnapshotBuildJobError>
    where
        P: SnapshotBuildRetention + CanonicalBarrierRead + Send + 'static,
        S: BlobStore + ?Sized,
        O: FnMut(SnapshotBuildStage),
    {
        ensure_active(cancellation)?;
        let assignments = copy_assignments(&plan, self.config.copy_limits.max_workers)?;
        if connections.copy_workers.len() < assignments.len() {
            return Err(SnapshotBuildJobError::InsufficientCopyConnections {
                required: assignments.len(),
                available: connections.copy_workers.len(),
            });
        }
        let copy_backend_pids = await_operation(
            copy_backend_pids(connections.copy_workers.iter().take(assignments.len())),
            cancellation,
        )
        .await?;

        let ddl_guard = await_operation(
            SnapshotDdlGuard::acquire(
                connections
                    .ddl_guard
                    .take()
                    .ok_or(SnapshotBuildJobError::InvalidConnectionOwnership)?,
                registry,
            ),
            cancellation,
        )
        .await?;
        let mut reservations = Vec::with_capacity(assignments.len());
        for client in connections.copy_workers.iter_mut().take(assignments.len()) {
            match await_operation(ddl_guard.reserve_copy(client), cancellation).await {
                Ok(reservation) => reservations.push(reservation),
                Err(error) => {
                    abort_reservations(
                        reservations,
                        self.config.safety.watchdog.no_progress_timeout(),
                    )
                    .await?;
                    tokio::time::timeout(
                        self.config.safety.watchdog.no_progress_timeout(),
                        ddl_guard.abort(),
                    )
                    .await
                    .map_err(|_| SnapshotBuildJobError::CleanupTimedOut)??;
                    return Err(error);
                }
            }
        }

        let coordinator = SnapshotCoordinator::export_controlled(
            &self.config.replication_connection,
            &mut connections.control,
            registry,
            ddl_guard,
            self.config.stream.clone(),
            cancellation,
            self.config.safety.watchdog.no_progress_timeout(),
        )
        .await?;
        let exported = coordinator.exported_snapshot().clone();
        journal.transition(SnapshotBuildStage::Exported, now_ms()?)?;
        on_stage(SnapshotBuildStage::Exported);
        if cancellation.is_cancelled() {
            abort_reservations(
                reservations,
                self.config.safety.watchdog.no_progress_timeout(),
            )
            .await?;
            abort_coordinator(
                coordinator,
                self.config.safety.watchdog.no_progress_timeout(),
            )
            .await?;
            return Err(SnapshotBuildJobError::Cancelled);
        }
        let pressure_target =
            SnapshotPressureTarget::new(exported.slot_name.clone(), copy_backend_pids)?;
        let pressure_monitor = SnapshotPressureMonitor::postgres(
            self.config.safety.pressure_interval,
            self.config.safety.pressure_sample_timeout,
            self.config.safety.pressure_blind_limit,
            connections
                .pressure
                .take()
                .ok_or(SnapshotBuildJobError::InvalidConnectionOwnership)?,
            pressure_target,
            self.config.safety.pressure,
        )?;
        if let Err(error) =
            await_operation(watchdog.attach_pressure(pressure_monitor), cancellation).await
        {
            abort_coordinator(
                coordinator,
                self.config.safety.watchdog.no_progress_timeout(),
            )
            .await?;
            return Err(error);
        }
        observe_watchdog(watchdog, pressure_revision, journal)?;

        let barrier_assignments = assignments
            .iter()
            .map(|assignment| (assignment.worker_id.clone(), assignment.table_ids.clone()));
        let mut barrier = ImportBarrier::new(&exported, barrier_assignments)?;
        let workers = match await_operation(
            import_workers(
                reservations,
                &assignments,
                &exported,
                &mut barrier,
                self.config.safety.watchdog.no_progress_timeout(),
            ),
            cancellation,
        )
        .await
        {
            Ok(workers) => workers,
            Err(error) => {
                detach_pressure_and_abort(
                    watchdog,
                    coordinator,
                    self.config.safety.watchdog.no_progress_timeout(),
                )
                .await?;
                return Err(error);
            }
        };

        let (build, stats) = self
            .roll_forward_then_copy(
                &mut connections.control,
                registry,
                activated_source,
                plan,
                coordinator,
                barrier,
                workers,
                &assignments,
                cancellation,
                watchdog,
                pressure_revision,
                journal,
                on_stage,
            )
            .await?;

        journal.transition(SnapshotBuildStage::WaitingForPersistentBarrier, now_ms()?)?;
        on_stage(SnapshotBuildStage::WaitingForPersistentBarrier);
        ensure_active(cancellation)?;
        observe_watchdog(watchdog, pressure_revision, journal)?;
        let lease = lease_guard.current_lease()?;
        let callback_cancellation = cancellation.clone();
        let mut progress_failure = None;
        let wait = wait_for_persistent_barrier(
            Arc::clone(&provider),
            signal,
            &lease,
            &stats.cut,
            self.config.barrier_wait,
            cancellation,
            |progress| {
                if progress_failure.is_some() {
                    return;
                }
                let update = now_ms().and_then(|now| {
                    journal
                        .record_barrier_progress(
                            progress.minimum_sequence,
                            progress.durable_head_sequence,
                            progress.records_scanned as u64,
                            now,
                        )
                        .map_err(SnapshotBuildJobError::from)
                });
                if let Err(error) = update {
                    progress_failure = Some(error);
                    callback_cancellation.cancel();
                    return;
                }
                if let Err(error) = observe_watchdog(watchdog, pressure_revision, journal) {
                    progress_failure = Some(error);
                    callback_cancellation.cancel();
                }
            },
        )
        .await;
        if let Some(error) = progress_failure {
            return Err(error);
        }
        let proof = wait?.proof;

        let private_artifact_path = self.config.artifact_path.clone();
        let private_cleanup_timeout = self.config.safety.watchdog.no_progress_timeout();
        let before_publish = Box::pin(async move {
            cleanup_private_work_dir_progress_controlled(
                private_artifact_path,
                private_cleanup_timeout,
            )
            .await
        });
        let on_phase = |phase| {
            let stage = match phase {
                SnapshotPublicationPhase::Finalizing => SnapshotBuildStage::Finalizing,
                SnapshotPublicationPhase::Uploading => SnapshotBuildStage::Uploading,
                SnapshotPublicationPhase::Publishing => SnapshotBuildStage::Publishing,
            };
            journal.transition(
                stage,
                now_ms().map_err(|_| SnapshotBuildProgressError::InvalidTimestamp)?,
            )?;
            on_stage(stage);
            observe_watchdog(watchdog, pressure_revision, journal)
                .map_err(|_| SnapshotBuildProgressError::InvalidTimestamp)?;
            Ok(())
        };
        publish_direct_with_proven_cut_before_publish_controlled(
            build,
            &proof,
            self.config.publication.clone(),
            catalog,
            store,
            cancellation,
            before_publish,
            on_phase,
            self.config.safety.watchdog.no_progress_timeout() / 4,
            || watchdog.heartbeat(),
        )
        .await
        .map_err(Into::into)
    }

    /// Starts the temporary stream once every worker has imported the
    /// exported snapshot, rolls it forward to the cut marker into the build's
    /// overlay, then COPYs every table through the overlay into the encoder.
    /// COPY output is final once encoded, so no row is copied before the cut.
    /// The stream stays open, unread past the cut, until COPY ends: snapshot
    /// pressure is sampled against its slot.
    #[allow(clippy::too_many_arguments)]
    async fn roll_forward_then_copy<O>(
        &self,
        control: &mut Client,
        registry: &SchemaRegistry,
        activated_source: &ActivatedSource,
        plan: SnapshotBuildPlan,
        mut coordinator: SnapshotCoordinator,
        mut barrier: ImportBarrier,
        workers: Vec<SnapshotCopyWorker<'_>>,
        assignments: &[CopyAssignment],
        cancellation: &CancellationToken,
        watchdog: &SnapshotLifecycleWatchdog,
        pressure_revision: &mut u64,
        journal: &mut SnapshotBuildJournal,
        on_stage: &mut O,
    ) -> Result<(DirectBuildSession, SnapshotRollForwardStats), SnapshotBuildJobError>
    where
        O: FnMut(SnapshotBuildStage),
    {
        let cleanup_timeout = self.config.safety.watchdog.no_progress_timeout();
        let exported = coordinator.exported_snapshot().clone();
        let at_cut = async {
            let mut build = DirectBuildSession::create(
                &self.config.artifact_path,
                plan,
                &exported,
                activated_source,
                self.config.encoder_limits,
            )?;
            coordinator
                .start_stream_after_import_controlled(
                    &barrier,
                    Some(&*control),
                    cancellation,
                    cleanup_timeout,
                )
                .await?;
            journal.transition(SnapshotBuildStage::RollingForward, now_ms()?)?;
            on_stage(SnapshotBuildStage::RollingForward);
            ensure_active(cancellation)?;
            observe_watchdog(watchdog, pressure_revision, journal)?;
            let generated = CutIdentity::generate();
            let cut_identity = CutIdentity::new(self.config.build_id.as_str(), generated.nonce)?;
            let emission =
                await_operation(emit_cut_marker(control, cut_identity), cancellation).await?;
            let stats = build
                .roll_forward_to_cut_controlled(
                    &mut coordinator,
                    &emission,
                    self.config.roll_forward_timeout,
                    self.config.decoder_limits.clone(),
                    registry,
                    self.config.registry_revalidation.clone(),
                    cancellation,
                )
                .await?;
            journal.transition(SnapshotBuildStage::Copying, now_ms()?)?;
            on_stage(SnapshotBuildStage::Copying);
            ensure_active(cancellation)?;
            observe_watchdog(watchdog, pressure_revision, journal)?;
            Ok::<_, SnapshotBuildJobError>((build, stats))
        }
        .await;
        let (mut build, stats) = match at_cut {
            Ok(at_cut) => at_cut,
            Err(error) => {
                abort_workers(workers, cleanup_timeout).await?;
                detach_pressure_and_abort(watchdog, coordinator, cleanup_timeout).await?;
                return Err(error);
            }
        };

        let workloads = workers
            .into_iter()
            .zip(assignments)
            .map(|(worker, assignment)| {
                SnapshotCopyWorkload::new(worker, assignment.table_indices.clone())
            })
            .collect();
        let copy_cancellation = cancellation.child_token();
        let stop_holding = CancellationToken::new();
        let mut progress_failure = None;
        let copy_started = std::time::Instant::now();
        let mut progress_calls = 0u64;
        let mut progress_elapsed = std::time::Duration::ZERO;
        let hold = async {
            let held = coordinator
                .hold_after_cut_controlled(&stop_holding, cancellation)
                .await;
            if held.is_err() {
                copy_cancellation.cancel();
            }
            held
        };
        let copy = async {
            let copied = build
                .load_copy_workers_bounded_controlled(
                    workloads,
                    self.config.copy_limits,
                    cleanup_timeout,
                    &copy_cancellation,
                    |progress| {
                        if progress_failure.is_some() {
                            return;
                        }
                        let progress_started = std::time::Instant::now();
                        progress_calls += 1;
                        let update = now_ms().and_then(|now| {
                            journal
                                .record_copy_progress(
                                    progress.tables_completed as u64,
                                    progress.total_rows,
                                    progress.total_decoded_value_bytes,
                                    progress.total_committed_batches,
                                    progress.active_workers as u64,
                                    progress.queued_tables as u64,
                                    now,
                                )
                                .map_err(SnapshotBuildJobError::from)
                        });
                        if let Err(error) = update {
                            progress_failure = Some(error);
                            copy_cancellation.cancel();
                            return;
                        }
                        if let Err(error) = observe_watchdog(watchdog, pressure_revision, journal) {
                            progress_failure = Some(error);
                            copy_cancellation.cancel();
                        }
                        progress_elapsed += progress_started.elapsed();
                    },
                )
                .await;
            stop_holding.cancel();
            copied
        };
        let (held, copied) = tokio::join!(hold, copy);
        tracing::info!(
            target: "calque::preparation",
            phase = "copy_and_encode",
            elapsed_ms = copy_started.elapsed().as_millis() as u64,
            progress_calls,
            progress_ms = progress_elapsed.as_millis() as u64,
            "preparation phase finished"
        );
        // A lost stream cancels COPY, so its error is the cause; COPY's own
        // failure stops the hold cleanly.
        let failure = match (progress_failure, held, copied) {
            (Some(error), _, _) => Some(error),
            (None, Err(error), _) => Some(error.into()),
            (None, Ok(()), Err(error)) => Some(error.into()),
            (None, Ok(()), Ok(completions)) => completions
                .into_iter()
                .try_for_each(|completion| barrier.record_completion(completion))
                .and_then(|()| barrier.ensure_complete())
                .err()
                .map(Into::into),
        };
        detach_pressure_and_abort(watchdog, coordinator, cleanup_timeout).await?;
        match failure {
            Some(error) => Err(error),
            None => Ok((build, stats)),
        }
    }
}

#[derive(Debug)]
struct CopyAssignment {
    worker_id: String,
    table_indices: Vec<usize>,
    table_ids: Vec<sync_types::TableId>,
}

fn copy_assignments(
    plan: &SnapshotBuildPlan,
    maximum_workers: usize,
) -> Result<Vec<CopyAssignment>, SnapshotBuildJobError> {
    if maximum_workers == 0 || plan.tables().is_empty() {
        return Err(SnapshotBuildJobError::InvalidConfiguration);
    }
    let worker_count = maximum_workers.min(plan.tables().len());
    let mut assignments = (0..worker_count)
        .map(|index| CopyAssignment {
            worker_id: format!("snapshot-copy-{index}"),
            table_indices: Vec::new(),
            table_ids: Vec::new(),
        })
        .collect::<Vec<_>>();
    for (table_index, table) in plan.tables().iter().enumerate() {
        let assignment = &mut assignments[table_index % worker_count];
        assignment.table_indices.push(table_index);
        assignment.table_ids.push(table.table_id().clone());
    }
    Ok(assignments)
}

async fn import_workers<'a>(
    reservations: Vec<SnapshotCopyReservation<'a>>,
    assignments: &[CopyAssignment],
    exported: &snapshot_coordinator::ExportedSnapshot,
    barrier: &mut ImportBarrier,
    cleanup_timeout: Duration,
) -> Result<Vec<SnapshotCopyWorker<'a>>, SnapshotBuildJobError> {
    let mut reservations = reservations.into_iter();
    let mut workers = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let reservation = reservations
            .next()
            .ok_or(SnapshotBuildJobError::InvalidConfiguration)?;
        match import_snapshot(reservation, exported, barrier, &assignment.worker_id).await {
            Ok(worker) => workers.push(worker),
            Err(error) => {
                abort_workers(workers, cleanup_timeout).await?;
                abort_reservations(reservations.collect(), cleanup_timeout).await?;
                return Err(error.into());
            }
        }
    }
    Ok(workers)
}

async fn abort_workers(
    workers: Vec<SnapshotCopyWorker<'_>>,
    timeout: Duration,
) -> Result<(), SnapshotBuildJobError> {
    for worker in workers {
        tokio::time::timeout(timeout, worker.abort())
            .await
            .map_err(|_| SnapshotBuildJobError::CleanupTimedOut)??;
    }
    Ok(())
}

async fn abort_reservations(
    reservations: Vec<SnapshotCopyReservation<'_>>,
    timeout: Duration,
) -> Result<(), SnapshotBuildJobError> {
    for reservation in reservations {
        tokio::time::timeout(timeout, reservation.abort())
            .await
            .map_err(|_| SnapshotBuildJobError::CleanupTimedOut)??;
    }
    Ok(())
}

async fn detach_pressure_and_abort(
    watchdog: &SnapshotLifecycleWatchdog,
    coordinator: SnapshotCoordinator,
    timeout: Duration,
) -> Result<(), SnapshotBuildJobError> {
    let detached = tokio::time::timeout(timeout, watchdog.detach_pressure())
        .await
        .map_err(|_| SnapshotBuildJobError::CleanupTimedOut)?;
    let aborted = tokio::time::timeout(timeout, coordinator.abort())
        .await
        .map_err(|_| SnapshotBuildJobError::CleanupTimedOut)?;
    aborted?;
    match detached {
        Ok(())
        | Err(SnapshotWatchdogError::PressureDetachTooLate)
        | Err(SnapshotWatchdogError::PressureAlreadyDetached) => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

async fn abort_coordinator(
    coordinator: SnapshotCoordinator,
    timeout: Duration,
) -> Result<(), SnapshotBuildJobError> {
    tokio::time::timeout(timeout, coordinator.abort())
        .await
        .map_err(|_| SnapshotBuildJobError::CleanupTimedOut)??;
    Ok(())
}

/// Fences and releases the journal owner's orphaned retention capability.
///
/// The caller must hold exclusive startup recovery authority. Reacquiring an
/// expired lease is cleanup authority only; it is never evidence that the
/// interrupted artifact remains safe to resume or publish.
async fn fence_and_release_recovered_retention_exclusive<P>(
    provider: Arc<Mutex<P>>,
    config: &SnapshotBuildJobConfig,
    progress: &crate::SnapshotBuildProgress,
) -> Result<(), SnapshotBuildJobError>
where
    P: SnapshotBuildRetention + CanonicalBarrierRead + Send + 'static,
{
    let expected_epoch = progress.source_epoch.clone();
    let owner = SnapshotBuildOwnerId::new(progress.owner_id.clone())
        .map_err(|_| SnapshotBuildJobError::RecoveryIdentityMismatch)?;
    let guard = SnapshotBuildLeaseGuard::acquire(
        provider,
        config.build_id.clone(),
        owner,
        config.retention_ttl,
        config.retention_renewal_interval,
        &CancellationToken::new(),
    )
    .await?;
    if guard.current_lease()?.source_epoch() != &expected_epoch {
        guard.release().await?;
        return Err(SnapshotBuildJobError::RecoverySourceEpochMismatch);
    }
    guard.release().await?;
    Ok(())
}

async fn verify_recovery_source_epoch<P>(
    provider: Arc<Mutex<P>>,
    expected: &SourceEpoch,
    timeout: Duration,
) -> Result<(), SnapshotBuildJobError>
where
    P: CanonicalBarrierRead + Send + 'static,
{
    let provider_epoch = tokio::time::timeout(
        timeout,
        tokio::task::spawn_blocking(move || {
            provider
                .lock()
                .map_err(|_| SnapshotBuildJobError::RecoveryProviderUnavailable)?
                .source_epoch()
                .map_err(|_| SnapshotBuildJobError::RecoveryProviderUnavailable)
        }),
    )
    .await
    .map_err(|_| SnapshotBuildJobError::RecoveryProviderTimedOut)?
    .map_err(SnapshotBuildJobError::RecoveryProviderTask)??;
    if provider_epoch != *expected {
        return Err(SnapshotBuildJobError::RecoverySourceEpochMismatch);
    }
    Ok(())
}

fn cleanup_step(
    result: Result<(), SnapshotBuildJobError>,
    fallback: SnapshotCleanupFailureCode,
) -> SnapshotCleanupStep {
    match result {
        Ok(()) => SnapshotCleanupStep::Complete,
        Err(SnapshotBuildJobError::CleanupTimedOut)
        | Err(SnapshotBuildJobError::RecoveryProviderTimedOut) => {
            SnapshotCleanupStep::Pending(SnapshotCleanupFailureCode::Deadline)
        }
        Err(SnapshotBuildJobError::CleanupTask(_))
        | Err(SnapshotBuildJobError::RecoveryProviderTask(_)) => {
            SnapshotCleanupStep::Pending(SnapshotCleanupFailureCode::TaskFailure)
        }
        Err(_) => SnapshotCleanupStep::Pending(fallback),
    }
}

fn retention_cleanup_step(error: SnapshotBuildJobError) -> SnapshotCleanupStep {
    let code = match error {
        SnapshotBuildJobError::RecoverySourceEpochMismatch => {
            SnapshotCleanupFailureCode::RetentionSourceMismatch
        }
        SnapshotBuildJobError::RecoveryProviderTimedOut
        | SnapshotBuildJobError::CleanupTimedOut => SnapshotCleanupFailureCode::Deadline,
        SnapshotBuildJobError::RecoveryProviderTask(_) | SnapshotBuildJobError::CleanupTask(_) => {
            SnapshotCleanupFailureCode::TaskFailure
        }
        SnapshotBuildJobError::Lease(SnapshotBuildLeaseGuardError::CanonicalLog(
            canonical_log::LogError::SnapshotBuildRetentionLeaseConflict,
        )) => SnapshotCleanupFailureCode::RetentionConflict,
        _ => SnapshotCleanupFailureCode::RetentionUnavailable,
    };
    SnapshotCleanupStep::Pending(code)
}

async fn await_operation<T, E, F>(
    operation: F,
    cancellation: &CancellationToken,
) -> Result<T, SnapshotBuildJobError>
where
    F: Future<Output = Result<T, E>>,
    SnapshotBuildJobError: From<E>,
{
    tokio::pin!(operation);
    tokio::select! {
        biased;
        result = &mut operation => result.map_err(Into::into),
        () = cancellation.cancelled() => Err(SnapshotBuildJobError::Cancelled),
    }
}

async fn copy_backend_pids<'a>(
    clients: impl IntoIterator<Item = &'a Client>,
) -> Result<Vec<i32>, SnapshotBuildJobError> {
    let mut pids = Vec::new();
    for client in clients {
        pids.push(
            client
                .query_one("SELECT pg_backend_pid()", &[])
                .await?
                .get(0),
        );
    }
    Ok(pids)
}

fn observe_watchdog(
    watchdog: &SnapshotLifecycleWatchdog,
    pressure_revision: &mut u64,
    journal: &mut SnapshotBuildJournal,
) -> Result<(), SnapshotBuildJobError> {
    watchdog.heartbeat();
    record_watchdog_state(&watchdog.latest(), pressure_revision, journal)
}

fn record_watchdog_state(
    state: &SnapshotWatchdogState,
    pressure_revision: &mut u64,
    journal: &mut SnapshotBuildJournal,
) -> Result<(), SnapshotBuildJobError> {
    if let Some(observation) = state
        .latest_pressure
        .as_ref()
        .filter(|observation| observation.revision > *pressure_revision)
    {
        journal.record_pressure(&observation.sample, &observation.evaluation, now_ms()?)?;
        *pressure_revision = observation.revision;
    }
    if let Some(reason) = state.failure_reason {
        return Err(SnapshotBuildJobError::Watchdog(reason));
    }
    Ok(())
}

fn ensure_active(cancellation: &CancellationToken) -> Result<(), SnapshotBuildJobError> {
    if cancellation.is_cancelled() {
        Err(SnapshotBuildJobError::Cancelled)
    } else {
        Ok(())
    }
}

async fn prepare_private_work_dir_controlled(
    artifact_path: &Path,
    timeout: Duration,
) -> Result<(), SnapshotBuildJobError> {
    tokio::time::timeout(
        timeout,
        prepare_private_work_dir_async(artifact_path.to_path_buf()),
    )
    .await
    .map_err(|_| SnapshotBuildJobError::CleanupTimedOut)?
}

async fn prepare_private_work_dir_async(
    artifact_path: PathBuf,
) -> Result<(), SnapshotBuildJobError> {
    let directory = artifact_path
        .parent()
        .ok_or(SnapshotBuildJobError::InvalidConfiguration)?
        .to_path_buf();
    let root = directory
        .parent()
        .ok_or(SnapshotBuildJobError::InvalidConfiguration)?
        .to_path_buf();
    tokio::fs::create_dir_all(&root)
        .await
        .map_err(SnapshotBuildJobError::Cleanup)?;
    let root_metadata = tokio::fs::symlink_metadata(&root)
        .await
        .map_err(SnapshotBuildJobError::Cleanup)?;
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        return Err(SnapshotBuildJobError::UnsafePrivateWorkPath(root));
    }
    match tokio::fs::create_dir(&directory).await {
        Ok(()) => sync_directory_async(root)
            .await
            .map_err(SnapshotBuildJobError::Cleanup),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = tokio::fs::symlink_metadata(&directory)
                .await
                .map_err(SnapshotBuildJobError::Cleanup)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(SnapshotBuildJobError::UnsafePrivateWorkPath(directory));
            }
            let mut entries = tokio::fs::read_dir(&directory)
                .await
                .map_err(SnapshotBuildJobError::Cleanup)?;
            if entries
                .next_entry()
                .await
                .map_err(SnapshotBuildJobError::Cleanup)?
                .is_some()
            {
                return Err(SnapshotBuildJobError::PrivateWorkDirectoryNotEmpty);
            }
            Ok(())
        }
        Err(error) => Err(SnapshotBuildJobError::Cleanup(error)),
    }
}

#[cfg(test)]
fn prepare_private_work_dir(artifact_path: &Path) -> Result<(), SnapshotBuildJobError> {
    let directory = artifact_path
        .parent()
        .ok_or(SnapshotBuildJobError::InvalidConfiguration)?;
    let root = directory
        .parent()
        .ok_or(SnapshotBuildJobError::InvalidConfiguration)?;
    std::fs::create_dir_all(root).map_err(SnapshotBuildJobError::Cleanup)?;
    let root_metadata = std::fs::symlink_metadata(root).map_err(SnapshotBuildJobError::Cleanup)?;
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        return Err(SnapshotBuildJobError::UnsafePrivateWorkPath(
            root.to_path_buf(),
        ));
    }
    match std::fs::create_dir(directory) {
        Ok(()) => sync_directory(root).map_err(SnapshotBuildJobError::Cleanup),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata =
                std::fs::symlink_metadata(directory).map_err(SnapshotBuildJobError::Cleanup)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                Err(SnapshotBuildJobError::UnsafePrivateWorkPath(
                    directory.to_path_buf(),
                ))
            } else if std::fs::read_dir(directory)
                .map_err(SnapshotBuildJobError::Cleanup)?
                .next()
                .is_some()
            {
                Err(SnapshotBuildJobError::PrivateWorkDirectoryNotEmpty)
            } else {
                Ok(())
            }
        }
        Err(error) => Err(SnapshotBuildJobError::Cleanup(error)),
    }
}

async fn take_over_cleanup_after_expiry(
    catalog: &mut SqliteArtifactCatalog,
    artifact: &sync_types::ArtifactId,
    owner: &str,
    cleanup_lease_duration_ms: i64,
) -> Result<BuildCleanupLease, SnapshotBuildJobError> {
    let now = now_ms()?;
    match catalog.take_over_abandoned_build_cleanup(artifact, owner, now, cleanup_lease_duration_ms)
    {
        Ok(cleanup) => Ok(cleanup),
        Err(CatalogError::BuildCleanupLeaseStillActive { expires, .. }) => {
            let wait_ms = expires.saturating_sub(now).saturating_add(1);
            tokio::time::sleep(Duration::from_millis(
                u64::try_from(wait_ms).map_err(|_| SnapshotBuildJobError::Clock)?,
            ))
            .await;
            catalog
                .take_over_abandoned_build_cleanup(
                    artifact,
                    owner,
                    now_ms()?,
                    cleanup_lease_duration_ms,
                )
                .map_err(SnapshotPublicationError::AbandonedBuildCleanup)
                .map_err(Into::into)
        }
        Err(error) => Err(SnapshotPublicationError::AbandonedBuildCleanup(error).into()),
    }
}

fn cleanup_lease_duration_ms(timeout: Duration) -> Result<i64, SnapshotBuildJobError> {
    let attempt_ms = timeout.as_millis().max(1);
    let margin_ms = (attempt_ms / 10).max(1);
    i64::try_from(
        attempt_ms
            .checked_add(margin_ms)
            .ok_or(SnapshotBuildJobError::InvalidConfiguration)?,
    )
    .map_err(|_| SnapshotBuildJobError::InvalidConfiguration)
}

async fn cleanup_private_work_dir_controlled(
    path: &Path,
    timeout: Duration,
) -> Result<(), SnapshotBuildJobError> {
    tokio::time::timeout(timeout, cleanup_private_work_dir_async(path.to_path_buf()))
        .await
        .map_err(|_| SnapshotBuildJobError::CleanupTimedOut)?
        .map_err(SnapshotBuildJobError::Cleanup)
}

async fn cleanup_private_work_dir_progress_controlled(
    path: PathBuf,
    timeout: Duration,
) -> Result<(), SnapshotBuildProgressError> {
    tokio::time::timeout(timeout, cleanup_private_work_dir_async(path))
        .await
        .map_err(|_| {
            SnapshotBuildProgressError::PrivateWorkDirCleanup(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "private work directory cleanup timed out",
            ))
        })?
        .map_err(SnapshotBuildProgressError::PrivateWorkDirCleanup)
}

async fn cleanup_private_work_dir_async(path: PathBuf) -> Result<(), std::io::Error> {
    let directory = path
        .parent()
        .ok_or_else(|| std::io::Error::other("private artifact path has no parent"))?
        .to_path_buf();
    let root = directory
        .parent()
        .ok_or_else(|| std::io::Error::other("private work directory has no parent"))?
        .to_path_buf();
    match tokio::fs::symlink_metadata(&directory).await {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(std::io::Error::other("private work path is unsafe"));
            }
            reject_symlink_tree_async(&directory).await?;
            tokio::fs::remove_dir_all(&directory).await?;
            sync_directory_async(root).await?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    Ok(())
}

async fn reject_symlink_tree_async(path: &Path) -> Result<(), std::io::Error> {
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let mut entries = tokio::fs::read_dir(directory).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if file_type.is_symlink() {
                return Err(std::io::Error::other(
                    "private work tree contains a symlink",
                ));
            }
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if !file_type.is_file() {
                return Err(std::io::Error::other(
                    "private work tree contains an unsafe entry",
                ));
            }
        }
    }
    Ok(())
}

/// The sweep runs on the blocking pool because it is synchronous filesystem
/// work; `store` is borrowed for the whole of it, so the timeout has to abandon
/// the join handle rather than the borrow. `Arc` gives the blocking closure an
/// owned handle without requiring the store itself to be `Clone`, which
/// `dyn ManagedBlobStore` is not.
async fn cleanup_local_staging_controlled<S>(
    store: &Arc<S>,
    timeout: Duration,
) -> Result<(), SnapshotBuildJobError>
where
    S: ManagedBlobStore + ?Sized + 'static,
{
    let store = Arc::clone(store);
    tokio::time::timeout(timeout, async move {
        tokio::task::spawn_blocking(move || store.cleanup_staging())
            .await
            .map_err(SnapshotBuildJobError::CleanupTask)?
            .map_err(SnapshotPublicationError::from)?;
        Ok(())
    })
    .await
    .map_err(|_| SnapshotBuildJobError::CleanupTimedOut)?
}

async fn sync_directory_async(path: PathBuf) -> Result<(), std::io::Error> {
    tokio::fs::File::open(path).await?.sync_all().await
}

#[cfg(test)]
fn cleanup_private_work_dir(path: &Path) -> Result<(), SnapshotBuildJobError> {
    cleanup_private_work_dir_raw(path).map_err(SnapshotBuildJobError::Cleanup)
}

#[cfg(test)]
fn cleanup_private_work_dir_raw(path: &Path) -> Result<(), std::io::Error> {
    let directory = path
        .parent()
        .ok_or_else(|| std::io::Error::other("private artifact path has no parent"))?;
    let root = directory
        .parent()
        .ok_or_else(|| std::io::Error::other("private work directory has no parent"))?;
    match std::fs::symlink_metadata(directory) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(std::io::Error::other("private work path is unsafe"));
            }
            reject_symlink_tree(directory)?;
            std::fs::remove_dir_all(directory)?;
            sync_directory(root)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    Ok(())
}

#[cfg(test)]
fn reject_symlink_tree(path: &Path) -> Result<(), std::io::Error> {
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Err(std::io::Error::other(
                "private work tree contains a symlink",
            ));
        }
        if file_type.is_dir() {
            reject_symlink_tree(&entry.path())?;
        } else if !file_type.is_file() {
            return Err(std::io::Error::other(
                "private work tree contains an unsafe entry",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
fn sync_directory(path: &Path) -> Result<(), std::io::Error> {
    std::fs::File::open(path)?.sync_all()
}

fn cleanup_state_is_uncertain(error: &SnapshotBuildJobError) -> bool {
    match error {
        SnapshotBuildJobError::Cleanup(_)
        | SnapshotBuildJobError::CleanupTask(_)
        | SnapshotBuildJobError::CleanupTimedOut => true,
        SnapshotBuildJobError::Publication(
            SnapshotPublicationError::AbandonedBuildCleanup(_)
            | SnapshotPublicationError::AbandonedBuildCleanupTimedOut,
        ) => true,
        SnapshotBuildJobError::Publication(SnapshotPublicationError::Progress(
            SnapshotBuildProgressError::PrivateWorkDirCleanup(_),
        )) => false,
        SnapshotBuildJobError::Publication(SnapshotPublicationError::Progress(_)) => true,
        SnapshotBuildJobError::Publication(SnapshotPublicationError::Catalog(
            artifact_catalog::CatalogError::ArtifactBuildAbandoned(_),
        )) => true,
        _ => false,
    }
}

fn now_ms() -> Result<i64, SnapshotBuildJobError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SnapshotBuildJobError::Clock)?
        .as_millis();
    i64::try_from(millis).map_err(|_| SnapshotBuildJobError::Clock)
}

fn classify_failure(
    error: &SnapshotBuildJobError,
    caller_cancelled: bool,
    lease_healthy: bool,
) -> (SnapshotBuildFailureCode, SnapshotBuildDisposition) {
    if caller_cancelled {
        return (
            SnapshotBuildFailureCode::CallerCancelled,
            SnapshotBuildDisposition::Cancelled,
        );
    }
    if !lease_healthy {
        return (
            SnapshotBuildFailureCode::LeaseLost,
            SnapshotBuildDisposition::Retry,
        );
    }
    if matches!(error, SnapshotBuildJobError::Cancelled) {
        return (
            SnapshotBuildFailureCode::CleanupFailed,
            SnapshotBuildDisposition::Retry,
        );
    }
    match error {
        SnapshotBuildJobError::Watchdog(SnapshotWatchdogFailureReason::MaximumBuildDuration) => (
            SnapshotBuildFailureCode::BuildDurationLimit,
            SnapshotBuildDisposition::Retry,
        ),
        SnapshotBuildJobError::Watchdog(SnapshotWatchdogFailureReason::NoProgress) => (
            SnapshotBuildFailureCode::NoProgressLimit,
            SnapshotBuildDisposition::Retry,
        ),
        SnapshotBuildJobError::Watchdog(SnapshotWatchdogFailureReason::PressureHardLimit {
            kind,
        }) => {
            let code = match kind {
                crate::SnapshotPressureLimitKind::WalPressure => {
                    SnapshotBuildFailureCode::WalPressureLimit
                }
                crate::SnapshotPressureLimitKind::CatalogXidAge => {
                    SnapshotBuildFailureCode::CatalogXidAgeLimit
                }
                crate::SnapshotPressureLimitKind::CopyTransactionAge => {
                    SnapshotBuildFailureCode::CopyTransactionAgeLimit
                }
            };
            (code, SnapshotBuildDisposition::OperatorAction)
        }
        SnapshotBuildJobError::Watchdog(
            SnapshotWatchdogFailureReason::PressureSamplingUncertain { .. },
        ) => (
            SnapshotBuildFailureCode::PressureSamplingUncertain,
            SnapshotBuildDisposition::Retry,
        ),
        SnapshotBuildJobError::Barrier(BarrierWaitError::RetentionCrossed { .. }) => (
            SnapshotBuildFailureCode::RetentionCrossed,
            SnapshotBuildDisposition::Rebuild,
        ),
        SnapshotBuildJobError::Barrier(BarrierWaitError::Timeout) => (
            SnapshotBuildFailureCode::PersistentBarrierTimeout,
            SnapshotBuildDisposition::Retry,
        ),
        SnapshotBuildJobError::Barrier(BarrierWaitError::SourceEpochChanged) => (
            SnapshotBuildFailureCode::SourceEpochChanged,
            SnapshotBuildDisposition::Rebuild,
        ),
        SnapshotBuildJobError::Builder(SnapshotBuildStateError::Copy(_)) => (
            SnapshotBuildFailureCode::CopyWorkerFailed,
            SnapshotBuildDisposition::Retry,
        ),
        SnapshotBuildJobError::Builder(SnapshotBuildStateError::Coordinator(_)) => (
            SnapshotBuildFailureCode::CopyWorkerFailed,
            SnapshotBuildDisposition::Retry,
        ),
        SnapshotBuildJobError::Builder(SnapshotBuildStateError::RollForward(
            SnapshotRollForwardError::Stream(RollForwardError::Coordinator(_)),
        )) => (
            SnapshotBuildFailureCode::TemporaryStreamLost,
            SnapshotBuildDisposition::Retry,
        ),
        SnapshotBuildJobError::Builder(SnapshotBuildStateError::Registry(_)) => (
            SnapshotBuildFailureCode::SchemaChanged,
            SnapshotBuildDisposition::Rebuild,
        ),
        SnapshotBuildJobError::Publication(
            SnapshotPublicationError::DirectFinalization(_)
            | SnapshotPublicationError::FinalizationTask(_)
            | SnapshotPublicationError::Representation(_),
        ) => (
            SnapshotBuildFailureCode::FinalizationFailed,
            SnapshotBuildDisposition::Rebuild,
        ),
        SnapshotBuildJobError::Publication(SnapshotPublicationError::Storage(_)) => (
            SnapshotBuildFailureCode::StorageFailed,
            SnapshotBuildDisposition::Retry,
        ),
        SnapshotBuildJobError::Publication(SnapshotPublicationError::Catalog(_)) => (
            SnapshotBuildFailureCode::CatalogFailed,
            SnapshotBuildDisposition::Retry,
        ),
        SnapshotBuildJobError::Publication(SnapshotPublicationError::Progress(
            SnapshotBuildProgressError::PrivateWorkDirCleanup(_),
        )) => (
            SnapshotBuildFailureCode::CleanupFailed,
            SnapshotBuildDisposition::OperatorAction,
        ),
        SnapshotBuildJobError::Publication(SnapshotPublicationError::AbandonedBuildCleanup(_)) => (
            SnapshotBuildFailureCode::CleanupFailed,
            SnapshotBuildDisposition::OperatorAction,
        ),
        SnapshotBuildJobError::Cleanup(_)
        | SnapshotBuildJobError::CleanupTask(_)
        | SnapshotBuildJobError::CleanupTimedOut => (
            SnapshotBuildFailureCode::CleanupFailed,
            SnapshotBuildDisposition::OperatorAction,
        ),
        SnapshotBuildJobError::Coordinator(_) => (
            SnapshotBuildFailureCode::TemporaryStreamLost,
            SnapshotBuildDisposition::Retry,
        ),
        _ => (
            SnapshotBuildFailureCode::CleanupFailed,
            SnapshotBuildDisposition::OperatorAction,
        ),
    }
}

#[derive(Debug, Error)]
pub enum SnapshotBuildJobError {
    #[error("snapshot build job configuration is invalid")]
    InvalidConfiguration,
    #[error("snapshot build connection ownership is invalid")]
    InvalidConnectionOwnership,
    #[error("snapshot build requires {required} COPY connections but only {available} exist")]
    InsufficientCopyConnections { required: usize, available: usize },
    #[error("snapshot build was cancelled")]
    Cancelled,
    #[error("snapshot publication visibility is uncertain after a failed lifecycle")]
    PublicationVisibilityUncertain,
    #[error("snapshot recovery configuration does not match its durable journal identity")]
    RecoveryIdentityMismatch,
    #[error("snapshot recovery cannot find catalog history for a journaled publication")]
    RecoveryCatalogHistoryMissing,
    #[error("snapshot recovery canonical provider is unavailable")]
    RecoveryProviderUnavailable,
    #[error("snapshot recovery canonical provider operation exceeded its safety deadline")]
    RecoveryProviderTimedOut,
    #[error("snapshot recovery canonical provider task failed: {0}")]
    RecoveryProviderTask(#[source] tokio::task::JoinError),
    #[error("snapshot recovery source epoch differs from the durable journal")]
    RecoverySourceEpochMismatch,
    #[error("snapshot build system clock is invalid")]
    Clock,
    #[error("snapshot lifecycle watchdog terminated the build: {0:?}")]
    Watchdog(SnapshotWatchdogFailureReason),
    #[error("snapshot private work directory cleanup failed: {0}")]
    Cleanup(#[source] std::io::Error),
    #[error("snapshot filesystem cleanup task failed: {0}")]
    CleanupTask(#[source] tokio::task::JoinError),
    #[error("snapshot private work path is unsafe: {0}")]
    UnsafePrivateWorkPath(PathBuf),
    #[error("snapshot private work directory is not empty at admission")]
    PrivateWorkDirectoryNotEmpty,
    #[error("snapshot resource cleanup exceeded its safety deadline")]
    CleanupTimedOut,
    #[error(transparent)]
    Lease(#[from] SnapshotBuildLeaseGuardError),
    #[error(transparent)]
    Progress(#[from] SnapshotBuildProgressError),
    #[error(transparent)]
    Coordinator(#[from] CoordinatorError),
    #[error(transparent)]
    Pressure(#[from] SnapshotPressureError),
    #[error(transparent)]
    WatchdogControl(#[from] SnapshotWatchdogError),
    #[error("snapshot PostgreSQL control query failed")]
    Postgres(#[from] tokio_postgres::Error),
    #[error(transparent)]
    Builder(#[from] SnapshotBuildStateError),
    #[error(transparent)]
    Barrier(#[from] BarrierWaitError),
    #[error(transparent)]
    Publication(#[from] SnapshotPublicationError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use canonical_log::{CanonicalPage, LogError};

    struct RecoveryProvider {
        epoch: SourceEpoch,
    }

    impl CanonicalBarrierRead for RecoveryProvider {
        fn maximum_barrier_read_duration(&self) -> Duration {
            Duration::from_millis(10)
        }

        fn source_epoch(&self) -> Result<SourceEpoch, LogError> {
            Ok(self.epoch.clone())
        }

        fn minimum_retained_sequence(&self) -> Result<u64, LogError> {
            unreachable!("recovery epoch preflight must not inspect retention")
        }

        fn durable_head_sequence(&self) -> Result<u64, LogError> {
            unreachable!("recovery epoch preflight must not inspect the head")
        }

        fn read_page_after_bounded(
            &self,
            _after: u64,
            _through: u64,
            _max_records: usize,
            _max_bytes: u64,
        ) -> Result<CanonicalPage, LogError> {
            unreachable!("recovery epoch preflight must not scan canonical data")
        }
    }

    #[tokio::test]
    async fn recovery_source_epoch_preflight_fails_closed() {
        let expected = SourceEpoch::new("epoch-expected").unwrap();
        let provider = Arc::new(Mutex::new(RecoveryProvider {
            epoch: SourceEpoch::new("epoch-other").unwrap(),
        }));

        assert!(matches!(
            verify_recovery_source_epoch(provider, &expected, Duration::from_secs(1)).await,
            Err(SnapshotBuildJobError::RecoverySourceEpochMismatch)
        ));
    }

    #[tokio::test]
    async fn cleanup_deadline_and_join_failure_keep_lifecycle_nonterminal() {
        assert!(cleanup_state_is_uncertain(
            &SnapshotBuildJobError::CleanupTimedOut
        ));
        let task = tokio::spawn(async { panic!("injected cleanup task failure") });
        let failure = task.await.unwrap_err();
        assert!(cleanup_state_is_uncertain(
            &SnapshotBuildJobError::CleanupTask(failure)
        ));
    }

    #[test]
    fn private_workspace_cleanup_is_recursive_durable_and_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let artifact = root
            .path()
            .join("private")
            .join("build-private-cleanup")
            .join(SNAPSHOT_ARTIFACT_FILE_NAME);
        prepare_private_work_dir(&artifact).unwrap();
        let directory = artifact.parent().unwrap();
        std::fs::write(&artifact, b"calque").unwrap();
        std::fs::write(directory.join("compressed-representation.tmp"), b"zstd").unwrap();
        std::fs::create_dir(directory.join("nested")).unwrap();
        std::fs::write(directory.join("nested").join("upload.tmp"), b"upload").unwrap();

        cleanup_private_work_dir(&artifact).unwrap();
        assert!(!directory.exists());
        cleanup_private_work_dir(&artifact).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn private_workspace_cleanup_rejects_symlink_entries() {
        let root = tempfile::tempdir().unwrap();
        let artifact = root
            .path()
            .join("private")
            .join("build-private-symlink")
            .join(SNAPSHOT_ARTIFACT_FILE_NAME);
        prepare_private_work_dir(&artifact).unwrap();
        let outside = root.path().join("outside");
        std::fs::write(&outside, b"preserve").unwrap();
        std::os::unix::fs::symlink(&outside, artifact.parent().unwrap().join("unsafe-link"))
            .unwrap();

        assert!(cleanup_private_work_dir(&artifact).is_err());
        assert_eq!(std::fs::read(outside).unwrap(), b"preserve");
    }
}
