use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use artifact_catalog::{PublishedArtifactQuery, SqliteArtifactCatalog};
use artifact_contract::{ArtifactCacheKey, ArtifactCompatibility, ValidatedArtifactManifest};
use blob_store::ManagedBlobStore;
use canonical_log::{PostgresFencedCanonicalLog, SnapshotBuildId, SnapshotBuildOwnerId};
use direct_columnar::EncodeError;
use pgoutput_decoder::DecoderLimits;
use sha2::{Digest, Sha256};
use snapshot_builder::{
    BoundedCopyLimits, CopyLoadError, CopyLoadLimits, DirectArtifactRequest,
    DirectColumnarCopyError, EncoderLimits, SnapshotApplyError, SnapshotBuildPlan,
    SnapshotBuildStateError, SnapshotRollForwardError, COLLATION_GENERATION_V1,
    NO_INDEX_PROFILE_HASH, PUBLICATION_GENERATION_V1, SOURCE_WIDE_SHARD_PROFILE_HASH,
    TYPE_CODEC_GENERATION_V1,
};
use snapshot_coordinator::{
    SnapshotPressurePolicy, SnapshotPressureThresholds, SnapshotRemainingWalThresholds,
    SnapshotStreamConfig,
};
use snapshot_pipeline::{
    BarrierWaitError, BarrierWaitRequest, CanonicalHeadSignal, PublicationAdmission,
    SnapshotBuildConnections, SnapshotBuildJobConfig, SnapshotBuildJobError, SnapshotBuildJournal,
    SnapshotBuildRecoveryOutcome, SnapshotBuildSafetyPolicy, SnapshotBuildStage,
    SnapshotBuildSupervisor, SnapshotPublicationReport, SnapshotPublicationRequest,
    SnapshotWatchdogConfig, SNAPSHOT_ARTIFACT_FILE_NAME,
};
use sync_types::ArtifactId;
use thiserror::Error;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    columnar::local_columnar_artifact_id,
    guard::PostgresConnectionFamily,
    source::replication_database_url,
    state::{recut_journal_root, snapshot_journal_root, snapshot_workspace_root},
    DatabaseUrl, OperationErrorClass, OperationsMetrics, PreparedLocalSource,
    LOCAL_ARTIFACT_GROWTH_RESERVE_BYTES, MAX_LOCAL_ARTIFACT_BYTES,
};

const LOCAL_BARRIER_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
// `wait_for_persistent_barrier` requires the polling interval to cover the
// provider's declared maximum synchronous read duration. The PostgreSQL
// canonical provider declares a three-second bound.
const LOCAL_POSTGRES_BARRIER_POLL_INTERVAL: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalSnapshotOutcome {
    Existing,
    Published,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalSnapshotPaths {
    journal_root: PathBuf,
    workspace_root: PathBuf,
}

impl LocalSnapshotPaths {
    fn new(state_root: &Path) -> Self {
        Self {
            journal_root: snapshot_journal_root(state_root),
            workspace_root: snapshot_workspace_root(state_root),
        }
    }

    fn recut(state_root: &Path) -> Self {
        Self {
            journal_root: recut_journal_root(state_root),
            workspace_root: snapshot_workspace_root(state_root),
        }
    }

    fn private_work_dir(&self, build_id: &SnapshotBuildId) -> PathBuf {
        self.workspace_root.join(build_id.as_str())
    }
}

/// What a build of a source's artifacts reads from and publishes through.
#[derive(Clone)]
pub struct ArtifactBuilder {
    pub database_url: DatabaseUrl,
    pub state_root: PathBuf,
    /// Opened afresh by each build started after startup, beside the handle
    /// bootstrap plans from, so a build that takes minutes never holds the
    /// planner's lock.
    pub catalog_path: PathBuf,
    pub store: Arc<dyn ManagedBlobStore>,
    /// The CDC runtime's barrier provider, which builds wait on.
    pub barrier_provider: Arc<Mutex<PostgresFencedCanonicalLog>>,
    pub head_signal: CanonicalHeadSignal,
    pub cancellation: CancellationToken,
    pub metrics: OperationsMetrics,
}

impl ArtifactBuilder {
    /// Builds and publishes one prepared scope's direct-columnar artifact, or finds
    /// the one already published for this source, schema generation, table set
    /// and decoder profile.
    pub async fn ensure_initial_snapshot(
        &self,
        prepared: &PreparedLocalSource,
        catalog: &mut SqliteArtifactCatalog,
    ) -> Result<LocalSnapshotOutcome, LocalSnapshotError> {
        let state_root = self.state_root.as_path();
        let plan = SnapshotBuildPlan::from_registry(&prepared.registry, &prepared.scope)
            .map_err(|_| LocalSnapshotError::InvalidPlan)?;
        let identity = stable_snapshot_identity(prepared, &plan);
        let query = published_query(prepared, &plan);
        if first_published(catalog, state_root, &query)?.is_some() {
            return Ok(LocalSnapshotOutcome::Existing);
        }

        let build_id = SnapshotBuildId::new(format!("local-build-{identity}"))
            .map_err(|_| LocalSnapshotError::InvalidConfiguration)?;
        let artifact_id = local_columnar_artifact_id(&identity)
            .map_err(|_| LocalSnapshotError::InvalidConfiguration)?;
        refuse_stored_artifact(catalog, state_root, &artifact_id)?;
        let paths = LocalSnapshotPaths::new(state_root);
        let private_work_dir = paths.private_work_dir(&build_id);
        if own_interrupted_build(&paths.journal_root, &build_id)? {
            refuse_foreign_artifact_build(
                state_root,
                &paths.journal_root,
                &build_id,
                &artifact_id,
            )?;
            let recovery = snapshot_supervisor(
                &self.database_url,
                &paths,
                prepared,
                &plan,
                build_id.clone(),
                artifact_id.clone(),
                random_owner_id()?,
                None,
                MAX_LOCAL_ARTIFACT_BYTES,
            )?;
            let outcome = recovery
                .recover_interrupted_local(Arc::clone(&self.barrier_provider), catalog, &self.store)
                .await
                .map_err(|_| LocalSnapshotError::Recovery)?;
            match outcome {
                SnapshotBuildRecoveryOutcome::WaitingForCatalogLease { .. } => {
                    return Err(LocalSnapshotError::RecoveryPending)
                }
                SnapshotBuildRecoveryOutcome::AlreadyTerminal
                | SnapshotBuildRecoveryOutcome::Published { .. }
                | SnapshotBuildRecoveryOutcome::InterruptedBuildCleaned => {}
            }
            if first_published(catalog, state_root, &query)?.is_some() {
                return Ok(LocalSnapshotOutcome::Existing);
            }
            SnapshotBuildJournal::remove_recovered_unpublished(
                &paths.journal_root,
                &build_id,
                &artifact_id,
                &private_work_dir,
                prepared.activated.source_epoch(),
            )
            .map_err(|_| LocalSnapshotError::Recovery)?;
        }
        let supervisor = snapshot_supervisor(
            &self.database_url,
            &paths,
            prepared,
            &plan,
            build_id,
            artifact_id,
            random_owner_id()?,
            None,
            MAX_LOCAL_ARTIFACT_BYTES,
        )?;
        self.run_build(prepared, plan, supervisor, catalog, &self.cancellation)
            .await?;
        Ok(LocalSnapshotOutcome::Published)
    }

    /// Runs one build to publication, and records its time.
    async fn run_build(
        &self,
        prepared: &PreparedLocalSource,
        plan: SnapshotBuildPlan,
        supervisor: SnapshotBuildSupervisor,
        catalog: &mut SqliteArtifactCatalog,
        cancellation: &CancellationToken,
    ) -> Result<ValidatedArtifactManifest, LocalSnapshotError> {
        let metrics = &self.metrics;
        let workers = plan.tables().len().min(2);
        let connections = snapshot_connections(&self.database_url, workers).await?;
        let build_started = Instant::now();
        let report = supervisor
            .run_controlled(
                Arc::clone(&self.barrier_provider),
                &self.head_signal,
                connections,
                &prepared.registry,
                &prepared.activated,
                plan,
                catalog,
                self.store.as_ref(),
                cancellation,
                |stage| {
                    tracing::info!(
                        target: "calque::preparation",
                        ?stage,
                        elapsed_ms = build_started.elapsed().as_millis() as u64,
                        "snapshot build stage entered"
                    );
                },
            )
            .await
            .map_err(|error| {
                if !cancellation.is_cancelled() {
                    metrics.record_error(OperationErrorClass::ArtifactBuild);
                }
                LocalSnapshotError::Build(error)
            })?;
        metrics.observe_artifact_build_seconds(build_started.elapsed().as_secs_f64());
        metrics.observe_artifact_publication_seconds(
            (report
                .finalization_micros
                .saturating_add(report.upload_and_publish_micros)) as f64
                / 1_000_000.0,
        );
        Ok(report.manifest)
    }

    /// Cuts `prepared`'s scope again at the canonical head and publishes the
    /// artifact beside those already published, provided `admission` holds when
    /// it would become visible. Its journal is removed once it is published or
    /// cleaned up; [`Self::recover_interrupted_recuts`] finds any other.
    pub(crate) async fn publish_recut(
        &self,
        prepared: &PreparedLocalSource,
        catalog: &mut SqliteArtifactCatalog,
        cancellation: &CancellationToken,
        admission: PublicationAdmission,
    ) -> Result<ValidatedArtifactManifest, LocalSnapshotError> {
        let plan = SnapshotBuildPlan::from_registry(&prepared.registry, &prepared.scope)
            .map_err(|_| LocalSnapshotError::InvalidPlan)?;
        let identity = format!(
            "{}-recut-{}",
            stable_snapshot_identity(prepared, &plan),
            &Uuid::new_v4().simple().to_string()[..12]
        );
        let build_id = SnapshotBuildId::new(format!("local-build-{identity}"))
            .map_err(|_| LocalSnapshotError::InvalidConfiguration)?;
        let artifact_id = local_columnar_artifact_id(&identity)
            .map_err(|_| LocalSnapshotError::InvalidConfiguration)?;
        let paths = LocalSnapshotPaths::recut(&self.state_root);
        let supervisor = snapshot_supervisor(
            &self.database_url,
            &paths,
            prepared,
            &plan,
            build_id.clone(),
            artifact_id.clone(),
            random_owner_id()?,
            Some(admission),
            MAX_LOCAL_ARTIFACT_BYTES,
        )?;
        let built = self
            .run_build(prepared, plan, supervisor, catalog, cancellation)
            .await;
        let removed = match &built {
            Ok(_) => {
                SnapshotBuildJournal::remove_published(&paths.journal_root, &build_id, &artifact_id)
            }
            Err(_) => SnapshotBuildJournal::remove_recovered_unpublished(
                &paths.journal_root,
                &build_id,
                &artifact_id,
                &paths.private_work_dir(&build_id),
                prepared.activated.source_epoch(),
            ),
        };
        if let Err(error) = removed {
            tracing::warn!(
                build = build_id.as_str(),
                %error,
                "the re-cut's journal is left for startup recovery"
            );
        }
        built
    }

    /// Reconciles every re-cut a previous process left unfinished, and removes
    /// the journal of each it published. Startup calls it before anything
    /// publishes, since recovery sweeps the store's staging. A build whose catalog
    /// lease is still live is left for the next start.
    pub async fn recover_interrupted_recuts(
        &self,
        prepared: &PreparedLocalSource,
        catalog: &mut SqliteArtifactCatalog,
    ) -> Result<(), LocalSnapshotError> {
        let plan = SnapshotBuildPlan::from_registry(&prepared.registry, &prepared.scope)
            .map_err(|_| LocalSnapshotError::InvalidPlan)?;
        let paths = LocalSnapshotPaths::recut(&self.state_root);
        for build_id in SnapshotBuildSupervisor::interrupted_builds(&paths.journal_root)
            .map_err(|_| LocalSnapshotError::Recovery)?
        {
            let progress = SnapshotBuildJournal::reopen(&paths.journal_root, &build_id)
                .map_err(|_| LocalSnapshotError::Recovery)?
                .progress()
                .clone();
            let artifact_id = ArtifactId::new(progress.artifact_id.clone())
                .map_err(|_| LocalSnapshotError::Recovery)?;
            if progress.stage == SnapshotBuildStage::Published {
                SnapshotBuildJournal::remove_published(
                    &paths.journal_root,
                    &build_id,
                    &artifact_id,
                )
                .map_err(|_| LocalSnapshotError::Recovery)?;
                continue;
            }
            let recovery = snapshot_supervisor(
                &self.database_url,
                &paths,
                prepared,
                &plan,
                build_id.clone(),
                artifact_id.clone(),
                random_owner_id()?,
                None,
                MAX_LOCAL_ARTIFACT_BYTES,
            )?;
            match recovery
                .recover_interrupted_local(Arc::clone(&self.barrier_provider), catalog, &self.store)
                .await
                .map_err(|_| LocalSnapshotError::Recovery)?
            {
                SnapshotBuildRecoveryOutcome::WaitingForCatalogLease { .. } => {
                    tracing::warn!(
                        build = build_id.as_str(),
                        "an interrupted re-cut waits for its catalog lease"
                    );
                    continue;
                }
                SnapshotBuildRecoveryOutcome::Published { .. } => {
                    SnapshotBuildJournal::remove_published(
                        &paths.journal_root,
                        &build_id,
                        &artifact_id,
                    )
                }
                SnapshotBuildRecoveryOutcome::AlreadyTerminal
                | SnapshotBuildRecoveryOutcome::InterruptedBuildCleaned => {
                    SnapshotBuildJournal::remove_recovered_unpublished(
                        &paths.journal_root,
                        &build_id,
                        &artifact_id,
                        &paths.private_work_dir(&build_id),
                        &progress.source_epoch,
                    )
                }
            }
            .map_err(|_| LocalSnapshotError::Recovery)?;
        }
        Ok(())
    }
}

/// Builds and publishes one artifact of `prepared`'s scope, a tenant's, under
/// a build of its own beside any artifact already published for the scope: a
/// tenant is cut again while its earlier cut still serves. `nonce` tells the
/// builds of one scope apart, and `decoded_bound` is the most decoded bytes
/// the artifact may hold.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_scope_artifact(
    database_url: &DatabaseUrl,
    state_root: &Path,
    journal_root: &Path,
    prepared: &PreparedLocalSource,
    nonce: &str,
    decoded_bound: u64,
    provider: Arc<Mutex<PostgresFencedCanonicalLog>>,
    signal: &CanonicalHeadSignal,
    catalog: &mut SqliteArtifactCatalog,
    store: &Arc<dyn ManagedBlobStore>,
    cancellation: &CancellationToken,
    on_stage: impl FnMut(SnapshotBuildStage),
) -> Result<SnapshotPublicationReport, LocalSnapshotError> {
    let plan = SnapshotBuildPlan::from_registry(&prepared.registry, &prepared.scope)
        .map_err(|_| LocalSnapshotError::InvalidPlan)?;
    let identity = format!("{}-{nonce}", stable_snapshot_identity(prepared, &plan));
    let build_id = SnapshotBuildId::new(format!("tenant-build-{identity}"))
        .map_err(|_| LocalSnapshotError::InvalidConfiguration)?;
    let artifact_id = local_columnar_artifact_id(&identity)
        .map_err(|_| LocalSnapshotError::InvalidConfiguration)?;
    let paths = LocalSnapshotPaths {
        journal_root: journal_root.to_path_buf(),
        workspace_root: snapshot_workspace_root(state_root),
    };
    let supervisor = snapshot_supervisor(
        database_url,
        &paths,
        prepared,
        &plan,
        build_id.clone(),
        artifact_id,
        random_owner_id()?,
        None,
        decoded_bound,
    )?;
    let workers = plan.tables().len().min(2);
    let connections = snapshot_connections(database_url, workers).await?;
    let report = supervisor
        .run_controlled(
            provider,
            signal,
            connections,
            &prepared.registry,
            &prepared.activated,
            plan,
            catalog,
            store.as_ref(),
            cancellation,
            on_stage,
        )
        .await
        .map_err(LocalSnapshotError::Build);
    // A build that ended leaves nothing to recover. One whose cleanup did not
    // finish keeps its journal for the next start's recovery.
    let _ = SnapshotBuildJournal::remove_ended(journal_root, &build_id);
    report
}

/// Recovers, or cleans, every build a journal under `journal_root` names,
/// and removes its journal. It runs at startup before any build: recovery
/// clears the store's staging area, which every build shares.
pub(crate) async fn recover_scope_builds(
    database_url: &DatabaseUrl,
    state_root: &Path,
    journal_root: &Path,
    prepared: &PreparedLocalSource,
    provider: Arc<Mutex<PostgresFencedCanonicalLog>>,
    catalog: &mut SqliteArtifactCatalog,
    store: &Arc<dyn ManagedBlobStore>,
) -> Result<usize, LocalSnapshotError> {
    let plan = SnapshotBuildPlan::from_registry(&prepared.registry, &prepared.scope)
        .map_err(|_| LocalSnapshotError::InvalidPlan)?;
    let paths = LocalSnapshotPaths {
        journal_root: journal_root.to_path_buf(),
        workspace_root: snapshot_workspace_root(state_root),
    };
    let builds = SnapshotBuildSupervisor::interrupted_builds(journal_root)
        .map_err(|_| LocalSnapshotError::Recovery)?;
    for build_id in &builds {
        let artifact_id = SnapshotBuildJournal::reopen(journal_root, build_id)
            .ok()
            .and_then(|journal| ArtifactId::new(journal.progress().artifact_id.clone()).ok())
            .ok_or(LocalSnapshotError::Recovery)?;
        let supervisor = snapshot_supervisor(
            database_url,
            &paths,
            prepared,
            &plan,
            build_id.clone(),
            artifact_id,
            random_owner_id()?,
            None,
            MAX_LOCAL_ARTIFACT_BYTES,
        )?;
        loop {
            let outcome = supervisor
                .recover_interrupted_local(Arc::clone(&provider), catalog, store)
                .await
                .map_err(|_| LocalSnapshotError::Recovery)?;
            let SnapshotBuildRecoveryOutcome::WaitingForCatalogLease {
                retry_at_unix_millis,
            } = outcome
            else {
                break;
            };
            let now = OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
            let wait = u64::try_from(i128::from(retry_at_unix_millis) - now).unwrap_or(0);
            tokio::time::sleep(Duration::from_millis(wait.clamp(100, 60_000))).await;
        }
        SnapshotBuildJournal::remove_ended(journal_root, build_id)
            .map_err(|_| LocalSnapshotError::Recovery)?;
    }
    Ok(builds.len())
}

/// [`published_query`] for `prepared`'s own plan.
pub(crate) fn published_query_for(
    prepared: &PreparedLocalSource,
) -> Result<PublishedArtifactQuery, LocalSnapshotError> {
    let plan = SnapshotBuildPlan::from_registry(&prepared.registry, &prepared.scope)
        .map_err(|_| LocalSnapshotError::InvalidPlan)?;
    Ok(published_query(prepared, &plan))
}

/// The catalog query that finds one prepared scope's published artifact: the
/// scope's source-wide cache key under the direct-columnar decoder profile.
fn published_query(
    prepared: &PreparedLocalSource,
    plan: &SnapshotBuildPlan,
) -> PublishedArtifactQuery {
    let authorization = plan.authorization_identity();
    PublishedArtifactQuery {
        cache_key: ArtifactCacheKey {
            source_epoch: prepared.activated.source_epoch().clone(),
            publication_generation: PUBLICATION_GENERATION_V1,
            schema_generation: prepared.registry.schema_generation(),
            authorization_generation: authorization.generation(),
            authorization_policy_hash: authorization.policy_hash().to_string(),
            authorization_scope_hash: authorization.scope_hash().to_string(),
            shard_profile_hash: SOURCE_WIDE_SHARD_PROFILE_HASH.to_owned(),
            index_profile_hash: NO_INDEX_PROFILE_HASH.to_owned(),
            decoder_profile: crate::columnar::direct_columnar_decoder_profile(),
            type_codec_generation: TYPE_CODEC_GENERATION_V1,
            collation_generation: COLLATION_GENERATION_V1,
        },
        table_set_hash: plan.table_set_hash().to_owned(),
    }
}

/// The first artifact the catalog under `state_root` has published for
/// `query`.
///
/// A manifest this build publishes has passed this build's contract before it
/// is stored, so a stored one the contract refuses was written by another
/// build, or damaged. That is reported as the state directory's problem, which
/// only `calque destroy` gets past, rather than as an unavailable catalog.
pub(crate) fn first_published(
    catalog: &mut SqliteArtifactCatalog,
    state_root: &Path,
    query: &PublishedArtifactQuery,
) -> Result<Option<ValidatedArtifactManifest>, LocalSnapshotError> {
    let published = catalog.published(query).map_err(|error| match error {
        artifact_catalog::CatalogError::Manifest(refusal) => {
            LocalSnapshotError::StoredManifestRefused {
                state_dir: state_root.to_path_buf(),
                refusal,
            }
        }
        _ => LocalSnapshotError::Catalog,
    })?;
    Ok(published.into_iter().next())
}

/// Refuses `state_root` when the catalog holds, under `artifact_id`, an
/// artifact whose manifest this build refuses.
///
/// A lookup by cache key does not select such a row when the key names
/// another format version -- a direct-columnar key carries the decoder
/// profile -- so [`first_published`] misses it, and the build would go on to
/// register `artifact_id` again and fail on a manifest conflict that names no
/// remedy.
fn refuse_stored_artifact(
    catalog: &SqliteArtifactCatalog,
    state_root: &Path,
    artifact_id: &ArtifactId,
) -> Result<(), LocalSnapshotError> {
    match catalog.manifest(artifact_id) {
        Ok(_) => Ok(()),
        Err(artifact_catalog::CatalogError::Manifest(refusal)) => {
            Err(LocalSnapshotError::StoredManifestRefused {
                state_dir: state_root.to_path_buf(),
                refusal,
            })
        }
        Err(_) => Err(LocalSnapshotError::Catalog),
    }
}

/// Whether this build identity left a journal behind, after refusing any
/// other identity's build that did not finish.
///
/// A published build keeps its journal, so a deployment whose authorization
/// generation moved finds its previous generation's build here under another
/// identity. That build holds nothing a new one could strand, so only a
/// foreign build that never published is refused.
fn own_interrupted_build(
    journal_root: &Path,
    build_id: &SnapshotBuildId,
) -> Result<bool, LocalSnapshotError> {
    let mut own = false;
    for candidate in SnapshotBuildSupervisor::interrupted_builds(journal_root)
        .map_err(|_| LocalSnapshotError::Recovery)?
    {
        if &candidate == build_id {
            own = true;
            continue;
        }
        let published = SnapshotBuildJournal::reopen(journal_root, &candidate)
            .map_err(|_| LocalSnapshotError::Recovery)?
            .progress()
            .stage
            == snapshot_pipeline::SnapshotBuildStage::Published;
        if !published {
            return Err(LocalSnapshotError::ForeignInterruptedBuild);
        }
    }
    Ok(own)
}

/// Refuses this build identity's interrupted journal when it was writing an
/// artifact other than `artifact_id`.
///
/// Only a supervisor for the journaled artifact may recover a build, so a
/// journal under this build id that names another artifact fails closed rather
/// than being resumed as this one.
fn refuse_foreign_artifact_build(
    state_root: &Path,
    journal_root: &Path,
    build_id: &SnapshotBuildId,
    artifact_id: &ArtifactId,
) -> Result<(), LocalSnapshotError> {
    let journaled = SnapshotBuildJournal::reopen(journal_root, build_id)
        .map_err(|_| LocalSnapshotError::Recovery)?
        .progress()
        .artifact_id
        .clone();
    if journaled == artifact_id.as_str() {
        Ok(())
    } else {
        Err(LocalSnapshotError::ForeignArtifactBuild {
            state_dir: state_root.to_path_buf(),
            artifact: journaled,
        })
    }
}

fn random_owner_id() -> Result<SnapshotBuildOwnerId, LocalSnapshotError> {
    SnapshotBuildOwnerId::new(format!("local-owner-{}", Uuid::new_v4().simple()))
        .map_err(|_| LocalSnapshotError::InvalidConfiguration)
}

#[allow(clippy::too_many_arguments)]
fn snapshot_supervisor(
    database_url: &DatabaseUrl,
    paths: &LocalSnapshotPaths,
    prepared: &PreparedLocalSource,
    plan: &SnapshotBuildPlan,
    build_id: SnapshotBuildId,
    artifact_id: ArtifactId,
    owner_id: SnapshotBuildOwnerId,
    admission: Option<PublicationAdmission>,
    decoded_bound: u64,
) -> Result<SnapshotBuildSupervisor, LocalSnapshotError> {
    let authorization = plan.authorization_identity();
    let workers = plan.tables().len().min(2);
    let copy_limits = BoundedCopyLimits {
        table: CopyLoadLimits {
            max_rows: 10_000_000,
            max_value_bytes: 16 * 1024 * 1024,
            max_row_bytes: 64 * 1024 * 1024,
            max_decoded_value_bytes: decoded_bound,
            max_batch_bytes: 16 * 1024 * 1024,
            batch_rows: 2_048,
        },
        max_workers: workers,
        max_inflight_batch_bytes: (workers as u64) * 16 * 1024 * 1024,
    };
    let pressure = SnapshotPressurePolicy::new(
        SnapshotPressureThresholds::new(512 * 1024 * 1024, 1024 * 1024 * 1024)
            .map_err(|_| LocalSnapshotError::InvalidConfiguration)?,
        SnapshotPressureThresholds::new(100_000_000, 200_000_000)
            .map_err(|_| LocalSnapshotError::InvalidConfiguration)?,
        Duration::from_secs(120),
        Duration::from_secs(300),
        SnapshotRemainingWalThresholds::new(256 * 1024 * 1024, 128 * 1024 * 1024)
            .map_err(|_| LocalSnapshotError::InvalidConfiguration)?,
    )
    .map_err(|_| LocalSnapshotError::InvalidConfiguration)?;
    let safety = SnapshotBuildSafetyPolicy::new(
        SnapshotWatchdogConfig::new(Duration::from_secs(600), Duration::from_secs(60))
            .map_err(|_| LocalSnapshotError::InvalidConfiguration)?,
        Duration::from_secs(2),
        Duration::from_secs(2),
        pressure,
    )
    .map_err(|_| LocalSnapshotError::InvalidConfiguration)?;
    let created_at = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .map_err(|_| LocalSnapshotError::Clock)?;
    let replication_connection = replication_database_url(database_url)
        .map_err(|_| LocalSnapshotError::InvalidConfiguration)?;
    SnapshotBuildSupervisor::new(SnapshotBuildJobConfig {
        build_id: build_id.clone(),
        owner_id: owner_id.clone(),
        journal_root: paths.journal_root.clone(),
        artifact_path: paths
            .workspace_root
            .join(build_id.as_str())
            .join(SNAPSHOT_ARTIFACT_FILE_NAME),
        encoder_limits: EncoderLimits {
            max_artifact_bytes: decoded_bound,
            ..EncoderLimits::default()
        },
        replication_connection,
        stream: SnapshotStreamConfig::new(
            format!("calque_snapshot_{}", Uuid::new_v4().simple()),
            prepared.registry.publication().name.clone(),
        )
        .map_err(|_| LocalSnapshotError::InvalidConfiguration)?,
        retention_ttl: Duration::from_secs(30),
        retention_renewal_interval: Duration::from_secs(5),
        copy_limits,
        roll_forward_timeout: Duration::from_secs(30),
        decoder_limits: DecoderLimits::default(),
        barrier_wait: local_barrier_wait_request(),
        safety,
        registry_revalidation: database_url
            .endpoint(PostgresConnectionFamily::SnapshotRegistry)
            .map_err(|_| LocalSnapshotError::InvalidConfiguration)?,
        publication: SnapshotPublicationRequest {
            artifact: DirectArtifactRequest {
                artifact_id,
                created_at,
                compatibility: ArtifactCompatibility {
                    publication_generation: PUBLICATION_GENERATION_V1,
                    schema_generation: prepared.registry.schema_generation(),
                    authorization_generation: authorization.generation(),
                    authorization_policy_hash: authorization.policy_hash().to_string(),
                    authorization_scope_hash: authorization.scope_hash().to_string(),
                    table_set_hash: plan.table_set_hash().to_owned(),
                    shard_profile_hash: SOURCE_WIDE_SHARD_PROFILE_HASH.to_owned(),
                    index_profile_hash: NO_INDEX_PROFILE_HASH.to_owned(),
                    type_codec_generation: TYPE_CODEC_GENERATION_V1,
                    collation_generation: COLLATION_GENERATION_V1,
                },
            },
            growth_reserve_bytes: LOCAL_ARTIFACT_GROWTH_RESERVE_BYTES,
            lease_owner: owner_id.as_str().to_owned(),
            lease_duration_ms: 30_000,
            catalog_lease_renewal_interval: Duration::from_secs(5),
            publication_cleanup_timeout: Duration::from_secs(30),
            max_encoded_representation_bytes: MAX_LOCAL_ARTIFACT_BYTES,
            admission,
        },
    })
    .map_err(|_| LocalSnapshotError::InvalidConfiguration)
}

fn local_barrier_wait_request() -> BarrierWaitRequest {
    BarrierWaitRequest {
        timeout: LOCAL_BARRIER_WAIT_TIMEOUT,
        poll_interval: LOCAL_POSTGRES_BARRIER_POLL_INTERVAL,
        max_records: 10_000,
        max_bytes: 64 * 1024 * 1024,
    }
}

async fn snapshot_connections(
    database_url: &DatabaseUrl,
    workers: usize,
) -> Result<SnapshotBuildConnections, LocalSnapshotError> {
    let control = connect_owned(database_url, PostgresConnectionFamily::SnapshotControl).await?;
    let ddl_guard = connect_owned(database_url, PostgresConnectionFamily::SnapshotDdlGuard).await?;
    let pressure = connect_owned(database_url, PostgresConnectionFamily::SnapshotPressure).await?;
    let mut copy_workers = Vec::with_capacity(workers);
    for _ in 0..workers {
        copy_workers
            .push(connect_owned(database_url, PostgresConnectionFamily::SnapshotCopy).await?);
    }
    Ok(SnapshotBuildConnections {
        control,
        ddl_guard: Some(ddl_guard),
        copy_workers,
        pressure: Some(pressure),
    })
}

async fn connect_owned(
    database_url: &DatabaseUrl,
    family: PostgresConnectionFamily,
) -> Result<tokio_postgres::Client, LocalSnapshotError> {
    let mut endpoint = database_url
        .endpoint(family)
        .map_err(|_| LocalSnapshotError::Connection)?;
    endpoint.config_mut().application_name("calque-snapshot");
    let (client, connection) = endpoint
        .connect_async()
        .await
        .map_err(|_| LocalSnapshotError::Connection)?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

/// The build and artifact id one prepared scope owns.
///
/// The authorization identity is part of it so that two scopes over one source
/// epoch, schema generation and table set never share a build journal, a
/// private work directory, or an `ArtifactId`.
pub(crate) fn stable_snapshot_identity(
    prepared: &PreparedLocalSource,
    plan: &SnapshotBuildPlan,
) -> String {
    let authorization = plan.authorization_identity();
    let mut digest = Sha256::new();
    digest.update(b"calque.local-snapshot.v2\0");
    digest.update(prepared.activated.source_epoch().as_str().as_bytes());
    digest.update([0]);
    digest.update(prepared.registry.schema_generation().to_be_bytes());
    digest.update([0]);
    digest.update(plan.table_set_hash().as_bytes());
    digest.update([0]);
    digest.update(authorization.generation().to_be_bytes());
    digest.update(authorization.policy_hash().as_bytes());
    digest.update(authorization.scope_hash().as_bytes());
    hex::encode(digest.finalize())[..24].to_owned()
}

#[derive(Debug, Error)]
pub enum LocalSnapshotError {
    #[error("local snapshot configuration is invalid")]
    InvalidConfiguration,
    #[error("local snapshot table plan is invalid")]
    InvalidPlan,
    #[error("local snapshot catalog is unavailable")]
    Catalog,
    #[error("{state_dir} holds a published artifact whose manifest this build of calque refuses ({refusal}); another build wrote it, or it is damaged, and calque does not migrate it.")]
    StoredManifestRefused {
        state_dir: std::path::PathBuf,
        refusal: artifact_contract::ManifestError,
    },
    #[error("local snapshot PostgreSQL connection failed")]
    Connection,
    #[error(transparent)]
    Guard(#[from] crate::GuardError),
    #[error("local snapshot recovery failed")]
    Recovery,
    #[error("another interrupted local snapshot requires cleanup")]
    ForeignInterruptedBuild,
    #[error("{state_dir} holds an interrupted build of {artifact}, an artifact this build of calque does not make, and calque does not migrate it.")]
    ForeignArtifactBuild {
        state_dir: std::path::PathBuf,
        artifact: String,
    },
    #[error("local snapshot recovery is waiting for a live cleanup lease")]
    RecoveryPending,
    #[error("local snapshot build failed: {0}")]
    Build(#[source] snapshot_pipeline::SnapshotBuildJobError),
    #[error("local snapshot clock value is invalid")]
    Clock,
}

impl LocalSnapshotError {
    /// Whether the build stopped at a bound on what one artifact may hold:
    /// its rows, its decoded bytes, or an encoder limit. Such a scope is
    /// refused whole rather than built in part.
    pub(crate) fn exceeds_bound(&self) -> bool {
        let encoder = |error: &EncodeError| {
            matches!(
                error,
                EncodeError::RowLimit(_)
                    | EncodeError::TableRowLimit(_)
                    | EncodeError::VariableColumnLimit { .. }
                    | EncodeError::ArtifactLimit(_)
            )
        };
        let Self::Build(SnapshotBuildJobError::Builder(error)) = self else {
            return false;
        };
        match error {
            SnapshotBuildStateError::Copy(
                CopyLoadError::RowLimitExceeded { .. }
                | CopyLoadError::DecodedByteLimitExceeded { .. },
            ) => true,
            SnapshotBuildStateError::DirectSchema(DirectColumnarCopyError::Encode(error))
            | SnapshotBuildStateError::Encode(error) => encoder(error),
            _ => false,
        }
    }

    /// Whether a row moved into or out of the build's partition between its
    /// snapshot and its cut, which no build of that cut can hold.
    pub(crate) fn partition_moved(&self) -> bool {
        matches!(
            self,
            Self::Build(SnapshotBuildJobError::Builder(
                SnapshotBuildStateError::RollForward(SnapshotRollForwardError::Stream(
                    snapshot_coordinator::RollForwardError::Apply(
                        SnapshotApplyError::Authorization(
                            authz::CanonicalAuthorizationError::PartitionMoved
                        )
                    )
                ))
            ))
        )
    }

    /// Whether only `calque destroy` gets past this refusal: a stored artifact
    /// or an interrupted build this build of calque refuses, or a canonical
    /// log entry an earlier release wrote, which the build's barrier reads.
    /// The build lease's log errors come from retention-lease rows, which hold
    /// no entry.
    pub fn requires_destroying_state(&self) -> bool {
        match self {
            Self::StoredManifestRefused { .. } | Self::ForeignArtifactBuild { .. } => true,
            Self::Build(SnapshotBuildJobError::Barrier(BarrierWaitError::CanonicalLog(error))) => {
                error.requires_destroying_state()
            }
            _ => false,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::process::Command;

    use artifact_catalog::{ArtifactState, PublishOutcome};
    use artifact_contract::{RepresentationEncoding, ValidatedArtifactManifest};
    use authz::{AuthorizationPolicy, PolicyDefinition, TablePermitDefinition};
    use blob_store::{ByteStream, ObjectMetadata};
    use canonical_log::FileCanonicalLog;
    use snapshot_pipeline::{SnapshotBuildProgressError, SnapshotBuildStage};
    use source_control::{ActivatedSource, PgoutputLaunchProfile};
    use sync_types::{CursorCodec, SourceEpoch, SourceId};
    use tokio::io::AsyncWriteExt;

    use super::*;

    pub(crate) fn prepared_source(epoch: SourceEpoch) -> PreparedLocalSource {
        prepared_source_for(epoch, crate::test_support::registry())
    }

    pub(crate) fn prepared_source_for(
        epoch: SourceEpoch,
        registry: source_control::SchemaRegistry,
    ) -> PreparedLocalSource {
        let policy = AuthorizationPolicy::compile_for_registry(
            PolicyDefinition {
                generation: 1,
                tables: registry
                    .tables()
                    .iter()
                    .map(|table| TablePermitDefinition {
                        table_id: table.table_id.clone(),
                        permitted_columns: table.column_ids.clone(),
                    })
                    .collect(),
            },
            &registry,
        )
        .unwrap();
        let scope = policy.scope();
        let activated = ActivatedSource::new(
            &registry,
            epoch,
            CursorCodec::new([7_u8; 32]).unwrap(),
            PgoutputLaunchProfile::V1,
        );
        PreparedLocalSource {
            registry,
            activated,
            policy,
            scope,
        }
    }

    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
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

    fn byte_stream(bytes: &[u8]) -> ByteStream {
        let (mut writer, reader) = tokio::io::duplex(bytes.len().max(1));
        let bytes = bytes.to_vec();
        tokio::spawn(async move {
            writer.write_all(&bytes).await.unwrap();
        });
        Box::pin(reader)
    }

    /// A published manifest over `bytes`, one row per table. The catalog paths
    /// that use it never decode the bytes, so its schema hash and chunk
    /// digests are placeholders.
    pub(crate) fn retry_manifest(
        bytes: &[u8],
        artifact_id: &ArtifactId,
        prepared: &PreparedLocalSource,
        plan: &SnapshotBuildPlan,
    ) -> ValidatedArtifactManifest {
        let zstd = encoded_fixture(RepresentationEncoding::Zstd, bytes);
        let decoded_digest = digest(bytes);
        let zstd_digest = digest(&zstd);
        let authorization = plan.authorization_identity();
        let tables = plan
            .tables()
            .iter()
            .map(|table| table.artifact_table(1))
            .collect::<Vec<_>>();
        let schema_tables = tables
            .iter()
            .map(|table| {
                serde_json::json!({
                    "name": table.local_table,
                    "primaryKey": [table.columns[0].name],
                    "columns": table
                        .columns
                        .iter()
                        .map(|column| serde_json::json!({
                            "name": column.name,
                            "type": "utf8",
                            "nullable": false
                        }))
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>();
        let value = serde_json::json!({
            "protocolVersion": 1,
            "artifactId": artifact_id.as_str(),
            "createdAt": "2026-07-19T12:00:00Z",
            "decoded": {
                "objectId": format!("sha256:{decoded_digest}"),
                "sha256": decoded_digest,
                "bytes": bytes.len(),
                "mediaType": artifact_contract::DIRECT_COLUMNAR_MEDIA_TYPE,
                "format": "directColumnar",
                "schemaHash": format!("sha256:{}", "a".repeat(64)),
                "collectionSetHash": format!("sha256:{}", "b".repeat(64)),
                "formatVersion": 4,
                "identity": {
                    "manifestVersion": "local-retry-manifest",
                    "authorizationScopeVersion": "local-retry-scope"
                },
                "schema": {
                    "format": "calque-direct-columnar",
                    "version": 4,
                    "tables": schema_tables
                },
                "chunks": {
                    "bytes": artifact_contract::DIRECT_COLUMNAR_CHUNK_BYTES,
                    "sha256": [format!("sha256:{decoded_digest}")]
                }
            },
            "pit": {
                "sourceEpoch": prepared.activated.source_epoch(),
                "snapshotCursor": "local-retry-cursor",
                "copyConsistentLsn": "0/A",
                "cutMessageLsn": "0/B",
                "cutMarkerId": "local-abrupt-restart-build"
            },
            "compatibility": {
                "publicationGeneration": PUBLICATION_GENERATION_V1,
                "schemaGeneration": prepared.registry.schema_generation(),
                "authorizationGeneration": authorization.generation(),
                "authorizationPolicyHash": authorization.policy_hash().to_string(),
                "authorizationScopeHash": authorization.scope_hash().to_string(),
                "tableSetHash": plan.table_set_hash(),
                "shardProfileHash": SOURCE_WIDE_SHARD_PROFILE_HASH,
                "indexProfileHash": NO_INDEX_PROFILE_HASH,
                "typeCodecGeneration": TYPE_CODEC_GENERATION_V1,
                "collationGeneration": COLLATION_GENERATION_V1
            },
            "tables": tables,
            "representations": [
                {
                    "representationId": format!("sha256:{zstd_digest}"),
                    "objectId": format!("sha256:{zstd_digest}"),
                    "encoding": "zstd",
                    "encodedSha256": zstd_digest,
                    "encodedBytes": zstd.len(),
                    "zstd": { "level": 3, "windowLog": 23 }
                },
                {
                    "representationId": format!("sha256:{decoded_digest}"),
                    "objectId": format!("sha256:{decoded_digest}"),
                    "encoding": "identity",
                    "encodedSha256": decoded_digest,
                    "encodedBytes": bytes.len()
                }
            ],
            "memory": {
                "estimatedPeakBytes": 4096,
                "recommendedGrowthReserveBytes": 1024
            }
        });
        ValidatedArtifactManifest::parse(&serde_json::to_vec(&value).unwrap()).unwrap()
    }

    pub(crate) async fn upload_retry_objects(
        store: &Arc<dyn ManagedBlobStore>,
        manifest: &ValidatedArtifactManifest,
        decoded: &[u8],
    ) {
        let decoded_metadata = ObjectMetadata::new(decoded.len() as u64, digest(decoded)).unwrap();
        store
            .put_if_absent(
                &manifest.manifest().decoded.object_id,
                byte_stream(decoded),
                &decoded_metadata,
            )
            .await
            .unwrap();
        for representation in &manifest.manifest().representations {
            let encoded = encoded_fixture(representation.encoding, decoded);
            let metadata = ObjectMetadata::new(encoded.len() as u64, digest(&encoded)).unwrap();
            store
                .put_if_absent(&representation.object_id, byte_stream(&encoded), &metadata)
                .await
                .unwrap();
        }
    }

    #[test]
    fn local_snapshot_limits_are_bounded() {
        assert_eq!(MAX_LOCAL_ARTIFACT_BYTES, 1280 * 1024 * 1024);
        let barrier = local_barrier_wait_request();
        assert_eq!(barrier.timeout, Duration::from_secs(30));
        assert_eq!(barrier.poll_interval, Duration::from_secs(3));
        assert!(barrier.poll_interval <= barrier.timeout);
    }

    /// A published build keeps its journal. When the authorization generation
    /// moves, the next build has another identity and finds that journal; it
    /// must build, while a foreign build that never finished still stops it.
    #[test]
    fn only_an_unfinished_foreign_build_blocks_a_new_identity() {
        let temporary = tempfile::tempdir().unwrap();
        let state = crate::StateStore::open(temporary.path().join("state")).unwrap();
        let paths = LocalSnapshotPaths::new(state.root());
        let previous = SnapshotBuildId::new("local-build-generation-1").unwrap();
        let next = SnapshotBuildId::new("local-build-generation-2").unwrap();
        let mut journal = SnapshotBuildJournal::create(
            &paths.journal_root,
            &previous,
            &SnapshotBuildOwnerId::new("local-owner-1").unwrap(),
            &ArtifactId::new("local-artifact-generation-1").unwrap(),
            &paths.private_work_dir(&previous),
            SourceEpoch::new("epoch-1").unwrap(),
            1,
            1_000,
        )
        .unwrap();
        assert!(matches!(
            own_interrupted_build(&paths.journal_root, &next),
            Err(LocalSnapshotError::ForeignInterruptedBuild)
        ));
        journal.reconcile_published(2_000).unwrap();
        assert!(!own_interrupted_build(&paths.journal_root, &next).unwrap());
        assert!(own_interrupted_build(&paths.journal_root, &previous).unwrap());
    }

    /// An interrupted journal under this build id names another artifact,
    /// which no supervisor here can recover, so the start is refused, naming
    /// the state directory, as a refusal only `calque destroy` gets past.
    #[test]
    fn an_interrupted_build_of_another_artifact_is_refused_naming_its_state_directory() {
        let temporary = tempfile::tempdir().unwrap();
        let state = crate::StateStore::open(temporary.path().join("state")).unwrap();
        let paths = LocalSnapshotPaths::new(state.root());
        let build_id = SnapshotBuildId::new("local-build-earlier-release").unwrap();
        let image = ArtifactId::new("local-artifact-earlier-release").unwrap();
        let direct = local_columnar_artifact_id("earlier-release").unwrap();
        SnapshotBuildJournal::create(
            &paths.journal_root,
            &build_id,
            &SnapshotBuildOwnerId::new("local-owner-earlier").unwrap(),
            &image,
            &paths.private_work_dir(&build_id),
            SourceEpoch::new("epoch-earlier").unwrap(),
            1,
            1_000,
        )
        .unwrap();

        let refusal =
            refuse_foreign_artifact_build(state.root(), &paths.journal_root, &build_id, &direct)
                .unwrap_err();
        assert!(matches!(
            &refusal,
            LocalSnapshotError::ForeignArtifactBuild { artifact, .. }
                if artifact == image.as_str()
        ));
        assert!(refusal.requires_destroying_state());
        assert_eq!(
            refusal.to_string(),
            format!(
                "{} holds an interrupted build of {}, an artifact this build of calque does not make, and calque does not migrate it.",
                state.root().display(),
                image.as_str()
            )
        );
        refuse_foreign_artifact_build(state.root(), &paths.journal_root, &build_id, &image)
            .unwrap();
    }

    /// A refused stored artifact, a foreign interrupted build, and a canonical
    /// log entry an earlier release wrote, met by the build's barrier, each
    /// need `calque destroy`. A lost PostgreSQL connection, whether the
    /// snapshot's own or the log's behind the barrier, an unavailable catalog
    /// and a barrier that timed out do not.
    #[test]
    fn only_refused_snapshot_state_requires_destroying_it() {
        let state_dir = std::path::PathBuf::from("/srv/calque/state");
        let barrier = |error| LocalSnapshotError::Build(SnapshotBuildJobError::Barrier(error));
        for error in [
            LocalSnapshotError::StoredManifestRefused {
                state_dir: state_dir.clone(),
                refusal: artifact_contract::ManifestError::SchemaDefinition("refused".to_owned()),
            },
            LocalSnapshotError::ForeignArtifactBuild {
                state_dir: state_dir.clone(),
                artifact: "local-artifact-earlier-release".to_owned(),
            },
            barrier(BarrierWaitError::CanonicalLog(
                canonical_log::LogError::EarlierEntryFormat {
                    state_dir: state_dir.clone(),
                },
            )),
        ] {
            assert!(error.requires_destroying_state(), "{error}");
        }
        for error in [
            LocalSnapshotError::Connection,
            LocalSnapshotError::Catalog,
            barrier(BarrierWaitError::Timeout),
            barrier(BarrierWaitError::CanonicalLog(
                canonical_log::LogError::PostgresUnavailable,
            )),
        ] {
            assert!(!error.requires_destroying_state(), "{error}");
        }
    }

    #[tokio::test]
    async fn abrupt_restart_scans_recovers_and_retries_with_separate_namespaces() {
        const CHILD_ROOT: &str = "CALQUE_LOCAL_ABRUPT_SNAPSHOT_CHILD_ROOT";
        let build_id = SnapshotBuildId::new("local-abrupt-restart-build").unwrap();
        let artifact_id = ArtifactId::new("local-abrupt-restart-artifact").unwrap();
        let epoch = SourceEpoch::new("local-abrupt-restart-epoch").unwrap();

        if let Some(root) = std::env::var_os(CHILD_ROOT) {
            let state = crate::StateStore::open(std::path::PathBuf::from(root)).unwrap();
            let paths = LocalSnapshotPaths::new(state.root());
            let private_work_dir = paths.private_work_dir(&build_id);
            let journal = SnapshotBuildJournal::create(
                &paths.journal_root,
                &build_id,
                &SnapshotBuildOwnerId::new("local-abrupt-restart-owner").unwrap(),
                &artifact_id,
                &private_work_dir,
                epoch,
                1,
                1_000,
            )
            .unwrap();
            std::fs::create_dir(&private_work_dir).unwrap();
            std::fs::write(
                private_work_dir.join(SNAPSHOT_ARTIFACT_FILE_NAME),
                b"interrupted private artifact",
            )
            .unwrap();
            std::fs::write(
                private_work_dir.join("compressed-representation.tmp"),
                b"residue",
            )
            .unwrap();
            std::mem::forget(journal);
            std::process::exit(86);
        }

        let temporary = tempfile::tempdir().unwrap();
        let state_root = temporary.path().join("state");
        let child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("snapshot::tests::abrupt_restart_scans_recovers_and_retries_with_separate_namespaces")
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(CHILD_ROOT, &state_root)
            .status()
            .unwrap();
        assert_eq!(child.code(), Some(86));

        let state = crate::StateStore::open(&state_root).unwrap();
        let paths = LocalSnapshotPaths::new(state.root());
        assert_ne!(paths.journal_root, paths.workspace_root);

        let prepared = prepared_source(epoch.clone());
        let plan = SnapshotBuildPlan::from_registry(&prepared.registry, &prepared.scope).unwrap();
        let owner_id = SnapshotBuildOwnerId::new("local-abrupt-restart-owner").unwrap();
        let private_work_dir = paths.private_work_dir(&build_id);
        let supervisor = snapshot_supervisor(
            &DatabaseUrl::parse("postgresql:///calque?host=%2Ftmp").unwrap(),
            &paths,
            &prepared,
            &plan,
            build_id.clone(),
            artifact_id.clone(),
            owner_id.clone(),
            None,
            MAX_LOCAL_ARTIFACT_BYTES,
        )
        .unwrap();

        assert_eq!(
            SnapshotBuildSupervisor::interrupted_builds(&paths.journal_root).unwrap(),
            vec![build_id.clone()]
        );
        assert!(matches!(
            SnapshotBuildJournal::scan(&paths.workspace_root),
            Err(SnapshotBuildProgressError::UnsafeStatus(_))
        ));

        let provider = Arc::new(Mutex::new(
            FileCanonicalLog::open(
                temporary.path().join("canonical"),
                SourceId::new("local-abrupt-restart-source").unwrap(),
                epoch.clone(),
            )
            .unwrap(),
        ));
        let mut catalog =
            SqliteArtifactCatalog::open(temporary.path().join("catalog.sqlite3")).unwrap();
        let store: std::sync::Arc<dyn blob_store::ManagedBlobStore> = std::sync::Arc::new(
            blob_store::LocalFilesystemBlobStore::open(
                temporary.path().join("objects"),
                MAX_LOCAL_ARTIFACT_BYTES,
            )
            .unwrap(),
        );
        let outcome = supervisor
            .recover_interrupted_local(Arc::clone(&provider), &mut catalog, &store)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            SnapshotBuildRecoveryOutcome::InterruptedBuildCleaned
        );
        assert_eq!(catalog.state(&artifact_id).unwrap(), None);
        assert!(!private_work_dir.exists());
        assert_eq!(
            SnapshotBuildJournal::reopen(&paths.journal_root, &build_id)
                .unwrap()
                .progress()
                .stage,
            SnapshotBuildStage::Failed
        );

        SnapshotBuildJournal::remove_recovered_unpublished(
            &paths.journal_root,
            &build_id,
            &artifact_id,
            &private_work_dir,
            &epoch,
        )
        .unwrap();
        let mut retry = SnapshotBuildJournal::create(
            &paths.journal_root,
            &build_id,
            &SnapshotBuildOwnerId::new("local-abrupt-restart-retry").unwrap(),
            &artifact_id,
            &private_work_dir,
            epoch,
            1,
            2_000,
        )
        .unwrap();
        assert_eq!(retry.progress().stage, SnapshotBuildStage::Admitted);
        std::fs::create_dir(&private_work_dir).unwrap();
        let finalized = b"retry finalized immutable artifact bytes";
        std::fs::write(
            private_work_dir.join(SNAPSHOT_ARTIFACT_FILE_NAME),
            finalized,
        )
        .unwrap();
        let manifest = retry_manifest(finalized, &artifact_id, &prepared, &plan);
        let mut now = 2_001;
        for stage in [
            SnapshotBuildStage::RetentionLeased,
            SnapshotBuildStage::Exported,
            SnapshotBuildStage::RollingForward,
            SnapshotBuildStage::Copying,
            SnapshotBuildStage::WaitingForPersistentBarrier,
            SnapshotBuildStage::Finalizing,
            SnapshotBuildStage::Uploading,
        ] {
            retry.transition(stage, now).unwrap();
            now += 1;
        }
        let lease = catalog
            .register_build(&manifest, "local-abrupt-restart-retry", now, 60_000)
            .unwrap();
        catalog.mark_uploading(&lease, now + 1).unwrap();
        upload_retry_objects(&store, &manifest, finalized).await;
        catalog.mark_uploaded(&lease, now + 2).unwrap();
        retry
            .transition(SnapshotBuildStage::Publishing, now + 3)
            .unwrap();
        assert_eq!(
            catalog
                .verify_and_publish(&lease, store.as_ref(), now + 4)
                .await
                .unwrap(),
            PublishOutcome::Published
        );
        std::fs::remove_dir_all(&private_work_dir).unwrap();
        retry
            .transition(SnapshotBuildStage::Published, now + 5)
            .unwrap();

        assert_eq!(
            SnapshotBuildSupervisor::interrupted_builds(&paths.journal_root).unwrap(),
            vec![build_id.clone()]
        );
        assert!(paths.workspace_root.read_dir().unwrap().next().is_none());
        assert_eq!(
            catalog.state(&artifact_id).unwrap(),
            Some(ArtifactState::Published)
        );
        assert_eq!(
            catalog
                .published(&PublishedArtifactQuery {
                    cache_key: manifest.cache_key(),
                    table_set_hash: manifest.manifest().compatibility.table_set_hash.clone(),
                })
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            SnapshotBuildJournal::reopen(&paths.journal_root, &build_id)
                .unwrap()
                .progress()
                .stage,
            SnapshotBuildStage::Published
        );
        assert!(matches!(
            supervisor
                .recover_interrupted_local(provider, &mut catalog, &store)
                .await
                .unwrap(),
            SnapshotBuildRecoveryOutcome::Published { .. }
        ));
    }
}
