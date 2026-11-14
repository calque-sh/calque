//! Cross-layer publication boundary for one exact point-in-time snapshot.
//!
//! This crate performs the only valid transition from a temporary-stream
//! direct-columnar build to a bootstrap-visible catalog row: it waits for the
//! cut's barrier in the independently retained canonical log, writes and
//! re-verifies the artifact at that proven cut, prepares and verifies
//! mandatory zstd and identity representations, uploads every immutable
//! object, verifies provider metadata, and only then crosses the catalog's
//! `PUBLISHED` boundary.

mod barrier_wait;
mod job;
mod lease_guard;
mod progress;
mod representations;
mod watchdog;
pub use barrier_wait::{
    wait_for_persistent_barrier, BarrierWaitError, BarrierWaitProgress, BarrierWaitRequest,
    BarrierWaitResult, CanonicalBarrierRead, CanonicalHeadSignal,
};
pub use job::{
    SnapshotBuildConnections, SnapshotBuildJobConfig, SnapshotBuildJobError,
    SnapshotBuildRecoveryOutcome, SnapshotBuildSafetyPolicy, SnapshotBuildSupervisor,
    SnapshotCleanupFailureCode, SnapshotCleanupStep, SnapshotOperationalCleanup,
    SNAPSHOT_ARTIFACT_FILE_NAME,
};
pub use lease_guard::{SnapshotBuildLeaseGuard, SnapshotBuildLeaseGuardError};
pub use progress::{
    SnapshotBuildDisposition, SnapshotBuildFailureCode, SnapshotBuildJournal,
    SnapshotBuildProgress, SnapshotBuildProgressError, SnapshotBuildStage,
};
use representations::ZstdWhileWriting;
pub use representations::{
    encode_brotli_large_window_controlled, encode_packed_brotli_large_window_controlled,
    pack_artifact_controlled, reencode_zstd_controlled, PackedArtifact, PreparedRepresentation,
    PreparedRepresentations, RepresentationError, SharedBuildZstd, BROTLI_QUALITY,
    REENCODED_ZSTD_LEVEL, REPLACED_ZSTD_LEVEL,
};
pub(crate) use watchdog::{
    SnapshotLifecycleWatchdog, SnapshotPressureMonitor, SnapshotWatchdogState,
};
pub use watchdog::{
    SnapshotPressureLimitKind, SnapshotPressureSamplingFailure, SnapshotWatchdogConfig,
    SnapshotWatchdogError, SnapshotWatchdogFailureReason,
};

use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use artifact_catalog::{
    ArtifactState, BuildLease, CatalogError, PublishOutcome, SqliteArtifactCatalog,
};
use artifact_contract::{RepresentationEncoding, ValidatedArtifactManifest};
use blob_store::{BlobStore, ObjectMetadata, PutOutcome, StorageError};
use snapshot_builder::{DirectArtifactRequest, DirectBuildSession, DirectFinalizationError};
use snapshot_coordinator::CanonicalCutProof;
use sync_types::{ArtifactId, IdentifierError, ObjectId};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

type BeforePublishFuture<'a> =
    Pin<Box<dyn Future<Output = Result<(), SnapshotBuildProgressError>> + Send + 'a>>;

/// Logs the wall time of one preparation phase that started at `started`.
pub(crate) fn phase_finished(phase: &'static str, started: Instant) {
    tracing::info!(
        target: "calque::preparation",
        phase,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "preparation phase finished"
    );
}

/// What must still hold when a publication becomes visible. The catalog asks
/// `admit` in the transaction that publishes, with `lock` held shared, so a
/// holder of `lock`'s exclusive side that changes the answer is never
/// straddled.
#[derive(Clone)]
pub struct PublicationAdmission {
    pub lock: Arc<tokio::sync::RwLock<()>>,
    pub admit: Arc<dyn Fn() -> bool + Send + Sync>,
}

impl std::fmt::Debug for PublicationAdmission {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PublicationAdmission")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotPublicationRequest {
    pub artifact: DirectArtifactRequest,
    /// Client heap the manifest asks for beyond the artifact's decoded bytes.
    pub growth_reserve_bytes: u64,
    pub lease_owner: String,
    pub lease_duration_ms: i64,
    pub catalog_lease_renewal_interval: Duration,
    /// Maximum duration allowed for cancellation-triggered catalog/provider
    /// cleanup before control returns to the lifecycle supervisor.
    pub publication_cleanup_timeout: Duration,
    pub max_encoded_representation_bytes: u64,
    /// How the zstd object is written when other builds run beside this
    /// one. `None` for a build that runs alone.
    pub shared_build_zstd: Option<SharedBuildZstd>,
    /// `None` publishes once everything else holds.
    pub admission: Option<PublicationAdmission>,
}

#[derive(Debug, Clone)]
pub struct SnapshotPublicationReport {
    pub manifest: ValidatedArtifactManifest,
    pub representation_uploads: Vec<RepresentationUpload>,
    pub publish_outcome: PublishOutcome,
    pub decoded_bytes: u64,
    pub decoded_sha256: String,
    pub finalization_micros: u64,
    pub upload_and_publish_micros: u64,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct ResumedSnapshotPublicationReport {
    representation_uploads: Vec<RepresentationUpload>,
    publish_outcome: PublishOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepresentationUpload {
    pub encoding: RepresentationEncoding,
    pub object_id: ObjectId,
    pub outcome: PutOutcome,
}

/// Observable publication boundaries used by the lifecycle journal. A phase
/// is emitted immediately before its work begins; observer failure cancels the
/// publication before crossing the next visibility boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotPublicationPhase {
    Finalizing,
    Uploading,
    Publishing,
}

/// Publishes a direct-columnar build once its cut is proven: writes the
/// artifact at the proof's cursor and checks it, prepares and verifies its
/// representations, uploads them, and crosses the catalog's `PUBLISHED`
/// boundary. While it writes and encodes the artifact, `on_progress` runs
/// every `progress_interval` in which the write or the encode advanced.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn publish_direct_with_proven_cut_before_publish_controlled<
    S: BlobStore + ?Sized,
>(
    build: DirectBuildSession,
    proof: &CanonicalCutProof,
    request: SnapshotPublicationRequest,
    catalog: &mut SqliteArtifactCatalog,
    store: &S,
    cancellation: &CancellationToken,
    before_publish: BeforePublishFuture<'_>,
    mut on_phase: impl FnMut(SnapshotPublicationPhase) -> Result<(), SnapshotBuildProgressError>,
    progress_interval: Duration,
    on_progress: impl FnMut(),
) -> Result<SnapshotPublicationReport, SnapshotPublicationError> {
    validate_request(&request)?;
    ensure_not_cancelled(cancellation)?;
    on_phase(SnapshotPublicationPhase::Finalizing)?;
    let finalization_started = Instant::now();
    let artifact_request = request.artifact.clone();
    let cut_proof = proof.clone();
    let max_encoded_bytes = request.max_encoded_representation_bytes;
    let directory = build
        .artifact_path()
        .parent()
        .ok_or(RepresentationError::MissingParentDirectory)?
        .to_path_buf();
    // The zstd representation is encoded from the artifact's bytes as they
    // are written, on the encoder's own workers, rather than from the file
    // afterwards.
    let mut zstd = ZstdWhileWriting::new(
        &directory,
        max_encoded_bytes,
        request.shared_build_zstd,
        cancellation,
    );
    let progress = zstd.progress();
    let finalization = tokio::task::spawn_blocking(move || {
        let at_cut = match build.finish_at_cut(&cut_proof, artifact_request, &mut zstd) {
            Ok(at_cut) => at_cut,
            Err(error) => {
                return Err(zstd
                    .abandon()
                    .map_or_else(|| error.into(), SnapshotPublicationError::from))
            }
        };
        phase_finished("artifact_write", finalization_started);
        let finalization_micros = elapsed_micros(finalization_started)?;
        let prepared = zstd.finish(
            at_cut.path(),
            at_cut.decoded_bytes(),
            at_cut.decoded_sha256(),
        )?;
        Ok::<_, SnapshotPublicationError>((at_cut, finalization_micros, prepared))
    });
    let (at_cut, finalization_micros, prepared) =
        await_progressing(finalization, &progress, progress_interval, on_progress)
            .await
            .map_err(SnapshotPublicationError::FinalizationTask)??;
    ensure_not_cancelled(cancellation)?;

    let decoded_sha256 = at_cut.decoded_sha256().to_owned();
    let decoded_bytes = at_cut.decoded_bytes();
    let decoded_object_id = ObjectId::new(format!("sha256:{decoded_sha256}"))?;
    let manifest = at_cut.validated_manifest(
        decoded_object_id,
        prepared.manifests(),
        prepared.decoded_chunks().clone(),
        request.growth_reserve_bytes,
    )?;

    let publication = CatalogPublicationOptions {
        lease_owner: &request.lease_owner,
        lease_duration_ms: request.lease_duration_ms,
        renewal_interval: request.catalog_lease_renewal_interval,
        cleanup_timeout: request.publication_cleanup_timeout,
        cancellation,
        before_publish: Some(before_publish),
        admission: request.admission.as_ref(),
        on_phase: &mut on_phase,
    };
    let (representation_uploads, publish_outcome, upload_and_publish_micros) =
        publish_manifest(Some(&prepared), &manifest, catalog, store, publication).await?;

    Ok(SnapshotPublicationReport {
        manifest,
        representation_uploads,
        publish_outcome,
        decoded_bytes,
        decoded_sha256,
        finalization_micros,
        upload_and_publish_micros,
    })
}

/// Awaits `work`, calling `on_progress` at most once per `interval`, and only
/// when `progress` moved since the last call: work that slows down keeps
/// its build alive, and work that stops does not.
async fn await_progressing<T>(
    work: impl Future<Output = T>,
    progress: &AtomicU64,
    interval: Duration,
    mut on_progress: impl FnMut(),
) -> T {
    tokio::pin!(work);
    let mut ticks = tokio::time::interval(interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut seen = progress.load(Ordering::Relaxed);
    loop {
        tokio::select! {
            output = &mut work => return output,
            _ = ticks.tick() => {
                let now = progress.load(Ordering::Relaxed);
                if now != seen {
                    seen = now;
                    on_progress();
                }
            }
        }
    }
}

// Test-only proof for catalog upload idempotency. Production recovery
// abandons every uncommitted publication: resuming without the exact active
// canonical-retention guard could publish a cursor whose required history has
// already been retained away.
#[cfg(test)]
async fn resume_snapshot<S: BlobStore + ?Sized>(
    finalized_path: &std::path::Path,
    artifact_id: &ArtifactId,
    lease_owner: &str,
    lease_duration_ms: i64,
    catalog: &mut SqliteArtifactCatalog,
    store: &S,
) -> Result<ResumedSnapshotPublicationReport, SnapshotPublicationError> {
    let renewal_interval = default_catalog_renewal_interval(lease_duration_ms)?;
    resume_snapshot_controlled(
        finalized_path,
        artifact_id,
        lease_owner,
        lease_duration_ms,
        renewal_interval,
        catalog,
        store,
        &CancellationToken::new(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
async fn resume_snapshot_controlled<S: BlobStore + ?Sized>(
    finalized_path: &std::path::Path,
    artifact_id: &ArtifactId,
    lease_owner: &str,
    lease_duration_ms: i64,
    renewal_interval: Duration,
    catalog: &mut SqliteArtifactCatalog,
    store: &S,
    cancellation: &CancellationToken,
) -> Result<ResumedSnapshotPublicationReport, SnapshotPublicationError> {
    validate_catalog_lease_timing(lease_duration_ms, renewal_interval)?;
    ensure_not_cancelled(cancellation)?;
    let manifest = catalog
        .manifest(artifact_id)?
        .ok_or_else(|| SnapshotPublicationError::UnknownResumeArtifact(artifact_id.clone()))?;
    let state = catalog
        .state(artifact_id)?
        .ok_or_else(|| SnapshotPublicationError::UnknownResumeArtifact(artifact_id.clone()))?;
    let prepared = if matches!(state, ArtifactState::Building | ArtifactState::Uploading) {
        let maximum = manifest
            .manifest()
            .representations
            .iter()
            .map(|representation| representation.encoded_bytes)
            .max()
            .ok_or(SnapshotPublicationError::ResumeManifestRepresentationMismatch)?;
        let prepared = PreparedRepresentations::prepare_controlled(
            finalized_path,
            manifest.manifest().decoded.bytes,
            &manifest.manifest().decoded.sha256,
            maximum,
            cancellation,
        )?;
        if prepared.manifests() != manifest.manifest().representations {
            return Err(SnapshotPublicationError::ResumeManifestRepresentationMismatch);
        }
        Some(prepared)
    } else {
        None
    };
    let mut ignore_phase = ignore_publication_phase;
    let publication = CatalogPublicationOptions {
        lease_owner,
        lease_duration_ms,
        renewal_interval,
        cleanup_timeout: Duration::from_secs(30),
        cancellation,
        before_publish: None,
        admission: None,
        on_phase: &mut ignore_phase,
    };
    let (representation_uploads, publish_outcome, _) =
        publish_manifest(prepared.as_ref(), &manifest, catalog, store, publication).await?;
    Ok(ResumedSnapshotPublicationReport {
        representation_uploads,
        publish_outcome,
    })
}

struct CatalogPublicationOptions<'a> {
    lease_owner: &'a str,
    lease_duration_ms: i64,
    renewal_interval: Duration,
    cleanup_timeout: Duration,
    cancellation: &'a CancellationToken,
    before_publish: Option<BeforePublishFuture<'a>>,
    admission: Option<&'a PublicationAdmission>,
    on_phase: &'a mut dyn FnMut(SnapshotPublicationPhase) -> Result<(), SnapshotBuildProgressError>,
}

#[cfg(test)]
fn ignore_publication_phase(
    _phase: SnapshotPublicationPhase,
) -> Result<(), SnapshotBuildProgressError> {
    Ok(())
}

async fn publish_manifest<S: BlobStore + ?Sized>(
    prepared: Option<&PreparedRepresentations>,
    manifest: &ValidatedArtifactManifest,
    catalog: &mut SqliteArtifactCatalog,
    store: &S,
    mut options: CatalogPublicationOptions<'_>,
) -> Result<(Vec<RepresentationUpload>, PublishOutcome, u64), SnapshotPublicationError> {
    ensure_not_cancelled(options.cancellation)?;
    let started = Instant::now();
    let registration = catalog.register_or_resume_build(
        manifest,
        options.lease_owner,
        current_time_ms()?,
        options.lease_duration_ms,
    )?;
    let lease = registration.lease().clone();
    let state = registration.state();
    let result = publish_registered_manifest(
        prepared,
        manifest,
        state,
        &lease,
        catalog,
        store,
        &mut options,
    )
    .await;
    match result {
        Ok((representation_uploads, publish_outcome)) => Ok((
            representation_uploads,
            publish_outcome,
            elapsed_micros_saturating(started),
        )),
        Err(error) if state == ArtifactState::Published => Err(error),
        Err(error) if !options.cancellation.is_cancelled() => Err(error),
        Err(error) => {
            let now = current_time_ms()?;
            let cleanup_lease_duration_ms = cleanup_lease_duration_ms(options.cleanup_timeout)?;
            let cleanup = catalog
                .abandon_build(
                    &lease,
                    options.lease_owner,
                    now,
                    0,
                    cleanup_lease_duration_ms,
                    "snapshot publication failed",
                )
                .map_err(SnapshotPublicationError::AbandonedBuildCleanup)?;
            tokio::time::timeout(
                options.cleanup_timeout,
                catalog.cleanup_abandoned_build(&cleanup, store, now),
            )
            .await
            .map_err(|_| SnapshotPublicationError::AbandonedBuildCleanupTimedOut)?
            .map_err(SnapshotPublicationError::AbandonedBuildCleanup)?;
            Err(error)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn publish_registered_manifest<S: BlobStore + ?Sized>(
    prepared: Option<&PreparedRepresentations>,
    manifest: &ValidatedArtifactManifest,
    state: ArtifactState,
    lease: &BuildLease,
    catalog: &mut SqliteArtifactCatalog,
    store: &S,
    options: &mut CatalogPublicationOptions<'_>,
) -> Result<(Vec<RepresentationUpload>, PublishOutcome), SnapshotPublicationError> {
    if state == ArtifactState::Published {
        let publish_outcome =
            verify_and_publish_controlled(catalog, lease, store, None, options.cancellation)
                .await?;
        return Ok((existing_representation_uploads(manifest), publish_outcome));
    }
    (options.on_phase)(SnapshotPublicationPhase::Uploading)?;
    let representation_uploads = match state {
        ArtifactState::Building | ArtifactState::Uploading => {
            if state == ArtifactState::Building {
                catalog.mark_uploading(lease, current_time_ms()?)?;
            }
            let prepared = prepared.ok_or(SnapshotPublicationError::RepresentationFilesRequired)?;
            if prepared.manifests() != manifest.manifest().representations {
                return Err(SnapshotPublicationError::ResumeManifestRepresentationMismatch);
            }
            upload_representations_controlled(
                prepared,
                store,
                catalog,
                lease,
                options.lease_duration_ms,
                options.renewal_interval,
                options.cancellation,
            )
            .await?
        }
        ArtifactState::Uploaded => existing_representation_uploads(manifest),
        state => return Err(SnapshotPublicationError::UnresumableCatalogState(state)),
    };
    if matches!(state, ArtifactState::Building | ArtifactState::Uploading) {
        renew_catalog_lease(catalog, lease, options.lease_duration_ms)?;
        ensure_not_cancelled(options.cancellation)?;
        catalog.mark_uploaded(lease, current_time_ms()?)?;
    }
    ensure_not_cancelled(options.cancellation)?;
    (options.on_phase)(SnapshotPublicationPhase::Publishing)?;
    ensure_not_cancelled(options.cancellation)?;
    if let Some(mut before_publish) = options.before_publish.take() {
        let waiting_started = Instant::now();
        loop {
            tokio::select! {
                biased;
                () = options.cancellation.cancelled() => {
                    return Err(SnapshotPublicationError::Cancelled);
                }
                result = &mut before_publish => {
                    result?;
                    phase_finished("before_publish", waiting_started);
                    break;
                }
                () = tokio::time::sleep(options.renewal_interval) => {
                    renew_catalog_lease(catalog, lease, options.lease_duration_ms)?;
                }
            }
        }
    }
    ensure_not_cancelled(options.cancellation)?;
    let verification_started = Instant::now();
    let publish_outcome = verify_and_publish_controlled(
        catalog,
        lease,
        store,
        options.admission,
        options.cancellation,
    )
    .await?;
    phase_finished("catalog_verify_and_publish", verification_started);
    Ok((representation_uploads, publish_outcome))
}

fn existing_representation_uploads(
    manifest: &ValidatedArtifactManifest,
) -> Vec<RepresentationUpload> {
    manifest
        .manifest()
        .representations
        .iter()
        .map(|representation| RepresentationUpload {
            encoding: representation.encoding,
            object_id: representation.object_id.clone(),
            outcome: PutOutcome::AlreadyPresent,
        })
        .collect()
}

fn elapsed_micros_saturating(started: Instant) -> u64 {
    started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
async fn upload_representation<S: BlobStore + ?Sized>(
    path: &std::path::Path,
    store: &S,
    object_id: &ObjectId,
    encoded_bytes: u64,
    encoded_sha256: &str,
) -> Result<PutOutcome, SnapshotPublicationError> {
    let metadata = ObjectMetadata::new(encoded_bytes, encoded_sha256.to_owned())?;
    Ok(store.put_file_if_absent(object_id, path, &metadata).await?)
}

/// Uploads every representation at once, so that their flushes overlap, and
/// renews the build's catalog lease while they run. The uploads are returned
/// in the order `prepared` lists them; the first to fail drops the rest, which
/// a store leaves never-visible, as it would a cancelled upload.
async fn upload_representations_controlled<S: BlobStore + ?Sized>(
    prepared: &PreparedRepresentations,
    store: &S,
    catalog: &mut SqliteArtifactCatalog,
    lease: &artifact_catalog::BuildLease,
    lease_duration_ms: i64,
    renewal_interval: Duration,
    cancellation: &CancellationToken,
) -> Result<Vec<RepresentationUpload>, SnapshotPublicationError> {
    ensure_not_cancelled(cancellation)?;
    let started = Instant::now();
    let uploads = futures_util::future::try_join_all(prepared.entries().map(|entry| async move {
        let representation = entry.manifest();
        let metadata = ObjectMetadata::new(
            representation.encoded_bytes,
            representation.encoded_sha256.clone(),
        )?;
        let outcome = store
            .put_file_if_absent(&representation.object_id, entry.path(), &metadata)
            .await?;
        phase_finished(
            match representation.encoding {
                RepresentationEncoding::Zstd => "upload_zstd",
                RepresentationEncoding::Identity => "upload_identity",
                RepresentationEncoding::BrotliLargeWindow => "upload_brotli_large_window",
                RepresentationEncoding::PackedBrotliLargeWindow => {
                    "upload_packed_brotli_large_window"
                }
            },
            started,
        );
        Ok::<_, SnapshotPublicationError>(RepresentationUpload {
            encoding: representation.encoding,
            object_id: representation.object_id.clone(),
            outcome,
        })
    }));
    tokio::pin!(uploads);
    loop {
        tokio::select! {
            () = cancellation.cancelled() => return Err(SnapshotPublicationError::Cancelled),
            uploads = &mut uploads => return uploads,
            () = tokio::time::sleep(renewal_interval) => {
                renew_catalog_lease(catalog, lease, lease_duration_ms)?;
            }
        }
    }
}

async fn verify_and_publish_controlled<S: BlobStore + ?Sized>(
    catalog: &mut SqliteArtifactCatalog,
    lease: &artifact_catalog::BuildLease,
    store: &S,
    admission: Option<&PublicationAdmission>,
    cancellation: &CancellationToken,
) -> Result<PublishOutcome, SnapshotPublicationError> {
    let _admission_lock = match admission {
        None => None,
        Some(admission) => tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(SnapshotPublicationError::Cancelled),
            shared = admission.lock.read() => Some(shared),
        },
    };
    {
        let verification =
            catalog.verify_and_publish_admitted(lease, store, current_time_ms()?, || {
                admission.is_none_or(|admission| (admission.admit)())
            });
        tokio::pin!(verification);
        tokio::select! {
            // If verification committed visibility in the same scheduler turn
            // that cancellation arrived, its result is authoritative.
            biased;
            outcome = &mut verification => return Ok(outcome?),
            () = cancellation.cancelled() => {}
        }
    }
    // Dropping the future after cancellation is an ambiguous boundary: the
    // catalog transaction may already have committed. Never translate a
    // committed immutable publication back into build failure.
    if catalog.state(lease.artifact_id())? == Some(ArtifactState::Published) {
        Ok(PublishOutcome::AlreadyPublished)
    } else {
        Err(SnapshotPublicationError::Cancelled)
    }
}

fn renew_catalog_lease(
    catalog: &mut SqliteArtifactCatalog,
    lease: &artifact_catalog::BuildLease,
    lease_duration_ms: i64,
) -> Result<(), SnapshotPublicationError> {
    catalog.renew_lease(lease, current_time_ms()?, lease_duration_ms)?;
    Ok(())
}

fn validate_request(request: &SnapshotPublicationRequest) -> Result<(), SnapshotPublicationError> {
    if request.lease_owner.is_empty()
        || request.lease_duration_ms <= 0
        || request.max_encoded_representation_bytes == 0
        || request.publication_cleanup_timeout.is_zero()
    {
        return Err(SnapshotPublicationError::InvalidRequest);
    }
    validate_catalog_lease_timing(
        request.lease_duration_ms,
        request.catalog_lease_renewal_interval,
    )?;
    Ok(())
}

fn cleanup_lease_duration_ms(timeout: Duration) -> Result<i64, SnapshotPublicationError> {
    let attempt_ms = timeout.as_millis().max(1);
    let margin_ms = (attempt_ms / 10).max(1);
    i64::try_from(
        attempt_ms
            .checked_add(margin_ms)
            .ok_or(SnapshotPublicationError::InvalidRequest)?,
    )
    .map_err(|_| SnapshotPublicationError::InvalidRequest)
}

#[cfg(test)]
fn default_catalog_renewal_interval(
    lease_duration_ms: i64,
) -> Result<Duration, SnapshotPublicationError> {
    if lease_duration_ms < 3 {
        return Err(SnapshotPublicationError::InvalidRequest);
    }
    Ok(Duration::from_millis(
        u64::try_from(lease_duration_ms / 3)
            .map_err(|_| SnapshotPublicationError::InvalidRequest)?,
    ))
}

fn validate_catalog_lease_timing(
    lease_duration_ms: i64,
    renewal_interval: Duration,
) -> Result<(), SnapshotPublicationError> {
    let lease_ms =
        u128::try_from(lease_duration_ms).map_err(|_| SnapshotPublicationError::InvalidRequest)?;
    if renewal_interval.is_zero() || renewal_interval.as_millis() * 2 > lease_ms {
        return Err(SnapshotPublicationError::InvalidRequest);
    }
    Ok(())
}

fn ensure_not_cancelled(cancellation: &CancellationToken) -> Result<(), SnapshotPublicationError> {
    if cancellation.is_cancelled() {
        Err(SnapshotPublicationError::Cancelled)
    } else {
        Ok(())
    }
}

fn elapsed_micros(start: Instant) -> Result<u64, SnapshotPublicationError> {
    u64::try_from(start.elapsed().as_micros())
        .map_err(|_| SnapshotPublicationError::DurationOverflow)
}

fn current_time_ms() -> Result<i64, SnapshotPublicationError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SnapshotPublicationError::ClockBeforeUnixEpoch)?
        .as_millis();
    i64::try_from(millis).map_err(|_| SnapshotPublicationError::ClockOverflow)
}

#[derive(Debug, Error)]
pub enum SnapshotPublicationError {
    #[error("snapshot publication was cancelled")]
    Cancelled,
    #[error("snapshot publication request has an invalid lease, size limit or cleanup timeout")]
    InvalidRequest,
    #[error("snapshot publication duration exceeded u64 microseconds")]
    DurationOverflow,
    #[error("system clock is before the Unix epoch")]
    ClockBeforeUnixEpoch,
    #[error("system clock milliseconds exceed i64")]
    ClockOverflow,
    #[error("catalog state {0:?} cannot resume immutable snapshot publication")]
    UnresumableCatalogState(ArtifactState),
    #[error("artifact {0} has no interrupted catalog publication to resume")]
    UnknownResumeArtifact(ArtifactId),
    #[error("regenerated artifact representations do not match the registered manifest")]
    ResumeManifestRepresentationMismatch,
    #[error("resuming BUILDING or UPLOADING requires all prepared representation files")]
    RepresentationFilesRequired,
    #[error("abandoned snapshot build cleanup did not reach its durable terminal boundary: {0}")]
    AbandonedBuildCleanup(#[source] CatalogError),
    #[error("abandoned snapshot build cleanup exceeded its deadline")]
    AbandonedBuildCleanupTimedOut,
    #[error(transparent)]
    DirectFinalization(#[from] DirectFinalizationError),
    #[error("snapshot finalization blocking owner failed: {0}")]
    FinalizationTask(#[source] tokio::task::JoinError),
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Identifier(#[from] IdentifierError),
    #[error(transparent)]
    Representation(#[from] RepresentationError),
    #[error(transparent)]
    Progress(#[from] SnapshotBuildProgressError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Mutex,
        },
    };

    use super::*;
    use artifact_catalog::SqliteArtifactCatalog;
    use artifact_contract::ValidatedArtifactManifest;
    use blob_store::{BlobStore, LocalFilesystemBlobStore};
    use sha2::{Digest, Sha256};

    /// Work that advances `progress` once a second for `advancing` seconds,
    /// then stalls until it is cancelled or `stalled` seconds pass.
    async fn work_that_stalls(
        progress: Arc<AtomicU64>,
        advancing: u64,
        stalled: u64,
        cancellation: CancellationToken,
    ) -> bool {
        for _ in 0..advancing {
            tokio::time::sleep(Duration::from_secs(1)).await;
            progress.fetch_add(1, Ordering::Relaxed);
        }
        tokio::select! {
            () = cancellation.cancelled() => false,
            () = tokio::time::sleep(Duration::from_secs(stalled)) => true,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_finalization_that_advances_outlives_the_no_progress_timeout_and_one_that_stalls_does_not(
    ) {
        let timeout = Duration::from_secs(2);
        let config = SnapshotWatchdogConfig::new(Duration::from_secs(600), timeout).unwrap();

        // Ten seconds of slow progress, then a second's pause, then done.
        let cancellation = CancellationToken::new();
        let watchdog = SnapshotLifecycleWatchdog::spawn(config, cancellation.clone(), None);
        let progress = Arc::new(AtomicU64::new(0));
        let work = work_that_stalls(Arc::clone(&progress), 10, 1, cancellation.clone());
        let finished =
            await_progressing(work, &progress, timeout / 4, || watchdog.heartbeat()).await;
        assert!(finished);
        assert!(!cancellation.is_cancelled());
        assert_eq!(watchdog.latest().failure_reason, None);

        // Three seconds of progress, then a stall longer than the timeout.
        let cancellation = CancellationToken::new();
        let watchdog = SnapshotLifecycleWatchdog::spawn(config, cancellation.clone(), None);
        let progress = Arc::new(AtomicU64::new(0));
        let started = tokio::time::Instant::now();
        let work = work_that_stalls(Arc::clone(&progress), 3, 600, cancellation.clone());
        let finished =
            await_progressing(work, &progress, timeout / 4, || watchdog.heartbeat()).await;
        assert!(!finished);
        assert_eq!(
            watchdog.latest().failure_reason,
            Some(SnapshotWatchdogFailureReason::NoProgress)
        );
        assert!(started.elapsed() < Duration::from_secs(3) + timeout * 2);
    }

    struct HangingCleanupStore;

    #[derive(Default)]
    struct ReadyBlobStore {
        objects: Mutex<BTreeMap<String, ObjectMetadata>>,
        head_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl BlobStore for ReadyBlobStore {
        async fn put_if_absent(
            &self,
            object: &ObjectId,
            _body: blob_store::ByteStream,
            metadata: &ObjectMetadata,
        ) -> blob_store::Result<PutOutcome> {
            let mut objects = self.objects.lock().expect("ready store lock");
            if objects.contains_key(object.as_str()) {
                Ok(PutOutcome::AlreadyPresent)
            } else {
                objects.insert(object.as_str().to_owned(), metadata.clone());
                Ok(PutOutcome::Created)
            }
        }

        async fn head(&self, object: &ObjectId) -> blob_store::Result<Option<ObjectMetadata>> {
            self.head_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self
                .objects
                .lock()
                .expect("ready store lock")
                .get(object.as_str())
                .cloned())
        }

        async fn get(
            &self,
            _object: &ObjectId,
        ) -> blob_store::Result<Option<blob_store::ByteStream>> {
            Ok(None)
        }

        async fn delete(&self, object: &ObjectId) -> blob_store::Result<()> {
            self.objects
                .lock()
                .expect("ready store lock")
                .remove(object.as_str());
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl BlobStore for HangingCleanupStore {
        async fn put_if_absent(
            &self,
            _object: &ObjectId,
            _body: blob_store::ByteStream,
            _metadata: &ObjectMetadata,
        ) -> blob_store::Result<PutOutcome> {
            std::future::pending().await
        }

        async fn head(&self, _object: &ObjectId) -> blob_store::Result<Option<ObjectMetadata>> {
            Ok(None)
        }

        async fn get(
            &self,
            _object: &ObjectId,
        ) -> blob_store::Result<Option<blob_store::ByteStream>> {
            Ok(None)
        }

        async fn delete(&self, _object: &ObjectId) -> blob_store::Result<()> {
            std::future::pending().await
        }
    }

    fn representation_manifest(bytes: &[u8]) -> ValidatedArtifactManifest {
        let digest = hex::encode(Sha256::digest(bytes));
        let decoded = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(decoded.path(), bytes).unwrap();
        let representations = PreparedRepresentations::prepare(
            decoded.path(),
            bytes.len() as u64,
            &digest,
            bytes.len() as u64 + 1024,
        )
        .unwrap()
        .manifests();
        let value = serde_json::json!({
            "protocolVersion": 1,
            "artifactId": "resume-artifact",
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
                        "name": "items",
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
                "schemaGeneration": 1,
                "authorizationGeneration": 0,
                "authorizationPolicyHash": format!("sha256:{}", "9".repeat(64)),
                "authorizationScopeHash": format!("sha256:{}", "b".repeat(64)),
                "tableSetHash": format!("sha256:{}", "c".repeat(64)),
                "shardProfileHash": format!("sha256:{}", "d".repeat(64)),
                "indexProfileHash": format!("sha256:{}", "e".repeat(64)),
                "typeCodecGeneration": 1,
                "collationGeneration": 1
            },
            "tables": [{
                "tableId": "table-1",
                "sourceSchema": "public",
                "sourceTable": "items",
                "localSchema": "main",
                "localTable": "items",
                "schemaHash": format!("sha256:{}", "f".repeat(64)),
                "columnProfileHash": format!("sha256:{}", "1".repeat(64)),
                "rowCount": 1,
                "primaryKeyCodec": "pk-v1/protobuf",
                "columns": [{ "columnId": 1, "name": "id" }]
            }],
            "representations": representations,
            "memory": {
                "estimatedPeakBytes": 4096,
                "recommendedGrowthReserveBytes": 1024
            }
        });
        ValidatedArtifactManifest::parse(&serde_json::to_vec(&value).unwrap()).unwrap()
    }

    #[tokio::test]
    async fn interrupted_catalog_publication_resumes_without_the_snapshot_stream() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("finalized.artifact");
        let bytes = b"finalized immutable bytes";
        std::fs::write(&path, bytes).unwrap();
        let manifest = representation_manifest(bytes);
        let artifact = manifest.manifest().artifact_id.clone();
        let object = manifest.manifest().decoded.object_id.clone();
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("objects"), 1024).unwrap();
        catalog
            .register_build(&manifest, "worker", current_time_ms().unwrap(), 60_000)
            .unwrap();

        let resumed = resume_snapshot(&path, &artifact, "worker", 60_000, &mut catalog, &store)
            .await
            .unwrap();
        assert_eq!(resumed.publish_outcome, PublishOutcome::Published);
        assert_eq!(resumed.representation_uploads.len(), 2);
        assert!(resumed
            .representation_uploads
            .iter()
            .all(|upload| upload.outcome == PutOutcome::Created));
        for representation in &manifest.manifest().representations {
            assert_eq!(
                store.head(&representation.object_id).await.unwrap(),
                Some(
                    ObjectMetadata::new(
                        representation.encoded_bytes,
                        representation.encoded_sha256.clone(),
                    )
                    .unwrap(),
                )
            );
        }
        assert_eq!(
            catalog.state(&artifact).unwrap(),
            Some(ArtifactState::Published)
        );

        std::fs::remove_file(&path).unwrap();
        let repeated = resume_snapshot(&path, &artifact, "worker", 60_000, &mut catalog, &store)
            .await
            .unwrap();
        assert_eq!(repeated.publish_outcome, PublishOutcome::AlreadyPublished);

        store.delete(&object).await.unwrap();
        assert!(matches!(
            resume_snapshot(&path, &artifact, "worker", 60_000, &mut catalog, &store,).await,
            Err(SnapshotPublicationError::Catalog(
                CatalogError::MissingObject(_)
            ))
        ));
        assert_eq!(
            catalog.state(&artifact).unwrap(),
            Some(ArtifactState::Retired)
        );
    }

    #[tokio::test]
    async fn partial_multi_representation_upload_resumes_idempotently() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("finalized.artifact");
        let bytes = b"repeatable finalized immutable bytes";
        std::fs::write(&path, bytes).unwrap();
        let manifest = representation_manifest(bytes);
        let artifact = manifest.manifest().artifact_id.clone();
        let digest = hex::encode(Sha256::digest(bytes));
        let prepared = PreparedRepresentations::prepare(
            &path,
            bytes.len() as u64,
            &digest,
            bytes.len() as u64 + 1024,
        )
        .unwrap();
        assert_eq!(prepared.manifests(), manifest.manifest().representations);
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("objects"), 2048).unwrap();
        let lease = catalog
            .register_build(&manifest, "worker", current_time_ms().unwrap(), 60_000)
            .unwrap();
        catalog
            .mark_uploading(&lease, current_time_ms().unwrap())
            .unwrap();
        let first = prepared.entries().next().unwrap();
        assert_eq!(
            upload_representation(
                first.path(),
                &store,
                &first.manifest().object_id,
                first.manifest().encoded_bytes,
                &first.manifest().encoded_sha256,
            )
            .await
            .unwrap(),
            PutOutcome::Created
        );
        drop(prepared);

        let resumed = resume_snapshot(&path, &artifact, "worker", 60_000, &mut catalog, &store)
            .await
            .unwrap();
        assert_eq!(resumed.publish_outcome, PublishOutcome::Published);
        assert_eq!(
            resumed.representation_uploads[0].outcome,
            PutOutcome::AlreadyPresent
        );
        assert!(resumed.representation_uploads[1..]
            .iter()
            .all(|upload| upload.outcome == PutOutcome::Created));
        assert_eq!(
            catalog.state(&artifact).unwrap(),
            Some(ArtifactState::Published)
        );
    }

    /// Refuses any upload that is not joined by every other upload of the
    /// publication within five seconds.
    struct OverlapRequiredStore {
        inner: ReadyBlobStore,
        arrived: tokio::sync::Barrier,
    }

    #[async_trait::async_trait]
    impl BlobStore for OverlapRequiredStore {
        async fn put_if_absent(
            &self,
            object: &ObjectId,
            body: blob_store::ByteStream,
            metadata: &ObjectMetadata,
        ) -> blob_store::Result<PutOutcome> {
            tokio::time::timeout(Duration::from_secs(5), self.arrived.wait())
                .await
                .map_err(|_| {
                    blob_store::StorageError::Io(std::io::Error::other(
                        "an upload ran while no other was in flight",
                    ))
                })?;
            self.inner.put_if_absent(object, body, metadata).await
        }

        async fn head(&self, object: &ObjectId) -> blob_store::Result<Option<ObjectMetadata>> {
            self.inner.head(object).await
        }

        async fn get(
            &self,
            object: &ObjectId,
        ) -> blob_store::Result<Option<blob_store::ByteStream>> {
            self.inner.get(object).await
        }

        async fn delete(&self, object: &ObjectId) -> blob_store::Result<()> {
            self.inner.delete(object).await
        }
    }

    #[tokio::test]
    async fn a_publication_uploads_its_representations_at_once() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("finalized.artifact");
        let bytes = b"representations uploaded together";
        std::fs::write(&path, bytes).unwrap();
        let manifest = representation_manifest(bytes);
        let artifact = manifest.manifest().artifact_id.clone();
        let representations = manifest.manifest().representations.len();
        assert!(representations > 1);
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = OverlapRequiredStore {
            inner: ReadyBlobStore::default(),
            arrived: tokio::sync::Barrier::new(representations),
        };
        catalog
            .register_build(&manifest, "worker", current_time_ms().unwrap(), 60_000)
            .unwrap();

        let published = resume_snapshot(&path, &artifact, "worker", 60_000, &mut catalog, &store)
            .await
            .unwrap();
        assert_eq!(published.publish_outcome, PublishOutcome::Published);
        assert_eq!(
            published
                .representation_uploads
                .iter()
                .map(|upload| (upload.encoding, upload.object_id.clone(), upload.outcome))
                .collect::<Vec<_>>(),
            manifest
                .manifest()
                .representations
                .iter()
                .map(|representation| (
                    representation.encoding,
                    representation.object_id.clone(),
                    PutOutcome::Created
                ))
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn resume_rejects_changed_finalized_bytes_before_visibility() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("finalized.artifact");
        let bytes = b"registered finalized immutable bytes";
        std::fs::write(&path, bytes).unwrap();
        let manifest = representation_manifest(bytes);
        let artifact = manifest.manifest().artifact_id.clone();
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("objects"), 2048).unwrap();
        catalog
            .register_build(&manifest, "worker", current_time_ms().unwrap(), 60_000)
            .unwrap();
        std::fs::write(&path, b"different finalized immutable bytes").unwrap();

        assert!(matches!(
            resume_snapshot(&path, &artifact, "worker", 60_000, &mut catalog, &store).await,
            Err(SnapshotPublicationError::Representation(
                RepresentationError::DecodedFileChanged { .. }
                    | RepresentationError::DecodedDigestMismatch
            ))
        ));
        assert_eq!(
            catalog.state(&artifact).unwrap(),
            Some(ArtifactState::Building)
        );
        for representation in &manifest.manifest().representations {
            assert_eq!(store.head(&representation.object_id).await.unwrap(), None);
        }
    }

    /// The zstd verifier decodes the encoder's output as it is written and
    /// never reads the file back, and the encoder takes the artifact's bytes
    /// as they are written, so the blob store's hash of what it stores is the
    /// only check between either file on disk and publication. A zstd or
    /// artifact file changed after it was written, in place with its length
    /// and modification time kept or cut short, is refused at upload and never
    /// stored.
    #[tokio::test]
    async fn a_representation_file_changed_after_it_was_written_is_refused_at_upload() {
        for (encoding, truncated) in [
            (RepresentationEncoding::Zstd, false),
            (RepresentationEncoding::Zstd, true),
            (RepresentationEncoding::Identity, false),
            (RepresentationEncoding::Identity, true),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("finalized.artifact");
            let bytes = b"encoded then changed finalized immutable bytes";
            std::fs::write(&path, bytes).unwrap();
            let manifest = representation_manifest(bytes);
            let artifact = manifest.manifest().artifact_id.clone();
            let digest = hex::encode(Sha256::digest(bytes));
            let prepared = PreparedRepresentations::prepare(
                &path,
                bytes.len() as u64,
                &digest,
                bytes.len() as u64 + 1024,
            )
            .unwrap();
            let changed = prepared
                .entries()
                .find(|entry| entry.manifest().encoding == encoding)
                .unwrap();
            let mut file = std::fs::File::options()
                .write(true)
                .open(changed.path())
                .unwrap();
            if truncated {
                file.set_len(changed.manifest().encoded_bytes - 1).unwrap();
            } else {
                let modified = file.metadata().unwrap().modified().unwrap();
                let mut encoded = std::fs::read(changed.path()).unwrap();
                let last = encoded.len() - 1;
                encoded[last] ^= 0xff;
                std::io::Write::write_all(&mut file, &encoded).unwrap();
                file.set_modified(modified).unwrap();
            }
            drop(file);
            let mut catalog =
                SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
            let store =
                LocalFilesystemBlobStore::open(directory.path().join("objects"), 2048).unwrap();
            let cancellation = CancellationToken::new();
            let mut ignore_phase = ignore_publication_phase;

            let result = publish_manifest(
                Some(&prepared),
                &manifest,
                &mut catalog,
                &store,
                CatalogPublicationOptions {
                    lease_owner: "changed-encoding-worker",
                    lease_duration_ms: 60_000,
                    renewal_interval: Duration::from_secs(10),
                    cleanup_timeout: Duration::from_secs(1),
                    cancellation: &cancellation,
                    before_publish: None,
                    admission: None,
                    on_phase: &mut ignore_phase,
                },
            )
            .await;

            assert!(
                matches!(
                    result,
                    Err(SnapshotPublicationError::Storage(
                        StorageError::BodyMismatch { .. }
                    ))
                ),
                "{encoding:?}, truncated: {truncated}"
            );
            assert_eq!(
                store.head(&changed.manifest().object_id).await.unwrap(),
                None,
                "{encoding:?}, truncated: {truncated}"
            );
            assert_eq!(
                catalog.state(&artifact).unwrap(),
                Some(ArtifactState::Uploading),
                "{encoding:?}, truncated: {truncated}"
            );
        }
    }

    #[tokio::test]
    async fn cancellation_cleanup_deadline_bounds_a_hung_provider_delete() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("finalized.artifact");
        let bytes = b"cancel cleanup timeout bytes";
        std::fs::write(&path, bytes).unwrap();
        let manifest = representation_manifest(bytes);
        let artifact = manifest.manifest().artifact_id.clone();
        let digest = hex::encode(Sha256::digest(bytes));
        let prepared = PreparedRepresentations::prepare(
            &path,
            bytes.len() as u64,
            &digest,
            bytes.len() as u64 + 1024,
        )
        .unwrap();
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let cancellation = CancellationToken::new();
        let trigger = cancellation.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            trigger.cancel();
        });
        let mut ignore_phase = ignore_publication_phase;
        let result = publish_manifest(
            Some(&prepared),
            &manifest,
            &mut catalog,
            &HangingCleanupStore,
            CatalogPublicationOptions {
                lease_owner: "hung-cleanup-worker",
                lease_duration_ms: 60_000,
                renewal_interval: Duration::from_secs(10),
                cleanup_timeout: Duration::from_millis(25),
                cancellation: &cancellation,
                before_publish: None,
                admission: None,
                on_phase: &mut ignore_phase,
            },
        )
        .await;
        assert!(matches!(
            result,
            Err(SnapshotPublicationError::AbandonedBuildCleanupTimedOut)
        ));
        assert_eq!(
            catalog.state(&artifact).unwrap(),
            Some(ArtifactState::Uploading)
        );
    }

    #[tokio::test]
    async fn publishing_phase_cancellation_precedes_immediately_ready_verification() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("finalized.artifact");
        let bytes = b"ready provider cancellation bytes";
        std::fs::write(&path, bytes).unwrap();
        let manifest = representation_manifest(bytes);
        let artifact = manifest.manifest().artifact_id.clone();
        let digest = hex::encode(Sha256::digest(bytes));
        let prepared = PreparedRepresentations::prepare(
            &path,
            bytes.len() as u64,
            &digest,
            bytes.len() as u64 + 1024,
        )
        .unwrap();
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let cancellation = CancellationToken::new();
        let trigger = cancellation.clone();
        let mut on_phase = move |phase| {
            if phase == SnapshotPublicationPhase::Publishing {
                trigger.cancel();
            }
            Ok(())
        };
        let store = ReadyBlobStore::default();

        let result = publish_manifest(
            Some(&prepared),
            &manifest,
            &mut catalog,
            &store,
            CatalogPublicationOptions {
                lease_owner: "ready-store-worker",
                lease_duration_ms: 60_000,
                renewal_interval: Duration::from_secs(10),
                cleanup_timeout: Duration::from_millis(100),
                cancellation: &cancellation,
                before_publish: None,
                admission: None,
                on_phase: &mut on_phase,
            },
        )
        .await;

        assert!(matches!(result, Err(SnapshotPublicationError::Cancelled)));
        assert_eq!(catalog.state(&artifact).unwrap(), None);
        assert_eq!(store.head_calls.load(Ordering::SeqCst), 0);
    }

    /// An admission is asked once its lock's exclusive holder lets go, and a
    /// refusal leaves the build uploaded and invisible.
    #[tokio::test]
    async fn an_admission_is_asked_after_its_lock_and_a_refusal_publishes_nothing() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("finalized.artifact");
        let bytes = b"admitted publication bytes";
        std::fs::write(&path, bytes).unwrap();
        let manifest = representation_manifest(bytes);
        let artifact = manifest.manifest().artifact_id.clone();
        let digest = hex::encode(Sha256::digest(bytes));
        let prepared = PreparedRepresentations::prepare(
            &path,
            bytes.len() as u64,
            &digest,
            bytes.len() as u64 + 1024,
        )
        .unwrap();
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("objects"), 2048).unwrap();
        let cancellation = CancellationToken::new();
        let admitted = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let lock = Arc::new(tokio::sync::RwLock::new(()));
        let exclusive = Arc::clone(&lock).write_owned().await;
        let flip = Arc::clone(&admitted);
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            // What the exclusive holder changes is what the admission reads.
            flip.store(false, Ordering::SeqCst);
            drop(exclusive);
        });
        let admission = PublicationAdmission {
            lock: Arc::clone(&lock),
            admit: {
                let admitted = Arc::clone(&admitted);
                Arc::new(move || admitted.load(Ordering::SeqCst))
            },
        };
        let mut ignore_phase = ignore_publication_phase;

        let refused = publish_manifest(
            Some(&prepared),
            &manifest,
            &mut catalog,
            &store,
            CatalogPublicationOptions {
                lease_owner: "admitted-worker",
                lease_duration_ms: 60_000,
                renewal_interval: Duration::from_secs(10),
                cleanup_timeout: Duration::from_secs(1),
                cancellation: &cancellation,
                before_publish: None,
                admission: Some(&admission),
                on_phase: &mut ignore_phase,
            },
        )
        .await;
        release.await.unwrap();

        assert!(matches!(
            refused,
            Err(SnapshotPublicationError::Catalog(CatalogError::PublicationRefused(refused)))
                if refused == artifact
        ));
        assert_eq!(
            catalog.state(&artifact).unwrap(),
            Some(ArtifactState::Uploaded)
        );
        admitted.store(true, Ordering::SeqCst);
        let published = publish_manifest(
            Some(&prepared),
            &manifest,
            &mut catalog,
            &store,
            CatalogPublicationOptions {
                lease_owner: "admitted-worker",
                lease_duration_ms: 60_000,
                renewal_interval: Duration::from_secs(10),
                cleanup_timeout: Duration::from_secs(1),
                cancellation: &cancellation,
                before_publish: None,
                admission: Some(&admission),
                on_phase: &mut ignore_phase,
            },
        )
        .await
        .unwrap();
        assert_eq!(published.1, PublishOutcome::Published);
        assert_eq!(
            catalog.state(&artifact).unwrap(),
            Some(ArtifactState::Published)
        );
    }
}
